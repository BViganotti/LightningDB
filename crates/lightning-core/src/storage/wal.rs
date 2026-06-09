/// Write-Ahead Log (WAL) for durability and crash recovery.
///
/// ## Sync invariants (SyncMode::Normal)
///
/// 1. `log_page_update` — writes page data to WAL but does NOT fsync.
///    Page updates are durable only if the transaction commits.
///
/// 2. `log_commit` — writes the commit record, then calls `flush()` + `sync_all()`.
///    The fsync happens BEFORE acknowledging the commit to the caller.
///    This ensures the commit record is on disk before the caller proceeds.
///
/// 3. `checkpoint` — the Database::checkpoint() sequence is:
///    a. BufferManager::checkpoint() — flush dirty pages to data files, sync data files, truncate WAL
///    b. Save catalog
///    c. Save header with new last_checkpoint_ts
///    On crash between (a) and (c): data is on disk, WAL is truncated, header has old timestamp.
///    Recovery skips entries before old timestamp — correct since data is already on disk.
///
/// 4. WAL is always written BEFORE data. On replay, committed transactions' page updates
///    are applied to data files, ensuring no committed data is lost.
use crate::storage::buffer_manager::PAGE_SIZE;
use crate::SyncMode;
use crate::Result;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use crc::{Algorithm, Crc, Digest};

const CRC32C: Crc<u32> = Crc::<u32>::new(&Algorithm {
    width: 32,
    poly: 0x1EDC6F41,
    init: 0xFFFFFFFF,
    refin: true,
    refout: true,
    xorout: 0xFFFFFFFF,
    check: 0xE3069283,
    residue: 0xB798B438,
});

const WAL_MAGIC: [u8; 4] = *b"LNIW";
const WAL_VERSION: u8 = 0x01;
const WAL_HEADER_SIZE: usize = 5;

const RECORD_TYPE_PAGE_UPDATE: u8 = 1;
const RECORD_TYPE_COMMIT: u8 = 2;

const WAL_CHECKSUM_SIZE: usize = 4;
const WAL_ALIGNMENT: usize = 8;

pub struct WAL {
    file: Mutex<File>,
    committed_txs: Mutex<HashSet<u64>>,
    sync_mode: SyncMode,
    archive_path: Option<std::path::PathBuf>,
    archive_seq: AtomicU64,
}

impl WAL {
    pub fn new(path: &Path, sync_mode: SyncMode) -> Result<Self> {
        let wal_path = path.join("wal.lbug");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(wal_path)?;

        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            Self::write_header(&mut file)?;
            if sync_mode == SyncMode::Normal {
                file.sync_all()?;
            }
        } else {
            Self::validate_header(&mut file)?;
        }

        Ok(Self {
            file: Mutex::new(file),
            committed_txs: Mutex::new(HashSet::new()),
            sync_mode,
            archive_path: None,
            archive_seq: AtomicU64::new(0),
        })
    }

    /// Enable WAL archiving to the specified directory.
    /// Before truncation, the current WAL content is copied to a sequenced
    /// archive file in this directory, enabling point-in-time recovery.
    pub fn enable_archive<P: AsRef<std::path::Path>>(&mut self, archive_dir: P) -> Result<()> {
        let dir = archive_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let max_seq = std::fs::read_dir(&dir)?
            .filter_map(|e| {
                match e {
                    Ok(entry) => Some(entry),
                    Err(err) => {
                        tracing::warn!("Cannot read directory entry in archive: {err}");
                        None
                    }
                }
            })
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wal_") && name.ends_with(".lbug") {
                    match name.trim_start_matches("wal_")
                        .trim_end_matches(".lbug")
                        .parse::<u64>()
                    {
                        Ok(seq) => Some(seq),
                        Err(_) => {
                            tracing::warn!("Archive file '{}' does not have a valid sequence number", name);
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .max()
            .map(|s| s + 1)
            .unwrap_or(0);
        self.archive_seq.store(max_seq, Ordering::Relaxed);
        self.archive_path = Some(dir);
        Ok(())
    }

    fn write_header(file: &mut File) -> Result<()> {
        file.write_all(&WAL_MAGIC)?;
        file.write_all(&[WAL_VERSION])?;
        Ok(())
    }

    fn validate_header(file: &mut File) -> Result<()> {
        let mut magic = [0u8; 4];
        let mut version = [0u8; 1];
        file.read_exact(&mut magic)?;
        file.read_exact(&mut version)?;

        if magic != WAL_MAGIC {
            return Err(crate::LightningError::Internal(format!(
                "WAL file has invalid magic: expected LNIW, got {}",
                String::from_utf8_lossy(&magic)
            )));
        }
        if version[0] != WAL_VERSION {
            return Err(crate::LightningError::Internal(format!(
                "WAL file has unsupported version {}. Expected {}",
                version[0], WAL_VERSION
            )));
        }
        Ok(())
    }

    fn align_position(file: &mut File) -> Result<()> {
        let pos = file.stream_position()?;
        let padding = (WAL_ALIGNMENT - (pos as usize % WAL_ALIGNMENT)) % WAL_ALIGNMENT;
        if padding > 0 {
            let pad = [0u8; 8];
            file.write_all(&pad[..padding])?;
        }
        Ok(())
    }

    pub fn log_page_update(
        &self,
        tx_id: u64,
        file_id: u64,
        page_idx: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut file = self.file.lock();

        let mut digest = CRC32C.digest();
        digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
        digest.update(&tx_id.to_le_bytes());
        digest.update(&file_id.to_le_bytes());
        digest.update(&page_idx.to_le_bytes());
        digest.update(data);
        let checksum = digest.finalize();

        file.write_all(&[RECORD_TYPE_PAGE_UPDATE])?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(&tx_id.to_le_bytes())?;
        file.write_all(&file_id.to_le_bytes())?;
        file.write_all(&page_idx.to_le_bytes())?;
        file.write_all(data)?;

        Self::align_position(&mut file)?;

        Ok(())
    }

    pub fn log_commit(&self, tx_id: u64) -> Result<()> {
        let mut file = self.file.lock();

        let mut digest = CRC32C.digest();
        digest.update(&[RECORD_TYPE_COMMIT]);
        digest.update(&tx_id.to_le_bytes());
        let checksum = digest.finalize();

        file.write_all(&[RECORD_TYPE_COMMIT])?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(&tx_id.to_le_bytes())?;

        Self::align_position(&mut file)?;

        file.flush()?;
        if self.sync_mode == SyncMode::Normal {
            file.sync_all()?;
        }

        drop(file);
        self.committed_txs.lock().insert(tx_id);
        Ok(())
    }

    pub fn replay<F>(
        &self,
        mut apply_page: F,
        last_checkpoint_ts: u64,
    ) -> Result<WALReplayReport>
    where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        let mut commits: HashSet<u64> = HashSet::new();
        let mut pending: HashMap<u64, Vec<(u64, u64, Vec<u8>)>> = HashMap::new();

        let mut records_read = 0u64;
        let mut corrupt_records_skipped = 0u64;
        let mut partial_record_at_eof = false;

        let mut record_type = [0u8; 1];
        loop {
            match file.read_exact(&mut record_type) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let mut checksum_bytes = [0u8; WAL_CHECKSUM_SIZE];

            let record_ok = match record_type[0] {
                RECORD_TYPE_PAGE_UPDATE => {
                    if file.read_exact(&mut checksum_bytes).is_err() {
                        partial_record_at_eof = true;
                        break;
                    }
                    let stored_crc = u32::from_le_bytes(checksum_bytes);

                    let mut tx_id_bytes = [0u8; 8];
                    let mut file_id_bytes = [0u8; 8];
                    let mut page_idx_bytes = [0u8; 8];
                    let mut data = vec![0u8; PAGE_SIZE];

                    if file.read_exact(&mut tx_id_bytes).is_err()
                        || file.read_exact(&mut file_id_bytes).is_err()
                        || file.read_exact(&mut page_idx_bytes).is_err()
                        || file.read_exact(&mut data).is_err()
                    {
                        partial_record_at_eof = true;
                        break;
                    }

                    let mut digest = CRC32C.digest();
                    digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
                    digest.update(&tx_id_bytes);
                    digest.update(&file_id_bytes);
                    digest.update(&page_idx_bytes);
                    digest.update(&data);
                    if digest.finalize() != stored_crc {
                        corrupt_records_skipped += 1;
                        tracing::warn!("Skipping corrupt WAL page update record (checksum mismatch)");
                        continue;
                    }

                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    let file_id = u64::from_le_bytes(file_id_bytes);
                    let page_idx = u64::from_le_bytes(page_idx_bytes);

                    if commits.contains(&tx_id) && tx_id > last_checkpoint_ts {
                        apply_page(file_id, page_idx, &data)?;
                    } else {
                        pending.entry(tx_id).or_default().push((file_id, page_idx, data));
                    }
                    true
                }
                RECORD_TYPE_COMMIT => {
                    if file.read_exact(&mut checksum_bytes).is_err() {
                        partial_record_at_eof = true;
                        break;
                    }
                    let stored_crc = u32::from_le_bytes(checksum_bytes);

                    let mut tx_id_bytes = [0u8; 8];
                    if file.read_exact(&mut tx_id_bytes).is_err() {
                        partial_record_at_eof = true;
                        break;
                    }

                    let mut digest = CRC32C.digest();
                    digest.update(&[RECORD_TYPE_COMMIT]);
                    digest.update(&tx_id_bytes);
                    if digest.finalize() != stored_crc {
                        corrupt_records_skipped += 1;
                        tracing::warn!("Skipping corrupt WAL commit record (checksum mismatch)");
                        continue;
                    }

                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    commits.insert(tx_id);

                    if tx_id > last_checkpoint_ts {
                        if let Some(updates) = pending.remove(&tx_id) {
                            for (file_id, page_idx, data) in updates {
                                apply_page(file_id, page_idx, &data)?;
                            }
                        }
                    }
                    true
                }
                _ => {
                    tracing::warn!(
                        "Skipping unknown WAL record type: {}",
                        record_type[0]
                    );
                    false
                }
            };

            if record_ok {
                records_read += 1;
            }
        }

        for (tx_id, updates) in pending.drain() {
            if tx_id > last_checkpoint_ts && commits.contains(&tx_id) {
                for (file_id, page_idx, data) in updates {
                    apply_page(file_id, page_idx, &data)?;
                }
            }
        }

        for tx_id in &commits {
            if *tx_id > last_checkpoint_ts {
                self.committed_txs.lock().insert(*tx_id);
            }
        }

        Ok(WALReplayReport {
            records_read,
            corrupt_records_skipped,
            partial_record_at_eof,
        })
    }

    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock();

        // Archive WAL before truncation if archiving is enabled
        if let Some(ref archive_dir) = self.archive_path {
            let current_len = file.metadata()?.len();
            if current_len > WAL_HEADER_SIZE as u64 {
                let seq = self.archive_seq.fetch_add(1, Ordering::Relaxed);
                let archive_name = format!("wal_{seq}.lbug");
                let archive_path = archive_dir.join(&archive_name);
                file.seek(SeekFrom::Start(0))?;
                let mut archive_file = File::create(&archive_path)?;
                let mut buf = [0u8; 65536];
                loop {
                    let n = file.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    archive_file.write_all(&buf[..n])?;
                }
                if self.sync_mode == SyncMode::Normal {
                    archive_file.sync_all()?;
                }
                tracing::info!("WAL archived: {} ({} bytes)", archive_name, current_len);
            }
        }

        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Self::write_header(&mut file)?;
        if self.sync_mode == SyncMode::Normal {
            file.sync_all()?;
        }
        drop(file);
        self.committed_txs.lock().clear();
        Ok(())
    }

    pub fn size(&self) -> Result<u64> {
        let file = self.file.lock();
        let metadata = file.metadata()?;
        Ok(metadata.len())
    }

    /// Read WAL records starting at a byte offset. Skips the WAL header.
    /// Returns an iterator that yields parsed records until EOF or error.
    /// If `offset` is past the end of the file (e.g., after truncation),
    /// returns an empty iterator.
    pub fn read_records_from(&self, offset: u64) -> Result<WALRecordIter> {
        let mut file = self.file.lock();
        let file_len = file.metadata()?.len();

        let start = if offset < WAL_HEADER_SIZE as u64 {
            WAL_HEADER_SIZE as u64
        } else {
            offset
        };

        if start >= file_len {
            return Ok(WALRecordIter { buf: Vec::new(), pos: 0, base_offset: start });
        }

        file.seek(SeekFrom::Start(start))?;
        let remaining = (file_len - start) as usize;
        let mut buf = vec![0u8; remaining];
        file.read_exact(&mut buf)?;
        drop(file);

        Ok(WALRecordIter { buf, pos: 0, base_offset: start })
    }
}

/// Iterator over parsed WAL records from a byte buffer.
pub struct WALRecordIter {
    buf: Vec<u8>,
    pos: usize,
    /// Absolute file offset where `buf` starts.
    base_offset: u64,
}

impl WALRecordIter {
    /// The absolute byte position in the WAL file after the last read record.
    /// Use this as the starting offset for the next `read_records_from` call
    /// to avoid re-reading the same records.
    pub fn absolute_pos(&self) -> u64 {
        self.base_offset + self.pos as u64
    }

    pub fn next_record(&mut self) -> Option<WALRecord> {
        while self.pos + 1 <= self.buf.len() {
            let record_type = self.buf[self.pos];
            match record_type {
                RECORD_TYPE_PAGE_UPDATE => {
                    let needed = 1 + WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE;
                    if self.pos + needed > self.buf.len() {
                        return None;
                    }
                    let mut crc_bytes = [0u8; WAL_CHECKSUM_SIZE];
                    crc_bytes.copy_from_slice(&self.buf[self.pos + 1..self.pos + 1 + WAL_CHECKSUM_SIZE]);
                    let stored_crc = u32::from_le_bytes(crc_bytes);

                    let off = self.pos + 1 + WAL_CHECKSUM_SIZE;
                    let tx_id = u64::from_le_bytes(self.buf[off..off + 8].try_into().ok()?);
                    let file_id = u64::from_le_bytes(self.buf[off + 8..off + 16].try_into().ok()?);
                    let page_idx = u64::from_le_bytes(self.buf[off + 16..off + 24].try_into().ok()?);
                    let data_start = off + 24;
                    let data = self.buf[data_start..data_start + PAGE_SIZE].to_vec();

                    let mut digest = CRC32C.digest();
                    digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
                    digest.update(&tx_id.to_le_bytes());
                    digest.update(&file_id.to_le_bytes());
                    digest.update(&page_idx.to_le_bytes());
                    digest.update(&data);
                    let _computed_crc = digest.finalize();

                    let record = WALRecord::PageUpdate { tx_id, file_id, page_idx, data };

                    // Advance past record + alignment
                    self.pos += needed;
                    self.align_position();
                    return Some(record);
                }
                RECORD_TYPE_COMMIT => {
                    let needed = 1 + WAL_CHECKSUM_SIZE + 8;
                    if self.pos + needed > self.buf.len() {
                        return None;
                    }
                    let off = self.pos + 1 + WAL_CHECKSUM_SIZE;
                    let tx_id = u64::from_le_bytes(self.buf[off..off + 8].try_into().ok()?);

                    let record = WALRecord::Commit { tx_id };

                    self.pos += needed;
                    self.align_position();
                    return Some(record);
                }
                _ => {
                    // Unknown record type — skip one byte and continue
                    self.pos += 1;
                }
            }
        }
        None
    }

    fn align_position(&mut self) {
        let padding = (WAL_ALIGNMENT - (self.pos % WAL_ALIGNMENT)) % WAL_ALIGNMENT;
        self.pos = std::cmp::min(self.pos + padding, self.buf.len());
    }
}

pub enum WALRecord {
    PageUpdate {
        tx_id: u64,
        file_id: u64,
        page_idx: u64,
        data: Vec<u8>,
    },
    Commit {
        tx_id: u64,
    },
}

pub struct WALReplayReport {
    pub records_read: u64,
    pub corrupt_records_skipped: u64,
    pub partial_record_at_eof: bool,
}
