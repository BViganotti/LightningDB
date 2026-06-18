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
use crc::{Algorithm, Crc};

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
    /// Pending group-commit buffer: serialized WAL bytes not yet written to disk.
    /// Accumulated by `log_page_update`, flushed by `log_commit`.
    pending_buf: Mutex<Vec<u8>>,
    /// Read-write lock for CDC coordination: CDC readers acquire read lock,
    /// writers (log_commit) acquire write lock. Ensures the CDC thread never
    /// observes a partially-written commit batch.
    cdc_lock: parking_lot::RwLock<()>,
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
            pending_buf: Mutex::new(Vec::with_capacity(65536)),
            cdc_lock: parking_lot::RwLock::new(()),
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
        self.archive_seq.store(max_seq, Ordering::Release);
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
        let mut digest = CRC32C.digest();
        digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
        digest.update(&tx_id.to_le_bytes());
        digest.update(&file_id.to_le_bytes());
        digest.update(&page_idx.to_le_bytes());
        digest.update(data);
        let checksum = digest.finalize();

        // Serialize into pending buffer (group commit: batched write at commit time)
        let mut buf = self.pending_buf.lock();
        buf.extend_from_slice(&[RECORD_TYPE_PAGE_UPDATE]);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&tx_id.to_le_bytes());
        buf.extend_from_slice(&file_id.to_le_bytes());
        buf.extend_from_slice(&page_idx.to_le_bytes());
        buf.extend_from_slice(data);

        Ok(())
    }

    pub fn log_commit(&self, tx_id: u64) -> Result<()> {
        // Flush all pending page updates from group commit buffer
        let pending_data = {
            let mut buf = self.pending_buf.lock();
            if buf.is_empty() {
                None
            } else {
                Some(std::mem::take(&mut *buf))
            }
        };

        let mut commit_record = Vec::new();
        if let Some(ref data) = pending_data {
            commit_record.extend_from_slice(data);
        }

        let mut digest = CRC32C.digest();
        digest.update(&[RECORD_TYPE_COMMIT]);
        digest.update(&tx_id.to_le_bytes());
        let checksum = digest.finalize();

        commit_record.extend_from_slice(&[RECORD_TYPE_COMMIT]);
        commit_record.extend_from_slice(&checksum.to_le_bytes());
        commit_record.extend_from_slice(&tx_id.to_le_bytes());

        // Write commit record and flush while holding file lock (fast path).
        // Obtain the raw fd under the lock, then release before sync_all()
        // so concurrent WAL writers are NOT blocked during the slow fsync call.
        // The cdc_lock write guard ensures CDC readers never observe a
        // partially-written commit batch.
        #[cfg(unix)]
        let sync_fd = {
            let _cdc_guard = self.cdc_lock.write();
            use std::os::unix::io::AsRawFd;
            let mut file = self.file.lock();
            if !commit_record.is_empty() {
                file.write_all(&commit_record)?;
            }
            Self::align_position(&mut file)?;
            file.flush()?;
            file.as_raw_fd()
        };
        #[cfg(not(unix))]
        {
            let _cdc_guard = self.cdc_lock.write();
            let mut file = self.file.lock();
            if !commit_record.is_empty() {
                file.write_all(&commit_record)?;
            }
            Self::align_position(&mut file)?;
            file.flush()?;
        }

        // sync_all() WITHOUT holding the file lock.
        // The raw fd remains valid as long as the WAL file is not closed/reopened.
        if self.sync_mode == SyncMode::Normal {
            #[cfg(unix)]
            {
                let ret = unsafe { libc::fsync(sync_fd) };
                if ret != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            #[cfg(not(unix))]
            {
                let file = self.file.lock();
                file.sync_all()?;
            }
        }

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
                let seq = self.archive_seq.fetch_add(1, Ordering::AcqRel);
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
    /// Maximum bytes to read from the WAL in a single `read_records_from` call.
    /// If the remaining WAL is larger, callers should loop by passing the
    /// iterator's `absolute_pos()` as the next offset.
    const MAX_WAL_READ_SIZE: usize = 64 * 1024 * 1024; // 64 MB

    pub fn read_records_from(&self, offset: u64) -> Result<WALRecordIter> {
        // Acquire CDC read lock to prevent concurrent log_commit from
        // writing partial data while we read.
        let _cdc_guard = self.cdc_lock.read();
        let (buf, start) = {
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
            let to_read = remaining.min(Self::MAX_WAL_READ_SIZE);
            let mut buf = vec![0u8; to_read];
            file.read_exact(&mut buf)?;
            drop(file);

            (buf, start)
        };

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
                    let computed_crc = digest.finalize();
                    if computed_crc != stored_crc {
                        return Some(WALRecord::Corrupt {
                            msg: format!(
                                "CRC mismatch at offset {}: computed {:08x} != stored {:08x}",
                                self.base_offset + self.pos as u64,
                                computed_crc,
                                stored_crc
                            ),
                        });
                    }

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

                    // Validate CRC for commit records
                    let mut crc_bytes = [0u8; WAL_CHECKSUM_SIZE];
                    crc_bytes.copy_from_slice(&self.buf[self.pos + 1..self.pos + 1 + WAL_CHECKSUM_SIZE]);
                    let stored_crc = u32::from_le_bytes(crc_bytes);
                    let data_start = self.pos + 1 + WAL_CHECKSUM_SIZE;
                    let data_end = data_start + 8;
                    if data_end <= self.buf.len() {
                        let mut digest = CRC32C.digest();
                        digest.update(&self.buf[data_start..data_end]);
                        let computed_crc = digest.finalize();
                        if computed_crc != stored_crc {
                            tracing::warn!(
                                "Skipping corrupt WAL commit record (CRC mismatch: computed {:08x} != stored {:08x})",
                                computed_crc, stored_crc
                            );
                            self.pos += needed;
                            self.align_position();
                            continue;
                        }
                    }

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
    Corrupt {
        msg: String,
    },
}

pub struct WALReplayReport {
    pub records_read: u64,
    pub corrupt_records_skipped: u64,
    pub partial_record_at_eof: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_manager::PAGE_SIZE;

    fn create_wal(sync_mode: SyncMode) -> (WAL, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal = WAL::new(dir.path(), sync_mode).unwrap();
        (wal, dir)
    }

    #[test]
    fn test_new_wal_creates_header() {
        let (wal, _dir) = create_wal(SyncMode::Normal);
        let size = wal.size().unwrap();
        assert_eq!(size, WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_new_wal_invalid_magic() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.lbug");
        std::fs::write(&wal_path, b"BOGUS").unwrap();
        let result = WAL::new(dir.path(), SyncMode::Normal);
        match result {
            Err(e) => assert!(e.to_string().contains("invalid magic")),
            Ok(_) => panic!("expected Err for invalid magic"),
        }
    }

    #[test]
    fn test_log_page_update_and_commit() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let data = vec![0xABu8; PAGE_SIZE];
        wal.log_page_update(1, 42, 7, &data).unwrap();
        wal.log_commit(1).unwrap();
        let size = wal.size().unwrap();
        assert!(size > WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_replay_committed_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.lbug");
        {
            let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
            let data = vec![0xCDu8; PAGE_SIZE];
            wal.log_page_update(1, 0, 0, &data).unwrap();
            wal.log_commit(1).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let mut applied: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        let report = wal.replay(
            |fid, pid, data| {
                applied.push((fid, pid, data.to_vec()));
                Ok(())
            },
            0,
        ).unwrap();
        assert_eq!(report.records_read, 2);
        assert_eq!(report.corrupt_records_skipped, 0);
        assert!(!report.partial_record_at_eof);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0], (0, 0, vec![0xCDu8; PAGE_SIZE]));
    }

    #[test]
    fn test_replay_skips_pre_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
            let data = vec![0xEFu8; PAGE_SIZE];
            wal.log_page_update(1, 0, 0, &data).unwrap();
            wal.log_commit(1).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let mut applied = 0u64;
        let report = wal.replay(
            |_fid, _pid, _data| { applied += 1; Ok(()) },
            1,
        ).unwrap();
        assert_eq!(applied, 0);
        assert_eq!(report.records_read, 2);
    }

    #[test]
    fn test_replay_partial_record_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.lbug");
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[WAL_VERSION]).unwrap();
            f.write_all(&[RECORD_TYPE_PAGE_UPDATE]).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert!(report.partial_record_at_eof);
    }

    #[test]
    fn test_replay_corrupt_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.lbug");
        {
            let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
            let data = vec![0xAAu8; PAGE_SIZE];
            wal.log_page_update(1, 0, 0, &data).unwrap();
            wal.log_commit(1).unwrap();
        }
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_path)
                .unwrap();
            f.seek(std::io::SeekFrom::Start(WAL_HEADER_SIZE as u64 + 10)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert_eq!(report.corrupt_records_skipped, 1);
    }

    #[test]
    fn test_truncate_resets_wal() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let data = vec![0xBBu8; PAGE_SIZE];
        wal.log_page_update(1, 0, 0, &data).unwrap();
        wal.log_commit(1).unwrap();
        let before = wal.size().unwrap();
        assert!(before > WAL_HEADER_SIZE as u64);
        wal.truncate().unwrap();
        let after = wal.size().unwrap();
        assert_eq!(after, WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_sync_mode_off() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let data = vec![0xCCu8; PAGE_SIZE];
        wal.log_page_update(1, 0, 0, &data).unwrap();
        wal.log_commit(1).unwrap();
        assert!(wal.size().unwrap() > WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_multiple_transactions() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        for tx in 0..5 {
            let data = vec![tx as u8; PAGE_SIZE];
            wal.log_page_update(tx, 0, tx, &data).unwrap();
            wal.log_commit(tx).unwrap();
        }
        let size = wal.size().unwrap();
        assert!(size > WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_read_records_from_offset() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let data = vec![0xDDu8; PAGE_SIZE];
        wal.log_page_update(1, 0, 0, &data).unwrap();
        wal.log_commit(1).unwrap();

        let mut iter = wal.read_records_from(0).unwrap();
        let r1 = iter.next_record();
        assert!(matches!(r1, Some(WALRecord::PageUpdate { .. })));
        let r2 = iter.next_record();
        assert!(matches!(r2, Some(WALRecord::Commit { .. })));
        let r3 = iter.next_record();
        assert!(r3.is_none());
    }

    #[test]
    fn test_read_records_past_eof() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let mut iter = wal.read_records_from(999_999).unwrap();
        assert!(iter.next_record().is_none());
    }

    #[test]
    fn test_empty_wal_no_records() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        let mut iter = wal.read_records_from(0).unwrap();
        assert!(iter.next_record().is_none());
    }

    #[test]
    fn test_align_position_roundtrip() {
        let data = vec![0xFFu8; PAGE_SIZE];
        let dir = tempfile::tempdir().unwrap();
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        for tx in 0..3 {
            wal.log_page_update(tx, 0, tx, &data).unwrap();
            wal.log_commit(tx).unwrap();
        }
        let mut iter = wal.read_records_from(0).unwrap();
        let mut count = 0u64;
        while let Some(record) = iter.next_record() {
            match record {
                WALRecord::PageUpdate { .. } | WALRecord::Commit { .. } => count += 1,
                WALRecord::Corrupt { msg } => panic!("Unexpected corrupt record: {msg}"),
            }
        }
        assert_eq!(count, 6);
    }

    #[test]
    fn test_archiving() {
        let dir = tempfile::tempdir().unwrap();
        let archive_dir = tempfile::tempdir().unwrap();
        let mut wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        wal.enable_archive(archive_dir.path()).unwrap();
        let data = vec![0xEEu8; PAGE_SIZE];
        wal.log_page_update(1, 0, 0, &data).unwrap();
        wal.log_commit(1).unwrap();
        wal.truncate().unwrap();
        let entries: Vec<_> = std::fs::read_dir(archive_dir.path()).unwrap().collect();
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_group_commit_buffer_flushed() {
        let (wal, _dir) = create_wal(SyncMode::Off);
        for i in 0..10 {
            let data = vec![i; PAGE_SIZE];
            wal.log_page_update(1, 0, i as u64, &data).unwrap();
        }
        assert_eq!(wal.size().unwrap(), WAL_HEADER_SIZE as u64);
        wal.log_commit(1).unwrap();
        assert!(wal.size().unwrap() > WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_replay_applies_pending_after_commit_record() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
            let data = vec![0x11u8; PAGE_SIZE];
            wal.log_page_update(2, 1, 100, &data).unwrap();
            wal.log_commit(2).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let mut applied = Vec::new();
        wal.replay(
            |fid, pid, data| { applied.push((fid, pid, data.to_vec())); Ok(()) },
            0,
        ).unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0], (1, 100, vec![0x11u8; PAGE_SIZE]));
    }

    #[test]
    fn test_replay_commit_without_page_update() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
            wal.log_commit(99).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert_eq!(report.records_read, 1);
        assert_eq!(report.corrupt_records_skipped, 0);
    }

    #[test]
    fn test_replay_unknown_record_type() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.lbug");
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[WAL_VERSION]).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert_eq!(report.records_read, 0);
        assert_eq!(report.corrupt_records_skipped, 0);
    }
}
