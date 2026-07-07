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
///
/// ## WAL Format
///
/// ### Header (v1 and v2, 5 bytes):
///   `[magic:4="LNIW"][version:1]`
///
/// ### Record v1 (legacy, detected during migration):
///   `[type:1][crc32c:4][payload...]`
///
/// ### Record v2 (current, WAL_VERSION=0x02):
///   `[type:1][length:2][crc32c:4][payload...]`
///
///   - `length` covers everything after the length field (CRC + payload).
///   - `crc32c` covers type + length + payload.
///
///   The per-record length prefix allows the parser to skip past corrupted records
///   without losing synchronization — the fundamental improvement over v1.
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
const WAL_VERSION: u8 = 0x02;
const WAL_VERSION_V1: u8 = 0x01;
const WAL_HEADER_SIZE: usize = 5;

const RECORD_TYPE_PAGE_UPDATE: u8 = 1;
const RECORD_TYPE_COMMIT: u8 = 2;

const WAL_CHECKSUM_SIZE: usize = 4;
const WAL_LENGTH_SIZE: usize = 2;
const WAL_ALIGNMENT: usize = 8;

/// Maximum bytes to skip for a single corrupt record before giving up
/// and marking the remainder of the WAL as unrecoverable.
/// Prevents infinite loops on severely corrupted WALs.
const MAX_SKIP_PER_CORRUPT_RECORD: usize = 65536;

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
    /// WAL version read from the header at open time.
    /// Dictates whether records use v1 or v2 on-disk format during replay.
    wal_version: u8,
}

impl WAL {
    pub fn new(path: &Path, sync_mode: SyncMode) -> Result<Self> {
        let wal_path = path.join("wal.ltng");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)?;

        let version = {
            let metadata = file.metadata()?;
            if metadata.len() == 0 {
                // Fresh WAL — write v2 header
                Self::write_header(&mut file)?;
                if sync_mode == SyncMode::Normal {
                    file.sync_all()?;
                }
                WAL_VERSION
            } else if metadata.len() < WAL_HEADER_SIZE as u64 {
                // File exists but too small for a valid header.
                // This happens when a crash occurs between set_len(0) and
                // write_header() during truncation. Safe to reinitialize:
                // all committed data is already on data files at that point.
                tracing::warn!(
                    "WAL file truncated ({} bytes < {} byte header), reinitializing",
                    metadata.len(), WAL_HEADER_SIZE
                );
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                Self::write_header(&mut file)?;
                if sync_mode == SyncMode::Normal {
                    file.sync_all()?;
                }
                WAL_VERSION
            } else {
                match Self::validate_header(&mut file) {
                    Ok(ver) => ver,
                    Err(e) => {
                        // Header corrupt (bad magic, unknown version, etc.).
                        // Safe to reinitialize: a corrupt WAL header means we
                        // can't replay any records anyway, and all checkpointed
                        // data is on data files. Uncheckpointed committed
                        // transactions are lost, but this is the same outcome
                        // as if the crash had occurred before the next checkpoint.
                        tracing::warn!("WAL header corrupt, reinitializing: {e}");
                        file.set_len(0)?;
                        file.seek(SeekFrom::Start(0))?;
                        Self::write_header(&mut file)?;
                        if sync_mode == SyncMode::Normal {
                            file.sync_all()?;
                        }
                        WAL_VERSION
                    }
                }
            }
        };

        Ok(Self {
            file: Mutex::new(file),
            committed_txs: Mutex::new(HashSet::new()),
            sync_mode,
            archive_path: None,
            archive_seq: AtomicU64::new(0),
            pending_buf: Mutex::new(Vec::with_capacity(65536)),
            cdc_lock: parking_lot::RwLock::new(()),
            wal_version: version,
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
                if name.starts_with("wal_") && name.ends_with(".ltng") {
                    match name.trim_start_matches("wal_")
                        .trim_end_matches(".ltng")
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

    fn validate_header(file: &mut File) -> Result<u8> {
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
        match version[0] {
            WAL_VERSION_V1 => Ok(WAL_VERSION_V1),
            WAL_VERSION => Ok(WAL_VERSION),
            _ => Err(crate::LightningError::Internal(format!(
                "WAL file has unsupported version {}. Expected {} or {}",
                version[0], WAL_VERSION_V1, WAL_VERSION
            ))),
        }
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

    /// Compute CRC32C for a v2 record.
    /// CRC covers: record_type + length_bytes + payload
    fn compute_checksum_v2(record_type: u8, length: u16, payload: &[u8]) -> u32 {
        let mut digest = CRC32C.digest();
        digest.update(&[record_type]);
        digest.update(&length.to_le_bytes());
        digest.update(payload);
        digest.finalize()
    }

    pub fn log_page_update(
        &self,
        tx_id: u64,
        file_id: u64,
        page_idx: u64,
        data: &[u8],
    ) -> Result<()> {
        let payload_len = WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE;
        let length = payload_len as u16;

        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        payload.extend_from_slice(&file_id.to_le_bytes());
        payload.extend_from_slice(&page_idx.to_le_bytes());
        payload.extend_from_slice(data);
        let checksum = Self::compute_checksum_v2(RECORD_TYPE_PAGE_UPDATE, length, &payload);

        let mut buf = self.pending_buf.lock();
        buf.extend_from_slice(&[RECORD_TYPE_PAGE_UPDATE]);
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&payload);

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

        // v2 commit record: [type:1][length:2][crc32c:4][tx_id:8]
        let payload_len = WAL_CHECKSUM_SIZE + 8;
        let length = payload_len as u16;

        let mut payload = Vec::with_capacity(8);
        payload.extend_from_slice(&tx_id.to_le_bytes());
        let checksum = Self::compute_checksum_v2(RECORD_TYPE_COMMIT, length, &payload);

        commit_record.extend_from_slice(&[RECORD_TYPE_COMMIT]);
        commit_record.extend_from_slice(&length.to_le_bytes());
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

            let record_ok = match record_type[0] {
                RECORD_TYPE_PAGE_UPDATE => {
                    if !self.read_and_apply_page_update(&mut file, &mut commits, &mut pending, &mut apply_page, last_checkpoint_ts, &mut corrupt_records_skipped, &mut partial_record_at_eof) {
                        if partial_record_at_eof {
                            break;
                        }
                        continue;
                    }
                    true
                }
                RECORD_TYPE_COMMIT => {
                    if !self.read_and_apply_commit(&mut file, &mut commits, &mut pending, &mut apply_page, last_checkpoint_ts, &mut corrupt_records_skipped, &mut partial_record_at_eof) {
                        if partial_record_at_eof {
                            break;
                        }
                        continue;
                    }
                    true
                }
                0 => {
                    // Zero bytes are valid alignment padding written by align_position()
                    // after commit records to maintain 8-byte alignment boundaries.
                    // Skip silently — they carry no data and are not unknown records.
                    false
                }
                _ => {
                    self.skip_unknown_record(&mut file, record_type[0], &mut corrupt_records_skipped, &mut partial_record_at_eof);
                    if partial_record_at_eof {
                        break;
                    }
                    false
                }
            };

            if record_ok {
                records_read += 1;
            }
        }

        // Drain remaining pending: apply committed transactions that
        // had page updates before the commit record in the WAL.
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

    /// Read and process a v1 or v2 PAGE_UPDATE record.
    /// Returns false if the record was corrupt and should be skipped.
    /// Sets partial_record_at_eof if EOF was encountered mid-record.
    fn read_and_apply_page_update<F>(
        &self,
        file: &mut File,
        commits: &mut HashSet<u64>,
        pending: &mut HashMap<u64, Vec<(u64, u64, Vec<u8>)>>,
        apply_page: &mut F,
        last_checkpoint_ts: u64,
        corrupt_records_skipped: &mut u64,
        partial_record_at_eof: &mut bool,
    ) -> bool
    where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        let mut checksum_bytes = [0u8; WAL_CHECKSUM_SIZE];

        if self.wal_version >= 2 {
            // v2 format: [type:1][length:2][crc32c:4][tx_id:8][file_id:8][page_idx:8][data:4096]
            let mut length_bytes = [0u8; WAL_LENGTH_SIZE];
            if file.read_exact(&mut length_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }
            let record_length = u16::from_le_bytes(length_bytes) as usize;

            if file.read_exact(&mut checksum_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }

            let payload_size = record_length.saturating_sub(WAL_CHECKSUM_SIZE);
            let mut payload = vec![0u8; payload_size];
            if file.read_exact(&mut payload).is_err() {
                *partial_record_at_eof = true;
                return false;
            }

            let stored_crc = u32::from_le_bytes(checksum_bytes);
            let expected_crc = Self::compute_checksum_v2(
                RECORD_TYPE_PAGE_UPDATE,
                record_length as u16,
                &payload,
            );
            if expected_crc != stored_crc {
                *corrupt_records_skipped += 1;
                tracing::warn!(
                    "Skipping corrupt WAL page update record (checksum mismatch)"
                );
                return false;
            }

            if payload_size < 24 {
                *corrupt_records_skipped += 1;
                tracing::warn!(
                    "Skipping WAL page update record with truncated payload ({} bytes)",
                    payload_size
                );
                return false;
            }

            let tx_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let file_id = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            let page_idx = u64::from_le_bytes(payload[16..24].try_into().unwrap());
            let data = &payload[24..];

            Self::handle_page_update(commits, pending, apply_page, last_checkpoint_ts, tx_id, file_id, page_idx, data);
            true
        } else {
            // v1 format: [type:1][crc32c:4][tx_id:8][file_id:8][page_idx:8][data:4096]
            if file.read_exact(&mut checksum_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
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
                *partial_record_at_eof = true;
                return false;
            }

            let mut digest = CRC32C.digest();
            digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
            digest.update(&tx_id_bytes);
            digest.update(&file_id_bytes);
            digest.update(&page_idx_bytes);
            digest.update(&data);
            if digest.finalize() != stored_crc {
                *corrupt_records_skipped += 1;
                tracing::warn!("Skipping corrupt WAL page update record (checksum mismatch)");
                return false;
            }

            let tx_id = u64::from_le_bytes(tx_id_bytes);
            let file_id = u64::from_le_bytes(file_id_bytes);
            let page_idx = u64::from_le_bytes(page_idx_bytes);

            Self::handle_page_update(commits, pending, apply_page, last_checkpoint_ts, tx_id, file_id, page_idx, &data);
            true
        }
    }

    fn handle_page_update<F>(
        commits: &HashSet<u64>,
        pending: &mut HashMap<u64, Vec<(u64, u64, Vec<u8>)>>,
        apply_page: &mut F,
        last_checkpoint_ts: u64,
        tx_id: u64,
        file_id: u64,
        page_idx: u64,
        data: &[u8],
    ) where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        if commits.contains(&tx_id) && tx_id > last_checkpoint_ts {
            let _ = apply_page(file_id, page_idx, data);
        } else {
            pending.entry(tx_id).or_default().push((file_id, page_idx, data.to_vec()));
        }
    }

    /// Read and process a v1 or v2 COMMIT record.
    /// Returns false if the record was corrupt and should be skipped.
    /// Sets partial_record_at_eof if EOF was encountered mid-record.
    fn read_and_apply_commit<F>(
        &self,
        file: &mut File,
        commits: &mut HashSet<u64>,
        pending: &mut HashMap<u64, Vec<(u64, u64, Vec<u8>)>>,
        apply_page: &mut F,
        last_checkpoint_ts: u64,
        corrupt_records_skipped: &mut u64,
        partial_record_at_eof: &mut bool,
    ) -> bool
    where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        let mut checksum_bytes = [0u8; WAL_CHECKSUM_SIZE];

        if self.wal_version >= 2 {
            // v2 format: [type:1][length:2][crc32c:4][tx_id:8]
            let mut length_bytes = [0u8; WAL_LENGTH_SIZE];
            if file.read_exact(&mut length_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }
            let record_length = u16::from_le_bytes(length_bytes) as usize;

            if file.read_exact(&mut checksum_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }

            let payload_size = record_length.saturating_sub(WAL_CHECKSUM_SIZE);
            // Sanity check: a commit record payload should be exactly 8 bytes (tx_id)
            if payload_size != 8 {
                *corrupt_records_skipped += 1;
                tracing::warn!(
                    "Skipping corrupt WAL commit record with invalid payload size {}",
                    payload_size
                );
                // Skip the payload bytes to maintain file position
                let mut discard = vec![0u8; payload_size.min(65536)];
                let _ = file.read(&mut discard);
                return false;
            }

            let mut tx_id_bytes = [0u8; 8];
            if file.read_exact(&mut tx_id_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }

            let stored_crc = u32::from_le_bytes(checksum_bytes);
            let expected_crc = Self::compute_checksum_v2(
                RECORD_TYPE_COMMIT,
                record_length as u16,
                &tx_id_bytes,
            );
            if expected_crc != stored_crc {
                *corrupt_records_skipped += 1;
                tracing::warn!("Skipping corrupt WAL commit record (checksum mismatch)");
                return false;
            }

            let tx_id = u64::from_le_bytes(tx_id_bytes);
            commits.insert(tx_id);

            if tx_id > last_checkpoint_ts {
                if let Some(updates) = pending.remove(&tx_id) {
                    for (fid, pid, data) in updates {
                        let _ = apply_page(fid, pid, &data);
                    }
                }
            }
            true
        } else {
            // v1 format: [type:1][crc32c:4][tx_id:8]
            if file.read_exact(&mut checksum_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }
            let stored_crc = u32::from_le_bytes(checksum_bytes);

            let mut tx_id_bytes = [0u8; 8];
            if file.read_exact(&mut tx_id_bytes).is_err() {
                *partial_record_at_eof = true;
                return false;
            }

            let mut digest = CRC32C.digest();
            digest.update(&[RECORD_TYPE_COMMIT]);
            digest.update(&tx_id_bytes);
            if digest.finalize() != stored_crc {
                *corrupt_records_skipped += 1;
                tracing::warn!("Skipping corrupt WAL commit record (checksum mismatch)");
                return false;
            }

            let tx_id = u64::from_le_bytes(tx_id_bytes);
            commits.insert(tx_id);

            if tx_id > last_checkpoint_ts {
                if let Some(updates) = pending.remove(&tx_id) {
                    for (fid, pid, data) in updates {
                        let _ = apply_page(fid, pid, &data);
                    }
                }
            }
            true
        }
    }

    /// Skip past an unknown record type.
    ///
    /// In v2 format, we read the 2-byte length and skip that many bytes.
    /// In v1 format, we can only advance 1 byte (no length prefix).
    ///
    /// A max skip limit prevents infinite loops on heavily corrupted WALs.
    fn skip_unknown_record(
        &self,
        file: &mut File,
        record_type_byte: u8,
        corrupt_records_skipped: &mut u64,
        partial_record_at_eof: &mut bool,
    ) {
        if self.wal_version >= 2 {
            let mut length_bytes = [0u8; WAL_LENGTH_SIZE];
            if file.read_exact(&mut length_bytes).is_err() {
                *partial_record_at_eof = true;
                return;
            }
            let record_length = u16::from_le_bytes(length_bytes) as usize;

            // Cap skip to prevent massive jumps from corrupted length fields
            let skip_bytes = record_length.min(MAX_SKIP_PER_CORRUPT_RECORD);
            let mut discard = vec![0u8; skip_bytes.min(8192)];
            let mut remaining = skip_bytes;
            while remaining > 0 {
                let to_read = remaining.min(discard.len());
                match file.read(&mut discard[..to_read]) {
                    Ok(0) => {
                        *partial_record_at_eof = true;
                        return;
                    }
                    Ok(n) => remaining -= n,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        *partial_record_at_eof = true;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("I/O error skipping corrupt WAL record: {e}");
                        *partial_record_at_eof = true;
                        return;
                    }
                }
            }

            *corrupt_records_skipped += 1;
            if record_length > MAX_SKIP_PER_CORRUPT_RECORD {
                tracing::warn!(
                    "Skipping unknown WAL record type: {} with implausible length {} \
                     (capped at {} byte skip)",
                    record_type_byte, record_length, MAX_SKIP_PER_CORRUPT_RECORD
                );
            } else {
                tracing::warn!(
                    "Skipping unknown WAL record type: {} ({} byte record)",
                    record_type_byte, 1 + WAL_LENGTH_SIZE + record_length
                );
            }
        } else {
            // v1: no length prefix — advance one byte and retry.
            // This will produce one warning per byte of corrupted data,
            // which is noisy but the only option without record framing.
            tracing::warn!(
                "Skipping unknown WAL record type: {}",
                record_type_byte
            );
            *corrupt_records_skipped += 1;
        }
    }

    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock();

        // Archive WAL before truncation if archiving is enabled
        if let Some(ref archive_dir) = self.archive_path {
            let current_len = file.metadata()?.len();
            if current_len > WAL_HEADER_SIZE as u64 {
                let seq = self.archive_seq.fetch_add(1, Ordering::AcqRel);
                let archive_name = format!("wal_{seq}.ltng");
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
        let version = self.wal_version;
        let (buf, start) = {
            let mut file = self.file.lock();
            let file_len = file.metadata()?.len();

            let start = if offset < WAL_HEADER_SIZE as u64 {
                WAL_HEADER_SIZE as u64
            } else {
                offset
            };

            if start >= file_len {
                return Ok(WALRecordIter { buf: Vec::new(), pos: 0, base_offset: start, version });
            }

            file.seek(SeekFrom::Start(start))?;
            let remaining = (file_len - start) as usize;
            let to_read = remaining.min(Self::MAX_WAL_READ_SIZE);
            let mut buf = vec![0u8; to_read];
            file.read_exact(&mut buf)?;
            drop(file);

            (buf, start)
        };

        Ok(WALRecordIter { buf, pos: 0, base_offset: start, version })
    }
}

/// Iterator over parsed WAL records from a byte buffer.
pub struct WALRecordIter {
    buf: Vec<u8>,
    pos: usize,
    /// Absolute file offset where `buf` starts.
    base_offset: u64,
    /// WAL version used to determine record format.
    version: u8,
}

impl WALRecordIter {
    /// The absolute byte position in the WAL file after the last read record.
    /// Use this as the starting offset for the next `read_records_from` call
    /// to avoid re-reading the same records.
    pub fn absolute_pos(&self) -> u64 {
        self.base_offset + self.pos as u64
    }

    pub fn next_record(&mut self) -> Option<WALRecord> {
        while self.pos < self.buf.len() {
            let record_type = self.buf[self.pos];
            match record_type {
                RECORD_TYPE_PAGE_UPDATE => {
                    let needed = if self.version >= 2 {
                        1 + WAL_LENGTH_SIZE + WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE
                    } else {
                        1 + WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE
                    };
                    if self.pos + needed > self.buf.len() {
                        return None;
                    }

                    if self.version >= 2 {
                        // v2: [type:1][length:2][crc32c:4][tx_id:8][file_id:8][page_idx:8][data:4096]
                        let length_bytes: [u8; 2] = self.buf[self.pos + 1..self.pos + 1 + 2].try_into().ok()?;
                        let record_length = u16::from_le_bytes(length_bytes);

                        let crc_start = self.pos + 1 + WAL_LENGTH_SIZE;
                        let payload_start = crc_start + WAL_CHECKSUM_SIZE;

                        let stored_crc = u32::from_le_bytes(
                            self.buf[crc_start..crc_start + WAL_CHECKSUM_SIZE].try_into().ok()?
                        );

                        // Validate the length matches expected page update size
                        let expected_payload = WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE;
                        if record_length as usize != expected_payload {
                            return Some(WALRecord::Corrupt {
                                msg: format!(
                                    "PageUpdate record at offset {} has invalid length {} (expected {})",
                                    self.base_offset + self.pos as u64,
                                    record_length,
                                    expected_payload,
                                ),
                            });
                        }

                        let tx_id = u64::from_le_bytes(
                            self.buf[payload_start..payload_start + 8].try_into().ok()?
                        );
                        let file_id = u64::from_le_bytes(
                            self.buf[payload_start + 8..payload_start + 16].try_into().ok()?
                        );
                        let page_idx = u64::from_le_bytes(
                            self.buf[payload_start + 16..payload_start + 24].try_into().ok()?
                        );
                        let data = self.buf[payload_start + 24..payload_start + 24 + PAGE_SIZE].to_vec();

                        let computed_crc = WAL::compute_checksum_v2(
                            RECORD_TYPE_PAGE_UPDATE,
                            record_length,
                            &self.buf[payload_start..payload_start + 8 + 8 + 8 + PAGE_SIZE],
                        );

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

                        self.pos += needed;
                        return Some(WALRecord::PageUpdate { tx_id, file_id, page_idx, data });
                    } else {
                        // v1: [type:1][crc32c:4][tx_id:8][file_id:8][page_idx:8][data:4096]
                        let off = self.pos + 1 + WAL_CHECKSUM_SIZE;
                        let stored_crc = u32::from_le_bytes(
                            self.buf[self.pos + 1..self.pos + 1 + WAL_CHECKSUM_SIZE].try_into().ok()?
                        );

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

                        self.pos += needed;
                        return Some(record);
                    }
                }
                RECORD_TYPE_COMMIT => {
                    let needed = if self.version >= 2 {
                        1 + WAL_LENGTH_SIZE + WAL_CHECKSUM_SIZE + 8
                    } else {
                        1 + WAL_CHECKSUM_SIZE + 8
                    };
                    if self.pos + needed > self.buf.len() {
                        return None;
                    }

                    if self.version >= 2 {
                        // v2: [type:1][length:2][crc32c:4][tx_id:8]
                        let length_bytes: [u8; 2] = self.buf[self.pos + 1..self.pos + 1 + 2].try_into().ok()?;
                        let record_length = u16::from_le_bytes(length_bytes);

                        let crc_start = self.pos + 1 + WAL_LENGTH_SIZE;
                        let payload_start = crc_start + WAL_CHECKSUM_SIZE;

                        let stored_crc = u32::from_le_bytes(
                            self.buf[crc_start..crc_start + WAL_CHECKSUM_SIZE].try_into().ok()?
                        );

                        // Validate the length matches expected commit size
                        let expected_payload = WAL_CHECKSUM_SIZE + 8;
                        if record_length as usize != expected_payload {
                            return Some(WALRecord::Corrupt {
                                msg: format!(
                                    "Commit record at offset {} has invalid length {} (expected {})",
                                    self.base_offset + self.pos as u64,
                                    record_length,
                                    expected_payload,
                                ),
                            });
                        }

                        let tx_id_bytes: [u8; 8] = self.buf[payload_start..payload_start + 8].try_into().ok()?;
                        let tx_id = u64::from_le_bytes(tx_id_bytes);

                        let computed_crc = WAL::compute_checksum_v2(
                            RECORD_TYPE_COMMIT,
                            record_length,
                            &tx_id_bytes,
                        );

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

                        self.pos += needed;
                        self.align_position();
                        return Some(WALRecord::Commit { tx_id });
                    } else {
                        // v1: [type:1][crc32c:4][tx_id:8]
                        let off = self.pos + 1 + WAL_CHECKSUM_SIZE;
                        let tx_id = u64::from_le_bytes(self.buf[off..off + 8].try_into().ok()?);

                        let mut crc_bytes = [0u8; WAL_CHECKSUM_SIZE];
                        crc_bytes.copy_from_slice(&self.buf[self.pos + 1..self.pos + 1 + WAL_CHECKSUM_SIZE]);
                        let stored_crc = u32::from_le_bytes(crc_bytes);

                        let mut digest = CRC32C.digest();
                        digest.update(&[RECORD_TYPE_COMMIT]);
                        digest.update(&self.buf[off..off + 8]);
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

                        let record = WALRecord::Commit { tx_id };

                        self.pos += needed;
                        self.align_position();
                        return Some(record);
                    }
                }
                0 => {
                    // Zero bytes are valid alignment padding — skip silently.
                    self.pos += 1;
                }
                _ => {
                    // Unknown record type. In v2, we can use the length prefix
                    // to skip the full record. In v1, we advance one byte.
                    if self.version >= 2 {
                        // Try to read length and skip that many bytes
                        if self.pos + 1 + WAL_LENGTH_SIZE > self.buf.len() {
                            return None;
                        }
                        let length_bytes: [u8; 2] = self.buf[self.pos + 1..self.pos + 1 + 2].try_into().ok()?;
                        let record_length = u16::from_le_bytes(length_bytes) as usize;
                        let skip = record_length.min(MAX_SKIP_PER_CORRUPT_RECORD);
                        let total = 1 + WAL_LENGTH_SIZE + skip;
                        if self.pos + total > self.buf.len() {
                            self.pos = self.buf.len();
                            return None;
                        }
                        tracing::warn!(
                            "Skipping unknown WAL record type {} at offset {} ({} byte record)",
                            record_type,
                            self.base_offset + self.pos as u64,
                            1 + WAL_LENGTH_SIZE + record_length,
                        );
                        self.pos += total;
                    } else {
                        // v1: advance one byte
                        self.pos += 1;
                    }
                }
            }
        }
        None
    }

    fn align_position(&mut self) {
        // Alignment must be computed relative to the file position, not the buffer position.
        // The buffer starts at base_offset (WAL_HEADER_SIZE) bytes into the file.
        let file_pos = self.base_offset as usize + self.pos;
        let padding = (WAL_ALIGNMENT - (file_pos % WAL_ALIGNMENT)) % WAL_ALIGNMENT;
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
    fn test_new_wal_auto_repair_invalid_magic() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
        std::fs::write(&wal_path, b"BOGUS").unwrap();
        // WAL auto-repair reinitializes on corrupt header
        let wal = WAL::new(dir.path(), SyncMode::Normal).unwrap();
        assert_eq!(wal.size().unwrap(), WAL_HEADER_SIZE as u64);
        assert_eq!(wal.wal_version, WAL_VERSION);
    }

    #[test]
    fn test_new_wal_auto_repair_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
        // Create an empty file (simulates crash after set_len(0))
        std::fs::write(&wal_path, b"").unwrap();
        let wal = WAL::new(dir.path(), SyncMode::Normal).unwrap();
        assert_eq!(wal.size().unwrap(), WAL_HEADER_SIZE as u64);
        assert_eq!(wal.wal_version, WAL_VERSION);
    }

    #[test]
    fn test_new_wal_auto_repair_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
        // File present but too small for a header (1 byte)
        std::fs::write(&wal_path, &[0xFF]).unwrap();
        let wal = WAL::new(dir.path(), SyncMode::Normal).unwrap();
        assert_eq!(wal.size().unwrap(), WAL_HEADER_SIZE as u64);
        assert_eq!(wal.wal_version, WAL_VERSION);
    }

    #[test]
    fn test_new_wal_auto_repair_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
        // File with valid size but bad magic
        let data = vec![0xFFu8; WAL_HEADER_SIZE];
        std::fs::write(&wal_path, &data).unwrap();
        let wal = WAL::new(dir.path(), SyncMode::Normal).unwrap();
        assert_eq!(wal.size().unwrap(), WAL_HEADER_SIZE as u64);
        assert_eq!(wal.wal_version, WAL_VERSION);
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
        let wal_path = dir.path().join("wal.ltng");
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[WAL_VERSION]).unwrap();
            // Write a partial page update (v2 format: type + length + partial crc)
            f.write_all(&[RECORD_TYPE_PAGE_UPDATE]).unwrap();
            let length: u16 = (WAL_CHECKSUM_SIZE + 8 + 8 + 8 + PAGE_SIZE) as u16;
            f.write_all(&length.to_le_bytes()).unwrap();
            // Omit the rest to create a torn write
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert!(report.partial_record_at_eof);
    }

    #[test]
    fn test_replay_corrupt_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
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
            // Corrupt a byte in the first record's payload (skip header + type + length + crc)
            let v2_record_header = WAL_HEADER_SIZE + 1 + WAL_LENGTH_SIZE + WAL_CHECKSUM_SIZE;
            f.seek(std::io::SeekFrom::Start(v2_record_header as u64 + 5)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert_eq!(report.corrupt_records_skipped, 1);
    }

    #[test]
    fn test_replay_unknown_record_type_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[WAL_VERSION]).unwrap();
            // Write an unknown record type with a plausible v2 length
            f.write_all(&[0xFF]).unwrap();
            let skip_length: u16 = 0;
            f.write_all(&skip_length.to_le_bytes()).unwrap();
        }
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        assert_eq!(report.records_read, 0);
        // Unknown record with length=0 is skipped
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
    fn test_v1_wal_replay_migration() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");

        // Write a v1 format WAL: header + page_update + commit
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(b"LNIW").unwrap();
            f.write_all(&[WAL_VERSION_V1]).unwrap();

            // v1 page update: [type:1][crc32c:4][tx_id:8][file_id:8][page_idx:8][data:4096]
            let mut digest = CRC32C.digest();
            digest.update(&[RECORD_TYPE_PAGE_UPDATE]);
            let tx_id = 1u64.to_le_bytes();
            let file_id = 0u64.to_le_bytes();
            let page_idx = 0u64.to_le_bytes();
            let data = vec![0x42u8; PAGE_SIZE];
            digest.update(&tx_id);
            digest.update(&file_id);
            digest.update(&page_idx);
            digest.update(&data);
            let crc = digest.finalize();

            f.write_all(&[RECORD_TYPE_PAGE_UPDATE]).unwrap();
            f.write_all(&crc.to_le_bytes()).unwrap();
            f.write_all(&tx_id).unwrap();
            f.write_all(&file_id).unwrap();
            f.write_all(&page_idx).unwrap();
            f.write_all(&data).unwrap();

            // v1 commit: [type:1][crc32c:4][tx_id:8]
            let mut digest = CRC32C.digest();
            digest.update(&[RECORD_TYPE_COMMIT]);
            let tx_id = 1u64.to_le_bytes();
            digest.update(&tx_id);
            let crc = digest.finalize();

            f.write_all(&[RECORD_TYPE_COMMIT]).unwrap();
            f.write_all(&crc.to_le_bytes()).unwrap();
            f.write_all(&tx_id).unwrap();
        }

        // Open and replay — should handle v1 format
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        assert_eq!(wal.wal_version, WAL_VERSION_V1);

        let mut applied = Vec::new();
        let report = wal.replay(
            |fid, pid, data| { applied.push((fid, pid, data.to_vec())); Ok(()) },
            0,
        ).unwrap();
        assert_eq!(report.records_read, 2);
        assert_eq!(report.corrupt_records_skipped, 0);
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0], (0, 0, vec![0x42u8; PAGE_SIZE]));

        // After truncate (checkpoint), the WAL is rewritten as v2
        wal.truncate().unwrap();

        // Reopen — should be v2 now
        drop(wal);
        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        assert_eq!(wal.wal_version, WAL_VERSION);
    }

    #[test]
    fn test_v2_replay_detects_corrupt_crc() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.ltng");

        // Write a v2 record with a CRC that doesn't match the payload
        {
            let mut f = std::fs::File::create(&wal_path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[WAL_VERSION]).unwrap();

            let length: u16 = (WAL_CHECKSUM_SIZE + 12) as u16; // small record
            f.write_all(&[RECORD_TYPE_PAGE_UPDATE]).unwrap();
            f.write_all(&length.to_le_bytes()).unwrap();
            // Deliberately wrong CRC (computed over different data)
            let wrong_crc: u32 = 0xDEADBEEF;
            f.write_all(&wrong_crc.to_le_bytes()).unwrap();
            f.write_all(&[0x42u8; 12]).unwrap();
        }

        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        let report = wal.replay(|_, _, _| Ok(()), 0).unwrap();
        // CRC mismatch should be detected
        assert_eq!(report.corrupt_records_skipped, 1);
    }

    #[test]
    fn test_version_is_persisted_across_reopen() {
        let (wal, dir) = create_wal(SyncMode::Off);
        assert_eq!(wal.wal_version, WAL_VERSION);
        drop(wal);

        let wal = WAL::new(dir.path(), SyncMode::Off).unwrap();
        assert_eq!(wal.wal_version, WAL_VERSION);
    }
}
