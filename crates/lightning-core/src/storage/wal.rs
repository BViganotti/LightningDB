use crate::storage::buffer_manager::PAGE_SIZE;
use crate::SyncMode;
use crate::Result;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

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
}

impl WAL {
    pub fn new(path: &Path, sync_mode: SyncMode) -> Result<Self> {
        let wal_path = path.join("wal.lbug");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
        })
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
                "WAL file has invalid magic: expected LNIW, got {:?}",
                std::str::from_utf8(&magic).unwrap_or("?")
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
            let pad = vec![0u8; padding];
            file.write_all(&pad)?;
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

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[RECORD_TYPE_PAGE_UPDATE]);
        hasher.update(&tx_id.to_le_bytes());
        hasher.update(&file_id.to_le_bytes());
        hasher.update(&page_idx.to_le_bytes());
        hasher.update(data);
        let checksum = hasher.finalize();

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

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[RECORD_TYPE_COMMIT]);
        hasher.update(&tx_id.to_le_bytes());
        let checksum = hasher.finalize();

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
        let mut updates: Vec<(u64, u64, u64, Vec<u8>)> = Vec::new();

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

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(&[RECORD_TYPE_PAGE_UPDATE]);
                    hasher.update(&tx_id_bytes);
                    hasher.update(&file_id_bytes);
                    hasher.update(&page_idx_bytes);
                    hasher.update(&data);
                    if hasher.finalize() != stored_crc {
                        corrupt_records_skipped += 1;
                        tracing::warn!("Skipping corrupt WAL page update record (checksum mismatch)");
                        continue;
                    }

                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    let file_id = u64::from_le_bytes(file_id_bytes);
                    let page_idx = u64::from_le_bytes(page_idx_bytes);
                    updates.push((tx_id, file_id, page_idx, data));
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

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(&[RECORD_TYPE_COMMIT]);
                    hasher.update(&tx_id_bytes);
                    if hasher.finalize() != stored_crc {
                        corrupt_records_skipped += 1;
                        tracing::warn!("Skipping corrupt WAL commit record (checksum mismatch)");
                        continue;
                    }

                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    commits.insert(tx_id);
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

        for (tx_id, file_id, page_idx, data) in &updates {
            if *tx_id > last_checkpoint_ts && commits.contains(tx_id) {
                apply_page(*file_id, *page_idx, data)?;
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
}

pub struct WALReplayReport {
    pub records_read: u64,
    pub corrupt_records_skipped: u64,
    pub partial_record_at_eof: bool,
}
