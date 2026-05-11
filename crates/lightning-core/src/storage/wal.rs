use crate::storage::buffer_manager::PAGE_SIZE;
use crate::SyncMode;
use crate::Result;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct WAL {
    file: Mutex<File>,
    committed_txs: Mutex<HashSet<u64>>,
    sync_mode: SyncMode,
}

impl WAL {
    pub fn new(path: &Path, sync_mode: SyncMode) -> Result<Self> {
        let wal_path = path.join("wal.lbug");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(wal_path)?;

        Ok(Self {
            file: Mutex::new(file),
            committed_txs: Mutex::new(HashSet::new()),
            sync_mode,
        })
    }

    pub fn log_page_update(
        &self,
        tx_id: u64,
        file_id: u64,
        page_idx: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut file = self.file.lock();

        file.write_all(&[1u8])?;
        file.write_all(&tx_id.to_le_bytes())?;
        file.write_all(&file_id.to_le_bytes())?;
        file.write_all(&page_idx.to_le_bytes())?;
        file.write_all(data)?;

        Ok(())
    }

    pub fn log_commit(&self, tx_id: u64) -> Result<()> {
        let mut file = self.file.lock();

        file.write_all(&[2u8])?;
        file.write_all(&tx_id.to_le_bytes())?;

        file.flush()?;
        if self.sync_mode == SyncMode::Normal {
            file.sync_all()?;
        }

        drop(file);
        self.committed_txs.lock().insert(tx_id);
        Ok(())
    }

    /// Recover by reading the WAL from beginning.
    /// Only applies page updates whose transaction committed.
    /// Skips entries before last_checkpoint_ts (they were already checkpointed).
    pub fn replay<F>(
        &self,
        mut apply_page: F,
        last_checkpoint_ts: u64,
    ) -> Result<()>
    where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;

        let mut commits: HashSet<u64> = HashSet::new();
        let mut updates: Vec<(u64, u64, u64, Vec<u8>)> = Vec::new();

        let mut record_type = [0u8; 1];
        loop {
            match file.read_exact(&mut record_type) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            match record_type[0] {
                1 => {
                    let mut tx_id_bytes = [0u8; 8];
                    let mut file_id_bytes = [0u8; 8];
                    let mut page_idx_bytes = [0u8; 8];
                    let mut data = vec![0u8; PAGE_SIZE];

                    if file.read_exact(&mut tx_id_bytes).is_err() { break; }
                    if file.read_exact(&mut file_id_bytes).is_err() { break; }
                    if file.read_exact(&mut page_idx_bytes).is_err() { break; }
                    if file.read_exact(&mut data).is_err() { break; }

                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    let file_id = u64::from_le_bytes(file_id_bytes);
                    let page_idx = u64::from_le_bytes(page_idx_bytes);
                    updates.push((tx_id, file_id, page_idx, data));
                }
                2 => {
                    let mut tx_id_bytes = [0u8; 8];
                    if file.read_exact(&mut tx_id_bytes).is_err() { break; }
                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    commits.insert(tx_id);
                }
                _ => break,
            }
        }

        // Apply page updates for committed transactions that are after the last checkpoint
        for (tx_id, file_id, page_idx, data) in &updates {
            if *tx_id > last_checkpoint_ts && commits.contains(tx_id) {
                apply_page(*file_id, *page_idx, data)?;
            }
        }

        // Record all committed transactions from WAL
        for tx_id in &commits {
            if *tx_id > last_checkpoint_ts {
                self.committed_txs.lock().insert(*tx_id);
            }
        }

        Ok(())
    }

    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
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
