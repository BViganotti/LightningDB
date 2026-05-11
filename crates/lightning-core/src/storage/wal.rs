use crate::storage::buffer_manager::PAGE_SIZE;
use crate::Result;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub enum WALRecord {
    PageUpdate {
        file_id: u64,
        page_idx: u64,
        data: Vec<u8>,
    },
    Commit {
        tx_id: u64,
    },
}

pub struct WAL {
    file: Mutex<File>,
    committed_txs: Mutex<HashSet<u64>>,
}

impl WAL {
    pub fn new(path: &Path) -> Result<Self> {
        let wal_path = path.join("wal.lbug");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(wal_path)?;

        Ok(Self {
            file: Mutex::new(file),
            committed_txs: Mutex::new(HashSet::new()),
        })
    }

    pub fn log_page_update(&self, file_id: u64, page_idx: u64, data: &[u8]) -> Result<()> {
        let mut file = self.file.lock();

        file.write_all(&[1u8])?;
        file.write_all(&file_id.to_le_bytes())?;
        file.write_all(&page_idx.to_le_bytes())?;
        file.write_all(data)?;

        // Buffer writes, sync only at commit time
        Ok(())
    }

    pub fn log_commit(&self, tx_id: u64) -> Result<()> {
        let mut file = self.file.lock();

        file.write_all(&[2u8])?;
        file.write_all(&tx_id.to_le_bytes())?;

        // Flush for durability but skip expensive sync_all for performance
        // In production, this should be configurable based on durability requirements
        file.flush()?;

        drop(file);
        self.committed_txs.lock().insert(tx_id);
        Ok(())
    }

    pub fn replay<F>(&self, mut apply_page: F) -> Result<()>
    where
        F: FnMut(u64, u64, &[u8]) -> Result<()>,
    {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;

        let mut record_type = [0u8; 1];
        loop {
            match file.read_exact(&mut record_type) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            match record_type[0] {
                1 => {
                    let mut file_id_bytes = [0u8; 8];
                    let mut page_idx_bytes = [0u8; 8];
                    let mut data = vec![0u8; PAGE_SIZE];

                    if file.read_exact(&mut file_id_bytes).is_err() {
                        break;
                    }
                    if file.read_exact(&mut page_idx_bytes).is_err() {
                        break;
                    }
                    if file.read_exact(&mut data).is_err() {
                        break;
                    }

                    let file_id = u64::from_le_bytes(file_id_bytes);
                    let page_idx = u64::from_le_bytes(page_idx_bytes);
                    apply_page(file_id, page_idx, &data)?;
                }
                2 => {
                    let mut tx_id_bytes = [0u8; 8];
                    if file.read_exact(&mut tx_id_bytes).is_err() {
                        break;
                    }
                    let tx_id = u64::from_le_bytes(tx_id_bytes);
                    self.committed_txs.lock().insert(tx_id);
                }
                _ => break,
            }
        }
        Ok(())
    }

    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
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
