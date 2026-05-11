use crate::processor::Value;
use crate::storage::undo_buffer::UndoBuffer;
use crate::storage::WAL;
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::storage::row_version::RowVersion;

pub struct Transaction {
    pub tx_id: u64,
    pub is_read_only: bool,
    pub read_ts: u64,
    pub commit_ts: AtomicU64,
    pub undo_buffer: Arc<UndoBuffer>,
    pub modified_pages: parking_lot::Mutex<Vec<(u64, u64)>>,
    pub modified_rows: parking_lot::Mutex<Vec<(Arc<RowVersion>, u64)>>,
    pub bulk_row_ranges: parking_lot::Mutex<Vec<(Arc<RowVersion>, u64, u64)>>,
    pub uncommitted_cache: RwLock<HashMap<(String, u64), Value>>,
    pub buffered_tables: RwLock<Vec<std::sync::Arc<()>>>,
}

pub struct TransactionManager {
    next_tx_id: AtomicU64,
    current_ts: AtomicU64,
    committed_tx_count: AtomicU64,
    active_tx_ids: RwLock<HashSet<u64>>,
    active_read_ts: RwLock<std::collections::BTreeMap<u64, usize>>,
    wal: Arc<WAL>,
}

impl TransactionManager {
    pub fn new(wal: Arc<WAL>) -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            current_ts: AtomicU64::new(1),
            committed_tx_count: AtomicU64::new(0),
            active_tx_ids: RwLock::new(HashSet::new()),
            active_read_ts: RwLock::new(std::collections::BTreeMap::new()),
            wal,
        }
    }

    pub fn begin(&self, is_read_only: bool) -> Result<Transaction> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_ts = self.current_ts.load(Ordering::SeqCst);

        if !is_read_only {
            self.active_tx_ids.write().insert(tx_id);
        }
        *self.active_read_ts.write().entry(read_ts).or_insert(0) += 1;

        Ok(Transaction {
            tx_id,
            is_read_only,
            read_ts,
            commit_ts: AtomicU64::new(0),
            undo_buffer: Arc::new(UndoBuffer::new()),
            modified_pages: parking_lot::Mutex::new(Vec::new()),
            modified_rows: parking_lot::Mutex::new(Vec::new()),
            bulk_row_ranges: parking_lot::Mutex::new(Vec::new()),
            uncommitted_cache: RwLock::new(HashMap::new()),
            buffered_tables: RwLock::new(Vec::new()),
        })
    }

    pub fn commit(
        &self,
        tx: &Transaction,
        bm: &crate::storage::buffer_manager::BufferManager,
        db: &crate::Database,
    ) -> Result<()> {
        if !tx.is_read_only {
            let commit_ts = self.current_ts.fetch_add(1, Ordering::SeqCst) + 1;
            tx.commit_ts.store(commit_ts, Ordering::SeqCst);

            // Only update timestamps for non-bulk pages (bulk pages handled by bulk_row_ranges)
            let modified = tx.modified_pages.lock();
            if !modified.is_empty() {
                for (file_id, page_idx) in modified.iter() {
                    bm.update_timestamps(*file_id, *page_idx, tx.tx_id, commit_ts);
                }
            }

            let bulk_ranges = tx.bulk_row_ranges.lock();
            for (version_info, start, end) in bulk_ranges.iter() {
                version_info.commit_row_batch(*start..*end, commit_ts);
            }

            let modified_rows = tx.modified_rows.lock();
            for (version_info, row_id) in modified_rows.iter() {
                version_info.commit_row(*row_id, commit_ts);
            }

            self.wal.log_commit(tx.tx_id)?;
            self.active_tx_ids.write().remove(&tx.tx_id);

            let committed_count = self.committed_tx_count.fetch_add(1, Ordering::SeqCst) + 1;
            db.catalog.save_if_needed(committed_count)?;
        }
        self.remove_read_ts(tx.read_ts);
        Ok(())
    }

    pub fn rollback(&self, db: &crate::Database, tx: &Transaction) -> Result<()> {
        if !tx.is_read_only {
            tx.undo_buffer.rollback(db, tx.tx_id)?;
            let modified_rows = tx.modified_rows.lock();
            for (version_info, row_id) in modified_rows.iter() {
                version_info.rollback_row(*row_id);
            }
            self.active_tx_ids.write().remove(&tx.tx_id);
        }
        self.remove_read_ts(tx.read_ts);
        Ok(())
    }

    fn remove_read_ts(&self, read_ts: u64) {
        let mut active = self.active_read_ts.write();
        if let Some(count) = active.get_mut(&read_ts) {
            *count -= 1;
            if *count == 0 {
                active.remove(&read_ts);
            }
        }
    }

    pub fn get_min_active_read_ts(&self) -> u64 {
        let active = self.active_read_ts.read();
        active
            .keys()
            .min()
            .cloned()
            .unwrap_or(self.current_ts.load(Ordering::Acquire))
    }

    pub fn get_active_tx_ids(&self) -> HashSet<u64> {
        self.active_tx_ids.read().clone()
    }

    pub fn get_current_ts(&self) -> u64 {
        self.current_ts.load(Ordering::Acquire)
    }
}

impl Transaction {
    pub fn get_from_cache(&self, table_name: &str, row_id: u64) -> Option<Value> {
        self.uncommitted_cache
            .read()
            .get(&(table_name.to_string(), row_id))
            .cloned()
    }

    pub fn add_to_cache(&self, table_name: &str, row_id: u64, value: Value) {
        self.uncommitted_cache
            .write()
            .insert((table_name.to_string(), row_id), value);
    }

    pub fn cache_contains(&self, table_name: &str, row_id: u64) -> bool {
        self.uncommitted_cache
            .read()
            .contains_key(&(table_name.to_string(), row_id))
    }
}
