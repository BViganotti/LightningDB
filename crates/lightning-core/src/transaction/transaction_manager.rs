use crate::processor::Value;
use crate::storage::buffer_manager::BufferManager;
use crate::storage::row_version::RowVersion;
use crate::storage::undo_buffer::UndoBuffer;
use crate::storage::WAL;
use crate::Result;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// Records a per-row page modification for merge-on-commit.
/// When two transactions modify different rows on the same page,
/// this enables the second committer to merge its row changes
/// into the first committer's page without conflict.
#[derive(Debug, Clone)]
pub struct PageRowMod {
    pub file_id: u64,
    pub page_idx: u64,
    pub row_id: u64,
    pub element_size: usize,
    pub row_data: [u8; 64],
    /// For overflow strings (>63 chars), the full overflow page content
    /// is captured here during write. This ensures the merge-on-commit
    /// path can reconstruct the overflow data independently of the
    /// external overflow file, preventing data loss when concurrent
    /// transactions write overflow strings to different rows on the
    /// same page.
    pub overflow_row_data: Option<Vec<u8>>,
}

pub struct Transaction {
    pub tx_id: u64,
    pub is_read_only: bool,
    pub read_ts: u64,
    pub commit_ts: AtomicU64,
    pub undo_buffer: Arc<UndoBuffer>,
    pub modified_pages: Mutex<Vec<(u64, u64)>>,
    pub modified_rows: Mutex<Vec<(Arc<RowVersion>, u64)>>,
    pub bulk_row_ranges: Mutex<Vec<(Arc<RowVersion>, u64, u64)>>,
    pub uncommitted_cache: RwLock<HashMap<(String, u64), Value>>,
    pub buffered_tables: RwLock<Vec<Arc<()>>>,
    pub modified_page_rows: Mutex<Vec<PageRowMod>>,
    pub finalized: AtomicBool,
    tx_mgr: Option<Weak<TransactionManager>>,
    bm: Option<Weak<BufferManager>>,
}

pub struct TransactionManager {
    next_tx_id: AtomicU64,
    current_ts: AtomicU64,
    committed_tx_count: AtomicU64,
    active_tx_ids: RwLock<HashSet<u64>>,
    active_read_ts: RwLock<std::collections::BTreeMap<u64, usize>>,
    wal: Arc<WAL>,
    self_weak: Mutex<Option<Weak<Self>>>,
    bm_weak: Mutex<Option<Weak<BufferManager>>>,
    page_merge_locks: Mutex<HashMap<(u64, u64), Arc<Mutex<()>>>>,
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
            self_weak: Mutex::new(None),
            bm_weak: Mutex::new(None),
            page_merge_locks: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_self_weak(&self, weak: Weak<Self>) {
        *self.self_weak.lock() = Some(weak);
    }

    pub fn set_bm_weak(&self, weak: Weak<BufferManager>) {
        *self.bm_weak.lock() = Some(weak);
    }

    pub fn begin(&self, is_read_only: bool) -> Result<Transaction> {
        self.begin_at(is_read_only, self.current_ts.load(Ordering::SeqCst))
    }

    /// Begin a read-only transaction that sees the database as it was
    /// at `snapshot_ts`. This enables time-travel queries — any read
    /// will show only data committed at or before `snapshot_ts`, as if
    /// the clock was frozen at that moment.
    pub fn begin_at(&self, is_read_only: bool, snapshot_ts: u64) -> Result<Transaction> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let read_ts = snapshot_ts;

        if !is_read_only {
            self.active_tx_ids.write().insert(tx_id);
        }
        *self.active_read_ts.write().entry(read_ts).or_insert(0) += 1;

        let tx_mgr = self.self_weak.lock().clone();
        let bm = self.bm_weak.lock().clone();

        Ok(Transaction {
            tx_id,
            is_read_only,
            read_ts,
            commit_ts: AtomicU64::new(0),
            undo_buffer: Arc::new(UndoBuffer::new()),
            modified_pages: Mutex::new(Vec::new()),
            modified_rows: Mutex::new(Vec::new()),
            bulk_row_ranges: Mutex::new(Vec::new()),
            uncommitted_cache: RwLock::new(HashMap::new()),
            buffered_tables: RwLock::new(Vec::new()),
            modified_page_rows: Mutex::new(Vec::new()),
            finalized: AtomicBool::new(false),
            tx_mgr,
            bm,
        })
    }

    pub fn commit(
        &self,
        tx: &Transaction,
        bm: &crate::storage::buffer_manager::BufferManager,
        db: &crate::Database,
    ) -> Result<()> {
        if !tx.is_read_only {
            // Flush any pending write buffers so all data reaches the columns
            db.storage_manager.read().flush_all_pending(bm, tx)?;

            // Sync all data files to disk before committing.
            // This ensures column data is durable before the WAL commit record
            // is written, maintaining the invariant that WAL entries only reference
            // data that has already been persisted to column files.
            db.storage_manager.read().sync_all_data_files()?;

            // Sync the catalog's num_rows from the storage stats so that
            // subsequent connections see the correct row count even if the
            // catalog metadata wasn't explicitly dirtied by DML operators.
            {
                // Only iterate tables that were actually modified in this txn
                let modified_versions: std::collections::HashSet<*const RowVersion> = tx
                    .modified_rows
                    .lock()
                    .iter()
                    .map(|(rv, _)| Arc::as_ptr(rv))
                    .collect();

                let storage = db.storage_manager.read();
                let mut cat = db.catalog.write();

                let has_modified = !modified_versions.is_empty();
                if has_modified {
                    for (name, table) in storage.node_tables.iter() {
                        if !modified_versions.contains(&Arc::as_ptr(&table.version_info)) {
                            continue;
                        }
                        if let Some(entry) = cat.get_node_table_mut(name) {
                            entry.num_rows = entry.num_rows.max(table.next_row_id.load(std::sync::atomic::Ordering::Acquire));
                        }
                    }
                    for (name, table) in storage.rel_tables.iter() {
                        if !modified_versions.contains(&Arc::as_ptr(&table.version_info)) {
                            continue;
                        }
                        if let Some(entry) = cat.get_rel_table_mut(name) {
                            entry.num_rows = entry.num_rows.max(table.next_row_id.load(std::sync::atomic::Ordering::Acquire));
                        }
                    }
                    db.catalog.mark_dirty();
                }
            }

            let commit_ts = self.current_ts.fetch_add(1, Ordering::SeqCst) + 1;
            tx.commit_ts.store(commit_ts, Ordering::SeqCst);

            // Phase 1: Row-level merge — re-read the latest committed page for each
            // modified page and apply this transaction's row modifications on top.
            // This ensures concurrent transactions modifying DIFFERENT rows on the
            // same page don't lose each other's changes.
            //
            // Without this merge, if Tx1 modifies row 0 and Tx2 modifies row 1 on
            // the same page, Tx2's commit would overwrite Tx1's changes because
            // Tx2's page version is based on the snapshot BEFORE Tx1 committed.
            {
                let page_mods = tx.modified_page_rows.lock();
                if !page_mods.is_empty() {
                    use std::collections::HashMap;
                    // Group modifications by (file_id, page_idx) for efficient merge
                    let mut page_groups: HashMap<(u64, u64), Vec<&PageRowMod>> = HashMap::new();
                    for mod_entry in page_mods.iter() {
                        page_groups
                            .entry((mod_entry.file_id, mod_entry.page_idx))
                            .or_default()
                            .push(mod_entry);
                    }

                    for ((file_id, page_idx), mods) in &page_groups {
                        // Re-read the latest committed page and merge our row changes
                        let storage_guard = db.storage_manager.read();
                        let fh_opt = storage_guard.get_file_handle(*file_id);
                        drop(storage_guard);

                        if let Some(fh) = fh_opt {
                            let merge_lock = self.get_page_merge_lock(*file_id, *page_idx);
                            let _merge_guard = merge_lock
                                .try_lock_for(std::time::Duration::from_secs(5))
                                .ok_or_else(|| crate::LightningError::Internal(
                                    "deadlock detected while acquiring page merge lock".into()
                                ))?;

                            // Pin the latest committed version INSIDE the lock
                            let latest_frame = bm.pin_latest_committed(
                                std::sync::Arc::clone(&fh),
                                *page_idx,
                            )?;

                            // Copy-on-write: clone frame data into local buffer
                            let mut merged_data = [0u8; 4096];
                            merged_data.copy_from_slice(latest_frame.as_slice());

                            // Apply all our row modifications to the local buffer
                            for row_mod in mods {
                                let es = row_mod.element_size;
                                let vpp = 4096 / es as u64;
                                let offset_in_page = (row_mod.row_id % vpp) as usize * es;
                                if offset_in_page + es <= 4096 {
                                    // SAFETY: Writing to local buffer with checked bounds
                                    unsafe {
                                        std::ptr::copy_nonoverlapping(
                                            row_mod.row_data.as_ptr(),
                                            merged_data.as_mut_ptr().add(offset_in_page),
                                            es,
                                        );
                                    }
                                }
                            }

                            // Write merged data back to frame under the per-page lock
                            // SAFETY: Under per-page merge lock, exclusive write access
                            unsafe {
                                *latest_frame.data.get() = merged_data;
                            }

                            // Log the merged page to WAL
                            bm.log_page_update(*file_id, *page_idx, latest_frame.as_slice())?;
                            bm.unpin_page(&fh, *page_idx, latest_frame);
                        }
                    }
                }
            }

            // Phase 2: Update timestamps for non-bulk pages
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

            // Flush all dirty committed frames to disk so subsequent scans
            // can read from files instead of relying on buffer pool alone.
            // DML operators (e.g. PhysicalCreateRel) write data to buffer
            // pool frames via batch_append_values, which never writes to
            // the data files. Without this flush, a scan that uses the
            // direct file read path (scan_primitive_direct) will see
            // empty pages and produce zero rows.
            bm.flush_all();

            self.wal.log_commit(tx.tx_id)?;
            self.active_tx_ids.write().remove(&tx.tx_id);

            let committed_count = self.committed_tx_count.fetch_add(1, Ordering::SeqCst) + 1;
            db.catalog.save_if_needed(committed_count)?;
        }
        self.remove_read_ts(tx.read_ts);
        tx.finalized.store(true, Ordering::SeqCst);
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
        tx.finalized.store(true, Ordering::SeqCst);
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

    fn get_page_merge_lock(&self, file_id: u64, page_idx: u64) -> Arc<Mutex<()>> {
        let mut locks = self.page_merge_locks.lock();
        locks
            .entry((file_id, page_idx))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.finalized.swap(true, Ordering::SeqCst) {
            return;
        }
        if self.is_read_only {
            return;
        }
        if let Some(ref weak_mgr) = self.tx_mgr {
            if let Some(mgr) = weak_mgr.upgrade() {
                mgr.active_tx_ids.write().remove(&self.tx_id);
                mgr.remove_read_ts(self.read_ts);
            } else {
                tracing::warn!(
                    "Transaction {} dropped without commit/rollback; \
                     TransactionManager already dropped, cannot clean up",
                    self.tx_id
                );
            }
        }
        if let Some(ref weak_bm) = self.bm {
            if let Some(bm) = weak_bm.upgrade() {
                if let Err(e) = bm.rollback_versions(self.tx_id) {
                    tracing::error!(
                        "Failed to rollback page versions for tx {} during Drop: {}",
                        self.tx_id,
                        e
                    );
                }
            }
        }
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
