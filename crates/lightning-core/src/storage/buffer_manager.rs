use crate::storage::file_handle::FileHandle;
use crate::{LightningError, Result};
use parking_lot::Mutex;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub const PAGE_SIZE: usize = 4096;

const UNCOMMITTED_BIT: u64 = 1 << 63;

pub struct Frame {
    pub data: UnsafeCell<[u8; PAGE_SIZE]>,
    pub version: AtomicU64,
    pub pin_count: AtomicU64,
}

impl Frame {
    pub fn new(data: [u8; PAGE_SIZE], version: u64) -> Self {
        Self {
            data: UnsafeCell::new(data),
            version: AtomicU64::new(version),
            pin_count: AtomicU64::new(0),
        }
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.data.get() as *mut u8
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: SAFETY: Frame.data is UnsafeCell; access is serialized by shard RwLock or per-page Mutex. All callers hold the appropriate lock before calling as_slice().
        unsafe { &*self.data.get() }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        // SAFETY: SAFETY: Same as as_slice() — exclusive access guaranteed by shard write lock or page-level synchronization.
        unsafe { &mut *self.data.get() }
    }
}

unsafe impl Send for Frame {}
unsafe impl Sync for Frame {}

struct BufferSlot {
    key: Option<(u64, u64)>,
    frame: Arc<Frame>,
    dirty: bool,
    referenced: bool,
}

struct BufferPool {
    page_to_slots: HashMap<(u64, u64), Vec<usize>>,
    file_handles: HashMap<u64, Arc<FileHandle>>,
    slots: Vec<BufferSlot>,
    clock_ptr: usize,
    capacity: usize,
    wal: Option<Arc<crate::storage::WAL>>,
    page_locks: HashMap<(u64, u64), Arc<Mutex<()>>>,
    shutdown: AtomicBool,
    dirty_count: AtomicU64,
    /// Free candidate queue: slot indices whose pin_count dropped to 0.
    /// evict_with_clock pops from here first before scanning via CLOCK.
    free_candidates: VecDeque<usize>,
}

pub struct BufferManager {
    shards: Vec<RwLock<BufferPool>>,
    num_shards: usize,
    pub prefetch_tracker: Arc<crate::storage::prefetch::PrefetchTracker>,
    prefetch_enabled: bool,
    prefetch_depth: usize,
    prefetch_confidence: f64,
}

impl BufferManager {
    pub fn new(
        capacity: usize,
        wal: Option<Arc<crate::storage::WAL>>,
        prefetch_enabled: bool,
        prefetch_depth: usize,
        prefetch_confidence: f64,
    ) -> Self {
        let num_shards: usize = 16;
        debug_assert!(num_shards.count_ones() == 1, "num_shards must be a power of 2");
        let shard_capacity = capacity / num_shards;
        let prefetch_tracker = Arc::new(crate::storage::prefetch::PrefetchTracker::new());
        let mut shards = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let mut slots = Vec::with_capacity(shard_capacity);
            for _ in 0..shard_capacity {
                slots.push(BufferSlot {
                    key: None,
                    frame: Arc::new(Frame::new([0u8; PAGE_SIZE], 0)),
                    dirty: false,
                    referenced: false,
                });
            }

            shards.push(RwLock::new(BufferPool {
                page_to_slots: HashMap::with_capacity(shard_capacity),
                file_handles: HashMap::new(),
                slots,
                clock_ptr: 0,
                capacity: shard_capacity,
                wal: wal.as_ref().map(|w| Arc::clone(w)),
                page_locks: HashMap::new(),
                shutdown: AtomicBool::new(false),
                dirty_count: AtomicU64::new(0),
                free_candidates: VecDeque::new(),
            }));
        }

        Self {
            shards,
            num_shards,
            prefetch_tracker,
            prefetch_enabled,
            prefetch_depth,
            prefetch_confidence,
        }
    }

    fn get_shard_idx(&self, key: (u64, u64)) -> usize {
        // Simple hash to distribute keys
        let mut h = key.0 ^ key.1;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        (h as usize) & (self.num_shards - 1)
    }

    fn get_page_lock(&self, pool: &mut BufferPool, key: (u64, u64)) -> Arc<Mutex<()>> {
        pool.page_locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn pin_page(
        &self,
        fh_arc: Arc<FileHandle>,
        page_idx: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Arc<Frame>> {
        let tx_id_marked = tx.tx_id | UNCOMMITTED_BIT;
        let read_ts = tx.read_ts;
        let key = (fh_arc.file_id, page_idx);
        let shard_idx = self.get_shard_idx(key);

        // 1. Try with read lock first (FIX #1)
        {
            let pool = self.shards[shard_idx].read();
            if let Some(slot_indices) = pool.page_to_slots.get(&key) {
                let mut best_frame: Option<Arc<Frame>> = None;
                let mut best_version: u64 = 0;
                let mut found_our_own = false;

                for &idx in slot_indices.iter().rev() {
                    let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                    if version == tx_id_marked {
                        best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                        found_our_own = true;
                        break;
                    }
                    if (version & UNCOMMITTED_BIT) == 0
                        && version <= read_ts
                        && (version > best_version || (version == 0 && best_frame.is_none()))
                    {
                        best_version = version;
                        best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                    }
                }

                if let Some(frame) = best_frame {
                    frame.pin_count.fetch_add(1, Ordering::AcqRel);
                    self.prefetch_tracker.record_access(fh_arc.file_id, page_idx);
                    return Ok(frame);
                }
            }
        }

        // 2. Fallback to write lock
        let mut pool = self.shards[shard_idx].write();

        // Double check after acquiring write lock
        if let Some(slot_indices) = pool.page_to_slots.get(&key) {
            let mut best_frame: Option<Arc<Frame>> = None;
            let mut best_version: u64 = 0;
            let mut found_our_own = false;

            for &idx in slot_indices.iter().rev() {
                let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                if version == tx_id_marked {
                    best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                    found_our_own = true;
                    break;
                }
                if (version & UNCOMMITTED_BIT) == 0
                    && version <= read_ts
                    && (version > best_version || (version == 0 && best_frame.is_none()))
                {
                    best_version = version;
                    best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                }
            }

            if let Some(frame) = best_frame {
                frame.pin_count.fetch_add(1, Ordering::AcqRel);
                self.prefetch_tracker.record_access(fh_arc.file_id, page_idx);
                return Ok(frame);
            }
        }

        // Load from disk
        pool.file_handles
            .insert(fh_arc.file_id, Arc::clone(&fh_arc));
        let slot_idx = self.evict_with_clock(&mut pool)?;

        // FIX #26: Skip disk read for brand new pages
        let mut data = [0u8; PAGE_SIZE];
        if (page_idx as usize) < fh_arc.get_num_pages() as usize {
            fh_arc.read_page(page_idx, &mut data)?;
        }

        let new_frame = Arc::new(Frame::new(data, 0));

        if let Some(old_key) = pool.slots[slot_idx].key {
            if let Some(slots) = pool.page_to_slots.get_mut(&old_key) {
                slots.retain(|&idx| idx != slot_idx);
            }
        }

        pool.slots[slot_idx].key = Some(key);
        pool.slots[slot_idx].frame = new_frame.clone();
        pool.slots[slot_idx].dirty = false;
        pool.slots[slot_idx].referenced = true;
        pool.page_to_slots.entry(key).or_default().push(slot_idx);

        new_frame.pin_count.fetch_add(1, Ordering::AcqRel);
        Ok(new_frame)
    }

    pub fn create_new_version(
        &self,
        fh_arc: Arc<FileHandle>,
        page_idx: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Arc<Frame>> {
        let tx_id = tx.tx_id;
        let tx_id_marked = tx_id | UNCOMMITTED_BIT;
        let key = (fh_arc.file_id, page_idx);
        let shard_idx = self.get_shard_idx(key);

        tx.modified_pages.lock().push(key);

        let mut pool = self.shards[shard_idx].write();
        let lock = self.get_page_lock(&mut pool, key);
        let _guard = lock.lock();

        let mut source_data: Option<[u8; PAGE_SIZE]> = None;
        let mut best_version: u64 = 0;

        if let Some(slot_indices) = pool.page_to_slots.get(&key) {
            for &idx in slot_indices {
                let version = pool.slots[idx].frame.version.load(Ordering::Acquire);

                // Row-level conflict detection: allow concurrent page versions.
                // If another transaction has an uncommitted version of this page,
                // we still create our own version. Row-level conflicts are detected
                // by RowVersion::mark_row when two transactions modify the same row.
                // On commit, per-row modifications are merged into the latest page.
                if version == tx_id_marked {
                    best_version = version;
                    // SAFETY: SAFETY: Copying PAGE_SIZE bytes from a Frame's data behind Arc. The frame is pinned (pin_count > 0) so it won't be evicted during access. The shard read lock ensures no concurrent write to this slot.
                    source_data = Some(unsafe { *pool.slots[idx].frame.data.get() });
                    break;
                }

                // Select the best snapshot-visible version as source data.
                // We ignore uncommitted versions (from other transactions)
                // and versions committed after our read_ts.
                if (version & UNCOMMITTED_BIT) == 0
                    && version <= tx.read_ts
                    && (version > best_version || (version == 0 && source_data.is_none()))
                {
                    best_version = version;
                    // SAFETY: SAFETY: Same as above — pinned frame, shard read lock held.
                    source_data = Some(unsafe { *pool.slots[idx].frame.data.get() });
                }
            }
        }

        if source_data.is_none() {
            if (page_idx as usize) < fh_arc.get_num_pages() as usize {
                let mut data = [0u8; PAGE_SIZE];
                fh_arc.read_page(page_idx, &mut data)?;
                source_data = Some(data);
            } else {
                source_data = Some([0u8; PAGE_SIZE]);
            }
        }

        // Ensure file handle is in the pool for flushing
        pool.file_handles
            .insert(fh_arc.file_id, Arc::clone(&fh_arc));

        let slot_idx = self.evict_with_clock(&mut pool)?;

        let mut data = [0u8; PAGE_SIZE];
        if let Some(src) = source_data {
            data.copy_from_slice(&src);
        }

        let new_frame = Arc::new(Frame::new(data, tx_id_marked));

        if let Some(old_key) = pool.slots[slot_idx].key {
            if let Some(slots) = pool.page_to_slots.get_mut(&old_key) {
                slots.retain(|&idx| idx != slot_idx);
            }
        }

        pool.slots[slot_idx].key = Some(key);
        pool.slots[slot_idx].frame = new_frame.clone();
        pool.slots[slot_idx].dirty = true;
        pool.dirty_count.fetch_add(1, Ordering::Release);
        pool.slots[slot_idx].referenced = true;
        pool.page_to_slots.entry(key).or_default().push(slot_idx);

        new_frame.pin_count.fetch_add(1, Ordering::AcqRel);

        // Record access for learned prefetch prediction.
        // This builds the transition matrix used to predict future accesses.
        self.prefetch_tracker.record_access(fh_arc.file_id, page_idx);

        // Speculative prefetch: predict which pages will be accessed next
        // and read them into the OS page cache to reduce disk I/O latency.
        if self.prefetch_enabled {
            let predicted = self.prefetch_tracker.predict_next(
                fh_arc.file_id,
                page_idx,
                self.prefetch_depth,
                self.prefetch_confidence,
            );
            for (pf_id, pf_pg) in predicted {
                let pf_key = (pf_id, pf_pg);
                if !pool.page_to_slots.contains_key(&pf_key) {
                    if let Some(pf_fh) = pool.file_handles.get(&pf_id) {
                        if (pf_pg as usize) < pf_fh.get_num_pages() as usize {
                            let mut pf_data = [0u8; PAGE_SIZE];
                            let _ = pf_fh.read_page(pf_pg, &mut pf_data);
                        }
                    }
                }
            }
        }

        Ok(new_frame)
    }

    /// Pin the latest committed version of a page (for commit-time merging).
    /// This returns the frame with the highest committed version, regardless of
    /// read_ts. Used by merge-on-commit to re-read the current page state and
    /// apply this transaction's row modifications on top.
    pub fn pin_latest_committed(
        &self,
        fh_arc: Arc<FileHandle>,
        page_idx: u64,
    ) -> Result<Arc<Frame>> {
        let key = (fh_arc.file_id, page_idx);
        let shard_idx = self.get_shard_idx(key);

        // Try buffer pool first (read lock)
        {
            let pool = self.shards[shard_idx].read();
            if let Some(slot_indices) = pool.page_to_slots.get(&key) {
                let mut best_version: u64 = 0;
                let mut best_frame: Option<Arc<Frame>> = None;
                for &idx in slot_indices {
                    let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                    // Find the highest committed version
                    if (version & UNCOMMITTED_BIT) == 0 && version >= best_version {
                        best_version = version;
                        best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                    }
                }
                if let Some(frame) = best_frame {
                    frame.pin_count.fetch_add(1, Ordering::AcqRel);
                    return Ok(frame);
                }
            }
        }

        // Fallback: load from disk
        let mut data = [0u8; PAGE_SIZE];
        if (page_idx as usize) < fh_arc.get_num_pages() as usize {
            fh_arc.read_page(page_idx, &mut data)?;
        }
        let frame = Arc::new(Frame::new(data, 0));
        frame.pin_count.fetch_add(1, Ordering::AcqRel);
        Ok(frame)
    }

    pub fn update_timestamps(&self, file_id: u64, page_idx: u64, tx_id: u64, commit_ts: u64) {
        let key = (file_id, page_idx);
        let tx_id_marked = tx_id | UNCOMMITTED_BIT;
        let shard_idx = self.get_shard_idx(key);

        let pool = self.shards[shard_idx].read();
        if let Some(slot_indices) = pool.page_to_slots.get(&key) {
            for &idx in slot_indices {
                let current_version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                if current_version == tx_id_marked {
                    // Use compare_exchange to avoid needing a write lock for the store
                    let _ = pool.slots[idx].frame.version.compare_exchange(
                        current_version,
                        commit_ts,
                        Ordering::Release,
                        Ordering::Acquire,
                    );
                }
            }
        }
    }

    pub fn unpin_page(&self, fh: &FileHandle, page_idx: u64, frame: Arc<Frame>) {
        frame.pin_count.fetch_sub(1, Ordering::Release);
    }

    pub fn reclaim_expired_versions(&self, min_active_ts: u64) -> Result<usize> {
        let mut total_reclaimed = 0;
        for shard in &self.shards {
            let mut pool = shard.write();
            for i in 0..pool.slots.len() {
                let pin_count = pool.slots[i].frame.pin_count.load(Ordering::Acquire);
                let version = pool.slots[i].frame.version.load(Ordering::Acquire);

                if pin_count == 0 && version != 0 && version < min_active_ts
                    && version & UNCOMMITTED_BIT == 0
                {
                    if pool.slots[i].dirty {
                        if let Some((fid, pid)) = pool.slots[i].key {
                            if let Some(fh) = pool.file_handles.get(&fid) {
                                fh.write_page(pid, pool.slots[i].frame.as_slice())?;
                            }
                        }
                    }
                    if let Some(key) = pool.slots[i].key {
                        if let Some(slots) = pool.page_to_slots.get_mut(&key) {
                            slots.retain(|&idx| idx != i);
                        }
                    }
                    pool.slots[i].key = None;
                    pool.slots[i].frame = Arc::new(Frame::new([0u8; PAGE_SIZE], 0));
                    if pool.slots[i].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[i].dirty = false;
                    pool.slots[i].referenced = false;
                    pool.free_candidates.push_back(i);
                    total_reclaimed += 1;
                }
            }
        }
        Ok(total_reclaimed)
    }

    pub fn evict_pages_for_file(&self, file_id: u64, first_page: u64, num_pages: u64) {
        let last_page = first_page + num_pages;
        for page in first_page..last_page {
            let key = (file_id, page);
            let shard_idx = self.get_shard_idx(key);
            let mut pool = self.shards[shard_idx].write();
            if let Some(slot_indices) = pool.page_to_slots.get(&key) {
                let indices: Vec<usize> = slot_indices.clone();
                for &idx in &indices {
                    if pool.slots[idx].dirty {
                        let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                        if version & UNCOMMITTED_BIT == 0 {
                            if let Some((fid, pid)) = pool.slots[idx].key {
                                if let Some(fh) = pool.file_handles.get(&fid) {
                                    if let Err(e) = fh.write_page(pid, pool.slots[idx].frame.as_slice()) {
                                        tracing::error!(
                                            "Failed to write page {} on file {} during eviction: {}",
                                            pid, fid, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    pool.slots[idx].key = None;
                    pool.slots[idx].frame = Arc::new(Frame::new([0u8; PAGE_SIZE], 0));
                    if pool.slots[idx].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[idx].dirty = false;
                    pool.slots[idx].referenced = false;
                    pool.free_candidates.push_back(idx);
                }
                pool.page_to_slots.remove(&key);
            }
        }
    }

    pub fn log_page_update(&self, file_id: u64, page_idx: u64, data: &[u8]) -> Result<()> {
        let key = (file_id, page_idx);
        let shard_idx = self.get_shard_idx(key);
        let pool = self.shards[shard_idx].read();
        if let Some(wal) = &pool.wal {
            let tx_id = if let Some(slot_indices) = pool.page_to_slots.get(&key) {
                if let Some(&idx) = slot_indices.first() {
                    let version = pool.slots[idx].frame.version.load(std::sync::atomic::Ordering::Acquire);
                    version & !UNCOMMITTED_BIT
                } else {
                    0
                }
            } else {
                0
            };
            wal.log_page_update(tx_id, file_id, page_idx, data)?;
        }
        Ok(())
    }

    pub fn log_page_update_for_tx(&self, tx_id: u64, file_id: u64, page_idx: u64, data: &[u8]) -> Result<()> {
        let shard_idx = self.get_shard_idx((file_id, page_idx));
        let pool = self.shards[shard_idx].read();
        if let Some(wal) = &pool.wal {
            wal.log_page_update(tx_id, file_id, page_idx, data)?;
        }
        Ok(())
    }

    pub fn rollback_versions(&self, tx_id: u64) -> Result<()> {
        let tx_id_marked = tx_id | UNCOMMITTED_BIT;
        for shard in &self.shards {
            let mut pool = shard.write();
            for i in 0..pool.slots.len() {
                let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                if version == tx_id_marked {
                    if let Some(key) = pool.slots[i].key.take() {
                        if let Some(slots) = pool.page_to_slots.get_mut(&key) {
                            slots.retain(|&idx| idx != i);
                        }
                    }
                    pool.slots[i].frame = Arc::new(Frame::new([0u8; PAGE_SIZE], 0));
                    if pool.slots[i].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[i].dirty = false;
                    pool.slots[i].referenced = false;
                }
            }
        }
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let synced_fids: parking_lot::Mutex<std::collections::HashSet<u64>> =
            parking_lot::Mutex::new(std::collections::HashSet::new());

        // Phase 1: Parallel flush of dirty pages across all shards
        let results: Vec<Result<()>> = self.shards.par_iter().map(|shard| {
            let mut pool = shard.write();
            for i in 0..pool.slots.len() {
                if pool.slots[i].dirty {
                    let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                    if version & UNCOMMITTED_BIT != 0 {
                        continue;
                    }
                    if let Some((fid, pid)) = pool.slots[i].key {
                        if let Some(fh) = pool.file_handles.get(&fid) {
                            fh.write_page(pid, pool.slots[i].frame.as_slice())?;
                            synced_fids.lock().insert(fid);
                            if pool.slots[i].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[i].dirty = false;
                        }
                    }
                }
            }
            Ok(())
        }).collect();

        // Check for errors from parallel phase
        for r in &results {
            if let Err(e) = r {
                return Err(crate::LightningError::Internal(format!(
                    "Checkpoint write error: {}", e
                )));
            }
        }

        // Phase 2: Sync each file handle exactly once
        let fids: Vec<u64> = synced_fids.lock().iter().copied().collect();
        let mut synced = std::collections::HashSet::new();
        for shard in &self.shards {
            let pool = shard.read();
            for fid in &fids {
                if synced.insert(*fid) {
                    if let Some(fh) = pool.file_handles.get(fid) {
                        fh.sync()?;
                    }
                }
            }
        }

        // Phase 3: Truncate WAL after data is safely on disk
        for shard in &self.shards {
            let pool = shard.read();
            if let Some(wal) = &pool.wal {
                wal.truncate()?;
            }
        }
        Ok(())
    }

    fn evict_with_clock(&self, pool: &mut BufferPool) -> Result<usize> {
        // Fast path: pop a free candidate if available
        if let Some(idx) = pool.free_candidates.pop_front() {
            if pool.slots[idx].key.is_none() && pool.slots[idx].frame.pin_count.load(Ordering::Acquire) == 0 {
                // Check if the slot was dirtied since being queued
                if pool.slots[idx].dirty {
                    if let Some((fid, pid)) = pool.slots[idx].key {
                        if let Some(fh) = pool.file_handles.get(&fid) {
                            fh.write_page(pid, pool.slots[idx].frame.as_slice())?;
                        }
                    }
                }
                return Ok(idx);
            }
        }
        let start_ptr = pool.clock_ptr;
        let mut all_uncommitted = true;
        loop {
            let idx = pool.clock_ptr;
            pool.clock_ptr = (pool.clock_ptr + 1) % pool.capacity;

            let pin_count = pool.slots[idx].frame.pin_count.load(Ordering::Acquire);
            if pin_count == 0 {
                if pool.slots[idx].referenced {
                    pool.slots[idx].referenced = false;
                    all_uncommitted = false;
                    continue;
                }
                if pool.slots[idx].dirty {
                    let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                    if version & UNCOMMITTED_BIT != 0 {
                        continue;
                    }
                    all_uncommitted = false;
                    if let Some((fid, pid)) = pool.slots[idx].key {
                        if let Some(fh) = pool.file_handles.get(&fid) {
                            fh.write_page(pid, pool.slots[idx].frame.as_slice())?;
                        }
                    }
                }
                return Ok(idx);
            }

            if pool.clock_ptr == start_ptr {
                if all_uncommitted {
                    return Err(LightningError::Internal(
                        "Buffer pool exhausted: all unpinned pages are dirty with uncommitted data".into(),
                    ));
                }
                return Err(LightningError::Internal("Buffer pool exhausted".into()));
            }
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        // Just check the first shard
        self.shards[0].read().shutdown.load(Ordering::Acquire)
    }

    pub fn dirty_page_count(&self) -> usize {
        self.shards.iter().map(|shard| {
            shard.read().dirty_count.load(Ordering::Acquire) as usize
        }).sum()
    }

    pub fn shutdown(&self) {
        if let Err(e) = self.checkpoint() {
            tracing::error!("Checkpoint failed during shutdown: {}", e);
        }
        self.flush_all();
        for shard in &self.shards {
            shard.write().shutdown.store(true, Ordering::Release);
        }
    }

    pub fn flush_all(&self) {
        for shard in &self.shards {
            let mut pool = shard.write();
            for i in 0..pool.slots.len() {
                if pool.slots[i].dirty {
                    let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                    if version & UNCOMMITTED_BIT != 0 {
                        continue;
                    }
                    if let Some((fid, pid)) = pool.slots[i].key {
                        if let Some(fh) = pool.file_handles.get(&fid) {
                            if let Err(e) = fh.write_page(pid, pool.slots[i].frame.as_slice()) {
                                tracing::error!("flush_all: write error on page {} file {}: {}", pid, fid, e);
                            } else {
                                if pool.slots[i].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[i].dirty = false;
                            }
                        }
                    }
                }
            }
        }
    }

    fn reset_referenced(&self) {
        for shard in &self.shards {
            let mut pool = shard.write();
            for slot in &mut pool.slots {
                slot.referenced = false;
            }
        }
    }

    pub fn flush_all_with_handles(&self, file_handles: &[std::sync::Arc<FileHandle>]) {
        if file_handles.is_empty() {
            return;
        }
        let mut fh_map: std::collections::HashMap<u64, std::sync::Arc<FileHandle>> =
            std::collections::HashMap::new();
        for fh in file_handles {
            fh_map.insert(fh.file_id, Arc::clone(fh));
        }

        for shard in &self.shards {
            let mut pool = shard.write();
            for i in 0..pool.slots.len() {
                if pool.slots[i].dirty {
                    let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                    if version & UNCOMMITTED_BIT != 0 {
                        continue;
                    }
                    if let Some((fid, pid)) = pool.slots[i].key {
                        if let Some(fh) = fh_map.get(&fid) {
                            if let Err(e) = fh.write_page(pid, pool.slots[i].frame.as_slice()) {
                                tracing::error!("flush_all_with_handles: write error on page {} file {}: {}", pid, fid, e);
                            } else {
                                if pool.slots[i].dirty {
                        pool.dirty_count.fetch_sub(1, Ordering::Release);
                    }
                    pool.slots[i].dirty = false;
                            }
                        }
                    }
                }
            }
        }
    }
}
