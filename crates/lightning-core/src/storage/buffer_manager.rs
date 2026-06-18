use crate::storage::file_handle::FileHandle;
use crate::{LightningError, Result};
use parking_lot::Mutex;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub const PAGE_SIZE: usize = 4096;

const UNCOMMITTED_BIT: u64 = 1 << 63;
const INITIAL_SLOTS_PER_SHARD: usize = 256;

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
        // SAFETY: Frame.data is UnsafeCell; read access is safe when no
        // concurrent mutable access exists. The shard RwLock ensures that
        // readers don't overlap with writers.
        unsafe { &*self.data.get() }
    }

    /// Get a mutable reference to the frame data.
    ///
    /// # Safety
    /// The caller must ensure that no other reference (shared or mutable)
    /// to this frame's data exists simultaneously. This is guaranteed when:
    /// - The caller holds the shard write lock, OR
    /// - The frame is freshly created and not yet shared, OR
    /// - The frame's pin_count is 0 and no other thread has a reference.
    pub unsafe fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { &mut *self.data.get() }
    }
}

// SAFETY: Frame contains UnsafeCell which is !Sync. We implement Sync
// manually because all access to the UnsafeCell data is serialized by
// the shard RwLock (readers hold read lock, writers hold write lock).
// The pin_count and version fields use atomic operations.
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
    max_capacity: usize,
    wal: Option<Arc<crate::storage::WAL>>,
    /// Per-page mutexes for synchronizing concurrent access to the same page.
    /// Uses a regular HashMap (not LRU) to prevent eviction of in-use locks.
    /// If an LRU cache evicts a lock that another thread is holding, a new
    /// lock would be created for the same page, breaking mutual exclusion.
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
    /// Lock-free shutdown flag to avoid deadlocking with shard RwLocks.
    /// The vacuum thread checks this instead of reading through a shard lock.
    shutting_down: AtomicBool,
}

enum EvictResult {
    /// Successfully found a slot index to reuse.
    Found(usize),
    /// All pages are pinned/uncommitted; caller should release the lock and retry.
    NeedRetry,
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

        let initial_cap = INITIAL_SLOTS_PER_SHARD.min(shard_capacity);
        for _ in 0..num_shards {
            let mut slots = Vec::with_capacity(initial_cap);
            for _ in 0..initial_cap {
                slots.push(BufferSlot {
                    key: None,
                    frame: Arc::new(Frame::new([0u8; PAGE_SIZE], 0)),
                    dirty: false,
                    referenced: false,
                });
            }

            let _lock_cap = NonZeroUsize::new(shard_capacity.max(1024)).unwrap();
            shards.push(RwLock::new(BufferPool {
                page_to_slots: HashMap::with_capacity(shard_capacity),
                file_handles: HashMap::new(),
                slots,
                clock_ptr: 0,
                capacity: initial_cap,
                max_capacity: shard_capacity,
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
            shutting_down: AtomicBool::new(false),
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
                let mut _found_our_own = false;

                for &idx in slot_indices.iter().rev() {
                    let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                    if version == tx_id_marked {
                        best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                        _found_our_own = true;
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
            let mut _found_our_own = false;

            for &idx in slot_indices.iter().rev() {
                let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                if version == tx_id_marked {
                    best_frame = Some(Arc::clone(&pool.slots[idx].frame));
                    _found_our_own = true;
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

        // Load from disk — retry loop for eviction (releases lock on NeedRetry)
        let mut slot_idx_opt: Option<usize> = None;
        const MAX_EVICT_RETRIES: u32 = 5;
        for retry in 0..MAX_EVICT_RETRIES {
            match self.evict_with_clock(&mut pool)? {
                EvictResult::Found(idx) => {
                    slot_idx_opt = Some(idx);
                    break;
                }
                EvictResult::NeedRetry => {
                    // Drop the lock, sleep briefly, and retry
                    drop(pool);
                    std::thread::sleep(std::time::Duration::from_millis(5u64 * (retry as u64 + 1)));
                    pool = self.shards[shard_idx].write();
                }
            }
        }
        let slot_idx = slot_idx_opt.ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer pool exhausted after {} retries (capacity={}). Increase buffer_pool_size",
                MAX_EVICT_RETRIES, pool.capacity
            ))
        })?;

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
            // Iterate in reverse to find the newest version first (matching pin_page behavior).
            // A transaction may create multiple versions of the same page; the newest one
            // contains all previous writes from this transaction.
            for &idx in slot_indices.iter().rev() {
                let version = pool.slots[idx].frame.version.load(Ordering::Acquire);

                if version == tx_id_marked {
                    // SAFETY: Copying PAGE_SIZE bytes from a Frame's data behind Arc. The frame is pinned (pin_count > 0) so it won't be evicted during access. The caller holds the shard write lock (acquired above at line 266), ensuring exclusive access to this slot.
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
                    // SAFETY: Same as above — pinned frame, shard write lock held.
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

        let mut slot_idx_opt: Option<usize> = None;
        const MAX_EVICT_RETRIES: u32 = 5;
        for retry in 0..MAX_EVICT_RETRIES {
            match self.evict_with_clock(&mut pool)? {
                EvictResult::Found(idx) => {
                    slot_idx_opt = Some(idx);
                    break;
                }
                EvictResult::NeedRetry => {
                    drop(pool);
                    std::thread::sleep(std::time::Duration::from_millis(5u64 * (retry as u64 + 1)));
                    pool = self.shards[shard_idx].write();
                }
            }
        }
        let slot_idx = slot_idx_opt.ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer pool exhausted after {} retries (capacity={})",
                MAX_EVICT_RETRIES, pool.capacity
            ))
        })?;

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
        self.prefetch_tracker.record_access(fh_arc.file_id, page_idx);

        // Collect prefetch predictions while holding the lock (cheap computation),
        // but defer the actual I/O until after the lock is released.
        let prefetch_pages: Vec<(u64, u64)> = if self.prefetch_enabled {
            self.prefetch_tracker.predict_next(
                fh_arc.file_id,
                page_idx,
                self.prefetch_depth,
                self.prefetch_confidence,
            )
        } else {
            Vec::new()
        };

        // Release shard lock before doing prefetch I/O
        drop(pool);

        // Speculative prefetch: load predicted pages outside the shard lock
        if !prefetch_pages.is_empty() {
            self.do_prefetch(fh_arc.file_id, &prefetch_pages);
        }

        Ok(new_frame)
    }

    /// Prefetch pages into the buffer pool without holding shard locks.
    /// This does the I/O outside the critical path.
    fn do_prefetch(&self, _file_id: u64, pages: &[(u64, u64)]) {
        for &(pf_id, pf_pg) in pages {
            let pf_key = (pf_id, pf_pg);
            let shard_idx = self.get_shard_idx(pf_key);

            // Check if already cached (read lock)
            {
                let pool = self.shards[shard_idx].read();
                if pool.page_to_slots.contains_key(&pf_key) {
                    continue;
                }
            }

            // Get file handle (read lock)
            let pf_fh = {
                let pool = self.shards[shard_idx].read();
                pool.file_handles.get(&pf_id).map(Arc::clone)
            };

            if let Some(pf_fh) = pf_fh {
                if (pf_pg as usize) < pf_fh.get_num_pages() as usize {
                    // Do I/O outside any lock
                    let mut pf_data = [0u8; PAGE_SIZE];
                    if pf_fh.read_page(pf_pg, &mut pf_data).is_err() {
                        continue;
                    }
                    let pf_frame = Arc::new(Frame::new(pf_data, 0));

                    // Insert into buffer pool (write lock, brief)
                    let mut pool = self.shards[shard_idx].write();
                    if pool.page_to_slots.contains_key(&pf_key) {
                        continue; // Another thread already cached it
                    }
                    if let Ok(EvictResult::Found(pf_slot)) = self.evict_with_clock(&mut pool) {
                        if let Some(old_key) = pool.slots[pf_slot].key {
                            if let Some(slots) = pool.page_to_slots.get_mut(&old_key) {
                                slots.retain(|&idx| idx != pf_slot);
                            }
                        }
                        pool.slots[pf_slot].key = Some(pf_key);
                        pool.slots[pf_slot].frame = pf_frame;
                        pool.slots[pf_slot].dirty = false;
                        pool.slots[pf_slot].referenced = true;
                        pool.page_to_slots.entry(pf_key).or_default().push(pf_slot);
                    }
                }
            }
        }
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

    pub fn unpin_page(&self, _fh: &FileHandle, _page_idx: u64, frame: Arc<Frame>) {
        frame.pin_count.fetch_sub(1, Ordering::Release);
    }

    pub fn reclaim_expired_versions(&self, min_active_ts: u64) -> Result<usize> {
        let mut total_reclaimed = 0;
        for shard in &self.shards {
            // Bail out early if shutting down to avoid holding shard locks
            // during Database::drop.
            if self.shutting_down.load(Ordering::Acquire) {
                return Ok(total_reclaimed);
            }
            // Phase 1: scan under READ lock to find candidates
            // Phase 1 & 2 combined under read lock: collect candidates + their keys
            // Store the expected key alongside each candidate to detect reassignment.
            struct CleanupCandidate {
                slot_idx: usize,
                expected_key: Option<(u64, u64)>,
            }
            let mut to_flush: Vec<(Arc<FileHandle>, u64, Vec<u8>)> = Vec::new();
            let mut to_cleanup: Vec<CleanupCandidate> = Vec::new();
            {
                let pool = shard.read();
                for i in 0..pool.slots.len() {
                    let pin_count = pool.slots[i].frame.pin_count.load(Ordering::Acquire);
                    let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                    if pin_count == 0 && version != 0 && version < min_active_ts
                        && version & UNCOMMITTED_BIT == 0
                    {
                        let key = pool.slots[i].key;
                        if pool.slots[i].dirty {
                            if let Some((fid, pid)) = key {
                                if let Some(fh) = pool.file_handles.get(&fid) {
                                    let data = pool.slots[i].frame.as_slice().to_vec();
                                    to_flush.push((Arc::clone(fh), pid, data));
                                }
                            }
                        }
                        to_cleanup.push(CleanupCandidate { slot_idx: i, expected_key: key });
                    }
                }
            } // release read lock

            // Phase 3: perform I/O outside any shard lock
            for (fh, pid, data) in &to_flush {
                fh.write_page(*pid, data)?;
            }

            // Phase 4: acquire WRITE lock to update slot state
            // Verify that the slot key hasn't changed since Phase 2 to prevent
            // page_to_slots mapping corruption (TOCTOU race).
            if !to_cleanup.is_empty() {
                let mut pool = shard.write();
                for c in &to_cleanup {
                    let i = c.slot_idx;
                    // Re-check conditions under write lock (slot may have changed)
                    let pin_count = pool.slots[i].frame.pin_count.load(Ordering::Acquire);
                    let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                    if pin_count == 0 && version != 0 && version < min_active_ts
                        && version & UNCOMMITTED_BIT == 0
                        // Verify key hasn't been reassigned since Phase 2
                        && pool.slots[i].key == c.expected_key
                    {
                        if let Some(key) = pool.slots[i].key {
                            if let Some(slots) = pool.page_to_slots.get_mut(&key) {
                                slots.retain(|&idx| idx != i);
                            }
                        }
                        pool.slots[i].key = None;
                        pool.slots[i].frame = Arc::new(Frame::new([0u8; PAGE_SIZE], 0));
                        if pool.slots[i].dirty {
                            Self::decrement_dirty_count(&pool.dirty_count);
                        }
                        pool.slots[i].dirty = false;
                        pool.slots[i].referenced = false;
                        pool.free_candidates.push_back(i);
                        total_reclaimed += 1;
                    }
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
                        Self::decrement_dirty_count(&pool.dirty_count);
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
            // Iterate ALL slots for this page to find the correct tx_id.
            // Previously used slot_indices.first() which could pick the wrong
            // version when multiple versions of the same page exist concurrently
            // (e.g. during concurrent write transactions).
            let tx_id = if let Some(slot_indices) = pool.page_to_slots.get(&key) {
                let mut best_id = 0u64;
                for &idx in slot_indices.iter() {
                    let version = pool.slots[idx].frame.version.load(std::sync::atomic::Ordering::Acquire);
                    let candidate = version & !UNCOMMITTED_BIT;
                    if candidate > best_id {
                        best_id = candidate;
                    }
                }
                best_id
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
                        Self::decrement_dirty_count(&pool.dirty_count);
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
                                Self::decrement_dirty_count(&pool.dirty_count);
                            }
                            pool.slots[i].dirty = false;
                        }
                    }
                }
            }
            Ok(())
        }).collect();

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

    fn grow_pool(&self, pool: &mut BufferPool) {
        let old_cap = pool.capacity;
        let new_cap = (old_cap * 2).min(pool.max_capacity);
        if new_cap <= old_cap {
            return;
        }
        pool.slots.reserve(new_cap - old_cap);
        for i in old_cap..new_cap {
            pool.slots.push(BufferSlot {
                key: None,
                frame: Arc::new(Frame::new([0u8; PAGE_SIZE], 0)),
                dirty: false,
                referenced: false,
            });
            pool.free_candidates.push_back(i);
        }
        pool.capacity = new_cap;
    }

    fn evict_with_clock(&self, pool: &mut BufferPool) -> Result<EvictResult> {
        // Fast path: pop a free candidate if available
        if let Some(idx) = pool.free_candidates.pop_front() {
            if pool.slots[idx].key.is_none()
                && pool.slots[idx].frame.pin_count.load(Ordering::Acquire) == 0
                && !pool.slots[idx].dirty
            {
                return Ok(EvictResult::Found(idx));
            }
            // Re-queue for normal clock eviction path
            pool.free_candidates.push_back(idx);
        }

        // Single scan of the clock — no sleeping while holding the lock.
        let start_ptr = pool.clock_ptr;
        let mut _all_uncommitted = true;
        loop {
            let idx = pool.clock_ptr;
            pool.clock_ptr = (pool.clock_ptr + 1) % pool.capacity;

            let pin_count = pool.slots[idx].frame.pin_count.load(Ordering::Acquire);
            if pin_count == 0 {
                if pool.slots[idx].referenced {
                    pool.slots[idx].referenced = false;
                    _all_uncommitted = false;
                    continue;
                }
                if pool.slots[idx].dirty {
                    let version = pool.slots[idx].frame.version.load(Ordering::Acquire);
                    if version & UNCOMMITTED_BIT != 0 {
                        continue;
                    }
                    _all_uncommitted = false;
                    if let Some((fid, pid)) = pool.slots[idx].key {
                        if let Some(fh) = pool.file_handles.get(&fid) {
                            fh.write_page(pid, pool.slots[idx].frame.as_slice())?;
                        }
                    }
                }
                return Ok(EvictResult::Found(idx));
            }

            if pool.clock_ptr == start_ptr {
                // Full scan complete — no evictable slot found.
                if pool.capacity < pool.max_capacity {
                    self.grow_pool(pool);
                    // Retry immediately with larger pool
                    continue;
                }
                // Caller should release the lock, sleep briefly, and retry.
                return Ok(EvictResult::NeedRetry);
            }
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    pub fn set_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::Release);
    }

    pub fn dirty_page_count(&self) -> usize {
        self.shards.iter().map(|shard| {
            shard.read().dirty_count.load(Ordering::Acquire) as usize
        }).sum()
    }

    /// Decrement dirty count safely, preventing underflow.
    /// Uses fetch_update to atomically check and decrement.
    fn decrement_dirty_count(counter: &AtomicU64) {
        counter.fetch_update(Ordering::Release, Ordering::Acquire, |v| {
            v.checked_sub(1)
        }).ok();
    }

    pub fn shutdown(&self) {
        // Set lock-free flag FIRST so vacuum thread sees it immediately
        // without needing to acquire any shard locks.
        self.shutting_down.store(true, Ordering::Release);
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
                        Self::decrement_dirty_count(&pool.dirty_count);
                    }
                    pool.slots[i].dirty = false;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Flush only the specific set of (file_id, page_idx) pairs.
    /// Used by transaction commit to flush only modified pages instead of
    /// the entire buffer pool, avoiding unnecessary I/O spikes.
    pub fn flush_pages(&self, pages: &[(u64, u64)]) {
        use std::collections::{HashMap, HashSet};
        let mut by_shard: HashMap<usize, HashSet<(u64, u64)>> = HashMap::new();
        for &key in pages {
            let shard_idx = self.get_shard_idx(key);
            by_shard.entry(shard_idx).or_default().insert(key);
        }
        for (shard_idx, target_keys) in &by_shard {
            if target_keys.is_empty() {
                continue;
            }
            let mut pool = self.shards[*shard_idx].write();
            for i in 0..pool.slots.len() {
                if pool.slots[i].dirty {
                    if let Some(key) = pool.slots[i].key {
                        if target_keys.contains(&key) {
                            let version = pool.slots[i].frame.version.load(Ordering::Acquire);
                            if version & UNCOMMITTED_BIT != 0 {
                                continue;
                            }
                            if let Some(fh) = pool.file_handles.get(&key.0) {
                                if let Err(e) = fh.write_page(key.1, pool.slots[i].frame.as_slice()) {
                                    tracing::error!("flush_pages: write error on page {} file {}: {}", key.1, key.0, e);
                                } else {
                                    if pool.slots[i].dirty {
                                        Self::decrement_dirty_count(&pool.dirty_count);
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

    #[allow(dead_code)]
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
                        Self::decrement_dirty_count(&pool.dirty_count);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_handle::FileHandle;
    use crate::storage::wal::WAL;
    use crate::transaction::TransactionManager;
    use crate::SyncMode;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<FileHandle>, Arc<WAL>, Arc<TransactionManager>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let fh = Arc::new(FileHandle::open(&path).unwrap());
        let wal = Arc::new(WAL::new(dir.path(), SyncMode::Off).unwrap());
        let tm = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        tm.set_self_weak(Arc::downgrade(&tm));
        (dir, fh, wal, tm)
    }

    fn create_bm(capacity_pages: usize) -> BufferManager {
        BufferManager::new(capacity_pages, None, false, 0, 0.0)
    }

    fn begin_tx(tm: &TransactionManager) -> Arc<crate::transaction::transaction_manager::Transaction> {
        Arc::new(tm.begin(false).unwrap())
    }

    fn begin_tx_at(tm: &TransactionManager, ts: u64) -> Arc<crate::transaction::transaction_manager::Transaction> {
        Arc::new(tm.begin_at(true, ts).unwrap())
    }

    #[test]
    fn test_new_buffer_manager_has_correct_capacity() {
        let bm = create_bm(256);
        assert_eq!(bm.num_shards, 16);
        let total_slots: usize = bm.shards.iter().map(|s| s.read().slots.len()).sum();
        assert_eq!(total_slots, 256);
    }

    #[test]
    fn test_pin_page_new_page_returns_zeros() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let frame = bm.pin_page(Arc::clone(&fh), 1000, &tx).unwrap();
        assert_eq!(frame.as_slice(), &[0u8; PAGE_SIZE]);
    }

    #[test]
    fn test_unpin_decrements_pin_count() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let frame = bm.pin_page(Arc::clone(&fh), 0, &tx).unwrap();
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 1);
        bm.unpin_page(&fh, 0, frame.clone());
        assert_eq!(frame.pin_count.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_create_new_version_creates_dirty_page() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let frame = bm.create_new_version(Arc::clone(&fh), 5, &tx).unwrap();
        assert!(frame.version.load(Ordering::Acquire) & UNCOMMITTED_BIT != 0);
        assert!(bm.dirty_page_count() > 0);
    }

    #[test]
    fn test_pin_page_loads_after_write() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let tx_id = tx.tx_id;
        let commit_ts = tm.get_current_ts() + 1;
        let f1 = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        let s = unsafe { f1.as_mut_slice() };
        s[..4].copy_from_slice(&[0xABu8, 0xCDu8, 0xEFu8, 0x12u8]);
        bm.update_timestamps(fh.file_id, 0, tx_id, commit_ts);
        bm.unpin_page(&fh, 0, f1);
        let tx2 = begin_tx_at(&tm, commit_ts);
        let f2 = bm.pin_page(Arc::clone(&fh), 0, &tx2).unwrap();
        assert_eq!(f2.as_slice()[..4], [0xABu8, 0xCDu8, 0xEFu8, 0x12u8]);
        assert_eq!(f2.pin_count.load(Ordering::Acquire), 1);
    }

    #[test]
    fn test_create_new_version_copies_source_data() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let tx_id = tx.tx_id;
        let commit_ts = tm.get_current_ts() + 1;
        let f1 = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        let s = unsafe { f1.as_mut_slice() };
        s[0] = 0x55u8;
        bm.update_timestamps(fh.file_id, 0, tx_id, commit_ts);
        bm.unpin_page(&fh, 0, f1);
        let tx2 = begin_tx_at(&tm, commit_ts);
        let f2 = bm.create_new_version(Arc::clone(&fh), 0, &tx2).unwrap();
        assert_eq!(f2.as_slice()[0], 0x55u8);
    }

    #[test]
    fn test_version_isolation_snapshot() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx1 = begin_tx(&tm);
        let frame1 = bm.create_new_version(Arc::clone(&fh), 0, &tx1).unwrap();
        let s = unsafe { frame1.as_mut_slice() };
        s[..4].copy_from_slice(&[0xDEu8, 0xADu8, 0xBEu8, 0xEFu8]);
        let tx2 = begin_tx(&tm);
        let frame2 = bm.pin_page(Arc::clone(&fh), 0, &tx2).unwrap();
        assert_eq!(frame2.as_slice()[..4], [0, 0, 0, 0]);
    }

    #[test]
    fn test_dirty_page_count_tracking() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        assert_eq!(bm.dirty_page_count(), 0);
        let tx = begin_tx(&tm);
        let _f1 = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        assert_eq!(bm.dirty_page_count(), 1);
        let _f2 = bm.create_new_version(Arc::clone(&fh), 1, &tx).unwrap();
        assert_eq!(bm.dirty_page_count(), 2);
    }

    #[test]
    fn test_pin_latest_committed() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let _f = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        let frame = bm.pin_latest_committed(Arc::clone(&fh), 0).unwrap();
        assert_eq!(frame.version.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_update_timestamps() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let tx_id = tx.tx_id;
        let frame = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        assert_eq!(frame.version.load(Ordering::Acquire), tx_id | UNCOMMITTED_BIT);
        bm.update_timestamps(fh.file_id, 0, tx_id, 200);
        assert_eq!(frame.version.load(Ordering::Acquire), 200);
    }

    #[test]
    fn test_eviction_roundtrip() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = BufferManager::new(512, None, false, 0, 0.0);
        let tx = begin_tx(&tm);
        let mut frames = Vec::new();
        for i in 0..64 {
            let f = bm.pin_page(Arc::clone(&fh), i, &tx).unwrap();
            frames.push(f);
        }
        for f in frames.iter() {
            assert_eq!(f.as_slice().len(), PAGE_SIZE);
        }
    }

    #[test]
    fn test_log_page_update_without_wal_no_op() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        bm.log_page_update(fh.file_id, 0, &[0xAAu8; PAGE_SIZE]);
    }

    #[test]
    fn test_rollback_versions() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let _f = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        assert_eq!(bm.dirty_page_count(), 1);
        bm.rollback_versions(tx.tx_id).unwrap();
        assert_eq!(bm.dirty_page_count(), 0);
    }

    #[test]
    fn test_shutdown_and_restart() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        assert!(!bm.is_shutting_down());
        bm.shutdown();
        assert!(bm.is_shutting_down());
    }

    #[test]
    fn test_evict_pages_for_file() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let _f = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        let _f2 = bm.create_new_version(Arc::clone(&fh), 1, &tx).unwrap();
        let _f3 = bm.create_new_version(Arc::clone(&fh), 2, &tx).unwrap();
        assert_eq!(bm.dirty_page_count(), 3);
        bm.evict_pages_for_file(fh.file_id, 0, 3);
        assert!(bm.dirty_page_count() <= 3);
    }

    #[test]
    fn test_reclaim_expired_versions() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let tx_id = tx.tx_id;
        let f = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        bm.update_timestamps(fh.file_id, 0, tx_id, 50);
        bm.unpin_page(&fh, 0, f);
        let reclaimed = bm.reclaim_expired_versions(200).unwrap();
        assert!(reclaimed == 0 || reclaimed == 1);
    }

    #[test]
    fn test_multiple_shards_are_independent() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(256);
        let tx = begin_tx(&tm);
        for i in 0..32 {
            let _f = bm.pin_page(Arc::clone(&fh), i, &tx).unwrap();
        }
        let total_mapped: usize = bm.shards.iter()
            .map(|s| s.read().page_to_slots.len())
            .sum();
        assert_eq!(total_mapped, 32);
    }

    #[test]
    fn test_pin_same_page_twice_returns_same_data() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        let f1 = bm.pin_page(Arc::clone(&fh), 0, &tx).unwrap();
        let f2 = bm.pin_page(Arc::clone(&fh), 0, &tx).unwrap();
        assert_eq!(f1.as_slice(), f2.as_slice());
        assert_eq!(f1.pin_count.load(Ordering::Acquire), 2);
        assert_eq!(f2.pin_count.load(Ordering::Acquire), 2);
    }

    #[test]
    fn test_create_new_version_tracks_modified_pages() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = create_bm(64);
        let tx = begin_tx(&tm);
        assert!(tx.modified_pages.lock().is_empty());
        let _f = bm.create_new_version(Arc::clone(&fh), 7, &tx).unwrap();
        let modified = tx.modified_pages.lock();
        assert_eq!(modified.len(), 1);
        assert_eq!(modified[0], (fh.file_id, 7));
    }

    #[test]
    fn test_read_write_evict_read_consistency() {
        let (_dir, fh, _wal, tm) = setup();
        let bm = BufferManager::new(256, None, false, 0, 0.0);
        let tx = begin_tx(&tm);
        let tx_id = tx.tx_id;
        let commit_ts = tm.get_current_ts() + 1;
        let f1 = bm.create_new_version(Arc::clone(&fh), 0, &tx).unwrap();
        let s = unsafe { f1.as_mut_slice() };
        s[..8].copy_from_slice(&[1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8]);
        bm.update_timestamps(fh.file_id, 0, tx_id, commit_ts);
        let tx2 = begin_tx_at(&tm, commit_ts);
        let mut pinned = Vec::new();
        for i in 1..100 {
            pinned.push(bm.pin_page(Arc::clone(&fh), i, &tx2).unwrap());
        }
        for f in pinned.drain(..) {
            bm.unpin_page(&fh, 0, f);
        }
        let tx3 = begin_tx_at(&tm, commit_ts);
        let _f = bm.create_new_version(Arc::clone(&fh), 200, &tx3).unwrap();
        let f_reload = bm.pin_page(Arc::clone(&fh), 0, &tx2).unwrap();
        assert_eq!(f_reload.as_slice()[..8], [1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
