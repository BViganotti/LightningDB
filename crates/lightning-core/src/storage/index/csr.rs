use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::Result;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Bitmask for the highest bit of a u64 adjacency value.
/// When set, the adjacency entry is a tombstone (deleted edge).
/// Node IDs are expected to be < 2^63, so this bit is safe to use.
const DELETED_BIT: u64 = 1 << 63;

pub struct CSRIndex {
    pub(crate) offset_fh: Arc<FileHandle>,
    pub(crate) adj_node_fh: Arc<FileHandle>,

    /// Pending edge insertions that have not yet been compacted into the base CSR.
    /// New edges are appended here and merged during `for_each_neighbor`.
    pending_edges: RwLock<Vec<(u64, u64)>>,

    /// Pending edge deletions tracked as (src, dst).
    /// Applied during `for_each_neighbor` by filtering out matching edges.
    pending_deletions: RwLock<Vec<(u64, u64)>>,

    /// Total number of edges in the base CSR (used for compaction ratio).
    base_edge_count: AtomicU64,
}

impl CSRIndex {
    pub fn new(offset_fh: Arc<FileHandle>, adj_node_fh: Arc<FileHandle>) -> Self {
        Self {
            offset_fh,
            adj_node_fh,
            pending_edges: RwLock::new(Vec::new()),
            pending_deletions: RwLock::new(Vec::new()),
            base_edge_count: AtomicU64::new(0),
        }
    }

    /// Insert a single edge into the pending buffer.
    /// Does not rebuild the base CSR — lightweight O(1) operation.
    pub fn insert_edge(&self, src: u64, dst: u64) {
        self.pending_edges.write().push((src, dst));
    }

    /// Insert a batch of edges into the pending buffer.
    pub fn insert_batch(&self, edges: &[(u64, u64)]) {
        self.pending_edges.write().extend_from_slice(edges);
    }

    /// Mark an edge as deleted. On next `for_each_neighbor` the deletion
    /// is applied by skipping the matching (src, dst) pair.
    pub fn delete_edge(&self, src: u64, dst: u64) {
        self.pending_deletions.write().push((src, dst));
    }

    /// Check if the pending buffer has grown large enough to warrant
    /// a full rebuild. Returns `true` when pending edges exceed 10% of
    /// the base edge count (or when base has no edges but pending is non-empty).
    pub fn needs_compaction(&self) -> bool {
        let pending = self.pending_edges.read().len() as u64;
        let base = self.base_edge_count.load(Ordering::Relaxed);
        base > 0 && pending > base / 10
    }

    /// Compact the pending buffer into the base CSR by rebuilding
    /// the full structure. After compaction, the pending buffer is cleared.
    pub fn compact(
        &self,
        bm: &BufferManager,
        num_nodes: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let pending = self.pending_edges.read().clone();
        let all_edges = self.collect_all_edges(bm, tx)?;

        Self::build(bm, self.offset_fh.clone(), self.adj_node_fh.clone(), &all_edges, num_nodes, tx)?;

        self.pending_edges.write().clear();
        self.pending_deletions.write().clear();
        self.base_edge_count.store(all_edges.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Collect all edges from the base CSR plus pending insertions,
    /// minus pending deletions. This is the full edge set.
    fn collect_all_edges(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, u64)>> {
        let base_edges = self.scan_edges_from_csr(bm, tx)?;
        let pending = self.pending_edges.read().clone();
        let deletions = self.pending_deletions.read().clone();

        let mut all_edges: Vec<(u64, u64)> = base_edges;
        all_edges.extend(pending);

        if !deletions.is_empty() {
            all_edges.retain(|e| !deletions.contains(e));
        }

        Ok(all_edges)
    }

    /// Read all edges from the base CSR by scanning the offset and adjacency files.
    /// Returns (src, dst) pairs. Skips adjacency entries with DELETED_BIT set.
    fn scan_edges_from_csr(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, u64)>> {
        let num_offset_pages = self.offset_fh.get_num_pages();
        if num_offset_pages == 0 {
            return Ok(Vec::new());
        }

        // Read all offsets
        let max_nodes = (num_offset_pages * PAGE_SIZE as u64) / 8;
        let mut offsets = vec![0u64; (max_nodes + 1) as usize];
        for i in 0..=max_nodes as usize {
            let page_idx = (i as u64 * 8) / PAGE_SIZE as u64;
            if page_idx >= self.offset_fh.get_num_pages() {
                break;
            }
            let offset_in_page = (i as u64 * 8) % PAGE_SIZE as u64;
            let frame = bm.pin_page(self.offset_fh.clone(), page_idx, tx)?;
            offsets[i] = u64::from_le_bytes(
                frame.as_slice()[offset_in_page as usize..offset_in_page as usize + 8]
                    .try_into()
                    .expect("infallible: fixed-size array conversion"),
            );
        }

        // Find the actual number of active nodes by looking at the last non-zero offset
        let mut num_nodes = 0u64;
        for i in 0..max_nodes {
            if offsets[i as usize] < offsets[(i + 1) as usize] {
                num_nodes = i + 1;
            }
        }

        let total_adj = offsets[num_nodes as usize];
        if total_adj == 0 {
            return Ok(Vec::new());
        }

        // Read all adjacency values and pair with src nodes via offsets
        let mut adj_values = Vec::with_capacity(total_adj as usize);
        let mut adj_idx = 0u64;
        while adj_idx < total_adj {
            let page_idx = (adj_idx * 8) / PAGE_SIZE as u64;
            let offset_in_page = (adj_idx * 8) % PAGE_SIZE as u64;
            if page_idx >= self.adj_node_fh.get_num_pages() {
                break;
            }
            let frame = bm.pin_page(self.adj_node_fh.clone(), page_idx, tx)?;
            let remaining = (PAGE_SIZE as u64 - offset_in_page) / 8;
            let to_read = std::cmp::min(total_adj - adj_idx, remaining) as usize;

            for j in 0..to_read {
                let off = (offset_in_page as usize) + (j * 8);
                let val = u64::from_le_bytes(
                    frame.as_slice()[off..off + 8]
                        .try_into()
                        .expect("infallible: fixed-size array conversion"),
                );
                if val & DELETED_BIT == 0 {
                    adj_values.push(val);
                }
            }
            adj_idx += to_read as u64;
        }

        // Pair adjacency values with their src nodes using the offset array
        let mut result = Vec::with_capacity(adj_values.len());
        let mut pos = 0usize;
        for src in 0..=num_nodes as usize {
            let end = offsets[src + 1] as usize;
            while pos < end && pos < adj_values.len() {
                result.push((src as u64, adj_values[pos]));
                pos += 1;
            }
            pos = end;
        }
        Ok(result)
    }

    /// Allocation-free neighbor iteration. Checks both the base CSR
    /// and the pending buffer. Edges in pending_deletions are filtered out.
    pub fn for_each_neighbor<F>(
        &self,
        bm: &BufferManager,
        node_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        self.for_each_base_neighbor(bm, node_id, tx, &mut f)?;
        self.for_each_pending_neighbor(node_id, &mut f);
        Ok(())
    }

    fn for_each_base_neighbor<F>(
        &self,
        bm: &BufferManager,
        node_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        let start_page = (node_id * 8) / PAGE_SIZE as u64;
        let start_offset_in_page = (node_id * 8) % PAGE_SIZE as u64;
        if start_page >= self.offset_fh.get_num_pages() {
            return Ok(());
        }

        let end_node_id = node_id + 1;
        let end_page = (end_node_id * 8) / PAGE_SIZE as u64;
        let end_offset_in_page = (end_node_id * 8) % PAGE_SIZE as u64;

        let (start, end) = {
            let start_frame = bm.pin_page(self.offset_fh.clone(), start_page, tx)?;
            let start = u64::from_le_bytes(
                start_frame.as_slice()[start_offset_in_page as usize..start_offset_in_page as usize + 8]
                    .try_into()
                    .expect("infallible: fixed-size array conversion"),
            );

            let end = if start_page == end_page {
                u64::from_le_bytes(
                    start_frame.as_slice()[end_offset_in_page as usize..end_offset_in_page as usize + 8]
                        .try_into()
                        .expect("infallible: fixed-size array conversion"),
                )
            } else {
                if end_page >= self.offset_fh.get_num_pages() {
                    return Ok(());
                }
                let end_frame = bm.pin_page(self.offset_fh.clone(), end_page, tx)?;
                u64::from_le_bytes(
                    end_frame.as_slice()[end_offset_in_page as usize..end_offset_in_page as usize + 8]
                        .try_into()
                        .expect("infallible: fixed-size array conversion"),
                )
            };
            (start, end)
        };

        if end <= start {
            return Ok(());
        }

        let deletions = self.pending_deletions.read();
        let has_deletions = !deletions.is_empty();

        let mut i = start;
        while i < end {
            let adj_page = (i * 8) / PAGE_SIZE as u64;
            let adj_offset_in_page = (i * 8) % PAGE_SIZE as u64;
            let adj_frame = bm.pin_page(self.adj_node_fh.clone(), adj_page, tx)?;

            let remaining_in_page = (PAGE_SIZE as u64 - adj_offset_in_page) / 8;
            let to_read = std::cmp::min(end - i, remaining_in_page) as usize;

            for j in 0..to_read {
                let offset = (adj_offset_in_page as usize) + (j * 8);
                let val = u64::from_le_bytes(
                    adj_frame.as_slice()[offset..offset + 8]
                        .try_into()
                        .expect("infallible: fixed-size array conversion"),
                );
                let neighbor = val & !DELETED_BIT;
                if val & DELETED_BIT != 0 {
                    continue;
                }
                if has_deletions && deletions.contains(&(node_id, neighbor)) {
                    continue;
                }
                f(neighbor);
            }
            i += to_read as u64;
        }

        Ok(())
    }

    fn for_each_pending_neighbor<F>(&self, node_id: u64, f: &mut F)
    where
        F: FnMut(u64),
    {
        let pending = self.pending_edges.read();
        let deletions = self.pending_deletions.read();

        for &(src, dst) in pending.iter() {
            if src == node_id && !deletions.contains(&(src, dst)) {
                f(dst);
            }
        }
    }

    /// Set the base edge count after a build or load.
    /// Called by StorageManager after initial CSR construction.
    pub fn set_base_edge_count(&self, count: u64) {
        self.base_edge_count.store(count, Ordering::Relaxed);
    }

    pub fn get_neighbors(
        &self,
        bm: &BufferManager,
        node_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<u64>> {
        let mut neighbors = Vec::new();
        self.for_each_neighbor(bm, node_id, tx, |n| neighbors.push(n))?;
        Ok(neighbors)
    }

    pub fn build(
        bm: &BufferManager,
        offset_fh: Arc<FileHandle>,
        adj_node_fh: Arc<FileHandle>,
        edges: &[(u64, u64)],
        num_nodes: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let mut sorted_edges = edges.to_vec();
        sorted_edges.sort_by_key(|e| e.0);

        let mut offsets = vec![0u64; (num_nodes + 2) as usize];
        for &(src, _) in &sorted_edges {
            if src <= num_nodes {
                offsets[(src + 1) as usize] += 1;
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }

        for (i, &val) in offsets.iter().enumerate() {
            let page_idx = (i as u64 * 8) / PAGE_SIZE as u64;
            let offset_in_page = (i as u64 * 8) % PAGE_SIZE as u64;
            while offset_fh.get_num_pages() <= page_idx {
                offset_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(offset_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                std::ptr::copy_nonoverlapping(
                    val.to_le_bytes().as_ptr(),
                    ptr.add(offset_in_page as usize),
                    8,
                );
            }
        }

        for (i, &(_, dst)) in sorted_edges.iter().enumerate() {
            let page_idx = (i as u64 * 8) / PAGE_SIZE as u64;
            let offset_in_page = (i as u64 * 8) % PAGE_SIZE as u64;
            while adj_node_fh.get_num_pages() <= page_idx {
                adj_node_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(adj_node_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                std::ptr::copy_nonoverlapping(
                    dst.to_le_bytes().as_ptr(),
                    ptr.add(offset_in_page as usize),
                    8,
                );
            }
        }

        Ok(())
    }
}
