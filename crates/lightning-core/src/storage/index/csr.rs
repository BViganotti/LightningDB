use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::Result;
use crc::{Algorithm, Crc};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

/// Bitmask for the highest bit of a u64 adjacency value.
/// When set, the adjacency entry is a tombstone (deleted edge).
/// Node IDs are expected to be < 2^63, so this bit is safe to use.
const DELETED_BIT: u64 = 1 << 63;

/// Size of the CSR format safety header in bytes.
const CSR_HEADER_SIZE: usize = 12;

/// Hard ceiling on the number of offset slots a single CSR build may allocate.
///
/// Node ids are row indices, so an id beyond this cannot correspond to a real
/// table row; such a value can only come from corrupt edge data. Refusing to
/// allocate here returns a recoverable error instead of aborting the process on
/// a multi-gigabyte allocation. Normal databases are bounded far below this by
/// the node-table capacity supplied by the storage layer
/// (`StorageManager::node_id_capacity`), so this ceiling is a last-resort net.
const MAX_CSR_NODES: u64 = 1 << 27; // 134,217,728 slots = 1 GiB offset table

/// Compute the number of offset slots required for `num_nodes + 2` entries
/// (the `+2` matches the inclusive sentinel entry written by `build`).
///
/// Rejects overflow and absurd counts *before* any allocation, so a corrupt
/// node id surfaces as a `Result::Err` rather than an OOM abort.
fn checked_offset_slots(num_nodes: u64) -> Result<usize> {
    let slots = num_nodes.checked_add(2).ok_or_else(|| {
        crate::LightningError::Internal(format!(
            "CSR build refused: node count {num_nodes} overflows the offset table"
        ))
    })?;
    if slots > MAX_CSR_NODES {
        return Err(crate::LightningError::Internal(format!(
            "CSR build refused: {num_nodes} nodes exceeds the {MAX_CSR_NODES}-node safety \
             limit (offsets would need {slots} slots); the edge data likely contains a \
             corrupt node id"
        )));
    }
    Ok(slots as usize)
}

/// Number of whole u64 values that can be read from `[start_byte, end_byte)`
/// without reading past the end of a file of `file_bytes` bytes.
///
/// Bounds corrupt offset/adjacency counts by the on-disk size so a garbage
/// count cannot trigger a huge pre-allocation.
fn bounded_value_count(start_byte: u64, end_byte: u64, file_bytes: u64) -> usize {
    let end = end_byte.min(file_bytes);
    if start_byte >= end {
        return 0;
    }
    ((end - start_byte) / 8) as usize
}

/// Magic bytes for the CSR offset file.
const CSR_OFFSET_MAGIC: [u8; 4] = *b"CSRO";
/// Magic bytes for the CSR adjacency file.
const CSR_ADJ_MAGIC: [u8; 4] = *b"CSRA";
/// Current CSR format version.
const CSR_VERSION: u8 = 0x01;

/// Write the CSR format header into a byte buffer at offset 0.
/// Header layout: 4B magic, 1B version, 3B reserved, 4B CRC32C.
fn write_csr_header(buf: &mut [u8; PAGE_SIZE], magic: [u8; 4]) {
    buf[..4].copy_from_slice(&magic);
    buf[4] = CSR_VERSION;
    // bytes 5-7: reserved (zeroed)
    // bytes 8-11: CRC32C of bytes 0-7 (simple checksum for the header itself)
    let mut digest = CRC32C.digest();
    digest.update(&buf[..8]);
    let checksum = digest.finalize();
    buf[8..12].copy_from_slice(&checksum.to_le_bytes());
}

/// Validate the CSR format header from a byte buffer.
/// Returns Ok(()) if valid, Err with description if invalid.
fn validate_csr_header(buf: &[u8; PAGE_SIZE], expected_magic: [u8; 4]) -> Result<()> {
    if buf[..4] != expected_magic {
        let got = &buf[..4];
        return Err(crate::LightningError::Internal(format!(
            "CSR file has invalid magic: expected {:?}, got {:?}",
            std::str::from_utf8(&expected_magic).unwrap_or("??"),
            std::str::from_utf8(got).unwrap_or("??"),
        )));
    }
    if buf[4] != CSR_VERSION {
        return Err(crate::LightningError::Internal(format!(
            "CSR file has unsupported version {}. Expected {}",
            buf[4], CSR_VERSION
        )));
    }
    if buf.len() < 12 {
        return Err(crate::LightningError::Internal(
            "CSR header too short: expected at least 12 bytes".into(),
        ));
    }
    let stored_crc = u32::from_le_bytes(buf[8..12].try_into().expect("infallible: checked buf.len() >= 12"));
    let mut digest = CRC32C.digest();
    digest.update(&buf[..8]);
    if digest.finalize() != stored_crc {
        return Err(crate::LightningError::Internal(
            "CSR header checksum mismatch".into(),
        ));
    }
    Ok(())
}

/// Compute the byte offset for a node_id's offset entry, accounting for the header.
fn csr_offset_byte(node_id: u64) -> u64 {
    (CSR_HEADER_SIZE as u64) + node_id * 8
}

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

    /// Cumulative count of edge deletions since the last compaction.
    /// Used to trigger compaction when deletions accumulate without new inserts.
    deletion_count: AtomicU64,
}

impl CSRIndex {
    pub fn new(offset_fh: Arc<FileHandle>, adj_node_fh: Arc<FileHandle>) -> Self {
        Self {
            offset_fh,
            adj_node_fh,
            pending_edges: RwLock::new(Vec::new()),
            pending_deletions: RwLock::new(Vec::new()),
            base_edge_count: AtomicU64::new(0),
            deletion_count: AtomicU64::new(0),
        }
    }

    /// Recover `base_edge_count` from the base CSR files after restart.
    /// Called during `ensure_schema` so that `needs_compaction` makes correct
    /// decisions and `for_each_base_neighbor` reports existing edges.
    pub fn recover_from_base(&self, bm: &BufferManager, tx: &crate::transaction::transaction_manager::Transaction) -> Result<()> {
        let edges = self.scan_edges_from_csr(bm, tx)?;
        self.base_edge_count.store(edges.len() as u64, Ordering::Release);
        Ok(())
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

    /// Check if compaction is needed and compact if so.
    /// Must be called from a context with access to BufferManager and Transaction.
    pub fn compact_if_needed(
        &self,
        bm: &crate::storage::buffer_manager::BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        if self.needs_compaction() {
            self.compact(bm, tx)?;
        }
        Ok(())
    }

    /// Mark an edge as deleted. On next `for_each_neighbor` the deletion
    /// is applied by skipping the matching (src, dst) pair.
    pub fn delete_edge(&self, src: u64, dst: u64) {
        self.pending_deletions.write().push((src, dst));
        self.deletion_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if the pending buffer has grown large enough to warrant
    /// a full rebuild. Returns `true` when pending edges exceed 10% of
    /// the base edge count (or when base has no edges but pending is non-empty),
    /// OR when accumulated deletions exceed 10% of the base edge count.
    pub fn needs_compaction(&self) -> bool {
        let pending = self.pending_edges.read().len() as u64;
        let deleted = self.deletion_count.load(Ordering::Relaxed);
        if pending == 0 && deleted == 0 {
            return false;
        }
        let base = self.base_edge_count.load(Ordering::Relaxed);
        if base == 0 {
            return pending > 0 || deleted > 0;
        }
        pending > base / 10 || deleted > base / 10
    }

    /// Compact the pending buffer into the base CSR by rebuilding
    /// the full structure. After compaction, the pending buffer is cleared.
    pub fn compact(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let all_edges = self.collect_all_edges(bm, tx)?;
        if all_edges.is_empty() {
            return Ok(());
        }

        // Compute num_nodes from actual edges so that node IDs with gaps
        // (e.g. after forget+re-store) are not silently dropped.
        let num_nodes = all_edges.iter().map(|e| e.0).max().unwrap_or(0);

        Self::build(bm, self.offset_fh.clone(), self.adj_node_fh.clone(), &all_edges, num_nodes, tx)?;

        self.pending_edges.write().clear();
        self.pending_deletions.write().clear();
        self.base_edge_count.store(all_edges.len() as u64, Ordering::Relaxed);
        self.deletion_count.store(0, Ordering::Relaxed);
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

    /// Read a u64 value at a specific byte offset within a file handle, handling
    /// page boundary crossing. Each call pins and unpins the required pages, so
    /// this is safe to call in loops without leaking buffer pool slots.
    fn read_u64_at(&self, bm: &BufferManager, fh: &Arc<FileHandle>, byte_pos: u64, tx: &crate::transaction::transaction_manager::Transaction) -> Result<u64> {
        let page_idx = byte_pos / PAGE_SIZE as u64;
        let offset_in_page = byte_pos as usize % PAGE_SIZE;
        if offset_in_page + 8 <= PAGE_SIZE {
            let frame = bm.pin_page(fh.clone(), page_idx, tx)?;
            let val = u64::from_le_bytes(
                frame.as_slice()[offset_in_page..offset_in_page + 8]
                    .try_into()
                    .expect("infallible: u64 read"),
            );
            bm.unpin_page(fh, page_idx, frame);
            return Ok(val);
        }
        let mut buf = [0u8; 8];
        let first_part = PAGE_SIZE - offset_in_page;
        let frame0 = bm.pin_page(fh.clone(), page_idx, tx)?;
        buf[..first_part].copy_from_slice(&frame0.as_slice()[offset_in_page..]);
        bm.unpin_page(fh, page_idx, frame0);
        let frame1 = bm.pin_page(fh.clone(), page_idx + 1, tx)?;
        buf[first_part..8].copy_from_slice(&frame1.as_slice()[..8 - first_part]);
        bm.unpin_page(fh, page_idx + 1, frame1);
        Ok(u64::from_le_bytes(buf))
    }

    /// Batch-read u64 values from a given byte range in a file handle.
    /// Each page is pinned once, all values on it are extracted, then unpinned.
    /// Returns all u64 values from `start_byte` to `end_byte` (exclusive).
    fn read_u64_batch(
        &self,
        bm: &BufferManager,
        fh: &Arc<FileHandle>,
        start_byte: u64,
        end_byte: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<u64>> {
        if start_byte >= end_byte {
            return Ok(Vec::new());
        }
        // Never read past the end of the file, and never pre-allocate a buffer
        // sized from an unvalidated count: a corrupt offset/adjacency value must
        // not turn into a huge allocation.
        let file_bytes = fh.get_num_pages().saturating_mul(PAGE_SIZE as u64);
        let end_byte = end_byte.min(file_bytes);
        if start_byte >= end_byte {
            return Ok(Vec::new());
        }
        let num_values = bounded_value_count(start_byte, end_byte, file_bytes);
        let mut result = Vec::new();
        result.try_reserve_exact(num_values).map_err(|e| {
            crate::LightningError::Internal(format!(
                "CSR read refused: cannot allocate {num_values} values: {e}"
            ))
        })?;
        let mut byte_pos = start_byte;
        while byte_pos < end_byte {
            let page_idx = byte_pos / PAGE_SIZE as u64;
            let offset_in_page = byte_pos as usize % PAGE_SIZE;
            let remaining_in_page = PAGE_SIZE - offset_in_page;
            let bytes_in_page = std::cmp::min(remaining_in_page as u64, end_byte - byte_pos);
            let values_in_page = (bytes_in_page / 8) as usize;
            if values_in_page == 0 {
                byte_pos += bytes_in_page;
                continue;
            }
            let frame = bm.pin_page(fh.clone(), page_idx, tx)?;
            for k in 0..values_in_page {
                let start = offset_in_page + k * 8;
                result.push(u64::from_le_bytes(
                    frame.as_slice()[start..start + 8].try_into().expect("infallible: u64 read"),
                ));
            }
            bm.unpin_page(fh, page_idx, frame);
            byte_pos += bytes_in_page as u64;
        }
        Ok(result)
    }

    fn scan_edges_from_csr(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, u64)>> {
        let num_offset_pages = self.offset_fh.get_num_pages();
        if num_offset_pages == 0 {
            return Ok(Vec::new());
        }

        let header_frame = bm.pin_page(self.offset_fh.clone(), 0, tx)?;
        let mut header_buf = [0u8; PAGE_SIZE];
        header_buf.copy_from_slice(header_frame.as_slice());
        bm.unpin_page(&self.offset_fh, 0, header_frame);
        validate_csr_header(&header_buf, CSR_OFFSET_MAGIC)?;

        if self.adj_node_fh.get_num_pages() > 0 {
            let adj_header = bm.pin_page(self.adj_node_fh.clone(), 0, tx)?;
            let mut adj_buf = [0u8; PAGE_SIZE];
            adj_buf.copy_from_slice(adj_header.as_slice());
            bm.unpin_page(&self.adj_node_fh, 0, adj_header);
            validate_csr_header(&adj_buf, CSR_ADJ_MAGIC)?
        }

        let max_entries = ((num_offset_pages * PAGE_SIZE as u64).saturating_sub(CSR_HEADER_SIZE as u64)) / 8;
        let offset_end = csr_offset_byte(max_entries + 1);
        let offsets = self.read_u64_batch(bm, &self.offset_fh, CSR_HEADER_SIZE as u64, offset_end.min(num_offset_pages * PAGE_SIZE as u64), tx)?;

        if offsets.len() < 2 {
            return Ok(Vec::new());
        }

        let num_entries = offsets.len() - 1;
        let mut num_nodes = 0u64;
        for i in 0..num_entries {
            if i + 1 < offsets.len() && offsets[i] < offsets[i + 1] {
                num_nodes = i as u64 + 1;
            }
        }

        let total_adj = *offsets.get(num_nodes as usize).unwrap_or(&0);
        if total_adj == 0 {
            return Ok(Vec::new());
        }

        let adj_start = CSR_HEADER_SIZE as u64;
        // `total_adj` comes from the on-disk offset table and may be corrupt;
        // saturate here and let `read_u64_batch` clamp to the real file size so
        // a garbage count cannot drive a huge allocation.
        let adj_end = adj_start.saturating_add(total_adj.saturating_mul(8));
        let adj_values = self.read_u64_batch(bm, &self.adj_node_fh, adj_start, adj_end, tx)?;

        let mut result = Vec::with_capacity(adj_values.len());
        let mut pos = 0usize;
        for src in 0..num_nodes as usize {
            if src + 1 >= offsets.len() {
                break;
            }
            let end = offsets[src + 1] as usize;
            while pos < end && pos < adj_values.len() {
                if adj_values[pos] & DELETED_BIT == 0 {
                    result.push((src as u64, adj_values[pos]));
                }
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
        let byte_pos = csr_offset_byte(node_id);
        if byte_pos / PAGE_SIZE as u64 >= self.offset_fh.get_num_pages() {
            return Ok(());
        }

        let end_byte_pos = csr_offset_byte(node_id + 1);

        let start = self.read_u64_at(bm, &self.offset_fh, byte_pos, tx)?;
        let end = self.read_u64_at(bm, &self.offset_fh, end_byte_pos, tx)?;

        if end <= start {
            return Ok(());
        }

        let deletions = self.pending_deletions.read();
        let has_deletions = !deletions.is_empty();

        let deletion_set: HashSet<(u64, u64)> = deletions.iter().copied().collect();

        let mut i = start;
        while i < end {
            let adj_byte = (CSR_HEADER_SIZE as u64) + i * 8;
            let adj_page = adj_byte / PAGE_SIZE as u64;
            let adj_offset_in_page = adj_byte % PAGE_SIZE as u64;
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
                if has_deletions && deletion_set.contains(&(node_id, neighbor)) {
                    continue;
                }
                f(neighbor);
            }
            bm.unpin_page(&self.adj_node_fh, adj_page, adj_frame);
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
        let deletion_set: HashSet<(u64, u64)> = deletions.iter().copied().collect();

        for &(src, dst) in pending.iter() {
            if src == node_id && !deletion_set.contains(&(src, dst)) {
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

        // Size the offset table defensively: reject corrupt/overflowing node
        // counts before allocating (see `checked_offset_slots`). This is a
        // recoverable error, not an OOM abort.
        let offset_slots = checked_offset_slots(num_nodes)?;
        let mut offsets: Vec<u64> = Vec::new();
        offsets.try_reserve_exact(offset_slots).map_err(|e| {
            crate::LightningError::Internal(format!(
                "CSR build refused: cannot allocate {offset_slots} offset slots: {e}"
            ))
        })?;
        offsets.resize(offset_slots, 0);
        for &(src, _) in &sorted_edges {
            if src <= num_nodes {
                // `src <= num_nodes` and `num_nodes + 2` did not overflow, so
                // `src + 1` is in bounds; checked() keeps this panic-free anyway.
                if let Some(idx) = src.checked_add(1) {
                    offsets[idx as usize] += 1;
                }
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }

        // Write offset file header + data
        // Ensure page 0 exists for the header
        while offset_fh.get_num_pages() == 0 {
            offset_fh.add_new_page()?;
        }
        let header_frame = bm.create_new_version(offset_fh.clone(), 0, tx)?;
        let mut header_buf = [0u8; PAGE_SIZE];
        // Preserve existing data on page 0 beyond the header
        header_buf.copy_from_slice(header_frame.as_slice());
        write_csr_header(&mut header_buf, CSR_OFFSET_MAGIC);
        unsafe {
            std::ptr::copy_nonoverlapping(
                header_buf.as_ptr(),
                header_frame.as_ptr(),
                PAGE_SIZE,
            );
        }
        bm.unpin_page(&offset_fh, 0, header_frame);

        // Batch offset writes by page: group all entries per page,
        // then create one version per page, write all values, unpin.
        let mut offset_page_entries: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for (i, &val) in offsets.iter().enumerate() {
            let byte_pos = csr_offset_byte(i as u64);
            let page_idx = byte_pos / PAGE_SIZE as u64;
            offset_page_entries.entry(page_idx).or_default().push((byte_pos, val));
        }
        for (&page_idx, entries) in &offset_page_entries {
            while offset_fh.get_num_pages() <= page_idx {
                offset_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(offset_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                for &(byte_pos, val) in entries {
                    let offset_in_page = (byte_pos % PAGE_SIZE as u64) as usize;
                    std::ptr::copy_nonoverlapping(
                        val.to_le_bytes().as_ptr(),
                        ptr.add(offset_in_page),
                        8,
                    );
                }
            }
            bm.unpin_page(&offset_fh, page_idx, frame);
        }

        // Write adjacency file header + data
        while adj_node_fh.get_num_pages() == 0 {
            adj_node_fh.add_new_page()?;
        }
        let adj_header_frame = bm.create_new_version(adj_node_fh.clone(), 0, tx)?;
        let mut adj_header_buf = [0u8; PAGE_SIZE];
        adj_header_buf.copy_from_slice(adj_header_frame.as_slice());
        write_csr_header(&mut adj_header_buf, CSR_ADJ_MAGIC);
        unsafe {
            std::ptr::copy_nonoverlapping(
                adj_header_buf.as_ptr(),
                adj_header_frame.as_ptr(),
                PAGE_SIZE,
            );
        }
        bm.unpin_page(&adj_node_fh, 0, adj_header_frame);

        // Batch adjacency writes by page — one create_new_version per page.
        let mut adj_page_entries: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for (i, &(_, dst)) in sorted_edges.iter().enumerate() {
            let adj_byte = (CSR_HEADER_SIZE as u64) + (i as u64 * 8);
            let page_idx = adj_byte / PAGE_SIZE as u64;
            adj_page_entries.entry(page_idx).or_default().push((adj_byte, dst));
        }
        for (&page_idx, entries) in &adj_page_entries {
            while adj_node_fh.get_num_pages() <= page_idx {
                adj_node_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(adj_node_fh.clone(), page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                for &(adj_byte, dst) in entries {
                    let offset_in_page = (adj_byte % PAGE_SIZE as u64) as usize;
                    std::ptr::copy_nonoverlapping(
                        dst.to_le_bytes().as_ptr(),
                        ptr.add(offset_in_page),
                        8,
                    );
                }
            }
            bm.unpin_page(&adj_node_fh, page_idx, frame);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_manager::BufferManager;
    use crate::storage::wal::WAL;
    use crate::transaction::TransactionManager;
    use crate::SyncMode;

    type CsrFixture = (
        tempfile::TempDir,
        BufferManager,
        Arc<FileHandle>,
        Arc<FileHandle>,
        Arc<crate::transaction::transaction_manager::Transaction>,
    );

    fn setup_csr() -> CsrFixture {
        let dir = tempfile::tempdir().unwrap();
        let offset_fh = Arc::new(FileHandle::open(&dir.path().join("fwd_offset.ltng")).unwrap());
        let adj_fh = Arc::new(FileHandle::open(&dir.path().join("fwd_adj.ltng")).unwrap());
        let wal = Arc::new(WAL::new(dir.path(), SyncMode::Off).unwrap());
        let tm = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        tm.set_self_weak(Arc::downgrade(&tm));
        let tx = Arc::new(tm.begin(false).unwrap());
        let bm = BufferManager::new(256, None, false, 0, 0.0);
        (dir, bm, offset_fh, adj_fh, tx)
    }

    #[test]
    fn build_rejects_overflowing_and_absurd_node_counts() {
        let (_dir, bm, offset_fh, adj_fh, tx) = setup_csr();
        let edges = vec![(0u64, 1u64)];
        // u64::MAX overflows; MAX_CSR_NODES exceeds the safety ceiling. Both
        // must return an error instead of attempting a huge allocation.
        assert!(
            CSRIndex::build(&bm, offset_fh.clone(), adj_fh.clone(), &edges, u64::MAX, &tx).is_err()
        );
        assert!(CSRIndex::build(
            &bm,
            offset_fh.clone(),
            adj_fh.clone(),
            &edges,
            MAX_CSR_NODES,
            &tx
        )
        .is_err());
    }

    #[test]
    fn build_and_scan_round_trip() {
        let (_dir, bm, offset_fh, adj_fh, tx) = setup_csr();
        let edges = vec![(0u64, 1u64), (0, 2), (3, 4)];
        CSRIndex::build(&bm, offset_fh.clone(), adj_fh.clone(), &edges, 3, &tx).unwrap();

        let idx = CSRIndex::new(offset_fh, adj_fh);
        let mut scanned = idx.scan_edges_from_csr(&bm, &tx).unwrap();
        scanned.sort_unstable();
        let mut expected = edges.clone();
        expected.sort_unstable();
        assert_eq!(scanned, expected);
    }

    #[test]
    fn checked_offset_slots_normal_and_boundary() {
        assert_eq!(checked_offset_slots(0).unwrap(), 2);
        assert_eq!(checked_offset_slots(10).unwrap(), 12);
        // Exactly at the ceiling: MAX_CSR_NODES - 2 nodes -> MAX_CSR_NODES slots.
        assert_eq!(
            checked_offset_slots(MAX_CSR_NODES - 2).unwrap(),
            MAX_CSR_NODES as usize
        );
    }

    #[test]
    fn checked_offset_slots_rejects_overflow_and_absurd_counts() {
        // u64::MAX would overflow `num_nodes + 2`.
        assert!(checked_offset_slots(u64::MAX).is_err());
        assert!(checked_offset_slots(u64::MAX - 1).is_err());
        // One node past the ceiling is refused with a recoverable error.
        assert!(checked_offset_slots(MAX_CSR_NODES - 1).is_err());
        assert!(checked_offset_slots(MAX_CSR_NODES).is_err());
    }

    #[test]
    fn bounded_value_count_clamps_to_file_size() {
        // Requested range well past a 4096-byte file: only 4096 bytes are read.
        assert_eq!(bounded_value_count(0, 1 << 40, 4096), 512);
        // Non-zero start.
        assert_eq!(bounded_value_count(16, 1 << 40, 4096), (4096 - 16) / 8);
        // Requested range fully inside the file is honoured.
        assert_eq!(bounded_value_count(0, 80, 4096), 10);
        // Degenerate ranges read nothing.
        assert_eq!(bounded_value_count(4096, 8192, 4096), 0);
        assert_eq!(bounded_value_count(100, 50, 4096), 0);
        assert_eq!(bounded_value_count(0, 0, 4096), 0);
    }
}
