use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::Result;
use crc::{Algorithm, Crc};
use parking_lot::RwLock;
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
    }

    /// Check if the pending buffer has grown large enough to warrant
    /// a full rebuild. Returns `true` when pending edges exceed 10% of
    /// the base edge count (or when base has no edges but pending is non-empty).
    pub fn needs_compaction(&self) -> bool {
        let pending = self.pending_edges.read().len() as u64;
        if pending == 0 {
            return false;
        }
        let base = self.base_edge_count.load(Ordering::Relaxed);
        base == 0 || pending > base / 10
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
    fn read_u64_at(&self, bm: &BufferManager, fh: &Arc<FileHandle>, byte_pos: u64, tx: &crate::transaction::transaction_manager::Transaction) -> Result<u64> {
        let page_idx = byte_pos / PAGE_SIZE as u64;
        let offset_in_page = byte_pos as usize % PAGE_SIZE;
        if offset_in_page + 8 <= PAGE_SIZE {
            let frame = bm.pin_page(fh.clone(), page_idx, tx)?;
            return Ok(u64::from_le_bytes(
                frame.as_slice()[offset_in_page..offset_in_page + 8]
                    .try_into()
                    .expect("infallible: u64 read"),
            ));
        }
        let mut buf = [0u8; 8];
        let first_part = PAGE_SIZE - offset_in_page;
        {
            let frame = bm.pin_page(fh.clone(), page_idx, tx)?;
            buf[..first_part].copy_from_slice(&frame.as_slice()[offset_in_page..]);
        }
        {
            let frame = bm.pin_page(fh.clone(), page_idx + 1, tx)?;
            buf[first_part..8].copy_from_slice(&frame.as_slice()[..8 - first_part]);
        }
        Ok(u64::from_le_bytes(buf))
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

        // Validate offset file header
        let header_frame = bm.pin_page(self.offset_fh.clone(), 0, tx)?;
        let mut header_buf = [0u8; PAGE_SIZE];
        header_buf.copy_from_slice(header_frame.as_slice());
        validate_csr_header(&header_buf, CSR_OFFSET_MAGIC)?;

        // Validate adjacency file header if it has pages
        if self.adj_node_fh.get_num_pages() > 0 {
            let adj_header = bm.pin_page(self.adj_node_fh.clone(), 0, tx)?;
            let mut adj_buf = [0u8; PAGE_SIZE];
            adj_buf.copy_from_slice(adj_header.as_slice());
            validate_csr_header(&adj_buf, CSR_ADJ_MAGIC)?
        }

        // Read all offsets using header-aware positions with page-boundary safety
        let max_entries = ((num_offset_pages * PAGE_SIZE as u64).saturating_sub(CSR_HEADER_SIZE as u64)) / 8;
        let mut offsets = Vec::with_capacity(max_entries as usize + 1);
        for i in 0..=max_entries as usize {
            let byte_pos = csr_offset_byte(i as u64);
            let page_idx = byte_pos / PAGE_SIZE as u64;
            if page_idx >= self.offset_fh.get_num_pages() {
                break;
            }
            match self.read_u64_at(bm, &self.offset_fh, byte_pos, tx) {
                Ok(val) => offsets.push(val),
                Err(_) => break,
            }
        }
        if offsets.len() < 2 {
            return Ok(Vec::new());
        }

        // Find the actual number of active nodes by looking at the last non-zero offset
        let num_entries = offsets.len() - 1;
        let mut num_nodes = 0u64;
        for i in 0..num_entries {
            if i + 1 < offsets.len() && offsets[i] < offsets[i + 1] {
                num_nodes = i as u64 + 1;
            }
        }

        let total_adj = offsets[num_nodes as usize];
        if total_adj == 0 {
            return Ok(Vec::new());
        }

        // Read all adjacency values using header-aware positions
        let mut adj_values = Vec::with_capacity(total_adj as usize);
        for adj_idx in 0..total_adj {
            let adj_byte = (CSR_HEADER_SIZE as u64) + adj_idx * 8;
            match self.read_u64_at(bm, &self.adj_node_fh, adj_byte, tx) {
                Ok(val) => {
                    if val & DELETED_BIT == 0 {
                        adj_values.push(val);
                    }
                }
                Err(_) => break,
            }
        }

        // Pair adjacency values with their src nodes using the offset array
        let mut result = Vec::with_capacity(adj_values.len());
        let mut pos = 0usize;
        for src in 0..num_nodes as usize {
            if src + 1 >= offsets.len() {
                break;
            }
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
        let byte_pos = csr_offset_byte(node_id);
        let start_page = byte_pos / PAGE_SIZE as u64;
        let start_offset_in_page = byte_pos % PAGE_SIZE as u64;
        if start_page >= self.offset_fh.get_num_pages() {
            return Ok(());
        }

        let end_node_id = node_id + 1;
        let end_byte_pos = csr_offset_byte(end_node_id);
        let end_page = end_byte_pos / PAGE_SIZE as u64;
        let end_offset_in_page = end_byte_pos % PAGE_SIZE as u64;

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

        for (i, &val) in offsets.iter().enumerate() {
            let byte_pos = csr_offset_byte(i as u64);
            let page_idx = byte_pos / PAGE_SIZE as u64;
            let offset_in_page = byte_pos % PAGE_SIZE as u64;
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

        for (i, &(_, dst)) in sorted_edges.iter().enumerate() {
            let adj_byte = (CSR_HEADER_SIZE as u64) + (i as u64 * 8);
            let page_idx = adj_byte / PAGE_SIZE as u64;
            let offset_in_page = adj_byte % PAGE_SIZE as u64;
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
