use crate::storage::buffer_manager::BufferManager;
use crate::storage::file_handle::FileHandle;
use crate::Result;
use rayon::prelude::*;
use std::collections::BinaryHeap;
use std::sync::Arc;

const VI_HEADER_PAGE: u64 = 0;
const VI_DATA_START_PAGE: u64 = 1;

fn vi_entry_bytes(dim: usize) -> usize {
    4 + dim * 4
}

fn vi_entries_per_page(dim: usize) -> usize {
    let bps = 4096usize;
    let entry_bytes = vi_entry_bytes(dim);
    bps / entry_bytes
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredNode {
    id: u64,
    score: f32,
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.score.partial_cmp(&self.score)
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

pub struct VectorIndex {
    pub(crate) file_handle: Arc<FileHandle>,
    dimension: usize,
    #[allow(dead_code)]
    page_header_size: usize,
    node_index: parking_lot::Mutex<std::collections::HashMap<u64, usize>>,
}

impl VectorIndex {
    pub fn new(file_handle: Arc<FileHandle>, dimension: usize) -> Self {
        Self {
            file_handle,
            dimension,
            page_header_size: 0,
            node_index: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    // --- SIMD-accelerated dot product ---
    // Uses runtime detection (is_x86_feature_detected!, NEON on aarch64)
    fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(target_arch = "aarch64")]
        if a.len() >= 4 && std::arch::is_aarch64_feature_detected!("neon") {
            // SAFETY: NEON SIMD dot product reads from valid f32 slices; bounds-checked by the caller's `a.len() >= 4` guard.
            return unsafe { Self::neon_dot(a, b) };
        }
        #[cfg(target_arch = "x86_64")]
        if a.len() >= 8 && std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 dot product with bounds guard `a.len() >= 8`.
            return unsafe { Self::avx2_dot(a, b) };
        }
        #[cfg(target_arch = "x86_64")]
        if a.len() >= 4 && std::arch::is_x86_feature_detected!("sse") {
            // SAFETY: SSE dot product with bounds guard `a.len() >= 4`.
            return unsafe { Self::sse_dot(a, b) };
        }
        // Fallback: f32 dot product (matches SIMD precision)
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    /// ARM64 NEON SIMD dot product: processes 4 f32 values per iteration
    /// using FMA (fused multiply-add) for maximum throughput.
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    unsafe fn neon_dot(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::aarch64::*;
        let n = a.len();
        let mut sum = vdupq_n_f32(0.0);
        let mut i = 0;
        while i + 4 <= n {
            let va = vld1q_f32(a.as_ptr().add(i));
            let vb = vld1q_f32(b.as_ptr().add(i));
            sum = vfmaq_f32(sum, va, vb);
            i += 4;
        }
        // Horizontal add: extract lanes and sum
        let arr: [f32; 4] = std::mem::transmute(sum);
        let mut result = arr[0] + arr[1] + arr[2] + arr[3];
        while i < n {
            result += a[i] * b[i];
            i += 1;
        }
        result
    }

    #[cfg(target_feature = "avx2")]
    unsafe fn avx2_dot(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut sum = _mm256_setzero_ps();
        let mut i = 0;
        while i + 8 <= n {
            let va = _mm256_loadu_ps(a.as_ptr().add(i));
            let vb = _mm256_loadu_ps(b.as_ptr().add(i));
            sum = _mm256_fmadd_ps(va, vb, sum);
            i += 8;
        }
        let mut result: [f32; 8] = [0.0; 8];
        _mm256_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        while i < n {
            total += a[i] * b[i];
            i += 1;
        }
        total
    }

    #[cfg(target_feature = "sse")]
    unsafe fn sse_dot(a: &[f32], b: &[f32]) -> f32 {
        use std::arch::x86_64::*;
        let n = a.len();
        let mut sum = _mm_setzero_ps();
        let mut i = 0;
        while i + 4 <= n {
            let va = _mm_loadu_ps(a.as_ptr().add(i));
            let vb = _mm_loadu_ps(b.as_ptr().add(i));
            sum = _mm_add_ps(_mm_mul_ps(va, vb), sum);
            i += 4;
        }
        let mut result: [f32; 4] = [0.0; 4];
        _mm_storeu_ps(result.as_mut_ptr(), sum);
        let mut total = result.iter().sum::<f32>();
        while i < n {
            total += a[i] * b[i];
            i += 1;
        }
        total
    }

    // #47: TODO — WAL/MVCC integration for vector index.
    // Rollback does NOT revert vector index changes (insert/delete/update).
    // When a transaction is rolled back, the vector index retains changes
    // from the aborted transaction, causing persistent index corruption.
    // Fix: (1) store UndoRecord for each vector index mutation alongside
    // the data undo buffer, (2) replay undo records on rollback to revert
    // vector index to its pre-transaction state.
    // DEEP_AUDIT_FULL_2024.md item #47.
    pub fn insert(
        &self,
        node_id: u64,
        embedding: &[f32],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        if embedding.len() != self.dimension {
            return Err(crate::LightningError::Internal(format!(
                "Embedding dimension mismatch: expected {} but got {}",
                self.dimension,
                embedding.len()
            )));
        }
        self.insert_batch(&[(node_id, embedding.to_vec())], bm, tx)
    }

    pub fn insert_batch(
        &self,
        vectors: &[(u64, Vec<f32>)],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let dim = self.dimension;
        for (_, vec) in vectors {
            if vec.len() != dim {
                return Err(crate::LightningError::Internal(format!(
                    "Embedding dimension mismatch: expected {} but got {}",
                    dim,
                    vec.len()
                )));
            }
        }
        let entry_bytes = vi_entry_bytes(dim);
        let eps = vi_entries_per_page(dim);
        let bps = 4096usize;

        // Ensure header page exists
        if self.file_handle.get_num_pages() == 0 {
            let header_frame = bm.create_new_version(
                Arc::clone(&self.file_handle),
                VI_HEADER_PAGE,
                tx,
            )?;
            let ptr = header_frame.as_ptr();
            // SAFETY: SAFETY: Writing to a freshly created CoW page via create_new_version. The page is owned exclusively by this transaction.
            unsafe { ptr.write_bytes(0, bps) };
            bm.log_page_update(self.file_handle.file_id, VI_HEADER_PAGE, header_frame.as_slice())?;
            bm.unpin_page(&self.file_handle, VI_HEADER_PAGE, header_frame);
        }

        // Read current entry count from header
        let header_frame = bm.pin_page(Arc::clone(&self.file_handle), VI_HEADER_PAGE, tx)?;
        let current_entries = u64::from_le_bytes(header_frame.as_slice()[0..8].try_into()                .expect("header entry count is 8 bytes"));
        bm.unpin_page(&self.file_handle, VI_HEADER_PAGE, header_frame);

        let mut next_entry_idx = current_entries as usize;
        let total_new = vectors.len();

        for (node_id, vec) in vectors {
            // Accumulate in f64 to avoid overflow for large f32 values
            let norm_sq: f64 = vec.iter().map(|v| *v as f64 * *v as f64).sum();
            let inv_norm = 1.0 / (norm_sq.sqrt() as f32 + 1e-10);
            let page_idx = VI_DATA_START_PAGE + (next_entry_idx / eps) as u64;
            let slot_in_page = next_entry_idx % eps;

            while self.file_handle.get_num_pages() <= page_idx {
                self.file_handle.add_new_page()?;
            }

            let frame = bm.create_new_version(
                Arc::clone(&self.file_handle),
                page_idx,
                tx,
            )?;
            let ptr = frame.as_ptr();
            let offset = slot_in_page * entry_bytes;
            let write_end = offset + 12 + dim * 4;

            // Bounds check: ensure we don't write beyond the page
            if write_end > 4096 {
                return Err(crate::LightningError::Internal(format!(
                    "Vector index write: entry exceeds page boundary (offset={offset}, dim={dim}, end={write_end})"
                )));
            }

            // SAFETY: Bounds checked above. Copying data into a CoW page frame.
            // Frame is pinned, within pin-unpin lifecycle.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    node_id.to_le_bytes().as_ptr(),
                    ptr.add(offset),
                    8,
                );
                std::ptr::copy_nonoverlapping(
                    inv_norm.to_le_bytes().as_ptr(),
                    ptr.add(offset + 8),
                    4,
                );
                for j in 0..dim {
                    let val = vec[j].to_le_bytes();
                    std::ptr::copy_nonoverlapping(
                        val.as_ptr(),
                        ptr.add(offset + 12 + j * 4),
                        4,
                    );
                }
            }

            bm.log_page_update(self.file_handle.file_id, page_idx, frame.as_slice())?;
            bm.unpin_page(&self.file_handle, page_idx, frame);
            next_entry_idx += 1;
        }

        // Update header entry count
        let header_frame = bm.create_new_version(
            Arc::clone(&self.file_handle),
            VI_HEADER_PAGE,
            tx,
        )?;
        let new_count = (current_entries as usize + total_new) as u64;
        // SAFETY: SAFETY: Copying vector data into CoW page frame.
        unsafe {
            std::ptr::copy_nonoverlapping(
                new_count.to_le_bytes().as_ptr(),
                header_frame.as_ptr(),
                8,
            );
        }
        bm.log_page_update(self.file_handle.file_id, VI_HEADER_PAGE, header_frame.as_slice())?;
        bm.unpin_page(&self.file_handle, VI_HEADER_PAGE, header_frame);

        Ok(())
    }

    /// Exhaustive parallel vector search with SIMD dot product.
    /// Returns top-k (node_id, cosine_similarity) pairs.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        if query.len() != self.dimension {
            return Err(crate::LightningError::Internal(format!(
                "Query dimension mismatch: expected {} but got {}",
                self.dimension,
                query.len()
            )));
        }
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let bps = 4096usize;
        let num_entries = self.get_num_entries(bm, tx)? as usize;

        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let query_norm = (query.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10).recip();
        let query_normed: Vec<f32> = query.iter().map(|v| v * query_norm).collect();
        let num_pages = self.file_handle.get_num_pages();

        let heap: BinaryHeap<ScoredNode> = (0..num_entries)
            .into_par_iter()
            .fold(
                || BinaryHeap::with_capacity(k),
                |mut heap, entry_idx| {
                    let page_idx = VI_DATA_START_PAGE + ((entry_idx * entry_bytes) / bps) as u64;
                    let page_off = (entry_idx * entry_bytes) % bps;

                    if page_idx >= num_pages {
                        return heap;
                    }

                    let frame = match bm.pin_page(Arc::clone(&self.file_handle), page_idx, tx) {
                        Ok(f) => f,
                        Err(_) => return heap,
                    };

                    let offset = page_off;
                    if offset + dim * 4 > bps {
                        bm.unpin_page(&self.file_handle, page_idx, frame);
                        return heap;
                    }

                    let node_id_bytes: [u8; 8] = match frame.as_slice()[offset..offset + 8].try_into() {
                        Ok(b) => b,
                        Err(_) => {
                            bm.unpin_page(&self.file_handle, page_idx, frame);
                            return heap;
                        }
                    };
                    let node_id = u64::from_le_bytes(node_id_bytes);

                    let inv_norm_bytes: [u8; 4] = match frame.as_slice()[offset + 8..offset + 12].try_into() {
                        Ok(b) => b,
                        Err(_) => {
                            bm.unpin_page(&self.file_handle, page_idx, frame);
                            return heap;
                        }
                    };
                    let inv_norm = f32::from_le_bytes(inv_norm_bytes);

                    let emb_offset = offset + 12;
                    let frame_slice = frame.as_slice();
                    let emb_bytes = &frame_slice[emb_offset..emb_offset + dim * 4];

                    // SAFETY: emb_bytes may not be aligned to 4 bytes (f32 alignment).
                    // We check alignment and fall back to byte-by-byte conversion if needed.
                    let emb_f32: Vec<f32> = if emb_bytes.as_ptr().align_offset(std::mem::align_of::<f32>()) == 0 {
                        // Aligned: can use direct slice cast
                        let raw = unsafe {
                            std::slice::from_raw_parts(emb_bytes.as_ptr() as *const f32, dim)
                        };
                        raw.to_vec()
                    } else {
                        // Unaligned: convert byte-by-byte
                        emb_bytes.chunks_exact(4)
                            .take(dim)
                            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                            .collect()
                    };
                    let dot = Self::dot_product(&emb_f32, &query_normed);

                    bm.unpin_page(&self.file_handle, page_idx, frame);

                    heap.push(ScoredNode {
                        id: node_id,
                        score: dot * inv_norm,
                    });
                    if heap.len() > k {
                        heap.pop();
                    }
                    heap
                },
            )
            .reduce(
                || BinaryHeap::with_capacity(k),
                |mut a, b| {
                    for item in b {
                        a.push(item);
                        if a.len() > k {
                            a.pop();
                        }
                    }
                    a
                },
            );

        let mut results: Vec<(u64, f32)> = heap
            .into_iter()
            .map(|node| (node.id, node.score))
            .collect();
        results.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
        Ok(results)
    }

    pub fn get_num_entries(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<u64> {
        if self.file_handle.get_num_pages() == 0 {
            return Ok(0);
        }
        let header_frame = bm.pin_page(Arc::clone(&self.file_handle), VI_HEADER_PAGE, tx)?;
        let num_entries = u64::from_le_bytes(header_frame.as_slice()[0..8].try_into()                .expect("header entry count is 8 bytes"));
        bm.unpin_page(&self.file_handle, VI_HEADER_PAGE, header_frame);
        Ok(num_entries)
    }

    pub fn delete(
        &self,
        node_id: u64,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<bool> {
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let eps = vi_entries_per_page(dim);

        let num_entries = self.get_num_entries(bm, tx)? as usize;
        if num_entries == 0 {
            return Ok(false);
        }

        // Use node_index for O(1) lookup instead of O(n) scan.
        // Rebuild the index on cache miss (e.g., after entries change).
        let found_idx: usize = {
            let mut index = self.node_index.lock();
            if index.len() != num_entries {
                index.clear();
                index.reserve(num_entries);
                for entry_idx in 0..num_entries {
                    let page_idx = VI_DATA_START_PAGE + (entry_idx / eps) as u64;
                    let slot_in_page = entry_idx % eps;
                    let offset = slot_in_page * entry_bytes;

                    let frame = bm.pin_page(Arc::clone(&self.file_handle), page_idx, tx)?;
                    let stored_id = u64::from_le_bytes(
                        frame.as_slice()[offset..offset + 8]
                            .try_into()
                            .map_err(|_| crate::LightningError::Internal("invalid node_id bytes".into()))?,
                    );
                    bm.unpin_page(&self.file_handle, page_idx, frame);
                    index.insert(stored_id, entry_idx);
                }
            }
            match index.get(&node_id).copied() {
                Some(idx) => idx,
                None => return Ok(false),
            }
        }; // drop index lock
        
        if found_idx + 1 < num_entries {
            let last_idx = num_entries - 1;
            let src_page = VI_DATA_START_PAGE + (last_idx / eps) as u64;
            let src_slot = last_idx % eps;
            let dst_page = VI_DATA_START_PAGE + (found_idx / eps) as u64;
            let dst_slot = found_idx % eps;

            let src_offset = src_slot * entry_bytes;
            let dst_offset = dst_slot * entry_bytes;
            let src_frame = bm.pin_page(Arc::clone(&self.file_handle), src_page, tx)?;
            let entry_data = &src_frame.as_slice()[src_offset..src_offset + entry_bytes];
            let entry_vec = entry_data.to_vec();
            bm.unpin_page(&self.file_handle, src_page, src_frame);

            if src_page == dst_page {
                let frame = bm.create_new_version(Arc::clone(&self.file_handle), dst_page, tx)?;
                let ptr = frame.as_ptr();
                unsafe {
                    std::ptr::copy_nonoverlapping(entry_vec.as_ptr(), ptr.add(dst_slot * entry_bytes), entry_bytes);
                    std::ptr::write_bytes(ptr.add(src_slot * entry_bytes), 0, entry_bytes);
                }
                bm.log_page_update(self.file_handle.file_id, dst_page, frame.as_slice())?;
                bm.unpin_page(&self.file_handle, dst_page, frame);
            } else {
                let dst_frame = bm.create_new_version(Arc::clone(&self.file_handle), dst_page, tx)?;
                let dst_ptr = dst_frame.as_ptr();
                // SAFETY: SAFETY: Copying last entry to deleted slot in newly created version.
                unsafe {
                    std::ptr::copy_nonoverlapping(entry_vec.as_ptr(), dst_ptr.add(dst_offset), entry_bytes);
                }
                bm.log_page_update(self.file_handle.file_id, dst_page, dst_frame.as_slice())?;
                bm.unpin_page(&self.file_handle, dst_page, dst_frame);

                let last_frame = bm.create_new_version(Arc::clone(&self.file_handle), src_page, tx)?;
                // SAFETY: SAFETY: Zeroing out the old last slot in newly created version.
                unsafe {
                    std::ptr::write_bytes(last_frame.as_ptr().add(src_slot * entry_bytes), 0, entry_bytes);
                }
                bm.log_page_update(self.file_handle.file_id, src_page, last_frame.as_slice())?;
                bm.unpin_page(&self.file_handle, src_page, last_frame);
            }
        }

        let header_frame = bm.create_new_version(Arc::clone(&self.file_handle), VI_HEADER_PAGE, tx)?;
        let new_count = (num_entries - 1) as u64;
        // SAFETY: SAFETY: Writing updated embedding data into CoW page frame.
        unsafe {
            std::ptr::copy_nonoverlapping(new_count.to_le_bytes().as_ptr(), header_frame.as_ptr(), 8);
        }
        bm.log_page_update(self.file_handle.file_id, VI_HEADER_PAGE, header_frame.as_slice())?;
        bm.unpin_page(&self.file_handle, VI_HEADER_PAGE, header_frame);

        Ok(true)
    }

    pub fn update(
        &self,
        node_id: u64,
        embedding: &[f32],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<bool> {
        if embedding.len() != self.dimension {
            return Err(crate::LightningError::Internal(format!(
                "Embedding dimension mismatch: expected {} but got {}",
                self.dimension,
                embedding.len()
            )));
        }
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let eps = vi_entries_per_page(dim);

        let num_entries = self.get_num_entries(bm, tx)? as usize;
        if num_entries == 0 {
            return Ok(false);
        }

        let inv_norm = 1.0 / (embedding.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10);

        for entry_idx in 0..num_entries {
            let page_idx = VI_DATA_START_PAGE + (entry_idx / eps) as u64;
            let slot_in_page = entry_idx % eps;
            let offset = slot_in_page * entry_bytes;

            let frame = bm.pin_page(Arc::clone(&self.file_handle), page_idx, tx)?;
            let stored_id = u64::from_le_bytes(
                frame.as_slice()[offset..offset + 8]
                    .try_into()
                    .expect("node_id is 8 bytes"),
            );
            bm.unpin_page(&self.file_handle, page_idx, frame);

            if stored_id == node_id {
                let frame = bm.create_new_version(Arc::clone(&self.file_handle), page_idx, tx)?;
                let ptr = frame.as_ptr();
                // SAFETY: SAFETY: Reading from pinned frame in search hot path.
                unsafe {
                    std::ptr::copy_nonoverlapping(node_id.to_le_bytes().as_ptr(), ptr.add(offset), 8);
                    std::ptr::copy_nonoverlapping(inv_norm.to_le_bytes().as_ptr(), ptr.add(offset + 8), 4);
                    for j in 0..dim {
                        let val = embedding[j].to_le_bytes();
                        std::ptr::copy_nonoverlapping(val.as_ptr(), ptr.add(offset + 12 + j * 4), 4);
                    }
                }
                bm.log_page_update(self.file_handle.file_id, page_idx, frame.as_slice())?;
                bm.unpin_page(&self.file_handle, page_idx, frame);
                return Ok(true);
            }
        }

        Ok(false)
    }
}
