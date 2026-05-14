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

fn vi_entries_per_page() -> usize {
    let bps = 4096usize;
    let entry_bytes = 4 + 768 * 4;
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
    page_header_size: usize,
}

impl VectorIndex {
    pub fn new(file_handle: Arc<FileHandle>) -> Self {
        Self {
            file_handle,
            dimension: 768,
            page_header_size: 0,
        }
    }

    // --- SIMD-accelerated dot product ---
    // Uses portable SIMD when available, falls back to scalar
    fn dot_product(a: &[f32], b: &[f32]) -> f32 {
        #[cfg(target_feature = "avx2")]
        {
            if a.len() >= 8 {
                return unsafe { Self::avx2_dot(a, b) };
            }
        }
        #[cfg(target_feature = "sse")]
        {
            if a.len() >= 4 {
                return unsafe { Self::sse_dot(a, b) };
            }
        }
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
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

    pub fn insert(
        &self,
        node_id: u64,
        embedding: &[f32; 768],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.insert_batch(&[(node_id, *embedding)], bm, tx)
    }

    pub fn insert_batch(
        &self,
        vectors: &[(u64, [f32; 768])],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let eps = vi_entries_per_page();
        let bps = 4096usize;

        // Ensure header page exists
        if self.file_handle.get_num_pages() == 0 {
            let header_frame = bm.create_new_version(
                Arc::clone(&self.file_handle),
                VI_HEADER_PAGE,
                tx,
            )?;
            let ptr = header_frame.as_ptr();
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
            let inv_norm = 1.0 / (vec.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10);
            let page_idx = VI_DATA_START_PAGE + (next_entry_idx / eps) as u64;
            let slot_in_page = next_entry_idx % eps;

            while (self.file_handle.get_num_pages() as u64) <= page_idx {
                self.file_handle.add_new_page()?;
            }

            let frame = bm.create_new_version(
                Arc::clone(&self.file_handle),
                page_idx,
                tx,
            )?;
            let ptr = frame.as_ptr();
            let offset = slot_in_page * entry_bytes;

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
        query: &[f32; 768],
        k: usize,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let bps = 4096usize;
        let num_entries = self.get_num_entries(bm, tx)? as usize;

        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let query_norm = (query.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10).recip();
        let query_normed: Vec<f32> = query.iter().map(|v| v * query_norm).collect();

        let heap: BinaryHeap<ScoredNode> = (0..num_entries)
            .into_par_iter()
            .fold(
                || BinaryHeap::with_capacity(k),
                |mut heap, entry_idx| {
                    let page_idx = VI_DATA_START_PAGE + ((entry_idx * entry_bytes) / bps) as u64;
                    let page_off = (entry_idx * entry_bytes) % bps;

                    if page_idx >= self.file_handle.get_num_pages() {
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
                    let mut dot = 0.0f32;
                    let mut i = 0;
                    while i + 8 <= dim {
                        let mut va = [0.0f32; 8];
                        let mut vb = [0.0f32; 8];
                        for j in 0..8 {
                            let idx = emb_offset + (i + j) * 4;
                            if idx + 4 <= bps {
                                let bytes: [u8; 4] = match frame.as_slice()[idx..idx + 4].try_into() {
                                    Ok(b) => b,
                                    Err(_) => break,
                                };
                                va[j] = f32::from_le_bytes(bytes);
                                vb[j] = query_normed[i + j];
                            }
                        }
                        dot += Self::dot_product(&va, &vb);
                        i += 8;
                    }
                    while i < dim {
                        let idx = emb_offset + i * 4;
                        if idx + 4 <= bps {
                            let bytes: [u8; 4] = match frame.as_slice()[idx..idx + 4].try_into() {
                                Ok(b) => b,
                                Err(_) => break,
                            };
                            let emb_val = f32::from_le_bytes(bytes);
                            dot += emb_val * query_normed[i];
                        }
                        i += 1;
                    }

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
        let eps = vi_entries_per_page();

        let num_entries = self.get_num_entries(bm, tx)? as usize;
        if num_entries == 0 {
            return Ok(false);
        }

        let mut found_idx = None;
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
                found_idx = Some(entry_idx);
                break;
            }
        }

        let found_idx = match found_idx {
            Some(idx) => idx,
            None => return Ok(false),
        };

        if found_idx + 1 < num_entries {
            let last_idx = num_entries - 1;
            let src_page = VI_DATA_START_PAGE + (last_idx / eps) as u64;
            let src_slot = last_idx % eps;
            let dst_page = VI_DATA_START_PAGE + (found_idx / eps) as u64;
            let dst_slot = found_idx % eps;

            let src_frame = bm.pin_page(Arc::clone(&self.file_handle), src_page, tx)?;
            let src_offset = src_slot * entry_bytes;
            let entry_data = &src_frame.as_slice()[src_offset..src_offset + entry_bytes];
            let entry_vec = entry_data.to_vec();
            bm.unpin_page(&self.file_handle, src_page, src_frame);

            let dst_frame = bm.create_new_version(Arc::clone(&self.file_handle), dst_page, tx)?;
            let dst_ptr = dst_frame.as_ptr();
            let dst_offset = dst_slot * entry_bytes;
            unsafe {
                std::ptr::copy_nonoverlapping(entry_vec.as_ptr(), dst_ptr.add(dst_offset), entry_bytes);
            }
            bm.log_page_update(self.file_handle.file_id, dst_page, dst_frame.as_slice())?;
            bm.unpin_page(&self.file_handle, dst_page, dst_frame);

            let last_frame = bm.create_new_version(Arc::clone(&self.file_handle), src_page, tx)?;
            unsafe {
                std::ptr::write_bytes(last_frame.as_ptr().add(src_slot * entry_bytes), 0, entry_bytes);
            }
            bm.log_page_update(self.file_handle.file_id, src_page, last_frame.as_slice())?;
            bm.unpin_page(&self.file_handle, src_page, last_frame);
        }

        let header_frame = bm.create_new_version(Arc::clone(&self.file_handle), VI_HEADER_PAGE, tx)?;
        let new_count = (num_entries - 1) as u64;
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
        embedding: &[f32; 768],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<bool> {
        let dim = self.dimension;
        let entry_bytes = vi_entry_bytes(dim);
        let eps = vi_entries_per_page();

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
