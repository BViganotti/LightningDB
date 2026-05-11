use crate::storage::buffer_manager::BufferManager;
use crate::storage::file_handle::FileHandle;
use crate::LightningError;
use crate::Result;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

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
            page_header_size: 8,
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
        let entry_bytes = 4 + dim * 4;
        let _page_header_size = self.page_header_size;

        let mut page_groups: HashMap<u64, Vec<(usize, Vec<u8>)>> = HashMap::new();
        let mut max_page = 0u64;

        for (idx, (node_id, vec)) in vectors.iter().enumerate() {
            let inv_norm = 1.0 / (vec.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10);
            let page_idx = *node_id;

            let mut buf = Vec::with_capacity(entry_bytes);
            buf.extend_from_slice(&node_id.to_le_bytes());
            buf.extend_from_slice(&inv_norm.to_le_bytes());
            for v in vec.iter() {
                buf.extend_from_slice(&v.to_le_bytes());
            }

            page_groups.entry(page_idx).or_default().push((*node_id as usize, buf));
            max_page = max_page.max(page_idx);
        }

        let bps = 4096usize;
        for (_page_idx, entries) in &page_groups {
            if entries.is_empty() {
                continue;
            }
            let total_data: usize = entries.iter().map(|(_, buf)| buf.len()).sum();
            let num_data_pages = (total_data + bps - 1) / bps;

            for dp in 0..num_data_pages {
                let start = dp * bps;
                let end = std::cmp::min(start + bps, total_data);
                let chunk_size = end - start;

                while self.file_handle.get_num_pages() as usize <= dp {
                    self.file_handle.add_new_page()?;
                }
                let frame = bm.create_new_version(Arc::clone(&self.file_handle), dp as u64, tx)?;
                let mut offset = 0usize;
                for (_, buf) in entries {
                    if offset >= start && offset < end {
                        let copy_start = if offset >= start { 0 } else { start - offset };
                        let copy_end = std::cmp::min(buf.len(), end - offset);
                        if copy_end > copy_start {
                            let dest_start = if offset >= start { offset - start } else { 0 };
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    buf.as_ptr().add(copy_start),
                                    frame.data.as_ptr() as *mut u8,
                                    copy_end - copy_start,
                                );
                            }
                        }
                    }
                    offset += buf.len();
                }
                bm.log_page_update(self.file_handle.file_id, dp as u64, &frame.data)?;
                bm.unpin_page(&self.file_handle, dp as u64, frame);
            }
        }

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
        let entry_bytes = 4 + dim * 4;
        let bps = 4096usize;
        let num_pages = self.file_handle.get_num_pages() as usize;
        let entries_per_page = bps / entry_bytes;
        let num_entries = num_pages * entries_per_page;

        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let query_norm = (query.iter().map(|v| v * v).sum::<f32>().sqrt() + 1e-10).recip();
        let query_normed: Vec<f32> = query.iter().map(|v| v * query_norm).collect();

        let mut results: Vec<(u64, f32)> = (0..num_entries)
            .into_par_iter()
            .map(|entry_idx| -> Option<(u64, f32)> {
                let page_idx = (entry_idx * entry_bytes) / bps;
                let page_off = (entry_idx * entry_bytes) % bps;

                if page_idx >= num_pages {
                    return None;
                }

                let frame = match bm.pin_page(Arc::clone(&self.file_handle), page_idx as u64, tx) {
                    Ok(f) => f,
                    Err(_) => return None,
                };

                let offset = page_off + self.page_header_size;
                if offset + dim * 4 > bps {
                    bm.unpin_page(&self.file_handle, page_idx as u64, frame);
                    return None;
                }

                let node_id_bytes: [u8; 8] = match frame.data[page_off..page_off + 8].try_into() {
                    Ok(b) => b,
                    Err(_) => {
                        bm.unpin_page(&self.file_handle, page_idx as u64, frame);
                        return None;
                    }
                };
                let node_id = u64::from_le_bytes(node_id_bytes);

                let inv_norm_bytes: [u8; 4] = match frame.data[page_off + 8..page_off + 12].try_into() {
                    Ok(b) => b,
                    Err(_) => {
                        bm.unpin_page(&self.file_handle, page_idx as u64, frame);
                        return None;
                    }
                };
                let inv_norm = f32::from_le_bytes(inv_norm_bytes);

                let emb_offset = page_off + 12;
                let mut dot = 0.0f32;
                let mut i = 0;
                while i + 8 <= dim {
                    let mut va = [0.0f32; 8];
                    let mut vb = [0.0f32; 8];
                    for j in 0..8 {
                        let idx = emb_offset + (i + j) * 4;
                        if idx + 4 <= bps {
                            let bytes: [u8; 4] = match frame.data[idx..idx + 4].try_into() {
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
                        let bytes: [u8; 4] = match frame.data[idx..idx + 4].try_into() {
                            Ok(b) => b,
                            Err(_) => break,
                        };
                        let emb_val = f32::from_le_bytes(bytes);
                        dot += emb_val * query_normed[i];
                    }
                    i += 1;
                }

                bm.unpin_page(&self.file_handle, page_idx as u64, frame);
                Some((node_id, dot * inv_norm))
            })
            .filter_map(|x| x)
            .collect();

        results.par_sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        results.truncate(k);
        Ok(results)
    }

    pub fn get_num_entries(
        &self,
        _bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<u64> {
        let dim = self.dimension;
        let entry_bytes = 4 + dim * 4;
        let bps = 4096usize;
        let num_pages = self.file_handle.get_num_pages() as usize;
        let entries_per_page = bps / entry_bytes;
        Ok((num_pages * entries_per_page) as u64)
    }
}
