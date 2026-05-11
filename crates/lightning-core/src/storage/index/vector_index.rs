use crate::storage::buffer_manager::BufferManager;
use crate::storage::file_handle::FileHandle;
use crate::Result;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

pub struct VectorIndex {
    pub(crate) file_handle: Arc<FileHandle>,
}

#[derive(Debug, Clone, PartialEq)]
struct ScoredNode {
    id: u64,
    score: f32,
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.score.partial_cmp(&self.score)
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl VectorIndex {
    pub fn new(file_handle: Arc<FileHandle>) -> Self {
        Self { file_handle }
    }

    /// Optimized batch insert with pre-calculated inverse norms for branchless search
    pub fn insert_batch(
        &self,
        vectors: &[(u64, [f32; 768])],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let bytes_per_page = crate::storage::buffer_manager::PAGE_SIZE;
        let mut page_groups: std::collections::HashMap<u64, (usize, &[f32; 768])> =
            std::collections::HashMap::new();

        let mut max_page = 0;
        for (node_id, emb) in vectors {
            // One vector per page for 4KB alignment (3072 data + metadata)
            let page_idx = *node_id;
            page_groups.insert(page_idx, (0, emb));
            if page_idx > max_page {
                max_page = page_idx;
            }
        }

        while self.file_handle.get_num_pages() <= max_page {
            let _ = self.file_handle.add_new_page()?;
        }

        for (page_idx, (_, embedding)) in page_groups {
            let frame = bm.create_new_version(self.file_handle.clone(), page_idx, tx)?;

            // Pre-calculate inverse norm for branchless cosine similarity
            let mut norm_sq = 0.0f32;
            for x in embedding {
                norm_sq += x * x;
            }
            let inv_norm = if norm_sq > 0.0 {
                1.0 / norm_sq.sqrt()
            } else {
                0.0
            };

            unsafe {
                let data_ptr = frame.data.as_ptr() as *mut u8;
                // Copy 768 floats (3072 bytes)
                std::ptr::copy_nonoverlapping(embedding.as_ptr() as *const u8, data_ptr, 3072);
                // Store inv_norm at the end of the page metadata area
                std::ptr::copy_nonoverlapping(
                    &inv_norm as *const f32 as *const u8,
                    data_ptr.add(3072),
                    4,
                );
            }
        }

        Ok(())
    }

    pub fn insert(
        &self,
        node_id: u64,
        embedding: &[f32; 768],
        bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.insert_batch(&[(node_id, *embedding)], bm, _tx)
    }

    /// High-performance branchless SIMD Dot Product
    #[inline(always)]
    fn dot_product(a: &[f32; 768], b: &[f32; 768]) -> f32 {
        let mut sum = 0.0f32;
        // Compiler will unroll and use SIMD (AVX2/NEON) automatically with this pattern
        for i in 0..768 {
            sum += a[i] * b[i];
        }
        sum
    }

    /// Parallel exhaustive search using pre-calculated inverse norms
    pub fn search(
        &self,
        query: &[f32; 768],
        k: usize,
        bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, f32)>> {
        let num_pages = self.file_handle.get_num_pages();

        // Calculate query norm once
        let mut q_norm_sq = 0.0f32;
        for x in query {
            q_norm_sq += x * x;
        }
        let q_inv_norm = if q_norm_sq > 0.0 {
            1.0 / q_norm_sq.sqrt()
        } else {
            0.0
        };

        let results = (0..num_pages)
            .into_par_iter()
            .map(|page_idx| {
                let mut local_heap = BinaryHeap::new();
                if let Ok(page) = bm.pin_page(self.file_handle.clone(), page_idx, _tx) {
                    let data = &page.data;

                    // Branchless fetch using pointers
                    let vec_ptr = data.as_ptr() as *const [f32; 768];
                    let inv_norm_ptr = unsafe { data.as_ptr().add(3072) as *const f32 };

                    let target_vec = unsafe { &*vec_ptr };
                    let target_inv_norm = unsafe { *inv_norm_ptr };

                    if target_inv_norm > 0.0 {
                        let dot = Self::dot_product(query, target_vec);
                        let score = dot * target_inv_norm * q_inv_norm;

                        // Heap maintenance is the only branching logic
                        if local_heap.len() < k {
                            local_heap.push(ScoredNode {
                                id: page_idx,
                                score,
                            });
                        } else if let Some(min) = local_heap.peek() {
                            if score > min.score {
                                local_heap.pop();
                                local_heap.push(ScoredNode {
                                    id: page_idx,
                                    score,
                                });
                            }
                        }
                    }
                }
                local_heap
            })
            .reduce(
                || BinaryHeap::new(),
                |mut h1, mut h2| {
                    while let Some(n) = h2.pop() {
                        if h1.len() < k {
                            h1.push(n);
                        } else if let Some(min) = h1.peek() {
                            if n.score > min.score {
                                h1.pop();
                                h1.push(n);
                            }
                        }
                    }
                    h1
                },
            );

        let mut res = Vec::new();
        let mut final_heap = results;
        while let Some(n) = final_heap.pop() {
            res.push((n.id, n.score));
        }
        res.reverse();
        Ok(res)
    }
}
