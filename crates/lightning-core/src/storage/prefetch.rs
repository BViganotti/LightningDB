use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

/// Tracks page access patterns and predicts future page accesses using
/// a transition frequency table. The tracker observes which pages are
/// accessed after which other pages and builds a Markov-chain model.
///
/// The predictor is used by `BufferManager` to prefetch pages before
/// they are requested, reducing disk I/O latency.
pub struct PrefetchTracker {
    /// Transition matrix: (file_id, page) → [(next_file_id, next_page, count)]
    transitions: RwLock<HashMap<(u64, u64), Vec<((u64, u64), u64)>>>,
    /// Recent access window for correlation analysis
    access_window: RwLock<VecDeque<(u64, u64)>>,
    /// Access frequency counter: (file_id, page) → total_access_count
    access_counts: RwLock<HashMap<(u64, u64), u64>>,
    window_size: usize,
    min_observations: u64,
    max_transitions_per_page: usize,
}

impl PrefetchTracker {
    pub fn new() -> Self {
        Self {
            transitions: RwLock::new(HashMap::new()),
            access_window: RwLock::new(VecDeque::new()),
            access_counts: RwLock::new(HashMap::new()),
            window_size: 64,
            min_observations: 3,
            max_transitions_per_page: 16,
        }
    }

    /// Record that `page` was just accessed. Updates the transition matrix
    /// and access frequency counter.
    pub fn record_access(&self, file_id: u64, page_idx: u64) {
        let key = (file_id, page_idx);

        // Increment access count
        {
            let mut counts = self.access_counts.write();
            *counts.entry(key).or_insert(0) += 1;
        }

        // Update transition matrix from the previous page to this one
        let mut window = self.access_window.write();
        if let Some(&prev_key) = window.back() {
            if prev_key != key {
                let mut trans = self.transitions.write();
                let entries = trans.entry(prev_key).or_default();
                if let Some(pos) = entries.iter().position(|(k, _)| *k == key) {
                    entries[pos].1 += 1;
                } else {
                    if entries.len() >= self.max_transitions_per_page {
                        // Prune lowest-frequency transition
                        if let Some(min_pos) = entries
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, (_, cnt))| *cnt)
                            .map(|(i, _)| i)
                        {
                            entries.remove(min_pos);
                        }
                    }
                    entries.push((key, 1));
                }
            }
        }

        // Maintain window
        window.push_back(key);
        if window.len() > self.window_size {
            window.pop_front();
        }
    }

    /// Given the current page, return the top-K most likely next pages.
    /// Only returns predictions with confidence >= `min_confidence` (0.0 to 1.0).
    pub fn predict_next(
        &self,
        file_id: u64,
        page_idx: u64,
        top_k: usize,
        min_confidence: f64,
    ) -> Vec<(u64, u64)> {
        let key = (file_id, page_idx);
        let trans = self.transitions.read();
        let Some(entries) = trans.get(&key) else {
            return Vec::new();
        };

        let total: u64 = entries.iter().map(|(_, c)| c).sum();
        if total < self.min_observations {
            return Vec::new();
        }

        let mut scored: Vec<((u64, u64), f64)> = entries
            .iter()
            .map(|(k, c)| (*k, *c as f64 / total as f64))
            .filter(|(_, conf)| *conf >= min_confidence)
            .collect();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scored.truncate(top_k);
        scored.into_iter().map(|(k, _)| k).collect()
    }

    /// Return the most frequently accessed pages (hot pages) that should
    /// be preferentially kept in the buffer pool.
    pub fn get_hot_pages(&self, top_n: usize) -> Vec<((u64, u64), f64)> {
        let counts = self.access_counts.read();
        let mut pages: Vec<((u64, u64), u64)> = counts.iter().map(|(k, c)| (*k, *c)).collect();
        pages.sort_by(|a, b| b.1.cmp(&a.1));
        let total: u64 = pages.iter().map(|(_, c)| c).sum();
        pages
            .into_iter()
            .take(top_n)
            .map(|(k, c)| (k, if total > 0 { c as f64 / total as f64 } else { 0.0 }))
            .collect()
    }

    /// Get the total number of unique pages tracked.
    pub fn num_tracked_pages(&self) -> usize {
        self.access_counts.read().len()
    }

    /// Get the total number of transitions observed.
    pub fn num_transitions(&self) -> usize {
        self.transitions.read().len()
    }
}
