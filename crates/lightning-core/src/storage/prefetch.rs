use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks page access patterns and predicts future page accesses using
/// multi-order Markov chains with time-decayed weights and automatic
/// confidence threshold tuning.
pub struct PrefetchTracker {
    /// 1st-order transitions: (file_id, page) → [(next_file_id, next_page, weighted_count)]
    transitions_1st: RwLock<HashMap<(u64, u64), Vec<((u64, u64), f64)>>>,
    /// 2nd-order transitions: ((prev_fid, prev_pid), (cur_fid, cur_pid)) → [(next_fid, next_pid, weighted_count)]
    transitions_2nd: RwLock<HashMap<((u64, u64), (u64, u64)), Vec<((u64, u64), f64)>>>,
    /// Recent access window for correlation analysis and 2nd-order tracking
    access_window: RwLock<VecDeque<(u64, u64)>>,
    /// Access frequency counter
    access_counts: RwLock<HashMap<(u64, u64), u64>>,
    window_size: usize,
    min_observations: u64,
    max_transitions_per_page: usize,
    /// Decay factor applied to existing transitions (0.0-1.0). Higher = faster decay.
    decay_factor: f64,
    /// Prediction accuracy tracking
    predictions_made: AtomicU64,
    predictions_hit: AtomicU64,
    auto_confidence: RwLock<f64>,
    min_confidence: f64,
}

impl Default for PrefetchTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefetchTracker {
    pub fn new() -> Self {
        Self {
            transitions_1st: RwLock::new(HashMap::new()),
            transitions_2nd: RwLock::new(HashMap::new()),
            access_window: RwLock::new(VecDeque::new()),
            access_counts: RwLock::new(HashMap::new()),
            window_size: 64,
            min_observations: 3,
            max_transitions_per_page: 16,
            decay_factor: 0.05,
            predictions_made: AtomicU64::new(0),
            predictions_hit: AtomicU64::new(0),
            auto_confidence: RwLock::new(0.3),
            min_confidence: 0.3,
        }
    }

    fn apply_decay(entries: &mut Vec<((u64, u64), f64)>, decay: f64) {
        let total: f64 = entries.iter().map(|(_, w)| *w).sum();
        if total == 0.0 {
            return;
        }
        for (_, w) in entries.iter_mut() {
            *w *= 1.0 - decay;
        }
        // Remove entries that decayed below threshold
        entries.retain(|(_, w)| *w > 0.01);
    }

    fn record_transition(
        trans: &mut HashMap<(u64, u64), Vec<((u64, u64), f64)>>,
        from: (u64, u64),
        to: (u64, u64),
        decay: f64,
        max_entries: usize,
    ) {
        let entries = trans.entry(from).or_default();
        Self::apply_decay(entries, decay);
        if let Some(pos) = entries.iter().position(|(k, _)| *k == to) {
            entries[pos].1 += 1.0;
        } else {
            if entries.len() >= max_entries {
                if let Some(min_pos) = entries
                    .iter()
                    .enumerate()
                    .min_by(|(_, (_, a)), (_, (_, b))| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(i, _)| i)
                {
                    entries.remove(min_pos);
                }
            }
            entries.push((to, 1.0));
        }
    }

    fn record_transition_2nd(
        trans: &mut HashMap<((u64, u64), (u64, u64)), Vec<((u64, u64), f64)>>,
        from: ((u64, u64), (u64, u64)),
        to: (u64, u64),
        decay: f64,
        max_entries: usize,
    ) {
        let entries = trans.entry(from).or_default();
        Self::apply_decay(entries, decay);
        if let Some(pos) = entries.iter().position(|(k, _)| *k == to) {
            entries[pos].1 += 1.0;
        } else {
            if entries.len() >= max_entries {
                if let Some(min_pos) = entries
                    .iter()
                    .enumerate()
                    .min_by(|(_, (_, a)), (_, (_, b))| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(i, _)| i)
                {
                    entries.remove(min_pos);
                }
            }
            entries.push((to, 1.0));
        }
    }

    pub fn record_access(&self, file_id: u64, page_idx: u64) {
        let key = (file_id, page_idx);

        {
            let mut counts = self.access_counts.write();
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut window = self.access_window.write();
        if let Some(&prev_key) = window.back() {
            if prev_key != key {
                let decay = self.decay_factor;
                let max_entries = self.max_transitions_per_page;

                // Record 1st-order: prev → current
                let mut trans1 = self.transitions_1st.write();
                Self::record_transition(&mut *trans1, prev_key, key, decay, max_entries);

                // Record 2nd-order: (prev_prev, prev) → current
                if window.len() >= 2 {
                    let prev_prev = window[window.len() - 2];
                    let mut trans2 = self.transitions_2nd.write();
                    Self::record_transition_2nd(&mut *trans2, (prev_prev, prev_key), key, decay, max_entries);
                }
            }
        }

        window.push_back(key);
        if window.len() > self.window_size {
            window.pop_front();
        }
    }

    /// Report whether a previous prediction was correct (the predicted page was accessed).
    pub fn report_prediction_result(&self, was_hit: bool) {
        let made = self.predictions_made.fetch_add(1, Ordering::Release);
        if was_hit {
            self.predictions_hit.fetch_add(1, Ordering::Release);
        }
        // Auto-tune confidence every 100 predictions
        if made >= 99 {
            // Atomically snapshot and reset both counters
            self.predictions_made.store(0, Ordering::Release);
            let hit_total = self.predictions_hit.swap(0, Ordering::Release);
            let made_total = made + 1;
            let hit_rate = if made_total > 0 {
                hit_total as f64 / made_total as f64
            } else {
                0.0
            };
            let mut conf = self.auto_confidence.write();
            if hit_rate > 0.7 && *conf < 0.8 {
                *conf += 0.05;
            } else if hit_rate < 0.3 && *conf > 0.1 {
                *conf -= 0.05;
            }
        }
    }

    /// Get the current auto-tuned confidence threshold.
    pub fn get_confidence_threshold(&self) -> f64 {
        let auto = *self.auto_confidence.read();
        auto.max(self.min_confidence)
    }

    pub fn predict_next(
        &self,
        file_id: u64,
        page_idx: u64,
        top_k: usize,
        min_confidence: f64,
    ) -> Vec<(u64, u64)> {
        let key = (file_id, page_idx);
        let window = self.access_window.read();

        // Try 2nd-order prediction first (more precise)
        if window.len() >= 2 {
            let prev_key = window[window.len() - 2];
            let state = (prev_key, key);
            let trans2 = self.transitions_2nd.read();
            if let Some(entries) = trans2.get(&state) {
                let total: f64 = entries.iter().map(|(_, w)| *w).sum();
                if total >= self.min_observations as f64 {
                    let mut scored: Vec<((u64, u64), f64)> = entries
                        .iter()
                        .map(|(k, w)| (*k, *w / total))
                        .filter(|(_, conf)| *conf >= min_confidence)
                        .collect();
                    if !scored.is_empty() {
                        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                        scored.truncate(top_k);
                        return scored.into_iter().map(|(k, _)| k).collect();
                    }
                }
            }
        }

        // Fall back to 1st-order
        let trans1 = self.transitions_1st.read();
        let Some(entries) = trans1.get(&key) else {
            return Vec::new();
        };

        let total: f64 = entries.iter().map(|(_, w)| *w).sum();
        if total < self.min_observations as f64 {
            return Vec::new();
        }

        let mut scored: Vec<((u64, u64), f64)> = entries
            .iter()
            .map(|(k, w)| (*k, *w / total))
            .filter(|(_, conf)| *conf >= min_confidence)
            .collect();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);
        scored.into_iter().map(|(k, _)| k).collect()
    }

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

    pub fn num_tracked_pages(&self) -> usize {
        self.access_counts.read().len()
    }

    pub fn num_transitions(&self) -> usize {
        self.transitions_1st.read().len()
    }
}
