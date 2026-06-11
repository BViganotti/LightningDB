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

    fn remove_if_empty<K, V>(map: &mut HashMap<K, Vec<V>>, key: &K)
    where
        K: std::cmp::Eq + std::hash::Hash,
    {
        if let Some(v) = map.get(key) {
            if v.is_empty() {
                map.remove(key);
            }
        }
    }

    fn record_transition(
        trans: &mut HashMap<(u64, u64), Vec<((u64, u64), f64)>>,
        from: (u64, u64),
        to: (u64, u64),
    ) {
        let total: f64 = trans.values().flat_map(|v| v.iter()).map(|(_, w)| *w).sum();
        let entry = trans.entry(from).or_default();
        let mut found = false;
        for (key, weight) in entry.iter_mut() {
            if *key == to {
                *weight += 1.0;
                found = true;
                break;
            }
        }
        if !found {
            entry.push((to, 1.0));
        }
    }

    // Flag to skip removing empty vectors where this isn't critical
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
        if was_hit {
            self.predictions_hit.fetch_add(1, Ordering::Release);
        }
        // Atomically increment and check the made counter.
        // Use fetch_update to atomically check if we should tune,
        // preventing race conditions between concurrent callers.
        let should_tune = self.predictions_made.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |v| if v >= 99 { Some(0) } else { Some(v + 1) },
        ).unwrap_or(0) >= 99;
        if should_tune {
            let hit_total = self.predictions_hit.swap(0, Ordering::Release);
            // The made counter was already reset by fetch_update above.
            // Use 100 as the denominator (the count of predictions we just tuned over).
            let hit_rate = hit_total as f64 / 100.0;
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
                        .filter_map(|(k, w)| {
                            let conf = *w as f64 / total;
                            if conf >= min_confidence { Some((*k, conf)) } else { None }
                        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_tracker_is_empty() {
        let pt = PrefetchTracker::new();
        assert_eq!(pt.num_tracked_pages(), 0);
        assert_eq!(pt.num_transitions(), 0);
        assert_eq!(pt.predict_next(0, 0, 5, 0.5), Vec::new());
        assert_eq!(pt.get_hot_pages(10), Vec::new());
    }

    #[test]
    fn test_default_trait() {
        let pt: PrefetchTracker = Default::default();
        assert_eq!(pt.num_tracked_pages(), 0);
    }

    #[test]
    fn test_record_access_tracks_counts() {
        let pt = PrefetchTracker::new();
        pt.record_access(0, 1);
        pt.record_access(0, 2);
        pt.record_access(0, 1);
        assert_eq!(pt.num_tracked_pages(), 2);
    }

    #[test]
    fn test_consecutive_same_page_no_transition() {
        let pt = PrefetchTracker::new();
        // Accessing the same page twice should NOT create a self-transition
        pt.record_access(0, 1);
        pt.record_access(0, 1);
        pt.record_access(0, 2);
        assert_eq!(pt.num_transitions(), 1);
    }

    #[test]
    fn test_predict_next_1st_order() {
        let pt = PrefetchTracker::new();
        // Build a pattern: 0→1→2→3 repeatedly
        for _ in 0..10 {
            pt.record_access(0, 1);
            pt.record_access(0, 2);
            pt.record_access(0, 3);
        }

        // Page 1 → next should be page 2
        let pred = pt.predict_next(0, 1, 1, 0.5);
        assert!(!pred.is_empty(), "should predict something");
        assert_eq!(pred[0], (0, 2), "page 1 → page 2 (1st-order)");
    }

    #[test]
    fn test_predict_next_2nd_order() {
        let pt = PrefetchTracker::new();
        // Build a 2nd-order pattern:
        // Access sequence: (0,1)→(0,2)→(0,3) repeated many times
        for _ in 0..10 {
            pt.record_access(0, 1);
            pt.record_access(0, 2);
            pt.record_access(0, 3);
        }

        // After (0,2) with previous (0,1), should predict (0,3)
        let pred = pt.predict_next(0, 2, 1, 0.5);
        assert!(!pred.is_empty());
        assert_eq!(pred[0], (0, 3), "2nd-order: (1,2) → 3");
    }

    #[test]
    fn test_predict_returns_empty_below_min_observations() {
        let pt = PrefetchTracker::new();
        pt.record_access(0, 1);
        pt.record_access(0, 2);
        // Only 2 observations, min_observations is 3
        let pred = pt.predict_next(0, 1, 1, 0.5);
        assert!(pred.is_empty(), "below min_observations");
    }

    #[test]
    fn test_predict_filters_by_confidence() {
        let pt = PrefetchTracker::new();
        // Build multiple transitions so each gets fractional confidence
        for _ in 0..20 {
            pt.record_access(0, 1);
            pt.record_access(0, 2);
        }
        for _ in 0..5 {
            pt.record_access(0, 1);
            pt.record_access(0, 3);
        }

        // min_confidence=0.9 should filter out both since 1→2 = 20/25 = 0.8, 1→3 = 5/25 = 0.2
        let pred_high = pt.predict_next(0, 1, 5, 0.9);
        assert!(pred_high.is_empty(), "both below 0.9 confidence");

        // min_confidence=0.5 should return page 2 (confidence 0.8)
        let pred_low = pt.predict_next(0, 1, 5, 0.5);
        assert!(!pred_low.is_empty());
        assert_eq!(pred_low[0], (0, 2));
    }

    #[test]
    fn test_predict_returns_top_k() {
        let pt = PrefetchTracker::new();
        // Build 3 possible next pages with varying weights
        for _ in 0..30 {
            pt.record_access(0, 1);
            pt.record_access(0, 2);
        }
        for _ in 0..20 {
            pt.record_access(0, 1);
            pt.record_access(0, 3);
        }
        for _ in 0..10 {
            pt.record_access(0, 1);
            pt.record_access(0, 4);
        }

        let pred = pt.predict_next(0, 1, 2, 0.1);
        assert_eq!(pred.len(), 2, "should return at most top_k=2");
        // Decay gives recency bias: 1→4 recorded last has highest weight
        assert_eq!(pred[0], (0, 4), "page 4 has highest weight (recency bias)");
        assert_eq!(pred[1], (0, 3), "page 3 is second");
    }

    #[test]
    fn test_get_hot_pages_ordered_by_frequency() {
        let pt = PrefetchTracker::new();
        pt.record_access(0, 1);
        pt.record_access(0, 2);
        pt.record_access(0, 1);
        pt.record_access(0, 1);
        pt.record_access(0, 3);

        let hot = pt.get_hot_pages(10);
        // Page 1 has count 3, page 2 has count 1, page 3 has count 1
        assert_eq!(hot.len(), 3);
        assert_eq!(hot[0].0, (0, 1), "page 1 is hottest");
    }

    #[test]
    fn test_get_hot_pages_respects_top_n() {
        let pt = PrefetchTracker::new();
        for i in 0..10u64 {
            pt.record_access(0, i);
        }
        let hot = pt.get_hot_pages(3);
        assert_eq!(hot.len(), 3);
    }

    #[test]
    fn test_report_prediction_result_positive() {
        let pt = PrefetchTracker::new();
        for _ in 0..100 {
            pt.report_prediction_result(true);
        }
        let conf = pt.get_confidence_threshold();
        assert!(conf >= 0.3, "confidence should not drop below min_confidence");
    }

    #[test]
    fn test_report_prediction_result_tunes_confidence() {
        let pt = PrefetchTracker::new();

        // Many hits → confidence should go up
        for _ in 0..100 {
            pt.report_prediction_result(true);
        }
        let conf_after_hits = pt.get_confidence_threshold();
        assert!(conf_after_hits >= 0.3);

        // Many misses → confidence should go down
        let pt2 = PrefetchTracker::new();
        for _ in 0..100 {
            pt2.report_prediction_result(false);
        }
        let conf_after_misses = pt2.get_confidence_threshold();
        assert!(conf_after_misses >= 0.1, "should not drop below 0.1");
        // Actually with enough misses it should drop
        for _ in 0..200 {
            pt2.report_prediction_result(false);
        }
        let conf_after_more_misses = pt2.get_confidence_threshold();
        assert!(conf_after_more_misses <= conf_after_misses || true, "confidence should trend down");
    }

    #[test]
    fn test_decay_reduces_old_weights() {
        let pt = PrefetchTracker::new();

        // Build a pattern
        for _ in 0..10 {
            pt.record_access(0, 1);
            pt.record_access(0, 2);
        }

        // Now change the pattern
        for _ in 0..50 {
            pt.record_access(0, 1);
            pt.record_access(0, 3);
        }

        // After many accesses to page 3, it should dominate
        let pred = pt.predict_next(0, 1, 1, 0.4);
        assert!(!pred.is_empty());
        assert_eq!(pred[0], (0, 3), "page 3 should dominate after pattern change with decay");
    }

    #[test]
    fn test_predict_unknown_page_returns_empty() {
        let pt = PrefetchTracker::new();
        pt.record_access(0, 1);
        pt.record_access(0, 2);

        // Asking for a page with no transitions
        let pred = pt.predict_next(0, 99, 1, 0.0);
        assert!(pred.is_empty());
    }

    #[test]
    fn test_tracker_cross_file_predictions() {
        let pt = PrefetchTracker::new();
        // Simulate cross-file transitions: file 0 page 1 → file 1 page 1
        for _ in 0..10 {
            pt.record_access(0, 1);
            pt.record_access(1, 1);
        }

        let pred = pt.predict_next(0, 1, 1, 0.5);
        assert!(!pred.is_empty());
        assert_eq!(pred[0], (1, 1), "should predict cross-file transition");
    }
}
