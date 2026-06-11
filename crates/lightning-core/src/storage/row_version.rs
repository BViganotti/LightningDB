use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct RowVersionShard {
    versions: RwLock<HashMap<u64, u64>>,  // row_id -> tx_id
    committed: RwLock<HashMap<u64, u64>>, // row_id -> commit_ts
}

pub struct RowVersion {
    shards: Vec<RowVersionShard>,
    num_shards: usize,
    dirty_flag: AtomicBool,
}

impl Default for RowVersion {
    fn default() -> Self {
        Self::new()
    }
}

impl RowVersion {
    pub fn new() -> Self {
        let num_shards = 16;
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(RowVersionShard {
                versions: RwLock::new(HashMap::new()),
                committed: RwLock::new(HashMap::new()),
            });
        }
        Self { shards, num_shards, dirty_flag: AtomicBool::new(false) }
    }

    fn get_shard_idx(&self, row_id: u64) -> usize {
        let mut h = row_id;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        (h as usize) % self.num_shards
    }

    pub fn mark_row(&self, row_id: u64, tx_id: u64, _read_ts: u64) -> Result<(), String> {
        let shard_idx = self.get_shard_idx(row_id);
        let mut versions = self.shards[shard_idx].versions.write();
        if let Some(&existing_tx) = versions.get(&row_id) {
            if existing_tx != tx_id {
                return Err(format!(
                    "Write-Write Conflict: Row {row_id} already modified by active tx {existing_tx}"
                ));
            }
        }
        let committed = self.shards[shard_idx].committed.read();
        if let Some(&commit_ts) = committed.get(&row_id) {
            if commit_ts > _read_ts {
                return Err(format!(
                    "Write-Write Conflict: Row {row_id} modified by committed tx at {commit_ts}"
                ));
            }
        }
        versions.insert(row_id, tx_id);
        self.dirty_flag.store(true, Ordering::Release);
        Ok(())
    }

    pub fn mark_row_batch(&self, row_ids: std::ops::Range<u64>, tx_id: u64) {
        // Optimization: Group row IDs by shard to minimize lock acquisitions
        let mut shard_updates: Vec<Vec<u64>> = vec![Vec::new(); self.num_shards];
        for row_id in row_ids {
            shard_updates[self.get_shard_idx(row_id)].push(row_id);
        }

        for (shard_idx, ids) in shard_updates.into_iter().enumerate() {
            if ids.is_empty() {
                continue;
            }
            let mut versions = self.shards[shard_idx].versions.write();
            for row_id in ids {
                versions.insert(row_id, tx_id);
            }
            self.dirty_flag.store(true, Ordering::Release);
        }
    }

    pub fn commit_row(&self, row_id: u64, commit_ts: u64) {
        let shard_idx = self.get_shard_idx(row_id);
        let mut versions = self.shards[shard_idx].versions.write();
        versions.remove(&row_id);
        self.shards[shard_idx]
            .committed
            .write()
            .insert(row_id, commit_ts);
    }

    pub fn commit_row_batch(&self, row_ids: std::ops::Range<u64>, commit_ts: u64) {
        let mut shard_updates: Vec<Vec<u64>> = vec![Vec::new(); self.num_shards];
        for row_id in row_ids {
            shard_updates[self.get_shard_idx(row_id)].push(row_id);
        }

        for shard_idx in 0..self.num_shards {
            let ids = &shard_updates[shard_idx];
            if ids.is_empty() {
                continue;
            }
            let mut versions = self.shards[shard_idx].versions.write();
            let mut committed = self.shards[shard_idx].committed.write();
            for row_id in ids {
                versions.remove(row_id);
                committed.insert(*row_id, commit_ts);
            }
        }
    }

    pub fn rollback_row(&self, row_id: u64) {
        let shard_idx = self.get_shard_idx(row_id);
        self.shards[shard_idx].versions.write().remove(&row_id);
    }

    pub fn is_visible(&self, row_id: u64, tx_id: u64, read_ts: u64) -> bool {
        let shard_idx = self.get_shard_idx(row_id);
        let versions = self.shards[shard_idx].versions.read();
        if let Some(&mod_tx) = versions.get(&row_id) {
            if mod_tx == tx_id {
                return true;
            }
            // Row is being modified by another uncommitted tx — not visible to us
            drop(versions);
            let committed = self.shards[shard_idx].committed.read();
            return committed.get(&row_id).map_or(false, |&commit_ts| commit_ts <= read_ts);
        }
        let committed = self.shards[shard_idx].committed.read();
        if let Some(&commit_ts) = committed.get(&row_id) {
            return commit_ts <= read_ts;
        }
        true
    }

    pub fn get_visibility_mask(
        &self,
        row_ids: &[u64],
        tx_id: u64,
        read_ts: u64,
        mask: &mut Vec<bool>,
    ) {
        // Optimization: Pre-allocate and batch shard access
        mask.reserve(row_ids.len());

        // Group row IDs by shard to minimize lock acquisitions
        let mut shard_groups: Vec<Vec<(usize, u64)>> = vec![Vec::new(); self.num_shards];
        for (i, &row_id) in row_ids.iter().enumerate() {
            shard_groups[self.get_shard_idx(row_id)].push((i, row_id));
        }

        // Initialize mask with all visible
        mask.resize(row_ids.len(), true);

        for shard_idx in 0..self.num_shards {
            let group = &shard_groups[shard_idx];
            if group.is_empty() {
                continue;
            }

            let versions = self.shards[shard_idx].versions.read();
            let committed = self.shards[shard_idx].committed.read();

            for (idx, row_id) in group {
                let mut visible = true;
                if let Some(&mod_tx) = versions.get(row_id) {
                    if mod_tx != tx_id {
                        visible = false;
                    }
                } else if let Some(&commit_ts) = committed.get(row_id) {
                    visible = commit_ts <= read_ts;
                }
                mask[*idx] = visible;
            }
        }
    }

    pub fn has_modifications(&self) -> bool {
        if !self.dirty_flag.load(Ordering::Acquire) {
            return false;
        }
        for shard in &self.shards {
            let versions = shard.versions.read();
            if !versions.is_empty() {
                return true;
            }
            drop(versions);
            let committed = shard.committed.read();
            if !committed.is_empty() {
                return true;
            }
        }
        false
    }

    pub fn has_committed(&self) -> bool {
        for shard in &self.shards {
            if !shard.committed.read().is_empty() {
                return true;
            }
        }
        false
    }

    /// Remove committed entries with `commit_ts < min_active_ts`.
    /// These entries are no longer needed because no active transaction
    /// can reference a state older than `min_active_ts`.
    /// Returns the number of removed entries for metrics.
    pub fn vacuum(&self, min_active_ts: u64) -> usize {
        let mut total_removed = 0;
        for shard in &self.shards {
            let mut committed = shard.committed.write();
            let before = committed.len();
            committed.retain(|_, &mut commit_ts| commit_ts >= min_active_ts);
            total_removed += before - committed.len();
        }
        total_removed
    }

    /// Reset the dirty hint after vacuum confirms no pending modifications.
    pub fn clear_dirty_flag(&self) {
        self.dirty_flag.store(false, Ordering::Release);
    }

    /// Force-set the dirty hint (used externally when entries may exist).
    pub fn mark_dirty(&self) {
        self.dirty_flag.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_and_commit_row() {
        let rv = RowVersion::new();
        assert!(!rv.has_modifications());

        rv.mark_row(42, 100, 0).unwrap();
        assert!(rv.has_modifications());
        assert!(rv.has_committed() == false);

        rv.commit_row(42, 200);
        assert!(rv.has_committed());
        // has_modifications includes committed rows (used for read-path optimization)
        assert!(rv.has_modifications());
    }

    #[test]
    fn test_mark_and_rollback_row() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        assert!(rv.has_modifications());

        rv.rollback_row(42);
        assert!(!rv.has_modifications());
        assert!(!rv.has_committed());
    }

    #[test]
    fn test_is_visible_self_modification() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        // Same tx should see its own modifications
        assert!(rv.is_visible(42, 100, 0));
    }

    #[test]
    fn test_is_visible_other_tx_uncommitted() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        // Different tx should NOT see uncommitted modification
        assert!(!rv.is_visible(42, 200, 0));
    }

    #[test]
    fn test_is_visible_committed_within_read_ts() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.commit_row(42, 50);
        // read_ts=100 >= commit_ts=50 → visible
        assert!(rv.is_visible(42, 200, 100));
    }

    #[test]
    fn test_is_visible_committed_after_read_ts() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.commit_row(42, 200);
        // read_ts=100 < commit_ts=200 → NOT visible
        assert!(!rv.is_visible(42, 300, 100));
    }

    #[test]
    fn test_is_visible_no_record() {
        let rv = RowVersion::new();
        // No record → visible by default
        assert!(rv.is_visible(42, 100, 0));
    }

    #[test]
    fn test_write_write_conflict_same_tx_ok() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        // Same tx marking again should succeed
        assert!(rv.mark_row(42, 100, 0).is_ok());
    }

    #[test]
    fn test_write_write_conflict_different_tx() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        // Different tx should fail
        let result = rv.mark_row(42, 200, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-Write Conflict"));
    }

    #[test]
    fn test_write_write_conflict_after_commit() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.commit_row(42, 50);
        // Different tx, read_ts=0 < commit_ts=50 → conflict
        let result = rv.mark_row(42, 200, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-Write Conflict"));
    }

    #[test]
    fn test_write_after_committed_before_read_ts() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.commit_row(42, 50);
        // read_ts=100 >= commit_ts=50 → no conflict
        assert!(rv.mark_row(42, 200, 100).is_ok());
    }

    #[test]
    fn test_mark_row_batch() {
        let rv = RowVersion::new();
        rv.mark_row_batch(0..100, 100);
        assert!(rv.has_modifications());

        for row_id in 0..100u64 {
            assert!(rv.is_visible(row_id, 100, 0));
        }
    }

    #[test]
    fn test_commit_row_batch() {
        let rv = RowVersion::new();
        rv.mark_row_batch(0..100, 100);
        rv.commit_row_batch(0..100, 200);
        // has_modifications includes committed rows (used for read-path optimization)
        assert!(rv.has_modifications());
        assert!(rv.has_committed());

        for row_id in 0..100u64 {
            assert!(rv.is_visible(row_id, 300, 300));
        }
    }

    #[test]
    fn test_commit_row_batch_mixed_visibility() {
        let rv = RowVersion::new();
        rv.mark_row_batch(0..50, 100);
        rv.commit_row_batch(0..50, 200);

        // Batch with commit_ts=200, read_ts=100 → NOT visible
        for row_id in 0..50u64 {
            assert!(!rv.is_visible(row_id, 300, 100));
        }
        // read_ts=300 → visible
        for row_id in 0..50u64 {
            assert!(rv.is_visible(row_id, 300, 300));
        }

        // Unmarked rows should be visible
        assert!(rv.is_visible(999, 300, 0));
    }

    #[test]
    fn test_rollback_row_removes_mark() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.rollback_row(42);
        // After rollback, row should be visible by default
        assert!(rv.is_visible(42, 200, 0));
    }

    #[test]
    fn test_get_visibility_mask_all_visible() {
        let rv = RowVersion::new();
        let row_ids = vec![1, 2, 3, 4, 5];
        let mut mask = Vec::new();
        rv.get_visibility_mask(&row_ids, 100, 0, &mut mask);
        assert_eq!(mask, vec![true, true, true, true, true]);
    }

    #[test]
    fn test_get_visibility_mask_mixed() {
        let rv = RowVersion::new();
        rv.mark_row(2, 50, 0).unwrap(); // uncommitted by tx 50
        rv.commit_row(4, 200); // committed at 200

        let row_ids = vec![1, 2, 3, 4, 5];
        let mut mask = Vec::new();
        // tx=100, read_ts=150
        rv.get_visibility_mask(&row_ids, 100, 150, &mut mask);
        // row 2: uncommitted by tx 50 (not us) → invisible
        assert_eq!(mask[1], false);
        // row 4: committed at 200 > read_ts 150 → invisible
        assert_eq!(mask[3], false);
        // rows 1, 3, 5: no record → visible
        assert_eq!(mask[0], true);
        assert_eq!(mask[2], true);
        assert_eq!(mask[4], true);
    }

    #[test]
    fn test_get_visibility_mask_self_uncommitted() {
        let rv = RowVersion::new();
        rv.mark_row(2, 100, 0).unwrap(); // our own uncommitted

        let row_ids = vec![1, 2, 3];
        let mut mask = Vec::new();
        rv.get_visibility_mask(&row_ids, 100, 0, &mut mask);
        // row 2: our own modification → visible
        assert_eq!(mask, vec![true, true, true]);
    }

    #[test]
    fn test_vacuum_removes_old_entries() {
        let rv = RowVersion::new();
        rv.mark_row(1, 100, 0).unwrap();
        rv.commit_row(1, 50);
        rv.mark_row(2, 100, 0).unwrap();
        rv.commit_row(2, 100);
        rv.mark_row(3, 100, 0).unwrap();
        rv.commit_row(3, 200);

        // Vacuum: remove entries with commit_ts < 150
        let removed = rv.vacuum(150);
        assert_eq!(removed, 2, "rows 1 (ts=50) and 2 (ts=100) should be removed");

        // Row 3 still visible (commit_ts=200 >= 150)
        assert!(rv.is_visible(3, 300, 300));
        // Row 2 is gone — no record → visible by default
        assert!(rv.is_visible(2, 300, 300));
    }

    #[test]
    fn test_vacuum_noop_when_nothing_to_remove() {
        let rv = RowVersion::new();
        rv.mark_row(1, 100, 0).unwrap();
        rv.commit_row(1, 200);

        let removed = rv.vacuum(50);
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_has_modifications_false_when_dirty_but_empty() {
        // edge case: dirty flag was set but all entries were cleaned up
        let rv = RowVersion::new();
        rv.mark_dirty();
        // has_modifications should scan and find nothing
        assert!(!rv.has_modifications());
    }

    #[test]
    fn test_clear_dirty_flag() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        assert!(rv.dirty_flag.load(Ordering::Acquire));
        rv.rollback_row(42);
        rv.clear_dirty_flag();
        assert!(!rv.dirty_flag.load(Ordering::Acquire));
        assert!(!rv.has_modifications());
    }

    #[test]
    fn test_has_committed_false_after_rollback() {
        let rv = RowVersion::new();
        rv.mark_row(42, 100, 0).unwrap();
        rv.rollback_row(42);
        assert!(!rv.has_committed());
    }

    #[test]
    fn test_multiple_rows_independent() {
        let rv = RowVersion::new();
        rv.mark_row(1, 100, 0).unwrap();
        rv.mark_row(2, 100, 0).unwrap();
        rv.mark_row(3, 100, 0).unwrap();

        rv.commit_row(1, 200);
        rv.rollback_row(2);

        assert!(rv.is_visible(1, 300, 300));
        assert!(rv.is_visible(2, 300, 300)); // rolled back → visible by default
        assert!(rv.is_visible(3, 100, 0));   // still uncommitted by us
    }

    #[test]
    fn test_shard_distribution_covers_all_shards() {
        let rv = RowVersion::new();
        let mut used_shards = std::collections::HashSet::new();

        // Mark rows across a wide range to hit all shards
        for row_id in 0..1000u64 {
            used_shards.insert(rv.get_shard_idx(row_id));
        }

        assert_eq!(used_shards.len(), 16, "all 16 shards should see use");
    }

    #[test]
    fn test_default_trait() {
        let rv: RowVersion = Default::default();
        assert_eq!(rv.num_shards, 16);
        assert!(!rv.has_modifications());
    }
}
