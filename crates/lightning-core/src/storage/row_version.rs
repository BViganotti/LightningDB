use parking_lot::RwLock;
use std::collections::HashMap;

pub struct RowVersionShard {
    versions: RwLock<HashMap<u64, u64>>,  // row_id -> tx_id
    committed: RwLock<HashMap<u64, u64>>, // row_id -> commit_ts
}

pub struct RowVersion {
    shards: Vec<RowVersionShard>,
    num_shards: usize,
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
        Self { shards, num_shards }
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
}
