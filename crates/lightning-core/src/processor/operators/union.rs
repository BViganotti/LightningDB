use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct SharedUnionState {
    pub seen_hashes: RwLock<HashSet<u64>>,
    /// Rows whose hash collides with another row; stored for full comparison.
    pub collision_rows: RwLock<Vec<(u64, Vec<Value>)>>,
    pub left_exhausted: AtomicBool,
}

pub struct PhysicalUnion {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,
    is_all: bool,
    shared_state: Arc<SharedUnionState>,
    local_left_exhausted: bool,
}

impl PhysicalUnion {
    pub fn new(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        is_all: bool,
    ) -> Self {
        Self {
            left,
            right,
            is_all,
            shared_state: Arc::new(SharedUnionState {
                seen_hashes: RwLock::new(HashSet::new()),
                collision_rows: RwLock::new(Vec::new()),
                left_exhausted: AtomicBool::new(false),
            }),
            local_left_exhausted: false,
        }
    }

    fn deduplicate(&mut self, chunk: DataChunk) -> Result<Option<DataChunk>> {
        let batch = chunk.batch;
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        let mut filtered_indices = Vec::new();
        {
            let mut seen = self.shared_state.seen_hashes.write();
            let mut collisions = self.shared_state.collision_rows.write();
            for i in 0..num_rows {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                for j in 0..num_cols {
                    Value::from_arrow(batch.column(j), i).hash(&mut hasher);
                }
                let hash = hasher.finish();

                if seen.insert(hash) {
                    // Fast path: hash is unique, definitely a new row
                    filtered_indices.push(i as u64);
                } else {
                    // Hash collision: check all collision rows for equality
                    let row: Vec<Value> = (0..num_cols)
                        .map(|j| Value::from_arrow(batch.column(j), i))
                        .collect();
                    let is_dup = collisions.iter().any(|(h, r)| *h == hash && r == &row);
                    if !is_dup {
                        collisions.push((hash, row));
                        filtered_indices.push(i as u64);
                    }
                }
            }
        }

        if filtered_indices.is_empty() {
            return Ok(None);
        }

        if filtered_indices.len() == num_rows {
            return Ok(Some(DataChunk::new(batch)));
        }

        let indices = arrow::array::UInt64Array::from(filtered_indices);
        let mut columns = Vec::new();
        for i in 0..num_cols {
            columns.push(
                arrow::compute::take(batch.column(i), &indices, None)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?,
            );
        }

        let new_batch = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        Ok(Some(DataChunk::new(new_batch)))
    }
}

impl PhysicalOperator for PhysicalUnion {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        // Loop instead of recursion to avoid stack overflow on many consecutive duplicates
        loop {
            if !self.local_left_exhausted {
                if let Some(chunk) = self.left.get_next(database, tx, params)? {
                    if !self.is_all {
                        if let Some(deduped) = self.deduplicate(chunk)? {
                            return Ok(Some(deduped));
                        } else {
                            continue;
                        }
                    }
                    return Ok(Some(chunk));
                }
                self.local_left_exhausted = true;
            }

            if let Some(chunk) = self.right.get_next(database, tx, params)? {
                if !self.is_all {
                    if let Some(deduped) = self.deduplicate(chunk)? {
                        return Ok(Some(deduped));
                    } else {
                        continue;
                    }
                }
                return Ok(Some(chunk));
            } else {
                return Ok(None);
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            left: self.left.clone_box(),
            right: self.right.clone_box(),
            is_all: self.is_all,
            shared_state: self.shared_state.clone(),
            local_left_exhausted: self.local_left_exhausted,
        })
    }
}
