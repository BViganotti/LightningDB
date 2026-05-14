use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::UInt32Builder;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PartitionedState {
    pub partitions: Vec<Mutex<Vec<RecordBatch>>>,
    pub num_partitions: usize,
}

pub struct PhysicalPartitioner {
    child: Box<dyn PhysicalOperator>,
    key_idx: usize,
    state: Arc<PartitionedState>,
    executed: bool,
}

impl PhysicalPartitioner {
    pub fn new(child: Box<dyn PhysicalOperator>, key_idx: usize, num_partitions: usize) -> Self {
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitions.push(Mutex::new(Vec::new()));
        }
        Self {
            child,
            key_idx,
            state: Arc::new(PartitionedState {
                partitions,
                num_partitions,
            }),
            executed: false,
        }
    }

    fn partition_batch(&self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let mut partition_indices = Vec::with_capacity(self.state.num_partitions);
        for _ in 0..self.state.num_partitions {
            partition_indices.push(UInt32Builder::with_capacity(
                num_rows / self.state.num_partitions + 1,
            ));
        }

        // 1. Calculate partition for each row
        for i in 0..num_rows {
            let key = Value::from_arrow(batch.column(self.key_idx), i);
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            use std::hash::Hash;
            key.hash(&mut hasher);
            let p_idx = (std::hash::Hasher::finish(&hasher) as usize) % self.state.num_partitions;
            partition_indices[p_idx].append_value(i as u32);
        }

        // 2. Create sub-batches for each partition
        for (p_idx, mut builder) in partition_indices.into_iter().enumerate() {
            let indices = builder.finish();
            if indices.is_empty() {
                continue;
            }

            let mut partitioned_columns = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let partitioned_col = take(col.as_ref(), &indices, None)?;
                partitioned_columns.push(partitioned_col);
            }

            let partitioned_batch = RecordBatch::try_new(batch.schema(), partitioned_columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

            self.state.partitions[p_idx].lock().push(partitioned_batch);
        }

        Ok(())
    }
}

impl PhysicalOperator for PhysicalPartitioner {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.executed {
            self.executed = true;
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                self.partition_batch(chunk.batch)?;
            }
        }

        // Emit each partition in sequence
        for p_idx in 0..self.state.num_partitions {
            let mut guard = self.state.partitions[p_idx].lock();
            if let Some(batch) = guard.pop() {
                return Ok(Some(DataChunk { batch }));
            }
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            key_idx: self.key_idx,
            state: self.state.clone(),
            executed: self.executed,
        })
    }
}
