use crate::processor::{DataChunk, PhysicalOperator};
use crate::Value;
use crate::Result;
use arrow::array::*;
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::Hash;
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

    fn hash_row(column: &dyn Array, row: usize) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match column.data_type() {
            DataType::UInt8 => {
                let arr = column.as_any().downcast_ref::<UInt8Array>().expect("partitioner UInt8");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::UInt16 => {
                let arr = column.as_any().downcast_ref::<UInt16Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::UInt32 => {
                let arr = column.as_any().downcast_ref::<UInt32Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::UInt64 => {
                let arr = column.as_any().downcast_ref::<UInt64Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Int8 => {
                let arr = column.as_any().downcast_ref::<Int8Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0i64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Int16 => {
                let arr = column.as_any().downcast_ref::<Int16Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0i64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Int32 => {
                let arr = column.as_any().downcast_ref::<Int32Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0i64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Int64 => {
                let arr = column.as_any().downcast_ref::<Int64Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0i64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Float32 => {
                let arr = column.as_any().downcast_ref::<Float32Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).to_bits().hash(&mut hasher); }
            }
            DataType::Float64 => {
                let arr = column.as_any().downcast_ref::<Float64Array>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).to_bits().hash(&mut hasher); }
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let arr = column.as_any().downcast_ref::<StringArray>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            DataType::Boolean => {
                let arr = column.as_any().downcast_ref::<BooleanArray>().expect("partitioner downcast matches data_type arm");
                if arr.is_null(row) { 0u64.hash(&mut hasher); } else { arr.value(row).hash(&mut hasher); }
            }
            _ => {
                // Fallback: hash raw bytes
                let data = column.to_data();
                let byte_slice = data.buffers().iter().flat_map(|b| b.as_slice()).cloned().collect::<Vec<_>>();
                byte_slice.hash(&mut hasher);
            }
        }
        std::hash::Hasher::finish(&hasher)
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

        let key_column = batch.column(self.key_idx);

        // 1. Calculate partition for each row using column-based hashing
        for i in 0..num_rows {
            let hash = Self::hash_row(key_column, i);
            let p_idx = (hash as usize) % self.state.num_partitions;
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
