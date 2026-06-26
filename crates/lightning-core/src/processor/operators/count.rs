use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalCount {
    count: i64,
    done: bool,
}

impl PhysicalCount {
    pub fn new(count: i64) -> Self {
        Self { count, done: false }
    }
}

impl PhysicalOperator for PhysicalCount {
    fn get_next(
        &mut self,
        _database: &crate::Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let arr = Int64Array::from(vec![self.count]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count(*)",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)])
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(Some(DataChunk::new(batch)))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            count: self.count,
            done: false,
        })
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
