use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Database;
use crate::Result;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// EXPLAIN: returns the formatted logical plan as a single-row result WITHOUT
/// executing the underlying query. (`PROFILE` is the analyze variant, which
/// both executes and reports runtime statistics.)
pub struct PhysicalExplain {
    plan: String,
    emitted: bool,
}

impl PhysicalExplain {
    pub fn new(plan: String) -> Self {
        Self {
            plan,
            emitted: false,
        }
    }
}

impl PhysicalOperator for PhysicalExplain {
    fn get_next(
        &mut self,
        _database: &Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let schema = Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, false)]));
        let array = Arc::new(StringArray::from(vec![self.plan.clone()]));
        let batch = RecordBatch::try_new(schema, vec![array])
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(Some(DataChunk { batch }))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            plan: self.plan.clone(),
            emitted: self.emitted,
        })
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
