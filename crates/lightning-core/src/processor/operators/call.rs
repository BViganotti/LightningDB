use crate::planner::binder::BoundCallClause as BoundCall;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalCall {
    call: BoundCall,
    executed: bool,
}

impl PhysicalCall {
    pub fn new(call: BoundCall) -> Self {
        Self {
            call,
            executed: false,
        }
    }
}

impl PhysicalOperator for PhysicalCall {
    fn get_next(
        &mut self,
        _database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        if self.call.procedure_name.to_lowercase() == "show_tables" {
            // Placeholder: return an empty list or some mock data
            let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
            return Ok(Some(DataChunk {
                batch: RecordBatch::new_empty(schema),
            }));
        }

        Err(crate::LightningError::Internal(format!(
            "Procedure {} not found",
            self.call.procedure_name
        )))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            call: self.call.clone(),
            executed: self.executed,
        })
    }
}
