use crate::planner::binder::BoundTransactionAction;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct PhysicalTransaction {
    action: BoundTransactionAction,
    executed: bool,
}

impl PhysicalTransaction {
    pub fn new(action: BoundTransactionAction) -> Self {
        Self {
            action,
            executed: false,
        }
    }
}

impl PhysicalOperator for PhysicalTransaction {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let bm = &database.buffer_manager;
        match self.action {
            BoundTransactionAction::Commit => {
                database.transaction_manager.commit(tx, bm, database)?;
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec!["Transaction committed"]))],
                )
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                Ok(Some(DataChunk::new(batch)))
            }
            BoundTransactionAction::Rollback => {
                database.transaction_manager.rollback(database, tx)?;
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec!["Transaction rolled back"]))],
                )
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                Ok(Some(DataChunk::new(batch)))
            }
            BoundTransactionAction::Begin => {
                // BEGIN within an operator context starts a nested transaction.
                // The explicit transaction is managed by the Connection layer.
                // This operator is reached when a query contains an explicit BEGIN
                // statement. The Connection layer already created a transaction for
                // the query execution, so we signal success.
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(vec!["Transaction started"]))],
                )
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                Ok(Some(DataChunk::new(batch)))
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(self.clone())
    }
}
