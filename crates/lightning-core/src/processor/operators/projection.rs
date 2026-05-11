use crate::planner::binder::BoundProjectionItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct PhysicalProjection {
    child: Box<dyn PhysicalOperator>,
    items: Vec<BoundProjectionItem>,
}

impl PhysicalProjection {
    pub fn new(child: Box<dyn PhysicalOperator>, items: Vec<BoundProjectionItem>) -> Self {
        Self { child, items }
    }
}

impl PhysicalOperator for PhysicalProjection {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.child.get_next(database, tx, params)? {
            let num_rows = chunk.num_rows();
            let mut projected_columns = Vec::new();
            let mut fields = Vec::new();

            for item in &self.items {
                let array = ExpressionEvaluator::evaluate(
                    &item.expression,
                    Some(&chunk.batch),
                    params,
                    num_rows,
                    &database.function_registry,
                    database,
                )?;
                fields.push(Field::new(&item.alias, array.data_type().clone(), true));
                projected_columns.push(array);
            }

            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(schema, projected_columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

            return Ok(Some(DataChunk { batch }));
        }
        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            items: self.items.clone(),
        })
    }
}
