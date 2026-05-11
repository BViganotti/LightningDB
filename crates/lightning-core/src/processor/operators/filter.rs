use crate::planner::binder::BoundExpression;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use arrow::array::AsArray;
use arrow::compute::filter_record_batch;

pub struct PhysicalFilter {
    child: Box<dyn PhysicalOperator>,
    expression: BoundExpression,
}

impl PhysicalFilter {
    pub fn new(child: Box<dyn PhysicalOperator>, expression: BoundExpression) -> Self {
        Self { child, expression }
    }
}

impl PhysicalOperator for PhysicalFilter {
    fn get_next(
        &mut self,
        database: &crate::Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Option<DataChunk>> {
        while let Some(chunk) = self.child.get_next(database, _tx, params)? {
            let num_rows = chunk.num_rows();
            if num_rows == 0 {
                continue;
            }

            let eval_res = ExpressionEvaluator::evaluate(
                &self.expression,
                Some(&chunk.batch),
                params,
                num_rows,
                &database.function_registry,
                database,
            )?;
            let mask = eval_res.as_boolean();

            let filtered_batch = filter_record_batch(&chunk.batch, mask)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

            if filtered_batch.num_rows() > 0 {
                return Ok(Some(DataChunk {
                    batch: filtered_batch,
                }));
            }
        }
        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            expression: self.expression.clone(),
        })
    }
}
