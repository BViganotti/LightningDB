use crate::planner::binder::BoundExpression;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use arrow::array::{Array, AsArray, BooleanArray};
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
            let mask_raw = eval_res.as_boolean();

            // Arrow's filter_record_batch reads only the data bits of the
            // BooleanArray mask, ignoring null bits. This means null entries
            // in the mask (produced by comparisons like null = true) would
            // be treated as "true" (their underlying data bit value), causing
            // rows with null comparisons to pass the filter incorrectly.
            // Replace nulls with false to ensure correct filtering.
            let mask: &BooleanArray = if mask_raw.null_count() > 0 {
                &mask_raw.iter().map(|v| v.unwrap_or(false)).collect::<BooleanArray>()
            } else {
                mask_raw
            };

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

    fn is_parallel_safe(&self) -> bool {
        self.child.is_parallel_safe()
    }

    fn set_partition(&mut self, index: usize, total: usize) {
        self.child.set_partition(index, total);
    }
}
