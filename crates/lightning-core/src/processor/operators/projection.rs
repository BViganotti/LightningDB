use crate::planner::binder::BoundProjectionItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use arrow::array::ArrayRef;
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
            let num_items = self.items.len();
            let mut projected_columns: Vec<ArrayRef> = Vec::with_capacity(num_items);
            let mut fields: Vec<Field> = Vec::with_capacity(num_items);
            let mut evaluated: Vec<Option<ArrayRef>> = vec![None; num_items];

            // Deduplicate identical expressions to avoid redundant evaluation.
            // Uses Debug format for comparison — this is a heuristic that works
            // for common cases (PropertyLookup, Function) but may have false
            // positives for expressions with identical debug representations.
            // A production implementation would use structural equality.
            let expr_debugs: Vec<String> = self.items
                .iter()
                .map(|item| format!("{:?}", item.expression))
                .collect();
            for i in 0..num_items {
                if evaluated[i].is_some() {
                    continue;
                }
                let arr = ExpressionEvaluator::evaluate(
                    &self.items[i].expression,
                    Some(&chunk.batch),
                    params,
                    num_rows,
                    &database.function_registry,
                    database,
                )?;
                evaluated[i] = Some(arr.clone());
                for j in (i + 1)..num_items {
                    if expr_debugs[j] == expr_debugs[i] {
                        evaluated[j] = Some(arr.clone());
                    }
                }
            }

            for i in 0..num_items {
                let array = evaluated[i].clone().ok_or_else(|| {
                    crate::LightningError::Internal(format!("Expression evaluation slot {} was not populated", i))
                })?;
                fields.push(Field::new(&self.items[i].alias, array.data_type().clone(), true));
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

    fn is_parallel_safe(&self) -> bool {
        self.child.is_parallel_safe()
    }

    fn set_partition(&mut self, index: usize, total: usize) {
        self.child.set_partition(index, total);
    }
}
