use crate::planner::binder::{BoundExpression, BoundProjectionItem};
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
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
            let mut projected_columns: Vec<ArrayRef> = Vec::new();
            let mut fields: Vec<Field> = Vec::new();
            let mut expr_cache: HashMap<u64, ArrayRef> = HashMap::new();

            for item in &self.items {
                let expr_hash = expression_hash(&item.expression);
                let array = if let Some(cached) = expr_cache.get(&expr_hash) {
                    cached.clone()
                } else {
                    let arr = ExpressionEvaluator::evaluate(
                        &item.expression,
                        Some(&chunk.batch),
                        params,
                        num_rows,
                        &database.function_registry,
                        database,
                    )?;
                    expr_cache.insert(expr_hash, arr.clone());
                    arr
                };
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

    fn is_parallel_safe(&self) -> bool {
        true
    }

    fn set_partition(&mut self, index: usize, total: usize) {
        self.child.set_partition(index, total);
    }
}

fn expression_hash(expr: &BoundExpression) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    expr.hash(&mut hasher);
    hasher.finish()
}
