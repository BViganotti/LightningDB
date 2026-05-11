use crate::planner::binder::BoundExpression;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct SharedUnwindDedup {
    pub seen_values: HashSet<Value>,
}

pub struct PhysicalUnwindDedup {
    child: Box<dyn PhysicalOperator>,
    key_expr: BoundExpression,
    shared: Arc<RwLock<SharedUnwindDedup>>,
}

impl PhysicalUnwindDedup {
    pub fn new(child: Box<dyn PhysicalOperator>, key_expr: BoundExpression) -> Self {
        Self {
            child,
            key_expr,
            shared: Arc::new(RwLock::new(SharedUnwindDedup {
                seen_values: HashSet::new(),
            })),
        }
    }
}

impl PhysicalOperator for PhysicalUnwindDedup {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            let batch = chunk.batch;
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let key_array = ExpressionEvaluator::evaluate(
                &self.key_expr,
                Some(&batch),
                params,
                num_rows,
                &database.function_registry,
                database,
            )?;

            let mut indices_to_keep = Vec::new();
            {
                let mut shared = self.shared.write();
                for i in 0..num_rows {
                    let val = Value::from_arrow(&key_array, i);
                    if shared.seen_values.insert(val) {
                        indices_to_keep.push(i as u32);
                    }
                }
            }

            if indices_to_keep.is_empty() {
                continue;
            }

            let indices = UInt32Array::from(indices_to_keep);
            let mut columns = Vec::new();
            for i in 0..batch.num_columns() {
                let col = batch.column(i);
                let filtered_col = take(col.as_ref(), &indices, None).map_err(|e| {
                    crate::LightningError::Internal(format!("Arrow take error: {}", e))
                })?;
                columns.push(filtered_col);
            }

            let filtered_batch = RecordBatch::try_new(batch.schema(), columns)
                .map_err(|e| crate::LightningError::Internal(format!("Arrow error: {}", e)))?;

            return Ok(Some(DataChunk {
                batch: filtered_batch,
            }));
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            key_expr: self.key_expr.clone(),
            shared: self.shared.clone(),
        })
    }
}
