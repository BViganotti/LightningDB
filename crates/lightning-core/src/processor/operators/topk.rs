use crate::planner::binder::BoundOrderByItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct PhysicalTopK {
    child: Box<dyn PhysicalOperator>,
    order_by: Vec<BoundOrderByItem>,
    limit: u64,
    collected: bool,
    result_batch: Option<RecordBatch>,
}

impl PhysicalTopK {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        order_by: Vec<BoundOrderByItem>,
        limit: u64,
    ) -> Self {
        Self {
            child,
            order_by,
            limit,
            collected: false,
            result_batch: None,
        }
    }
}

impl PhysicalOperator for PhysicalTopK {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.collected {
            self.collected = true;
            let mut all_batches = Vec::new();
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                all_batches.push(chunk.batch);
            }
            if all_batches.is_empty() {
                return Ok(None);
            }

            let full_batch = concat_batches(&all_batches[0].schema(), &all_batches)?;
            if full_batch.num_rows() == 0 {
                return Ok(None);
            }

            // Perform sort and truncate to limit
            let mut sort_columns = Vec::new();
            for item in &self.order_by {
                let val = ExpressionEvaluator::evaluate(
                    &item.expression,
                    Some(&full_batch),
                    params,
                    full_batch.num_rows(),
                    &database.function_registry,
                    database,
                )?;
                sort_columns.push(SortColumn {
                    values: val,
                    options: Some(SortOptions {
                        descending: item.descending,
                        nulls_first: true,
                    }),
                });
            }

            let indices = lexsort_to_indices(&sort_columns, None)?;
            let slice_indices = if indices.len() > self.limit as usize {
                indices.slice(0, self.limit as usize)
            } else {
                indices
            };

            let mut columns = Vec::new();
            for i in 0..full_batch.num_columns() {
                columns.push(take(full_batch.column(i).as_ref(), &slice_indices, None)?);
            }
            let top_k_batch = RecordBatch::try_new(full_batch.schema(), columns)?;
            self.result_batch = Some(top_k_batch);
        }

        if let Some(batch) = self.result_batch.take() {
            return Ok(Some(DataChunk { batch }));
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            collected: self.collected,
            result_batch: self.result_batch.clone(),
        })
    }
}
