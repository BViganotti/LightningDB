use crate::planner::binder::BoundOrderByItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;

use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// NWayMerge merges multiple sorted streams from parallel sort workers
/// into a single sorted output. Each child is a PhysicalSort clone that
/// has collected and sorted its partition of the data. NWayMerge reads
/// the sorted result from each child and performs a final merge.
pub struct NWayMerge {
    children: Vec<Box<dyn PhysicalOperator + Send + Sync>>,
    order_by: Vec<BoundOrderByItem>,
    merged: bool,
}

impl NWayMerge {
    pub fn new(children: Vec<Box<dyn PhysicalOperator + Send + Sync>>, order_by: Vec<BoundOrderByItem>) -> Self {
        Self {
            children,
            order_by,
            merged: false,
        }
    }
}

impl PhysicalOperator for NWayMerge {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.merged {
            return Ok(None);
        }
        self.merged = true;

        // Collect the sorted result from each child (each is a PhysicalSort that
        // has already collected and sorted its partition)
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for child in &mut self.children {
            while let Some(chunk) = child.get_next(database, tx, params)? {
                all_batches.push(chunk.batch);
            }
        }

        if all_batches.is_empty() {
            return Ok(None);
        }

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(None);
        }

        // Concatenate all batches into one
        let num_cols = all_batches[0].schema().fields().len();
        let mut data_arrays: Vec<arrow::array::ArrayRef> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let col_arrays: Vec<&arrow::array::ArrayRef> = all_batches.iter()
                .map(|b| b.column(col_idx))
                .collect();
            let col_refs: Vec<&dyn arrow::array::Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = arrow::compute::concat(&col_refs)
                .map_err(|e| crate::LightningError::Internal(format!("NWayMerge concat: {e}")))?;
            data_arrays.push(concatenated);
        }
        let merged_batch = RecordBatch::try_new(all_batches[0].schema(), data_arrays.clone())
            .map_err(|e| crate::LightningError::Internal(format!("NWayMerge batch: {e}")))?;

        // Compute sort columns on the merged batch
        let merged_rows = merged_batch.num_rows();
        let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(self.order_by.len());
        for item in &self.order_by {
            let array = ExpressionEvaluator::evaluate(
                &item.expression,
                Some(&merged_batch),
                params,
                merged_rows,
                &database.function_registry,
                database,
            )?;
            sort_cols.push(SortColumn {
                values: array,
                options: Some(SortOptions {
                    descending: item.descending,
                    nulls_first: true,
                }),
            });
        }

        // Sort the merged data
        let indices = lexsort_to_indices(&sort_cols, None)
            .map_err(|e| crate::LightningError::Internal(format!("NWayMerge sort: {e}")))?;

        let sorted_columns: Vec<arrow::array::ArrayRef> = data_arrays.iter()
            .map(|col| {
                take(col, &indices, None)
                    .map_err(|e| crate::LightningError::Internal(format!("NWayMerge take: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        let sorted_batch = RecordBatch::try_new(all_batches[0].schema(), sorted_columns)
            .map_err(|e| crate::LightningError::Internal(format!("NWayMerge batch: {e}")))?;

        Ok(Some(DataChunk { batch: sorted_batch }))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            children: self.children.iter().map(|c| c.clone_box()).collect(),
            order_by: self.order_by.clone(),
            merged: false,
        })
    }
}
