use crate::planner::binder::BoundOrderByItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;

use arrow::array::ArrayRef;
use arrow::compute::SortColumn;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// NWayMerge merges multiple sorted streams from parallel sort workers
/// into a single sorted output using a K-way merge. Each child is a
/// PhysicalSort clone that has collected and sorted its partition.
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

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(na), Value::Number(nb)) => na.partial_cmp(nb).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
        (Value::Boolean(ba), Value::Boolean(bb)) => ba.cmp(bb),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn compare_rows(
    sort_keys: &[Vec<Value>],
    idx_a: usize,
    idx_b: usize,
    order_by: &[BoundOrderByItem],
) -> std::cmp::Ordering {
    for (i, item) in order_by.iter().enumerate() {
        let cmp = compare_values(&sort_keys[i][idx_a], &sort_keys[i][idx_b]);
        let ordered = if item.descending { cmp.reverse() } else { cmp };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
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

        // Collect batches from each child
        let mut child_batches: Vec<Vec<RecordBatch>> = Vec::with_capacity(self.children.len());
        for child in &mut self.children {
            let mut batches = Vec::new();
            while let Some(chunk) = child.get_next(database, tx, params)? {
                batches.push(chunk.batch);
            }
            child_batches.push(batches);
        }

        // Compute total rows and concatenate each child's batches
        let num_children = child_batches.len();
        let mut child_data: Vec<RecordBatch> = Vec::with_capacity(num_children);
        let mut total_rows = 0usize;
        for batches in &child_batches {
            if batches.is_empty() {
                child_data.push(RecordBatch::new_empty(
                    Arc::new(arrow::datatypes::Schema::empty()),
                ));
                continue;
            }
            if batches.len() == 1 {
                let b = batches[0].clone();
                total_rows += b.num_rows();
                child_data.push(b);
            } else {
                let schema = batches[0].schema();
                let num_cols = schema.fields().len();
                let mut cols = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let refs: Vec<&dyn arrow::array::Array> =
                        batches.iter().map(|b| b.column(col_idx).as_ref()).collect();
                    cols.push(arrow::compute::concat(&refs)
                        .map_err(|e| crate::LightningError::Internal(format!("NWayMerge concat child: {e}")))?);
                }
                let b = RecordBatch::try_new(schema, cols)
                    .map_err(|e| crate::LightningError::Internal(format!("NWayMerge child batch: {e}")))?;
                total_rows += b.num_rows();
                child_data.push(b);
            }
        }

        if total_rows == 0 {
            return Ok(None);
        }

        let schema = child_data.iter().find(|b| b.num_rows() > 0)
            .map(|b| b.schema())
            .or_else(|| child_batches.iter().flatten().next().map(|b| b.schema()))
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        let num_cols = schema.fields().len();

        // Pre-compute sort key values for each child as Vec<Value>
        let mut child_sort_keys: Vec<Vec<Vec<Value>>> = Vec::with_capacity(num_children);
        for child_idx in 0..num_children {
            let batch = &child_data[child_idx];
            if batch.num_rows() == 0 {
                child_sort_keys.push(Vec::new());
                continue;
            }
            let mut keys: Vec<Vec<Value>> = Vec::with_capacity(self.order_by.len());
            for item in &self.order_by {
                let array = ExpressionEvaluator::evaluate(
                    &item.expression,
                    Some(batch),
                    params,
                    batch.num_rows(),
                    &database.function_registry,
                    database,
                )?;
                let vals: Vec<Value> = (0..batch.num_rows())
                    .map(|i| Value::from_arrow(&array, i))
                    .collect();
                keys.push(vals);
            }
            child_sort_keys.push(keys);
        }

        // K-way merge: build output column by column
        let mut cursors: Vec<usize> = vec![0; num_children];
        let mut out_arrays: Vec<Vec<Value>> = vec![Vec::with_capacity(total_rows); num_cols];

        for _ in 0..total_rows {
            let mut best_child = None;
            for child_idx in 0..num_children {
                if cursors[child_idx] >= child_data[child_idx].num_rows() {
                    continue;
                }
                match best_child {
                    None => best_child = Some(child_idx),
                    Some(best) => {
                        if compare_rows(&child_sort_keys[child_idx], cursors[child_idx], cursors[best], &self.order_by) == std::cmp::Ordering::Less {
                            best_child = Some(child_idx);
                        }
                    }
                }
            }
            let bc = best_child.unwrap();
            let row = cursors[bc];
            for col_idx in 0..num_cols {
                out_arrays[col_idx].push(Value::from_arrow(child_data[bc].column(col_idx), row));
            }
            cursors[bc] += 1;
        }

        // Convert output columns to arrow arrays
        let arrow_cols: Vec<ArrayRef> = out_arrays.iter().enumerate()
            .map(|(col_idx, vals)| {
                let dt = schema.field(col_idx).data_type();
                crate::processor::arrow_utils::values_to_array(vals, dt)
            })
            .collect();

        let result_batch = RecordBatch::try_new(schema, arrow_cols)
            .map_err(|e| crate::LightningError::Internal(format!("NWayMerge result batch: {e}")))?;

        Ok(Some(DataChunk { batch: result_batch }))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            children: self.children.iter().map(|c| c.clone_box()).collect(),
            order_by: self.order_by.clone(),
            merged: false,
        })
    }
}
