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
use std::sync::Arc;

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

            let n = full_batch.num_rows();
            let k = self.limit as usize;

            // Compute sort key arrays via ExpressionEvaluator
            let mut sort_arrays = Vec::new();
            for item in &self.order_by {
                let val = ExpressionEvaluator::evaluate(
                    &item.expression,
                    Some(&full_batch),
                    params,
                    n,
                    &database.function_registry,
                    database,
                )?;
                sort_arrays.push(val);
            }

            let indices: ArrayRef = if n > k * 2 {
                // Bounded top-K: extract sort values, use select_nth_unstable
                // O(N) partition + O(K log K) final sort instead of O(N log N).
                let _num_sort_keys = sort_arrays.len();
                let mut rows: Vec<(Vec<Value>, u64)> = Vec::with_capacity(n);
                for i in 0..n {
                    let keys: Vec<Value> = sort_arrays.iter()
                        .map(|arr| Value::from_arrow(arr, i))
                        .collect();
                    rows.push((keys, i as u64));
                }

                let kk = k.min(rows.len());
                if kk == 0 {
                    Arc::new(UInt64Array::from(Vec::<u64>::new()))
                } else {
                    rows.select_nth_unstable_by(kk - 1, |a, b| {
                        for ((ak, bk), item) in a.0.iter().zip(b.0.iter()).zip(&self.order_by) {
                            let cmp = ak.partial_cmp(bk).unwrap_or(Ordering::Equal);
                            if cmp != Ordering::Equal {
                                return if item.descending { cmp.reverse() } else { cmp };
                            }
                        }
                        Ordering::Equal
                    });
                    rows[..kk].sort_unstable_by(|a, b| {
                        for ((ak, bk), item) in a.0.iter().zip(b.0.iter()).zip(&self.order_by) {
                            let cmp = ak.partial_cmp(bk).unwrap_or(Ordering::Equal);
                            if cmp != Ordering::Equal {
                                return if item.descending { cmp.reverse() } else { cmp };
                            }
                        }
                        Ordering::Equal
                    });
                    Arc::new(UInt64Array::from(
                        rows[..kk].iter().map(|(_, idx)| *idx).collect::<Vec<_>>(),
                    ))
                }
            } else {
                let mut sort_columns = Vec::new();
                for (item, arr) in self.order_by.iter().zip(sort_arrays.iter()) {
                    sort_columns.push(SortColumn {
                        values: arr.clone(),
                        options: Some(SortOptions {
                            descending: item.descending,
                            nulls_first: true,
                        }),
                    });
                }
                let all_indices = lexsort_to_indices(&sort_columns, None)?;
                let sliced = if all_indices.len() > k {
                    all_indices.slice(0, k)
                } else {
                    all_indices
                };
                arrow::compute::kernels::cast::cast(&sliced, &DataType::UInt64)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?
            };

            let mut columns = Vec::new();
            for i in 0..full_batch.num_columns() {
                columns.push(take(full_batch.column(i).as_ref(), &indices, None)?);
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
