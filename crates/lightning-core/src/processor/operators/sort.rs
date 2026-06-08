use crate::planner::binder::BoundOrderByItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::{LightningError, Result};

use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Maximum number of rows to sort in-memory before switching to external sort.
/// Set to ~10M rows (approx 500MB for typical row width).
const MAX_SORT_MEMORY_ROWS: usize = 10_000_000;

pub struct SharedSort {
    pub batches: Vec<RecordBatch>,
    pub sorted_result: Option<RecordBatch>,
    pub num_active_collectors: AtomicUsize,
    pub results_returned: AtomicUsize,
}

pub struct PhysicalSort {
    child: Box<dyn PhysicalOperator>,
    order_by: Vec<BoundOrderByItem>,
    shared: Arc<RwLock<SharedSort>>,
    sort_done: Arc<(Mutex<bool>, Condvar)>,
    collected: bool,
}

impl PhysicalSort {
    pub fn new(child: Box<dyn PhysicalOperator>, order_by: Vec<BoundOrderByItem>) -> Self {
        Self {
            child,
            order_by,
            shared: Arc::new(RwLock::new(SharedSort {
                batches: Vec::new(),
                sorted_result: None,
                num_active_collectors: AtomicUsize::new(0),
                results_returned: AtomicUsize::new(0),
            })),
            sort_done: Arc::new((Mutex::new(false), Condvar::new())),
            collected: false,
        }
    }

    fn collect_and_sort(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        self.shared
            .read()
            .num_active_collectors
            .fetch_add(1, Ordering::SeqCst);

        let mut local_batches = Vec::new();
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            local_batches.push(chunk.batch);
        }

        {
            let mut shared = self.shared.write();
            shared.batches.extend(local_batches);
            shared.num_active_collectors.fetch_sub(1, Ordering::SeqCst);

            // If we are the last one, perform the sort
            if shared.num_active_collectors.load(Ordering::SeqCst) == 0 {
                if shared.batches.is_empty() {
                    return Ok(());
                }

                let schema = shared.batches[0].schema();
                let mut columns = Vec::new();
                for i in 0..schema.fields().len() {
                    let arrays: Vec<&dyn arrow::array::Array> = shared
                        .batches
                        .iter()
                        .map(|b| b.column(i).as_ref())
                        .collect();
                    let big_array = arrow::compute::concat(&arrays)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    columns.push(big_array);
                }
                let big_batch = RecordBatch::try_new(schema.clone(), columns)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                let total_rows = big_batch.num_rows();
                if total_rows > MAX_SORT_MEMORY_ROWS {
                    return Err(LightningError::Internal(format!(
                        "Sort of {} rows exceeds in-memory limit of {}. Use ORDER BY ... LIMIT to reduce rows, or increase MAX_SORT_MEMORY_ROWS.",
                        total_rows, MAX_SORT_MEMORY_ROWS
                    )));
                }
                let mut sort_columns = Vec::new();
                for item in &self.order_by {
                    let array = ExpressionEvaluator::evaluate(
                        &item.expression,
                        Some(&big_batch),
                        params,
                        total_rows,
                        &database.function_registry,
                        database,
                    )?;
                    sort_columns.push(SortColumn {
                        values: array,
                        options: Some(SortOptions {
                            descending: item.descending,
                            nulls_first: true,
                        }),
                    });
                }

                let indices = lexsort_to_indices(&sort_columns, None)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                let sorted_columns_res: std::result::Result<Vec<_>, arrow::error::ArrowError> =
                    big_batch
                        .columns()
                        .iter()
                        .map(|col| take(col.as_ref(), &indices, None))
                        .collect();

                let sorted_columns = sorted_columns_res
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                let result = RecordBatch::try_new(schema, sorted_columns)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                shared.sorted_result = Some(result);
                shared.batches.clear(); // Free memory
                let (ref lock, ref cvar) = &*self.sort_done;
                *lock.lock() = true;
                cvar.notify_all();
            }
        }

        Ok(())
    }
}

impl PhysicalOperator for PhysicalSort {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.collected {
            self.collect_and_sort(database, tx, params)?;
            self.collected = true;
        }

        loop {
            // Wait for sort to complete (last collector signals via Condvar)
            let (ref lock, ref cvar) = &*self.sort_done;
            let mut done = lock.lock();
            while !*done {
                cvar.wait(&mut done);
            }
            drop(done);

            let shared = self.shared.read();

            if let Some(ref result) = shared.sorted_result {
                let start = shared.results_returned.load(Ordering::SeqCst);
                let total = result.num_rows();
                if start >= total {
                    return Ok(None);
                }

                let batch_size = 1024;
                let end = (start + batch_size).min(total);

                if shared
                    .results_returned
                    .compare_exchange(start, end, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let batch = result.slice(start, end - start);
                    return Ok(Some(DataChunk { batch }));
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn is_parallel_safe(&self) -> bool {
        true
    }

    fn try_parallelize(
        &self,
        num_workers: usize,
    ) -> Result<Option<Box<dyn PhysicalOperator + Send + Sync>>> {
        let mut children = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            let mut child_clone = self.child.clone_box();
            child_clone.set_partition(i, num_workers);
            let sort_clone = Box::new(PhysicalSort {
                child: child_clone,
                order_by: self.order_by.clone(),
                shared: Arc::new(::parking_lot::RwLock::new(SharedSort {
                    batches: Vec::new(),
                    sorted_result: None,
                    num_active_collectors: ::std::sync::atomic::AtomicUsize::new(0),
                    results_returned: ::std::sync::atomic::AtomicUsize::new(0),
                })),
                sort_done: Arc::new((::parking_lot::Mutex::new(false), ::parking_lot::Condvar::new())),
                collected: false,
            });
            children.push(sort_clone as Box<dyn PhysicalOperator + Send + Sync>);
        }
        let merge = Box::new(super::nway_merge::NWayMerge::new(children, self.order_by.clone()));
        Ok(Some(merge))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            order_by: self.order_by.clone(),
            shared: self.shared.clone(),
            sort_done: self.sort_done.clone(),
            collected: false,
        })
    }
}
