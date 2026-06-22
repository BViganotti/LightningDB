use crate::planner::binder::BoundOrderByItem;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::{LightningError, Result};

use arrow::compute::{lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Maximum number of rows to sort in-memory before switching to external sort.
/// Set to ~10M rows (approx 500MB for typical row width).
const MAX_SORT_MEMORY_ROWS: usize = 10_000_000;

pub struct SharedSort {
    pub batches: Vec<RecordBatch>,
    pub sorted_result: Option<RecordBatch>,
    pub sort_started: AtomicBool,
    pub results_returned: AtomicUsize,
    pub num_collected: AtomicUsize,
}

pub struct PhysicalSort {
    child: Box<dyn PhysicalOperator>,
    order_by: Vec<BoundOrderByItem>,
    shared: Arc<RwLock<SharedSort>>,
    sort_done: Arc<(Mutex<bool>, Condvar)>,
    collected: bool,
    num_partitions: AtomicUsize,
}

impl PhysicalSort {
    pub fn new(child: Box<dyn PhysicalOperator>, order_by: Vec<BoundOrderByItem>) -> Self {
        Self {
            child,
            order_by,
            shared: Arc::new(RwLock::new(SharedSort {
                batches: Vec::new(),
                sorted_result: None,
                sort_started: AtomicBool::new(false),
                results_returned: AtomicUsize::new(0),
                num_collected: AtomicUsize::new(0),
            })),
            sort_done: Arc::new((Mutex::new(false), Condvar::new())),
            collected: false,
            num_partitions: AtomicUsize::new(1),
        }
    }

    /// Signal the condvar so get_next can wake and consume the result.
    /// Must be called on every exit path (success, error, or panic).
    fn signal_sort_done(&self) {
        let (ref lock, ref cvar) = &*self.sort_done;
        *lock.lock() = true;
        cvar.notify_all();
    }

    fn collect_and_sort(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        let mut local_batches = Vec::new();
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            local_batches.push(chunk.batch);
        }

        {
            let mut shared = self.shared.write();
            shared.batches.extend(local_batches);
            let prev = shared.num_collected.fetch_add(1, Ordering::Release);
            let num_parts = self.num_partitions.load(Ordering::Acquire);
            if prev + 1 < num_parts {
                // Other workers still collecting. If we win the CAS, we wait
                // for them before checking batches.
            }
        }

        if self
            .shared
            .read()
            .sort_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // Another thread is sorting. Wait for it by returning Ok(())
            // and letting get_next check sort_done via the condvar.
            self.signal_sort_done();
            return Ok(());
        }

        // We won the CAS — responsible for sorting. Wait until all partitions
        // have contributed their data before checking or processing batches.
        let num_parts = self.num_partitions.load(Ordering::Acquire);
        while self.shared.read().num_collected.load(Ordering::Acquire) < num_parts {
            std::hint::spin_loop();
        }

        let result = self.do_sort(database, params, tx);

        // Always signal that sorting is done, regardless of success or failure.
        // Without this, get_next hangs forever on the condvar.
        self.signal_sort_done();

        result
    }

    /// Core sort logic — separated so signal_sort_done fires on every exit path.
    fn do_sort(
        &mut self,
        database: &crate::Database,
        params: Option<&HashMap<String, Value>>,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let schema;
        let big_batch;
        let total_rows;
        {
            let shared = self.shared.read();
            if shared.batches.is_empty() {
                return Ok(());
            }
            schema = shared.batches[0].schema();
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
            big_batch = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            total_rows = big_batch.num_rows();
        }
        if total_rows > MAX_SORT_MEMORY_ROWS {
            return Err(LightningError::Internal(format!(
                "Sort of {total_rows} rows exceeds in-memory limit of {MAX_SORT_MEMORY_ROWS}. Use ORDER BY ... LIMIT to reduce rows, or increase MAX_SORT_MEMORY_ROWS."
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

        {
            let mut shared = self.shared.write();
            shared.sorted_result = Some(result);
            shared.batches.clear();
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
        false
    }

    fn set_partition(&mut self, index: usize, total: usize) {
        self.num_partitions.store(total, Ordering::Release);
        self.child.set_partition(index, total);
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
                    sort_started: ::std::sync::atomic::AtomicBool::new(false),
                    results_returned: ::std::sync::atomic::AtomicUsize::new(0),
                    num_collected: ::std::sync::atomic::AtomicUsize::new(0),
                })),
                sort_done: Arc::new((::parking_lot::Mutex::new(false), ::parking_lot::Condvar::new())),
                collected: false,
                num_partitions: ::std::sync::atomic::AtomicUsize::new(1),
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
            shared: Arc::new(RwLock::new(SharedSort {
                batches: Vec::new(),
                sorted_result: None,
                sort_started: AtomicBool::new(false),
                results_returned: AtomicUsize::new(0),
                num_collected: AtomicUsize::new(0),
            })),
            sort_done: Arc::new((Mutex::new(false), Condvar::new())),
            collected: false,
            num_partitions: AtomicUsize::new(self.num_partitions.load(Ordering::Acquire)),
        })
    }
}
