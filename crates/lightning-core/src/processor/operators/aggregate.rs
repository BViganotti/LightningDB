use crate::processor::aggregate::AggregateFunction;
use crate::processor::functions::aggregate_ext::{
    CollectDistinct, GroupConcat, Median, StdDevPop, StdDevSamp, VarPop, VarSamp,
};
use crate::processor::functions::aggregate_function::{
    AggregateFunction as IAggregateFunction, Avg, Collect, Count, CountDistinct, Max, Min, Sum,
};
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Threshold for switching from hash-based to sort-based aggregation.
/// When the estimated number of groups exceeds this value, sort-based
/// aggregation is used instead to avoid building a large HashMap.
const SORT_AGGREGATION_THRESHOLD: usize = 100_000;

pub struct SharedAggregateState {
    pub groups: RwLock<HashMap<Vec<Value>, (Vec<Box<dyn IAggregateFunction>>, usize)>>,
    pub num_active_builders: AtomicU64,
    pub is_done: AtomicBool,
    pub final_result: RwLock<Option<RecordBatch>>,
}

pub struct Aggregate {
    child: Box<dyn PhysicalOperator>,
    group_by_indices: Vec<usize>,
    aggregates: Vec<(AggregateFunction, usize)>,
    shared_state: Arc<SharedAggregateState>,
    built: bool,
}

impl Aggregate {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        group_by_indices: Vec<usize>,
        aggregates: Vec<(AggregateFunction, usize)>,
    ) -> Self {
        Self {
            child,
            group_by_indices,
            aggregates,
            shared_state: Arc::new(SharedAggregateState {
                groups: RwLock::new(HashMap::new()),
                num_active_builders: AtomicU64::new(0),
                is_done: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            built: false,
        }
    }

    fn create_agg_functions(&self) -> Vec<Box<dyn IAggregateFunction>> {
        self.aggregates
            .iter()
            .map(|(t, _)| match t {
                AggregateFunction::Count => Box::new(Count::new()) as Box<dyn IAggregateFunction>,
                AggregateFunction::CountDistinct => {
                    Box::new(CountDistinct::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::Sum => Box::new(Sum::new()) as Box<dyn IAggregateFunction>,
                AggregateFunction::Avg => Box::new(Avg::new()) as Box<dyn IAggregateFunction>,
                AggregateFunction::Min => Box::new(Min::new()) as Box<dyn IAggregateFunction>,
                AggregateFunction::Max => Box::new(Max::new()) as Box<dyn IAggregateFunction>,
                AggregateFunction::Collect => {
                    Box::new(Collect::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::GroupConcat => {
                    Box::new(GroupConcat::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::Median => {
                    Box::new(Median::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::CollectDistinct => {
                    Box::new(CollectDistinct::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::StdDevPop => {
                    Box::new(StdDevPop::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::StdDevSamp => {
                    Box::new(StdDevSamp::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::VarPop => {
                    Box::new(VarPop::new()) as Box<dyn IAggregateFunction>
                }
                AggregateFunction::VarSamp => {
                    Box::new(VarSamp::new()) as Box<dyn IAggregateFunction>
                }
            })
            .collect()
    }

    fn build(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        self.shared_state
            .num_active_builders
            .fetch_add(1, Ordering::SeqCst);

        // FIX: Specialized fast path for global aggregation (no GROUP BY)
        if self.group_by_indices.is_empty() {
            let mut local_agg_funcs = self.create_agg_functions();
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let batch = &chunk.batch;
                let num_rows = batch.num_rows();
                for (i, (agg_type, col_idx)) in self.aggregates.iter().enumerate() {
                    // Fast path for count(*) when scan returns empty batches (count-only)
                    if (*agg_type == AggregateFunction::Count
                        || *agg_type == AggregateFunction::CountDistinct)
                        && batch.num_columns() == 0
                    {
                        // For count(*), any row is a match. For count(col), this path shouldn't be hit
                        // unless the planner optimized it.
                        // We use a dummy null array to satisfy the update_vector call if needed,
                        // but CountStar would be better.
                        // Since Count aggregate needs an array, we provide one if possible or
                        // handle it specially.

                        // Let's check if the aggregate is actually CountStar or similar
                        // For now, if 0 columns, we assume it's a row count
                        let dummy = Arc::new(arrow::array::NullArray::new(num_rows)) as ArrayRef;
                        local_agg_funcs[i].update_vector(&dummy)?;
                    } else {
                        let col = batch.column(*col_idx);
                        local_agg_funcs[i].update_vector(col)?;
                    }
                }
            }

            // Merge into shared state once at the end
            let mut groups = self.shared_state.groups.write();
            let (agg_funcs, _count) = groups
                .entry(Vec::new())
                .or_insert_with(|| (self.create_agg_functions(), 0));

            for (i, local_func) in local_agg_funcs.into_iter().enumerate() {
                agg_funcs[i].merge(local_func.as_ref())?;
            }
        } else {
            // Adaptive aggregation: switch to sort-based when row count is large
            // to avoid HashMap memory pressure for high-cardinality group keys.
            let mut all_batches: Vec<RecordBatch> = Vec::new();
            let mut use_sort_based = false;

            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let batch = &chunk.batch;
                let num_rows = batch.num_rows();

                if !use_sort_based && all_batches.iter().map(|b| b.num_rows()).sum::<usize>() + num_rows > SORT_AGGREGATION_THRESHOLD {
                    // Switch to sort-based approach — flush existing hash map first
                    use_sort_based = true;
                }

                if use_sort_based {
                    // Collect batches for sort-based aggregation (columnar)
                    all_batches.push(batch.clone());
                } else {
                    // Vectorized hash-based aggregation:
                    // Build local group → row_indices map for this chunk,
                    // then call update_vector() per group (not per row).
                    let mut local_groups: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
                    for row_idx in 0..num_rows {
                        let mut group_key = Vec::with_capacity(self.group_by_indices.len());
                        for &idx in &self.group_by_indices {
                            group_key.push(Value::from_arrow(batch.column(idx), row_idx));
                        }
                        local_groups.entry(group_key).or_default().push(row_idx);
                    }

                    // Now merge local groups into shared state with vectorized updates
                    let mut groups = self.shared_state.groups.write();
                    for (group_key, row_indices) in local_groups {
                        let (agg_funcs, count) = groups
                            .entry(group_key)
                            .or_insert_with(|| (self.create_agg_functions(), 0));

                        *count += row_indices.len();
                        for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                            let col = batch.column(*col_idx);
                            // Use filter with a boolean mask instead of per-group take
                            let mut mask = vec![false; num_rows];
                            for &r in &row_indices {
                                mask[r] = true;
                            }
                            let mask_arr = arrow::array::BooleanArray::from(mask);
                            let gathered = arrow::compute::filter(col, &mask_arr)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                            agg_funcs[i].update_vector(&gathered)?;
                        }
                    }
                }
            }

            // If sort-based was used, process the collected batches (columnar)
            if use_sort_based && !all_batches.is_empty() {
                // Concatenate all collected batches into one
                let schema = all_batches[0].schema();
                let full_batch = arrow::compute::concat_batches(&schema, &all_batches)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                let total = full_batch.num_rows();

                // Compute sort key arrays for group-by columns
                let mut sort_columns = Vec::with_capacity(self.group_by_indices.len());
                for &idx in &self.group_by_indices {
                    sort_columns.push(full_batch.column(idx).clone());
                }

                // Build SortColumn descriptors
                let sort_cols: Vec<arrow::compute::SortColumn> = sort_columns.iter().map(|arr| {
                    arrow::compute::SortColumn {
                        values: arr.clone(),
                        options: Some(arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: true,
                        }),
                    }
                }).collect();

                // O(N log N) lexsort on group keys
                let indices = arrow::compute::lexsort_to_indices(&sort_cols, None)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                // Walk sorted indices, group contiguous equal keys
                let mut groups = self.shared_state.groups.write();
                let mut start = 0usize;
                while start < total {
                    let key_row = indices.value(start) as usize;
                    let mut key = Vec::with_capacity(self.group_by_indices.len());
                    for &idx in &self.group_by_indices {
                        key.push(Value::from_arrow(full_batch.column(idx), key_row));
                    }

                    // Find end of this group
                    let mut end = start + 1;
                    while end < total {
                        let cmp_row = indices.value(end) as usize;
                        let mut same = true;
                        for &idx in &self.group_by_indices {
                            if Value::from_arrow(full_batch.column(idx), key_row)
                                != Value::from_arrow(full_batch.column(idx), cmp_row)
                            {
                                same = false;
                                break;
                            }
                        }
                        if !same { break; }
                        end += 1;
                    }

                    let group_len = end - start;
                    let group_indices: Vec<u64> = (start..end).map(|i| indices.value(i) as u64).collect();
                    // Build the index array once and share it across all aggregate columns
                    let idx_arr = arrow::array::UInt64Array::from(group_indices);

                    let mut agg_funcs = self.create_agg_functions();
                    for (j, (_, col_idx)) in self.aggregates.iter().enumerate() {
                        let gathered = arrow::compute::take(
                            full_batch.column(*col_idx),
                            &idx_arr,
                            None,
                        ).map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        agg_funcs[j].update_vector(&gathered)?;
                    }
                    groups.insert(key, (agg_funcs, group_len));
                    start = end;
                }
            }
        }

        if self
            .shared_state
            .num_active_builders
            .fetch_sub(1, Ordering::SeqCst)
            == 1
        {
            // I am the last builder, finalize results
            let mut groups = self.shared_state.groups.write();

            // Global aggregate case: if no groups and no group-by columns, create a single default group
            if groups.is_empty() && self.group_by_indices.is_empty() {
                groups.insert(Vec::new(), (self.create_agg_functions(), 0));
            }

            let num_groups = groups.len();

            let mut fields = Vec::new();
            for i in 0..self.group_by_indices.len() {
                fields.push(Field::new(format!("group{i}"), DataType::Utf8, true));
            }
            for (i, (agg_type, _)) in self.aggregates.iter().enumerate() {
                // Determine return type based on aggregate function
                let return_type = match agg_type {
                    // COUNT and COUNT_DISTINCT return Int64
                    AggregateFunction::Count | AggregateFunction::CountDistinct => DataType::Int64,
                    // Others return Float64 for simplicity
                    _ => DataType::Float64,
                };
                fields.push(Field::new(format!("agg{i}"), return_type, true));
            }

            let mut columns: Vec<Box<dyn arrow::array::ArrayBuilder>> = Vec::new();
            for _ in 0..self.group_by_indices.len() {
                columns.push(Box::new(arrow::array::StringBuilder::new()));
            }
            for (agg_type, _) in self.aggregates.iter() {
                let builder: Box<dyn arrow::array::ArrayBuilder> = match agg_type {
                    AggregateFunction::Count | AggregateFunction::CountDistinct => {
                        Box::new(arrow::array::Int64Array::builder(num_groups))
                    }
                    _ => Box::new(arrow::array::Float64Array::builder(num_groups)),
                };
                columns.push(builder);
            }

            // Sort groups by key for deterministic output order.
            // Use string representation since Value doesn't implement Ord.
            let mut sorted_groups: Vec<_> = groups.iter().collect();
            sorted_groups.sort_by(|a, b| {
                let a_str: Vec<String> = a.0.iter().map(|v| format!("{v:?}")).collect();
                let b_str: Vec<String> = b.0.iter().map(|v| format!("{v:?}")).collect();
                a_str.cmp(&b_str)
            });

            for (key, (agg_funcs, _count)) in sorted_groups {
                for (i, val) in key.iter().enumerate() {
                    let builder = columns[i]
                        .as_any_mut()
                        .downcast_mut::<arrow::array::StringBuilder>()
                        .expect("group-by columns must be StringBuilder");
                    builder.append_value(val.to_string());
                }
                for (i, (agg_type, _)) in self.aggregates.iter().enumerate() {
                    let final_val = agg_funcs[i].finalize()?;
                    match agg_type {
                        AggregateFunction::Count | AggregateFunction::CountDistinct => {
                            let builder = columns[self.group_by_indices.len() + i]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::Int64Builder>()
                                .expect("Count aggregate columns must be Int64Builder");
                            // Convert the f64 value to i64
                            let count = final_val.as_number() as i64;
                            builder.append_value(count);
                        }
                        _ => {
                            let builder = columns[self.group_by_indices.len() + i]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::Float64Builder>()
                                .expect("Non-Count aggregate columns must be Float64Builder");
                            builder.append_value(final_val.as_number());
                        }
                    }
                }
            }

            let mut final_columns = Vec::new();
            for mut b in columns {
                final_columns.push(b.finish() as Arc<dyn arrow::array::Array>);
            }

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), final_columns)
                .map_err(|e| crate::LightningError::Internal(format!(
                    "Aggregate output schema mismatch: {e}"
                )))?;
            *self.shared_state.final_result.write() = Some(batch);
            self.shared_state.is_done.store(true, Ordering::SeqCst);
        }

        Ok(())
    }
}

impl PhysicalOperator for Aggregate {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.built {
            self.build(database, tx, params)?;
            self.built = true;
        }

        if self.shared_state.is_done.load(Ordering::SeqCst) {
            let mut final_result = self.shared_state.final_result.write();
            if let Some(batch) = final_result.take() {
                return Ok(Some(DataChunk { batch }));
            }
        }
        Ok(None)
    }

    fn is_single_row(&self) -> bool {
        self.group_by_indices.is_empty()
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            group_by_indices: self.group_by_indices.clone(),
            aggregates: self.aggregates.clone(),
            shared_state: self.shared_state.clone(),
            built: false,
        })
    }
}
