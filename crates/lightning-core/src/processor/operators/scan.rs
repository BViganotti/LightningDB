use crate::planner::binder::BoundExpression;
use crate::processor::{DataChunk, PhysicalOperator};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::storage_manager::Table;
use crate::{LightningError, Result};
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct ScanState {
    pub current_row: AtomicU64,
    pub num_rows: u64,
    pub has_modifications: bool,
}

#[derive(Clone)]
pub struct PhysicalScan {
    pub table: Table,
    pub variable: String,
    pub bm: Arc<BufferManager>,
    pub state: Arc<ScanState>,
    pub mask: Option<Arc<RwLock<super::semi_mask::SemiMask>>>,
    pub mask_column_idx: Option<usize>,
    pub projected_idxs: Option<Vec<usize>>,
    pub read_ts: u64,
    pub cached_schema: Arc<RwLock<Option<Arc<arrow::datatypes::Schema>>>>,
    pub filter_cached_schema: Arc<RwLock<Option<Arc<arrow::datatypes::Schema>>>>,
    pub pushdown_filter: Option<BoundExpression>,
    pub filter_column_idxs: Vec<usize>,
    /// Reusable buffer for column arrays to reduce per-morsel allocation.
    column_buffer: Vec<ArrayRef>,
    /// Reusable scratch mask for filter/visibility/null-id passes.
    scratch_mask: Vec<bool>,
    /// Per-worker position within the partition range for parallel execution.
    /// Each cloned PhysicalScan gets its own Arc<AtomicU64> via set_partition(),
    /// so workers do not contend on a shared counter.
    partition_position: Arc<AtomicU64>,
    /// Cached morsel size, computed once from table metadata.
    morsel_size: u64,
    /// Start of this partition's row range (inclusive). Equal to 0 when not partitioned.
    partition_start_row: u64,
    /// End of this partition's row range (exclusive). Equal to state.num_rows when not partitioned.
    partition_end_row: u64,
}

impl PhysicalScan {
    pub fn new(
        table: Table,
        variable: String,
        bm: Arc<BufferManager>,
        num_rows: u64,
        read_ts: u64,
    ) -> Result<Self> {
        if table.columns.is_empty() {
            return Err(LightningError::Internal(format!(
                "PhysicalScan::new: table '{}' has no columns. Schema mismatch: catalog and storage may be out of sync.",
                table.name
            )));
        }
        let has_modifications = table.columns[0].version_info.has_modifications();
        let avg_element_size = table.columns.iter().map(|c| c.element_size()).max().unwrap_or(8);
        let num_columns = table.columns.len() as u64;
        let target_bytes: u64 = 8 * 1024 * 1024;
        let per_row = avg_element_size as u64 * num_columns;
        let morsel_size = (target_bytes / per_row.max(1)).clamp(4096, 262_144);
        Ok(Self {
            table,
            variable,
            bm,
            state: Arc::new(ScanState {
                current_row: AtomicU64::new(0),
                num_rows,
                has_modifications,
            }),
            mask: None,
            mask_column_idx: None,
            projected_idxs: None,
            read_ts,
            cached_schema: Arc::new(RwLock::new(None)),
            filter_cached_schema: Arc::new(RwLock::new(None)),
            pushdown_filter: None,
            filter_column_idxs: Vec::new(),
            column_buffer: Vec::new(),
            scratch_mask: Vec::new(),
            partition_position: Arc::new(AtomicU64::new(0)),
            partition_start_row: 0,
            partition_end_row: num_rows,
            morsel_size,
        })
    }

    pub fn with_mask(
        mut self,
        mask: Arc<RwLock<super::semi_mask::SemiMask>>,
        col_idx: Option<usize>,
    ) -> Self {
        self.mask = Some(mask);
        self.mask_column_idx = col_idx;
        self
    }

    pub fn with_projected_idxs(mut self, mut idxs: Vec<usize>) -> Self {
        // Filter out indices that exceed the storage column count
        // (can happen if catalog and storage have different column counts after ALTER TABLE)
        idxs.retain(|&idx| idx < self.table.columns.len());
        self.projected_idxs = Some(idxs);
        self
    }

    pub fn with_filter(mut self, filter: BoundExpression) -> Self {
        let mut filter_idxs = self.extract_filter_columns(&filter);
        // Validate indices against the storage column count.
        // If an index is out of bounds (possible catalog/storage desync),
        // silently skip it — the full scan + filter path will handle the comparison.
        filter_idxs.retain(|&idx| idx < self.table.columns.len());

        // Ensure filter columns are included in the projected set so the
        // pushdown filter evaluation has access to all columns it needs.
        if let Some(ref mut idxs) = self.projected_idxs {
            for &fi in &filter_idxs {
                if !idxs.contains(&fi) {
                    idxs.push(fi);
                }
            }
        }

        self.filter_column_idxs = filter_idxs;
        self.pushdown_filter = Some(filter);
        self
    }

    fn extract_zone_maps(&self, expr: &BoundExpression) -> std::collections::HashMap<usize, crate::storage::column::ZoneMapEq> {
        let mut maps = std::collections::HashMap::new();
        self.collect_zone_maps(expr, &mut maps);
        maps
    }

    fn collect_zone_maps(
        &self,
        expr: &BoundExpression,
        maps: &mut std::collections::HashMap<usize, crate::storage::column::ZoneMapEq>,
    ) {
        match expr {
            BoundExpression::Comparison(left, crate::parser::ast::ComparisonOperator::Equal, right) => {
                match (left.as_ref(), right.as_ref()) {
                    (BoundExpression::PropertyLookup(_, prop_idx, _), BoundExpression::Literal(lit)) => {
                        let val = match lit {
                            crate::parser::ast::Literal::String(s) => crate::processor::Value::String(s.clone()),
                            crate::parser::ast::Literal::Number(n) => crate::processor::Value::Number(*n),
                            crate::parser::ast::Literal::Boolean(b) => crate::processor::Value::Boolean(*b),
                            crate::parser::ast::Literal::Null => crate::processor::Value::Null,
                        };
                        maps.insert(*prop_idx, crate::storage::column::ZoneMapEq { value: val });
                    }
                    (BoundExpression::Literal(lit), BoundExpression::PropertyLookup(_, prop_idx, _)) => {
                        let val = match lit {
                            crate::parser::ast::Literal::String(s) => crate::processor::Value::String(s.clone()),
                            crate::parser::ast::Literal::Number(n) => crate::processor::Value::Number(*n),
                            crate::parser::ast::Literal::Boolean(b) => crate::processor::Value::Boolean(*b),
                            crate::parser::ast::Literal::Null => crate::processor::Value::Null,
                        };
                        maps.insert(*prop_idx, crate::storage::column::ZoneMapEq { value: val });
                    }
                    _ => {}
                }
            }
            BoundExpression::Logical(left, crate::parser::ast::LogicalOperator::And, right) => {
                self.collect_zone_maps(left, maps);
                self.collect_zone_maps(right, maps);
            }
            _ => {}
        }
    }

    fn extract_filter_columns(&self, expr: &BoundExpression) -> Vec<usize> {
        let mut columns = Vec::new();
        self.collect_filter_columns(expr, &mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    fn collect_filter_columns(&self, expr: &BoundExpression, columns: &mut Vec<usize>) {
        match expr {
            BoundExpression::PropertyLookup(_, prop_idx, _) => {
                columns.push(*prop_idx);
            }
            BoundExpression::Arithmetic(left, _, right)
            | BoundExpression::Comparison(left, _, right)
            | BoundExpression::Logical(left, _, right) => {
                self.collect_filter_columns(left, columns);
                self.collect_filter_columns(right, columns);
            }
            BoundExpression::Not(inner) => {
                self.collect_filter_columns(inner, columns);
            }
            BoundExpression::Function(_, args, _) | BoundExpression::List(args, _) => {
                for arg in args {
                    self.collect_filter_columns(arg, columns);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    self.collect_filter_columns(e, columns);
                }
                for (w, t) in when_then {
                    self.collect_filter_columns(w, columns);
                    self.collect_filter_columns(t, columns);
                }
                if let Some(e) = else_expression {
                    self.collect_filter_columns(e, columns);
                }
            }
            _ => {}
        }
    }

    fn remap_filter_expression(&self, expr: &mut BoundExpression) {
        match expr {
            BoundExpression::PropertyLookup(_, original_idx, _) => {
                if let Some(new_idx) = self
                    .filter_column_idxs
                    .iter()
                    .position(|&i| i == *original_idx)
                {
                    *original_idx = new_idx;
                }
            }
            BoundExpression::Arithmetic(left, _, right)
            | BoundExpression::Comparison(left, _, right)
            | BoundExpression::Logical(left, _, right) => {
                self.remap_filter_expression(left);
                self.remap_filter_expression(right);
            }
            BoundExpression::Not(inner) => {
                self.remap_filter_expression(inner);
            }
            BoundExpression::Function(_, args, _) | BoundExpression::List(args, _) => {
                for arg in args {
                    self.remap_filter_expression(arg);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    self.remap_filter_expression(e);
                }
                for (w, t) in when_then {
                    self.remap_filter_expression(w);
                    self.remap_filter_expression(t);
                }
                if let Some(e) = else_expression {
                    self.remap_filter_expression(e);
                }
            }
            _ => {}
        }
    }
}

impl PhysicalOperator for PhysicalScan {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Option<DataChunk>> {
        let morsel = self.morsel_size;
        let end_bound = self.partition_end_row;
        loop {
            let start_row = self
                .partition_position
                .fetch_add(morsel, Ordering::Relaxed);
            if start_row >= end_bound {
                return Ok(None);
            }
            let num_rows_to_read = std::cmp::min(morsel, end_bound - start_row);

            let has_pushdown =
                self.pushdown_filter.is_some() && !self.filter_column_idxs.is_empty();
            let only_scan_filter_cols = has_pushdown && self.projected_idxs.is_none();
            // Track if the early filter path already confirmed all rows pass.
            let mut filter_all_pass = false;

            let zone_maps = self.pushdown_filter.as_ref()
                .map(|expr| self.extract_zone_maps(expr))
                .unwrap_or_default();

            if only_scan_filter_cols {
                // Compute a boolean mask from filter columns only (lightweight scan),
                // then fall through to the normal path which scans the full column set
                // and applies the mask. This avoids the O(n*m) cost of scanning
                // all columns for rows that would be filtered out.
                let filter_results: Vec<Result<ArrayRef>> = self
                    .filter_column_idxs
                    .iter()
                    .map(|&idx| {
                        let column = &self.table.columns[idx];
                        column.scan_to_array(&self.bm, start_row, num_rows_to_read, tx, zone_maps.get(&idx))
                    })
                    .collect();

                let mut filter_columns = Vec::with_capacity(self.filter_column_idxs.len());
                let mut filter_fields = Vec::with_capacity(self.filter_column_idxs.len());

                for (i, res) in filter_results.into_iter().enumerate() {
                    let array = res?;
                    let idx = self.filter_column_idxs[i];
                    filter_fields.push(arrow::datatypes::Field::new(
                        &self.table.columns[idx].name,
                        array.data_type().clone(),
                        true,
                    ));
                    filter_columns.push(array);
                }

                let filter_batch = RecordBatch::try_new(
                    Arc::new(arrow::datatypes::Schema::new(filter_fields)),
                    filter_columns,
                )
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                if let Some(ref filter_expr) = self.pushdown_filter {
                    let mut remapped = filter_expr.clone();
                    self.remap_filter_expression(&mut remapped);
                    let mask_arr = crate::processor::evaluator::ExpressionEvaluator::evaluate(
                        &remapped,
                        Some(&filter_batch),
                        params,
                        filter_batch.num_rows(),
                        &database.function_registry,
                        database,
                    )?;
                    let mask = mask_arr.as_any().downcast_ref::<BooleanArray>()
                        .ok_or_else(|| crate::LightningError::Internal(
                            "filter expression must evaluate to BooleanArray".into(),
                        ))?;

                    // If all rows in this morsel are filtered out, skip to next morsel
                    if mask.false_count() == mask.len() {
                        continue;
                    }

                    // Track that filter was already checked — skip re-evaluation on full batch
                    if mask.false_count() == 0 {
                        filter_all_pass = true;
                    }
                }
                // Fall through to normal path — all columns will be scanned below.
            }

            self.column_buffer.clear();
            let arrow_columns = &mut self.column_buffer;
            let results: Vec<Result<ArrayRef>> = if num_rows_to_read >= 4096 {
                use rayon::prelude::*;
                if let Some(idxs) = &self.projected_idxs {
                    idxs.par_iter()
                        .map(|&idx| {
                            let column = &self.table.columns[idx];
                            column.scan_to_array(&self.bm, start_row, num_rows_to_read, tx, zone_maps.get(&idx))
                        })
                        .collect()
                } else {
                    self.table
                        .columns
                        .par_iter()
                        .enumerate()
                        .map(|(idx, column)| {
                            column.scan_to_array(&self.bm, start_row, num_rows_to_read, tx, zone_maps.get(&idx))
                        })
                        .collect()
                }
            } else if let Some(idxs) = &self.projected_idxs {
                idxs.iter()
                    .map(|&idx| {
                        let column = &self.table.columns[idx];
                        column.scan_to_array(&self.bm, start_row, num_rows_to_read, tx, zone_maps.get(&idx))
                    })
                    .collect()
            } else {
                self.table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, column)| {
                        column.scan_to_array(&self.bm, start_row, num_rows_to_read, tx, zone_maps.get(&idx))
                    })
                    .collect()
            };

            for res in results {
                arrow_columns.push(res?);
            }

            let schema = {
                let cache_read = self.cached_schema.read();
                if let Some(s) = &*cache_read {
                    s.clone()
                } else {
                    drop(cache_read);
                    let mut cache = self.cached_schema.write();
                    if cache.is_none() {
                        let mut fields = Vec::new();
                        if let Some(idxs) = &self.projected_idxs {
                            for (i, &idx) in idxs.iter().enumerate() {
                                fields.push(arrow::datatypes::Field::new(
                                    &self.table.columns[idx].name,
                                    arrow_columns[i].data_type().clone(),
                                    true,
                                ));
                            }
                        } else {
                            for (i, column) in self.table.columns.iter().enumerate() {
                                fields.push(arrow::datatypes::Field::new(
                                    &column.name,
                                    arrow_columns[i].data_type().clone(),
                                    true,
                                ));
                            }
                        }
                        *cache = Some(Arc::new(arrow::datatypes::Schema::new(fields)));
                    }
                    cache.as_ref().expect("schema cache was just populated").clone()
                }
            };

            let mut batch = RecordBatch::try_new(schema, std::mem::take(arrow_columns))
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

            // Check has_modifications dynamically - it may change between plan creation and execution
            let has_modifications_now = self.table.columns[0].version_info.has_modifications();
            if has_modifications_now {
                self.scratch_mask.clear();

                if self.table.columns[0].name == "_id" {
                    let col = batch.column(0);
                    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                        self.table.columns[0].version_info.get_visibility_mask(
                            arr.values(),
                            tx.tx_id,
                            tx.read_ts,
                            &mut self.scratch_mask,
                        );
                    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        let row_ids: Vec<u64> = arr.values().iter().map(|&v| v as u64).collect();
                        self.table.columns[0].version_info.get_visibility_mask(
                            &row_ids,
                            tx.tx_id,
                            tx.read_ts,
                            &mut self.scratch_mask,
                        );
                    }
                } else {
                    let row_ids: Vec<u64> = (0..batch.num_rows())
                        .map(|i| start_row + i as u64)
                        .collect();
                    self.table.columns[0].version_info.get_visibility_mask(
                        &row_ids,
                        tx.tx_id,
                        tx.read_ts,
                        &mut self.scratch_mask,
                    );
                }

                let all_visible = self.scratch_mask.iter().all(|&v| v);
                let any_visible = self.scratch_mask.iter().any(|&v| v);

                if !all_visible {
                    if !any_visible {
                        continue;
                    }
                    batch = arrow::compute::filter_record_batch(
                        &batch,
                        &BooleanArray::from(std::mem::take(&mut self.scratch_mask)),
                    )
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                }
            }

            // Filter out rows with null _id (deleted via DELETE or soft-delete)
            if batch.num_rows() > 0 && batch.num_columns() > 0 {
                let id_col = batch.column(0);
                if id_col.data_type() == &arrow::datatypes::DataType::UInt64
                    || id_col.data_type() == &arrow::datatypes::DataType::Int64
                {
                    let mut keep = Vec::with_capacity(batch.num_rows());
                    let mut any_null = false;
                    for i in 0..batch.num_rows() {
                        let is_valid = id_col.is_valid(i);
                        keep.push(is_valid);
                        if !is_valid {
                            any_null = true;
                        }
                    }
                    if any_null {
                        let all_valid = keep.iter().all(|&v| v);
                        if !all_valid {
                            let keep_arr = BooleanArray::from(keep);
                            batch = arrow::compute::filter_record_batch(&batch, &keep_arr)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        }
                    }
                }
            }

            if batch.num_rows() == 0 {
                continue;
            }

            if let Some(mask_lock) = &self.mask {
                let mask = mask_lock.read();
                let mut filter_mask = Vec::with_capacity(batch.num_rows());
                let mut all_match_int = 1u8;
                let mut any_match_int = 0u8;
                if let Some(col_idx) = self.mask_column_idx {
                    let col = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| crate::LightningError::Internal(
                            "mask column must be UInt64Array".into(),
                        ))?;
                    for i in 0..batch.num_rows() {
                        let m = mask.contains(col.value(i));
                        filter_mask.push(m);
                        all_match_int &= m as u8;
                        any_match_int |= m as u8;
                    }
                } else {
                    for i in 0..batch.num_rows() {
                        let m = mask.contains(start_row + i as u64);
                        filter_mask.push(m);
                        all_match_int &= m as u8;
                        any_match_int |= m as u8;
                    }
                }

                let all_match = all_match_int != 0;
                let any_match = any_match_int != 0;

                if !all_match {
                    if !any_match {
                        continue;
                    }
                    batch = arrow::compute::filter_record_batch(
                        &batch,
                        &BooleanArray::from(filter_mask),
                    )
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                }
            }

            if batch.num_rows() == 0 {
                continue;
            }

            if !filter_all_pass {
                if let Some(ref filter_expr) = self.pushdown_filter {
                    let mask_result = crate::processor::evaluator::ExpressionEvaluator::evaluate(
                        filter_expr,
                        Some(&batch),
                        params,
                        batch.num_rows(),
                        &database.function_registry,
                        database,
                    );
                    match mask_result {
                        Ok(mask_arr) => {
                            let mask = mask_arr.as_any().downcast_ref::<BooleanArray>()
                                .ok_or_else(|| crate::LightningError::Internal(
                                    "filter expression must evaluate to BooleanArray".into(),
                                ))?;

                            let set_bits = mask.values().count_set_bits();
                            if set_bits == 0 {
                                continue;
                            }

                            if mask.null_count() > 0 || set_bits != mask.len() {
                                batch = arrow::compute::filter_record_batch(&batch, mask)
                                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                            }
                        }
                        Err(_) => {
                            tracing::debug!(
                                "Pushdown filter evaluation skipped (index mismatch — filter will be applied at join level)"
                            );
                        }
                    }
                }
            }

            if batch.num_rows() == 0 {
                continue;
            }

            return Ok(Some(DataChunk { batch }));
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(self.clone())
    }

    fn is_parallel_safe(&self) -> bool {
        true
    }

    fn set_partition(&mut self, index: usize, total: usize) {
        let num_rows = self.state.num_rows;
        let rows_per = num_rows / total as u64;
        let rem = num_rows % total as u64;
        let start = rows_per * index as u64 + std::cmp::min(index as u64, rem);
        let end = start + rows_per + if (index as u64) < rem { 1 } else { 0 };
        self.partition_start_row = start;
        self.partition_end_row = end;
        self.partition_position = Arc::new(AtomicU64::new(start));
    }
}

pub struct PhysicalSingleRow {
    done: Arc<std::sync::atomic::AtomicBool>,
}
impl Default for PhysicalSingleRow {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalSingleRow {
    pub fn new() -> Self {
        Self {
            done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}
impl PhysicalOperator for PhysicalSingleRow {
    fn get_next(
        &mut self,
        _database: &crate::Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.done.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        let batch = RecordBatch::try_new_with_options(
            Arc::new(arrow::datatypes::Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(Some(DataChunk { batch }))
    }
    fn is_single_row(&self) -> bool {
        true
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            done: self.done.clone(),
        })
    }
}
