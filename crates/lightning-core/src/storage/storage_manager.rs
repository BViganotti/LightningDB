use crate::processor::Value;
use crate::storage::buffer_manager::BufferManager;
use crate::storage::column::Column;
use crate::storage::compression::CompressionType;
use crate::storage::file_handle::FileHandle;
use crate::storage::free_space_manager::FreeSpaceManager;
use crate::storage::index::hash_index::HashIndex;
use crate::storage::row_version::RowVersion;
use crate::storage::stats::TableStats;
use crate::storage::trigram_index_worker::TrigramIndexWorker;
use crate::transaction::transaction_manager::Transaction;
use crate::Result;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use lightning_types::LogicalType;
use parking_lot::RwLock as PlRwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

const DEFAULT_WRITE_BUFFER_THRESHOLD: usize = 200;

struct WriteBuffer {
    /// Column-oriented buffer: columns[col_idx] = Vec of values for that column.
    columns: Vec<Vec<Value>>,
    row_ids: Vec<u64>,
    size_bytes: usize,
}

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub stats: Arc<PlRwLock<TableStats>>,
    pub version_info: Arc<RowVersion>,
    pub next_row_id: Arc<AtomicU64>,
    pub trigram_indexes:
        Arc<PlRwLock<HashMap<String, Arc<crate::storage::index::trigram_index::TrigramIndex>>>>,
    pub trigram_workers: Arc<PlRwLock<HashMap<String, Arc<TrigramIndexWorker>>>>,
    write_buffer: PlRwLock<Option<WriteBuffer>>,
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: self.columns.clone(),
            stats: Arc::clone(&self.stats),
            version_info: Arc::clone(&self.version_info),
            next_row_id: Arc::clone(&self.next_row_id),
            trigram_indexes: Arc::clone(&self.trigram_indexes),
            trigram_workers: Arc::clone(&self.trigram_workers),
            write_buffer: PlRwLock::new(None),
        }
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>, stats: Arc<PlRwLock<TableStats>>) -> Self {
        Self {
            name,
            columns,
            stats,
            version_info: Arc::new(RowVersion::new()),
            next_row_id: Arc::new(AtomicU64::new(0)),
            trigram_indexes: Arc::new(PlRwLock::new(HashMap::new())),
            trigram_workers: Arc::new(PlRwLock::new(HashMap::new())),
            write_buffer: PlRwLock::new(None),
        }
    }

    fn ensure_buffer(&self) {
        let mut guard = self.write_buffer.write();
        if guard.is_none() {
            let num_cols = self.columns.len();
            let mut columns = Vec::with_capacity(num_cols);
            for _ in 0..num_cols {
                columns.push(Vec::with_capacity(DEFAULT_WRITE_BUFFER_THRESHOLD));
            }
            *guard = Some(WriteBuffer {
                columns,
                row_ids: Vec::with_capacity(DEFAULT_WRITE_BUFFER_THRESHOLD),
                size_bytes: 0,
            });
        }
    }

    fn flush_buffer(&self, bm: &BufferManager, tx: &Transaction) -> Result<()> {
        let (columns, row_ids) = {
            let mut guard = self.write_buffer.write();
            if let Some(ref mut wb) = *guard {
                if wb.columns.is_empty() || wb.columns[0].is_empty() {
                    return Ok(());
                }
                let row_ids = std::mem::take(&mut wb.row_ids);
                let columns = std::mem::take(&mut wb.columns);
                wb.size_bytes = 0;
                // Re-initialize column vecs for next batch
                let num_cols = self.columns.len();
                for _ in 0..num_cols {
                    wb.columns.push(Vec::with_capacity(DEFAULT_WRITE_BUFFER_THRESHOLD));
                }
                (columns, row_ids)
            } else {
                return Ok(());
            }
        };

        if columns.is_empty() || columns[0].is_empty() {
            return Ok(());
        }

        let num_rows = columns[0].len();
        let start_id = row_ids[0];

        // Phase 1.1: Convert column-oriented buffer to Arrow arrays
        let num_cols = self.columns.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let col = &self.columns[col_idx];
            let col_values = &columns[col_idx];
            let arr: ArrayRef = match col.data_type {
                LogicalType::String => {
                    let mut builder =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for val in col_values {
                        match val {
                            Value::String(s) => builder.append_value(s),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Int64
                | LogicalType::Int32
                | LogicalType::Int16
                | LogicalType::Int8 => {
                    let mut builder = arrow::array::Int64Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Number(n) => builder.append_value(*n as i64),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Double | LogicalType::Float => {
                    let mut builder = arrow::array::Float64Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Number(n) => builder.append_value(*n),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Bool => {
                    let mut builder = arrow::array::BooleanBuilder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Boolean(b) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Node(_) => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Node(id) => builder.append_value(*id),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Rel(_) => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Relationship(id) => builder.append_value(*id),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Uint64
                | LogicalType::Uint32
                | LogicalType::Uint16
                | LogicalType::Uint8 => {
                    let mut builder = arrow::array::UInt64Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Node(id) => builder.append_value(*id),
                            Value::Number(n) => builder.append_value(*n as u64),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Date => {
                    let mut builder = arrow::array::Date32Builder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Date(d) => builder.append_value(*d),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                LogicalType::Timestamp => {
                    let mut builder =
                        arrow::array::TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for val in col_values {
                        match val {
                            Value::Timestamp(t) => builder.append_value(*t),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for val in col_values {
                        builder.append_value(val.to_string());
                    }
                    Arc::new(builder.finish())
                }
            };
            arrays.push(arr);
        }

        // Build RecordBatch without _id column (for internal storage)
        let fields: Vec<_> = self.columns.iter().map(|c| c.to_field()).collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;

        // Phase 1.2: Register bulk_row_range once for entire batch
        let version_info = if !self.columns.is_empty() {
            let vi = self.columns[0].version_info.clone();
            tx.bulk_row_ranges
                .lock()
                .push((vi, start_id, start_id + num_rows as u64));
            Some(())
        } else {
            None
        };

        // Phase 1.3: Use bulk_append_array_bulk_mode for efficient writes
        for (i, col) in self.columns.iter().enumerate() {
            if i < batch.num_columns() {
                col.bulk_append_array_bulk_mode(
                    bm,
                    batch.column(i),
                    start_id,
                    tx,
                    version_info.is_some(),
                )?;
            }
        }

        // Async trigram indexing via workers (Phase 3 optimization)
        self.bulk_append_trigram_batch(start_id, &batch)?;

        self.stats.write().cardinality += num_rows as u64;
        Ok(())
    }

    pub fn flush_pending(&self, bm: &BufferManager, tx: &Transaction) -> Result<()> {
        self.flush_buffer(bm, tx)
    }

    pub fn append_row(
        &self,
        bm: &BufferManager,
        values: &[Value],
        next_id: u64,
        tx: &Transaction,
    ) -> Result<()> {
        self.ensure_buffer();

        let should_flush = {
            let mut guard = self.write_buffer.write();
            if let Some(ref mut wb) = *guard {
                for (col_idx, val) in values.iter().enumerate() {
                    if col_idx < wb.columns.len() {
                        if let Value::String(ref s) = val {
                            wb.size_bytes += s.len();
                        }
                        wb.columns[col_idx].push(val.clone());
                    } else {
                        tracing::warn!(
                            "append_row: ignoring extra value at column index {} (table has {} columns)",
                            col_idx, wb.columns.len()
                        );
                    }
                }
                wb.row_ids.push(next_id);
                wb.columns.first().map(|c| c.len()).unwrap_or(0) >= DEFAULT_WRITE_BUFFER_THRESHOLD
            } else {
                false
            }
        };

        if should_flush {
            self.flush_buffer(bm, tx)?;
        }

        // Stats are updated in flush_buffer to avoid double-counting.
        // append_row buffers the row and flush_buffer increments cardinality
        // for the entire batch when flushed.
        Ok(())
    }

    pub fn batch_append_rows(
        &self,
        bm: &BufferManager,
        rows: &[Vec<crate::processor::Value>],
        start_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let num_cols = self.columns.len();
        let num_rows = rows.len();
        if num_rows == 0 {
            return Ok(());
        }
        let mut all_modified_rows = Vec::new();
        for col_idx in 0..num_cols {
            let mut vals = Vec::with_capacity(num_rows);
            for row in rows {
                vals.push(if col_idx < row.len() {
                    row[col_idx].clone()
                } else {
                    crate::processor::Value::Null
                });
            }
            let col_modified = self.columns[col_idx].batch_append_values(bm, &vals, start_id, tx)?;
            all_modified_rows.extend(col_modified);
        }
        if !all_modified_rows.is_empty() {
            tx.modified_rows.lock().extend(all_modified_rows);
        }
        if let Some(workers) = self.trigram_workers.try_read() {
            if !workers.is_empty() {
                let mut entries_by_worker: HashMap<String, Vec<(u64, String)>> = HashMap::new();
                for (row_idx, row) in rows.iter().enumerate() {
                    let row_id = start_id + row_idx as u64;
                    for (col_idx, val) in row.iter().enumerate() {
                        if col_idx >= num_cols {
                            break;
                        }
                        if let crate::processor::Value::String(s) = val {
                            if let Some(_worker) = workers.get(&self.columns[col_idx].name) {
                                entries_by_worker
                                    .entry(self.columns[col_idx].name.clone())
                                    .or_default()
                                    .push((row_id, s.clone()));
                            }
                        }
                    }
                }
                for (col_name, entries) in entries_by_worker {
                    if let Some(worker) = workers.get(&col_name) {
                        worker.insert_batch(entries);
                    }
                }
                for worker in workers.values() {
                    worker.flush();
                }
            }
        }
        self.stats.write().cardinality += num_rows as u64;
        Ok(())
    }

    pub fn bulk_append_batch(
        &self,
        bm: &BufferManager,
        batch: &RecordBatch,
        start_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Register bulk row range once (avoids per-row tracking)
        let version_info = if !self.columns.is_empty() {
            let vi = self.columns[0].version_info.clone();
            tx.bulk_row_ranges
                .lock()
                .push((vi.clone(), start_id, start_id + num_rows as u64));
            Some(vi)
        } else {
            None
        };

        // Parallelize column appends (only for larger batches to avoid overhead)
        if self.columns.len() >= 3 && num_rows >= 1000 {
            use rayon::prelude::*;
            self.columns
                .par_iter()
                .enumerate()
                .try_for_each(|(i, col)| {
                    if i < batch.num_columns() {
                        let array = batch.column(i);
                        col.bulk_append_array_bulk_mode(
                            bm,
                            array,
                            start_id,
                            tx,
                            version_info.is_some(),
                        )?;
                    }
                    Ok::<(), crate::LightningError>(())
                })?;
        } else {
            // Sequential for small batches (less overhead)
            for (i, col) in self.columns.iter().enumerate() {
                if i < batch.num_columns() {
                    let array = batch.column(i);
                    col.bulk_append_array_bulk_mode(
                        bm,
                        array,
                        start_id,
                        tx,
                        version_info.is_some(),
                    )?;
                }
            }
        }

        self.stats.write().cardinality += num_rows as u64;
        Ok(())
    }

    pub fn get_schema(&self) -> Arc<arrow::datatypes::Schema> {
        let fields: Vec<_> = self.columns.iter().map(|c| c.to_field()).collect();
        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    pub fn optimize(
        &mut self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        for col in &mut self.columns {
            col.optimize(bm, tx, true)?;
        }
        Ok(())
    }

    pub fn update_column_stats(&self) {
        let mut table_stats = self.stats.write();
        table_stats.column_stats.clear();
        for col in &self.columns {
            table_stats.column_stats.push(col.stats.read().clone());
        }
    }

    pub fn bulk_append_trigram_batch(&self, start_id: u64, batch: &RecordBatch) -> Result<()> {
        let workers = self.trigram_workers.read();
        for (col_idx, col) in self.columns.iter().enumerate() {
            if col.data_type == LogicalType::String {
                if let Some(worker) = workers.get(&col.name) {
                    if let Some(str_arr) =
                        batch.column(col_idx).as_any().downcast_ref::<StringArray>()
                    {
                        let mut entries = Vec::with_capacity(batch.num_rows());
                        for i in 0..batch.num_rows() {
                            if str_arr.is_valid(i) {
                                entries.push((start_id + i as u64, str_arr.value(i).to_string()));
                            }
                        }
                        if !entries.is_empty() {
                            worker.insert_batch(entries);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn flush_trigram_workers(&self) {
        let workers = self.trigram_workers.read();
        for worker in workers.values() {
            worker.flush();
        }
    }
}

pub struct StorageManager {
    pub db_path: PathBuf,
    pub data_fh: Arc<FileHandle>,
    pub overflow_fh: Arc<FileHandle>,
    pub node_tables: HashMap<String, Table>,
    pub rel_tables: HashMap<String, Table>,
    pub indexes: HashMap<String, Arc<HashIndex>>,
    pub fts_indexes: HashMap<String, Arc<crate::storage::index::inverted_index::InvertedIndex>>,
    pub vector_indexes: HashMap<String, Arc<crate::storage::index::vector_index::VectorIndex>>,
    pub fwd_csr: HashMap<String, Arc<crate::storage::index::csr::CSRIndex>>,
    pub bwd_csr: HashMap<String, Arc<crate::storage::index::csr::CSRIndex>>,
    csr_cardinalities: PlRwLock<HashMap<String, u64>>,
    file_handles: HashMap<u64, Arc<FileHandle>>,
    free_space_manager: Option<Arc<FreeSpaceManager>>,
}

impl StorageManager {
    pub fn new(path: &Path) -> Result<Self> {
        let data_path = path.join("data.lbug");
        let data_fh = Arc::new(FileHandle::open(&data_path)?);
        let overflow_path = path.join("overflow.lbug");
        let overflow_fh = Arc::new(FileHandle::open(&overflow_path)?);
        Ok(Self {
            db_path: path.to_path_buf(),
            data_fh,
            overflow_fh,
            node_tables: HashMap::new(),
            rel_tables: HashMap::new(),
            indexes: HashMap::new(),
            fts_indexes: HashMap::new(),
            vector_indexes: HashMap::new(),
            fwd_csr: HashMap::new(),
            bwd_csr: HashMap::new(),
            csr_cardinalities: PlRwLock::new(HashMap::new()),
            file_handles: HashMap::new(),
            free_space_manager: None,
        })
    }

    pub fn set_free_space_manager(&mut self, fsm: Arc<FreeSpaceManager>) {
        self.free_space_manager = Some(Arc::clone(&fsm));
        for fh in self.get_all_file_handles() {
            fh.set_free_space_manager(Arc::clone(&fsm));
        }
    }

    pub fn set_fsm_on_all_file_handles(&self) {
        if let Some(ref fsm) = self.free_space_manager {
            for fh in self.get_all_file_handles() {
                fh.set_free_space_manager(Arc::clone(fsm));
            }
        }
    }

    pub fn create_vector_index(&mut self, table_name: &str, dim: usize) -> Result<()> {
        let index_path = self.db_path.join(format!("{table_name}_vector.lbug"));
        let fh = Arc::new(FileHandle::open(&index_path)?);
        self.file_handles.insert(fh.file_id, Arc::clone(&fh));
        let index = Arc::new(crate::storage::index::vector_index::VectorIndex::new(fh, dim));
        self.vector_indexes.insert(table_name.to_string(), index);
        Ok(())
    }

    pub fn create_fts_index(&mut self, table_name: &str) -> Result<()> {
        let index_path = self.db_path.join(format!("{table_name}_fts"));
        let index = Arc::new(crate::storage::index::inverted_index::InvertedIndex::new(
            &index_path,
            &[],
        )?);
        self.fts_indexes.insert(table_name.to_string(), index);
        Ok(())
    }

    pub fn create_csr(&mut self, table_name: &str) -> Result<()> {
        let fwd_offset_path = self.db_path.join(format!("{table_name}_fwd_offset.lbug"));
        let fwd_adj_path = self.db_path.join(format!("{table_name}_fwd_adj.lbug"));
        let bwd_offset_path = self.db_path.join(format!("{table_name}_bwd_offset.lbug"));
        let bwd_adj_path = self.db_path.join(format!("{table_name}_bwd_adj.lbug"));

        let fwd_off_fh = Arc::new(FileHandle::open(&fwd_offset_path)?);
        let fwd_adj_fh = Arc::new(FileHandle::open(&fwd_adj_path)?);
        let bwd_off_fh = Arc::new(FileHandle::open(&bwd_offset_path)?);
        let bwd_adj_fh = Arc::new(FileHandle::open(&bwd_adj_path)?);

        self.file_handles.insert(fwd_off_fh.file_id, Arc::clone(&fwd_off_fh));
        self.file_handles.insert(fwd_adj_fh.file_id, Arc::clone(&fwd_adj_fh));
        self.file_handles.insert(bwd_off_fh.file_id, Arc::clone(&bwd_off_fh));
        self.file_handles.insert(bwd_adj_fh.file_id, Arc::clone(&bwd_adj_fh));

        let fwd = crate::storage::index::csr::CSRIndex::new(fwd_off_fh, fwd_adj_fh);
        let bwd = crate::storage::index::csr::CSRIndex::new(bwd_off_fh, bwd_adj_fh);

        self.fwd_csr.insert(table_name.to_string(), Arc::new(fwd));
        self.bwd_csr.insert(table_name.to_string(), Arc::new(bwd));
        Ok(())
    }

    pub fn create_table(
        &mut self,
        name: String,
        column_definitions: Vec<(String, LogicalType)>,
        is_rel: bool,
        stats: Option<crate::storage::stats::TableStats>,
    ) -> Result<()> {
        let mut columns = Vec::new();
        let version_info = Arc::new(RowVersion::new());
        if !is_rel {
            let col_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{}_{}.lbug", name, "_id")),
            )?);
            self.file_handles
                .insert(col_fh.file_id, Arc::clone(&col_fh));
            let null_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{}_{}_null.lbug", name, "_id")),
            )?);
            self.file_handles
                .insert(null_fh.file_id, Arc::clone(&null_fh));
            columns.push(Column::new(
                "_id".to_string(),
                LogicalType::Uint64,
                null_fh,
                col_fh,
                None,
                version_info.clone(),
            ));
            for (col_name, col_type) in &column_definitions {
                if col_name != "_id" {
                    let col = self.create_column_recursive(
                        &name,
                        col_name,
                        col_type.clone(),
                        version_info.clone(),
                    )?;
                    columns.push(col);
                }
            }
        } else {
            // Always add _src as the first rel column, regardless of whether
            // column_definitions already contains it (e.g. when restoring from
            // catalog on Database::new()). If _src IS in column_definitions,
            // skip it during the user-column iteration below.
            // Without this, a rel table restored from catalog on restart would
            // have only user columns (type, weight, etc.) and no _src/_dst,
            // causing PhysicalScan to fail with missing system columns.
            let src_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{name}_src.lbug")),
            )?);
            self.file_handles
                .insert(src_fh.file_id, Arc::clone(&src_fh));
            let src_null_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{name}_src_null.lbug")),
            )?);
            self.file_handles
                .insert(src_null_fh.file_id, Arc::clone(&src_null_fh));
            columns.push(Column::new(
                "_src".to_string(),
                LogicalType::Uint64,
                src_null_fh,
                src_fh,
                None,
                version_info.clone(),
            ));

            let dst_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{name}_dst.lbug")),
            )?);
            self.file_handles
                .insert(dst_fh.file_id, Arc::clone(&dst_fh));
            let dst_null_fh = Arc::new(FileHandle::open(
                &self.db_path.join(format!("{name}_dst_null.lbug")),
            )?);
            self.file_handles
                .insert(dst_null_fh.file_id, Arc::clone(&dst_null_fh));
            columns.push(Column::new(
                "_dst".to_string(),
                LogicalType::Uint64,
                dst_null_fh,
                dst_fh,
                None,
                version_info.clone(),
            ));

            for (col_name, col_type) in &column_definitions {
                if col_name != "_src" && col_name != "_dst" {
                    let col = self.create_column_recursive(
                        &name,
                        col_name,
                        col_type.clone(),
                        version_info.clone(),
                    )?;
                    columns.push(col);
                }
            }
        }
        let table_stats = stats.unwrap_or_else(|| TableStats::new(0));

        // Restore persisted ColumnStats from catalog to Column objects.
        // This includes num_values, null_count, and compression_meta (e.g.,
        // Constant for all-same-value columns, or IntegerBitpacking for
        // compressed int columns). Without this:
        //   - Columns optimized before restart would lose compression_meta,
        //     causing misreads of compressed pages as uncompressed data.
        //   - num_values would start at 0, causing optimize() to skip analysis.
        if !table_stats.column_stats.is_empty() {
            for (col, cat_stat) in columns.iter().zip(table_stats.column_stats.iter()) {
                let mut stats = col.stats.write();
                stats.num_values = cat_stat.num_values;
                stats.null_count = cat_stat.null_count;
                stats.min = cat_stat.min.clone();
                stats.max = cat_stat.max.clone();
                stats.distinct_count = cat_stat.distinct_count;
                stats.page_bounds = cat_stat.page_bounds.clone();
                if let Some(ref meta) = cat_stat.compression_meta {
                    if meta.compression != CompressionType::Uncompressed {
                        stats.compression_meta = Some(meta.clone());
                    }
                }
            }
        }

        let table = Table::new(name.clone(), columns, Arc::new(PlRwLock::new(table_stats)));

        if !is_rel {
            let mut workers = table.trigram_workers.write();
            for col in &table.columns {
                if col.data_type == LogicalType::String {
                    let idx = Arc::new(crate::storage::index::trigram_index::TrigramIndex::new(
                        col.name.clone(),
                    ));
                    table
                        .trigram_indexes
                        .write()
                        .insert(col.name.clone(), Arc::clone(&idx));
                    let worker = Arc::new(TrigramIndexWorker::new(idx)?);
                    workers.insert(col.name.clone(), worker);
                }
            }
            drop(workers);
        }

        if is_rel {
            self.rel_tables.insert(name, table);
        } else {
            self.node_tables.insert(name, table);
        }
        Ok(())
    }

    pub fn create_index(&mut self, table_name: &str) -> Result<()> {
        let index_path = self.db_path.join(format!("{table_name}_pk_index.lbug"));
        let index = HashIndex::open_or_create(&index_path)?;
        self.indexes.insert(table_name.to_string(), Arc::new(index));
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.node_tables
            .get(name)
            .or_else(|| self.rel_tables.get(name))
    }

    pub fn get_index(&self, table_name: &str) -> Option<Arc<HashIndex>> {
        self.indexes.get(table_name).cloned()
    }

    pub fn get_file_handle(&self, file_id: u64) -> Option<Arc<FileHandle>> {
        self.file_handles.get(&file_id).cloned()
    }

    fn create_column_recursive(
        &mut self,
        table_name: &str,
        col_name: &str,
        col_type: LogicalType,
        version_info: Arc<RowVersion>,
    ) -> Result<Column> {
        let path = self
            .db_path
            .join(format!("{table_name}_{col_name}.lbug"));
        let fh = Arc::new(FileHandle::open(&path)?);
        self.file_handles.insert(fh.file_id, Arc::clone(&fh));
        let null_path = self
            .db_path
            .join(format!("{table_name}_{col_name}_null.lbug"));
        let null_fh = Arc::new(FileHandle::open(&null_path)?);
        self.file_handles
            .insert(null_fh.file_id, Arc::clone(&null_fh));
        let mut children = Vec::new();
        match &col_type {
            LogicalType::List(child) => {
                children.push(self.create_column_recursive(
                    table_name,
                    &format!("{col_name}_child"),
                    *child.clone(),
                    version_info.clone(),
                )?);
            }
            LogicalType::Struct(fields) => {
                for field in fields {
                    children.push(self.create_column_recursive(
                        table_name,
                        &format!("{}_{}", col_name, field.name),
                        field.type_.clone(),
                        version_info.clone(),
                    )?);
                }
            }
            LogicalType::Map(key, value) => {
                children.push(self.create_column_recursive(
                    table_name,
                    &format!("{col_name}_key"),
                    *key.clone(),
                    version_info.clone(),
                )?);
                children.push(self.create_column_recursive(
                    table_name,
                    &format!("{col_name}_value"),
                    *value.clone(),
                    version_info.clone(),
                )?);
            }
            _ => {}
        }
        let mut col = Column::with_children(
            col_name.to_string(),
            col_type,
            null_fh,
            fh,
            None,
            version_info,
            children,
        );
        if col.data_type == LogicalType::String {
            col = col.with_overflow(Arc::clone(&self.overflow_fh));
        }
        Ok(col)
    }

    pub fn remove_table(&mut self, name: &str) {
        // Collect file IDs from the table's columns before removing
        let mut file_ids_to_remove: Vec<u64> = Vec::new();
        if let Some(table) = self.node_tables.get(name) {
            table.trigram_workers.write().clear();
            for col in &table.columns {
                file_ids_to_remove.push(col.fh.file_id);
                file_ids_to_remove.push(col.null_fh.file_id);
                if let Some(ref ofh) = col.overflow_fh {
                    file_ids_to_remove.push(ofh.file_id);
                }
            }
        }
        if let Some(table) = self.rel_tables.get(name) {
            table.trigram_workers.write().clear();
            for col in &table.columns {
                file_ids_to_remove.push(col.fh.file_id);
                file_ids_to_remove.push(col.null_fh.file_id);
                if let Some(ref ofh) = col.overflow_fh {
                    file_ids_to_remove.push(ofh.file_id);
                }
            }
        }
        self.node_tables.remove(name);
        self.rel_tables.remove(name);
        self.indexes.remove(name);
        self.fts_indexes.remove(name);
        self.vector_indexes.remove(name);
        self.fwd_csr.remove(name);
        self.bwd_csr.remove(name);
        // Clean up file handles for removed table
        for fid in file_ids_to_remove {
            self.file_handles.remove(&fid);
        }
    }

    pub fn add_column_to_table(
        &mut self,
        table_name: &str,
        col_name: &str,
        col_type: LogicalType,
    ) -> Result<()> {
        let version_info = self
            .node_tables
            .get(table_name)
            .or_else(|| self.rel_tables.get(table_name))
            .map(|t| t.version_info.clone())
            .ok_or_else(|| crate::LightningError::Database(format!("Table '{}' not found", table_name)))?;

        // Check for duplicate column
        let has_dup = self
            .node_tables
            .get(table_name)
            .or_else(|| self.rel_tables.get(table_name))
            .is_some_and(|t| t.columns.iter().any(|c| c.name == col_name));
        if has_dup {
            return Err(crate::LightningError::Database(format!(
                "Column '{}' already exists in table '{}'",
                col_name, table_name
            )));
        }

        let col = self.create_column_recursive(table_name, col_name, col_type, version_info)?;

        if let Some(ref fsm) = self.free_space_manager {
            let mut fhs = Vec::new();
            self.collect_fhs_recursive(&col, &mut fhs);
            for fh in fhs {
                fh.set_free_space_manager(Arc::clone(fsm));
            }
        }

        let table = self
            .node_tables
            .get_mut(table_name)
            .or_else(|| self.rel_tables.get_mut(table_name))
            .ok_or_else(|| crate::LightningError::Database(format!("Table '{}' not found", table_name)))?;
        table.columns.push(col);
        Ok(())
    }

    pub fn remove_column_from_table(
        &mut self,
        table_name: &str,
        col_name: &str,
    ) -> Result<()> {
        let table = self
            .node_tables
            .get_mut(table_name)
            .or_else(|| self.rel_tables.get_mut(table_name))
            .ok_or_else(|| crate::LightningError::Database(format!("Table '{}' not found", table_name)))?;

        let idx = table
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| {
                crate::LightningError::Database(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        table.columns.remove(idx);
        Ok(())
    }

    pub fn get_all_file_handles(&self) -> Vec<Arc<FileHandle>> {
        // Use the centrally tracked file_handles map instead of walking the
        // entire column tree recursively (which clones every child Arc).
        let mut fhs: Vec<Arc<FileHandle>> = self.file_handles.values().cloned().collect();
        fhs.push(Arc::clone(&self.data_fh));
        fhs.push(Arc::clone(&self.overflow_fh));
        fhs
    }

    pub fn flush_all_pending(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let any_pending = self
            .node_tables
            .values()
            .chain(self.rel_tables.values())
            .any(|table| {
                let guard = table.write_buffer.read();
                matches!(&*guard, Some(ref wb) if wb.columns.first().map(|c| !c.is_empty()).unwrap_or(false))
            });

        if any_pending {
            for table in self.node_tables.values().chain(self.rel_tables.values()) {
                table.flush_pending(bm, tx)?;
            }
        }
        Ok(())
    }

    /// Sync all column data files to disk.
    ///
    /// # Ordering invariant
    ///
    /// This method MUST be called *before* WAL truncation: column data files
    /// must be fully durable on stable storage before the WAL is truncated,
    /// otherwise a crash between `sync_all_data_files` and `WAL::truncate`
    /// would lose committed page updates with no WAL record to replay them
    /// from. Callers (e.g. `BufferManager::checkpoint`) guarantee this order
    /// by syncing data first, then truncating the WAL. Violating this order
    /// may produce an unrecoverable database on the next open.
    pub fn sync_all_data_files(&self) -> Result<()> {
        for table in self.node_tables.values().chain(self.rel_tables.values()) {
            for col in &table.columns {
                self.sync_column_files(col)?;
            }
        }
        Ok(())
    }

    fn sync_column_files(&self, col: &Column) -> Result<()> {
        if col.dirty.swap(false, std::sync::atomic::Ordering::AcqRel) {
            col.fh.sync()?;
            col.null_fh.sync()?;
        }
        for child in &col.child_columns {
            self.sync_column_files(child)?;
        }
        Ok(())
    }

    fn collect_fhs_recursive(&self, col: &Column, fhs: &mut Vec<Arc<FileHandle>>) {
        fhs.push(Arc::clone(&col.fh));
        fhs.push(Arc::clone(&col.null_fh));
        for child in &col.child_columns {
            self.collect_fhs_recursive(child, fhs);
        }
    }

    pub fn mark_csr_stale(&self, table_name: &str) {
        self.csr_cardinalities.write().remove(table_name);
    }

    /// Ensure the CSR for the given table is up-to-date. If stale, rebuild it.
    /// Call this before using the CSR for query execution.
    pub fn ensure_csr_fresh(
        &self,
        table_name: &str,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let has_csr = self.fwd_csr.contains_key(table_name) || self.bwd_csr.contains_key(table_name);
        if !has_csr {
            return Ok(());
        }
        let current_cardinality = match self.get_table(table_name) {
            Some(t) => t.stats.read().cardinality,
            None => return Ok(()),
        };
        let last_rebuilt = self.csr_cardinalities.read().get(table_name).copied().unwrap_or(0);
        if current_cardinality == last_rebuilt {
            return Ok(());
        }
        self.rebuild_csr(table_name, bm, tx)?;
        self.csr_cardinalities.write().insert(table_name.to_string(), current_cardinality);
        Ok(())
    }

    pub fn rebuild_csr_if_stale(
        &self,
        table_name: &str,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let has_csr = self.fwd_csr.contains_key(table_name) || self.bwd_csr.contains_key(table_name);
        if !has_csr {
            return Ok(());
        }
        let current_cardinality = match self.get_table(table_name) {
            Some(t) => t.stats.read().cardinality,
            None => return Ok(()),
        };
        let last_rebuilt = self.csr_cardinalities.read().get(table_name).copied().unwrap_or(0);
        if current_cardinality == last_rebuilt {
            return Ok(());
        }
        self.rebuild_csr(table_name, bm, tx)?;
        self.csr_cardinalities.write().insert(table_name.to_string(), current_cardinality);
        Ok(())
    }

    pub fn apply_page(&mut self, file_id: u64, page_idx: u64, data: &[u8]) -> Result<()> {
        if let Some(fh) = self.file_handles.get(&file_id) {
            fh.write_page(page_idx, data)?;
        } else {
            tracing::warn!(
                "WAL replay: skipping page {} for unknown file_id {} (file may have been dropped)",
                page_idx, file_id
            );
        }
        Ok(())
    }

    pub fn rebuild_csr(
        &self,
        table_name: &str,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let table = self.get_table(table_name).ok_or_else(|| {
            crate::LightningError::Query(format!("Table {table_name} not found"))
        })?;
        let fwd_csr = self.fwd_csr.get(table_name);
        let bwd_csr = self.bwd_csr.get(table_name);

        if fwd_csr.is_none() && bwd_csr.is_none() {
            return Ok(());
        }

        // Scan the table to get all edges
        let num_rows = table.stats.read().cardinality;
        if num_rows == 0 {
            return Ok(());
        }

        if table.columns.is_empty() || table.columns.len() < 2 {
            tracing::warn!("rebuild_csr: table {} has {} columns, need 2; skipping", table_name, table.columns.len());
            return Ok(());
        }

        let mut src_ids = Vec::new();
        let mut dst_ids = Vec::new();

        table.columns[0].scan(bm, 0, num_rows, tx, &mut src_ids)?;
        table.columns[1].scan(bm, 0, num_rows, tx, &mut dst_ids)?;

        let mut edges = Vec::with_capacity(num_rows as usize);
        let mut max_node_id = 0;
        for (src, dst) in src_ids.into_iter().zip(dst_ids.into_iter()) {
            let s = src.as_node();
            let d = dst.as_node();
            edges.push((s, d));
            max_node_id = std::cmp::max(max_node_id, std::cmp::max(s, d));
        }

        if let Some(fwd) = fwd_csr {
            crate::storage::index::csr::CSRIndex::build(
                bm,
                fwd.offset_fh.clone(),
                fwd.adj_node_fh.clone(),
                &edges,
                max_node_id,
                tx,
            )?;
        }

        if let Some(bwd) = bwd_csr {
            let reversed_edges: Vec<(u64, u64)> = edges.iter().map(|(s, d)| (*d, *s)).collect();
            crate::storage::index::csr::CSRIndex::build(
                bm,
                bwd.offset_fh.clone(),
                bwd.adj_node_fh.clone(),
                &reversed_edges,
                max_node_id,
                tx,
            )?;
        }

        Ok(())
    }
}
