pub mod api;
pub mod catalog;
pub use api::*;
pub mod capi;
pub mod memory;
pub mod optimizer;
pub mod wasm_function;
pub mod parser;
pub mod planner;
pub mod processor;
pub mod storage;
pub mod transaction;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use lightning_types::LogicalType;
use parking_lot::RwLock;
pub use processor::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

fn normalize_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"'[^']*'"#).unwrap())
}

fn normalize_query(query: &str) -> String {
    normalize_re().replace_all(query, "'?'").into_owned()
}

use crate::catalog::{Catalog, LazyCatalog};
use crate::parser::parse;
use crate::planner::logical_plan::LogicalPlanner;
use crate::planner::Binder;
use crate::processor::physical_plan::PhysicalPlanner;
use crate::processor::Processor;
use crate::storage::WAL;
use crate::transaction::transaction_manager::Transaction;
use crate::transaction::TransactionManager;
use regex::Regex;

#[derive(Error, Debug)]
pub enum LightningError {
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Query error: {0}")]
    Query(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<arrow::error::ArrowError> for LightningError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, LightningError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Normal,
    Off,
}

impl Default for SyncMode {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub buffer_pool_size: u64,
    pub max_num_threads: u32,
    pub read_only: bool,
    pub sync_mode: SyncMode,
    pub vacuum_interval_ms: u64,
    pub prefetch_enabled: bool,
    pub prefetch_depth: usize,
    pub prefetch_confidence: f64,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 1024 * 1024 * 1024,
            max_num_threads: 0,
            read_only: false,
            sync_mode: SyncMode::Normal,
            vacuum_interval_ms: 1000,
            prefetch_enabled: true,
            prefetch_depth: 2,
            prefetch_confidence: 0.15,
        }
    }
}

pub struct Database {
    pub(crate) _path: PathBuf,
    pub(crate) _config: SystemConfig,
    pub storage_manager: Arc<RwLock<crate::storage::storage_manager::StorageManager>>,
    pub wal: Arc<WAL>,
    pub transaction_manager: Arc<TransactionManager>,
    pub buffer_manager: Arc<crate::storage::buffer_manager::BufferManager>,
    pub free_space_manager: Arc<crate::storage::FreeSpaceManager>,
    pub catalog: Arc<LazyCatalog>,
    pub function_registry: Arc<crate::processor::functions::FunctionRegistry>,
    pub header: RwLock<crate::storage::DatabaseHeader>,
    pub plan_cache: Arc<RwLock<HashMap<String, crate::planner::binder::BoundStatement>>>,
    pub physical_plan_cache: Arc<
        RwLock<HashMap<String, Arc<Box<dyn crate::processor::PhysicalOperator + Send + Sync>>>>,
    >,
    vacuum_handle: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.buffer_manager.shutdown();
        // Commit any pending FTS index data before shutdown
        {
            let sm = self.storage_manager.read();
            for fts in sm.fts_indexes.values() {
                let _ = fts.commit();
            }
        }
        let fhs = {
            let sm = self.storage_manager.read();
            sm.get_all_file_handles()
        };
        std::thread::sleep(std::time::Duration::from_millis(1200));
        self.buffer_manager.flush_all_with_handles(&fhs);
        std::thread::sleep(std::time::Duration::from_millis(200));
        drop(fhs);
    }
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P, config: SystemConfig) -> Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        let header_path = path.join("database.header");
        let header = if header_path.exists() {
            crate::storage::DatabaseHeader::load(&header_path)?
        } else {
            std::fs::create_dir_all(&path)?;
            let h = crate::storage::DatabaseHeader::new();
            h.save(&header_path)?;
            h
        };

        let wal = Arc::new(WAL::new(&path, config.sync_mode)?);
        let mut storage_manager = crate::storage::storage_manager::StorageManager::new(&path)?;

        let catalog_path = path.join("catalog.lbug");
        let catalog = Arc::new(
            LazyCatalog::from_disk(&catalog_path)
                .unwrap_or_else(|_| LazyCatalog::new(Catalog::new(), Some(catalog_path.clone()))),
        );

        {
            let cat = catalog.read();
            for table_entry in cat.node_tables.values() {
                let col_defs: Vec<(String, LogicalType)> = table_entry
                    .properties
                    .iter()
                    .map(|p| (p.name.clone(), p.type_.clone()))
                    .collect();
                let mut stats = table_entry.stats.clone();
                stats.cardinality = stats.cardinality.max(table_entry.num_rows);
                storage_manager.create_table(
                    table_entry.name.clone(),
                    col_defs,
                    false,
                    Some(stats),
                )?;
                if table_entry.primary_key.is_some() {
                    storage_manager.create_index(&table_entry.name)?;
                }
                if let Err(e) = storage_manager.create_fts_index(&table_entry.name) {
                    tracing::warn!("FTS index creation failed for {}: {}", table_entry.name, e);
                }
                let _ = storage_manager.create_vector_index(&table_entry.name);
            }
            for table_entry in cat.rel_tables.values() {
                let col_defs: Vec<(String, LogicalType)> = table_entry
                    .properties
                    .iter()
                    .map(|p| (p.name.clone(), p.type_.clone()))
                    .collect();
                let mut stats = table_entry.stats.clone();
                stats.cardinality = stats.cardinality.max(table_entry.num_rows);
                storage_manager.create_table(
                    table_entry.name.clone(),
                    col_defs,
                    true,
                    Some(stats),
                )?;
            }
        }

        // REPLAY WAL after tables are created so apply_page can find file handles
        wal.replay(
            |fid, pid, data| storage_manager.apply_page(fid, pid, data),
            header.last_checkpoint_ts,
        )?;

        let fsm_path = path.join("free_space.bin");
        let free_space_manager = Arc::new(
            crate::storage::FreeSpaceManager::load(&fsm_path)
                .unwrap_or_else(|_| crate::storage::FreeSpaceManager::new()),
        );

        let transaction_manager = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        let buffer_manager = Arc::new(crate::storage::buffer_manager::BufferManager::new(
            config.buffer_pool_size as usize / 4096,
            Some(Arc::clone(&wal)),
            config.prefetch_enabled,
            config.prefetch_depth,
            config.prefetch_confidence,
        ));

        let tm_clone = Arc::clone(&transaction_manager);
        let bm_clone = Arc::clone(&buffer_manager);
        let vacuum_interval_ms = std::cmp::max(100, config.vacuum_interval_ms);
        let vacuum_handle = std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(vacuum_interval_ms));
            if bm_clone.is_shutting_down() {
                bm_clone.flush_all();
                break;
            }
            let min_ts = tm_clone.get_min_active_read_ts();
            if let Err(e) = bm_clone.reclaim_expired_versions(min_ts) {
                tracing::warn!("Vacuum reclaim failed: {}", e);
            }
        });

        Ok(Arc::new(Self {
            _path: path,
            _config: config,
            storage_manager: Arc::new(RwLock::new(storage_manager)),
            wal,
            transaction_manager,
            buffer_manager,
            free_space_manager,
            catalog,
            function_registry: Arc::new(crate::processor::functions::FunctionRegistry::new()),
            header: RwLock::new(header),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            physical_plan_cache: Arc::new(RwLock::new(HashMap::new())),
            vacuum_handle: Some(vacuum_handle),
        }))
    }

    pub fn connect(self: &Arc<Self>) -> Connection {
        Connection::new(Arc::clone(self))
    }

    /// Register a WebAssembly function that can be called from Cypher queries.
    ///
    /// The WASM module must export a function `func_name` with signature
    /// `(f64) -> f64`. It will be registered as a scalar function available
    /// in any query on this database.
    ///
    /// Example usage in Cypher:
    ///   RETURN wasm_score(t.val)
    pub fn register_wasm_function<P: AsRef<std::path::Path>>(
        &self,
        wasm_path: P,
        func_name: &str,
    ) -> Result<()> {
        let wasm_func = crate::wasm_function::WasmFunction::load(wasm_path, func_name)?;
        let scalar = wasm_func.to_scalar_function();
        // SAFETY: register_wasm_function is called at initialization before
        // any concurrent access to the function registry.
        let registry_ptr = Arc::as_ptr(&self.function_registry) as *mut crate::processor::functions::FunctionRegistry;
        unsafe {
            (*registry_ptr).scalar_functions.insert(scalar.name.clone(), scalar);
        }
        tracing::info!("Registered WASM function: {}", func_name);
        Ok(())
    }

    pub fn get_catalog_path(&self) -> PathBuf {
        self._path.join("catalog.lbug")
    }

    pub fn checkpoint(&self) -> Result<()> {
        // Flush all dirty pages to disk and sync data files
        self.buffer_manager.checkpoint()?;

        // Persist free space map
        {
            let fsm_path = self._path.join("free_space.bin");
            let _ = self.free_space_manager.save(&fsm_path);
        }

        // Update the last checkpoint timestamp so recovery can skip these entries
        let last_ts = self.transaction_manager.get_current_ts();
        {
            let mut header = self.header.write();
            header.last_checkpoint_ts = last_ts;
            let header_path = self._path.join("database.header");
            header.save(&header_path)?;
        }

        Ok(())
    }

    /// Repair table cardinalities from actual data file sizes.
    /// Called after init_schema to fix databases where catalog cardinality
    /// was reset to 0 (e.g., by old versions of init_fusion_schema).
    pub fn repair_cardinalities(&self) -> Result<()> {
        let mut repairs: Vec<(String, u64, bool)> = Vec::new(); // name, actual_rows, is_rel
        {
            let storage = self.storage_manager.read();
            for (name, table) in &storage.node_tables {
                let stats = table.stats.read();
                if stats.cardinality == 0 && !table.columns.is_empty() {
                    let file_size = table.columns[0].fh.get_file_size();
                    let esize = table.columns[0].element_size();
                    if esize > 0 && file_size > 0 {
                        let actual = file_size / esize as u64;
                        if actual > 0 {
                            repairs.push((name.clone(), actual, false));
                        }
                    }
                }
            }
            for (name, table) in &storage.rel_tables {
                let stats = table.stats.read();
                if stats.cardinality == 0 && !table.columns.is_empty() {
                    let file_size = table.columns[0].fh.get_file_size();
                    let esize = table.columns[0].element_size();
                    if esize > 0 && file_size > 0 {
                        let actual = file_size / esize as u64;
                        if actual > 0 {
                            repairs.push((name.clone(), actual, true));
                        }
                    }
                }
            }
        }
        if repairs.is_empty() {
            return Ok(());
        }
        // Apply repairs (no lock held, safe to call force_save)
        {
            let storage = self.storage_manager.read();
            for (name, actual, _is_rel) in &repairs {
                if let Some(table) = storage.get_table(name) {
                    table.stats.write().cardinality = *actual;
                    tracing::info!("Repaired cardinality for {}: -> {}", name, actual);
                }
            }
        }
        {
            let mut cat = self.catalog.write();
            for (name, actual, is_rel) in &repairs {
                if *is_rel {
                    if let Some(e) = cat.get_rel_table_mut(name) {
                        e.num_rows = *actual;
                        e.stats.cardinality = *actual;
                    }
                } else {
                    if let Some(e) = cat.get_node_table_mut(name) {
                        e.num_rows = *actual;
                        e.stats.cardinality = *actual;
                    }
                }
            }
            self.catalog.mark_dirty();
        }
        // force_save no longer holds catalog lock
        self.catalog.force_save()?;
        tracing::info!("Catalog saved after cardinality repair");
        Ok(())
    }
}

pub struct ClientContext {
    pub database: Arc<Database>,
    pub active_query_id: AtomicU64,
    pub query_timeout_ms: u64,
    pub memory_quota: u64,
}

impl ClientContext {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            active_query_id: AtomicU64::new(0),
            query_timeout_ms: 0,
            memory_quota: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    #[serde(skip)]
    pub batches: Vec<RecordBatch>,
    pub error: Option<String>,
}

impl QueryResult {
    pub fn new_arrow(
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        batches: Vec<RecordBatch>,
    ) -> Self {
        Self {
            column_names,
            column_types,
            batches,
            error: None,
        }
    }
    pub fn new_error(msg: String) -> Self {
        Self {
            column_names: vec![],
            column_types: vec![],
            batches: vec![],
            error: Some(msg),
        }
    }
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
    pub fn error_message(&self) -> Option<String> {
        self.error.clone()
    }
}

pub struct Connection {
    pub client_context: Arc<ClientContext>,
    pub transaction: parking_lot::Mutex<Option<Arc<Transaction>>>,
    pub pending_tables: parking_lot::RwLock<Vec<String>>,
}

impl Connection {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            client_context: Arc::new(ClientContext::new(database)),
            transaction: parking_lot::Mutex::new(None),
            pending_tables: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn begin(&self) -> Result<()> {
        let mut guard = self.transaction.lock();
        if guard.is_some() {
            return Err(LightningError::Query("Transaction already active".into()));
        }
        let tx = self
            .client_context
            .database
            .transaction_manager
            .begin(false)?;
        *guard = Some(Arc::new(tx));
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let tx = {
            let mut guard = self.transaction.lock();
            guard.take()
        }
        .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &*self.client_context.database;

        self.client_context
            .database
            .storage_manager
            .read()
            .flush_all_pending(bm, &tx)?;

        self.client_context
            .database
            .transaction_manager
            .commit(&tx, bm, db)
    }

    pub fn fast_insert(&self, table_name: &str, rows: Vec<Vec<(String, Value)>>) -> Result<usize> {
        use arrow::array::*;

        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {} not found", table_name)))?
        };

        let num_rows = rows.len();
        if num_rows == 0 {
            return Ok(0);
        }

        let bm = db.buffer_manager.clone();
        let tx = db.transaction_manager.begin(false)?;

        let start_id = table
            .next_row_id
            .fetch_add(num_rows as u64, Ordering::SeqCst);

        // Build Arrow arrays per column (skip _id)
        let columns = &table.columns;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(columns.len());

        // _id column
        let id_values: UInt64Array = (start_id..start_id + num_rows as u64).collect();
        fields.push(arrow::datatypes::Field::new(
            "_id",
            arrow::datatypes::DataType::UInt64,
            false,
        ));
        arrays.push(Arc::new(id_values) as ArrayRef);

        // Data columns
        for col in columns.iter().skip(1) {
            let arr: ArrayRef = match col.data_type {
                lightning_types::LogicalType::String => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::String(s))) => builder.append_value(s),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Number(n))) => builder.append_value(*n as i64),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Double => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Number(n))) => builder.append_value(*n),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Bool => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Boolean(b))) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Node(_) => {
                    let mut builder = UInt64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Node(id))) => builder.append_value(*id),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Date => {
                    let mut builder = Date32Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Date(d))) => builder.append_value(*d),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Timestamp(t))) => builder.append_value(*t),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, v)) => builder.append_value(&v.to_string()),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
            };
            fields.push(col.to_field());
            arrays.push(arr);
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;

        table.bulk_append_batch(&bm, &batch, start_id, &tx)?;
        table.bulk_append_trigram_batch(start_id, &batch)?;

        // Primary key index
        let pk_idx = db
            .catalog
            .read()
            .get_node_table(table_name)
            .and_then(|t| t.primary_key.as_ref())
            .and_then(|pk| columns.iter().position(|c| c.name == pk.as_str()));

        let index_opt = db.storage_manager.read().get_index(table_name);
        if let (Some(index), Some(pk_col_idx)) = (&index_opt, pk_idx) {
            if pk_col_idx < batch.num_columns() {
                let pk_array = batch.column(pk_col_idx);
                if let Some(str_arr) = pk_array.as_any().downcast_ref::<StringArray>() {
                    for i in 0..num_rows {
                        if str_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::String(str_arr.value(i).to_string()),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                } else if let Some(int_arr) = pk_array.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..num_rows {
                        if int_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::Number(int_arr.value(i) as f64),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                }
            }
        }

        db.storage_manager.read().flush_all_pending(&bm, &tx)?;
        db.transaction_manager.commit(&tx, &bm, &db)?;

        // Update catalog
        {
            let mut cat = db.catalog.write();
            if let Some(entry) = cat.get_node_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            } else if let Some(entry) = cat.get_rel_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            }
        }

        Ok(num_rows)
    }

    pub fn rollback(&self) -> Result<()> {
        let tx = {
            let mut guard = self.transaction.lock();
            guard.take()
        }
        .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let db: &Database = &*self.client_context.database;
        self.client_context
            .database
            .transaction_manager
            .rollback(db, &tx)
    }

    pub fn query(&self, query_str: &str) -> Result<QueryResult> {
        self.execute(query_str, None)
    }

    /// Execute a query and return results as a streaming channel.
    /// Each `DataChunk` is sent as it becomes available, allowing the
    /// caller to process large result sets without buffering.
    ///
    /// The receiver yields `Result<DataChunk>`. Drop the receiver to
    /// cancel the query early.
    pub fn query_stream(
        &self,
        query_str: &str,
    ) -> Result<crossbeam::channel::Receiver<Result<crate::processor::DataChunk>>> {
        self.execute_stream(query_str, None)
    }

    /// Streaming variant of `execute()`. Returns a channel receiver
    /// instead of collecting all chunks. See `query_stream()`.
    pub fn execute_stream(
        &self,
        query_str: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<crossbeam::channel::Receiver<Result<crate::processor::DataChunk>>> {
        let cache_key = normalize_query(query_str);

        let cached_stmt = {
            let cache = self.client_context.database.plan_cache.read();
            cache.get(&cache_key).cloned()
        };

        let tx = Arc::new(
            self.client_context
                .database
                .transaction_manager
                .begin(false)?,
        );

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &*self.client_context.database;
        db.storage_manager.read().flush_all_pending(bm, &tx)?;

        let (physical_plan, query_tx) = if let Some(stmt) = cached_stmt {
            let logical_plan = LogicalPlanner::plan(stmt)?;
            let pkey = &cache_key;
            let plan = {
                let mut plan_cache = self.client_context.database.physical_plan_cache.write();
                if let Some(cached) = plan_cache.get(pkey) {
                    cached.as_ref().clone_box()
                } else {
                    let mut planner = PhysicalPlanner::new(
                        Arc::clone(&self.client_context.database),
                        tx.read_ts,
                        tx.tx_id,
                        Arc::clone(&tx.undo_buffer),
                    );
                    let new_plan = planner.plan(logical_plan)?;
                    let boxed: Box<dyn crate::processor::PhysicalOperator + Send + Sync> = new_plan;
                    plan_cache.insert(pkey.clone(), Arc::new(boxed));
                    plan_cache.get(pkey).unwrap().as_ref().clone_box()
                }
            };
            (plan, tx)
        } else {
            let query = parse(query_str).map_err(|e| LightningError::Query(e.to_string()))?;
            let catalog = self.client_context.database.catalog.read();
            let mut binder = Binder::new(&catalog, &self.client_context.database.function_registry);
            let bound_query = binder.bind_query(&query)?;
            drop(catalog);

            if let Some(bound_union) = bound_query.union_queries.first() {
                self.client_context
                    .database
                    .plan_cache
                    .write()
                    .insert(cache_key.clone(), bound_union.statement.clone());
            }

            let bound_union = bound_query
                .union_queries
                .first()
                .ok_or_else(|| LightningError::Query("No query".into()))?;
            let logical_plan = LogicalPlanner::plan(bound_union.statement.clone())?;
            let mut planner = PhysicalPlanner::new(
                Arc::clone(&self.client_context.database),
                tx.read_ts,
                tx.tx_id,
                Arc::clone(&tx.undo_buffer),
            );
            (planner.plan(logical_plan)?, tx)
        };

        let mut processor = crate::processor::Processor::new(physical_plan);
        let rx = processor.execute_stream(
            Arc::clone(&self.client_context.database),
            query_tx,
            params,
        )?;
        Ok(rx)
    }

    /// Execute a query as of a specific point in time (time-travel).
    /// `snapshot_ts` is an MVCC timestamp — use `now_micros()` or a
    /// previously observed timestamp to see the graph at that moment.
    /// The MVCC engine handles all version filtering automatically.
    pub fn execute_at(
        &self,
        query_str: &str,
        snapshot_ts: u64,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        let _query_id = self
            .client_context
            .active_query_id
            .fetch_add(1, Ordering::SeqCst);

        let cache_key = normalize_query(query_str);
        let cached_stmt = {
            let cache = self.client_context.database.plan_cache.read();
            cache.get(&cache_key).cloned()
        };

        if let Some(stmt) = cached_stmt {
            let mut active_tx_guard = self.transaction.lock();
            let (tx, autocommit) = if let Some(ref tx) = *active_tx_guard {
                (Arc::clone(tx), false)
            } else {
                (
                    Arc::new(
                        self.client_context
                            .database
                            .transaction_manager
                            .begin_at(true, snapshot_ts)?,
                    ),
                    true,
                )
            };
            drop(active_tx_guard);

            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.storage_manager.read().flush_all_pending(bm, &tx)?;

            let res = (|| -> Result<QueryResult> {
                let logical_plan = LogicalPlanner::plan(stmt)?;
                let pkey = &cache_key;
                let physical_plan = {
                    let mut plan_cache = self.client_context.database.physical_plan_cache.write();
                    if let Some(cached) = plan_cache.get(pkey) {
                        cached.as_ref().clone_box()
                    } else {
                        let mut planner = PhysicalPlanner::new(
                            Arc::clone(&self.client_context.database),
                            tx.read_ts,
                            tx.tx_id,
                            Arc::clone(&tx.undo_buffer),
                        );
                        let new_plan = planner.plan(logical_plan)?;
                        let boxed: Box<dyn crate::processor::PhysicalOperator + Send + Sync> = new_plan;
                        plan_cache.insert(pkey.clone(), Arc::new(boxed));
                        plan_cache.get(pkey).unwrap().as_ref().clone_box()
                    }
                };
                let mut processor = crate::processor::Processor::new(physical_plan);
                let chunks = processor.execute(
                    Arc::clone(&self.client_context.database),
                    Arc::clone(&tx),
                    params,
                )?;
                Ok(QueryResult::new_arrow(
                    vec![], vec![],
                    chunks.into_iter().map(|c| c.batch).collect(),
                ))
            })();

            if autocommit {
                let bm = &self.client_context.database.buffer_manager;
                let db = &self.client_context.database;
                if res.is_ok() {
                    db.transaction_manager.commit(&tx, bm, db)?;
                } else {
                    db.transaction_manager.rollback(db, &tx)?;
                }
            }
            return res;
        }

        let query = parse(query_str).map_err(|e| LightningError::Query(e.to_string()))?;
        let mut active_tx_guard = self.transaction.lock();
        let (tx, autocommit) = if let Some(ref tx) = *active_tx_guard {
            (Arc::clone(tx), false)
        } else {
            (
                Arc::new(
                    self.client_context
                        .database
                        .transaction_manager
                        .begin_at(true, snapshot_ts)?,
                ),
                true,
            )
        };
        drop(active_tx_guard);

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &*self.client_context.database;
        db.storage_manager.read().flush_all_pending(bm, &tx)?;

        let res = (|| -> Result<QueryResult> {
            let catalog = self.client_context.database.catalog.read();
            let mut binder = Binder::new(&catalog, &self.client_context.database.function_registry);
            let bound_query = binder.bind_query(&query)?;
            drop(catalog);

            if let Some(bound_union) = bound_query.union_queries.first() {
                self.client_context.database.plan_cache.write()
                    .insert(cache_key.clone(), bound_union.statement.clone());
            }

            let bound_union = bound_query.union_queries.first()
                .ok_or_else(|| LightningError::Query("No query".into()))?;
            let logical_plan = LogicalPlanner::plan(bound_union.statement.clone())?;
            let mut planner = PhysicalPlanner::new(
                Arc::clone(&self.client_context.database),
                tx.read_ts,
                tx.tx_id,
                Arc::clone(&tx.undo_buffer),
            );
            let physical_plan = planner.plan(logical_plan)?;
            let mut processor = Processor::new(physical_plan);
            let chunks = processor.execute(
                Arc::clone(&self.client_context.database),
                Arc::clone(&tx),
                params,
            )?;
            Ok(QueryResult::new_arrow(
                vec![], vec![],
                chunks.into_iter().map(|c| c.batch).collect(),
            ))
        })();

        if autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &self.client_context.database;
            if res.is_ok() {
                db.transaction_manager.commit(&tx, bm, db)?;
            } else {
                db.transaction_manager.rollback(db, &tx)?;
            }
        }
        res
    }

    pub fn execute(
        &self,
        query_str: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        let _query_id = self
            .client_context
            .active_query_id
            .fetch_add(1, Ordering::SeqCst);

        let cache_key = normalize_query(query_str);

        let cached_stmt = {
            let cache = self.client_context.database.plan_cache.read();
            cache.get(&cache_key).cloned()
        };

        if let Some(stmt) = cached_stmt {
            let mut active_tx_guard = self.transaction.lock();
            let (tx, autocommit) = if let Some(ref tx) = *active_tx_guard {
                (Arc::clone(tx), false)
            } else {
                (
                    Arc::new(
                        self.client_context
                            .database
                            .transaction_manager
                            .begin(false)?,
                    ),
                    true,
                )
            };
            drop(active_tx_guard);

            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.storage_manager.read().flush_all_pending(bm, &tx)?;

            let res = (|| -> Result<QueryResult> {
                let logical_plan = LogicalPlanner::plan(stmt)?;

                let pkey = &cache_key;
                let physical_plan = {
                    let mut plan_cache = self.client_context.database.physical_plan_cache.write();
                    if let Some(cached) = plan_cache.get(pkey) {
                        cached.as_ref().clone_box()
                    } else {
                        let mut planner = PhysicalPlanner::new(
                            Arc::clone(&self.client_context.database),
                            tx.read_ts,
                            tx.tx_id,
                            Arc::clone(&tx.undo_buffer),
                        );
                        let new_plan = planner.plan(logical_plan)?;
                        let boxed: Box<dyn crate::processor::PhysicalOperator + Send + Sync> =
                            new_plan;
                        plan_cache.insert(pkey.clone(), Arc::new(boxed));
                        plan_cache.get(pkey).unwrap().as_ref().clone_box()
                    }
                };

                let mut processor = crate::processor::Processor::new(physical_plan);
                let chunks = processor.execute(
                    Arc::clone(&self.client_context.database),
                    Arc::clone(&tx),
                    params,
                )?;
                Ok(QueryResult::new_arrow(
                    vec![],
                    vec![],
                    chunks.into_iter().map(|c| c.batch).collect(),
                ))
            })();

        if autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &self.client_context.database;
            if res.is_ok() {
                db.storage_manager.read().flush_all_pending(bm, &tx)?;
                db.transaction_manager.commit(&tx, bm, &db)?;
            } else {
                db.transaction_manager.rollback(db, &tx)?;
            }
        }
        return res;
        }

        let query = parse(query_str).map_err(|e| LightningError::Query(e.to_string()))?;

        let mut active_tx_guard = self.transaction.lock();
        let (tx, autocommit) = if let Some(ref tx) = *active_tx_guard {
            (Arc::clone(tx), false)
        } else {
            (
                Arc::new(
                    self.client_context
                        .database
                        .transaction_manager
                        .begin(false)?,
                ),
                true,
            )
        };
        drop(active_tx_guard);

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &*self.client_context.database;
        db.storage_manager.read().flush_all_pending(bm, &tx)?;

        let res = (|| -> Result<QueryResult> {
            let catalog = self.client_context.database.catalog.read();
            let mut binder = Binder::new(&catalog, &self.client_context.database.function_registry);
            let bound_query = binder.bind_query(&query)?;
            drop(catalog);

            if let Some(bound_union) = bound_query.union_queries.first() {
                self.client_context
                    .database
                    .plan_cache
                    .write()
                    .insert(cache_key.clone(), bound_union.statement.clone());
            }

            let bound_union = bound_query
                .union_queries
                .first()
                .ok_or_else(|| LightningError::Query("No query".into()))?;
            let logical_plan = LogicalPlanner::plan(bound_union.statement.clone())?;
            let mut planner = PhysicalPlanner::new(
                Arc::clone(&self.client_context.database),
                tx.read_ts,
                tx.tx_id,
                Arc::clone(&tx.undo_buffer),
            );
            let physical_plan = planner.plan(logical_plan)?;
            let mut processor = Processor::new(physical_plan);
            let chunks = processor.execute(
                Arc::clone(&self.client_context.database),
                Arc::clone(&tx),
                params,
            )?;
            Ok(QueryResult::new_arrow(
                vec![],
                vec![],
                chunks.into_iter().map(|c| c.batch).collect(),
            ))
        })();

        if autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &self.client_context.database;
            if res.is_ok() {
                db.transaction_manager.commit(&tx, bm, db)?;
            } else {
                db.transaction_manager.rollback(db, &tx)?;
            }
        }
        res
    }

    pub fn bulk_insert_batch(&self, table_name: &str, batch: &RecordBatch) -> Result<usize> {
        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {} not found", table_name)))?
        };

        let tx = db.transaction_manager.begin(false)?;
        let bm = db.buffer_manager.clone();
        let num_rows = batch.num_rows();

        let start_id = table
            .next_row_id
            .fetch_add(num_rows as u64, Ordering::SeqCst);

        // Prepend _id column to the batch to align with table schema
        let id_values: UInt64Array = (start_id..start_id + num_rows as u64).collect();
        let id_field =
            arrow::datatypes::Field::new("_id", arrow::datatypes::DataType::UInt64, false);
        let mut fields = vec![id_field];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(id_values)];
        for i in 0..batch.num_columns() {
            fields.push(batch.schema().field(i).clone());
            columns.push(batch.column(i).clone());
        }
        let final_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let final_batch = RecordBatch::try_new(final_schema, columns)?;

        table.bulk_append_batch(&bm, &final_batch, start_id, &tx)?;

        table.bulk_append_trigram_batch(start_id, &final_batch)?;

        let storage = db.storage_manager.read();
        let fts_opt = storage.fts_indexes.get(table_name).cloned();
        let vec_opt = storage.vector_indexes.get(table_name).cloned();
        let index_opt = storage.get_index(table_name);

        // Find primary key column index if it exists
        let pk_idx = db
            .catalog
            .read()
            .get_node_table(table_name)
            .and_then(|t| t.primary_key.as_ref())
            .and_then(|pk| table.columns.iter().position(|c| c.name == pk.as_str()));

        // Insert into primary key hash index
        if let (Some(index), Some(pk_col_idx)) = (&index_opt, pk_idx) {
            if pk_col_idx < final_batch.num_columns() {
                let pk_array = final_batch.column(pk_col_idx);
                if let Some(str_arr) = pk_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..num_rows {
                        if str_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::String(str_arr.value(i).to_string()),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                } else if let Some(int_arr) =
                    pk_array.as_any().downcast_ref::<arrow::array::Int64Array>()
                {
                    for i in 0..num_rows {
                        if int_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::Number(int_arr.value(i) as f64),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                }
            }
        }

        // Index all string columns into FTS
        if let Some(fts) = fts_opt {
            for (col_idx, col) in table.columns.iter().enumerate() {
                if col_idx < final_batch.num_columns() {
                    let array = final_batch.column(col_idx);
                    if let Some(str_arr) =
                        array.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        let mut batch_docs = Vec::new();
                        for i in 0..num_rows {
                            if str_arr.is_valid(i) {
                                batch_docs.push((start_id + i as u64, str_arr.value(i)));
                            }
                        }
                        if !batch_docs.is_empty() {
                            if let Err(e) = fts.insert_batch(&batch_docs, &bm, &tx) {
                                tracing::warn!("FTS insert_batch error for column {}: {}", col.name, e);
                            }
                        }
                    }
                }
            }
            if let Err(e) = fts.commit() {
                tracing::warn!("FTS commit error: {}", e);
            }
        }

        // Index all FixedSizeList(Float32) columns as vector embeddings
        if let Some(vec_idx) = vec_opt {
            for (col_idx, col) in table.columns.iter().enumerate() {
                if col_idx < final_batch.num_columns() {
                    let array = final_batch.column(col_idx);
                    if let Some(list_arr) = array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeListArray>()
                    {
                        if list_arr.value_length() == 768 {
                            if let Some(values) = list_arr
                                .values()
                                .as_any()
                                .downcast_ref::<arrow::array::Float32Array>()
                            {
                                let mut batch_vecs = Vec::new();
                                for i in 0..num_rows {
                                    let mut emb = [0f32; 768];
                                    emb.copy_from_slice(&values.values()[i * 768..(i + 1) * 768]);
                                    batch_vecs.push((start_id + i as u64, emb));
                                }
                                let _ = vec_idx.insert_batch(&batch_vecs, &bm, &tx);
                            }
                        }
                    }
                }
            }
        }

        db.storage_manager.read().flush_all_pending(&bm, &tx)?;
        db.transaction_manager.commit(&tx, &bm, &db)?;

        // Update catalog with the new row count
        {
            let mut cat = db.catalog.write();
            if let Some(entry) = cat.get_node_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            } else if let Some(entry) = cat.get_rel_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            }
            db.catalog.mark_dirty();
        }

        // Sync catalog stats from storage manager to persist cardinality
        {
            let storage = db.storage_manager.read();
            let mut cat = db.catalog.write();
            for (name, table) in storage.rel_tables.iter() {
                if let Some(entry) = cat.get_rel_table_mut(name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            for (name, table) in storage.node_tables.iter() {
                if let Some(entry) = cat.get_node_table_mut(name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            db.catalog.mark_dirty();
        }

        Ok(num_rows)
    }
}
