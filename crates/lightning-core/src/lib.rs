pub mod api;
pub mod catalog;
pub mod cdc;
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

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;
use lightning_types::LogicalType;
use parking_lot::RwLock;
pub use processor::Value;
use serde::{Deserialize, Serialize};
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

use std::sync::atomic::{AtomicU64, Ordering};

/// Select a shard index for a cache key using its hash.
fn cache_shard(key: &str, num_shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % num_shards
}

/// Compute a 64-bit hash from a string key using the same DefaultHasher
/// that cache_shard uses internally. This ensures query_hash and shard
/// selection are always consistent: a given key maps to the same shard
/// regardless of which cache (plan vs physical plan) is being accessed.
fn hash_cache_key(key: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    h.finish()
}

use crate::catalog::{Catalog, LazyCatalog};
use crate::parser::parse;
use crate::planner::Binder;
use crate::processor::physical_plan::PhysicalPlanner;
use crate::processor::Processor;
use crate::storage::WAL;
use crate::transaction::transaction_manager::Transaction;
use crate::transaction::TransactionManager;
use crate::parser::ast::{Clause, NodePattern, Pattern, Query, Statement};


/// System table names that must not be accessible via Cypher queries.
/// These tables store authentication data (password hashes, tokens, API keys)
/// and are managed exclusively through the auth store API.
const SYSTEM_AUTH_TABLES: &[&str] = &["auth_users", "auth_refresh_tokens", "auth_api_keys"];

/// Extract all table label references from a parsed Query AST and check
/// that none of them target system auth tables. This prevents privilege
/// escalation via Cypher queries (e.g., `MATCH (u:auth_users) RETURN u.password_hash`).
pub fn validate_no_auth_table_access(query: &Query) -> Result<()> {
    for union_q in &query.union_queries {
        validate_statement_no_auth_tables(&union_q.statement)?;
    }
    Ok(())
}

fn validate_statement_no_auth_tables(stmt: &Statement) -> Result<()> {
    match stmt {
        Statement::Match(match_clause, _where, clauses) => {
            if let Some(ref mc) = match_clause {
                for pattern in &mc.patterns {
                    validate_pattern_no_auth_tables(pattern)?;
                }
            }
            for clause in clauses {
                validate_clause_no_auth_tables(clause)?;
            }
        }
        Statement::Create(pattern) => {
            validate_pattern_no_auth_tables(pattern)?;
        }
        Statement::Merge(mc) => {
            validate_pattern_no_auth_tables(&mc.pattern)?;
        }
        Statement::CreateTableNode { name, if_not_exists, .. } => {
            if SYSTEM_AUTH_TABLES.contains(&name.as_str()) {
                // Allow CREATE TABLE IF NOT EXISTS for auth tables (used during
                // server initialization). All other operations on auth tables
                // are blocked.
                if !*if_not_exists {
                    return Err(LightningError::Query(format!(
                        "Access to system table '{name}' is not allowed via Cypher"
                    )));
                }
            }
        }
        Statement::AlterTable { name, .. }
        | Statement::DropTable(name, _)
        | Statement::CreateIndex { table_label: name, .. }
        | Statement::CreateConstraint { table_label: name, .. }
        | Statement::CreateVectorIndex { table_name: name, .. } => {
            if SYSTEM_AUTH_TABLES.contains(&name.as_str()) {
                return Err(LightningError::Query(format!(
                    "Access to system table '{name}' is not allowed via Cypher"
                )));
            }
        }
        Statement::CopyFrom { table_name, .. } | Statement::CopyTo { table_name, .. } => {
            if SYSTEM_AUTH_TABLES.contains(&table_name.as_str()) {
                return Err(LightningError::Query(format!(
                    "Access to system table '{table_name}' is not allowed via Cypher"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_pattern_no_auth_tables(pattern: &Pattern) -> Result<()> {
    validate_node_labels_no_auth(&pattern.node_pattern)?;
    for chain in &pattern.relationship_chains {
        validate_node_labels_no_auth(&chain.node_pattern)?;
    }
    if let Some(ref start) = pattern.shortest_path_start {
        validate_node_labels_no_auth(start)?;
    }
    if let Some(ref end) = pattern.shortest_path_end {
        validate_node_labels_no_auth(end)?;
    }
    Ok(())
}

fn validate_node_labels_no_auth(node: &NodePattern) -> Result<()> {
    for label in &node.labels {
        if SYSTEM_AUTH_TABLES.contains(&label.as_str()) {
            return Err(LightningError::Query(format!(
                "Access to system table '{label}' is not allowed via Cypher"
            )));
        }
    }
    Ok(())
}

fn validate_clause_no_auth_tables(clause: &Clause) -> Result<()> {
    match clause {
        Clause::Create(pattern) => validate_pattern_no_auth_tables(pattern)?,
        Clause::Merge(mc) => validate_pattern_no_auth_tables(&mc.pattern)?,
        Clause::Match(mc) | Clause::OptionalMatch(mc) => {
            for pattern in &mc.patterns {
                validate_pattern_no_auth_tables(pattern)?;
            }
        }
        Clause::Subquery(sub_q) => validate_no_auth_table_access(sub_q)?,
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    Internal,
    Database,
    NotFound,
    AlreadyExists,
    SyntaxError,
    QueryError,
    ConfigError,
    IoError,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::Internal => write!(f, "INTERNAL_ERROR"),
            ErrorCode::Database => write!(f, "DATABASE_ERROR"),
            ErrorCode::NotFound => write!(f, "NOT_FOUND"),
            ErrorCode::AlreadyExists => write!(f, "ALREADY_EXISTS"),
            ErrorCode::SyntaxError => write!(f, "SYNTAX_ERROR"),
            ErrorCode::QueryError => write!(f, "QUERY_ERROR"),
            ErrorCode::ConfigError => write!(f, "CONFIG_ERROR"),
            ErrorCode::IoError => write!(f, "IO_ERROR"),
        }
    }
}

#[derive(Error, Debug)]
pub enum LightningError {
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Query error: {0}")]
    Query(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl LightningError {
    pub fn code(&self) -> ErrorCode {
        match self {
            LightningError::Internal(_) => ErrorCode::Internal,
            LightningError::Database(_) => ErrorCode::Database,
            LightningError::Query(msg) => {
                if msg.contains("Variable") && msg.contains("not found") {
                    ErrorCode::NotFound
                } else if msg.contains("already exists") {
                    ErrorCode::AlreadyExists
                } else if msg.contains("syntax") || msg.contains("parse") {
                    ErrorCode::SyntaxError
                } else {
                    ErrorCode::QueryError
                }
            }
            LightningError::Config(_) => ErrorCode::ConfigError,
            LightningError::Io(_) => ErrorCode::IoError,
        }
    }
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
    /// Queries exceeding this duration (in milliseconds) are logged as warnings.
    /// Set to 0 to disable slow query logging.
    pub slow_query_threshold_ms: u64,
    /// Base directory for COPY FROM/TO file operations.
    /// When set, all COPY file paths must resolve within this directory.
    /// When None (default), only relative paths are allowed and resolved
    /// relative to the database directory.
    pub copy_base_dir: Option<std::path::PathBuf>,
    /// Base directory for WASM UDF/module file loading.
    /// When set, all WASM file paths must resolve within this directory.
    /// When None (default), only relative paths are allowed and resolved
    /// relative to the database directory.
    pub wasm_base_dir: Option<std::path::PathBuf>,
    /// Default embedding dimension for memory/vector operations.
    pub embedding_dim: usize,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 1024 * 1024 * 1024,
            max_num_threads: 0, // 0 = auto-detect (use all available CPU cores)
            read_only: false,
            sync_mode: SyncMode::Normal,
            vacuum_interval_ms: 1000,
            prefetch_enabled: true,
            prefetch_depth: 2,
            prefetch_confidence: 0.15,
            slow_query_threshold_ms: 100,
            copy_base_dir: None,
            wasm_base_dir: None,
            embedding_dim: 384,
        }
    }
}

impl SystemConfig {
    pub fn validate(&self) -> Result<()> {
        if self.buffer_pool_size == 0 {
            return Err(LightningError::Config(
                "buffer_pool_size must be greater than 0".into()
            ));
        }
        if self.buffer_pool_size < 1024 * 1024 {
            return Err(LightningError::Config(
                "buffer_pool_size must be at least 1MB".into()
            ));
        }
        if self.vacuum_interval_ms < 100 {
            return Err(LightningError::Config(
                "vacuum_interval_ms must be at least 100ms".into()
            ));
        }
        if self.prefetch_depth > 100 {
            return Err(LightningError::Config(
                "prefetch_depth must be <= 100".into()
            ));
        }
        if !(0.0..=1.0).contains(&self.prefetch_confidence) {
            return Err(LightningError::Config(
                "prefetch_confidence must be between 0.0 and 1.0".into()
            ));
        }
        Ok(())
    }
}

/// Database-wide metrics for observability and performance monitoring.
pub struct DatabaseMetrics {
    pub total_queries: AtomicU64,
    pub total_checkpoints: AtomicU64,
    pub checkpoint_duration_us: AtomicU64,
    pub wal_bytes_written: AtomicU64,
    pub wal_fsync_count: AtomicU64,
    pub eviction_count: AtomicU64,
    pub buffer_miss_count: AtomicU64,
    pub buffer_hit_count: AtomicU64,
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            total_checkpoints: AtomicU64::new(0),
            checkpoint_duration_us: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
            wal_fsync_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            buffer_miss_count: AtomicU64::new(0),
            buffer_hit_count: AtomicU64::new(0),
        }
    }

    pub fn record_query(&self) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self, duration_us: u64) {
        self.total_checkpoints.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_duration_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_wal_write(&self, bytes: u64) {
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wal_fsync(&self) {
        self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.eviction_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_buffer_access(&self, hit: bool) {
        if hit {
            self.buffer_hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.buffer_miss_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn buffer_hit_rate(&self) -> f64 {
        let hits = self.buffer_hit_count.load(Ordering::Relaxed);
        let misses = self.buffer_miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    pub fn avg_checkpoint_duration_ms(&self) -> f64 {
        let count = self.total_checkpoints.load(Ordering::Relaxed);
        if count == 0 { return 0.0; }
        let total_us = self.checkpoint_duration_us.load(Ordering::Relaxed);
        (total_us / count) as f64 / 1000.0
    }
}

pub struct Database {
    pub(crate) _path: PathBuf,
    pub(crate) _config: SystemConfig,
    pub(crate) storage_manager: Arc<RwLock<crate::storage::storage_manager::StorageManager>>,
    pub(crate) wal: Arc<WAL>,
    pub(crate) transaction_manager: Arc<TransactionManager>,
    pub(crate) buffer_manager: Arc<crate::storage::buffer_manager::BufferManager>,
    pub(crate) free_space_manager: Arc<crate::storage::FreeSpaceManager>,
    pub(crate) catalog: Arc<LazyCatalog>,
    pub(crate) function_registry: Arc<crate::processor::functions::FunctionRegistry>,
    pub(crate) header: RwLock<crate::storage::DatabaseHeader>,
    /// Cached bound statements paired with their binder column offsets, keyed by
    /// normalized query string. The column offsets map variable names to their
    /// starting column position in the binder's flat layout and are needed by the
    /// physical plan builder to remap PropertyLookup indices after optimizer
    /// transforms (e.g. join reordering) alter the physical column layout.
    pub(crate) plan_caches: Vec<
        Arc<
            parking_lot::Mutex<
                LruCache<
                    String,
                    Arc<(
                        crate::planner::binder::BoundStatement,
                        std::collections::HashMap<String, usize>,
                    )>,
                >,
            >,
        >,
    >,
    pub(crate) physical_plan_caches: Vec<Arc<parking_lot::Mutex<LruCache<u64, Arc<dyn crate::processor::PhysicalOperator + Send + Sync>>>>>,
    pub(crate) metrics: DatabaseMetrics,

    vacuum_handle: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Signal vacuum thread to stop via lock-free flag.
        self.buffer_manager.set_shutting_down();

        // Wait for vacuum thread — it checks shutdown every ~100ms and
        // also bails out of reclaim_expired_versions early.
        if let Some(handle) = self.vacuum_handle.take() {
            let _ = handle.join();
        }

        // Flush dirty buffer pages to disk. We intentionally skip the full
        // Database::checkpoint() here (catalog save, header update, etc.)
        // because those operations can deadlock with other threads that hold
        // catalog/storage_manager locks. The buffer_manager checkpoint
        // handles the critical dirty-page flush.
        if let Err(e) = self.buffer_manager.checkpoint() {
            tracing::warn!("Checkpoint failed during database shutdown: {e}");
        }
    }
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P, config: SystemConfig) -> Result<Arc<Self>> {
        config.validate()?;
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

        let catalog_path = path.join("catalog.ltng");
        let catalog = Arc::new(
            LazyCatalog::from_disk(&catalog_path)
                .unwrap_or_else(|_| LazyCatalog::new(Catalog::new(), Some(catalog_path.clone()))),
        );

        Self::restore_tables_from_catalog(&catalog, &mut storage_manager)?;

        // REPLAY WAL after tables are created so apply_page can find file handles
        let replay_report = wal.replay(
            |fid, pid, data| storage_manager.apply_page(fid, pid, data),
            header.last_checkpoint_ts,
        )?;

        if replay_report.corrupt_records_skipped > 0 {
            tracing::warn!(
                "WAL replay: {} corrupt records skipped (torn writes)",
                replay_report.corrupt_records_skipped
            );
        }
        if replay_report.partial_record_at_eof {
            tracing::warn!(
                "WAL replay: incomplete record at end of WAL (partial write on last crash)"
            );
        }
        tracing::info!(
            "WAL replay: {} records processed",
            replay_report.records_read
        );

        let fsm_path = path.join("free_space.bin");
        let free_space_manager = Arc::new(
            crate::storage::FreeSpaceManager::load(&fsm_path)
                .unwrap_or_else(|_| crate::storage::FreeSpaceManager::new()),
        );

        // Wire FreeSpaceManager into all existing file handles so page
        // allocation reuses freed pages before extending files.
        {
            let mut sm = storage_manager;
            sm.set_free_space_manager(Arc::clone(&free_space_manager));
            storage_manager = sm;
        }

        let transaction_manager = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        let buffer_manager = Arc::new(crate::storage::buffer_manager::BufferManager::new(
            config.buffer_pool_size as usize / 4096,
            Some(Arc::clone(&wal)),
            config.prefetch_enabled,
            config.prefetch_depth,
            config.prefetch_confidence,
        ));

        transaction_manager.set_self_weak(Arc::downgrade(&transaction_manager));
        transaction_manager.set_bm_weak(Arc::downgrade(&buffer_manager));

        let tm_clone = Arc::clone(&transaction_manager);
        let bm_clone = Arc::clone(&buffer_manager);
        let vacuum_interval_ms = config.vacuum_interval_ms;
        let vacuum_handle = std::thread::spawn(move || loop {
            // Check shutdown flag at 100ms intervals regardless of configured interval
            // so that Database::drop never blocks longer than 100ms on join.
            let check_interval = std::cmp::min(vacuum_interval_ms, 100);
            let mut elapsed = 0u64;
            while elapsed < vacuum_interval_ms {
                std::thread::sleep(std::time::Duration::from_millis(check_interval));
                elapsed += check_interval;
                if bm_clone.is_shutting_down() {
                    // Don't flush here — Database::drop handles it after we exit.
                    return;
                }
            }
            let min_ts = tm_clone.get_min_active_read_ts();
            if let Err(e) = bm_clone.reclaim_expired_versions(min_ts) {
                tracing::warn!("Vacuum reclaim failed: {}", e);
            }
        });

        let db = Arc::new(Self {
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
            plan_caches: (0..4).map(|_| Arc::new(parking_lot::Mutex::new(LruCache::new(NonZeroUsize::new(256).expect("infallible: 256 > 0"))))).collect(),
            physical_plan_caches: (0..4).map(|_| Arc::new(parking_lot::Mutex::new(LruCache::new(NonZeroUsize::new(64).expect("infallible: 64 > 0"))))).collect(),
            metrics: DatabaseMetrics::new(),
            vacuum_handle: Some(vacuum_handle),
        });

        // Register the SEARCH scalar function for BM25 scoring
        Self::register_search_function(&db)?;

        Ok(db)
    }

    fn restore_tables_from_catalog(
        catalog: &Arc<LazyCatalog>,
        storage_manager: &mut crate::storage::storage_manager::StorageManager,
    ) -> Result<()> {
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
            if table_entry.num_rows > 0 {
                if let Some(table) = storage_manager.node_tables.get_mut(&table_entry.name) {
                    table.next_row_id.store(table_entry.num_rows, std::sync::atomic::Ordering::Release);
                }
            }
            if table_entry.primary_key.is_some() {
                storage_manager.create_index(&table_entry.name)?;
            }
            storage_manager.create_fts_index(&table_entry.name).map_err(|e| {
                LightningError::Internal(format!("FTS index creation failed for {}: {}", table_entry.name, e))
            })?;
            storage_manager.create_vector_index(&table_entry.name, crate::memory::DEFAULT_EMBEDDING_DIM).map_err(|e| {
                LightningError::Internal(format!("Vector index creation failed for {}: {}", table_entry.name, e))
            })?;
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
            if table_entry.num_rows > 0 {
                if let Some(table) = storage_manager.rel_tables.get_mut(&table_entry.name) {
                    table.next_row_id.store(table_entry.num_rows, std::sync::atomic::Ordering::Release);
                }
            }
        }
        Ok(())
    }

    /// Register the SEARCH() scalar function for full-text search BM25 scores.
    /// SEARCH(node_id_column, query_string) returns the BM25 score for each row.
    /// Used in ORDER BY clauses: ORDER BY SEARCH(content, 'query') DESC
    fn register_search_function(db: &Arc<Self>) -> Result<()> {
        let db_weak = Arc::downgrade(db);
        let search_func = crate::processor::functions::ScalarFunction::new(
            "SEARCH".to_string(),
            Arc::new(move |args, _num_rows| {
                if args.len() != 2 {
                    return Err(crate::LightningError::Internal(
                        "SEARCH requires 2 arguments: (node_id, query)".into(),
                    ));
                }
                let db = match db_weak.upgrade() {
                    Some(d) => d,
                    None => return Err(crate::LightningError::Internal(
                        "Database dropped during SEARCH evaluation".into(),
                    )),
                };
                let storage = db.storage_manager.read();
                let fts_values: Vec<Option<f32>> = {
                    let mut results = Vec::new();
                    let node_arr = args[0].as_any()
                        .downcast_ref::<arrow::array::UInt64Array>();
                    let query_arr = args[1].as_any()
                        .downcast_ref::<arrow::array::StringArray>();
                    match (node_arr, query_arr) {
                        (Some(ids), Some(queries)) => {
                            for i in 0..ids.len() {
                                let score = if ids.is_valid(i) && queries.is_valid(i) {
                                    let _node_id = ids.value(i);
                                    let query_str = queries.value(i);
                                    let mut best_score = 0.0f32;
                                    for fts in storage.fts_indexes.values() {
                                        let Ok(tx) = db.transaction_manager.begin(true) else { continue };
                                        if let Ok(res) = fts.search(query_str, 1, &db.buffer_manager, &tx) {
                                            if let Some(&(_, s)) = res.first() {
                                                if s > best_score { best_score = s; }
                                            }
                                        }
                                    }
                                    best_score
                                } else {
                                    0.0
                                };
                                results.push(Some(score));
                            }
                        }
                        _ => {
                            for _ in 0..args[0].len() {
                                results.push(Some(0.0f32));
                            }
                        }
                    }
                    results
                };
                Ok(Arc::new(arrow::array::Float32Array::from(fts_values)) as ArrayRef)
            }),
        );
        db.function_registry.register_scalar(search_func);
        tracing::info!("Registered SEARCH scalar function");
        Ok(())
    }

    pub fn connect(self: &Arc<Self>) -> Connection {
        Connection::new(Arc::clone(self))
    }

    /// Create a connection that bypasses system table access checks.
    /// Only for internal auth store use.
    pub fn connect_internal(self: &Arc<Self>) -> Connection {
        Connection::new_internal(Arc::clone(self))
    }

    // ── Accessor methods for encapsulated Database fields ────────────

    pub fn storage_manager(&self) -> &Arc<RwLock<crate::storage::storage_manager::StorageManager>> {
        &self.storage_manager
    }

    pub fn wal(&self) -> &Arc<WAL> {
        &self.wal
    }

    pub fn transaction_manager(&self) -> &Arc<TransactionManager> {
        &self.transaction_manager
    }

    pub fn buffer_manager(&self) -> &Arc<crate::storage::buffer_manager::BufferManager> {
        &self.buffer_manager
    }

    pub fn free_space_manager(&self) -> &Arc<crate::storage::FreeSpaceManager> {
        &self.free_space_manager
    }

    pub fn catalog(&self) -> &Arc<LazyCatalog> {
        &self.catalog
    }

    pub fn function_registry(&self) -> &Arc<crate::processor::functions::FunctionRegistry> {
        &self.function_registry
    }

    pub fn header(&self) -> &RwLock<crate::storage::DatabaseHeader> {
        &self.header
    }

    pub fn plan_caches(&self) -> &Vec<Arc<parking_lot::Mutex<LruCache<String, Arc<(crate::planner::binder::BoundStatement, std::collections::HashMap<String, usize>)>>>>> {
        &self.plan_caches
    }

    pub fn physical_plan_caches(&self) -> &Vec<Arc<parking_lot::Mutex<LruCache<u64, Arc<dyn crate::processor::PhysicalOperator + Send + Sync>>>>> {
        &self.physical_plan_caches
    }

    pub fn metrics(&self) -> &DatabaseMetrics {
        &self.metrics
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
        if self._config.read_only {
            return Err(LightningError::Config("Database is opened as read-only".into()));
        }
        let wat_bytes = self.read_wasm_file(wasm_path.as_ref())?;
        let wasm_func = crate::wasm_function::WasmFunction::from_wat_bytes(wat_bytes, func_name)?;
        let scalar = wasm_func.to_scalar_function();
        self.function_registry.register_scalar(scalar);
        tracing::info!("Registered WASM function: {}", func_name);
        Ok(())
    }

    pub fn get_catalog_path(&self) -> PathBuf {
        self._path.join("catalog.ltng")
    }

    /// Open and read a WASM file with path traversal validation and O_NOFOLLOW
    /// to prevent symlink-based TOCTOU attacks. The file is opened within the
    /// validation scope so there is no gap between path validation and file I/O.
    fn read_wasm_file(&self, user_path: &Path) -> Result<Vec<u8>> {
        use std::os::unix::fs::OpenOptionsExt;
        use std::path::Component;
        use std::io::Read;

        let base_dir = self._config.wasm_base_dir.as_deref();
        let database_path = &self._path;

        if user_path.is_absolute() && base_dir.is_none() {
            return Err(LightningError::Config(
                "Absolute WASM paths require wasm_base_dir to be set in SystemConfig".into(),
            ));
        }
        for component in user_path.components() {
            if let Component::ParentDir = component {
                return Err(LightningError::Config(format!(
                    "WASM path '{}' contains '..' traversal",
                    user_path.display()
                )));
            }
        }
        let base = base_dir.unwrap_or(database_path);
        let resolved = base.join(user_path);
        let canonical_base = base
            .canonicalize()
            .map_err(|e| LightningError::Config(format!("Cannot resolve WASM base: {e}")))?;
        let parent = resolved.parent().ok_or_else(|| {
            LightningError::Config(format!(
                "Cannot determine parent for WASM path '{}'",
                user_path.display()
            ))
        })?;
        let file_name = resolved.file_name().ok_or_else(|| {
            LightningError::Config(format!(
                "Cannot determine filename for WASM path '{}'",
                user_path.display()
            ))
        })?;
        let canonical_parent = parent
            .canonicalize()
            .map_err(|e| LightningError::Config(format!("Cannot resolve WASM path parent: {e}")))?;
        let canonical = canonical_parent.join(file_name);
        if !canonical.starts_with(&canonical_base) {
            return Err(LightningError::Config(format!(
                "WASM path '{}' escapes base directory '{}'",
                user_path.display(),
                canonical_base.display()
            )));
        }

        // Open with O_NOFOLLOW so symlinks in the final component are rejected.
        // This closes the symlink-based TOCTOU: even if the file is a symlink
        // to /etc/passwd, the open fails with ELOOP.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&canonical)
            .map_err(|e| LightningError::Database(format!(
                "Failed to open WASM file '{}': {e}",
                canonical.display()
            )))?;

        // O_NOFOLLOW guarantees on all supported platforms (Linux, macOS)
        // that we didn't follow a trailing symlink. The open would have failed
        // with ELOOP if the final component was a symlink.

        let mut wat_bytes = Vec::new();
        file.read_to_end(&mut wat_bytes)
            .map_err(|e| LightningError::Database(format!(
                "Failed to read WASM file '{}': {e}",
                canonical.display()
            )))?;

        Ok(wat_bytes)
    }

    pub fn checkpoint(&self) -> Result<()> {
        if self._config.read_only {
            return Err(LightningError::Config("Cannot checkpoint a read-only database".into()));
        }
        let start = std::time::Instant::now();
        // Flush all dirty pages to disk and sync data files
        self.buffer_manager.checkpoint()?;

        // Persist free space map
        {
            let fsm_path = self._path.join("free_space.bin");
            self.free_space_manager.save(&fsm_path).map_err(|e| {
                LightningError::Internal(format!("Failed to save free space map during checkpoint: {e}"))
            })?;
        }

        // Persist catalog to disk so num_rows and other metadata survive restart.
        // This is critical: without it, checkpoint-flushed data files may have
        // rows that the catalog doesn't know about, causing COUNT(*) to return
        // fewer rows than actually exist.
        {
            // Sync column statistics of all tables in storage to the catalog
            let storage = self.storage_manager.read();
            let mut cat = self.catalog.write();
            for (table_name, table) in &storage.node_tables {
                table.update_column_stats();
                if let Some(entry) = cat.node_tables.get_mut(table_name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            for (table_name, table) in &storage.rel_tables {
                table.update_column_stats();
                if let Some(entry) = cat.rel_tables.get_mut(table_name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            // Save while the write lock is still held, so no concurrent writer
            // can modify the catalog between our update and the save.
            self.catalog.force_save_with_catalog(&cat).map_err(|e| {
                LightningError::Internal(format!("Failed to save catalog during checkpoint: {e}"))
            })?;
            drop(cat);
        }

        // Update the last checkpoint timestamp so recovery can skip these entries
        let last_ts = self.transaction_manager.get_current_ts();
        {
            let mut header = self.header.write();
            header.last_checkpoint_ts = last_ts;
            let header_path = self._path.join("database.header");
            header.save(&header_path)?;
        }

        // Vacuum RowVersion committed entries that are older than
        // the minimum active read timestamp. These entries accumulate
        // unboundedly and are no longer needed for snapshot visibility.
        let min_active = self.transaction_manager.get_min_active_read_ts();
        let mut total_evicted = 0usize;
        for table in self.storage_manager.read().node_tables.values() {
            total_evicted += table.version_info.vacuum(min_active);
        }
        for table in self.storage_manager.read().rel_tables.values() {
            total_evicted += table.version_info.vacuum(min_active);
        }
        if total_evicted > 0 {
            tracing::debug!("Vacuumed {total_evicted} RowVersion committed entries");
        }

        self.metrics.record_checkpoint(start.elapsed().as_micros() as u64);
        Ok(())
    }

    pub fn is_column_indexed(&self, table_name: &str, col_name: &str) -> bool {
        let cat = self.catalog.read();
        if let Some(entry) = cat.node_tables.get(table_name) {
            if entry.primary_key.as_deref() == Some(col_name) {
                return true;
            }
            if entry.constraints.iter().any(|c| c.property == col_name) {
                return true;
            }
        }
        false
    }

    /// VACUUM: compact the database by reclaiming space from deleted rows.
    /// Optimizes each column by truncating trailing empty pages.
    pub fn vacuum(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let tables: Vec<String> = {
            let cat = self.catalog.read();
            let mut names: Vec<String> = cat.node_tables.keys().cloned().collect();
            names.extend(cat.rel_tables.keys().cloned());
            names
        };

        let bm = &self.buffer_manager;
        let tx = self.transaction_manager.begin(true)?;

        for table_name in &tables {
            let table = {
                let storage = self.storage_manager.read();
                storage.get_table(table_name).cloned()
            };
            if let Some(ref table) = table {
                for col in &table.columns {
                    let is_indexed = self.is_column_indexed(table_name, &col.name);
                    col.optimize(bm, &tx, is_indexed)?;
                }
            }
            // Rebuild CSR indexes if present
            self.storage_manager.write().ensure_csr_fresh(table_name, bm, &tx)?;
        }

        // Rollback the read transaction used for optimization.
        // If rollback fails, the database state is inconsistent — do NOT proceed
        // to checkpoint as that would persist the inconsistency.
        self.transaction_manager.rollback(self, &tx)?;

        // Force a checkpoint to persist the optimized state
        self.checkpoint()?;

        let elapsed = start.elapsed();
        tracing::info!("VACUUM completed in {:?} for {} tables", elapsed, tables.len());
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
                } else if let Some(e) = cat.get_node_table_mut(name) {
                    e.num_rows = *actual;
                    e.stats.cardinality = *actual;
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
    skip_auth_check: bool,
}

impl Connection {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            client_context: Arc::new(ClientContext::new(database)),
            transaction: parking_lot::Mutex::new(None),
            skip_auth_check: false,
        }
    }

    /// Create a connection with auth table checks disabled.
    /// Used internally by the auth store to manage system tables.
    pub fn new_internal(database: Arc<Database>) -> Self {
        let mut c = Self::new(database);
        c.skip_auth_check = true;
        c
    }

    pub fn is_internal(&self) -> bool {
        self.skip_auth_check
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
        let mut guard = self.transaction.lock();
        let tx = guard.take()
            .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &self.client_context.database;

        self.client_context
            .database
            .storage_manager
            .read()
            .flush_all_pending(bm, &tx)?;

        // Hold the transaction lock during commit to prevent another thread
        // from starting a new transaction or calling execute on a stale tx.
        // Only release after the commit record is written to the WAL.
        let result = self.client_context
            .database
            .transaction_manager
            .commit(&tx, bm, db);
        drop(guard);
        result
    }

    pub fn fast_insert(&self, table_name: &str, rows: Vec<Vec<(String, Value)>>) -> Result<usize> {
        if self.client_context.database._config.read_only {
            return Err(LightningError::Config("Database is opened as read-only".into()));
        }
        use arrow::array::*;

        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {table_name} not found")))?
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

        // Pre-build per-row HashMap<&str, &Value> for O(1) column lookups
        let row_maps: Vec<HashMap<&str, &Value>> = rows
            .iter()
            .map(|r| r.iter().map(|(n, v)| (n.as_str(), v)).collect())
            .collect();

        // Data columns
        for col in columns.iter().skip(1) {
            let arr: ArrayRef = match col.data_type {
                lightning_types::LogicalType::String => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::String(s)) => builder.append_value(s),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Number(n)) => builder.append_value(*n as i64),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Double => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Number(n)) => builder.append_value(*n),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Bool => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Boolean(b)) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Node(_) => {
                    let mut builder = UInt64Builder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Node(id)) => builder.append_value(*id),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Date => {
                    let mut builder = Date32Builder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Date(d)) => builder.append_value(*d),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(Value::Timestamp(t)) => builder.append_value(*t),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row_map in &row_maps {
                        let val = row_map.get(col.name.as_str());
                        match val {
                            Some(v) => builder.append_value(v.to_string()),
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
        let mut guard = self.transaction.lock();
        let tx = guard.take()
            .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let db: &Database = &self.client_context.database;
        let result = self.client_context
            .database
            .transaction_manager
            .rollback(db, &tx);
        drop(guard);
        result
    }

    fn plan_and_optimize(
        &self,
        stmt: crate::planner::binder::BoundStatement,
    ) -> Result<crate::planner::logical_plan::LogicalOperator> {
        let plan = crate::planner::LogicalPlanner::plan(stmt)?;
        let optimizer = crate::optimizer::Optimizer::new(
            self.client_context.database.catalog.inner_catalog(),
        );
        optimizer.optimize(plan)
    }

    /// Build a physical plan from a query string, handling cache lookup,
    /// parsing, binding, optimization, and physical planning.
    fn build_physical_plan(
        &self,
        query_str: &str,
        snapshot_ts: Option<u64>,
        explicit_tx: Option<Arc<crate::transaction::transaction_manager::Transaction>>,
    ) -> Result<(
        Box<dyn crate::processor::PhysicalOperator + Send + Sync>,
        Arc<crate::transaction::transaction_manager::Transaction>,
    )> {
        // Use the original query string as the cache key to avoid collisions
        // when queries differ only in literal values (the normalized form would
        // collapse e.g. `WHERE name = 'alice'` and `WHERE name = 'nobody'` into
        // the same key, causing stale literal values to be reused from cache).
        let query_hash = hash_cache_key(query_str);
        let pp_shard = cache_shard(query_str, 4);

        // Try physical plan cache first (fastest path: saves logical plan,
        // optimizer, and physical planner)
        let cached_pp = {
            let mut cache = self.client_context.database.physical_plan_caches[pp_shard].lock();
            cache.get(&query_hash).cloned()
        };
        if let Some(cached_plan) = cached_pp {
            // Read-only plans are safe to reuse across any transaction
            let plan = cached_plan.clone_box();
            let tx = match (snapshot_ts, explicit_tx) {
                (_, Some(tx)) => tx,
                (Some(ts), None) => Arc::new(
                    self.client_context
                        .database
                        .transaction_manager
                        .begin_at(true, ts)?,
                ),
                (None, None) => Arc::new(
                    self.client_context
                        .database
                        .transaction_manager
                        .begin(false)?,
                ),
            };
            let bm = &self.client_context.database.buffer_manager;
            let db: &Database = &self.client_context.database;
            db.storage_manager.read().flush_all_pending(bm, &tx)?;
            return Ok((plan, tx));
        }

        let tx = match (snapshot_ts, explicit_tx) {
            (_, Some(tx)) => tx,
            (Some(ts), None) => Arc::new(
                self.client_context
                    .database
                    .transaction_manager
                    .begin_at(true, ts)?,
            ),
            (None, None) => Arc::new(
                self.client_context
                    .database
                    .transaction_manager
                    .begin(false)?,
            ),
        };
        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &self.client_context.database;
        db.storage_manager.read().flush_all_pending(bm, &tx)?;

        // Try bound statement cache
        let (bound_stmt, binder_column_offsets) = {
            let plan_cache_key = query_str.to_string();
            let cache_shard_idx = cache_shard(&plan_cache_key, 4);
            let mut cache = self.client_context.database.plan_caches[cache_shard_idx].lock();
            let cached = cache.get(&plan_cache_key).cloned();
            if let Some(pair) = cached {
                let (stmt, offsets) = &*pair;
                (stmt.clone(), offsets.clone())
            } else {
                drop(cache);
                let query = parse(query_str)
                    .map_err(|e| LightningError::Query(e.to_string()))?;
                if !self.skip_auth_check {
                    validate_no_auth_table_access(&query)?;
                }
                let catalog = self.client_context.database.catalog.read();
                let mut binder = Binder::new(
                    &catalog,
                    &self.client_context.database.function_registry,
                );
                let bound_query = binder.bind_query(&query)?;
                drop(catalog);
                let binder_offsets = bound_query.column_offsets.clone();
                let bound_union = bound_query
                    .union_queries
                    .first()
                    .ok_or_else(|| LightningError::Query("No query".into()))?;
                let stmt = bound_union.statement.clone();
                // Only cache read-only Query statements. DML statements
                // (CREATE, DELETE, SET, etc.) contain literal values that
                // are normalized away by normalize_query, causing different
                // DML statements to share the same cache key and reuse
                // stale literal values from the first execution.
                // Note: Query statements can also contain literal values
                // (e.g., WHERE col = 'value'), so we use the original
                // query string (not normalized) as the cache key to avoid
                // collisions between queries with different literal values.
                if matches!(stmt, crate::planner::binder::BoundStatement::Query(..)) {
                    let mut cache = self.client_context.database.plan_caches[cache_shard_idx].lock();
                    cache.put(plan_cache_key, Arc::new((stmt.clone(), binder_offsets.clone())));
                }
                (stmt, binder_offsets)
            }
        };

        let logical_plan = self.plan_and_optimize(bound_stmt)?;
        let mut planner = PhysicalPlanner::new(
            Arc::clone(&self.client_context.database),
            tx.read_ts,
            tx.tx_id,
            Arc::clone(&tx.undo_buffer),
        );
        planner.binder_column_offsets = binder_column_offsets;
        let physical_plan = planner.plan(logical_plan)?;

        // Cache physical plan for read-only queries — these are safe to share
        // across all transactions since operators use tx.read_ts dynamically.
        // Non-read-only plans (DML, DDL) have tx_id baked into operators and
        // cannot be safely reused.
        if physical_plan.is_read_only() {
            self.client_context
                .database
                .physical_plan_caches[pp_shard]
                .lock()
                .put(query_hash, Arc::from(physical_plan.clone_box()));
        }

        Ok((physical_plan, tx))
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
        let (physical_plan, tx) = self.build_physical_plan(query_str, None, None)?;
        let mut processor = Processor::new(physical_plan);
        processor.execute_stream(
            Arc::clone(&self.client_context.database),
            tx,
            params,
        )
    }

    /// Execute a query as of a specific point in time (time-travel).
    /// `snapshot_ts` is an MVCC timestamp — use `now_micros()` or a
    /// previously observed timestamp to see the graph at that moment.
    /// The MVCC engine handles all version filtering automatically.
    #[tracing::instrument(skip(self, snapshot_ts, params))]
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
        self.client_context.database.metrics.record_query();

        let active_tx_guard = self.transaction.lock();
        if active_tx_guard.is_some() {
            drop(active_tx_guard);
            return Err(LightningError::Query(
                "execute_at cannot be used inside an explicit transaction; \
                 the snapshot timestamp would conflict with the transaction's read timestamp".into()
            ));
        }
        let is_autocommit = true;
        drop(active_tx_guard);

        let (physical_plan, tx) = self.build_physical_plan(
            query_str,
            Some(snapshot_ts),
            None,
        )?;
        let mut processor = Processor::new(physical_plan);
        let chunks = processor.execute(
            Arc::clone(&self.client_context.database),
            Arc::clone(&tx),
            params,
        )?;

        if is_autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.transaction_manager.commit(&tx, bm, db).inspect_err(|_e| {
                if let Err(rollback_err) = db.transaction_manager.rollback(db, &tx) {
                    tracing::warn!("Rollback after commit failure failed: {}", rollback_err);
                }
            })?;
        }

        Ok(QueryResult::new_arrow(
            vec![], vec![],
            chunks.into_iter().map(|c| c.batch).collect(),
        ))
    }

    #[tracing::instrument(skip(self, params))]
    pub fn execute(
        &self,
        query_str: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let _query_id = self
            .client_context
            .active_query_id
            .fetch_add(1, Ordering::SeqCst);
        self.client_context.database.metrics.record_query();

        let active_tx_guard = self.transaction.lock();
        let explicit_tx = active_tx_guard.as_ref().map(Arc::clone);
        let is_autocommit = explicit_tx.is_none();

        // Prevent use-after-commit: if this is an explicit transaction, check
        // that it hasn't been finalized by another thread between our lock
        // acquisition and the plan build.
        if let Some(ref tx) = explicit_tx {
            if tx.finalized.load(std::sync::atomic::Ordering::Acquire) {
                drop(active_tx_guard);
                return Err(LightningError::Internal(
                    "Transaction has already been committed or rolled back".into()
                ));
            }
        }
        drop(active_tx_guard);

        let (physical_plan, tx) = self.build_physical_plan(query_str, None, explicit_tx)?;
        let mut processor = Processor::new(physical_plan);
        let chunks = processor.execute(
            Arc::clone(&self.client_context.database),
            Arc::clone(&tx),
            params,
        )?;

        if is_autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.transaction_manager.commit(&tx, bm, db).inspect_err(|_e| {
                if let Err(rollback_err) = db.transaction_manager.rollback(db, &tx) {
                    tracing::warn!("Rollback after commit failure failed: {}", rollback_err);
                }
            })?;
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let threshold = self.client_context.database._config.slow_query_threshold_ms;
        if threshold > 0 && elapsed_ms >= threshold {
            tracing::warn!(
                "SLOW QUERY: {} ms | query: {}",
                elapsed_ms,
                query_str
            );
        }

        Ok(QueryResult::new_arrow(
            vec![],
            vec![],
            chunks.into_iter().map(|c| c.batch).collect(),
        ))
    }

    pub fn bulk_insert_batch(&self, table_name: &str, batch: &RecordBatch) -> Result<usize> {
        if self.client_context.database._config.read_only {
            return Err(LightningError::Config("Database is opened as read-only".into()));
        }
        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {table_name} not found")))?
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
        drop(storage);

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

        // Index all FixedSizeList(Float32) columns as vector embeddings
        // Iterate over final_batch columns (skip _id at index 0) to catch
        // dynamic embedding columns not present in the table catalog definition.
        if let Some(vec_idx) = vec_opt {
            let idx_dim = vec_idx.dimension();
            for col_idx in 1..final_batch.num_columns() {
                let array = final_batch.column(col_idx);
                if let Some(list_arr) = array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeListArray>()
                {
                    let arr_dim = list_arr.value_length() as usize;
                    if arr_dim == idx_dim {
                        if let Some(values) = list_arr
                            .values()
                            .as_any()
                            .downcast_ref::<arrow::array::Float32Array>()
                        {
                            let mut batch_vecs = Vec::with_capacity(num_rows);
                            for i in 0..num_rows {
                                let start = i * arr_dim;
                                let end = (i + 1) * arr_dim;
                                let emb = values.values()[start..end].to_vec();
                                batch_vecs.push((start_id + i as u64, emb));
                            }
                            if let Err(e) = vec_idx.insert_batch(&batch_vecs, &bm, &tx) {
                                tracing::warn!(
                                    "vector index insert_batch failed for table {}: {}",
                                    table_name,
                                    e,
                                );
                            }
                        }
                    }
                }
            }
        }

        db.storage_manager.read().flush_all_pending(&bm, &tx)?;
        db.transaction_manager.commit(&tx, &bm, &db)?;

        // Commit FTS index AFTER the database transaction so readers never see
        // an FTS document pointing to an uncommitted node_id.
        if let Some(fts) = fts_opt {
            let string_cols: Vec<usize> = table
                .columns
                .iter()
                .enumerate()
                .filter(|(col_idx, col)| {
                    *col_idx < final_batch.num_columns()
                        && col.data_type == lightning_types::LogicalType::String
                })
                .map(|(i, _)| i)
                .collect();

            if !string_cols.is_empty() {
                let col_names: Vec<String> = string_cols.iter()
                    .map(|&i| table.columns[i].name.clone())
                    .collect();
                let mut batch_docs: Vec<(u64, Vec<(String, &str)>)> = Vec::with_capacity(num_rows);
                let mut fields: Vec<(String, &str)> = Vec::new();
                for i in 0..num_rows {
                    let node_id = start_id + i as u64;
                    fields.clear();
                    for (j, &col_idx) in string_cols.iter().enumerate() {
                        let array = final_batch.column(col_idx);
                        if let Some(str_arr) =
                            array.as_any().downcast_ref::<arrow::array::StringArray>()
                        {
                            if str_arr.is_valid(i) && !str_arr.value(i).is_empty() {
                                fields.push((col_names[j].clone(), str_arr.value(i)));
                            }
                        }
                    }
                    if !fields.is_empty() {
                        batch_docs.push((node_id, std::mem::take(&mut fields)));
                    }
                }
                if !batch_docs.is_empty() {
                    if let Err(e) = fts.insert_multi_field_batch(&batch_docs) {
                        tracing::warn!(
                            "FTS insert_multi_field_batch error for table {}: {}",
                            table_name,
                            e,
                        );
                    }
                }
                if let Err(e) = fts.commit() {
                    tracing::warn!("FTS commit error: {}", e);
                }
            }
        }

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

        // Sync catalog stats from storage manager for the inserted table only.
        // Acquire catalog lock FIRST, then storage lock, to avoid deadlocks.
        let stats_snapshot = {
            let storage = db.storage_manager.read();
            (
                storage.rel_tables.get(table_name).map(|t| t.stats.read().clone()),
                storage.node_tables.get(table_name).map(|t| t.stats.read().clone()),
            )
        };
        {
            let mut cat = db.catalog.write();
            if let Some(entry) = cat.get_rel_table_mut(table_name) {
                if let Some(ref s) = stats_snapshot.0 {
                    entry.stats = s.clone();
                }
            }
            if let Some(entry) = cat.get_node_table_mut(table_name) {
                if let Some(ref s) = stats_snapshot.1 {
                    entry.stats = s.clone();
                }
            }
            db.catalog.mark_dirty();
        }

        Ok(num_rows)
    }
}
