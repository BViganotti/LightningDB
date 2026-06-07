pub mod api;
pub mod catalog;
pub use api::*;
pub mod capi;
pub mod config;
pub mod connection;
pub mod context;
pub mod error;
pub mod fusion;
pub mod ingestion;
pub mod memory;
pub mod metrics;
pub mod optimizer;
pub mod result;
pub mod util;
pub mod wasm_function;
pub mod parser;
pub mod planner;
pub mod processor;
pub mod storage;
pub mod transaction;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

pub use config::{SyncMode, SystemConfig};
pub use connection::Connection;
pub use context::ClientContext;
pub use error::{LightningError, Result};
pub use metrics::DatabaseMetrics;
pub use processor::Value;
pub use result::QueryResult;

use crate::catalog::{Catalog, LazyCatalog};
use crate::storage::WAL;
use crate::transaction::TransactionManager;

pub struct Database {
    pub(crate) _path: PathBuf,
    pub(crate) _config: SystemConfig,
    pub storage_manager: Arc<RwLock<crate::storage::storage_manager::StorageManager>>,
    pub wal: Arc<WAL>,
    pub transaction_manager: Arc<TransactionManager>,
    pub buffer_manager: Arc<crate::storage::buffer_manager::BufferManager>,
    pub free_space_manager: Arc<crate::storage::FreeSpaceManager>,
    pub catalog: Arc<LazyCatalog>,
    pub function_registry: Arc<RwLock<crate::processor::functions::FunctionRegistry>>,
    pub header: RwLock<crate::storage::DatabaseHeader>,
    pub plan_cache: Arc<RwLock<HashMap<String, crate::planner::binder::BoundStatement>>>,
    pub metrics: DatabaseMetrics,

    vacuum_handle: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.checkpoint().ok();

        self.buffer_manager.shutdown();

        let fhs = {
            let sm = self.storage_manager.read();
            sm.get_all_file_handles()
        };
        for _ in 0..20 {
            let dirty_exists = self.buffer_manager.dirty_page_count() > 0;
            if !dirty_exists {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        self.buffer_manager.flush_all_with_handles(&fhs);
        drop(fhs);

        let remaining_dirty = self.buffer_manager.dirty_page_count();
        if remaining_dirty > 0 {
            tracing::warn!(
                "{} dirty pages remain after final flush during Database::drop",
                remaining_dirty
            );
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

        let catalog_path = path.join("catalog.lbug");
        let catalog = Arc::new(
            LazyCatalog::from_disk(&catalog_path)
                .unwrap_or_else(|_| LazyCatalog::new(Catalog::new(), Some(catalog_path.clone()))),
        );

        {
            let cat = catalog.read();
            for table_entry in cat.node_tables.values() {
                let col_defs: Vec<(String, lightning_types::LogicalType)> = table_entry
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
                if let Err(e) = storage_manager.create_vector_index(&table_entry.name, crate::memory::DEFAULT_EMBEDDING_DIM) {
                    tracing::warn!("Vector index creation failed for {}: {}", table_entry.name, e);
                }
            }
            for table_entry in cat.rel_tables.values() {
                let col_defs: Vec<(String, lightning_types::LogicalType)> = table_entry
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
            function_registry: Arc::new(RwLock::new(crate::processor::functions::FunctionRegistry::new())),
            header: RwLock::new(header),
            plan_cache: Arc::new(RwLock::new(HashMap::new())),
            metrics: DatabaseMetrics::new(),

            vacuum_handle: Some(vacuum_handle),
        }))
    }

    pub fn connect(self: &Arc<Self>) -> Connection {
        Connection::new(Arc::clone(self))
    }

    pub fn register_wasm_function<P: AsRef<std::path::Path>>(
        &self,
        wasm_path: P,
        func_name: &str,
    ) -> Result<()> {
        let wasm_func = crate::wasm_function::WasmFunction::load(wasm_path, func_name)?;
        let scalar = wasm_func.to_scalar_function();
        self.function_registry.write().register_scalar(scalar);
        tracing::info!("Registered WASM function: {}", func_name);
        Ok(())
    }

    pub fn get_catalog_path(&self) -> PathBuf {
        self._path.join("catalog.lbug")
    }

    pub fn checkpoint(&self) -> Result<()> {
        let start = std::time::Instant::now();
        self.buffer_manager.checkpoint()?;

        {
            let fsm_path = self._path.join("free_space.bin");
            if let Err(e) = self.free_space_manager.save(&fsm_path) {
                tracing::warn!("Failed to save free space map during checkpoint: {}", e);
            }
        }

        {
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
            drop(cat);
            if let Err(e) = self.catalog.force_save() {
                tracing::warn!("Failed to save catalog during checkpoint: {}", e);
            }
        }

        let last_ts = self.transaction_manager.get_current_ts();
        {
            let mut header = self.header.write();
            header.last_checkpoint_ts = last_ts;
            let header_path = self._path.join("database.header");
            header.save(&header_path)?;
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
            self.storage_manager.write().rebuild_csr_if_stale(table_name, bm, &tx)?;
        }

        if let Err(e) = self.transaction_manager.rollback(self, &tx) {
            tracing::warn!("VACUUM transaction rollback failed: {}", e);
        }

        self.checkpoint()?;

        let elapsed = start.elapsed();
        tracing::info!("VACUUM completed in {:?} for {} tables", elapsed, tables.len());
        Ok(())
    }

    pub fn repair_cardinalities(&self) -> Result<()> {
        let mut repairs: Vec<(String, u64, bool)> = Vec::new();
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
        self.catalog.force_save()?;
        tracing::info!("Catalog saved after cardinality repair");
        Ok(())
    }
}
