use crate::processor::Value;
use crate::storage::column::Column;
use parking_lot::Mutex;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum UndoRecord {
    UpdateColumn(String, u64, Value),
    DeleteNode(String, u64),
    CreateNodeTable(String),
    CreateRelTable(String),
    DropTable(String, crate::catalog::TableEntry),
    AlterAddColumn {
        table_name: String,
        col_name: String,
    },
    AlterDropColumn {
        table_name: String,
        col_name: String,
        col_type: lightning_types::LogicalType,
    },
    AlterRenameTable {
        old_name: String,
        new_name: String,
    },
    AlterRenameColumn {
        table_name: String,
        old_name: String,
        new_name: String,
    },
    CreateSequence(String),
    CreateMacro(String),
    CreateConstraint {
        name: String,
        table_name: String,
        property: String,
    },
    DropConstraint(String),
    CreateIndex {
        name: String,
        index_path: std::path::PathBuf,
    },
    DropIndex(String),
}

pub struct UndoBuffer {
    records: Mutex<Vec<UndoRecord>>,
}

impl Default for UndoBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl UndoBuffer {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, record: UndoRecord) {
        self.records.lock().push(record);
    }

    pub fn rollback(&self, db: &crate::Database, tx_id: u64) -> crate::Result<()> {
        let mut records = self.records.lock();
        // Rollback in reverse order
        while let Some(record) = records.pop() {
            match record {
                UndoRecord::UpdateColumn(_table_name, _row_id, _old_val) => {
                    // Handled by page-level rollback in BufferManager::rollback_versions (below).
                    // Each transaction gets its own CoW page version via create_new_version, so
                    // rollback_versions discards only this transaction's versions without affecting
                    // concurrently committed changes on the same page.
                    // UNCOMMITTED_BIT checks (0.1.1/0.1.2) ensure dirty uncommitted pages are
                    // never evicted to disk, so page-level rollback is always safe.
                }
                UndoRecord::DeleteNode(_table_name, _row_id) => {
                    // Handled by page-level rollback in BufferManager::rollback_versions (below).
                    // Deletes create new page versions that remove the row's data. On rollback,
                    // discarding the CoW page version restores the original row data.
                    // See 0.1.1/0.1.2 for eviction safety guarantee.
                }
                UndoRecord::CreateNodeTable(name) => {
                    // Delete data files BEFORE removing from storage manager
                    // so we can enumerate exact column paths from the table definition
                    Self::cleanup_table_files(db, &name);
                    db.catalog.write().remove_table(&name);
                    db.storage_manager.write().remove_table(&name);
                }
                UndoRecord::CreateRelTable(name) => {
                    Self::cleanup_table_files(db, &name);
                    db.catalog.write().remove_table(&name);
                    db.storage_manager.write().remove_table(&name);
                }
                UndoRecord::DropTable(name, entry) => {
                    let mut catalog = db.catalog.write();
                    match entry {
                        crate::catalog::TableEntry::Node(e) => {
                            let col_defs: Vec<(String, lightning_types::LogicalType)> = e
                                .properties
                                .iter()
                                .map(|p| (p.name.clone(), p.type_.clone()))
                                .collect();
                            db.storage_manager.write().create_table(
                                name.clone(),
                                col_defs,
                                false,
                                Some(e.stats.clone()),
                            )?;
                            catalog.node_tables.insert(name, e);
                        }
                        crate::catalog::TableEntry::Rel(e) => {
                            let col_defs: Vec<(String, lightning_types::LogicalType)> = e
                                .properties
                                .iter()
                                .map(|p| (p.name.clone(), p.type_.clone()))
                                .collect();
                            db.storage_manager.write().create_table(
                                name.clone(),
                                col_defs,
                                true,
                                Some(e.stats.clone()),
                            )?;
                            catalog.rel_tables.insert(name, e);
                        }
                    }
                }
                UndoRecord::AlterAddColumn { table_name, col_name } => {
                    let mut catalog = db.catalog.write();
                    if let Err(e) = catalog.remove_column_from_table(&table_name, &col_name) {
                        tracing::error!("Rollback AlterAddColumn: failed to remove column from catalog: {}", e);
                    }
                    let mut storage = db.storage_manager.write();
                    if let Err(e) = storage.remove_column_from_table(&table_name, &col_name) {
                        tracing::error!("Rollback AlterAddColumn: failed to remove column from storage: {}", e);
                    }
                }
                UndoRecord::AlterDropColumn { table_name, col_name, col_type } => {
                    let mut catalog = db.catalog.write();
                    if let Err(e) = catalog.add_column_to_table(&table_name, col_name.clone(), col_type.clone()) {
                        tracing::error!("Rollback AlterDropColumn: failed to add column to catalog: {}", e);
                    }
                    let mut storage = db.storage_manager.write();
                    if let Err(e) = storage.add_column_to_table(&table_name, &col_name, col_type) {
                        tracing::error!("Rollback AlterDropColumn: failed to add column to storage: {}", e);
                    }
                }
                UndoRecord::AlterRenameTable { old_name, new_name } => {
                    {
                        let mut catalog = db.catalog.write();
                        if let Some(mut entry) = catalog.node_tables.remove(&new_name) {
                            entry.name = old_name.clone();
                            catalog.node_tables.insert(old_name.clone(), entry);
                        } else if let Some(mut entry) = catalog.rel_tables.remove(&new_name) {
                            entry.name = old_name.clone();
                            catalog.rel_tables.insert(old_name.clone(), entry);
                        }
                    }
                    let mut storage = db.storage_manager.write();
                    if let Some(mut entry) = storage.node_tables.remove(&new_name) {
                        entry.name = old_name.clone();
                        storage.node_tables.insert(old_name.clone(), entry);
                    } else if let Some(mut entry) = storage.rel_tables.remove(&new_name) {
                        entry.name = old_name.clone();
                        storage.rel_tables.insert(old_name.clone(), entry);
                    }
                }
                UndoRecord::AlterRenameColumn { table_name, old_name, new_name } => {
                    let mut catalog = db.catalog.write();
                    if let Err(e) = catalog.rename_column_in_table(&table_name, &new_name, &old_name) {
                        tracing::error!("Rollback AlterRenameColumn: failed to rename column in catalog: {}", e);
                    }
                    let mut storage = db.storage_manager.write();
                    let table = if storage.node_tables.contains_key(&table_name) {
                        storage.node_tables.get_mut(&table_name)
                    } else {
                        storage.rel_tables.get_mut(&table_name)
                    };
                    if let Some(table) = table {
                        if let Some(col) = table.columns.iter_mut().find(|c| c.name == *new_name) {
                            col.name = old_name.clone();
                        }
                    }
                }
                UndoRecord::CreateSequence(name) => {
                    db.catalog.write().sequences.remove(&name);
                }
                UndoRecord::CreateMacro(name) => {
                    db.catalog.write().macros.remove(&name);
                }
                UndoRecord::CreateConstraint {
                    name,
                    table_name,
                    property: _,
                } => {
                    let mut catalog = db.catalog.write();
                    if let Some(table) = catalog.node_tables.get_mut(&table_name) {
                        table.constraints.retain(|c| c.name != name);
                    }
                }
                UndoRecord::DropConstraint(name) => {
                    // Drop constraint undo is more complex — we'd need to re-add the constraint.
                    tracing::warn!("Drop constraint rollback not fully implemented: {name}");
                }
                UndoRecord::CreateIndex {
                    name,
                    index_path,
                } => {
                    let mut storage = db.storage_manager.write();
                    storage.indexes.remove(&name);
                    if let Err(e) = std::fs::remove_file(&index_path) {
                        tracing::error!("Rollback CreateIndex: failed to remove index file {}: {}", index_path.display(), e);
                    }
                }
                UndoRecord::DropIndex(name) => {
                    tracing::warn!("Drop index rollback not fully implemented: {name}");
                }
            }
        }
        // Essential: Discard all versions created by this transaction
        db.buffer_manager.rollback_versions(tx_id)?;
        Ok(())
    }

    pub fn clear(&self) {
        self.records.lock().clear();
    }

    /// Collect all file paths from a column hierarchy recursively.
    fn collect_column_paths(columns: &[Column], paths: &mut Vec<PathBuf>) {
        for col in columns {
            paths.push(col.fh.path.clone());
            paths.push(col.null_fh.path.clone());
            if let Some(ref overflow_fh) = col.overflow_fh {
                paths.push(overflow_fh.path.clone());
            }
            Self::collect_column_paths(&col.child_columns, paths);
        }
    }

    /// Delete all data files associated with a table using exact paths
    /// from the storage manager. Called during rollback of CREATE TABLE
    /// to prevent stale files from corrupting a subsequent CREATE.
    /// Must be called BEFORE remove_table so the column list is available.
    fn cleanup_table_files(db: &crate::Database, table_name: &str) {
        let mut paths_to_delete: Vec<PathBuf> = Vec::new();

        // Collect exact file paths from the storage manager's table definition
        let storage = db.storage_manager.read();
        if let Some(table) = storage
            .node_tables
            .get(table_name)
            .or_else(|| storage.rel_tables.get(table_name))
        {
            Self::collect_column_paths(&table.columns, &mut paths_to_delete);
        }
        if storage.fwd_csr.contains_key(table_name) {
            paths_to_delete.push(db._path.join(format!("{table_name}_fwd_offset.lbug")));
            paths_to_delete.push(db._path.join(format!("{table_name}_fwd_adj.lbug")));
        }
        if storage.bwd_csr.contains_key(table_name) {
            paths_to_delete.push(db._path.join(format!("{table_name}_bwd_offset.lbug")));
            paths_to_delete.push(db._path.join(format!("{table_name}_bwd_adj.lbug")));
        }
        if storage.vector_indexes.contains_key(table_name) {
            paths_to_delete.push(db._path.join(format!("{table_name}_vector.lbug")));
        }
        if storage.indexes.contains_key(table_name) {
            paths_to_delete.push(db._path.join(format!("{table_name}_pk_index.lbug")));
        }
        drop(storage);

        // Delete only the exact files known to belong to this table
        for path in &paths_to_delete {
            let _ = std::fs::remove_file(path);
        }

        // Also try to remove the FTS directory
        let fts_dir = db._path.join(format!("{}_fts", table_name));
        let _ = std::fs::remove_dir_all(&fts_dir);
    }
}
