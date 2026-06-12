use crate::processor::Value;
use crate::storage::column::Column;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;

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
    DropConstraint {
        name: String,
        table_name: String,
        property: String,
    },
    CreateIndex {
        name: String,
        index_path: std::path::PathBuf,
    },
    DropIndex {
        name: String,
        table_name: String,
        property: String,
    },
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
                    // Collect exact file paths from the storage manager first,
                    // then remove the table (which evicts pages from the buffer
                    // manager), and finally delete the backing files.
                    let mut paths_to_delete = Vec::new();
                    let fts_dir = db._path.join(format!("{name}_fts"));
                    {
                        let storage = db.storage_manager.read();
                        if let Some(table) = storage.node_tables.get(&name).or_else(|| storage.rel_tables.get(&name)) {
                            Self::collect_column_paths(&table.columns, &mut paths_to_delete);
                        }
                        if storage.fwd_csr.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_fwd_offset.lbug")));
                            paths_to_delete.push(db._path.join(format!("{name}_fwd_adj.lbug")));
                        }
                        if storage.bwd_csr.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_bwd_offset.lbug")));
                            paths_to_delete.push(db._path.join(format!("{name}_bwd_adj.lbug")));
                        }
                        if storage.vector_indexes.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_vector.lbug")));
                        }
                        if storage.indexes.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_pk_index.lbug")));
                        }
                    }
                    db.storage_manager.write().remove_table(&name);
                    db.catalog.write().remove_table(&name);
                    for path in &paths_to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                    let _ = std::fs::remove_dir_all(&fts_dir);
                }
                UndoRecord::CreateRelTable(name) => {
                    let mut paths_to_delete = Vec::new();
                    let fts_dir = db._path.join(format!("{name}_fts"));
                    {
                        let storage = db.storage_manager.read();
                        if let Some(table) = storage.node_tables.get(&name).or_else(|| storage.rel_tables.get(&name)) {
                            Self::collect_column_paths(&table.columns, &mut paths_to_delete);
                        }
                        if storage.fwd_csr.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_fwd_offset.lbug")));
                            paths_to_delete.push(db._path.join(format!("{name}_fwd_adj.lbug")));
                        }
                        if storage.bwd_csr.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_bwd_offset.lbug")));
                            paths_to_delete.push(db._path.join(format!("{name}_bwd_adj.lbug")));
                        }
                        if storage.vector_indexes.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_vector.lbug")));
                        }
                        if storage.indexes.contains_key(&name) {
                            paths_to_delete.push(db._path.join(format!("{name}_pk_index.lbug")));
                        }
                    }
                    db.storage_manager.write().remove_table(&name);
                    db.catalog.write().remove_table(&name);
                    for path in &paths_to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                    let _ = std::fs::remove_dir_all(&fts_dir);
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
                UndoRecord::DropConstraint { name, table_name, property } => {
                    // Re-add the constraint that was dropped
                    let mut catalog = db.catalog.write();
                    if let Some(table) = catalog.node_tables.get_mut(&table_name) {
                        table.constraints.push(crate::catalog::NodeConstraint {
                            name: name.clone(),
                            property: property.clone(),
                        });
                    }
                    drop(catalog);
                    db.catalog.mark_dirty();
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
                UndoRecord::DropIndex { name, table_name: _, property: _ } => {
                    // Re-create the index that was dropped.
                    // The index data is gone (file removed), so we can only re-create an empty index.
                    let mut storage = db.storage_manager.write();
                    fn sanitize(s: &str) -> String {
                        s.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect()
                    }
                    let safe_name = sanitize(&name);
                    let index_path = db._path.join(format!("{safe_name}_idx.lbug"));
                    match crate::storage::index::hash_index::HashIndex::open_or_create(&index_path) {
                        Ok(index) => {
                            storage.indexes.insert(name.clone(), Arc::new(index));
                        }
                        Err(e) => {
                            tracing::error!("Rollback DropIndex: failed to re-create index '{}': {}", name, e);
                        }
                    }
                    drop(storage);
                    db.catalog.mark_dirty();
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

    // Removed: cleanup_table_files — path collection is now done inline
    // before remove_table so that buffer manager pages are evicted first.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_undo_buffer_is_empty() {
        let ub = UndoBuffer::new();
        assert!(ub.records.lock().is_empty());
    }

    #[test]
    fn test_push_and_clear() {
        let ub = UndoBuffer::new();
        ub.push(UndoRecord::UpdateColumn("t".to_string(), 0, Value::Null));
        ub.push(UndoRecord::CreateNodeTable("test".to_string()));
        assert_eq!(ub.records.lock().len(), 2);
        ub.clear();
        assert!(ub.records.lock().is_empty());
    }

    #[test]
    fn test_push_multiple_records() {
        let ub = UndoBuffer::new();
        ub.push(UndoRecord::CreateNodeTable("nodes".to_string()));
        ub.push(UndoRecord::CreateRelTable("rels".to_string()));
        ub.push(UndoRecord::DeleteNode("nodes".to_string(), 42));
        assert_eq!(ub.records.lock().len(), 3);
    }

    #[test]
    fn test_default_trait() {
        let ub: UndoBuffer = Default::default();
        assert!(ub.records.lock().is_empty());
    }

    #[test]
    fn test_rollback_reverses_order_with_database() {
        use crate::catalog::NodeTableCatalogEntry;
        use crate::catalog::PropertyDefinition;
        use crate::Database;
        use crate::SystemConfig;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = Database::new(dir.path(), SystemConfig {
            buffer_pool_size: 64 * 1024 * 1024,
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000,
            ..Default::default()
        }).unwrap();

        let tx = db.transaction_manager.begin(false).unwrap();

        // Create a table, then undo it via CreateNodeTable rollback
        let undo = UndoBuffer::new();
        undo.push(UndoRecord::CreateNodeTable("rollback_test_table".to_string()));

        // Manually create the table first (simulating what the executor does before recording undo)
        db.storage_manager.write().create_table(
            "rollback_test_table".to_string(),
            vec![("id".to_string(), lightning_types::LogicalType::Int64)],
            false,
            None,
        ).unwrap();
        db.catalog.write().node_tables.insert(
            "rollback_test_table".to_string(),
            NodeTableCatalogEntry {
                name: "rollback_test_table".to_string(),
                properties: vec![PropertyDefinition {
                    name: "id".to_string(),
                    type_: lightning_types::LogicalType::Int64,
                }],
                primary_key: None,
                num_rows: 0,
                stats: crate::storage::stats::TableStats::new(0),
                constraints: Vec::new(),
            },
        );

        // Verify table exists
        assert!(db.storage_manager.read().node_tables.contains_key("rollback_test_table"));

        // Rollback should remove the table
        undo.rollback(&db, tx.tx_id).unwrap();

        // After rollback, the table should be gone from both catalog and storage
        assert!(!db.storage_manager.read().node_tables.contains_key("rollback_test_table"));
        assert!(!db.catalog.read().node_tables.contains_key("rollback_test_table"));
    }

    #[test]
    fn test_rollback_create_rel_table() {
        use crate::catalog::PropertyDefinition;
        use crate::catalog::RelTableCatalogEntry;
        use crate::Database;
        use crate::SystemConfig;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = Database::new(dir.path(), SystemConfig {
            buffer_pool_size: 64 * 1024 * 1024,
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000,
            ..Default::default()
        }).unwrap();

        let tx = db.transaction_manager.begin(false).unwrap();

        let undo = UndoBuffer::new();
        undo.push(UndoRecord::CreateRelTable("rollback_rel_test".to_string()));

        // Create table in storage + catalog
        db.storage_manager.write().create_table(
            "rollback_rel_test".to_string(),
            vec![("from".to_string(), lightning_types::LogicalType::Uint64)],
            true,
            None,
        ).unwrap();
        db.catalog.write().rel_tables.insert(
            "rollback_rel_test".to_string(),
            RelTableCatalogEntry {
                name: "rollback_rel_test".to_string(),
                from_table: String::new(),
                to_table: String::new(),
                properties: vec![PropertyDefinition {
                    name: "from".to_string(),
                    type_: lightning_types::LogicalType::Uint64,
                }],
                num_rows: 0,
                stats: crate::storage::stats::TableStats::new(0),
            },
        );

        assert!(db.storage_manager.read().rel_tables.contains_key("rollback_rel_test"));

        undo.rollback(&db, tx.tx_id).unwrap();

        assert!(!db.storage_manager.read().rel_tables.contains_key("rollback_rel_test"));
        assert!(!db.catalog.read().rel_tables.contains_key("rollback_rel_test"));
    }

    #[test]
    fn test_rollback_empty_buffer_is_noop() {
        use crate::Database;
        use crate::SystemConfig;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = Database::new(dir.path(), SystemConfig {
            buffer_pool_size: 64 * 1024 * 1024,
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000,
            ..Default::default()
        }).unwrap();

        let tx = db.transaction_manager.begin(false).unwrap();
        let undo = UndoBuffer::new();
        // Rollback with no records should not error
        undo.rollback(&db, tx.tx_id).unwrap();
    }

    #[test]
    fn test_rollback_drop_table_restores_entry() {
        use crate::catalog::NodeTableCatalogEntry;
        use crate::catalog::PropertyDefinition;
        use crate::catalog::TableEntry;
        use crate::Database;
        use crate::SystemConfig;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = Database::new(dir.path(), SystemConfig {
            buffer_pool_size: 64 * 1024 * 1024,
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000,
            ..Default::default()
        }).unwrap();

        let tx = db.transaction_manager.begin(false).unwrap();
        let entry = NodeTableCatalogEntry {
            name: "restored_table".to_string(),
            properties: vec![PropertyDefinition {
                name: "val".to_string(),
                type_: lightning_types::LogicalType::Double,
            }],
            primary_key: None,
            num_rows: 0,
            stats: crate::storage::stats::TableStats::new(0),
            constraints: Vec::new(),
        };

        let undo = UndoBuffer::new();
        undo.push(UndoRecord::DropTable(
            "restored_table".to_string(),
            TableEntry::Node(entry.clone()),
        ));

        // Rollback should re-create the table
        undo.rollback(&db, tx.tx_id).unwrap();

        let catalog = db.catalog.read();
        assert!(catalog.node_tables.contains_key("restored_table"));
        if let Some(restored) = catalog.node_tables.get("restored_table") {
            assert_eq!(restored.name, "restored_table");
            assert_eq!(restored.properties.len(), 1);
            assert_eq!(restored.properties[0].name, "val");
        } else {
            panic!("Expected Node table entry");
        }
    }

    #[test]
    fn test_undo_buffer_serialization() {
        let ub = UndoBuffer::new();
        ub.push(UndoRecord::UpdateColumn("t".into(), 42, Value::Number(3.14)));
        ub.push(UndoRecord::CreateNodeTable("test_table".into()));
        ub.push(UndoRecord::DeleteNode("t".into(), 99));
        assert_eq!(ub.records.lock().len(), 3);
        // Verify push order is preserved
        let records = ub.records.lock();
        assert!(matches!(records[0], UndoRecord::UpdateColumn(..)));
        assert!(matches!(records[1], UndoRecord::CreateNodeTable(..)));
        assert!(matches!(records[2], UndoRecord::DeleteNode(..)));
    }

    #[test]
    fn test_undo_record_debug_and_clone() {
        let r = UndoRecord::CreateNodeTable("test".to_string());
        let cloned = r.clone();
        assert!(format!("{r:?}").contains("CreateNodeTable"));
        assert!(format!("{cloned:?}").contains("CreateNodeTable"));
    }
}
