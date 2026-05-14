use crate::processor::Value;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub enum UndoRecord {
    UpdateColumn(String, u64, Value), // table_name, row_id, old_value
    DeleteNode(String, u64),
    CreateNodeTable(String),
    CreateRelTable(String),
    DropTable(String, crate::catalog::TableEntry), // name, original definition
    CreateSequence(String),
    CreateMacro(String),
}

pub struct UndoBuffer {
    records: Mutex<Vec<UndoRecord>>,
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
                    db.catalog.write().remove_table(&name);
                    db.storage_manager.write().remove_table(&name);
                }
                UndoRecord::CreateRelTable(name) => {
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
                UndoRecord::CreateSequence(name) => {
                    db.catalog.write().sequences.remove(&name);
                }
                UndoRecord::CreateMacro(name) => {
                    db.catalog.write().macros.remove(&name);
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
}
