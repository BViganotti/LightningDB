use crate::catalog::PropertyDefinition;
use crate::processor::*;
use crate::storage::undo_buffer::{UndoBuffer, UndoRecord};
use crate::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub enum DDLAction {
    CreateNode {
        name: String,
        columns: Vec<PropertyDefinition>,
        primary_key: String,
        if_not_exists: bool,
    },
    CreateRel {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<PropertyDefinition>,
        if_not_exists: bool,
    },
    DropTable(String, bool),
    AlterAddColumn {
        table_name: String,
        col_name: String,
        data_type: lightning_types::LogicalType,
    },
    AlterDropColumn {
        table_name: String,
        col_name: String,
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
    CreateSequence {
        name: String,
        start_with: u64,
        increment_by: i64,
    },
    CreateMacro {
        name: String,
        params: Vec<String>,
        body: crate::parser::ast::Expression,
    },
    CreateConstraint {
        name: String,
        table_name: String,
        property: String,
    },
    DropConstraint(String),
    CreateIndex {
        name: String,
        table_name: String,
        property: String,
    },
    CreateVectorIndex {
        table_name: String,
        metric: String,
        dimension: usize,
    },
    CreateFtsIndex {
        table_name: String,
    },
    DropIndex(String),
}

pub struct PhysicalDDL {
    action: DDLAction,
    db: Arc<Database>,
    undo_buffer: Arc<UndoBuffer>,
    executed: Arc<AtomicBool>,
}

impl PhysicalDDL {
    pub fn new_create_node(
        name: String,
        columns: Vec<PropertyDefinition>,
        primary_key: String,
        if_not_exists: bool,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_create_rel(
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<PropertyDefinition>,
        if_not_exists: bool,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_drop(name: String, if_exists: bool, db: Arc<Database>, undo_buffer: Arc<UndoBuffer>) -> Self {
        Self {
            action: DDLAction::DropTable(name, if_exists),
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_create_sequence(
        name: String,
        start_with: u64,
        increment_by: i64,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateSequence {
                name,
                start_with,
                increment_by,
            },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_create_macro(
        name: String,
        params: Vec<String>,
        body: crate::parser::ast::Expression,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateMacro { name, params, body },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_create_constraint(
        name: String,
        table_name: String,
        property: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateConstraint { name, table_name, property },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_drop_constraint(
        name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::DropConstraint(name),
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_create_index(
        name: String,
        table_name: String,
        property: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateIndex { name, table_name, property },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_drop_index(
        name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::DropIndex(name),
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_create_vector_index(
        table_name: String,
        metric: String,
        dimension: usize,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateVectorIndex { table_name, metric, dimension },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_create_fts_index(
        table_name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateFtsIndex { table_name },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_alter_add_column(
        table_name: String,
        col_name: String,
        data_type: lightning_types::LogicalType,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::AlterAddColumn { table_name, col_name, data_type },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_alter_drop_column(
        table_name: String,
        col_name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::AlterDropColumn { table_name, col_name },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_alter_rename_table(
        old_name: String,
        new_name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::AlterRenameTable { old_name, new_name },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }

    pub fn new_alter_rename_column(
        table_name: String,
        old_name: String,
        new_name: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::AlterRenameColumn { table_name, old_name, new_name },
            db,
            undo_buffer,
            executed: Arc::new(AtomicBool::new(false)),
    }
    }
}

impl crate::processor::PhysicalOperator for PhysicalDDL {
    fn get_next(
        &mut self,
        database: &crate::Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> crate::Result<Option<crate::processor::DataChunk>> {
        if self.executed.load(Ordering::Acquire) {
            return Ok(None);
        }
        if self
            .executed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(None);
        }

        match &self.action {
            DDLAction::CreateNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            } => {
                // Check for existing table
                let catalog_exists = database.catalog.read().get_node_table(name).is_some();
                if catalog_exists {
                    if *if_not_exists {
                        return Ok(None);
                    }
                    return Err(crate::LightningError::Database(format!(
                        "Table '{}' already exists", name
                    )));
                }
                // 1. Update Catalog
                let mut catalog = database.catalog.write();
                catalog.add_node_table(name.clone(), columns.clone(), Some(primary_key.clone()))?;

                // 2. Update Storage
                let mut storage = database.storage_manager.write();
                let col_defs = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_.clone()))
                    .collect();
                storage.create_table(name.clone(), col_defs, false, None)?;
                if !primary_key.is_empty() {
                    storage.create_index(name)?;
                }
                storage.set_fsm_on_all_file_handles();

                // 3. Register for rollback
                self.undo_buffer
                    .push(UndoRecord::CreateNodeTable(name.clone()));

                // 4. Save Catalog (lazy - will save on next commit if needed)
                database.catalog.mark_dirty();

                // 5. Mark executed
            }
            DDLAction::CreateRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            } => {
                let catalog_exists = database.catalog.read().get_rel_table(name).is_some();
                if catalog_exists {
                    if *if_not_exists {
                        return Ok(None);
                    }
                    return Err(crate::LightningError::Database(format!(
                        "Table '{}' already exists", name
                    )));
                }
                // 1. Update Catalog
                let mut catalog = database.catalog.write();
                catalog.add_rel_table(
                    name.clone(),
                    from_table.clone(),
                    to_table.clone(),
                    columns.clone(),
                )?;

                // 2. Update Storage
                let mut storage = database.storage_manager.write();
                let col_defs = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_.clone()))
                    .collect();
                storage.create_table(name.clone(), col_defs, true, None)?;
                storage.set_fsm_on_all_file_handles();

                // 3. Register for rollback
                self.undo_buffer
                    .push(UndoRecord::CreateRelTable(name.clone()));

                // 4. Save Catalog (lazy - will save on next commit if needed)
                database.catalog.mark_dirty();

                // 5. Mark executed
            }
            DDLAction::DropTable(name, _if_exists) => {
                // 1. Get original before dropping
                let entry = {
                    let catalog = database.catalog.read();
                    if let Some(node) = catalog.get_node_table(name) {
                        Some(crate::catalog::TableEntry::Node(node.clone()))
                    } else {
                        catalog
                            .get_rel_table(name)
                            .map(|rel| crate::catalog::TableEntry::Rel(rel.clone()))
                    }
                };

                if let Some(entry) = entry {
                    // 2. Update Catalog
                    let mut catalog = database.catalog.write();
                    catalog.remove_table(name);

                    // 3. Update Storage
                    let mut storage = database.storage_manager.write();
                    storage.remove_table(name);

                    // 4. Register for rollback
                    self.undo_buffer
                        .push(UndoRecord::DropTable(name.clone(), entry));

                    // 5. Save Catalog (lazy - will save on next commit if needed)
                    database.catalog.mark_dirty();
                }
            }
            DDLAction::CreateSequence {
                name,
                start_with,
                increment_by,
            } => {
                let mut catalog = database.catalog.write();
                catalog.add_sequence(name.clone(), *start_with, *increment_by)?;
                database.catalog.mark_dirty();
                self.undo_buffer
                    .push(UndoRecord::CreateSequence(name.clone()));
            }
            DDLAction::CreateMacro { name, params, body } => {
                let mut catalog = database.catalog.write();
                catalog.add_macro(name.clone(), params.clone(), body.clone())?;
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::CreateMacro(name.clone()));
            }
            DDLAction::CreateConstraint {
                name,
                table_name,
                property,
            } => {
                let mut catalog = database.catalog.write();
                catalog.add_constraint(
                    table_name,
                    crate::catalog::NodeConstraint {
                        name: name.clone(),
                        property: property.clone(),
                    },
                )?;
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::CreateConstraint {
                    name: name.clone(),
                    table_name: table_name.clone(),
                    property: property.clone(),
                });
            }
            DDLAction::DropConstraint(name) => {
                let mut catalog = database.catalog.write();
                // Capture constraint info before removing for undo
                let constraint_info = catalog.node_tables.values()
                    .flat_map(|t| t.constraints.iter().map(move |c| (t.name.clone(), c)))
                    .find(|(_, c)| c.name == *name)
                    .map(|(table_name, c)| (table_name, c.property.clone()));
                catalog.remove_constraint(name)?;
                database.catalog.mark_dirty();
                if let Some((table_name, property)) = constraint_info {
                    self.undo_buffer.push(UndoRecord::DropConstraint {
                        name: name.clone(),
                        table_name,
                        property,
                    });
                }
            }
            DDLAction::CreateIndex {
                name,
                table_name,
                property: _,
            } => {
                fn sanitize(s: &str) -> String {
                    s.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect()
                }
                let mut storage = database.storage_manager.write();
                let safe_name = sanitize(name);
                let safe_table = sanitize(table_name);
                let index_path = database._path.join(format!("{safe_table}_{safe_name}_idx.lbug"));
                let index = crate::storage::index::hash_index::HashIndex::open_or_create(
                    &index_path,
                )?;
                storage.indexes.insert(name.clone(), Arc::new(index));
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::CreateIndex {
                    name: name.clone(),
                    index_path,
                });
            }
            DDLAction::DropIndex(name) => {
                let mut storage = database.storage_manager.write();
                storage.indexes.remove(name);
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::DropIndex {
                    name: name.clone(),
                    table_name: String::new(),
                    property: String::new(),
                });
            }
            DDLAction::CreateVectorIndex {
                table_name,
                metric: _,
                dimension,
            } => {
                let mut storage = database.storage_manager.write();
                storage.create_vector_index(table_name, *dimension)?;
                database.catalog.mark_dirty();
            }
            DDLAction::CreateFtsIndex { table_name } => {
                let mut storage = database.storage_manager.write();
                storage.create_fts_index(table_name)?;
                database.catalog.mark_dirty();
            }
            DDLAction::AlterAddColumn { table_name, col_name, data_type } => {
                let mut storage = database.storage_manager.write();
                storage.add_column_to_table(table_name, col_name, data_type.clone())?;
                let mut catalog = database.catalog.write();
                catalog.add_column_to_table(table_name, col_name.clone(), data_type.clone())?;
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::AlterAddColumn {
                    table_name: table_name.clone(),
                    col_name: col_name.clone(),
                });
            }
            DDLAction::AlterDropColumn { table_name, col_name } => {
                let mut storage = database.storage_manager.write();
                storage.remove_column_from_table(table_name, col_name)?;
                let mut catalog = database.catalog.write();
                let removed = catalog.remove_column_from_table(table_name, col_name)?;
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::AlterDropColumn {
                    table_name: table_name.clone(),
                    col_name: col_name.clone(),
                    col_type: removed.type_,
                });
            }
            DDLAction::AlterRenameTable { old_name, new_name } => {
                let mut catalog = database.catalog.write();
                catalog.rename_table(old_name, new_name)?;
                {
                    let mut storage = database.storage_manager.write();
                    if let Some(table) = storage.node_tables.remove(old_name) {
                        let mut t = table;
                        t.name = new_name.clone();
                        storage.node_tables.insert(new_name.clone(), t);
                    } else if let Some(table) = storage.rel_tables.remove(old_name) {
                        let mut t = table;
                        t.name = new_name.clone();
                        storage.rel_tables.insert(new_name.clone(), t);
                    }
                }
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::AlterRenameTable {
                    old_name: old_name.clone(),
                    new_name: new_name.clone(),
                });
            }
            DDLAction::AlterRenameColumn { table_name, old_name, new_name } => {
                let mut catalog = database.catalog.write();
                catalog.rename_column_in_table(table_name, old_name, new_name)?;
                {
                    let mut storage = database.storage_manager.write();
                    let table = if storage.node_tables.contains_key(table_name) {
                        storage.node_tables.get_mut(table_name)
                    } else {
                        storage.rel_tables.get_mut(table_name)
                    };
                    if let Some(table) = table {
                        if let Some(col) = table.columns.iter_mut().find(|c| c.name == *old_name) {
                            col.name = new_name.clone();
                        }
                    }
                }
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::AlterRenameColumn {
                    table_name: table_name.clone(),
                    old_name: old_name.clone(),
                    new_name: new_name.clone(),
                });
            }
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            action: self.action.clone(),
            db: self.db.clone(),
            undo_buffer: self.undo_buffer.clone(),
            executed: Arc::clone(&self.executed),
        })
    }

    fn is_read_only(&self) -> bool {
        false
    }
}
