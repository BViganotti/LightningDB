use crate::catalog::PropertyDefinition;
use crate::processor::*;
use crate::storage::undo_buffer::{UndoBuffer, UndoRecord};
use crate::Database;
use std::sync::Arc;

pub enum DDLAction {
    CreateNode {
        name: String,
        columns: Vec<PropertyDefinition>,
        primary_key: String,
    },
    CreateRel {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<PropertyDefinition>,
    },
    DropTable(String),
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
}

pub struct PhysicalDDL {
    action: DDLAction,
    db: Arc<Database>,
    undo_buffer: Arc<UndoBuffer>,
    executed: bool,
}

impl PhysicalDDL {
    pub fn new_create_node(
        name: String,
        columns: Vec<PropertyDefinition>,
        primary_key: String,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateNode {
                name,
                columns,
                primary_key,
            },
            db,
            undo_buffer,
            executed: false,
        }
    }

    pub fn new_create_rel(
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<PropertyDefinition>,
        db: Arc<Database>,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            action: DDLAction::CreateRel {
                name,
                from_table,
                to_table,
                columns,
            },
            db,
            undo_buffer,
            executed: false,
        }
    }

    pub fn new_drop(name: String, db: Arc<Database>, undo_buffer: Arc<UndoBuffer>) -> Self {
        Self {
            action: DDLAction::DropTable(name),
            db,
            undo_buffer,
            executed: false,
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
            executed: false,
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
            executed: false,
        }
    }
}

impl crate::processor::PhysicalOperator for PhysicalDDL {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> crate::Result<Option<crate::processor::DataChunk>> {
        if self.executed {
            return Ok(None);
        }

        match &self.action {
            DDLAction::CreateNode {
                name,
                columns,
                primary_key,
            } => {
                // 1. Update Catalog
                let mut catalog = database.catalog.write();
                catalog.add_node_table(name.clone(), columns.clone(), Some(primary_key.clone()));

                // 2. Update Storage
                let mut storage = database.storage_manager.write();
                let col_defs = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_.clone()))
                    .collect();
                storage.create_table(name.clone(), col_defs, false, None)?;
                storage.create_index(&name)?;

                // 3. Register for rollback
                self.undo_buffer
                    .push(UndoRecord::CreateNodeTable(name.clone()));

                // 4. Save Catalog (lazy - will save on next commit if needed)
                database.catalog.mark_dirty();

                // 5. Mark executed
                self.executed = true;
            }
            DDLAction::CreateRel {
                name,
                from_table,
                to_table,
                columns,
            } => {
                // 1. Update Catalog
                let mut catalog = database.catalog.write();
                catalog.add_rel_table(
                    name.clone(),
                    from_table.clone(),
                    to_table.clone(),
                    columns.clone(),
                );

                // 2. Update Storage
                let mut storage = database.storage_manager.write();
                let col_defs = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_.clone()))
                    .collect();
                storage.create_table(name.clone(), col_defs, true, None)?;

                // 3. Register for rollback
                self.undo_buffer
                    .push(UndoRecord::CreateRelTable(name.clone()));

                // 4. Save Catalog (lazy - will save on next commit if needed)
                database.catalog.mark_dirty();

                // 5. Mark executed
                self.executed = true;
            }
            DDLAction::DropTable(name) => {
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
                self.executed = true;
            }
            DDLAction::CreateSequence {
                name,
                start_with,
                increment_by,
            } => {
                let mut catalog = database.catalog.write();
                catalog.add_sequence(name.clone(), *start_with, *increment_by);
                database.catalog.mark_dirty();
                self.undo_buffer
                    .push(UndoRecord::CreateSequence(name.clone()));
                self.executed = true;
            }
            DDLAction::CreateMacro { name, params, body } => {
                let mut catalog = database.catalog.write();
                catalog.add_macro(name.clone(), params.clone(), body.clone());
                database.catalog.mark_dirty();
                self.undo_buffer.push(UndoRecord::CreateMacro(name.clone()));
                self.executed = true;
            }
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            action: match &self.action {
                DDLAction::CreateNode {
                    name,
                    columns,
                    primary_key,
                } => DDLAction::CreateNode {
                    name: name.clone(),
                    columns: columns.clone(),
                    primary_key: primary_key.clone(),
                },
                DDLAction::CreateRel {
                    name,
                    from_table,
                    to_table,
                    columns,
                } => DDLAction::CreateRel {
                    name: name.clone(),
                    from_table: from_table.clone(),
                    to_table: to_table.clone(),
                    columns: columns.clone(),
                },
                DDLAction::DropTable(name) => DDLAction::DropTable(name.clone()),
                DDLAction::CreateSequence {
                    name,
                    start_with,
                    increment_by,
                } => DDLAction::CreateSequence {
                    name: name.clone(),
                    start_with: *start_with,
                    increment_by: *increment_by,
                },
                DDLAction::CreateMacro { name, params, body } => DDLAction::CreateMacro {
                    name: name.clone(),
                    params: params.clone(),
                    body: body.clone(),
                },
            },
            db: self.db.clone(),
            undo_buffer: self.undo_buffer.clone(),
            executed: self.executed,
        })
    }
}
