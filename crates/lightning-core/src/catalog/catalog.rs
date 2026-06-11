use crate::storage::stats::TableStats;
use lightning_types::LogicalType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub trait TableLike {
    fn properties_mut(&mut self) -> &mut Vec<PropertyDefinition>;
}

impl TableLike for NodeTableCatalogEntry {
    fn properties_mut(&mut self) -> &mut Vec<PropertyDefinition> {
        &mut self.properties
    }
}

impl TableLike for RelTableCatalogEntry {
    fn properties_mut(&mut self) -> &mut Vec<PropertyDefinition> {
        &mut self.properties
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub name: String,
    pub type_: LogicalType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConstraint {
    pub name: String,
    pub property: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTableCatalogEntry {
    pub name: String,
    pub properties: Vec<PropertyDefinition>,
    pub num_rows: u64,
    pub primary_key: Option<String>,
    pub constraints: Vec<NodeConstraint>,
    pub stats: TableStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelTableCatalogEntry {
    pub name: String,
    pub from_table: String,
    pub to_table: String,
    pub properties: Vec<PropertyDefinition>,
    pub num_rows: u64,
    pub stats: TableStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceCatalogEntry {
    pub name: String,
    pub next_val: u64,
    pub increment: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroCatalogEntry {
    pub name: String,
    pub params: Vec<String>,
    pub body: crate::parser::ast::Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableEntry {
    Node(NodeTableCatalogEntry),
    Rel(RelTableCatalogEntry),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Catalog {
    pub node_tables: HashMap<String, NodeTableCatalogEntry>,
    pub rel_tables: HashMap<String, RelTableCatalogEntry>,
    pub sequences: HashMap<String, SequenceCatalogEntry>,
    pub macros: HashMap<String, MacroCatalogEntry>,
    #[serde(skip)]
    pub constraint_by_name: HashMap<String, (String, usize)>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            node_tables: HashMap::new(),
            rel_tables: HashMap::new(),
            sequences: HashMap::new(),
            macros: HashMap::new(),
            constraint_by_name: HashMap::new(),
        }
    }

    pub fn rebuild_constraint_index(&mut self) {
        self.constraint_by_name.clear();
        for (table_name, table) in &self.node_tables {
            for (pos, c) in table.constraints.iter().enumerate() {
                self.constraint_by_name.insert(c.name.clone(), (table_name.clone(), pos));
            }
        }
    }

    fn is_builtin_column(name: &str) -> bool {
        matches!(name, "_id" | "_src" | "_dst")
    }

    pub fn add_column_to_table(
        &mut self,
        table_name: &str,
        col_name: String,
        col_type: LogicalType,
    ) -> Result<(), crate::LightningError> {
        if Self::is_builtin_column(&col_name) {
            return Err(crate::LightningError::Database(format!(
                "Cannot add built-in column '{}'",
                col_name
            )));
        }
        if let Some(node) = self.node_tables.get_mut(table_name) {
            if node.properties.iter().any(|p| p.name == col_name) {
                return Err(crate::LightningError::Database(format!(
                    "Column '{}' already exists in table '{}'",
                    col_name, table_name
                )));
            }
            node.properties.push(PropertyDefinition {
                name: col_name,
                type_: col_type,
            });
            Ok(())
        } else if let Some(rel) = self.rel_tables.get_mut(table_name) {
            if rel.properties.iter().any(|p| p.name == col_name) {
                return Err(crate::LightningError::Database(format!(
                    "Column '{}' already exists in table '{}'",
                    col_name, table_name
                )));
            }
            rel.properties.push(PropertyDefinition {
                name: col_name,
                type_: col_type,
            });
            Ok(())
        } else {
            Err(crate::LightningError::Database(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub fn remove_column_from_table(
        &mut self,
        table_name: &str,
        col_name: &str,
    ) -> Result<PropertyDefinition, crate::LightningError> {
        if Self::is_builtin_column(col_name) {
            return Err(crate::LightningError::Database(format!(
                "Cannot remove built-in column '{}'",
                col_name
            )));
        }
        if let Some(node) = self.node_tables.get_mut(table_name) {
            let idx = node
                .properties
                .iter()
                .position(|p| p.name == col_name)
                .ok_or_else(|| {
                    crate::LightningError::Database(format!(
                        "Column '{}' not found in table '{}'",
                        col_name, table_name
                    ))
                })?;
            Ok(node.properties.remove(idx))
        } else if let Some(rel) = self.rel_tables.get_mut(table_name) {
            let idx = rel
                .properties
                .iter()
                .position(|p| p.name == col_name)
                .ok_or_else(|| {
                    crate::LightningError::Database(format!(
                        "Column '{}' not found in table '{}'",
                        col_name, table_name
                    ))
                })?;
            Ok(rel.properties.remove(idx))
        } else {
            Err(crate::LightningError::Database(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub fn rename_column_in_table(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), crate::LightningError> {
        if Self::is_builtin_column(old_name) || Self::is_builtin_column(new_name) {
            return Err(crate::LightningError::Database(format!(
                "Cannot rename built-in column"
            )));
        }
        if let Some(node) = self.node_tables.get_mut(table_name) {
            if node.properties.iter().any(|p| p.name == new_name) {
                return Err(crate::LightningError::Database(format!(
                    "Column '{}' already exists in table '{}'",
                    new_name, table_name
                )));
            }
            let col = node
                .properties
                .iter_mut()
                .find(|p| p.name == old_name)
                .ok_or_else(|| {
                    crate::LightningError::Database(format!(
                        "Column '{}' not found in table '{}'",
                        old_name, table_name
                    ))
                })?;
            col.name = new_name.to_string();
            Ok(())
        } else if let Some(rel) = self.rel_tables.get_mut(table_name) {
            if rel.properties.iter().any(|p| p.name == new_name) {
                return Err(crate::LightningError::Database(format!(
                    "Column '{}' already exists in table '{}'",
                    new_name, table_name
                )));
            }
            let col = rel
                .properties
                .iter_mut()
                .find(|p| p.name == old_name)
                .ok_or_else(|| {
                    crate::LightningError::Database(format!(
                        "Column '{}' not found in table '{}'",
                        old_name, table_name
                    ))
                })?;
            col.name = new_name.to_string();
            Ok(())
        } else {
            Err(crate::LightningError::Database(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    pub fn rename_table(
        &mut self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), crate::LightningError> {
        if self.node_tables.contains_key(new_name) || self.rel_tables.contains_key(new_name) {
            return Err(crate::LightningError::Database(format!(
                "Table '{}' already exists",
                new_name
            )));
        }
        if self.node_tables.contains_key(old_name) {
            let mut entry = self.node_tables.remove(old_name).unwrap();
            entry.name = new_name.to_string();
            self.node_tables.insert(new_name.to_string(), entry);
            // Update all relationship tables that reference the renamed node table
            for rel_entry in self.rel_tables.values_mut() {
                if rel_entry.from_table == old_name {
                    rel_entry.from_table = new_name.to_string();
                }
                if rel_entry.to_table == old_name {
                    rel_entry.to_table = new_name.to_string();
                }
            }
            Ok(())
        } else if let Some(mut entry) = self.rel_tables.remove(old_name) {
            entry.name = new_name.to_string();
            self.rel_tables.insert(new_name.to_string(), entry);
            Ok(())
        } else {
            Err(crate::LightningError::Database(format!(
                "Table '{}' not found",
                old_name
            )))
        }
    }

    pub fn add_node_table(
        &mut self,
        name: String,
        mut properties: Vec<PropertyDefinition>,
        primary_key: Option<String>,
    ) -> Result<(), crate::LightningError> {
        if self.node_tables.contains_key(&name) || self.rel_tables.contains_key(&name) {
            return Err(crate::LightningError::Database(format!(
                "Table '{}' already exists",
                name
            )));
        }
        properties.insert(
            0,
            PropertyDefinition {
                name: "_id".to_string(),
                type_: LogicalType::Uint64,
            },
        );
        let num_props = properties.len();

        self.node_tables.insert(
            name.clone(),
            NodeTableCatalogEntry {
                name,
                properties,
                num_rows: 0,
                primary_key,
                constraints: Vec::new(),
                stats: TableStats::new(num_props),
            },
        );
        Ok(())
    }

    pub fn add_rel_table(
        &mut self,
        name: String,
        from: String,
        to: String,
        mut properties: Vec<PropertyDefinition>,
    ) -> Result<(), crate::LightningError> {
        if self.node_tables.contains_key(&name) || self.rel_tables.contains_key(&name) {
            return Err(crate::LightningError::Database(format!(
                "Table '{}' already exists",
                name
            )));
        }
        properties.insert(
            0,
            PropertyDefinition {
                name: "_src".to_string(),
                type_: LogicalType::Uint64,
            },
        );
        properties.insert(
            1,
            PropertyDefinition {
                name: "_dst".to_string(),
                type_: LogicalType::Uint64,
            },
        );
        let num_props = properties.len();

        self.rel_tables.insert(
            name.clone(),
            RelTableCatalogEntry {
                name,
                from_table: from,
                to_table: to,
                properties,
                num_rows: 0,
                stats: TableStats::new(num_props),
            },
        );
        Ok(())
    }

    pub fn remove_table(&mut self, name: &str) {
        self.node_tables.remove(name);
        self.rel_tables.remove(name);
    }

    pub fn save_to_disk(&self, path: &std::path::Path) -> crate::Result<()> {
        let shadow_path = path.with_extension("lbug.shadow");
        let buf = serde_json::to_vec_pretty(self)
            .map_err(|e| crate::LightningError::Database(e.to_string()))?;
        std::fs::write(&shadow_path, buf)?;
        std::fs::rename(shadow_path, path)?;
        // Sync the parent directory to ensure the rename is durable
        if let Some(parent) = path.parent() {
            if let Ok(f) = std::fs::File::open(parent) {
                f.sync_all()?;
            }
        }
        Ok(())
    }

    pub fn get_node_table(&self, name: &str) -> Option<&NodeTableCatalogEntry> {
        self.node_tables.get(name)
    }

    pub fn get_node_table_mut(&mut self, name: &str) -> Option<&mut NodeTableCatalogEntry> {
        self.node_tables.get_mut(name)
    }

    pub fn get_rel_table(&self, name: &str) -> Option<&RelTableCatalogEntry> {
        self.rel_tables.get(name)
    }

    pub fn get_rel_table_mut(&mut self, name: &str) -> Option<&mut RelTableCatalogEntry> {
        self.rel_tables.get_mut(name)
    }

    /// Look up a table (node or rel) and return its properties, a type tag,
    /// and the entry version for cache invalidation.
    pub fn get_table_properties(
        &self,
        name: &str,
    ) -> Option<(&[super::PropertyDefinition], u8)> {
        if let Some(t) = self.node_tables.get(name) {
            Some((&t.properties, 0u8))
        } else if let Some(t) = self.rel_tables.get(name) {
            Some((&t.properties, 1u8))
        } else {
            None
        }
    }

    pub fn load_from_disk(path: &std::path::Path) -> crate::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let mut catalog: Self = serde_json::from_reader(reader)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        catalog.rebuild_constraint_index();
        Ok(catalog)
    }

    pub fn add_sequence(&mut self, name: String, start_val: u64, increment: i64) -> Result<(), crate::LightningError> {
        if self.sequences.contains_key(&name) {
            return Err(crate::LightningError::Database(format!(
                "Sequence '{}' already exists",
                name
            )));
        }
        self.sequences.insert(
            name.clone(),
            SequenceCatalogEntry {
                name,
                next_val: start_val,
                increment,
            },
        );
        Ok(())
    }

    pub fn next_val(&mut self, name: &str) -> Option<u64> {
        if let Some(seq) = self.sequences.get_mut(name) {
            let res = seq.next_val;
            if seq.increment >= 0 {
                seq.next_val = seq.next_val.saturating_add(seq.increment as u64);
            } else {
                seq.next_val = seq.next_val.saturating_sub(seq.increment.unsigned_abs());
            }
            Some(res)
        } else {
            None
        }
    }

    pub fn add_macro(
        &mut self,
        name: String,
        params: Vec<String>,
        body: crate::parser::ast::Expression,
    ) -> Result<(), crate::LightningError> {
        if self.macros.contains_key(&name) {
            return Err(crate::LightningError::Database(format!(
                "Macro '{}' already exists",
                name
            )));
        }
        self.macros
            .insert(name.clone(), MacroCatalogEntry { name, params, body });
        Ok(())
    }

    pub fn get_macro(&self, name: &str) -> Option<&MacroCatalogEntry> {
        self.macros.get(name)
    }

    pub fn add_constraint(
        &mut self,
        table_name: &str,
        constraint: NodeConstraint,
    ) -> Result<(), crate::LightningError> {
        let table = self.node_tables.get_mut(table_name).ok_or_else(|| {
            crate::LightningError::Database(format!("Table '{}' not found", table_name))
        })?;
        if table.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(crate::LightningError::Database(format!(
                "Constraint '{}' already exists on table '{}'",
                constraint.name, table_name
            )));
        }
        if table.constraints.iter().any(|c| c.property == constraint.property) {
            return Err(crate::LightningError::Database(format!(
                "A constraint on property '{}' already exists on table '{}'",
                constraint.property, table_name
            )));
        }
        let pos = table.constraints.len();
        table.constraints.push(constraint.clone());
        self.constraint_by_name.insert(constraint.name.clone(), (table_name.to_string(), pos));
        Ok(())
    }

    pub fn remove_constraint(
        &mut self,
        constraint_name: &str,
    ) -> Result<(String, NodeConstraint), crate::LightningError> {
        if let Some((table_name, pos)) = self.constraint_by_name.remove(constraint_name) {
            if let Some(table) = self.node_tables.get_mut(&table_name) {
                if pos < table.constraints.len() {
                    let c = table.constraints.remove(pos);
                    self.rebuild_constraint_index();
                    return Ok((table_name, c));
                }
            }
            self.rebuild_constraint_index();
        }
        Err(crate::LightningError::Database(format!(
            "Constraint '{}' not found",
            constraint_name
        )))
    }

    pub fn get_constraint(
        &self,
        constraint_name: &str,
    ) -> Option<(&str, &NodeConstraint)> {
        for (table_name, table) in self.node_tables.iter() {
            if let Some(c) = table.constraints.iter().find(|c| c.name == constraint_name) {
                return Some((table_name.as_str(), c));
            }
        }
        None
    }
}
