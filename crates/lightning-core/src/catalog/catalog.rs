use crate::storage::stats::TableStats;
use lightning_types::LogicalType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub name: String,
    pub type_: LogicalType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTableCatalogEntry {
    pub name: String,
    pub properties: Vec<PropertyDefinition>,
    pub num_rows: u64,
    pub primary_key: Option<String>,
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
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            node_tables: HashMap::new(),
            rel_tables: HashMap::new(),
            sequences: HashMap::new(),
            macros: HashMap::new(),
        }
    }

    pub fn add_node_table(
        &mut self,
        name: String,
        mut properties: Vec<PropertyDefinition>,
        primary_key: Option<String>,
    ) {
        properties.insert(
            0,
            PropertyDefinition {
                name: "_id".to_string(),
                type_: LogicalType::Uint64,
            },
        );
        let num_props = properties.len();

        // Preserve existing num_rows and stats if table already exists
        let (num_rows, stats) = if let Some(existing) = self.node_tables.get(&name) {
            (existing.num_rows, existing.stats.clone())
        } else {
            (0, TableStats::new(num_props))
        };

        self.node_tables.insert(
            name.clone(),
            NodeTableCatalogEntry {
                name,
                properties,
                num_rows,
                primary_key,
                stats,
            },
        );
    }

    pub fn add_rel_table(
        &mut self,
        name: String,
        from: String,
        to: String,
        mut properties: Vec<PropertyDefinition>,
    ) {
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

        // Preserve existing num_rows and stats if table already exists
        let (num_rows, stats) = if let Some(existing) = self.rel_tables.get(&name) {
            (existing.num_rows, existing.stats.clone())
        } else {
            (0, TableStats::new(num_props))
        };

        self.rel_tables.insert(
            name.clone(),
            RelTableCatalogEntry {
                name,
                from_table: from,
                to_table: to,
                properties,
                num_rows,
                stats,
            },
        );
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

    pub fn load_from_disk(path: &std::path::Path) -> crate::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let catalog = serde_json::from_reader(reader)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(catalog)
    }

    pub fn add_sequence(&mut self, name: String, start_val: u64, increment: i64) {
        self.sequences.insert(
            name.clone(),
            SequenceCatalogEntry {
                name,
                next_val: start_val,
                increment,
            },
        );
    }

    pub fn next_val(&mut self, name: &str) -> Option<u64> {
        if let Some(seq) = self.sequences.get_mut(name) {
            let res = seq.next_val;
            seq.next_val += seq.increment as u64;
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
    ) {
        self.macros
            .insert(name.clone(), MacroCatalogEntry { name, params, body });
    }

    pub fn get_macro(&self, name: &str) -> Option<&MacroCatalogEntry> {
        self.macros.get(name)
    }
}
