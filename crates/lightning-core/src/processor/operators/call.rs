use crate::planner::binder::BoundCallClause as BoundCall;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalCall {
    call: BoundCall,
    executed: bool,
}

impl PhysicalCall {
    pub fn new(call: BoundCall) -> Self {
        Self {
            call,
            executed: false,
        }
    }

    fn proc_show_tables(database: &crate::Database) -> Result<DataChunk> {
        let cat = database.catalog.read();
        let node_names: Vec<&str> = cat.node_tables.keys().map(|s| s.as_str()).collect();
        let rel_names: Vec<&str> = cat.rel_tables.keys().map(|s| s.as_str()).collect();
        let mut all_names = Vec::with_capacity(node_names.len() + rel_names.len());
        for name in &node_names {
            all_names.push(*name);
        }
        for name in &rel_names {
            all_names.push(*name);
        }
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(all_names))],
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(DataChunk::new(batch))
    }

    fn proc_db_labels(database: &crate::Database) -> Result<DataChunk> {
        let cat = database.catalog.read();
        let names: Vec<&str> = cat.node_tables.keys().map(|s| s.as_str()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(names))],
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(DataChunk::new(batch))
    }

    fn proc_db_relationship_types(database: &crate::Database) -> Result<DataChunk> {
        let cat = database.catalog.read();
        let names: Vec<&str> = cat.rel_tables.keys().map(|s| s.as_str()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "relationshipType",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(names))],
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(DataChunk::new(batch))
    }

    fn proc_db_schema(database: &crate::Database) -> Result<DataChunk> {
        let cat = database.catalog.read();
        let mut names = Vec::new();
        let mut types = Vec::new();
        let mut row_counts = Vec::new();

        for (name, entry) in cat.node_tables.iter() {
            names.push(name.clone());
            types.push("NODE".to_string());
            row_counts.push(entry.num_rows);
        }
        for (name, entry) in cat.rel_tables.iter() {
            names.push(name.clone());
            types.push("REL".to_string());
            row_counts.push(entry.num_rows);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("row_count", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(types)),
                Arc::new(UInt64Array::from(row_counts)),
            ],
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
        Ok(DataChunk::new(batch))
    }
}

impl PhysicalOperator for PhysicalCall {
    fn get_next(
        &mut self,
        database: &crate::Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let proc_name = self.call.procedure_name.to_lowercase();
        match proc_name.as_str() {
            "show_tables" => Ok(Some(Self::proc_show_tables(database)?)),
            "db.labels" | "db_labels" => Ok(Some(Self::proc_db_labels(database)?)),
            "db.relationshiptypes" | "db_relationshiptypes" | "db.relationshiptype" => {
                Ok(Some(Self::proc_db_relationship_types(database)?))
            }
            "db.schema" | "db_schema" => Ok(Some(Self::proc_db_schema(database)?)),
            _ => Err(crate::LightningError::Internal(format!(
                "Procedure {} not found",
                self.call.procedure_name
            ))),
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            call: self.call.clone(),
            executed: self.executed,
        })
    }
}
