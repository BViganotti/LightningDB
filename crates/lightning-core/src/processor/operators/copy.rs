use crate::parser::ast::Literal;
use crate::processor::arrow_utils::logical_type_to_arrow_type;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::storage_manager::Table;
use crate::Database;
use crate::Result;
use arrow::array::{ArrayRef, Float64Array, UInt64Array};
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

pub struct PhysicalCopy {
    table_name: String,
    file_path: String,
    options: HashMap<String, Literal>,
    is_from: bool,
    db: Arc<Database>,
    executed: bool,
}

impl PhysicalCopy {
    pub fn new_from(
        table_name: String,
        file_path: String,
        options: HashMap<String, Literal>,
        db: Arc<Database>,
    ) -> Self {
        Self {
            table_name,
            file_path,
            options,
            is_from: true,
            db,
            executed: false,
        }
    }

    pub fn new_to(
        table_name: String,
        file_path: String,
        options: HashMap<String, Literal>,
        db: Arc<Database>,
    ) -> Self {
        Self {
            table_name,
            file_path,
            options,
            is_from: false,
            db,
            executed: false,
        }
    }
}

impl PhysicalOperator for PhysicalCopy {
    fn get_next(
        &mut self,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        let table = database
            .storage_manager
            .read()
            .get_table(&self.table_name)
            .ok_or_else(|| {
                crate::LightningError::Internal(format!("Table {} not found", self.table_name))
            })?
            .clone();

        let affected = if self.is_from {
            self.execute_copy_from(&table, database, tx)?
        } else {
            self.execute_copy_to(&table, database)?
        };

        // Return a single row with the number of affected rows
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Float64,
            true,
        )]));
        let count_array =
            Arc::new(Float64Array::from(vec![affected as f64])) as arrow::array::ArrayRef;
        Ok(Some(DataChunk {
            batch: RecordBatch::try_new(output_schema, vec![count_array]).unwrap(),
        }))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            table_name: self.table_name.clone(),
            file_path: self.file_path.clone(),
            options: self.options.clone(),
            is_from: self.is_from,
            db: self.db.clone(),
            executed: self.executed,
        })
    }
}

impl PhysicalCopy {
    fn execute_copy_from(
        &self,
        table: &Table,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<u64> {
        let file = File::open(&self.file_path)?;
        let delimiter = self
            .options
            .get("DELIM")
            .and_then(|l| {
                if let Literal::String(s) = l {
                    Some(s.as_bytes()[0])
                } else {
                    None
                }
            })
            .unwrap_or(b',');
        let has_header = self
            .options
            .get("HEADER")
            .and_then(|l| {
                if let Literal::Boolean(b) = l {
                    Some(*b)
                } else {
                    None
                }
            })
            .unwrap_or(true);

        // Ladybug tables usually have an internal ID as the first column, which isn't in the CSV
        // We need to check the schema
        let mut fields = Vec::new();
        let mut start_col = 0;
        if table.columns.len() > 0 && table.columns[0].name == "INTERNAL_ID" {
            start_col = 1;
        }

        for i in start_col..table.columns.len() {
            let col = &table.columns[i];
            fields.push(Field::new(
                &col.name,
                logical_type_to_arrow_type(&col.data_type),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));

        let csv = csv::ReaderBuilder::new(schema.clone())
            .with_header(has_header)
            .with_delimiter(delimiter)
            .build(file)?;

        let mut next_id = {
            let cat = database.catalog.read();
            cat.get_node_table(&self.table_name)
                .map(|t| t.num_rows)
                .unwrap_or(0)
        };
        let mut total_added = 0;

        for batch in csv {
            let batch = batch?;
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            if start_col == 1 {
                let ids: UInt64Array = (next_id..next_id + num_rows as u64).map(Some).collect();
                let mut columns = vec![Arc::new(ids) as ArrayRef];
                columns.extend(batch.columns().iter().cloned());
                let new_batch = RecordBatch::try_new(table.get_schema(), columns)?;
                table.bulk_append_batch(&database.buffer_manager, &new_batch, next_id, tx)?;
            } else {
                table.bulk_append_batch(&database.buffer_manager, &batch, next_id, tx)?;
            }

            next_id += num_rows as u64;
            total_added += num_rows as u64;
        }

        // Auto-build CSR indices after bulk insert
        database.storage_manager.read().rebuild_csr(
            &self.table_name,
            &database.buffer_manager,
            tx,
        )?;

        // Update cardinality in catalog
        {
            let mut cat = database.catalog.write();
            if let Some(t) = cat.get_node_table_mut(&self.table_name) {
                t.num_rows += total_added;
            } else if let Some(t) = cat.get_rel_table_mut(&self.table_name) {
                t.num_rows += total_added;
            }
        }

        Ok(total_added)
    }

    fn execute_copy_to(&self, _table: &Table, _database: &Database) -> Result<u64> {
        // TODO: Implement COPY TO
        Err(crate::LightningError::Internal(
            "COPY TO not yet implemented".into(),
        ))
    }
}
