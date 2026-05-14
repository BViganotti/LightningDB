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
use std::io::Write;
use std::sync::Arc;

pub struct PhysicalCopy {
    table_name: String,
    file_path: String,
    options: HashMap<String, Literal>,
    is_from: bool,
    db: Arc<Database>,
    executed: bool,
    scanned_columns: Vec<String>,
    scan_position: u64,
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
            scanned_columns: Vec::new(),
            scan_position: 0,
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
            scanned_columns: Vec::new(),
            scan_position: 0,
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
            self.execute_copy_to(&table, database, tx)?
        };

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Float64,
            true,
        )]));
        let count_array =
            Arc::new(Float64Array::from(vec![affected as f64])) as arrow::array::ArrayRef;
        Ok(Some(DataChunk {
            batch: RecordBatch::try_new(output_schema, vec![count_array])            .expect("copy output schema must match count array"),
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
            scanned_columns: self.scanned_columns.clone(),
            scan_position: self.scan_position,
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

        database.storage_manager.read().rebuild_csr(
            &self.table_name,
            &database.buffer_manager,
            tx,
        )?;

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

    fn execute_copy_to(
        &self,
        table: &Table,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<u64> {
        let bm = &database.buffer_manager;
        let num_rows = {
            let cat = database.catalog.read();
            cat.get_node_table(&self.table_name)
                .map(|t| t.num_rows)
                .or_else(|| {
                    cat.get_rel_table(&self.table_name).map(|t| t.num_rows)
                })
                .unwrap_or(0)
        };

        if num_rows == 0 {
            return Ok(0);
        }

        let format = self
            .options
            .get("FORMAT")
            .and_then(|l| {
                if let Literal::String(s) = l {
                    Some(s.to_uppercase())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "CSV".to_string());

        let mut fields = Vec::new();
        let mut col_idxs = Vec::new();
        for (i, col) in table.columns.iter().enumerate() {
            if col.name == "INTERNAL_ID" {
                continue;
            }
            fields.push(Field::new(
                &col.name,
                logical_type_to_arrow_type(&col.data_type),
                true,
            ));
            col_idxs.push(i);
        }
        let schema = Arc::new(Schema::new(fields));
        let morsel = 8192u64;
        let mut position = 0u64;
        let mut total_written = 0u64;

        match format.as_str() {
            "JSON" => {
                let mut file = File::create(&self.file_path)?;
                file.write_all(b"[")?;
                let mut first_row = true;
                while position < num_rows {
                    let to_read = std::cmp::min(morsel, num_rows - position);
                    let batch = self.read_batch(table, &col_idxs, &schema, position, to_read, bm, tx)?;
                    for row_idx in 0..batch.num_rows() {
                        if !first_row {
                            file.write_all(b",\n")?;
                        }
                        first_row = false;
                        file.write_all(b"{")?;
                        let mut first_col = true;
                        for (j, col_name) in schema.fields().iter().enumerate() {
                            if !first_col {
                                file.write_all(b", ")?;
                            }
                            first_col = false;
                            let col = batch.column(j);
                            if col.is_null(row_idx) {
                                write!(file, "\"{}\": null", col_name.name())?;
                            } else if let Some(s) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                                let escaped = s.value(row_idx).replace('\\', "\\\\").replace('"', "\\\"");
                                write!(file, "\"{}\": \"{}\"", col_name.name(), escaped)?;
                            } else if let Some(f) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                                write!(file, "\"{}\": {}", col_name.name(), f.value(row_idx))?;
                            } else if let Some(i) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                                write!(file, "\"{}\": {}", col_name.name(), i.value(row_idx))?;
                            } else if let Some(b) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                                write!(file, "\"{}\": {}", col_name.name(), b.value(row_idx))?;
                            } else {
                                let val = Value::from_arrow(col, row_idx);
                                write!(file, "\"{}\": \"{}\"", col_name.name(), val)?;
                            }
                        }
                        file.write_all(b"}")?;
                        total_written += 1;
                    }
                    position += to_read;
                }
                file.write_all(b"\n]")?;
            }
            _ => {
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

                let file = File::create(&self.file_path)?;
                let mut writer = csv::WriterBuilder::new()
                    .with_header(has_header)
                    .with_delimiter(delimiter)
                    .build(file);

                while position < num_rows {
                    let to_read = std::cmp::min(morsel, num_rows - position);
                    let batch = self.read_batch(table, &col_idxs, &schema, position, to_read, bm, tx)?;
                    writer
                        .write(&batch)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    total_written += to_read;
                    position += to_read;
                }

                let _ = writer.into_inner();
            }
        }

        Ok(total_written)
    }

    fn read_batch(
        &self,
        table: &Table,
        col_idxs: &[usize],
        schema: &Schema,
        offset: u64,
        num_rows: u64,
        bm: &crate::storage::buffer_manager::BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(col_idxs.len());
        for &col_idx in col_idxs {
            let col = &table.columns[col_idx];
            let array = col.scan_to_array(bm, offset, num_rows, tx)?;
            columns.push(array);
        }
        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .map_err(|e| crate::LightningError::Internal(e.to_string()))
    }
}
