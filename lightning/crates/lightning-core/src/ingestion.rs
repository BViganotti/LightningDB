use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use crate::error::{LightningError, Result};
use crate::processor::Value;
use crate::Database;

pub struct BulkIngestionService {
    db: Arc<Database>,
}

impl BulkIngestionService {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn ingest_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        start_id: u64,
        num_rows: usize,
    ) -> Result<usize> {
        let bm = self.db.buffer_manager.clone();
        let tx = self.db.transaction_manager.begin(false)?;

        let table = {
            let storage = self.db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {table_name} not found")))?
        };

        table.bulk_append_batch(&bm, batch, start_id, &tx)?;
        table.bulk_append_trigram_batch(start_id, batch)?;

        let (fts_opt, vec_opt, index_opt) = {
            let storage = self.db.storage_manager.read();
            (
                storage.fts_indexes.get(table_name).cloned(),
                storage.vector_indexes.get(table_name).cloned(),
                storage.get_index(table_name),
            )
        };

        let pk_idx = self
            .db
            .catalog
            .read()
            .get_node_table(table_name)
            .and_then(|t| t.primary_key.as_ref())
            .and_then(|pk| table.columns.iter().position(|c| c.name == pk.as_str()));

        // Primary key hash index
        if let (Some(index), Some(pk_col_idx)) = (&index_opt, pk_idx) {
            if pk_col_idx < batch.num_columns() {
                let pk_array = batch.column(pk_col_idx);
                if let Some(str_arr) = pk_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    for i in 0..num_rows {
                        if str_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::String(str_arr.value(i).to_string()),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                } else if let Some(int_arr) =
                    pk_array.as_any().downcast_ref::<arrow::array::Int64Array>()
                {
                    for i in 0..num_rows {
                        if int_arr.is_valid(i) {
                            index.insert(
                                &bm,
                                &Value::Number(int_arr.value(i) as f64),
                                start_id + i as u64,
                                &tx,
                            )?;
                        }
                    }
                }
            }
        }

        // FTS index: all string columns as one document per row
        if let Some(fts) = fts_opt {
            let string_cols: Vec<usize> = table
                .columns
                .iter()
                .enumerate()
                .filter(|(col_idx, col)| {
                    *col_idx < batch.num_columns()
                        && col.data_type == lightning_types::LogicalType::String
                })
                .map(|(i, _)| i)
                .collect();

            if !string_cols.is_empty() {
                let mut batch_docs: Vec<(u64, Vec<&str>)> = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    let node_id = start_id + i as u64;
                    let mut fields = Vec::new();
                    for &col_idx in &string_cols {
                        let array = batch.column(col_idx);
                        if let Some(str_arr) =
                            array.as_any().downcast_ref::<arrow::array::StringArray>()
                        {
                            if str_arr.is_valid(i) && !str_arr.value(i).is_empty() {
                                fields.push(str_arr.value(i));
                            }
                        }
                    }
                    if !fields.is_empty() {
                        batch_docs.push((node_id, fields));
                    }
                }
                if !batch_docs.is_empty() {
                    if let Err(e) = fts.insert_multi_field_batch(&batch_docs) {
                        tracing::warn!(
                            "FTS insert_multi_field_batch error for table {}: {}",
                            table_name,
                            e
                        );
                    }
                }
                if let Err(e) = fts.commit() {
                    tracing::warn!("FTS commit error: {}", e);
                }
            }
        }

        // Vector index: FixedSizeList(Float32) columns as embeddings
        if let Some(vec_idx) = vec_opt {
            let idx_dim = vec_idx.dimension();
            for (col_idx, _col) in table.columns.iter().enumerate() {
                if col_idx < batch.num_columns() {
                    let array = batch.column(col_idx);
                    if let Some(list_arr) = array
                        .as_any()
                        .downcast_ref::<arrow::array::FixedSizeListArray>()
                    {
                        let arr_dim = list_arr.value_length() as usize;
                        if arr_dim == idx_dim {
                            if let Some(values) = list_arr
                                .values()
                                .as_any()
                                .downcast_ref::<arrow::array::Float32Array>()
                            {
                                let mut batch_vecs = Vec::with_capacity(num_rows);
                                for i in 0..num_rows {
                                    let start = i * arr_dim;
                                    let end = (i + 1) * arr_dim;
                                    let emb = values.values()[start..end].to_vec();
                                    batch_vecs.push((start_id + i as u64, emb));
                                }
                                if let Err(e) = vec_idx.insert_batch(&batch_vecs, &bm, &tx) {
                                    tracing::warn!(
                                        "Vector index insert_batch error for table {}: {}",
                                        table_name,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        self.db
            .storage_manager
            .read()
            .flush_all_pending(&bm, &tx)?;
        self.db.transaction_manager.commit(&tx, &bm, &self.db)?;

        // Update catalog
        {
            let mut cat = self.db.catalog.write();
            if let Some(entry) = cat.get_node_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            } else if let Some(entry) = cat.get_rel_table_mut(table_name) {
                entry.num_rows += num_rows as u64;
            }
            self.db.catalog.mark_dirty();
        }

        // Sync catalog stats
        {
            let storage = self.db.storage_manager.read();
            let mut cat = self.db.catalog.write();
            for (name, table) in storage.rel_tables.iter() {
                if let Some(entry) = cat.get_rel_table_mut(name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            for (name, table) in storage.node_tables.iter() {
                if let Some(entry) = cat.get_node_table_mut(name) {
                    entry.stats = table.stats.read().clone();
                }
            }
            self.db.catalog.mark_dirty();
        }

        Ok(num_rows)
    }
}
