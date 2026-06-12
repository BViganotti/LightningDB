use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::storage_manager::Table;
use crate::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalPathPropertyProbe {
    child: Box<dyn PhysicalOperator>,
    node_table: Table,
    rel_table: Table,
    bm: Arc<BufferManager>,
    path_var_idx: usize, // Column containing the path (list of IDs)
    read_ts: u64,
}

impl PhysicalPathPropertyProbe {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        node_table: Table,
        rel_table: Table,
        bm: Arc<BufferManager>,
        path_var_idx: usize,
        read_ts: u64,
    ) -> Self {
        Self {
            child,
            node_table,
            rel_table,
            bm,
            path_var_idx,
            read_ts,
        }
    }
}

impl PhysicalOperator for PhysicalPathPropertyProbe {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.child.get_next(database, tx, params)? {
            let num_rows = chunk.num_rows();
            let mut path_properties = Vec::new();

            for i in 0..num_rows {
                let path_val = Value::from_arrow(chunk.batch.column(self.path_var_idx), i);
                if let Value::List(ids) = path_val {
                    let mut props = Vec::new();
                    for id_val in ids {
                        if let Value::Node(id) = id_val {
                            let mut row_vals = Vec::new();
                            for col in &self.node_table.columns {
                                let mut v = Vec::new();
                                col.scan(&self.bm, id, 1, tx, &mut v)?;
                                row_vals.push(v.remove(0));
                            }
                            props.push(Value::List(row_vals));
                        }
                    }
                    path_properties.push(Value::List(props));
                } else {
                    path_properties.push(Value::Null);
                }
            }

            let mut columns = chunk.batch.columns().to_vec();
            let json_props: Vec<Value> = path_properties
                .into_iter()
                .map(|p| {
                    let json_val = p.to_json();
                    Value::String(serde_json::to_string(&json_val).unwrap_or_default())
                })
                .collect();
            columns.push(crate::processor::arrow_utils::values_to_array(
                &json_props,
                &DataType::Utf8,
            ));

            let mut fields = chunk.batch.schema().fields().to_vec();
            fields.push(Field::new("path_properties", DataType::Utf8, true).into());

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            return Ok(Some(DataChunk { batch }));
        }
        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            node_table: self.node_table.clone(),
            rel_table: self.rel_table.clone(),
            bm: self.bm.clone(),
            path_var_idx: self.path_var_idx,
            read_ts: self.read_ts,
        })
    }
}
