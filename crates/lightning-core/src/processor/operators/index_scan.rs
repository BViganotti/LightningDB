use crate::planner::binder::BoundExpression;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::index::hash_index::HashIndex;
use crate::storage::storage_manager::Table;
use crate::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct PhysicalIndexScan {
    _table_name: String,
    table: Table,
    index: Arc<HashIndex>,
    pk_value_expr: BoundExpression,
    pub buffer_manager: Arc<BufferManager>,
    pub projected_idxs: Option<Vec<usize>>,
    pub read_ts: u64,
    done: bool,
}

impl PhysicalIndexScan {
    pub fn new(
        table_name: String,
        table: Table,
        index: Arc<HashIndex>,
        pk_value_expr: BoundExpression,
        buffer_manager: Arc<BufferManager>,
        read_ts: u64,
    ) -> Self {
        Self {
            _table_name: table_name,
            table,
            index,
            pk_value_expr,
            buffer_manager,
            projected_idxs: None,
            read_ts,
            done: false,
        }
    }
    pub fn with_projected_idxs(mut self, idxs: Vec<usize>) -> Self {
        self.projected_idxs = Some(idxs);
        self
    }
}

impl PhysicalOperator for PhysicalIndexScan {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let val_array = ExpressionEvaluator::evaluate(
            &self.pk_value_expr,
            None,
            params,
            1,
            &database.function_registry,
            database,
        )?;
        let pk_value = Value::from_arrow(&val_array, 0);
        let pk_col_idx = database
            .catalog
            .read()
            .get_node_table(&self.table.name)
            .and_then(|t| t.primary_key.clone())
            .and_then(|pk| self.table.columns.iter().position(|c| c.name == pk));
        let mut lookup_row_id = self.index.lookup(&self.buffer_manager, &pk_value, tx)?;
        // Validate the index entry. Under concurrent writes the PK hash index
        // (which has no per-row merge-on-commit) can transiently resolve a key
        // to the wrong row id; acting on it would delete/update the wrong row.
        // If the resolved row's primary key does not match the key we looked up,
        // fall back to a linear scan for the correct, visible row.
        if let (Some(rid), Some(ci)) = (lookup_row_id, pk_col_idx) {
            let actual = self.table.columns[ci]
                .get_value(&self.buffer_manager, rid, tx)
                .unwrap_or(Value::Null);
            if actual != pk_value {
                let n = self.table.next_row_id.load(Ordering::Acquire);
                let mut found = None;
                for cand in 0..n {
                    if !self.table.columns[0]
                        .version_info
                        .is_visible(cand, tx.tx_id, tx.read_ts)
                    {
                        continue;
                    }
                    let v = self.table.columns[ci]
                        .get_value(&self.buffer_manager, cand, tx)
                        .unwrap_or(Value::Null);
                    if v == pk_value {
                        found = Some(cand);
                        break;
                    }
                }
                lookup_row_id = found;
            }
        }
        if let Some(row_id) = lookup_row_id {
            // Check MVCC visibility for this row using the transaction's read_ts
            if !self.table.columns[0]
                .version_info
                .is_visible(row_id, tx.tx_id, tx.read_ts)
            {
                return Ok(None);
            }
            let mut columns = Vec::new();
            let mut fields = Vec::new();
            if let Some(idxs) = &self.projected_idxs {
                for &idx in idxs {
                    let col = &self.table.columns[idx];
                    let val = col.get_value(&self.buffer_manager, row_id, tx)?;
                    let target_type =
                        crate::processor::arrow_utils::logical_type_to_arrow_type(&col.data_type);
                    columns.push(crate::processor::arrow_utils::values_to_array(
                        &[val],
                        &target_type,
                    ));
                    fields.push(col.to_field());
                }
            } else {
                for col in &self.table.columns {
                    let val = col.get_value(&self.buffer_manager, row_id, tx)?;
                    let target_type =
                        crate::processor::arrow_utils::logical_type_to_arrow_type(&col.data_type);
                    columns.push(crate::processor::arrow_utils::values_to_array(
                        &[val],
                        &target_type,
                    ));
                    fields.push(col.to_field());
                }
            }
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            Ok(Some(DataChunk { batch }))
        } else {
            Ok(None)
        }
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(self.clone())
    }
}
