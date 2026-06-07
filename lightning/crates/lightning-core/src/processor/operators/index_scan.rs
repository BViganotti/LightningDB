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
            &*database.function_registry.read(),
            database,
        )?;
        let pk_value = Value::from_arrow(&val_array, 0);
        if let Some(row_id) = self.index.lookup(&self.buffer_manager, &pk_value, tx)? {
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
