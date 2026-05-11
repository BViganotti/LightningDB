use crate::planner::binder::BoundExpression;
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::{ArrayRef, AsArray};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalUnwind {
    child: Box<dyn PhysicalOperator>,
    expression: BoundExpression,
    alias: String,

    // State for pull-based execution
    current_chunk: Option<DataChunk>,
    current_row_idx: usize,
    current_list: Option<ArrayRef>, // This is the list for the current row
    current_list_idx: usize,
}

impl PhysicalUnwind {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        expression: BoundExpression,
        alias: String,
    ) -> Self {
        Self {
            child,
            expression,
            alias,
            current_chunk: None,
            current_row_idx: 0,
            current_list: None,
            current_list_idx: 0,
        }
    }
}

impl PhysicalOperator for PhysicalUnwind {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        loop {
            if self.current_chunk.is_none() {
                match self.child.get_next(database, tx, params)? {
                    Some(chunk) => {
                        self.current_chunk = Some(chunk);
                        self.current_row_idx = 0;
                        self.current_list = None;
                        self.current_list_idx = 0;
                    }
                    None => return Ok(None),
                }
            }

            let chunk = self.current_chunk.as_ref().unwrap();

            if self.current_row_idx >= chunk.num_rows() {
                self.current_chunk = None;
                continue;
            }

            if self.current_list.is_none() {
                let eval_res = ExpressionEvaluator::evaluate(
                    &self.expression,
                    Some(&chunk.batch),
                    params,
                    chunk.num_rows(),
                    &database.function_registry,
                    database,
                )?;

                // Check if it's a list
                if let Some(lists) = eval_res.as_list_opt::<i32>() {
                    self.current_list = Some(lists.value(self.current_row_idx));
                } else if let Some(lists) = eval_res.as_list_opt::<i64>() {
                    self.current_list = Some(lists.value(self.current_row_idx));
                } else {
                    // Not a list: treat as single-element list containing the value itself
                    // unless it's null (Cypher: UNWIND null produces 0 rows)
                    if eval_res.is_null(self.current_row_idx) {
                        self.current_row_idx += 1;
                        continue;
                    }

                    self.current_list = Some(eval_res.slice(self.current_row_idx, 1));
                }
                self.current_list_idx = 0;
            }

            let list = self.current_list.as_ref().unwrap();
            if self.current_list_idx >= list.len() {
                self.current_row_idx += 1;
                self.current_list = None;
                continue;
            }

            // Buffer as many rows as possible from the current list
            let num_to_emit = list.len() - self.current_list_idx;
            let mut columns = Vec::new();

            // 1. Duplicate input columns
            for i in 0..chunk.batch.num_columns() {
                let col = chunk.batch.column(i);

                // Duplicate row_val num_to_emit times
                let indices =
                    arrow::array::UInt32Array::from(vec![self.current_row_idx as u32; num_to_emit]);
                let repeated = arrow::compute::take(col.as_ref(), &indices, None)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                columns.push(repeated);
            }

            // 2. Append the unwound values
            let unwound_slice = list.slice(self.current_list_idx, num_to_emit);
            columns.push(unwound_slice);

            let mut fields = chunk.batch.schema().fields().to_vec();
            fields.push(Arc::new(arrow::datatypes::Field::new(
                &self.alias,
                list.data_type().clone(),
                true,
            )));
            let schema = Arc::new(arrow::datatypes::Schema::new(fields));

            let new_batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

            self.current_list_idx += num_to_emit;

            return Ok(Some(DataChunk::new(new_batch)));
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            expression: self.expression.clone(),
            alias: self.alias.clone(),
            current_chunk: None,
            current_row_idx: 0,
            current_list: None,
            current_list_idx: 0,
        })
    }
}
