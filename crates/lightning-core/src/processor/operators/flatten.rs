use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

const FLATTEN_BATCH_SIZE: usize = 1024;

pub struct PhysicalFlatten {
    child: Box<dyn PhysicalOperator>,
    current_batch: Option<RecordBatch>,
    cursor: usize,
}

impl PhysicalFlatten {
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self {
            child,
            current_batch: None,
            cursor: 0,
        }
    }
}

impl PhysicalOperator for PhysicalFlatten {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        loop {
            if let Some(batch) = &self.current_batch {
                let num_rows = batch.num_rows();
                if self.cursor < num_rows {
                    let start = self.cursor;
                    let end = (start + FLATTEN_BATCH_SIZE).min(num_rows);
                    let count = end - start;
                    self.cursor = end;

                    let mut columns = Vec::with_capacity(batch.num_columns());
                    for col_idx in 0..batch.num_columns() {
                        let col = batch.column(col_idx);
                        columns.push(col.slice(start, count));
                    }

                    let result_batch =
                        RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
                            crate::LightningError::Internal(format!("Arrow error: {e}"))
                        })?;

                    return Ok(Some(DataChunk {
                        batch: result_batch,
                    }));
                } else {
                    self.current_batch = None;
                    self.cursor = 0;
                }
            }

            match self.child.get_next(database, tx, params)? {
                Some(chunk) => {
                    self.current_batch = Some(chunk.batch);
                    self.cursor = 0;
                }
                None => return Ok(None),
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            current_batch: self.current_batch.clone(),
            cursor: self.cursor,
        })
    }
}
