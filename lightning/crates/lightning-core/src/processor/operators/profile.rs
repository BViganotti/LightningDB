use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct PhysicalProfile {
    pub child: Box<dyn PhysicalOperator>,
    pub start_time: Instant,
    pub total_rows: u64,
    pub finished: bool,
    pub explain_analyze: bool,
}

impl PhysicalProfile {
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self {
            child,
            start_time: Instant::now(),
            total_rows: 0,
            finished: false,
            explain_analyze: false,
        }
    }

    pub fn with_explain_analyze(mut self) -> Self {
        self.explain_analyze = true;
        self
    }
}

impl PhysicalOperator for PhysicalProfile {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.finished {
            return Ok(None);
        }

        if self.explain_analyze {
            let start = Instant::now();
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                self.total_rows += chunk.num_rows() as u64;
            }
            let elapsed = start.elapsed();
            self.finished = true;
            let summary = format!(
                "EXPLAIN ANALYZE: total rows: {}, elapsed: {:?}",
                self.total_rows, elapsed
            );
            let schema = Arc::new(Schema::new(vec![Field::new("QUERY PLAN", DataType::Utf8, false)]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![summary.as_str()]))])
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            return Ok(Some(DataChunk { batch }));
        }

        match self.child.get_next(database, tx, params)? {
            Some(chunk) => {
                self.total_rows += chunk.num_rows() as u64;
                Ok(Some(chunk))
            }
            None => {
                self.finished = true;
                let duration = self.start_time.elapsed();
                tracing::info!(
                    "PROFILE: Execution took {:?} and produced {} rows",
                    duration, self.total_rows
                );
                Ok(None)
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            start_time: self.start_time,
            total_rows: self.total_rows,
            finished: self.finished,
            explain_analyze: self.explain_analyze,
        })
    }
}
