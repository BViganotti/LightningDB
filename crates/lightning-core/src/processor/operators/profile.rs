use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use std::collections::HashMap;
use std::time::Instant;

pub struct PhysicalProfile {
    pub child: Box<dyn PhysicalOperator>,
    pub start_time: Instant,
    pub total_rows: u64,
    pub finished: bool,
}

impl PhysicalProfile {
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self {
            child,
            start_time: Instant::now(),
            total_rows: 0,
            finished: false,
        }
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

        match self.child.get_next(database, tx, params)? {
            Some(chunk) => {
                self.total_rows += chunk.num_rows() as u64;
                Ok(Some(chunk))
            }
            None => {
                self.finished = true;
                let duration = self.start_time.elapsed();
                // In a real system, we'd return a special row or update a shared dashboard
                println!(
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
        })
    }
}
