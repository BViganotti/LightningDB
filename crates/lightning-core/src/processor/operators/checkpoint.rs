use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Database;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalCheckpoint {
    db: Arc<Database>,
    executed: bool,
}

impl PhysicalCheckpoint {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            executed: false,
        }
    }
}

impl PhysicalOperator for PhysicalCheckpoint {
    fn get_next(
        &mut self,
        _database: &Database,
        _tx: &crate::transaction::transaction_manager::Transaction,
        _params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        self.db.checkpoint()?;
        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            db: self.db.clone(),
            executed: self.executed,
        })
    }
}
