use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use std::collections::VecDeque;

pub struct PhysicalAccumulate {
    child: Box<dyn PhysicalOperator>,
    buffer: Option<VecDeque<DataChunk>>,
}

impl PhysicalAccumulate {
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self {
            child,
            buffer: None,
        }
    }
}

impl PhysicalOperator for PhysicalAccumulate {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(ref mut buffer) = self.buffer {
            return Ok(buffer.pop_front());
        }

        let mut buffer = VecDeque::new();
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            buffer.push_back(chunk);
        }
        let result = buffer.pop_front();
        self.buffer = Some(buffer);
        Ok(result)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            buffer: None,
        })
    }

    fn is_single_row(&self) -> bool {
        false
    }
}
