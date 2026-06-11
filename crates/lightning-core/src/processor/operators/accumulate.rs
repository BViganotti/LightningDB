use crate::processor::{DataChunk, PhysicalOperator};
use crate::Result;
use std::collections::VecDeque;

const MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

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
        let mut buffered_bytes = 0usize;
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            let chunk_bytes = chunk.batch.get_array_memory_size();
            if buffered_bytes + chunk_bytes > MAX_BUFFER_BYTES && !buffer.is_empty() {
                buffer.push_front(chunk);
                break;
            }
            buffered_bytes += chunk_bytes;
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
