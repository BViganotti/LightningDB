use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use std::collections::HashMap;

pub struct PhysicalMultiplicityReducer {
    pub child: Box<dyn PhysicalOperator>,
    pub multiplicity: u64,
}

impl PhysicalMultiplicityReducer {
    pub fn new(child: Box<dyn PhysicalOperator>, multiplicity: u64) -> Self {
        Self {
            child,
            multiplicity,
        }
    }
}

impl PhysicalOperator for PhysicalMultiplicityReducer {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        // In the current implementation, we pass through the data.
        // If we needed to physically multiply the rows, we would clone them here.
        // However, Cypher multiplicity is often handled by adjusted aggregation results.
        self.child.get_next(database, tx, params)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            multiplicity: self.multiplicity,
        })
    }
}
