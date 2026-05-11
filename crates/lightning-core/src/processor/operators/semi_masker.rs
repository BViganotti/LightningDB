use super::semi_mask::SemiMask;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::Array;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalSemiMasker {
    child: Box<dyn PhysicalOperator>,
    column_idx: usize,
    mask: Arc<RwLock<SemiMask>>,
}

impl PhysicalSemiMasker {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        column_idx: usize,
        mask: Arc<RwLock<SemiMask>>,
    ) -> Self {
        Self {
            child,
            column_idx,
            mask,
        }
    }
}

impl PhysicalOperator for PhysicalSemiMasker {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        let chunk_opt = self.child.get_next(database, tx, params)?;

        if let Some(chunk) = &chunk_opt {
            let batch = &chunk.batch;
            let column = batch.column(self.column_idx);

            if let Some(offsets) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                let mut mask = self.mask.write();
                let initial_len = mask.len();
                for i in 0..offsets.len() {
                    if !Array::is_null(offsets, i) {
                        mask.insert(offsets.value(i));
                    }
                }
            } else {
                return Err(crate::LightningError::Internal(format!(
                    "SemiMasker expects UInt64 column, found {:?}",
                    column.data_type()
                )));
            }
        }

        Ok(chunk_opt)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            column_idx: self.column_idx,
            mask: self.mask.clone(),
        })
    }
}
