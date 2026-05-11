use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct SharedLimit {
    pub count: AtomicUsize,
    pub limit: usize,
}

pub struct PhysicalLimit {
    child: Box<dyn PhysicalOperator>,
    shared: Arc<SharedLimit>,
}

impl PhysicalLimit {
    pub fn new(child: Box<dyn PhysicalOperator>, limit: usize) -> Self {
        Self {
            child,
            shared: Arc::new(SharedLimit {
                count: AtomicUsize::new(0),
                limit,
            }),
        }
    }
}

impl PhysicalOperator for PhysicalLimit {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        loop {
            let current = self.shared.count.load(Ordering::SeqCst);
            if current >= self.shared.limit {
                return Ok(None);
            }

            match self.child.get_next(database, tx, params)? {
                Some(chunk) => {
                    let batch = &chunk.batch;
                    let num_rows = batch.num_rows();

                    let old_count = self.shared.count.fetch_add(num_rows, Ordering::SeqCst);

                    if old_count >= self.shared.limit {
                        return Ok(None);
                    }

                    if old_count + num_rows <= self.shared.limit {
                        return Ok(Some(chunk));
                    } else {
                        let take = self.shared.limit - old_count;
                        let sliced_batch = batch.slice(0, take);
                        return Ok(Some(DataChunk {
                            batch: sliced_batch,
                        }));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            shared: self.shared.clone(),
        })
    }
}

pub struct SharedSkip {
    pub count: AtomicUsize,
    pub skip: usize,
}

pub struct PhysicalSkip {
    child: Box<dyn PhysicalOperator>,
    shared: Arc<SharedSkip>,
}

impl PhysicalSkip {
    pub fn new(child: Box<dyn PhysicalOperator>, skip: usize) -> Self {
        Self {
            child,
            shared: Arc::new(SharedSkip {
                count: AtomicUsize::new(0),
                skip,
            }),
        }
    }
}

impl PhysicalOperator for PhysicalSkip {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            let batch = &chunk.batch;
            let num_rows = batch.num_rows();

            let old_count = self.shared.count.fetch_add(num_rows, Ordering::SeqCst);

            if old_count + num_rows <= self.shared.skip {
                continue;
            }

            let start = self.shared.skip.saturating_sub(old_count);
            if start < num_rows {
                let len = num_rows - start;
                let sliced_batch = batch.slice(start, len);
                return Ok(Some(DataChunk {
                    batch: sliced_batch,
                }));
            }
        }

        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            shared: self.shared.clone(),
        })
    }
}
