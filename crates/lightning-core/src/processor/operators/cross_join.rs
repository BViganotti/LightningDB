use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::datatypes::{FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

pub struct SharedCrossJoinBuild {
    pub right_chunks: Vec<DataChunk>,
    pub build_done: bool,
    pub right_schema: Option<Arc<Schema>>,
    pub num_active_builders: usize,
}

pub struct PhysicalCrossJoin {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,

    // Shared state
    shared_build: Arc<RwLock<SharedCrossJoinBuild>>,
    build_cv: Arc<Condvar>,
    build_mutex: Arc<Mutex<bool>>,

    // Thread-local state
    current_left_chunk: Option<DataChunk>,
    left_row_idx: usize,
    right_chunk_idx: usize,
    right_row_idx: usize,
}

impl PhysicalCrossJoin {
    pub fn new(left: Box<dyn PhysicalOperator>, right: Box<dyn PhysicalOperator>) -> Self {
        Self {
            left,
            right,
            shared_build: Arc::new(RwLock::new(SharedCrossJoinBuild {
                right_chunks: Vec::new(),
                build_done: false,
                right_schema: None,
                num_active_builders: 0,
            })),
            build_cv: Arc::new(Condvar::new()),
            build_mutex: Arc::new(Mutex::new(false)),
            current_left_chunk: None,
            left_row_idx: 0,
            right_chunk_idx: 0,
            right_row_idx: 0,
        }
    }

    fn build(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        {
            let mut shared = self.shared_build.write();
            if shared.build_done {
                return Ok(());
            }
            shared.num_active_builders += 1;
        }

        while let Some(chunk) = self.right.get_next(database, tx, params)? {
            let mut shared = self.shared_build.write();
            if shared.build_done {
                break;
            }
            if shared.right_schema.is_none() {
                shared.right_schema = Some(chunk.batch.schema());
            }
            shared.right_chunks.push(chunk);
        }

        let mut shared = self.shared_build.write();
        shared.num_active_builders -= 1;
        if shared.num_active_builders == 0 {
            if let Some(schema) = shared.right_schema.clone() {
                let batches: Vec<RecordBatch> = shared.right_chunks.iter().map(|c| c.batch.clone()).collect();
                if batches.len() > 1 {
                    match arrow::compute::concat_batches(&schema, &batches) {
                        Ok(merged) => {
                            shared.right_chunks = vec![DataChunk { batch: merged }];
                        }
                        Err(e) => {
                            tracing::warn!("cross_join concat_batches failed: {e} — using individual chunks");
                        }
                    }
                }
            }
            shared.build_done = true;
            let mut done = self.build_mutex.lock();
            *done = true;
            self.build_cv.notify_all();
        }
        Ok(())
    }

    fn wait_for_build(&self) {
        let mut done = self.build_mutex.lock();
        while !*done {
            self.build_cv.wait(&mut done);
        }
    }
}

impl PhysicalOperator for PhysicalCrossJoin {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        {
            let shared = self.shared_build.read();
            if !shared.build_done {
                drop(shared);
                self.build(database, tx, params)?;
            }
        }

        self.wait_for_build();
        let shared = self.shared_build.read();

        loop {
            if self.current_left_chunk.is_none() {
                self.current_left_chunk = self.left.get_next(database, tx, params)?;
                self.left_row_idx = 0;
                self.right_chunk_idx = 0;
                self.right_row_idx = 0;
                if self.current_left_chunk.is_none() {
                    return Ok(None);
                }
            }

            let left_batch = self.current_left_chunk.as_ref()
                .expect("current_left_chunk should be Some when processing")
                .batch.clone();
            let left_num_cols = left_batch.num_columns();
            let left_num_rows = left_batch.num_rows();
            let left_schema = left_batch.schema();

            let right_column_count = shared
                .right_schema
                .as_ref()
                .map(|s| s.fields().len())
                .unwrap_or(0);

            if shared.right_chunks.is_empty() {
                self.current_left_chunk = None;
                continue;
            }

            let mut left_indices = Vec::<u64>::with_capacity(1024);
            let mut right_indices = Vec::<u64>::with_capacity(1024);

            // Track the right chunk index that the current batch of indices
            // references. Each batch draws from exactly ONE right chunk so
            // the output path can use the correct chunk.
            let mut batch_chunk_idx: Option<usize> = None;

            while self.left_row_idx < left_num_rows
                && left_indices.len() < 1024
                && self.right_chunk_idx < shared.right_chunks.len()
            {
                let right_chunk_idx = self.right_chunk_idx;
                let right_batch = &shared.right_chunks[right_chunk_idx].batch;
                let right_num_rows = right_batch.num_rows();

                // If we already have rows from a different chunk, break so
                // each output batch references only one right chunk.
                if let Some(prev_chunk) = batch_chunk_idx {
                    if prev_chunk != right_chunk_idx {
                        break;
                    }
                } else {
                    batch_chunk_idx = Some(right_chunk_idx);
                }

                for r_row in self.right_row_idx..right_num_rows {
                    if left_indices.len() >= 1024 {
                        break;
                    }
                    left_indices.push(self.left_row_idx as u64);
                    right_indices.push(r_row as u64);
                }
                self.right_row_idx = 0;
                self.right_chunk_idx += 1;

                if self.right_chunk_idx >= shared.right_chunks.len() {
                    self.right_chunk_idx = 0;
                    self.left_row_idx += 1;
                }
            }

            if self.left_row_idx >= left_num_rows {
                self.current_left_chunk = None;
            }

            if !left_indices.is_empty() {
                let left_idx_arr = arrow::array::UInt64Array::from(left_indices);
                let right_idx_arr = arrow::array::UInt64Array::from(right_indices);
                let mut final_columns = Vec::new();
                let mut final_fields: Vec<FieldRef> = Vec::new();

                for i in 0..left_num_cols {
                    let field = left_schema.field(i);
                    final_fields.push(Arc::new((*field).clone()));
                    final_columns.push(arrow::compute::take(
                        left_batch.column(i),
                        &left_idx_arr,
                        None,
                    ).map_err(|e| crate::LightningError::Internal(e.to_string()))?);
                }

                if let Some(right_schema) = &shared.right_schema {
                    let ci = batch_chunk_idx.unwrap_or(0);
                    let right_batch = &shared.right_chunks[ci].batch;
                    for i in 0..right_column_count {
                        let field = right_schema.field(i);
                        final_fields.push(Arc::new((*field).clone()));
                        final_columns.push(arrow::compute::take(
                            right_batch.column(i),
                            &right_idx_arr,
                            None,
                        ).map_err(|e| crate::LightningError::Internal(e.to_string()))?);
                    }
                }

                let schema = Arc::new(Schema::new(final_fields));
                let batch = RecordBatch::try_new(schema, final_columns)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                return Ok(Some(DataChunk { batch }));
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            left: self.left.clone_box(),
            right: self.right.clone_box(),
            shared_build: self.shared_build.clone(),
            build_cv: self.build_cv.clone(),
            build_mutex: self.build_mutex.clone(),
            current_left_chunk: None,
            left_row_idx: 0,
            right_chunk_idx: 0,
            right_row_idx: 0,
        })
    }
}
