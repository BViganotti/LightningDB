use crate::processor::arrow_utils::values_to_array;
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
                    if let Ok(merged) = arrow::compute::concat_batches(&schema, &batches) {
                        shared.right_chunks = vec![DataChunk { batch: merged }];
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

            let mut left_rows: Vec<Vec<Value>> = vec![Vec::with_capacity(1024); left_num_cols];
            let mut right_rows: Vec<Vec<Value>> =
                vec![Vec::with_capacity(1024); right_column_count];

            while self.left_row_idx < left_num_rows {
                let right_chunk = &shared.right_chunks[self.right_chunk_idx];
                let right_batch = &right_chunk.batch;
                let right_num_rows = right_batch.num_rows();

                for r_row in 0..right_num_rows {
                    for col_idx in 0..left_num_cols {
                        left_rows[col_idx].push(Value::from_arrow(
                            left_batch.column(col_idx),
                            self.left_row_idx,
                        ));
                    }
                    for col_idx in 0..right_column_count {
                        right_rows[col_idx]
                            .push(Value::from_arrow(right_batch.column(col_idx), r_row));
                    }
                }

                self.right_chunk_idx += 1;
                if self.right_chunk_idx >= shared.right_chunks.len() {
                    self.right_chunk_idx = 0;
                    self.left_row_idx += 1;
                }

                if !left_rows.is_empty() && left_rows[0].len() >= 1024 {
                    break;
                }
            }

            if self.left_row_idx >= left_num_rows {
                self.current_left_chunk = None;
            }

            if !left_rows.is_empty() && !left_rows[0].is_empty() {
                let mut final_columns = Vec::new();
                let mut final_fields: Vec<FieldRef> = Vec::new();

                for i in 0..left_num_cols {
                    let field = left_schema.field(i);
                    final_fields.push(Arc::new((*field).clone()));
                    final_columns.push(values_to_array(&left_rows[i], field.data_type()));
                }

                if let Some(right_schema) = &shared.right_schema {
                    for i in 0..right_column_count {
                        let field = right_schema.field(i);
                        final_fields.push(Arc::new((*field).clone()));
                        final_columns.push(values_to_array(&right_rows[i], field.data_type()));
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
        })
    }
}
