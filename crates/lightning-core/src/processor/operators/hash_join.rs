use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

pub struct SharedBuildSide {
    pub hash_table: HashMap<Value, Vec<usize>>,
    pub u64_hash_table: HashMap<u64, Vec<usize>>,
    pub build_chunks: Vec<RecordBatch>,
    pub build_done: bool,
    pub right_schema: Option<Arc<Schema>>,
    pub num_active_builders: usize,
    pub key_type: Option<DataType>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    LeftSemi,
    LeftAnti,
}

pub struct HashJoin {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,
    left_key_idx: usize,
    right_key_idx: usize,
    join_type: JoinType,
    is_cross_join: bool,

    // Shared state
    shared_build: Arc<RwLock<SharedBuildSide>>,
    build_cv: Arc<Condvar>,
    build_mutex: Arc<Mutex<bool>>, // Used with Condvar to signal done

    // Probe phase state
    current_left_chunk: Option<DataChunk>,
    left_row_idx: usize,
}

impl HashJoin {
    pub fn new(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        left_key_idx: usize,
        right_key_idx: usize,
    ) -> Self {
        Self {
            left,
            right,
            left_key_idx,
            right_key_idx,
            join_type: JoinType::Inner,
            is_cross_join: false,
            shared_build: Arc::new(RwLock::new(SharedBuildSide {
                hash_table: HashMap::new(),
                u64_hash_table: HashMap::new(),
                build_chunks: Vec::new(),
                build_done: false,
                right_schema: None,
                num_active_builders: 0,
                key_type: None,
            })),
            build_cv: Arc::new(Condvar::new()),
            build_mutex: Arc::new(Mutex::new(false)),
            current_left_chunk: None,
            left_row_idx: 0,
        }
    }

    pub fn new_cross_join(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
    ) -> Self {
        Self {
            left,
            right,
            left_key_idx: 0,
            right_key_idx: 0,
            join_type: JoinType::Inner,
            is_cross_join: true,
            shared_build: Arc::new(RwLock::new(SharedBuildSide {
                hash_table: HashMap::new(),
                u64_hash_table: HashMap::new(),
                build_chunks: Vec::new(),
                build_done: false,
                right_schema: None,
                num_active_builders: 0,
                key_type: None,
            })),
            build_cv: Arc::new(Condvar::new()),
            build_mutex: Arc::new(Mutex::new(false)),
            current_left_chunk: None,
            left_row_idx: 0,
        }
    }

    pub fn new_left_outer(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        left_key_idx: usize,
        right_key_idx: usize,
        is_cross_join: bool,
    ) -> Self {
        Self {
            left,
            right,
            left_key_idx,
            right_key_idx,
            join_type: JoinType::LeftOuter,
            is_cross_join,
            shared_build: Arc::new(RwLock::new(SharedBuildSide {
                hash_table: HashMap::new(),
                u64_hash_table: HashMap::new(),
                build_chunks: Vec::new(),
                build_done: false,
                right_schema: None,
                num_active_builders: 0,
                key_type: None,
            })),
            build_cv: Arc::new(Condvar::new()),
            build_mutex: Arc::new(Mutex::new(false)),
            current_left_chunk: None,
            left_row_idx: 0,
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
            let batch = chunk.batch;
            let mut shared = self.shared_build.write();

            if shared.right_schema.is_none() {
                shared.right_schema = Some(batch.schema());
                shared.key_type = Some(batch.column(self.right_key_idx).data_type().clone());
            }

            let num_rows = batch.num_rows();
            let base_offset = shared
                .build_chunks
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();

            // Specialized build loop for u64/i64 keys
            let key_col = batch.column(self.right_key_idx);
            match key_col.data_type() {
                DataType::UInt64 => {
                    let arr = key_col.as_any().downcast_ref::<UInt64Array>().expect("hash join build: UInt64 key");
                    for row_idx in 0..num_rows {
                        if !arr.is_null(row_idx) {
                            shared
                                .u64_hash_table
                                .entry(arr.value(row_idx))
                                .or_default()
                                .push(base_offset + row_idx);
                        }
                    }
                }
                DataType::Int64 => {
                    let arr = key_col.as_any().downcast_ref::<Int64Array>()
                        .expect("hash join build: Int64 key");
                    for row_idx in 0..num_rows {
                        if !arr.is_null(row_idx) {
                            shared
                                .u64_hash_table
                                .entry(arr.value(row_idx) as u64)
                                .or_default()
                                .push(base_offset + row_idx);
                        }
                    }
                }
                _ => {
                    // Fallback for complex keys
                    for row_idx in 0..num_rows {
                        let key = Value::from_arrow(key_col, row_idx);
                        shared
                            .hash_table
                            .entry(key)
                            .or_default()
                            .push(base_offset + row_idx);
                    }
                }
            }
            shared.build_chunks.push(batch);
        }

        let mut shared = self.shared_build.write();
        shared.num_active_builders -= 1;
        if shared.num_active_builders == 0 {
            // Finalize build: Concatenate into single batch for faster 'take'
            if !shared.build_chunks.is_empty() {
                let schema = shared.right_schema.clone()
                    .expect("right_schema should be set after build phase");
                let table_batch = arrow::compute::concat_batches(&schema, &shared.build_chunks)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                shared.build_chunks = vec![table_batch];
            }
            shared.build_done = true;
            let mut done = self.build_mutex.lock();
            *done = true;
            self.build_cv.notify_all();
        }
        Ok(())
    }

    pub fn wait_for_build(&self) {
        let mut done = self.build_mutex.lock();
        while !*done {
            self.build_cv.wait(&mut done);
        }
    }

    pub fn new_semi(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        l_key: usize,
        r_key: usize,
    ) -> Self {
        let mut n = Self::new(left, right, l_key, r_key);
        n.join_type = JoinType::LeftSemi;
        n
    }

    pub fn new_anti(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        l_key: usize,
        r_key: usize,
    ) -> Self {
        let mut n = Self::new(left, right, l_key, r_key);
        n.join_type = JoinType::LeftAnti;
        n
    }
}

impl PhysicalOperator for HashJoin {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, crate::processor::Value>>,
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
                if self.current_left_chunk.is_none() {
                    return Ok(None);
                }
            }

            let left_batch = &self.current_left_chunk.as_ref().unwrap().batch;
            let left_num_rows = left_batch.num_rows();

            let mut left_indices = Vec::with_capacity(1024);
            let mut right_indices = Vec::with_capacity(1024);

            let left_key_col = left_batch.column(self.left_key_idx);

            // Vectorized Probe Phase
            while self.left_row_idx < left_num_rows && left_indices.len() < 1024 {
                if self.is_cross_join {
                    // Cross join optimized path
                    if !shared.build_chunks.is_empty() {
                        let n = shared.build_chunks[0].num_rows();
                        for build_idx in 0..n {
                            left_indices.push(self.left_row_idx as u32);
                            right_indices.push(Some(build_idx as u32));
                            if left_indices.len() >= 1024 {
                                break;
                            }
                        }
                    }
                } else {
                    // Find matches using specialized or generic hash table
                    let matches_ref = match left_key_col.data_type() {
                        DataType::UInt64 => {
                            let arr = left_key_col.as_any().downcast_ref::<UInt64Array>()
                                .expect("hash join probe: UInt64 key");
                            if arr.is_null(self.left_row_idx) {
                                None
                            } else {
                                shared.u64_hash_table.get(&arr.value(self.left_row_idx))
                            }
                        }
                        DataType::Int64 => {
                            let arr = left_key_col.as_any().downcast_ref::<Int64Array>()
                                .expect("hash join probe: Int64 key");
                            if arr.is_null(self.left_row_idx) {
                                None
                            } else {
                                shared
                                    .u64_hash_table
                                    .get(&(arr.value(self.left_row_idx) as u64))
                            }
                        }
                        _ => {
                            let key = Value::from_arrow(left_key_col, self.left_row_idx);
                            shared.hash_table.get(&key)
                        }
                    };

                    match self.join_type {
                        JoinType::Inner => {
                            if let Some(m) = matches_ref {
                                for &build_idx in m {
                                    left_indices.push(self.left_row_idx as u32);
                                    right_indices.push(Some(build_idx as u32));
                                    if left_indices.len() >= 1024 {
                                        break;
                                    }
                                }
                            }
                        }
                        JoinType::LeftOuter => {
                            if let Some(m) = matches_ref {
                                for &build_idx in m {
                                    left_indices.push(self.left_row_idx as u32);
                                    right_indices.push(Some(build_idx as u32));
                                    if left_indices.len() >= 1024 {
                                        break;
                                    }
                                }
                            } else {
                                left_indices.push(self.left_row_idx as u32);
                                right_indices.push(None);
                            }
                        }
                        JoinType::LeftSemi => {
                            if matches_ref.is_some() {
                                left_indices.push(self.left_row_idx as u32);
                            }
                        }
                        JoinType::LeftAnti => {
                            if matches_ref.is_none() {
                                left_indices.push(self.left_row_idx as u32);
                            }
                        }
                    }
                }

                if left_indices.len() < 1024 {
                    self.left_row_idx += 1;
                }
            }

            if !left_indices.is_empty() {
                let left_indices_arr = UInt32Array::from(left_indices);

                let mut final_columns = Vec::new();
                let mut final_fields = Vec::new();

                // Build output columns using 'take' kernel
                for i in 0..left_batch.num_columns() {
                    let col = left_batch.column(i);
                    final_columns.push(arrow::compute::take(
                        col.as_ref(),
                        &left_indices_arr,
                        None,
                    )?);
                    final_fields.push(left_batch.schema().field(i).clone());
                }

                if self.join_type != JoinType::LeftSemi && self.join_type != JoinType::LeftAnti {
                    let right_indices_arr = UInt32Array::from(right_indices);
                    if let Some(build_batch) = shared.build_chunks.first() {
                        for i in 0..build_batch.num_columns() {
                            let col = build_batch.column(i);
                            final_columns.push(arrow::compute::take(
                                col.as_ref(),
                                &right_indices_arr,
                                None,
                            )?);
                            final_fields.push(build_batch.schema().field(i).clone());
                        }
                    } else if let Some(schema) = &shared.right_schema {
                        for field in schema.fields() {
                            final_columns.push(arrow::array::new_null_array(
                                field.data_type(),
                                left_indices_arr.len(),
                            ));
                            final_fields.push(field.as_ref().clone());
                        }
                    }
                }

                if self.left_row_idx >= left_num_rows {
                    self.current_left_chunk = None;
                }

                let batch =
                    RecordBatch::try_new(Arc::new(Schema::new(final_fields)), final_columns)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                return Ok(Some(DataChunk { batch }));
            }

            if self.left_row_idx >= left_num_rows {
                self.current_left_chunk = None;
            }

            if self.current_left_chunk.is_none() {
                return Ok(None);
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            left: self.left.clone_box(),
            right: self.right.clone_box(),
            left_key_idx: self.left_key_idx,
            right_key_idx: self.right_key_idx,
            join_type: self.join_type,
            is_cross_join: self.is_cross_join,
            shared_build: self.shared_build.clone(),
            build_cv: self.build_cv.clone(),
            build_mutex: self.build_mutex.clone(),
            current_left_chunk: None,
            left_row_idx: 0,
        })
    }
}
