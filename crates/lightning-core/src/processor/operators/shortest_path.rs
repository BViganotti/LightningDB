use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fixedbitset::FixedBitSet;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub struct PhysicalShortestPath {
    child: Box<dyn PhysicalOperator>,
    rel_table_names: Vec<String>,
    bm: Arc<BufferManager>,
    src_var_idx: usize,
    dst_var_idx: usize,
    max_depth: u32,
    path_var_name: String,
}

impl PhysicalShortestPath {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        rel_table_names: Vec<String>,
        bm: Arc<BufferManager>,
        src_var_idx: usize,
        dst_var_idx: usize,
        max_depth: u32,
        path_var_name: String,
    ) -> Self {
        Self {
            child,
            rel_table_names,
            bm,
            src_var_idx,
            dst_var_idx,
            max_depth,
            path_var_name,
        }
    }

    /// High-performance Bi-directional BFS for Shortest Path
    fn find_shortest_path(
        &self,
        src_id: u64,
        dst_id: u64,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
        storage: &crate::storage::StorageManager,
    ) -> Result<Option<Vec<u64>>> {
        if src_id == dst_id {
            return Ok(Some(vec![src_id]));
        }

        let mut q_src = VecDeque::new();
        let mut q_dst = VecDeque::new();
        let mut visited_src = HashMap::new();
        let mut visited_dst = HashMap::new();

        q_src.push_back(src_id);
        visited_src.insert(src_id, None);
        q_dst.push_back(dst_id);
        visited_dst.insert(dst_id, None);

        let csrs_fwd: Vec<_> = self
            .rel_table_names
            .iter()
            .filter_map(|n| storage.fwd_csr.get(n))
            .collect();
        let csrs_bwd: Vec<_> = self
            .rel_table_names
            .iter()
            .filter_map(|n| storage.bwd_csr.get(n))
            .collect();

        for depth in 0..self.max_depth {
            // Expand from source (forward)
            if !q_src.is_empty() {
                for _ in 0..q_src.len() {
                    let curr = q_src.pop_front()
                        .expect("BFS source queue should not be empty during iteration");
                    for csr in &csrs_fwd {
                        if let Ok(neighbors) = csr.get_neighbors(bm, curr, tx) {
                            for n in neighbors {
                                if visited_dst.contains_key(&n) {
                                    return Ok(Some(self.reconstruct_path(
                                        n,
                                        &visited_src,
                                        &visited_dst,
                                        curr,
                                        true,
                                    )));
                                }
                                if !visited_src.contains_key(&n) {
                                    visited_src.insert(n, Some(curr));
                                    q_src.push_back(n);
                                }
                            }
                        }
                    }
                }
            }

            // Expand from destination (backward)
            if !q_dst.is_empty() {
                for _ in 0..q_dst.len() {
                    let curr = q_dst.pop_front()
                        .expect("BFS destination queue should not be empty during iteration");
                    for csr in &csrs_bwd {
                        if let Ok(neighbors) = csr.get_neighbors(bm, curr, tx) {
                            for n in neighbors {
                                if visited_src.contains_key(&n) {
                                    return Ok(Some(self.reconstruct_path(
                                        n,
                                        &visited_src,
                                        &visited_dst,
                                        curr,
                                        false,
                                    )));
                                }
                                if !visited_dst.contains_key(&n) {
                                    visited_dst.insert(n, Some(curr));
                                    q_dst.push_back(n);
                                }
                            }
                        }
                    }
                }
            }
            if depth * 2 >= self.max_depth {
                break;
            }
        }
        Ok(None)
    }

    fn reconstruct_path(
        &self,
        meeting_point: u64,
        visited_src: &HashMap<u64, Option<u64>>,
        visited_dst: &HashMap<u64, Option<u64>>,
        last_node: u64,
        from_src_side: bool,
    ) -> Vec<u64> {
        let mut path_src = Vec::new();
        let mut curr = if from_src_side {
            Some(last_node)
        } else {
            Some(meeting_point)
        };
        while let Some(id) = curr {
            path_src.push(id);
            curr = visited_src.get(&id).cloned().flatten();
        }
        path_src.reverse();

        let mut path_dst = Vec::new();
        curr = if from_src_side {
            Some(meeting_point)
        } else {
            Some(last_node)
        };
        while let Some(id) = curr {
            path_dst.push(id);
            curr = visited_dst.get(&id).cloned().flatten();
        }

        path_src.extend(path_dst);
        path_src
    }
}

impl PhysicalOperator for PhysicalShortestPath {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.child.get_next(database, tx, params)? {
            let num_rows = chunk.num_rows();
            let mut paths = Vec::new();

            for i in 0..num_rows {
                let src_val = Value::from_arrow(chunk.batch.column(self.src_var_idx), i);
                let dst_val = Value::from_arrow(chunk.batch.column(self.dst_var_idx), i);

                if let (Value::Node(src), Value::Node(dst)) = (src_val, dst_val) {
                    if let Some(path) = self.find_shortest_path(
                        src,
                        dst,
                        &database.buffer_manager,
                        tx,
                        &database.storage_manager.read(),
                    )? {
                        paths.push(Value::List(path.into_iter().map(Value::Node).collect()));
                    } else {
                        paths.push(Value::Null);
                    }
                } else {
                    paths.push(Value::Null);
                }
            }

            let mut columns = chunk.batch.columns().to_vec();
            columns.push(crate::processor::arrow_utils::values_to_array(
                &paths,
                &DataType::Utf8,
            ));

            let mut fields = chunk.batch.schema().fields().to_vec();
            fields.push(Field::new(&self.path_var_name, DataType::Utf8, true).into());

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            return Ok(Some(DataChunk { batch }));
        }
        Ok(None)
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            rel_table_names: self.rel_table_names.clone(),
            bm: self.bm.clone(),
            src_var_idx: self.src_var_idx,
            dst_var_idx: self.dst_var_idx,
            max_depth: self.max_depth,
            path_var_name: self.path_var_name.clone(),
        })
    }
}
