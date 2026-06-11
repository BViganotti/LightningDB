use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Database;
use crate::Result;
use arrow::array::{UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fixedbitset::FixedBitSet;
use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub struct PhysicalRecursiveJoin {
    child: Box<dyn PhysicalOperator>,
    rel_tables: Vec<String>,
    src_var: String,
    dst_node_table: String,
    dst_var: String,
    bounds: Option<(Option<u32>, Option<u32>)>,
    src_col_idx: Option<usize>,
    output_buffer: VecDeque<(u64, u64, u32)>,
    exhausted_child: bool,
}

impl PhysicalRecursiveJoin {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        rel_tables: Vec<String>,
        src_var: String,
        dst_node_table: String,
        dst_var: String,
        bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Self {
        Self {
            child,
            rel_tables,
            src_var,
            dst_node_table,
            dst_var,
            bounds,
            src_col_idx: None,
            output_buffer: VecDeque::new(),
            exhausted_child: false,
        }
    }

    /// Parallel Frontier-based BFS traversal (Branchless/Flat pattern)
    fn execute_bfs(
        &self,
        db: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        start_node: u64,
    ) -> Result<Vec<(u64, u32)>> {
        let min_depth = self.bounds.map_or(1, |b| b.0.unwrap_or(1));
        let max_depth = self.bounds.map_or(3, |b| b.1.unwrap_or(3));

        let storage = db.storage_manager.read();
        let bm = &db.buffer_manager;

        let max_id = match storage.get_table(&self.dst_node_table) {
            Some(t) => {
                let nid = t.next_row_id.load(std::sync::atomic::Ordering::SeqCst);
                if nid == 0 { 1 } else { nid }
            }
            None => {
                return Err(crate::LightningError::Internal(format!(
                    "destination node table '{}' not found",
                    self.dst_node_table
                )));
            }
        };

        let mut results = Vec::new();
        let mut visited = FixedBitSet::with_capacity(max_id as usize);

        let mut current_frontier = vec![start_node];
        visited.insert(start_node as usize);

        for depth in 1..=max_depth {
            if current_frontier.is_empty() {
                break;
            }

            // Parallel neighbor extraction - capture only Sync variables
            let rel_tables = &self.rel_tables;
            let storage = &storage;
            let bm = bm;
            let tx = tx;

            let next_frontier: Vec<u64> = current_frontier
                .par_iter()
                .flat_map(|&node| {
                    let mut neighbors = Vec::new();
                    for rel_table in rel_tables {
                        if let Some(fwd_csr) = storage.fwd_csr.get(rel_table) {
                            if let Err(e) = fwd_csr.for_each_neighbor(bm, node, tx, |n| neighbors.push(n)) {
                                tracing::warn!("CSR traversal error for node {} in BFS: {}", node, e);
                            }
                        }
                    }
                    neighbors
                })
                .collect();

            current_frontier.clear();
            for node in next_frontier {
                if node < max_id && !visited.contains(node as usize) {
                    visited.insert(node as usize);
                    current_frontier.push(node);
                    if depth >= min_depth {
                        results.push((node, depth));
                    }
                }
            }
        }

        Ok(results)
    }
}

impl PhysicalOperator for PhysicalRecursiveJoin {
    fn get_next(
        &mut self,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if self.src_col_idx.is_none() {
            // Determine src_col_idx from the src_var name by matching against child schema
            if let Some(chunk) = self.child.get_next(database, tx, params)? {
                let schema = chunk.batch.schema();
                let idx = schema.fields().iter().position(|f| f.name() == &self.src_var);
                self.src_col_idx = idx;
                // Put the chunk back into output_buffer for processing
                let src_idx = self.src_col_idx
                    .ok_or_else(|| crate::LightningError::Internal(format!(
                        "src_var '{}' not found in child schema", self.src_var
                    )))?;
                let num_rows = chunk.num_rows();
                for i in 0..num_rows {
                    let val = Value::from_arrow(chunk.batch.column(src_idx), i);
                    if let Value::Node(id) = val {
                        let bfs_results = self.execute_bfs(database, tx, id)?;
                        for (dst, depth) in bfs_results {
                            self.output_buffer.push_back((id, dst, depth));
                        }
                    }
                }
            } else {
                self.exhausted_child = true;
            }
            // Fall through to check output_buffer below
        }

        while !self.exhausted_child && self.output_buffer.is_empty() {
            if let Some(chunk) = self.child.get_next(database, tx, params)? {
                let src_idx = self.src_col_idx
                    .ok_or_else(|| crate::LightningError::Internal("src_col_idx not initialized".into()))?;
                let num_rows = chunk.num_rows();

                for i in 0..num_rows {
                    let val = Value::from_arrow(chunk.batch.column(src_idx), i);
                    if let Value::Node(id) = val {
                        let bfs_results = self.execute_bfs(database, tx, id)?;
                        for (dst, depth) in bfs_results {
                            self.output_buffer.push_back((id, dst, depth));
                        }
                    }
                }
            } else {
                self.exhausted_child = true;
            }
        }

        if self.output_buffer.is_empty() {
            return Ok(None);
        }

        let chunk_size = std::cmp::min(2048, self.output_buffer.len());
        let mut src_col = Vec::with_capacity(chunk_size);
        let mut dst_col = Vec::with_capacity(chunk_size);
        let mut depth_col = Vec::with_capacity(chunk_size);

        for _ in 0..chunk_size {
            if let Some((src, dst, depth)) = self.output_buffer.pop_front() {
                src_col.push(src);
                dst_col.push(dst);
                depth_col.push(depth);
            }
        }

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(&self.src_var, DataType::UInt64, false),
                Field::new(&self.dst_var, DataType::UInt64, false),
                Field::new("path_length", DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(src_col)) as Arc<dyn arrow::array::Array>,
                Arc::new(UInt64Array::from(dst_col)) as Arc<dyn arrow::array::Array>,
                Arc::new(UInt32Array::from(depth_col)) as Arc<dyn arrow::array::Array>,
            ],
        )
        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

        Ok(Some(DataChunk { batch }))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            rel_tables: self.rel_tables.clone(),
            src_var: self.src_var.clone(),
            dst_node_table: self.dst_node_table.clone(),
            dst_var: self.dst_var.clone(),
            bounds: self.bounds,
            src_col_idx: self.src_col_idx,
            output_buffer: self.output_buffer.clone(),
            exhausted_child: self.exhausted_child,
        })
    }
}
