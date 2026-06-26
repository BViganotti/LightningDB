use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::index::csr::CSRIndex;
use crate::transaction::transaction_manager::Transaction;
use crate::Database;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub struct PhysicalASP {
    child: Box<dyn PhysicalOperator>,
    rel_table_name: String,
    src_var_name: String,
    dst_var_name: String,
    path_var_name: String,
    max_depth: u32,

    // BFS state
    current_chunk: Option<DataChunk>,
    chunk_row_idx: usize,
    results: VecDeque<DataChunk>,
    bfs_queue: VecDeque<(u64, Vec<u64>)>,
    bfs_src_id: u64,
    bfs_dst_id: u64,
    bfs_phase: BFSPhase,
    cached_csr: Option<Arc<CSRIndex>>,
    /// Shortest distance found
    shortest_dist: u32,
    /// All shortest paths found so far
    found_paths: Vec<Vec<u64>>,
}

enum BFSPhase {
    Idle,
    Active,
}

/// Standalone BFS shortest path function usable from both PhysicalASP and the evaluator.
pub fn bfs_shortest_path(
    csr: &CSRIndex,
    src_id: u64,
    dst_id: u64,
    max_depth: u32,
    bm: &BufferManager,
    tx: &Transaction,
) -> Result<Vec<Vec<u64>>> {
    if src_id == dst_id {
        return Ok(vec![vec![src_id]]);
    }

    let mut visited: HashMap<u64, u32> = HashMap::new();
    let mut predecessors: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut queue: VecDeque<u64> = VecDeque::new();

    visited.insert(src_id, 0);
    queue.push_back(src_id);
    let mut found_distance = u32::MAX;

    while let Some(current) = queue.pop_front() {
        let dist = visited[&current];
        if dist >= found_distance || dist >= max_depth {
            continue;
        }

        let mut neighbors = Vec::new();
        csr.for_each_neighbor(bm, current, tx, |n| {
            neighbors.push(n);
        })?;

        for neighbor in neighbors {
            let new_dist = dist + 1;

            if neighbor == dst_id && new_dist <= max_depth {
                if found_distance == u32::MAX {
                    found_distance = new_dist;
                }
                if new_dist == found_distance {
                    predecessors.entry(neighbor).or_default().push(current);
                }
                continue;
            }

            if new_dist >= found_distance {
                continue;
            }

            if let Some(&existing_dist) = visited.get(&neighbor) {
                if new_dist == existing_dist {
                    predecessors.entry(neighbor).or_default().push(current);
                }
            } else if new_dist < *visited.get(&neighbor).unwrap_or(&u32::MAX) {
                visited.insert(neighbor, new_dist);
                predecessors.entry(neighbor).or_default().push(current);
                queue.push_back(neighbor);
            }
        }
    }

    if found_distance == u32::MAX {
        return Ok(Vec::new());
    }

    let mut all_paths = Vec::new();
    let mut stack = vec![(dst_id, vec![dst_id])];

    while let Some((node, path)) = stack.pop() {
        if node == src_id {
            let mut full_path = path.clone();
            full_path.reverse();
            all_paths.push(full_path);
            continue;
        }
        if let Some(preds) = predecessors.get(&node) {
            for &pred in preds {
                let mut new_path = path.clone();
                new_path.push(pred);
                stack.push((pred, new_path));
            }
        }
    }

    Ok(all_paths)
}

impl PhysicalASP {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        rel_table_name: String,
        src_var_name: String,
        dst_var_name: String,
        path_var_name: String,
        max_depth: u32,
    ) -> Self {
        Self {
            child,
            rel_table_name,
            src_var_name,
            dst_var_name,
            path_var_name,
            max_depth,
            current_chunk: None,
            chunk_row_idx: 0,
            results: VecDeque::new(),
            bfs_queue: VecDeque::new(),
            bfs_src_id: 0,
            bfs_dst_id: 0,
            bfs_phase: BFSPhase::Idle,
            cached_csr: None,
            shortest_dist: u32::MAX,
            found_paths: Vec::new(),
        }
    }

    fn get_csr(&self, database: &Database, tx: &crate::transaction::transaction_manager::Transaction) -> Option<Arc<CSRIndex>> {
        let sm = database.storage_manager.read();
        let _ = sm.ensure_csr_fresh(&self.rel_table_name, &database.buffer_manager, tx);
        sm.fwd_csr.get(&self.rel_table_name).cloned()
    }

    fn find_all_shortest_paths(
        &mut self,
        csr: &CSRIndex,
        src_id: u64,
        dst_id: u64,
        bm: &BufferManager,
        tx: &Transaction,
    ) -> Result<Vec<Vec<u64>>> {
        bfs_shortest_path(csr, src_id, dst_id, self.max_depth, bm, tx)
    }

    fn build_chunk_for_source(&self, src_id: u64, dst_id: u64) -> DataChunk {
        let mut path_values = Vec::new();

        for path in &self.found_paths {
            let nodes: Vec<Value> = path.iter().map(|&id| Value::Node(id)).collect();
            path_values.push(Value::List(nodes));
        }

        use arrow::array::{UInt64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let num_paths = self.found_paths.len();

        let mut src_ids = Vec::with_capacity(num_paths);
        let mut dst_ids = Vec::with_capacity(num_paths);
        for _ in 0..num_paths {
            src_ids.push(src_id);
            dst_ids.push(dst_id);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(&self.src_var_name, DataType::UInt64, false),
            Field::new(&self.dst_var_name, DataType::UInt64, false),
            Field::new(&self.path_var_name, DataType::Utf8, true),
        ]));

        let path_strs: Vec<String> = self
            .found_paths
            .iter()
            .map(|p| {
                let ids: Vec<String> = p.iter().map(|id| id.to_string()).collect();
                format!("[{}]", ids.join(", "))
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(src_ids)),
                Arc::new(UInt64Array::from(dst_ids)),
                Arc::new(StringArray::from(path_strs)),
            ],
        )
        .expect("ASP schema must match columns");

        DataChunk::new(batch)
    }
}

impl PhysicalOperator for PhysicalASP {
    fn get_next(
        &mut self,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(res) = self.results.pop_front() {
            return Ok(Some(res));
        }

        let bm = &database.buffer_manager;

        loop {
            if self.current_chunk.is_none() {
                self.current_chunk = self.child.get_next(database, tx, params)?;
                self.chunk_row_idx = 0;
                self.bfs_phase = BFSPhase::Active;
                if self.current_chunk.is_none() {
                    return Ok(None);
                }
            }

            if let Some(ref chunk) = self.current_chunk {
                match self.bfs_phase {
                    BFSPhase::Active => {
                        if self.chunk_row_idx + 1 < chunk.num_rows() {
                            let src_col = chunk.batch.column(0);
                            let src_id = match Value::from_arrow(src_col, self.chunk_row_idx) {
                                Value::Node(id) => id,
                                Value::Number(n) => n as u64,
                                _ => return Ok(None),
                            };
                            let dst_col = chunk.batch.column(1);
                            let dst_id = match Value::from_arrow(dst_col, self.chunk_row_idx) {
                                Value::Node(id) => id,
                                Value::Number(n) => n as u64,
                                _ => return Ok(None),
                            };
                            self.chunk_row_idx += 2;

                            if self.cached_csr.is_none() {
                                self.cached_csr = self.get_csr(database, tx);
                            }
                            let csr = self.cached_csr.clone();
                            if let Some(csr) = csr {
                                self.found_paths = self.find_all_shortest_paths(&csr, src_id, dst_id, bm, tx)?;
                                if !self.found_paths.is_empty() {
                                    let result_chunk = self.build_chunk_for_source(src_id, dst_id);
                                    self.results.push_back(result_chunk);
                                    if let Some(res) = self.results.pop_front() {
                                        return Ok(Some(res));
                                    }
                                }
                            }
                        } else {
                            self.current_chunk = None;
                            self.cached_csr = None;
                            self.bfs_phase = BFSPhase::Idle;
                        }
                    }
                    BFSPhase::Idle => {
                        self.current_chunk = None;
                    }
                }
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            rel_table_name: self.rel_table_name.clone(),
            src_var_name: self.src_var_name.clone(),
            dst_var_name: self.dst_var_name.clone(),
            path_var_name: self.path_var_name.clone(),
            max_depth: self.max_depth,
            current_chunk: None,
            chunk_row_idx: 0,
            results: VecDeque::new(),
            bfs_queue: VecDeque::new(),
            bfs_src_id: 0,
            bfs_dst_id: 0,
            bfs_phase: BFSPhase::Idle,
            cached_csr: None,
            shortest_dist: u32::MAX,
            found_paths: Vec::new(),
        })
    }
}
