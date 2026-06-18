use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::index::csr::CSRIndex;
use crate::Database;
use crate::Result;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// All Shortest Paths operator.
///
/// NOTE: Despite the name, this operator performs standard BFS which finds
/// the SHORTEST DISTANCE to each reachable node, not ALL shortest paths.
/// If multiple shortest paths exist between two nodes, only one distance
/// is reported. A true "all shortest paths" implementation would need to
/// track path counts at each distance level.
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
    bfs_queue: VecDeque<u64>,
    bfs_distance: HashMap<u64, u32>,
    bfs_src_id: u64,
    #[allow(dead_code)]
    bfs_depth: u32,
    bfs_phase: BFSPhase,
    cached_csr: Option<Arc<CSRIndex>>,
}

enum BFSPhase {
    Idle,
    Active,
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
            bfs_distance: HashMap::new(),
            bfs_src_id: 0,
            bfs_depth: 0,
            bfs_phase: BFSPhase::Idle,
            cached_csr: None,
        }
    }

    /// Get the CSR index, caching it so it's only fetched once per batch.
    fn get_csr(&self, database: &Database, tx: &crate::transaction::transaction_manager::Transaction) -> Option<Arc<CSRIndex>> {
        let sm = database.storage_manager.read();
        let _ = sm.ensure_csr_fresh(&self.rel_table_name, &database.buffer_manager, tx);
        sm.fwd_csr.get(&self.rel_table_name).cloned()
    }

    fn run_bfs(
        &mut self,
        csr: &CSRIndex,
        src_id: u64,
        bm: &crate::storage::buffer_manager::BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.bfs_queue.clear();
        self.bfs_distance.clear();
        self.bfs_queue.push_back(src_id);
        self.bfs_distance.insert(src_id, 0);
        self.bfs_src_id = src_id;

        while let Some(current) = self.bfs_queue.pop_front() {
            let dist = match self.bfs_distance.get(&current) {
                Some(&d) => d,
                None => continue,
            };
            if dist >= self.max_depth {
                continue;
            }

            let mut neighbors = Vec::new();
            csr.for_each_neighbor(bm, current, tx, |n| {
                if !self.bfs_distance.contains_key(&n) {
                    neighbors.push(n);
                }
            })?;

            for neighbor in neighbors {
                self.bfs_distance.insert(neighbor, dist + 1);
                self.bfs_queue.push_back(neighbor);
            }
        }

        Ok(())
    }

    fn build_chunk_for_source(&self, src_id: u64) -> DataChunk {
        let mut src_ids = Vec::new();
        let mut dst_ids = Vec::new();
        let mut distances = Vec::new();

        for (&dst_id, &dist) in self.bfs_distance.iter() {
            if dst_id != src_id {
                src_ids.push(src_id as f64);
                dst_ids.push(dst_id as f64);
                distances.push(dist as f64);
            }
        }

        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new(&self.src_var_name, DataType::Float64, false),
            Field::new(&self.dst_var_name, DataType::Float64, false),
            Field::new(&self.path_var_name, DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(src_ids)),
                Arc::new(Float64Array::from(dst_ids)),
                Arc::new(Float64Array::from(distances)),
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
        // Drain any queued results first
        if let Some(res) = self.results.pop_front() {
            return Ok(Some(res));
        }

        let bm = &database.buffer_manager;

        loop {
            // Load next chunk from child if needed
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
                        if self.chunk_row_idx < chunk.num_rows() {
                            let col = chunk.batch.column(0);
                            let src_id = match Value::from_arrow(col, self.chunk_row_idx) {
                                Value::Node(id) => id,
                                Value::Number(n) => n as u64,
                                _ => return Ok(None),
                            };
                            self.chunk_row_idx += 1;

                            // Cache CSR for the duration of processing this chunk
                            if self.cached_csr.is_none() {
                                self.cached_csr = self.get_csr(database, tx);
                            }
                            // Clone the CSR Arc to avoid borrow conflict with self.run_bfs
                            let csr = self.cached_csr.clone();
                            if let Some(csr) = csr {
                                self.run_bfs(&csr, src_id, bm, tx)?;
                                let result_chunk = self.build_chunk_for_source(src_id);
                                if result_chunk.batch.num_rows() > 0 {
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
            bfs_distance: HashMap::new(),
            bfs_src_id: 0,
            bfs_depth: 0,
            bfs_phase: BFSPhase::Idle,
            cached_csr: None,
        })
    }
}
