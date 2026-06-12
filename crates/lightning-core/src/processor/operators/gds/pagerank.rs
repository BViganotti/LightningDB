use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Database;
use crate::Result;
use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalPageRank {
    child: Box<dyn PhysicalOperator>,
    rel_table_names: Vec<String>,
    damping_factor: f64,
    max_iterations: u32,
    output_var_name: String,
    node_id_idx: usize,
    buffered_results: Option<Vec<DataChunk>>,
    accumulated: bool,
}

impl PhysicalPageRank {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        rel_table_names: Vec<String>,
        damping_factor: f64,
        max_iterations: u32,
        output_var_name: String,
        node_id_idx: usize,
    ) -> Self {
        Self {
            child,
            rel_table_names,
            damping_factor,
            max_iterations,
            output_var_name,
            node_id_idx,
            buffered_results: None,
            accumulated: false,
        }
    }
}

impl PhysicalOperator for PhysicalPageRank {
    fn get_next(
        &mut self,
        database: &Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(mut buf) = self.buffered_results.take() {
            if buf.is_empty() {
                return Ok(None);
            }
            let first = buf.remove(0);
            self.buffered_results = Some(buf);
            return Ok(Some(first));
        }

        if self.accumulated {
            return Ok(None);
        }

        // Accumulate ALL chunks from child before computing PageRank
        let mut all_chunks: Vec<DataChunk> = Vec::new();
        let mut all_node_ids: Vec<u64> = Vec::new();

        while let Some(chunk) = self.child.get_next(database, tx, params)? {
            let num_rows = chunk.num_rows();
            for i in 0..num_rows {
                let val = Value::from_arrow(chunk.batch.column(self.node_id_idx), i);
                if let Value::Node(id) = val {
                    all_node_ids.push(id);
                } else {
                    all_node_ids.push(u64::MAX);
                }
            }
            all_chunks.push(chunk);
        }

        self.accumulated = true;

        if all_chunks.is_empty() {
            return Ok(None);
        }

        // Build sparse representation: only nodes present in the result set
        let mut node_to_idx: HashMap<u64, usize> = HashMap::new();
        let mut active_nodes: Vec<u64> = Vec::new();
        for &nid in &all_node_ids {
            if nid != u64::MAX && !node_to_idx.contains_key(&nid) {
                node_to_idx.insert(nid, active_nodes.len());
                active_nodes.push(nid);
            }
        }

        let num_nodes = active_nodes.len();
        if num_nodes == 0 {
            return Ok(Some(DataChunk {
                batch: all_chunks.remove(0).batch,
            }));
        }

        let storage = database.storage_manager.read();
        let bm = &database.buffer_manager;

        let mut out_degrees = vec![0usize; num_nodes];
        let mut pr_scores = vec![0.0f64; num_nodes];

        // Collect CSR references once, outside the per-node loop
        let csrs: Vec<_> = self.rel_table_names.iter()
            .filter_map(|r| storage.fwd_csr.get(r))
            .collect();

        for (local_idx, &nid) in active_nodes.iter().enumerate() {
            pr_scores[local_idx] = 1.0 / (num_nodes as f64);

            let mut deg = 0;
            for csr in &csrs {
                if let Err(e) = csr.for_each_neighbor(bm, nid, tx, |_| deg += 1) {
                    tracing::warn!("PageRank: error counting neighbors for node {}: {}", nid, e);
                }
            }
            out_degrees[local_idx] = deg;
        }

        // Collect CSR references — lifetime tied to storage guard
        let csrs: Vec<_> = self
            .rel_table_names
            .iter()
            .filter_map(|r| storage.fwd_csr.get(r))
            .collect();

        for _ in 0..self.max_iterations {
            let mut next_scores = vec![0.0f64; num_nodes];
            let mut dangling_sum = 0.0;

            let contributions: Vec<(u64, f64)> = (0..num_nodes)
                .collect::<Vec<_>>()
                .par_iter()
                .flat_map(|&local_idx| {
                    let nid = active_nodes[local_idx];
                    let score = pr_scores[local_idx];
                    let degree = out_degrees[local_idx];
                    let mut local_contribs = Vec::new();

                    if degree > 0 {
                        let contrib = score / (degree as f64);
                        for csr in &csrs {
                            if let Err(e) = csr.for_each_neighbor(bm, nid, tx, |neighbor| {
                                local_contribs.push((neighbor, contrib));
                            }) {
                                tracing::warn!("PageRank: error traversing neighbors for node {}: {}", nid, e);
                            }
                        }
                    }
                    local_contribs
                })
                .collect();

            for (nid, contrib) in contributions {
                if let Some(&local_idx) = node_to_idx.get(&nid) {
                    next_scores[local_idx] += contrib;
                }
            }

            for local_idx in 0..num_nodes {
                if out_degrees[local_idx] == 0 {
                    dangling_sum += pr_scores[local_idx];
                }
            }

            let teleport = (1.0 - self.damping_factor) / (num_nodes as f64)
                + (self.damping_factor * dangling_sum) / (num_nodes as f64);

            for local_idx in 0..num_nodes {
                pr_scores[local_idx] = teleport + self.damping_factor * next_scores[local_idx];
            }
        }

        let mut chunk_start = 0usize;
        let mut results = Vec::with_capacity(all_chunks.len());
        for chunk in all_chunks.drain(..) {
            let num_rows = chunk.num_rows();
            let mut final_scores = Vec::with_capacity(num_rows);
            for row_offset in 0..num_rows {
                let nid = all_node_ids[chunk_start + row_offset];
                let score = if nid == u64::MAX {
                    None
                } else {
                    node_to_idx.get(&nid).map(|&local_idx| pr_scores[local_idx])
                };
                final_scores.push(score);
            }
            chunk_start += num_rows;

            let mut columns = chunk.batch.columns().to_vec();
            columns.push(Arc::new(Float64Array::from(final_scores)));

            let mut fields = chunk.batch.schema().fields().to_vec();
            fields.push(Field::new(&self.output_var_name, DataType::Float64, true).into());

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
            results.push(DataChunk { batch });
        }

        let first = results.remove(0);
        self.buffered_results = Some(results);
        Ok(Some(first))
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            rel_table_names: self.rel_table_names.clone(),
            damping_factor: self.damping_factor,
            max_iterations: self.max_iterations,
            output_var_name: self.output_var_name.clone(),
            node_id_idx: self.node_id_idx,
            buffered_results: None,
            accumulated: false,
        })
    }
}
