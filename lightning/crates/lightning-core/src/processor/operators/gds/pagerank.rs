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
        if let Some(chunk) = self.child.get_next(database, tx, params)? {
            let num_rows = chunk.num_rows();
            let mut node_ids = Vec::with_capacity(num_rows);
            let mut max_id = 0;

            for i in 0..num_rows {
                let val = Value::from_arrow(chunk.batch.column(self.node_id_idx), i);
                if let Value::Node(id) = val {
                    node_ids.push(id);
                    if id != u64::MAX && id > max_id {
                        max_id = id;
                    }
                } else {
                    node_ids.push(u64::MAX);
                }
            }

            let storage = database.storage_manager.read();
            let bm = &database.buffer_manager;

            let size = (max_id + 1) as usize;
            let mut out_degrees = vec![0usize; size];
            let mut pr_scores = vec![0.0f64; size];
            let mut node_mask = fixedbitset::FixedBitSet::with_capacity(size);

            for &id in &node_ids {
                if id == u64::MAX {
                    continue;
                }
                node_mask.insert(id as usize);
                pr_scores[id as usize] = 1.0 / (num_rows as f64);

                let mut degree = 0;
                for rel in &self.rel_table_names {
                    if let Some(csr) = storage.fwd_csr.get(rel) {
                        let _ = csr.for_each_neighbor(bm, id, tx, |_| degree += 1);
                    }
                }
                out_degrees[id as usize] = degree;
            }

            let csrs: Vec<_> = self
                .rel_table_names
                .iter()
                .filter_map(|r| storage.fwd_csr.get(r))
                .collect();

            for _ in 0..self.max_iterations {
                let mut next_scores = vec![0.0f64; size];
                let mut dangling_sum = 0.0;

                // Parallel contribution phase
                let contributions: Vec<(usize, f64)> = node_mask
                    .ones()
                    .collect::<Vec<_>>()
                    .par_iter()
                    .flat_map(|&id| {
                        let score = pr_scores[id];
                        let degree = out_degrees[id];
                        let mut local_contribs = Vec::new();

                        if degree > 0 {
                            let contrib = score / (degree as f64);
                            for csr in &csrs {
                                let _ = csr.for_each_neighbor(bm, id as u64, tx, |neighbor| {
                                    local_contribs.push((neighbor as usize, contrib));
                                });
                            }
                        }
                        local_contribs
                    })
                    .collect();

                for (nid, contrib) in contributions {
                    if nid < size && node_mask.contains(nid) {
                        next_scores[nid] += contrib;
                    }
                }

                // Collect dangling sum serially (fast enough)
                for id in node_mask.ones() {
                    if out_degrees[id] == 0 {
                        dangling_sum += pr_scores[id];
                    }
                }

                let teleport = (1.0 - self.damping_factor) / (num_rows as f64)
                    + (self.damping_factor * dangling_sum) / (num_rows as f64);

                for id in node_mask.ones() {
                    pr_scores[id] = teleport + self.damping_factor * next_scores[id];
                }
            }

            let final_scores: Vec<Option<f64>> = node_ids
                .iter()
                .map(|&id| {
                    if id == u64::MAX {
                        None
                    } else {
                        Some(pr_scores[id as usize])
                    }
                })
                .collect();

            let mut columns = chunk.batch.columns().to_vec();
            columns.push(Arc::new(Float64Array::from(final_scores)));

            let mut fields = chunk.batch.schema().fields().to_vec();
            fields.push(Field::new(&self.output_var_name, DataType::Float64, true).into());

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
            damping_factor: self.damping_factor,
            max_iterations: self.max_iterations,
            output_var_name: self.output_var_name.clone(),
            node_id_idx: self.node_id_idx,
        })
    }
}
