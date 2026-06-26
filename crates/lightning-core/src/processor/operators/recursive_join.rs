use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::storage_manager::Table;
use crate::{LightningError, Result};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

const DEFAULT_MAX_TRAVERSAL_MS: u64 = 30_000; // 30 seconds

pub struct PhysicalRecursiveJoin {
    pub child: Box<dyn PhysicalOperator>,
    pub rel_table: Table,
    pub dst_table: Table,
    pub bm: Arc<BufferManager>,
    pub num_rows: u64,
    pub src_var_idx: usize,
    pub bounds: (u32, u32),
    pub mask: Option<Arc<RwLock<super::semi_mask::SemiMask>>>,
    pub read_ts: u64,
    pub max_traversal_ms: u64,
    /// Name of the relationship variable (e.g. "r"). Used as the column name
    /// for the path length (depth) in the operator's output.
    pub rel_var_name: String,
}

impl PhysicalRecursiveJoin {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        rel_table: Table,
        dst_table: Table,
        bm: Arc<BufferManager>,
        num_rows: u64,
        src_var_idx: usize,
        (min_depth, max_depth): (u32, u32),
        read_ts: u64,
        rel_var_name: String,
    ) -> Self {
        Self {
            child,
            rel_table,
            dst_table,
            bm,
            num_rows,
            src_var_idx,
            bounds: (min_depth, max_depth),
            mask: None,
            read_ts,
            max_traversal_ms: DEFAULT_MAX_TRAVERSAL_MS,
            rel_var_name,
        }
    }
    pub fn with_max_traversal_ms(mut self, ms: u64) -> Self {
        self.max_traversal_ms = ms;
        self
    }
    pub fn with_mask(mut self, mask: Arc<RwLock<super::semi_mask::SemiMask>>) -> Self {
        self.mask = Some(mask);
        self
    }

    /// Return the Arrow data type for a given output column index.
    fn get_output_field_type(&self, chunk_schema: &arrow::datatypes::Schema, idx: usize) -> arrow::datatypes::DataType {
        let num_chunk_cols = chunk_schema.fields().len();
        let num_dst_cols = self.dst_table.columns.len();
        if idx < num_chunk_cols {
            chunk_schema.field(idx).data_type().clone()
        } else if idx < num_chunk_cols + num_dst_cols {
            // Destination node property (b._id, b.id, b.name, ...)
            let prop_idx = idx - num_chunk_cols;
            crate::processor::arrow_utils::logical_type_to_arrow_type(&self.dst_table.columns[prop_idx].data_type)
        } else if idx == num_chunk_cols + num_dst_cols {
            // Path length (depth) — named after rel_var_name
            arrow::datatypes::DataType::Int64
        } else {
            // Neighbor ID (internal node ID)
            arrow::datatypes::DataType::UInt64
        }
    }
}

impl PhysicalOperator for PhysicalRecursiveJoin {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        loop {
            let chunk = match self.child.get_next(database, tx, params)? {
                Some(c) => c,
                None => return Ok(None),
            };
            let num_dst_cols = self.dst_table.columns.len();
            let num_chunk_cols = chunk.batch.num_columns();
            // Output columns: [child cols] + [dst_props (_id,id,name...)] + [path_length] + [_neighbor]
            // dst_props are placed directly after child columns so that the `b` variable's
            // column range ([num_chunk_cols .. num_chunk_cols + num_dst_cols]) matches what
            // collect_variable_positions expects: b starts at child_cols and has num_dst_cols columns.
            // This ensures b._id → index 0, b.id → index 1, b.name → index 2, etc.
            let mut final_columns: Vec<Vec<Value>> =
                vec![Vec::new(); num_chunk_cols + num_dst_cols + 2];

            // Adjacency lookup mechanism
            // Clone the CSR Arc and drop the storage lock immediately so writers
            // are not blocked during the entire BFS traversal.
            let csr: Option<Arc<crate::storage::index::csr::CSRIndex>> = {
                let storage = database.storage_manager.read();
                storage.ensure_csr_fresh(&self.rel_table.name, &self.bm, tx)?;
                storage.fwd_csr.get(&self.rel_table.name).cloned()
            };

            let mut fallback_adj: Option<std::collections::HashMap<u64, Vec<(u64, u64)>>> = None;

            let deadline = Instant::now() + std::time::Duration::from_millis(self.max_traversal_ms);
            for i in 0..chunk.batch.num_rows() {
                let start_node = Value::from_arrow(chunk.batch.column(self.src_var_idx), i);
                let start_id = match start_node {
                    Value::Node(id) => id,
                    Value::Number(n) => n as u64,
                    _ => continue,
                };

                let mut visited_nodes: HashSet<(u64, u32)> = HashSet::new();
                let mut visited_edges: HashSet<(u64, u64, u64)> = HashSet::new();
                let mut queue = VecDeque::new();
                queue.push_back((start_id, 0u32));
                visited_nodes.insert((start_id, 0));

                while let Some((node_id, depth)) = queue.pop_front() {
                    if Instant::now() > deadline {
                        return Err(LightningError::Internal(format!(
                            "Relationship traversal timed out after {}ms",
                            self.max_traversal_ms,
                        )));
                    }
                    if depth > self.bounds.1 {
                        continue;
                    }
                    if depth >= self.bounds.0 && depth > 0 {
                        for (col_idx, fc) in final_columns.iter_mut().enumerate().take(num_chunk_cols) {
                            fc.push(Value::from_arrow(chunk.batch.column(col_idx), i));
                        }
                        // Destination node properties — placed right after child columns.
                        // This range [num_chunk_cols .. num_chunk_cols + num_dst_cols] represents
                        // the `b` variable in the RETURN clause.
                        let dst_base = num_chunk_cols;
                        for (prop_idx, col) in self.dst_table.columns.iter().enumerate() {
                            let val = col.get_value(&self.bm, node_id, tx)?;
                            final_columns[dst_base + prop_idx].push(val);
                        }
                        // Path length (depth) — after dst properties
                        let depth_idx = num_chunk_cols + num_dst_cols;
                        final_columns[depth_idx].push(Value::Number(depth as f64));
                        // Neighbor ID (internal node ID) — last column
                        let neighbor_idx = num_chunk_cols + num_dst_cols + 1;
                        final_columns[neighbor_idx].push(Value::Node(node_id));
                    }

                    if depth < self.bounds.1 {
                        if let Some(ref index) = csr {
                            let neighbors = index.get_neighbors(&self.bm, node_id, tx)?;
                            for neighbor_id in neighbors {
                                if visited_nodes.contains(&(neighbor_id, depth + 1)) {
                                    continue;
                                }
                                let edge_key = (node_id, neighbor_id, depth as u64 + 1);
                                if visited_edges.contains(&edge_key) {
                                    continue;
                                }
                                visited_nodes.insert((neighbor_id, depth + 1));
                                visited_edges.insert(edge_key);
                                queue.push_back((neighbor_id, depth + 1));
                            }
                        } else {
                            if fallback_adj.is_none() {
                                let src_col = &self.rel_table.columns[0];
                                let dst_col = &self.rel_table.columns[1];
                                let total_rels = self.rel_table.stats.read().cardinality;
                                // Limit fallback scan to prevent OOM on large graphs
                                const MAX_FALLBACK_RELS: u64 = 1_000_000;
                                let scan_limit = total_rels.min(MAX_FALLBACK_RELS);
                                if total_rels > MAX_FALLBACK_RELS {
                                    tracing::warn!(
                                        "Recursive join fallback: {} relationships exceeds limit {}, scanning first {} only",
                                        total_rels, MAX_FALLBACK_RELS, MAX_FALLBACK_RELS
                                    );
                                }
                                let mut map: std::collections::HashMap<u64, Vec<(u64, u64)>> = std::collections::HashMap::new();
                                for r_idx in 0..scan_limit {
                                    let Ok(s_val) = src_col.get_value(&self.bm, r_idx, tx) else { continue };
                                    let Ok(d_val) = dst_col.get_value(&self.bm, r_idx, tx) else { continue };
                                    if let (Value::Node(s_id), Value::Node(d_id)) = (s_val, d_val) {
                                        map.entry(s_id).or_default().push((d_id, r_idx));
                                    }
                                }
                                fallback_adj = Some(map);
                            }
                            let adj = fallback_adj.as_ref()
                                .expect("recursive_join: fallback_adj was just set to Some");
                            if let Some(neighbors) = adj.get(&node_id) {
                                for &(neighbor_id, row_id) in neighbors {
                                    if visited_nodes.contains(&(neighbor_id, depth + 1)) {
                                        continue;
                                    }
                                    let edge_key = (node_id, neighbor_id, row_id);
                                    if visited_edges.contains(&edge_key) {
                                        continue;
                                    }
                                    visited_nodes.insert((neighbor_id, depth + 1));
                                    visited_edges.insert(edge_key);
                                    queue.push_back((neighbor_id, depth + 1));
                                }
                            }
                        }
                    }
                }
            }

            if !final_columns[0].is_empty() {
                let mut arrow_cols = Vec::new();
                let chunk_schema = chunk.batch.schema();
                for (idx, col_vals) in final_columns.iter().enumerate() {
                    let ft = self.get_output_field_type(&chunk_schema, idx);
                    arrow_cols.push(crate::processor::arrow_utils::values_to_array(
                        col_vals, &ft,
                    ));
                }

                let mut schema_fields: Vec<Arc<arrow::datatypes::Field>> =
                    chunk_schema.fields().iter().cloned().collect();
                // Destination table properties — these represent the `b` variable.
                // Must come right after child columns so collect_variable_positions
                // correctly maps b._id → 0, b.id → 1, b.name → 2 within b's range.
                for col in &self.dst_table.columns {
                    schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                        &col.name,
                        crate::processor::arrow_utils::logical_type_to_arrow_type(&col.data_type),
                        true,
                    )));
                }
                // Path length column — named after the relationship variable
                schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                    &self.rel_var_name,
                    arrow::datatypes::DataType::Int64,
                    true,
                )));
                // Neighbor ID column (internal node ID for BFS)
                schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                    "_neighbor",
                    arrow::datatypes::DataType::UInt64,
                    true,
                )));

                let schema = Arc::new(arrow::datatypes::Schema::new(schema_fields));
                let batch = arrow::record_batch::RecordBatch::try_new(schema, arrow_cols)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                return Ok(Some(DataChunk { batch }));
            }
        }
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            rel_table: self.rel_table.clone(),
            dst_table: self.dst_table.clone(),
            bm: Arc::clone(&self.bm),
            num_rows: self.num_rows,
            src_var_idx: self.src_var_idx,
            bounds: self.bounds,
            mask: self.mask.clone(),
            read_ts: self.read_ts,
            max_traversal_ms: self.max_traversal_ms,
            rel_var_name: self.rel_var_name.clone(),
        })
    }
}
