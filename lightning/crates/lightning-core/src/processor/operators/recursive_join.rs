use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::storage_manager::Table;
use crate::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

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
        }
    }
    pub fn with_mask(mut self, mask: Arc<RwLock<super::semi_mask::SemiMask>>) -> Self {
        self.mask = Some(mask);
        self
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
            let mut final_columns: Vec<Vec<Value>> =
                vec![Vec::new(); chunk.batch.num_columns() + 1 + self.dst_table.columns.len() - 1];

            // Adjacency lookup mechanism
            let storage = database.storage_manager.read();
            storage.ensure_csr_fresh(&self.rel_table.name, &self.bm, tx)?;
            let csr = storage.fwd_csr.get(&self.rel_table.name);

            for i in 0..chunk.batch.num_rows() {
                let start_node = Value::from_arrow(chunk.batch.column(self.src_var_idx), i);
                let start_id = match start_node {
                    Value::Node(id) => id,
                    _ => continue,
                };

                let mut visited = HashSet::new();
                let mut queue = VecDeque::new();
                queue.push_back((start_id, 0u32));
                visited.insert(start_id);

                while let Some((node_id, depth)) = queue.pop_front() {
                    if depth > self.bounds.1 {
                        continue;
                    }
                    if depth >= self.bounds.0 && depth > 0 {
                        // Add result: Duplicate child row + this neighbor
                        for col_idx in 0..chunk.batch.num_columns() {
                            final_columns[col_idx]
                                .push(Value::from_arrow(chunk.batch.column(col_idx), i));
                        }
                        final_columns[chunk.batch.num_columns()].push(Value::Node(node_id));
                        // Add properties of the destination node
                        for (prop_idx, col) in self.dst_table.columns[1..].iter().enumerate() {
                            let val = col.get_value(&self.bm, node_id, tx)?;
                            final_columns[chunk.batch.num_columns() + 1 + prop_idx].push(val);
                        }
                    }

                    if depth < self.bounds.1 {
                        if let Some(index) = csr {
                            let neighbors = index.get_neighbors(&self.bm, node_id, tx)?;
                            for neighbor_id in neighbors {
                                if !visited.contains(&neighbor_id) {
                                    visited.insert(neighbor_id);
                                    queue.push_back((neighbor_id, depth + 1));
                                }
                            }
                        } else {
                            // Correct Fallback: Scan the relationship table for this node_id
                            let src_col = &self.rel_table.columns[0]; // _src
                            let dst_col = &self.rel_table.columns[1]; // _dst
                            let total_rels = self.rel_table.stats.read().cardinality;
                            for r_idx in 0..total_rels {
                                let s_val = src_col.get_value(&self.bm, r_idx, tx)?;
                                let d_val = dst_col.get_value(&self.bm, r_idx, tx)?;
                                if let Value::Node(s_id) = s_val {
                                    if s_id == node_id {
                                        if let Value::Node(d_id) = d_val {
                                            if !visited.contains(&d_id) {
                                                visited.insert(d_id);
                                                queue.push_back((d_id, depth + 1));
                                            }
                                        }
                                    }
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
                    let data_type = if idx < chunk.batch.num_columns() {
                        chunk_schema.field(idx).data_type()
                    } else if idx == chunk.batch.num_columns() {
                        &arrow::datatypes::DataType::UInt64
                    } else {
                        // Property of dst_table
                        let prop_idx = idx - chunk.batch.num_columns() - 1;
                        let logical_t = &self.dst_table.columns[prop_idx + 1].data_type; // skip _id
                        &crate::processor::arrow_utils::logical_type_to_arrow_type(logical_t)
                    };
                    arrow_cols.push(crate::processor::arrow_utils::values_to_array(
                        col_vals, data_type,
                    ));
                }

                let mut schema_fields: Vec<Arc<arrow::datatypes::Field>> =
                    chunk_schema.fields().iter().cloned().collect();
                schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                    "neighbor_id",
                    arrow::datatypes::DataType::UInt64,
                    true,
                )));
                for col in &self.dst_table.columns[1..] {
                    schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                        &col.name,
                        crate::processor::arrow_utils::logical_type_to_arrow_type(&col.data_type),
                        true,
                    )));
                }

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
        })
    }
}
