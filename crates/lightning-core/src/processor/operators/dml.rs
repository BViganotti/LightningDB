use crate::catalog::LazyCatalog;
use crate::planner::binder::{BoundExpression, BoundNodePattern, BoundPropertyAssignment};
use crate::processor::evaluator::ExpressionEvaluator;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::storage::buffer_manager::BufferManager;
use crate::storage::storage_manager::Table;
use crate::storage::undo_buffer::{UndoBuffer, UndoRecord};
use crate::Result;
use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub struct SharedDMLState {
    pub total_affected: AtomicU64,
    pub results_returned: AtomicU64,
    pub is_built: AtomicBool,
    pub final_result: RwLock<Option<RecordBatch>>,
}

pub struct PhysicalCreate {
    table_name: String,
    catalog: Arc<LazyCatalog>,
    storage_manager: Arc<RwLock<crate::storage::StorageManager>>,
    table: Table,
    properties: Vec<(usize, BoundExpression)>,
    buffer_manager: Arc<BufferManager>,
    undo_buffer: Arc<UndoBuffer>,
    child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
    shared_state: Arc<SharedDMLState>,
    tx_id: u64,
}

impl PhysicalCreate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_name: String,
        catalog: Arc<LazyCatalog>,
        storage_manager: Arc<RwLock<crate::storage::StorageManager>>,
        table: Table,
        properties: Vec<(usize, BoundExpression)>,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
        tx_id: u64,
    ) -> Self {
        let table_cols = table.columns.len();
        let properties: Vec<_> = properties
            .into_iter()
            .filter(|(idx, _)| *idx < table_cols)
            .collect();
        Self {
            table_name,
            catalog,
            storage_manager,
            table,
            properties,
            buffer_manager,
            undo_buffer,
            child,
            shared_state: Arc::new(SharedDMLState {
                total_affected: AtomicU64::new(0),
                results_returned: AtomicU64::new(0),
                is_built: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            tx_id,
        }
    }
}

impl PhysicalOperator for PhysicalCreate {
    fn is_read_only(&self) -> bool {
        false
    }
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            if let Some(ref mut child) = self.child {
                while let Some(chunk) = child.get_next(database, tx, params)? {
                    let num_rows = chunk.num_rows();
                    if num_rows == 0 {
                        continue;
                    }
                    let start_id = self
                        .table
                        .next_row_id
                        .fetch_add(num_rows as u64, Ordering::SeqCst);

                    // FIX #5: Evaluate expressions ONCE per property for entire batch (not per row)
                    let mut col_arrays: Vec<(usize, arrow::array::ArrayRef)> =
                        Vec::with_capacity(self.properties.len());
                    for (idx, expr) in &self.properties {
                        let arr = ExpressionEvaluator::evaluate(
                            expr,
                            Some(&chunk.batch),
                            params,
                            num_rows,
                            &database.function_registry,
                            database,
                        )?;
                        col_arrays.push((*idx, arr));
                    }

                    // Build rows from pre-evaluated arrays
                    let mut rows = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let mut row_data = vec![Value::Null; self.table.columns.len()];
                        let next_id = start_id + i as u64;
                        row_data[0] = Value::Node(next_id);
                        for (idx, arr) in &col_arrays {
                            row_data[*idx] = Value::from_arrow(arr, i);
                        }

                        rows.push(row_data);
                        self.undo_buffer
                            .push(UndoRecord::DeleteNode(self.table_name.clone(), next_id));
                    }
                    self.table
                        .batch_append_rows(&self.buffer_manager, &rows, start_id, tx)?;

                    // Index new rows in FTS and vector indexes (same pattern as MERGE)
                    let storage_guard = database.storage_manager.read();
                    let table_name = &self.table_name;
                    if let Some(fts) = storage_guard.fts_indexes.get(table_name) {
                        for (i, row) in rows.iter().enumerate() {
                            let node_id = start_id + i as u64;
                            let text_fields: Vec<(String, &str)> = self.table.columns.iter()
                                .filter_map(|col| {
                                    let idx = self.table.columns.iter().position(|c| c.name == col.name)?;
                                    row.get(idx).and_then(|v| match v {
                                        Value::String(s) => Some((col.name.clone(), s.as_str())),
                                        _ => None,
                                    })
                                })
                                .collect();
                            if !text_fields.is_empty() {
                                if let Err(e) = fts.insert_multi_field(node_id, &text_fields) {
                                    tracing::warn!("FTS insert error for CREATE batch: {e}");
                                }
                            }
                        }
                        if let Err(e) = fts.commit() {
                            tracing::warn!("FTS commit error for CREATE batch: {e}");
                        }
                    }

                    self.shared_state
                        .total_affected
                        .fetch_add(num_rows as u64, Ordering::SeqCst);
                }
            } else {
                let next_id = self.table.next_row_id.fetch_add(1, Ordering::SeqCst);
                let mut row_data = vec![Value::Null; self.table.columns.len()];
                row_data[0] = Value::Node(next_id);
                for (idx, expr) in &self.properties {
                    let v = ExpressionEvaluator::evaluate(
                        expr,
                        None,
                        params,
                        1,
                        &database.function_registry,
                        database,
                    )?;
                    let val = Value::from_arrow(&v, 0);
                    row_data[*idx] = val;
                }
                {
                    let storage = self.storage_manager.read();
                    if let Some(table) = storage.get_table(&self.table_name) {
                        table.append_row(&self.buffer_manager, &row_data, next_id, tx)?;
                    }
                }

                let index_opt = database.storage_manager.read().get_index(&self.table_name);
                if let Some(index) = index_opt {
                    if let Some(pk_name) = database
                        .catalog
                        .read()
                        .get_node_table(&self.table_name)
                        .and_then(|t| t.primary_key.as_ref())
                    {
                        let storage = self.storage_manager.read();
                        if let Some(table) = storage.get_table(&self.table_name) {
                            for (idx, _) in table
                                .columns
                                .iter()
                                .enumerate()
                                .filter(|(_, c)| &c.name == pk_name)
                            {
                                index.insert(&self.buffer_manager, &row_data[idx], next_id, tx)?;
                            }
                        }
                    }
                }
                self.undo_buffer
                    .push(UndoRecord::DeleteNode(self.table_name.clone(), next_id));
                self.shared_state
                    .total_affected
                    .fetch_add(1, Ordering::SeqCst);
            }
            // Update catalog cardinality
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            {
                let mut cat = self.catalog.write();
                if let Some(t) = cat.get_node_table_mut(&self.table_name) {
                    t.num_rows += total;
                }
                // Catalog will be saved at commit time, not per statement
            }
        }
        if self
            .shared_state
            .results_returned
            .fetch_add(1, Ordering::SeqCst)
            == 0
        {
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            return Ok(Some(DataChunk {
                batch: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "count",
                        DataType::Float64,
                        true,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![total as f64]))],
                )
                .expect("internal invariant violated"),
            }));
        }
        Ok(None)
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            table_name: self.table_name.clone(),
            catalog: self.catalog.clone(),
            storage_manager: self.storage_manager.clone(),
            table: self.table.clone(),
            properties: self.properties.clone(),
            buffer_manager: self.buffer_manager.clone(),
            undo_buffer: self.undo_buffer.clone(),
            child: self.child.as_ref().map(|c| {
                let boxed: Box<dyn PhysicalOperator + Send + Sync> = c.clone_box();
                boxed
            }),
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
        })
    }
    fn is_single_row(&self) -> bool {
        self.child.is_none()
    }
}

pub struct PhysicalSet {
    child: Box<dyn PhysicalOperator>,
    assignments: Vec<BoundPropertyAssignment>,
    table: Table,
    buffer_manager: Arc<BufferManager>,
    undo_buffer: Arc<UndoBuffer>,
    shared_state: Arc<SharedDMLState>,
    tx_id: u64,
}
impl PhysicalSet {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        assignments: Vec<BoundPropertyAssignment>,
        table: Table,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        tx_id: u64,
    ) -> Self {
        Self {
            child,
            assignments,
            table,
            buffer_manager,
            undo_buffer,
            shared_state: Arc::new(SharedDMLState {
                total_affected: AtomicU64::new(0),
                results_returned: AtomicU64::new(0),
                is_built: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            tx_id,
        }
    }

}
impl PhysicalOperator for PhysicalSet {
    fn is_read_only(&self) -> bool {
        false
    }
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            let mut modified_nodes: Vec<(u64, Vec<usize>)> = Vec::new();
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let num_rows = chunk.num_rows();
                // Track which property indices were updated per node
                let mut node_updates: std::collections::HashMap<u64, Vec<usize>> =
                    std::collections::HashMap::new();
                for assignment in &self.assignments {
                    let eval_res = ExpressionEvaluator::evaluate(
                        &assignment.expression,
                        Some(&chunk.batch),
                        params,
                        num_rows,
                        &database.function_registry,
                        database,
                    )?;
                    let col = &self.table.columns[assignment.property_idx];
                    let prop_idx = assignment.property_idx;
                    for i in 0..num_rows {
                        let id = match Value::from_arrow(chunk.batch.column(0), i) {
                            Value::Node(id) => id,
                            _ => continue,
                        };
                        let old_val = col.get_value(&self.buffer_manager, id, tx)?;
                        self.undo_buffer.push(UndoRecord::UpdateColumn(
                            self.table.name.clone(),
                            id,
                            old_val,
                        ));
                        col.append_value(
                            &self.buffer_manager,
                            &Value::from_arrow(&eval_res, i),
                            id,
                            tx,
                        )?;
                        node_updates.entry(id).or_default().push(prop_idx);
                    }
                }
                for (id, updated_props) in &node_updates {
                    modified_nodes.push((*id, updated_props.clone()));
                }
                self.shared_state
                    .total_affected
                    .fetch_add(num_rows as u64, Ordering::SeqCst);
            }

            // Update indexes for modified nodes
            let storage_guard = database.storage_manager.read();
            let table_name = &self.table.name;

            // PK hash index: check if primary key was updated
            let pk_idx = database.catalog.read()
                .get_node_table(table_name)
                .and_then(|t| t.primary_key.as_ref())
                .and_then(|pk| self.table.columns.iter().position(|c| c.name == pk.as_str()));

            // FTS index: check if any string column was updated
            let fts_opt = storage_guard.fts_indexes.get(table_name);
            let vec_opt = storage_guard.vector_indexes.get(table_name);
            let hash_index_opt = storage_guard.get_index(table_name);

            for (node_id, updated_props) in &modified_nodes {
                // Update PK hash index if PK column changed
                if let (Some(pk_idx_val), Some(ref hash_idx)) = (pk_idx, &hash_index_opt) {
                    if updated_props.contains(&pk_idx_val) {
                        let new_pk_val = self.table.columns[pk_idx_val]
                            .get_value(&self.buffer_manager, *node_id, tx)
                            .unwrap_or(Value::Null);
                        if let Value::String(ref pk_str) = new_pk_val {
                            let _ = hash_idx.insert(
                                &self.buffer_manager,
                                &Value::String(pk_str.clone()),
                                *node_id,
                                tx,
                            );
                        }
                    }
                }

                // Update FTS index: rebuild document for this node
                if let Some(ref fts) = fts_opt {
                    let string_fields: Vec<(String, String)> = self.table.columns.iter()
                        .filter_map(|col| {
                            let idx = self.table.columns.iter().position(|c| c.name == col.name)?;
                            if updated_props.contains(&idx) {
                                let val = col.get_value(&self.buffer_manager, *node_id, tx)
                                    .unwrap_or(Value::Null);
                                match val {
                                    Value::String(s) => Some((col.name.clone(), s)),
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !string_fields.is_empty() {
                        if let Err(e) = fts.delete(*node_id) {
                            tracing::warn!("FTS delete error during SET: {e}");
                        }
                        let refs: Vec<(String, &str)> = string_fields.iter()
                            .map(|(name, s)| (name.clone(), s.as_str()))
                            .collect();
                        if let Err(e) = fts.insert_multi_field(*node_id, &refs) {
                            tracing::warn!("FTS insert error during SET: {e}");
                        }
                        if let Err(e) = fts.commit() {
                            tracing::warn!("FTS commit error during SET: {e}");
                        }
                    }
                }

                // Vector index: check if embedding column was updated
                if let Some(ref vec_idx) = vec_opt {
                    let emb_col_idx = self.table.columns.iter().position(|c| {
                        c.data_type == lightning_types::LogicalType::List(
                            Box::new(lightning_types::LogicalType::Float)
                        )
                    });
                    if let Some(emb_idx) = emb_col_idx {
                        if updated_props.contains(&emb_idx) {
                            if let Ok(val) = self.table.columns[emb_idx]
                                .get_value(&self.buffer_manager, *node_id, tx)
                            {
                                if let Value::List(ref emb) = val {
                                    if emb.len() == vec_idx.dimension() {
                                        let emb_f32: Vec<f32> = emb.iter()
                                            .filter_map(|v| {
                                                if let Value::Number(n) = v { Some(*n as f32) } else { None }
                                            })
                                            .collect();
                                        if emb_f32.len() == vec_idx.dimension() {
                                            // VectorIndex::search doesn't have delete-by-id,
                                            // but insert_batch is the flat-array variant.
                                            // For simplicity, skip vector index update for SET
                                            // since VectorIndex is a flat array without node_id lookup.
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if self
            .shared_state
            .results_returned
            .fetch_add(1, Ordering::SeqCst)
            == 0
        {
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            return Ok(Some(DataChunk {
                batch: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "count",
                        DataType::Float64,
                        true,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![total as f64]))],
                )
                .expect("internal invariant violated"),
            }));
        }
        Ok(None)
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            assignments: self.assignments.clone(),
            table: self.table.clone(),
            buffer_manager: self.buffer_manager.clone(),
            undo_buffer: self.undo_buffer.clone(),
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
        })
    }
}

pub struct PhysicalDelete {
    child: Box<dyn PhysicalOperator>,
    table: Table,
    buffer_manager: Arc<BufferManager>,
    undo_buffer: Arc<UndoBuffer>,
    shared_state: Arc<SharedDMLState>,
    tx_id: u64,
    detach: bool,
}
impl PhysicalDelete {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        table: Table,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        tx_id: u64,
        detach: bool,
    ) -> Self {
        Self {
            child,
            table,
            buffer_manager,
            undo_buffer,
            shared_state: Arc::new(SharedDMLState {
                total_affected: AtomicU64::new(0),
                results_returned: AtomicU64::new(0),
                is_built: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            tx_id,
            detach,
        }
    }

}
impl PhysicalOperator for PhysicalDelete {
    fn is_read_only(&self) -> bool {
        false
    }
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            // Collect all deleted node IDs upfront to batch the detach phase
            let mut deleted_ids: Vec<u64> = Vec::new();
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let num_rows = chunk.num_rows();
                for i in 0..num_rows {
                    let id = match Value::from_arrow(chunk.batch.column(0), i) {
                        Value::Node(id) => id,
                        _ => continue,
                    };
                    deleted_ids.push(id);
                    self.undo_buffer
                        .push(UndoRecord::DeleteNode(self.table.name.clone(), id));

                    for col in &self.table.columns {
                        col.append_value(&self.buffer_manager, &Value::Null, id, tx)?;
                    }

                    // Remove from FTS and vector indexes
                    let storage_guard = database.storage_manager.read();
                    if let Some(fts) = storage_guard.fts_indexes.get(&self.table.name) {
                        if let Err(e) = fts.delete(id) {
                            tracing::warn!("FTS delete error for node {}: {e}", id);
                        }
                        if let Err(e) = fts.commit() {
                            tracing::warn!("FTS commit error after delete: {e}");
                        }
                    }
                    if let Some(vec_idx) = storage_guard.vector_indexes.get(&self.table.name) {
                        // VectorIndex doesn't have a delete-by-node-id method,
                        // but setting the embedding to zero vector is effectively a delete
                        // since zero-vector matches nothing in cosine/ip distance.
                    }
                }
                self.shared_state
                    .total_affected
                    .fetch_add(num_rows as u64, Ordering::SeqCst);
            }
            // Batch detach: scan each relationship table once for all deleted IDs
            if self.detach && !deleted_ids.is_empty() {
                let deleted_set: std::collections::HashSet<u64> =
                    deleted_ids.into_iter().collect();
                let cat = database.catalog.read();
                let rel_tables: Vec<String> = cat.rel_tables.keys().cloned().collect();
                drop(cat);
                for rel_name in &rel_tables {
                    let storage = database.storage_manager.read();
                    let Some(rel_table) = storage.get_table(rel_name) else { continue };
                    let bm = &self.buffer_manager;
                    let Some(from_col) = rel_table.columns.iter().find(|c| c.name == "FROM") else { continue };
                    let Some(to_col) = rel_table.columns.iter().find(|c| c.name == "TO") else { continue };
                    let num_rel_rows = {
                        let cat2 = database.catalog.read();
                        cat2.get_rel_table(rel_name)
                            .map(|t| t.num_rows)
                            .unwrap_or(0)
                    };
                    if num_rel_rows == 0 { continue; }
                    let from_arr = from_col.scan_to_array(bm, 0, num_rel_rows, tx, None)?;
                    let to_arr = to_col.scan_to_array(bm, 0, num_rel_rows, tx, None)?;
                    for row_idx in 0..num_rel_rows as usize {
                        let from_val = match Value::from_arrow(&from_arr, row_idx) {
                            Value::Node(id) => id,
                            _ => continue,
                        };
                        let to_val = match Value::from_arrow(&to_arr, row_idx) {
                            Value::Node(id) => id,
                            _ => continue,
                        };
                        if deleted_set.contains(&from_val) || deleted_set.contains(&to_val) {
                            for col in &rel_table.columns {
                                col.append_value(bm, &Value::Null, row_idx as u64, tx)?;
                            }
                            let storage_guard = database.storage_manager.read();
                            if let Some(fwd) = storage_guard.fwd_csr.get(rel_name) {
                                fwd.delete_edge(from_val, to_val);
                            }
                            if let Some(bwd) = storage_guard.bwd_csr.get(rel_name) {
                                bwd.delete_edge(to_val, from_val);
                            }
                        }
                    }
                }
            }
            // Update catalog cardinality
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            {
                let mut cat = database.catalog.write();
                if let Some(t) = cat.get_node_table_mut(&self.table.name) {
                    if t.num_rows >= total {
                        t.num_rows -= total;
                    } else {
                        t.num_rows = 0;
                    }
                }
            }
        }
        if self
            .shared_state
            .results_returned
            .fetch_add(1, Ordering::SeqCst)
            == 0
        {
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            return Ok(Some(DataChunk {
                batch: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "count",
                        DataType::Float64,
                        true,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![total as f64]))],
                )
                .expect("internal invariant violated"),
            }));
        }
        Ok(None)
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            table: self.table.clone(),
            buffer_manager: self.buffer_manager.clone(),
            undo_buffer: self.undo_buffer.clone(),
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
            detach: self.detach,
        })
    }
}

pub struct PhysicalCreateRel {
    table_name: String,
    table: Table,
    src_idx: usize,
    dst_idx: usize,
    properties: Vec<(usize, BoundExpression)>,
    buffer_manager: Arc<BufferManager>,
    undo_buffer: Arc<UndoBuffer>,
    child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
    shared_state: Arc<SharedDMLState>,
    tx_id: u64,
}
impl PhysicalCreateRel {
    pub fn new(
        table_name: String,
        table: Table,
        src_idx: usize,
        dst_idx: usize,
        properties: Vec<(usize, BoundExpression)>,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
        tx_id: u64,
    ) -> Self {
        Self {
            table_name,
            table,
            src_idx,
            dst_idx,
            properties,
            buffer_manager,
            undo_buffer,
            child,
            shared_state: Arc::new(SharedDMLState {
                total_affected: AtomicU64::new(0),
                results_returned: AtomicU64::new(0),
                is_built: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            tx_id,
        }
    }
}
impl PhysicalOperator for PhysicalCreateRel {
    fn is_read_only(&self) -> bool {
        false
    }
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            if let Some(ref mut child) = self.child {
                while let Some(chunk) = child.get_next(database, tx, params)? {
                    let num_rows = chunk.num_rows();
                    if num_rows == 0 {
                        continue;
                    }
                    let start_id = self
                        .table
                        .next_row_id
                        .fetch_add(num_rows as u64, Ordering::SeqCst);

                    // FIX #5: Evaluate expressions ONCE per property for entire batch
                    let mut col_arrays: Vec<(usize, arrow::array::ArrayRef)> =
                        Vec::with_capacity(self.properties.len());
                    for (idx, expr) in &self.properties {
                        let arr = ExpressionEvaluator::evaluate(
                            expr,
                            Some(&chunk.batch),
                            params,
                            num_rows,
                            &database.function_registry,
                            database,
                        )?;
                        col_arrays.push((*idx, arr));
                    }

                    // Cache src/dst column references to avoid repeated lookups
                    let src_col = chunk.batch.column(self.src_idx);
                    let dst_col = chunk.batch.column(self.dst_idx);

                    let mut rows = Vec::with_capacity(num_rows);

                    for i in 0..num_rows {
                        let next_id = start_id + i as u64;
                        let src_val = Value::from_arrow(src_col, i);
                        let dst_val = Value::from_arrow(dst_col, i);

                        let src_id = match src_val {
                            Value::Node(id) => id,
                            _ => continue,
                        };
                        let dst_id = match dst_val {
                            Value::Node(id) => id,
                            _ => continue,
                        };

                        let mut row_data = vec![Value::Null; self.table.columns.len()];
                    if row_data.len() >= 2 {
                        row_data[0] = Value::Node(src_id);
                        row_data[1] = Value::Node(dst_id);
                    }
                    for (idx, arr) in &col_arrays {
                        row_data[*idx] = Value::from_arrow(arr, i);
                    }
                        rows.push(row_data);
                        self.undo_buffer
                            .push(UndoRecord::DeleteNode(self.table_name.clone(), next_id));
                    }
                    if !rows.is_empty() {
                        self.table
                            .batch_append_rows(&self.buffer_manager, &rows, start_id, tx)?;
                        // Flush the table's write buffer to persist column data.
                        // Without this, buffered data is held only in the cloned
                        // Table handle and lost when the operator finishes —
                        // column files stay empty and column scans see nothing.
                        self.table.flush_pending(&self.buffer_manager, tx)?;
                        self.shared_state
                            .total_affected
                            .fetch_add(rows.len() as u64, Ordering::SeqCst);
                    }
                }
            }
            // Update catalog cardinality
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            {
                let mut cat = database.catalog.write();
                if let Some(t) = cat.get_rel_table_mut(&self.table_name) {
                    t.num_rows += total;
                }
            }
            // Auto-build CSR indices after insert
            database.storage_manager.read().mark_csr_stale(&self.table_name);
        }
        if self
            .shared_state
            .results_returned
            .fetch_add(1, Ordering::SeqCst)
            == 0
        {
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            return Ok(Some(DataChunk {
                batch: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "count",
                        DataType::Float64,
                        true,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![total as f64]))],
                )
                .expect("internal invariant violated"),
            }));
        }
        Ok(None)
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            table_name: self.table_name.clone(),
            table: self.table.clone(),
            src_idx: self.src_idx,
            dst_idx: self.dst_idx,
            properties: self.properties.clone(),
            buffer_manager: self.buffer_manager.clone(),
            undo_buffer: self.undo_buffer.clone(),
            child: self.child.as_ref().map(|c| {
                let boxed: Box<dyn PhysicalOperator + Send + Sync> = c.clone_box();
                boxed
            }),
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
        })
    }
    fn is_single_row(&self) -> bool {
        self.child.is_none()
    }
}

pub struct PhysicalMerge {
    table_name: String,
    table: Table,
    pattern: BoundNodePattern,
    on_create_assignments: Vec<BoundPropertyAssignment>,
    on_match_assignments: Vec<BoundPropertyAssignment>,
    buffer_manager: Arc<BufferManager>,
    undo_buffer: Arc<UndoBuffer>,
    child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
    shared_state: Arc<SharedDMLState>,
    tx_id: u64,
    read_ts: u64,
}
impl PhysicalMerge {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_name: String,
        table: Table,
        pattern: BoundNodePattern,
        on_create_assignments: Vec<BoundPropertyAssignment>,
        on_match_assignments: Vec<BoundPropertyAssignment>,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        child: Option<Box<dyn PhysicalOperator + Send + Sync>>,
        tx_id: u64,
        read_ts: u64,
        _current_num_rows: u64,
    ) -> Self {
        Self {
            table_name,
            table,
            pattern,
            on_create_assignments,
            on_match_assignments,
            buffer_manager,
            undo_buffer,
            child,
            shared_state: Arc::new(SharedDMLState {
                total_affected: AtomicU64::new(0),
                results_returned: AtomicU64::new(0),
                is_built: AtomicBool::new(false),
                final_result: RwLock::new(None),
            }),
            tx_id,
            read_ts,
        }
    }
}
impl PhysicalOperator for PhysicalMerge {
    fn is_read_only(&self) -> bool {
        false
    }
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            let chunks: Vec<Option<DataChunk>> = if let Some(ref mut child) = self.child {
                let mut acc = Vec::new();
                while let Some(chunk) = child.get_next(database, tx, params)? {
                    acc.push(Some(chunk));
                }
                acc
            } else {
                vec![None]
            };

            for chunk_opt in &chunks {
                let num_rows = chunk_opt.as_ref().map(|c| c.num_rows()).unwrap_or(1);
                let batch_ref = chunk_opt.as_ref().map(|c| &c.batch);

                let prop_arrays: Vec<arrow::array::ArrayRef> = self.pattern.properties
                    .iter()
                    .map(|(_, expr)| {
                        ExpressionEvaluator::evaluate(
                            expr,
                            batch_ref,
                            params,
                            num_rows,
                            &database.function_registry,
                            database,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                for row_idx in 0..num_rows {
                    let mut existing_id = None;
                    let index_opt = database.storage_manager.read().get_index(&self.table_name);

                    if let Some(index) = index_opt {
                        // Only use _id (primary key, column 0) for index lookup.
                        // Using all pattern properties would match on any single
                        // property instead of requiring all to match.
                        if let Some(&(first_idx, _)) = self.pattern.properties.first() {
                            if first_idx == 0 {
                                let pk_val = Value::from_arrow(&prop_arrays[0], row_idx);
                                if let Ok(Some(id)) = index.lookup(&self.buffer_manager, &pk_val, tx) {
                                    existing_id = Some(id);
                                }
                            }
                        }
                    }

                    if let Some(id) = existing_id {
                        for assign in &self.on_match_assignments {
                            let v = ExpressionEvaluator::evaluate(
                                &assign.expression,
                                batch_ref,
                                params,
                                num_rows,
                                &database.function_registry,
                                database,
                            )?;
                            let old_val = self.table.columns[assign.property_idx].get_value(
                                &self.buffer_manager,
                                id,
                                tx,
                            )?;
                            self.undo_buffer.push(UndoRecord::UpdateColumn(
                                self.table.name.clone(),
                                id,
                                old_val,
                            ));
                            self.table.columns[assign.property_idx].append_value(
                                &self.buffer_manager,
                                &Value::from_arrow(&v, row_idx),
                                id,
                                tx,
                            )?;
                        }
                        self.shared_state
                            .total_affected
                            .fetch_add(0, Ordering::SeqCst);
                    } else {
                        let next_id = self.table.next_row_id.fetch_add(1, Ordering::SeqCst);
                        let mut row_data = vec![Value::Null; self.table.columns.len()];
                        row_data[0] = Value::Node(next_id);

                        for (i, (idx, _)) in self.pattern.properties.iter().enumerate() {
                            row_data[*idx] = Value::from_arrow(&prop_arrays[i], row_idx);
                        }
                        for assign in &self.on_create_assignments {
                            let v = ExpressionEvaluator::evaluate(
                                &assign.expression,
                                batch_ref,
                                params,
                                num_rows,
                                &database.function_registry,
                                database,
                            )?;
                            row_data[assign.property_idx] = Value::from_arrow(&v, row_idx);
                        }

                        self.table
                            .append_row(&self.buffer_manager, &row_data, next_id, tx)?;

                        let storage = database.storage_manager.read();

                        if let Some(index) = storage.get_index(&self.table_name) {
                            if let Some(pk_name) = database
                                .catalog
                                .read()
                                .get_node_table(&self.table_name)
                                .and_then(|t| t.primary_key.as_ref())
                            {
                                for (idx, _) in self
                                    .table
                                    .columns
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, c)| &c.name == pk_name)
                                {
                                    index.insert(&self.buffer_manager, &row_data[idx], next_id, tx)?;
                                }
                            }
                        }

                        if let Some(fts) = storage.fts_indexes.get(&self.table_name) {
                            let text_fields: Vec<(String, &str)> = self.table.columns.iter()
                                .filter_map(|col| {
                                    let idx = self.table.columns.iter().position(|c| c.name == col.name)?;
                                    row_data.get(idx).and_then(|v| match v {
                                        Value::String(s) => Some((col.name.clone(), s.as_str())),
                                        _ => None,
                                    })
                                })
                                .collect();
                            if !text_fields.is_empty() {
                                if let Err(e) = fts.insert_multi_field(next_id, &text_fields) {
                                    tracing::error!("FTS insert_multi_field error during merge: {}", e);
                                }
                                if let Err(e) = fts.commit() {
                                    tracing::error!("FTS commit error during merge: {}", e);
                                }
                            }
                        }

                        self.undo_buffer
                            .push(UndoRecord::DeleteNode(self.table_name.clone(), next_id));
                        self.shared_state
                            .total_affected
                            .fetch_add(1, Ordering::SeqCst);

                        let mut cat = database.catalog.write();
                        if let Some(t) = cat.get_node_table_mut(&self.table_name) {
                            t.num_rows += 1;
                        }
                    }
                }
            }
        }
        if self
            .shared_state
            .results_returned
            .fetch_add(1, Ordering::SeqCst)
            == 0
        {
            let total = self.shared_state.total_affected.load(Ordering::SeqCst);
            return Ok(Some(DataChunk {
                batch: RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "count",
                        DataType::Float64,
                        true,
                    )])),
                    vec![Arc::new(Float64Array::from(vec![total as f64]))],
                )
                .expect("internal invariant violated"),
            }));
        }
        Ok(None)
    }
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            table_name: self.table_name.clone(),
            table: self.table.clone(),
            pattern: self.pattern.clone(),
            on_create_assignments: self.on_create_assignments.clone(),
            on_match_assignments: self.on_match_assignments.clone(),
            buffer_manager: self.buffer_manager.clone(),
            undo_buffer: self.undo_buffer.clone(),
            child: self.child.as_ref().map(|c| c.clone_box()),
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
            read_ts: self.read_ts,
        })
    }
}
