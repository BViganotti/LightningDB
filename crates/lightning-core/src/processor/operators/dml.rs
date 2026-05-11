use crate::catalog::{Catalog, LazyCatalog};
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
use std::path::PathBuf;
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
                .unwrap(),
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
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let num_rows = chunk.num_rows();
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
                    }
                }
                self.shared_state
                    .total_affected
                    .fetch_add(num_rows as u64, Ordering::SeqCst);
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
                .unwrap(),
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
}
impl PhysicalDelete {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        table: Table,
        buffer_manager: Arc<BufferManager>,
        undo_buffer: Arc<UndoBuffer>,
        tx_id: u64,
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
        }
    }
}
impl PhysicalOperator for PhysicalDelete {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            while let Some(chunk) = self.child.get_next(database, tx, params)? {
                let num_rows = chunk.num_rows();
                for i in 0..num_rows {
                    let id = match Value::from_arrow(chunk.batch.column(0), i) {
                        Value::Node(id) => id,
                        _ => continue,
                    };
                    self.undo_buffer
                        .push(UndoRecord::DeleteNode(self.table.name.clone(), id));
                    for col in &self.table.columns {
                        col.append_value(&self.buffer_manager, &Value::Null, id, tx)?;
                    }
                }
                self.shared_state
                    .total_affected
                    .fetch_add(num_rows as u64, Ordering::SeqCst);
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
                .unwrap(),
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
            database.storage_manager.read().rebuild_csr(
                &self.table_name,
                &self.buffer_manager,
                tx,
            )?;
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
                .unwrap(),
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
        tx_id: u64,
        read_ts: u64,
        current_num_rows: u64,
    ) -> Self {
        Self {
            table_name,
            table,
            pattern,
            on_create_assignments,
            on_match_assignments,
            buffer_manager,
            undo_buffer,
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
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        if !self.shared_state.is_built.swap(true, Ordering::SeqCst) {
            // 1. Try to find existing node
            let mut existing_id = None;
            let index_opt = database.storage_manager.read().get_index(&self.table_name);

            // Check if we can do an index lookup based on the pattern properties
            if let Some(index) = index_opt {
                for (idx, expr) in &self.pattern.properties {
                    if let Ok(val_array) = ExpressionEvaluator::evaluate(
                        expr,
                        None,
                        params,
                        1,
                        &database.function_registry,
                        database,
                    ) {
                        let pk_val = Value::from_arrow(&val_array, 0);
                        if let Ok(Some(id)) = index.lookup(&self.buffer_manager, &pk_val, tx) {
                            existing_id = Some(id);
                            break;
                        }
                    }
                }
            }

            if let Some(id) = existing_id {
                // MATCH case: Apply on_match_assignments
                for assign in &self.on_match_assignments {
                    let v = ExpressionEvaluator::evaluate(
                        &assign.expression,
                        None,
                        params,
                        1,
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
                        &Value::from_arrow(&v, 0),
                        id,
                        tx,
                    )?;
                }
                self.shared_state
                    .total_affected
                    .fetch_add(0, Ordering::SeqCst);
            } else {
                // CREATE case
                let next_id = self.table.next_row_id.fetch_add(1, Ordering::SeqCst);
                let mut row_data = vec![Value::Null; self.table.columns.len()];
                row_data[0] = Value::Node(next_id);

                for (idx, expr) in &self.pattern.properties {
                    let v = ExpressionEvaluator::evaluate(
                        expr,
                        None,
                        params,
                        1,
                        &database.function_registry,
                        database,
                    )?;
                    row_data[*idx] = Value::from_arrow(&v, 0);
                }
                for assign in &self.on_create_assignments {
                    let v = ExpressionEvaluator::evaluate(
                        &assign.expression,
                        None,
                        params,
                        1,
                        &database.function_registry,
                        database,
                    )?;
                    row_data[assign.property_idx] = Value::from_arrow(&v, 0);
                }

                self.table
                    .append_row(&self.buffer_manager, &row_data, next_id, tx)?;

                let storage = database.storage_manager.read();

                // 1. Primary Key Index
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

                // 2. Full-Text Search Index (Multi-field)
                if let Some(fts) = storage.fts_indexes.get(&self.table_name) {
                    let mut name = "";
                    let mut path = "";
                    let mut doc = "";
                    let mut sig = "";
                    for (i, col) in self.table.columns.iter().enumerate() {
                        match col.name.as_str() {
                            "name" => {
                                if let Value::String(s) = &row_data[i] {
                                    name = s;
                                }
                            }
                            "file_path" => {
                                if let Value::String(s) = &row_data[i] {
                                    path = s;
                                }
                            }
                            "docstring" => {
                                if let Value::String(s) = &row_data[i] {
                                    doc = s;
                                }
                            }
                            "signature" => {
                                if let Value::String(s) = &row_data[i] {
                                    sig = s;
                                }
                            }
                            _ => {}
                        }
                    }
                    let _ = fts.insert_node_fts(next_id, name, path, doc, sig);
                    let _ = fts.commit();
                }

                // 3. Vector Index
                if let Some(vec_idx) = storage.vector_indexes.get(&self.table_name) {
                    for (i, col) in self.table.columns.iter().enumerate() {
                        if col.name == "embedding" {
                            if let Value::List(vals) = &row_data[i] {
                                if vals.len() == 768 {
                                    let mut emb = [0f32; 768];
                                    for (j, v) in vals.iter().enumerate() {
                                        if let Value::Number(n) = v {
                                            emb[j] = *n as f32;
                                        }
                                    }
                                    let _ = vec_idx.insert(next_id, &emb, &self.buffer_manager, tx);
                                }
                            }
                        }
                    }
                }

                self.undo_buffer
                    .push(UndoRecord::DeleteNode(self.table_name.clone(), next_id));
                self.shared_state
                    .total_affected
                    .fetch_add(1, Ordering::SeqCst);

                // Update catalog cardinality
                let mut cat = database.catalog.write();
                if let Some(t) = cat.get_node_table_mut(&self.table_name) {
                    t.num_rows += 1;
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
                .unwrap(),
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
            shared_state: self.shared_state.clone(),
            tx_id: self.tx_id,
            read_ts: self.read_ts,
        })
    }
}
