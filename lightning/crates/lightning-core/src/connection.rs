use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::context::ClientContext;
use crate::error::{LightningError, Result};
use crate::ingestion::BulkIngestionService;
use crate::processor::Value;
use crate::result::QueryResult;
use crate::util::normalize_query;
use crate::Database;

pub struct Connection {
    pub client_context: Arc<ClientContext>,
    pub transaction: parking_lot::Mutex<Option<Arc<crate::transaction::transaction_manager::Transaction>>>,
    pub pending_tables: parking_lot::RwLock<Vec<String>>,
}

impl Connection {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            client_context: Arc::new(ClientContext::new(database)),
            transaction: parking_lot::Mutex::new(None),
            pending_tables: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn begin(&self) -> Result<()> {
        let mut guard = self.transaction.lock();
        if guard.is_some() {
            return Err(LightningError::Query("Transaction already active".into()));
        }
        let tx = self
            .client_context
            .database
            .transaction_manager
            .begin(false)?;
        *guard = Some(Arc::new(tx));
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let tx = {
            let mut guard = self.transaction.lock();
            guard.take()
        }
        .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &self.client_context.database;

        self.client_context
            .database
            .storage_manager
            .read()
            .flush_all_pending(bm, &tx)?;

        self.client_context
            .database
            .transaction_manager
            .commit(&tx, bm, db)
    }

    pub fn fast_insert(
        &self,
        table_name: &str,
        rows: Vec<Vec<(String, Value)>>,
    ) -> Result<usize> {
        use arrow::array::*;

        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {table_name} not found")))?
        };

        let num_rows = rows.len();
        if num_rows == 0 {
            return Ok(0);
        }

        let start_id = table
            .next_row_id
            .fetch_add(num_rows as u64, Ordering::SeqCst);

        let columns = &table.columns;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(columns.len());

        // _id column
        let id_values: UInt64Array = (start_id..start_id + num_rows as u64).collect();
        fields.push(arrow::datatypes::Field::new(
            "_id",
            arrow::datatypes::DataType::UInt64,
            false,
        ));
        arrays.push(Arc::new(id_values) as ArrayRef);

        // Data columns
        for col in columns.iter().skip(1) {
            let arr: ArrayRef = match col.data_type {
                lightning_types::LogicalType::String => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::String(s))) => builder.append_value(s),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Number(n))) => builder.append_value(*n as i64),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Double => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Number(n))) => builder.append_value(*n),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Bool => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Boolean(b))) => builder.append_value(*b),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Node(_) => {
                    let mut builder = UInt64Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Node(id))) => builder.append_value(*id),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Date => {
                    let mut builder = Date32Builder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Date(d))) => builder.append_value(*d),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                lightning_types::LogicalType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, Value::Timestamp(t))) => builder.append_value(*t),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                    for row in &rows {
                        let val = row.iter().find(|(n, _)| *n == col.name);
                        match val {
                            Some((_, v)) => builder.append_value(v.to_string()),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
            };
            fields.push(col.to_field());
            arrays.push(arr);
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)?;

        let service = BulkIngestionService::new(db);
        service.ingest_batch(table_name, &batch, start_id, num_rows)
    }

    pub fn rollback(&self) -> Result<()> {
        let tx = {
            let mut guard = self.transaction.lock();
            guard.take()
        }
        .ok_or_else(|| LightningError::Query("No active transaction".into()))?;

        let db: &Database = &self.client_context.database;
        self.client_context
            .database
            .transaction_manager
            .rollback(db, &tx)
    }

    fn plan_and_optimize(
        &self,
        stmt: crate::planner::binder::BoundStatement,
    ) -> Result<crate::planner::logical_plan::LogicalOperator> {
        let plan = crate::planner::LogicalPlanner::plan(stmt)?;
        let optimizer = crate::optimizer::Optimizer::new(
            self.client_context.database.catalog.inner_catalog(),
        );
        optimizer.optimize(plan)
    }

    fn build_physical_plan(
        &self,
        query_str: &str,
        snapshot_ts: Option<u64>,
        explicit_tx: Option<Arc<crate::transaction::transaction_manager::Transaction>>,
    ) -> Result<(
        Box<dyn crate::processor::PhysicalOperator + Send + Sync>,
        Arc<crate::transaction::transaction_manager::Transaction>,
    )> {
        let cache_key = normalize_query(query_str);
        let cached_stmt = {
            let cache = self.client_context.database.plan_cache.read();
            cache.get(&cache_key).cloned()
        };
        let tx = match (snapshot_ts, explicit_tx) {
            (_, Some(tx)) => tx,
            (Some(ts), None) => Arc::new(
                self.client_context
                    .database
                    .transaction_manager
                    .begin_at(true, ts)?,
            ),
            (None, None) => Arc::new(
                self.client_context
                    .database
                    .transaction_manager
                    .begin(false)?,
            ),
        };
        let bm = &self.client_context.database.buffer_manager;
        let db: &Database = &self.client_context.database;
        db.storage_manager.read().flush_all_pending(bm, &tx)?;
        let bound_stmt = if let Some(stmt) = cached_stmt {
            stmt
        } else {
            let query = crate::parser::parse(query_str)
                .map_err(|e| LightningError::Query(e.to_string()))?;
            let catalog = self.client_context.database.catalog.read();
            let fr = self.client_context.database.function_registry.read();
            let mut binder = crate::planner::Binder::new(&catalog, &fr);
            let bound_query = binder.bind_query(&query)?;
            drop(catalog);
            drop(fr);
            if let Some(bound_union) = bound_query.union_queries.first() {
                self.client_context
                    .database
                    .plan_cache
                    .write()
                    .insert(cache_key, bound_union.statement.clone());
            }
            let bound_union = bound_query
                .union_queries
                .first()
                .ok_or_else(|| LightningError::Query("No query".into()))?;
            bound_union.statement.clone()
        };
        let logical_plan = self.plan_and_optimize(bound_stmt)?;
        let mut planner = crate::processor::physical_plan::PhysicalPlanner::new(
            Arc::clone(&self.client_context.database),
            tx.read_ts,
            tx.tx_id,
            Arc::clone(&tx.undo_buffer),
        );
        let physical_plan = planner.plan(logical_plan)?;
        Ok((physical_plan, tx))
    }

    pub fn query(&self, query_str: &str) -> Result<QueryResult> {
        self.execute(query_str, None)
    }

    pub fn query_stream(
        &self,
        query_str: &str,
    ) -> Result<crossbeam::channel::Receiver<Result<crate::processor::DataChunk>>> {
        self.execute_stream(query_str, None)
    }

    pub fn execute_stream(
        &self,
        query_str: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<crossbeam::channel::Receiver<Result<crate::processor::DataChunk>>> {
        let (physical_plan, tx) = self.build_physical_plan(query_str, None, None)?;
        let mut processor = crate::processor::Processor::new(physical_plan);
        processor.execute_stream(
            Arc::clone(&self.client_context.database),
            tx,
            params,
        )
    }

    #[tracing::instrument(skip(self, snapshot_ts, params))]
    pub fn execute_at(
        &self,
        query_str: &str,
        snapshot_ts: u64,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        let _query_id = self
            .client_context
            .active_query_id
            .fetch_add(1, Ordering::SeqCst);
        self.client_context.database.metrics.record_query();

        let active_tx_guard = self.transaction.lock();
        let explicit_tx = active_tx_guard.as_ref().map(Arc::clone);
        let is_autocommit = explicit_tx.is_none();
        drop(active_tx_guard);

        let (physical_plan, tx) = self.build_physical_plan(
            query_str,
            Some(snapshot_ts),
            explicit_tx,
        )?;
        let mut processor = crate::processor::Processor::new(physical_plan);
        let chunks = processor.execute(
            Arc::clone(&self.client_context.database),
            Arc::clone(&tx),
            params,
        )?;

        if is_autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.transaction_manager.commit(&tx, bm, db).or_else(|e| {
                if let Err(rollback_err) = db.transaction_manager.rollback(db, &tx) {
                    tracing::warn!("Rollback after commit failure failed: {}", rollback_err);
                }
                Err(e)
            })?;
        }

        Ok(QueryResult::new_arrow(
            vec![],
            vec![],
            chunks.into_iter().map(|c| c.batch).collect(),
        ))
    }

    #[tracing::instrument(skip(self, params))]
    pub fn execute(
        &self,
        query_str: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let _query_id = self
            .client_context
            .active_query_id
            .fetch_add(1, Ordering::SeqCst);
        self.client_context.database.metrics.record_query();

        let active_tx_guard = self.transaction.lock();
        let explicit_tx = active_tx_guard.as_ref().map(Arc::clone);
        let is_autocommit = explicit_tx.is_none();
        drop(active_tx_guard);

        let (physical_plan, tx) = self.build_physical_plan(query_str, None, explicit_tx)?;
        let mut processor = crate::processor::Processor::new(physical_plan);
        let chunks = processor.execute(
            Arc::clone(&self.client_context.database),
            Arc::clone(&tx),
            params,
        )?;

        if is_autocommit {
            let bm = &self.client_context.database.buffer_manager;
            let db = &*self.client_context.database;
            db.transaction_manager.commit(&tx, bm, db).inspect_err(|_e| {
                if let Err(rollback_err) = db.transaction_manager.rollback(db, &tx) {
                    tracing::warn!("Rollback after commit failure failed: {}", rollback_err);
                }
            })?;
        }

        let elapsed_ms = start.elapsed().as_millis() as u64;
        let threshold = self.client_context.database._config.slow_query_threshold_ms;
        if threshold > 0 && elapsed_ms >= threshold {
            tracing::warn!(
                "SLOW QUERY: {} ms | query: {}",
                elapsed_ms,
                query_str
            );
        }

        Ok(QueryResult::new_arrow(
            vec![],
            vec![],
            chunks.into_iter().map(|c| c.batch).collect(),
        ))
    }

    pub fn bulk_insert_batch(&self, table_name: &str, batch: &RecordBatch) -> Result<usize> {
        let db = self.client_context.database.clone();
        let table = {
            let storage = db.storage_manager.read();
            storage
                .get_table(table_name)
                .cloned()
                .ok_or_else(|| LightningError::Query(format!("Table {table_name} not found")))?
        };

        let num_rows = batch.num_rows();

        let start_id = table
            .next_row_id
            .fetch_add(num_rows as u64, Ordering::SeqCst);

        // Prepend _id column
        let id_values: UInt64Array = (start_id..start_id + num_rows as u64).collect();
        let id_field =
            arrow::datatypes::Field::new("_id", arrow::datatypes::DataType::UInt64, false);
        let mut fields = vec![id_field];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(id_values)];
        for i in 0..batch.num_columns() {
            fields.push(batch.schema().field(i).clone());
            columns.push(batch.column(i).clone());
        }
        let final_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let final_batch = RecordBatch::try_new(final_schema, columns)?;

        let service = BulkIngestionService::new(db);
        service.ingest_batch(table_name, &final_batch, start_id, num_rows)
    }

    pub fn inner(&self) -> &ClientContext {
        &self.client_context
    }
}
