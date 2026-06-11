use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use lightning_core::{
    Connection as CoreConnection, Database as CoreDatabase,
    Value,
};

use crate::types::*;

/// A connection to a LightningDB database.
///
/// Connections are lightweight and can be created freely via
/// [`Database::connect`](crate::Database::connect).
/// Each connection manages its own transaction state. For multi-threaded access,
/// create one connection per thread.
///
/// # Transactions
///
/// Use [`begin()`](Self::begin), [`commit()`](Self::commit), and [`rollback()`](Self::rollback)
/// for explicit transaction control. When no explicit transaction is active,
/// each query runs in auto-commit mode (automatically wrapped in a transaction).
///
/// # Example
///
/// ```no_run
/// use lightning::prelude::*;
///
/// let db = Database::open("path/to/db").unwrap();
/// let conn = db.connect();
/// let result = conn.query("MATCH (n) RETURN n LIMIT 10").unwrap();
/// ```
pub struct Connection {
    inner: CoreConnection,
}

impl Connection {
    pub(crate) fn new(database: Arc<CoreDatabase>) -> Self {
        Self {
            inner: CoreConnection::new(database),
        }
    }

    // ── Query Execution ──────────────────────────────────────────────

    /// Execute a Cypher query and return results as Arrow record batches.
    ///
    /// This returns a raw [`QueryResult`] containing Arrow batches. For typed
    /// access, use [`execute_typed()`](Self::execute_typed) instead.
    pub fn query(&self, query: &str) -> Result<QueryResult> {
        self.inner.query(query)
    }

    /// Execute a Cypher query with named parameters.
    ///
    /// Parameters are specified as `$param_name` in the query and provided
    /// via the `params` map.
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use lightning::prelude::*;
    ///
    /// let mut params = HashMap::new();
    /// params.insert("name".to_string(), Value::String("Alice".to_string()));
    /// let result = conn.execute("MATCH (n) WHERE n.name = $name RETURN n", Some(params));
    /// ```
    pub fn execute(&self, query: &str, params: Option<HashMap<String, Value>>) -> Result<QueryResult> {
        self.inner.execute(query, params)
    }

    /// Execute a query as of a specific MVCC timestamp (time-travel).
    ///
    /// The database shows only data committed at or before `snapshot_micros`.
    /// Use `now_micros()` or a previously observed timestamp.
    pub fn execute_at(
        &self,
        query: &str,
        snapshot_micros: u64,
        params: Option<HashMap<String, Value>>,
    ) -> Result<QueryResult> {
        self.inner.execute_at(query, snapshot_micros, params)
    }

    /// Execute a query and return results as a streaming channel.
    ///
    /// Results arrive as they are produced — useful for large result sets.
    /// Drop the receiver to cancel the query early.
    pub fn query_stream(
        &self,
        query: &str,
    ) -> Result<crossbeam::channel::Receiver<Result<lightning_core::processor::DataChunk>>> {
        self.inner.query_stream(query)
    }

    /// Execute a streaming query with parameters.
    pub fn execute_stream(
        &self,
        query: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<crossbeam::channel::Receiver<Result<lightning_core::processor::DataChunk>>> {
        self.inner.execute_stream(query, params)
    }

    // ── Typed Results ────────────────────────────────────────────────

    /// Execute a query and return typed results (rows as JSON maps).
    ///
    /// Each row is deserialized into a `Row` (a `Map<String, Value>`).
    /// Column types are automatically converted: Int64, Float64, String, Bool, etc.
    ///
    /// ```no_run
    /// let result = conn.execute_typed("MATCH (n:Person) RETURN n.name, n.age LIMIT 5", None).unwrap();
    /// for row in &result.rows {
    ///     println!("{}: {}", row["n.name"], row["n.age"]);
    /// }
    /// ```
    pub fn execute_typed(
        &self,
        query: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<TypedQueryResult> {
        let result = self.inner.execute(query, params)?;
        Ok(TypedQueryResult::from_batches(&result.batches))
    }

    /// Execute a query and return the results serialized as JSON.
    pub fn execute_json(
        &self,
        query: &str,
        params: Option<HashMap<String, Value>>,
    ) -> Result<String> {
        let typed = self.execute_typed(query, params)?;
        Ok(typed.to_json())
    }

    // ── Database Schema ──────────────────────────────────────────────

    /// Run a raw DDL or DML statement (no result rows expected).
    ///
    /// Useful for CREATE, DROP, ALTER, INSERT, SET, DELETE statements.
    pub fn execute_ddl(&self, stmt: &str) -> Result<()> {
        self.inner.execute(stmt, None)?;
        Ok(())
    }

    fn quote_ident(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Create a node table with the given schema.
    ///
    /// ```no_run
    /// conn.create_node_table("Person", &[
    ///     ("name", "STRING"),
    ///     ("age", "INT64"),
    /// ], Some("name")).unwrap();
    /// ```
    pub fn create_node_table(
        &self,
        table_name: &str,
        columns: &[(&str, &str)],
        primary_key: Option<&str>,
    ) -> Result<()> {
        let quoted_table = Self::quote_ident(table_name);
        let cols: Vec<String> = columns
            .iter()
            .map(|(name, typ)| format!("{} {}", Self::quote_ident(name), typ))
            .collect();
        let pk_clause = primary_key
            .map(|pk| format!(", PRIMARY KEY ({})", Self::quote_ident(pk)))
            .unwrap_or_default();
        let stmt = format!("CREATE NODE TABLE {quoted_table} ({}{pk_clause})", cols.join(", "));
        self.execute_ddl(&stmt)
    }

    /// Create a relationship table.
    pub fn create_rel_table(
        &self,
        table_name: &str,
        from_table: &str,
        to_table: &str,
        columns: &[(&str, &str)],
    ) -> Result<()> {
        let quoted_table = Self::quote_ident(table_name);
        let quoted_from = Self::quote_ident(from_table);
        let quoted_to = Self::quote_ident(to_table);
        let cols: Vec<String> = columns
            .iter()
            .map(|(name, typ)| format!("{} {}", Self::quote_ident(name), typ))
            .collect();
        let extra = if cols.is_empty() {
            String::new()
        } else {
            format!(", {}", cols.join(", "))
        };
        let stmt = format!("CREATE REL TABLE {quoted_table} (FROM {quoted_from} TO {quoted_to}{extra})");
        self.execute_ddl(&stmt)
    }

    /// Drop a table by name.
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        self.execute_ddl(&format!("DROP TABLE {}", Self::quote_ident(table_name)))
    }

    // ── Bulk Insert ─────────────────────────────────────────────────

    /// Bulk insert rows from an Arrow RecordBatch.
    ///
    /// The batch must match the table schema (excluding the internal `_id` column).
    /// Returns the number of rows inserted.
    pub fn bulk_insert_batch(&self, table_name: &str, batch: &RecordBatch) -> Result<usize> {
        self.inner.bulk_insert_batch(table_name, batch)
    }

    /// Fast insert rows from a list of key-value maps.
    ///
    /// Each row is a `Vec<(column_name, Value)>`. Returns the number of rows inserted.
    ///
    /// ```no_run
    /// let rows = vec![
    ///     vec![("name".to_string(), Value::String("Alice".to_string())),
    ///          ("age".to_string(), Value::Number(30.0))],
    ///     vec![("name".to_string(), Value::String("Bob".to_string())),
    ///          ("age".to_string(), Value::Number(25.0))],
    /// ];
    /// conn.fast_insert("Person", rows).unwrap();
    /// ```
    pub fn fast_insert(
        &self,
        table_name: &str,
        rows: Vec<Vec<(String, Value)>>,
    ) -> Result<usize> {
        self.inner.fast_insert(table_name, rows)
    }

    // ── Transaction Management ───────────────────────────────────────

    /// Begin an explicit transaction.
    ///
    /// All subsequent queries on this connection will execute within this
    /// transaction until [`commit()`](Self::commit) or [`rollback()`](Self::rollback) is called.
    pub fn begin(&self) -> Result<()> {
        self.inner.begin()
    }

    /// Commit the active transaction.
    pub fn commit(&self) -> Result<()> {
        self.inner.commit()
    }

    /// Rollback the active transaction, discarding all changes.
    pub fn rollback(&self) -> Result<()> {
        self.inner.rollback()
    }

    // ── Utility ──────────────────────────────────────────────────────

    /// Access the underlying core connection for advanced use cases.
    pub fn inner(&self) -> &CoreConnection {
        &self.inner
    }

    /// Access the client context for advanced session configuration.
    ///
    /// The client context allows setting query timeouts and memory quotas:
    ///
    /// ```no_run
    /// use std::sync::atomic::Ordering;
    ///
    /// conn.client_context().query_timeout_ms = 5000;
    /// conn.client_context().memory_quota = 512 * 1024 * 1024;
    /// ```
    pub fn client_context(&self) -> &lightning_core::ClientContext {
        &self.inner.client_context
    }
}
