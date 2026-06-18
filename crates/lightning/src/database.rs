use std::path::{Path, PathBuf};
use std::sync::Arc;

use lightning_core::{
    Database as CoreDatabase, DatabaseMetrics, SystemConfig,
};

use crate::connection::Connection;
use crate::types::Result;

/// An open LightningDB database.
///
/// The `Database` is the top-level handle to a LightningDB instance. It manages
/// the storage engine, buffer pool, WAL, transaction manager, and catalog.
/// Use [`Database::open`] or [`Database::open_with_config`] to create one.
///
/// # Example
///
/// ```no_run
/// use lightning::prelude::*;
///
/// let db = Database::open("path/to/db").unwrap();
/// let conn = db.connect();
/// conn.execute("CREATE NODE TABLE Person (name STRING, age INT64, PRIMARY KEY (name))", None).unwrap();
/// ```
pub struct Database {
    inner: Arc<CoreDatabase>,
    path: PathBuf,
}

impl Database {
    /// Open a database at `path` with default configuration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref().to_path_buf();
        let inner = CoreDatabase::new(&p, SystemConfig::default())?;
        Ok(Self { inner, path: p })
    }

    /// Open a database at `path` with a custom [`SystemConfig`].
    ///
    /// Configuration options include buffer pool size, thread count, sync mode,
    /// prefetch settings, and more.
    pub fn open_with_config(path: impl AsRef<Path>, config: SystemConfig) -> Result<Self> {
        let p = path.as_ref().to_path_buf();
        let inner = CoreDatabase::new(&p, config)?;
        Ok(Self { inner, path: p })
    }

    /// Open a read-only database at `path`.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let config = SystemConfig {
            read_only: true,
            ..Default::default()
        };
        Self::open_with_config(path, config)
    }

    /// Create a new connection to this database.
    ///
    /// Each connection maintains its own transaction state and is safe to use
    /// concurrently from multiple threads.
    pub fn connect(&self) -> Connection {
        Connection::new(Arc::clone(&self.inner))
    }

    /// Create a connection with auth table checks disabled for internal use.
    pub fn connect_internal(&self) -> Connection {
        Connection::new_internal(Arc::clone(&self.inner))
    }

    /// Flush all dirty pages to disk and persist catalog metadata.
    ///
    /// Checkpoints ensure data durability and bound WAL size. Called
    /// automatically on drop, but can be triggered manually for safety.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// VACUUM: reclaim space from deleted rows by compacting columns.
    pub fn vacuum(&self) -> Result<()> {
        self.inner.vacuum()
    }

    /// Register a WebAssembly function as a callable scalar.
    ///
    /// The WASM module must export a function with signature `(f64) -> f64`.
    /// After registration, the function can be used in Cypher queries.
    ///
    /// ```cypher
    /// RETURN wasm_score(t.val)
    /// ```
    pub fn register_wasm_function(
        &self,
        wasm_path: impl AsRef<Path>,
        func_name: &str,
    ) -> Result<()> {
        self.inner.register_wasm_function(wasm_path, func_name)
    }

    /// Access database metrics (query counts, buffer hit rate, etc.).
    pub fn metrics(&self) -> &DatabaseMetrics {
        self.inner.metrics()
    }

    /// Repair table cardinalities from actual data file sizes.
    ///
    /// Useful after restoring from backup or after schema changes.
    pub fn repair_cardinalities(&self) -> Result<()> {
        self.inner.repair_cardinalities()
    }

    /// Get the path to the underlying database directory.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Access the inner database handle for advanced use cases.
    pub fn inner(&self) -> &Arc<CoreDatabase> {
        &self.inner
    }
}
