use crate::{Connection, Database, LightningError, Result};
use crossbeam::channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A fixed-size pool of reusable database connections.
///
/// Connections are created lazily up to `max_size` and returned to the
/// pool after use. If the pool is exhausted, `get()` blocks until a
/// connection becomes available or the optional timeout expires.
///
/// All connections share the same `Database` instance and thus the
/// same underlying storage, WAL, and transaction manager.
pub struct ConnectionPool {
    db: Arc<Database>,
    tx: Sender<Connection>,
    rx: Receiver<Connection>,
    max_size: usize,
    // Tracks created connections for diagnostics
    created: std::sync::atomic::AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool.
    ///
    /// `max_size` is the maximum number of idle connections to keep in the pool.
    /// Connections are created on demand — the pool starts empty.
    pub fn new(db: Arc<Database>, max_size: usize) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(max_size.max(1));
        Self {
            db,
            tx,
            rx,
            max_size: max_size.max(1),
            created: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Borrow a connection from the pool. Blocks until one is available.
    pub fn get(&self) -> Result<PooledConnection> {
        // Try to pop an idle connection
        match self.rx.try_recv() {
            Ok(conn) => return Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() }),
            Err(crossbeam::channel::TryRecvError::Empty) => {}
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                return Err(LightningError::Internal("Connection pool is shut down".into()));
            }
        }

        // No idle connection available — create a new one if under max_size
        let created = self.created.load(std::sync::atomic::Ordering::Relaxed);
        if created < self.max_size {
            // Attempt to atomically claim a creation slot
            if self.created.compare_exchange(
                created,
                created + 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            ).is_ok() {
                let conn = self.db.connect();
                return Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() });
            }
        }

        // Pool is full — block until a connection is returned
        match self.rx.recv() {
            Ok(conn) => Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() }),
            Err(_) => Err(LightningError::Internal("Connection pool is shut down".into())),
        }
    }

    /// Borrow a connection with a timeout.
    pub fn get_timeout(&self, timeout: Duration) -> Result<PooledConnection> {
        let deadline = Instant::now() + timeout;

        // Try to pop an idle connection
        match self.rx.try_recv() {
            Ok(conn) => return Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() }),
            Err(crossbeam::channel::TryRecvError::Empty) => {}
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                return Err(LightningError::Internal("Connection pool is shut down".into()));
            }
        }

        // No idle connection — create if under max_size
        let created = self.created.load(std::sync::atomic::Ordering::Relaxed);
        if created < self.max_size {
            if self.created.compare_exchange(
                created,
                created + 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            ).is_ok() {
                let conn = self.db.connect();
                return Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() });
            }
        }

        // Block with timeout
        match self.rx.recv_timeout(timeout) {
            Ok(conn) => Ok(PooledConnection { inner: Some(conn), tx: self.tx.clone() }),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                Err(LightningError::Internal("Connection pool timeout: no connection available".into()))
            }
            Err(_) => Err(LightningError::Internal("Connection pool is shut down".into())),
        }
    }

    /// Returns the maximum pool size.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Returns the approximate number of idle connections in the pool.
    pub fn idle_count(&self) -> usize {
        self.rx.len()
    }
}

/// A connection that automatically returns to the pool when dropped.
pub struct PooledConnection {
    inner: Option<Connection>,
    tx: Sender<Connection>,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.inner.as_ref().expect("PooledConnection was taken")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.inner.as_mut().expect("PooledConnection was taken")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            let _ = self.tx.try_send(conn);
        }
    }
}
