use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    Json,
};
use lightning::memory::MemoryStore;

use crate::models::response::ErrorResponse;
use crate::server::AppState;

/// A connection that is returned to the pool on drop.
pub struct DbConnection {
    inner: ManuallyDrop<lightning::Connection>,
    pool: Arc<ConnectionPool>,
}

impl Deref for DbConnection {
    type Target = lightning::Connection;
    fn deref(&self) -> &lightning::Connection {
        &self.inner
    }
}

impl DerefMut for DbConnection {
    fn deref_mut(&mut self) -> &mut lightning::Connection {
        &mut self.inner
    }
}

impl Drop for DbConnection {
    fn drop(&mut self) {
        // Safety: we take ownership of the connection here since
        // ManuallyDrop prevents double-drop. This is the only place
        // the connection is consumed.
        let conn = unsafe { ManuallyDrop::take(&mut self.inner) };
        self.pool.release(conn);
    }
}

/// A bounded pool of database connections for reuse across requests.
pub struct ConnectionPool {
    db: Arc<lightning::Database>,
    idle: Mutex<Vec<lightning::Connection>>,
    max_size: usize,
}

impl ConnectionPool {
    pub fn new(db: Arc<lightning::Database>, max_size: usize) -> Self {
        Self {
            db,
            idle: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
        }
    }

    pub fn acquire(self: &Arc<Self>) -> DbConnection {
        let mut idle = self.idle.lock().unwrap();
        let conn = idle.pop().unwrap_or_else(|| self.db.connect());
        DbConnection {
            inner: ManuallyDrop::new(conn),
            pool: Arc::clone(self),
        }
    }

    fn release(&self, conn: lightning::Connection) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max_size {
            idle.push(conn);
        }
    }
}

impl<S> FromRequestParts<S> for DbConnection
where
    S: Send + Sync,
    Arc<AppState>: FromRef<S>,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_ref(state);
        Ok(app_state.connection_pool.acquire())
    }
}

pub struct AppStore(pub Arc<MemoryStore>);

impl<S> FromRequestParts<S> for AppStore
where
    S: Send + Sync,
    Arc<AppState>: FromRef<S>,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_ref(state);
        Ok(AppStore(app_state.store.clone()))
    }
}

pub struct RequestId(pub String);

impl<S> FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        let request_id = parts
            .extensions
            .get::<RequestIdExtension>()
            .map(|ext| ext.0.clone())
            .unwrap_or_else(|| format!("auto-{}", uuid::Uuid::new_v4()));
        Ok(RequestId(request_id))
    }
}

#[derive(Clone)]
pub struct RequestIdExtension(pub String);
