use std::sync::{Arc, Mutex};

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    Json,
};
use lightning::memory::MemoryStore;

use crate::models::response::ErrorResponse;
use crate::server::AppState;

pub struct DbConnection(pub lightning::Connection);

/// A bounded pool of pre-created connections to the database.
///
/// Each `acquire()` returns a connection from the pool if available,
/// or creates a new one up to `max_size`. Connections are returned
/// to the pool on drop via a wrapper.
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

    pub fn acquire(&self) -> lightning::Connection {
        let mut idle = self.idle.lock().unwrap();
        idle.pop().unwrap_or_else(|| self.db.connect())
    }

    /// Return a connection to the pool for reuse.
    pub fn release(&self, conn: lightning::Connection) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max_size {
            idle.push(conn);
        }
        // else drop the excess connection
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
        Ok(DbConnection(app_state.connection_pool.acquire()))
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
