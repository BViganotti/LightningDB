use std::sync::Arc;

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    Json,
};
use lightning::memory::MemoryStore;

use crate::models::response::ErrorResponse;
use crate::server::AppState;

pub struct DbConnection(pub lightning::Connection);

pub struct ConnectionPool {
    db: Arc<lightning::Database>,
}

impl ConnectionPool {
    pub fn new(db: Arc<lightning::Database>) -> Self {
        Self { db }
    }

    pub fn acquire(&self) -> lightning::Connection {
        self.db.connect()
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
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        Ok(RequestId(request_id))
    }
}

#[derive(Clone)]
pub struct RequestIdExtension(pub String);
