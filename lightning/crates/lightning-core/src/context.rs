use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::Database;

pub struct ClientContext {
    pub database: Arc<Database>,
    pub active_query_id: AtomicU64,
    pub query_timeout_ms: u64,
    pub memory_quota: u64,
}

impl ClientContext {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            active_query_id: AtomicU64::new(0),
            query_timeout_ms: 0,
            memory_quota: 0,
        }
    }
}
