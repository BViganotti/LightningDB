pub mod connection;
pub mod database;
pub mod fusion;
pub mod memory;
pub mod types;

pub mod prelude {
    pub use crate::connection::Connection;
    pub use crate::database::Database;
    pub use crate::fusion::{ConnectedDirection, Fusion, ModuleCohesion};
    pub use crate::memory::{MemoryStore, DataChunk, DEFAULT_EMBEDDING_DIM};
    pub use crate::types::{Result, TypedQueryResult, Value};

    pub use lightning_core::memory::{
        ChangeEvent, ConsolidationReport, MemoryEntity, MemoryRelation, RagConfig, RagResult,
        SearchResult,
    };
    pub use lightning_core::{DatabaseMetrics, LightningError, QueryResult, SyncMode, SystemConfig};
}

pub use connection::Connection;
pub use database::Database;
pub use fusion::Fusion;
pub use memory::{MemoryStore, DataChunk};

pub use lightning_core::memory::{
    ChangeEvent, ConsolidationReport, MemoryEntity, MemoryRelation, RagConfig, RagResult,
    SearchResult, DEFAULT_EMBEDDING_DIM as CORE_DEFAULT_EMBEDDING_DIM,
};
pub use lightning_core::{DatabaseMetrics, LightningError, QueryResult, SyncMode, SystemConfig};
pub use types::TypedQueryResult;
