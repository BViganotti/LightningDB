use std::sync::Arc;

use crossbeam::channel::Receiver;
use lightning_core::memory::{
    ChangeEvent, ConsolidationReport, MemoryEntity, MemoryStore as CoreMemoryStore,
    RagConfig, RagResult, SearchResult,
};

use crate::connection::Connection;
use crate::types::Result;

pub use lightning_core::memory::DEFAULT_EMBEDDING_DIM;
pub use lightning_core::processor::DataChunk;

/// High-level memory store for LightningDB.
///
/// The `MemoryStore` provides entity storage, hybrid search (FTS + vector),
/// graph-based RAG querying, temporal queries, memory consolidation,
/// and change data capture (CDC).
///
/// Uses two tables under the hood: `Entity` and `Relates`.
///
/// # Example
///
/// ```no_run
/// use lightning::prelude::*;
///
/// let db = Database::open("path/to/db").unwrap();
/// let store = MemoryStore::new(db.connect(), DEFAULT_EMBEDDING_DIM);
///
/// let entity = MemoryEntity {
///     id: "note-1".into(),
///     content: "LightningDB supports Cypher queries and vector search.".into(),
///     entity_type: "note".into(),
///     ..Default::default()
/// };
/// store.store(entity).unwrap();
///
/// let results = store.recall("vector search", &[], 5).unwrap();
/// ```
pub struct MemoryStore {
    inner: Arc<CoreMemoryStore>,
}

impl MemoryStore {
    /// Create a new MemoryStore backed by the given connection.
    ///
    /// `embedding_dim` controls the dimension of vector embeddings used
    /// for similarity search. Common values: 768 (recommended), 384, 1024.
    pub fn new(conn: Connection, embedding_dim: usize) -> Self {
        let db = conn.inner().client_context.database.clone();
        let core_conn = db.connect();
        Self {
            inner: Arc::new(CoreMemoryStore::new(core_conn, embedding_dim)),
        }
    }

    /// Create a MemoryStore from an existing connection (takes ownership).
    pub fn from_connection(conn: Connection) -> Self {
        let db = conn.inner().client_context.database.clone();
        let core_conn = db.connect();
        Self {
            inner: Arc::new(CoreMemoryStore::new(core_conn, DEFAULT_EMBEDDING_DIM)),
        }
    }

    /// Ensure the Entity and Relates tables exist.
    /// Called automatically by most methods; safe to call explicitly at init.
    pub fn ensure_schema(&self) -> Result<()> {
        self.inner.ensure_schema()
    }

    // ── Entity Storage ───────────────────────────────────────────────

    /// Store a single entity. Overwrites any existing entity with the same id.
    pub fn store(&self, entity: MemoryEntity) -> Result<()> {
        self.inner.store(entity)
    }

    /// Store multiple entities in a single batch.
    /// Returns the number of entities stored.
    pub fn store_batch(&self, entities: Vec<MemoryEntity>) -> Result<usize> {
        self.inner.store_batch(entities)
    }

    /// Soft-delete an entity by setting its `valid_until` timestamp.
    /// Returns true if the entity was found.
    pub fn forget(&self, entity_id: &str) -> Result<bool> {
        self.inner.forget(entity_id)
    }

    /// Decay expired entities (those whose TTL has elapsed).
    /// Returns the number of entities expired.
    pub fn decay(&self) -> Result<usize> {
        self.inner.decay()
    }

    // ── Search & Recall ──────────────────────────────────────────────

    /// Hybrid search: FTS + vector similarity with Reciprocal Rank Fusion.
    ///
    /// Pass an empty `embedding` slice to use FTS-only search.
    /// Pass an empty `query_text` to use vector-only search.
    pub fn recall(&self, query_text: &str, embedding: &[f32], top_k: usize) -> Result<Vec<SearchResult>> {
        self.inner.recall(query_text, embedding, top_k)
    }

    /// Streaming variant of `recall()`. Results arrive on a channel.
    pub fn recall_stream(
        &self,
        query_text: &str,
        embedding: &[f32],
        top_k: usize,
    ) -> Result<Receiver<Result<SearchResult>>> {
        self.inner.recall_stream(query_text, embedding, top_k)
    }

    /// Recall entities by type, ordered by last access time.
    pub fn recall_by_type(&self, entity_type: &str, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.inner.recall_by_type(entity_type, top_k)
    }

    /// Recall the most recently created entities.
    pub fn recall_recent(&self, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.inner.recall_recent(top_k)
    }

    /// Recall entities created within a time range (microseconds since epoch).
    pub fn recall_by_time(&self, start: i64, end: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.inner.recall_by_time(start, end, top_k)
    }

    /// Recall entities valid at a specific point in time.
    pub fn recall_at_time(&self, at_micros: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
        self.inner.recall_at_time(at_micros, top_k)
    }

    /// Get the full version history of an entity across time.
    pub fn entity_history(&self, entity_id: &str) -> Result<Vec<MemoryEntity>> {
        self.inner.entity_history(entity_id)
    }

    // ── RAG Pipeline ─────────────────────────────────────────────────

    /// Full RAG pipeline: hybrid search → graph expansion → reranking → context assembly.
    ///
    /// Returns a `RagResult` with assembled context ready for an LLM.
    pub fn rag_query(&self, query_text: &str, embedding: &[f32], top_k: usize) -> Result<RagResult> {
        self.inner.rag_query(query_text, embedding, top_k)
    }

    /// RAG pipeline with custom configuration.
    ///
    /// See [`RagConfig`] for tunable parameters (expansion depth,
    /// search/recency/degree weights, cross-encoder WASM module).
    pub fn rag_query_with_config(
        &self,
        query_text: &str,
        embedding: &[f32],
        top_k: usize,
        config: &RagConfig,
    ) -> Result<RagResult> {
        self.inner.rag_query_with_config(query_text, embedding, top_k, config)
    }

    // ── Graph Operations ─────────────────────────────────────────────

    /// Expand from an entity by traversing connected edges up to `hops` levels.
    ///
    /// Optionally filter by edge types. Returns all entities reachable
    /// within the hop radius.
    pub fn expand(
        &self,
        entity_id: &str,
        hops: u32,
        edge_types: &[&str],
    ) -> Result<Vec<MemoryEntity>> {
        self.inner.expand(entity_id, hops, edge_types)
    }

    /// Create a relationship between two entities.
    pub fn associate(
        &self,
        src_id: &str,
        dst_id: &str,
        rel_type: &str,
        weight: f64,
    ) -> Result<()> {
        self.inner.associate(src_id, dst_id, rel_type, weight)
    }

    // ── Consolidation ────────────────────────────────────────────────

    /// Run the memory consolidation pipeline:
    /// 1. Compute content-based similarity (n-gram Jaccard)
    /// 2. Auto-link similar entities with RelatedTo edges
    /// 3. Detect contradictions (semantically close but lexically divergent)
    /// 4. Run PageRank to identify important entities
    pub fn consolidate(&self) -> Result<ConsolidationReport> {
        self.inner.consolidate()
    }

    // ── Streaming Queries ────────────────────────────────────────────

    /// Execute a streaming Cypher query. Results arrive on a channel.
    pub fn query_stream(
        &self,
        query: &str,
    ) -> Result<Receiver<Result<DataChunk>>> {
        self.inner.query_stream(query)
    }

    // ── Time Travel ──────────────────────────────────────────────────

    /// Execute a Cypher query as of a specific MVCC timestamp.
    pub fn execute_at(&self, query: &str, snapshot_micros: u64) -> Result<lightning_core::QueryResult> {
        self.inner.execute_at(query, snapshot_micros)
    }

    // ── Change Data Capture ──────────────────────────────────────────

    /// Subscribe to change events. Returns a receiver that yields
    /// [`ChangeEvent`] on every write operation.
    pub fn subscribe_changes(&self) -> Result<std::sync::mpsc::Receiver<ChangeEvent>> {
        self.inner.subscribe_changes()
    }

    // ── Utility ──────────────────────────────────────────────────────

    /// Get the current time in microseconds since epoch.
    pub fn now_micros() -> i64 {
        CoreMemoryStore::now_micros_for_test()
    }

    /// Access the inner core memory store.
    pub fn inner(&self) -> &CoreMemoryStore {
        &self.inner
    }
}
