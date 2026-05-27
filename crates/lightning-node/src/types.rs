use napi_derive::napi;

#[napi(object)]
pub struct JsSearchResult {
    pub id: String,
    pub content: String,
    pub entity_type: String,
    pub score: f64,
    pub metadata: String,
    pub embedding: Vec<f64>,
}

#[napi(object)]
pub struct JsMemoryEntity {
    pub id: String,
    pub entity_type: String,
    pub content: String,
    pub created_at: i64,
    pub last_accessed: i64,
    pub access_count: i64,
    pub ttl_seconds: i64,
    pub metadata: String,
    pub valid_from: i64,
    pub valid_until: i64,
    pub embedding: Vec<f64>,
}

#[napi(object)]
pub struct JsRagResult {
    pub context: String,
    pub sources: Vec<String>,
    pub total_sources: i64,
    pub query: String,
}

#[napi(object)]
pub struct JsConsolidationReport {
    pub links_created: i64,
    pub contradictions_found: i64,
    pub total_entities: i64,
}

#[napi(object)]
pub struct JsChangeEvent {
    pub timestamp: i64,
    pub bytes_written: i64,
    pub total_wal_bytes: i64,
}

impl JsSearchResult {
    pub fn from_parts(id: String, content: String, entity_type: String, score: f64, metadata: String, embedding: Vec<f64>) -> Self {
        Self { id, content, entity_type, score, metadata, embedding }
    }
}

impl JsMemoryEntity {
    pub fn from_core(e: lightning_core::memory::MemoryEntity) -> Self {
        Self {
            id: e.id,
            entity_type: e.entity_type,
            content: e.content,
            created_at: e.created_at,
            last_accessed: e.last_accessed,
            access_count: e.access_count,
            ttl_seconds: e.ttl_seconds,
            metadata: e.metadata,
            valid_from: e.valid_from,
            valid_until: e.valid_until,
            embedding: e.embedding.iter().map(|&v| v as f64).collect(),
        }
    }
}

impl From<lightning_core::memory::ChangeEvent> for JsChangeEvent {
    fn from(e: lightning_core::memory::ChangeEvent) -> Self {
        Self {
            timestamp: e.timestamp,
            bytes_written: e.bytes_written as i64,
            total_wal_bytes: e.total_wal_bytes as i64,
        }
    }
}

impl From<lightning_core::memory::ConsolidationReport> for JsConsolidationReport {
    fn from(r: lightning_core::memory::ConsolidationReport) -> Self {
        Self {
            links_created: r.links_created as i64,
            contradictions_found: r.contradictions_found as i64,
            total_entities: r.total_entities as i64,
        }
    }
}

impl From<lightning_core::memory::RagResult> for JsRagResult {
    fn from(r: lightning_core::memory::RagResult) -> Self {
        Self {
            context: r.context,
            sources: r.sources,
            total_sources: r.total_sources as i64,
            query: r.query,
        }
    }
}
