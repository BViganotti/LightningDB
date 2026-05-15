use std::sync::Arc;

use lightning_core::memory::MemoryEntity;
use lightning_core::memory::MemoryStore as CoreMemoryStore;
use lightning_core::{Database, SyncMode, SystemConfig};
use napi_derive::napi;

use crate::database::JsDatabase;
use crate::streaming::{JsChangeStream, JsQueryStream, JsRecallStream};
use crate::types::{
    JsConsolidationReport, JsMemoryEntity, JsRagResult, JsSearchResult,
};

#[napi]
pub struct JsMemoryStore {
    inner: Arc<CoreMemoryStore>,
}

#[napi]
impl JsMemoryStore {
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let config = SystemConfig {
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = Database::new(&path, config)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open database: {}", e)))?;
        let conn = db.connect();
        Ok(Self {
            inner: Arc::new(CoreMemoryStore::new(conn)),
        })
    }

    #[napi(factory)]
    pub fn open_with_config(path: String, buffer_pool_size: i64, max_threads: i64) -> napi::Result<Self> {
        let config = SystemConfig {
            buffer_pool_size: buffer_pool_size.max(0) as u64,
            max_num_threads: max_threads.max(1) as u32,
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = Database::new(&path, config)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open database: {}", e)))?;
        let conn = db.connect();
        Ok(Self {
            inner: Arc::new(CoreMemoryStore::new(conn)),
        })
    }

    #[napi(factory)]
    pub fn from_database(db: &JsDatabase) -> napi::Result<Self> {
        let conn = db.inner().connect();
        Ok(Self {
            inner: Arc::new(CoreMemoryStore::new(conn)),
        })
    }

    #[napi]
    pub async fn store(
        &self,
        id: String,
        content: String,
        entity_type: String,
        metadata: Option<String>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            let entity = MemoryEntity {
                id,
                entity_type,
                content,
                created_at: now,
                last_accessed: now,
                access_count: 0,
                ttl_seconds: 0,
                metadata: metadata.unwrap_or_else(|| "{}".to_string()),
                valid_from: now,
                valid_until: 0,
            };
            inner.store(entity)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
        .map_err(|e| napi::Error::from_reason(format!("Store failed: {}", e)))
    }

    #[napi]
    pub async fn recall(&self, query: String, top_k: Option<i64>) -> napi::Result<Vec<JsSearchResult>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;
        let embedding: Vec<f32> = Vec::new();

        tokio::task::spawn_blocking(move || {
            inner.recall(&query, &embedding, k)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
        .map(|results| {
            results
                .into_iter()
                .map(|r| {
                    JsSearchResult::from_parts(
                        r.entity.id,
                        r.entity.content,
                        r.entity.entity_type,
                        r.score,
                        r.entity.metadata,
                    )
                })
                .collect()
        })
        .map_err(|e| napi::Error::from_reason(format!("Recall failed: {}", e)))
    }

    #[napi]
    pub async fn recall_with_embedding(
        &self,
        query: String,
        embedding: Vec<f64>,
        top_k: Option<i64>,
    ) -> napi::Result<Vec<JsSearchResult>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;
        let emb: Vec<f32> = embedding.into_iter().map(|v| v as f32).collect();

        tokio::task::spawn_blocking(move || {
            inner.recall(&query, &emb, k)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
        .map(|results| {
            results
                .into_iter()
                .map(|r| {
                    JsSearchResult::from_parts(
                        r.entity.id,
                        r.entity.content,
                        r.entity.entity_type,
                        r.score,
                        r.entity.metadata,
                    )
                })
                .collect()
        })
        .map_err(|e| napi::Error::from_reason(format!("Recall failed: {}", e)))
    }

    #[napi]
    pub async fn recall_recent(&self, top_k: Option<i64>) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;

        tokio::task::spawn_blocking(move || inner.recall_recent(k))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Recall recent failed: {}", e)))
    }

    #[napi]
    pub async fn recall_by_type(
        &self,
        entity_type: String,
        top_k: Option<i64>,
    ) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;

        tokio::task::spawn_blocking(move || inner.recall_by_type(&entity_type, k))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Recall by type failed: {}", e)))
    }

    #[napi]
    pub async fn recall_at_time(
        &self,
        at_micros: i64,
        top_k: Option<i64>,
    ) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;

        tokio::task::spawn_blocking(move || inner.recall_at_time(at_micros, k))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Recall at time failed: {}", e)))
    }

    #[napi]
    pub async fn recall_by_time(
        &self,
        start: i64,
        end: i64,
        top_k: Option<i64>,
    ) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;

        tokio::task::spawn_blocking(move || inner.recall_by_time(start, end, k))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Recall by time failed: {}", e)))
    }

    #[napi]
    pub async fn entity_history(&self, entity_id: String) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || inner.entity_history(&entity_id))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Entity history failed: {}", e)))
    }

    #[napi]
    pub async fn associate(
        &self,
        src_id: String,
        dst_id: String,
        rel_type: String,
        weight: Option<f64>,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        let w = weight.unwrap_or(1.0);

        tokio::task::spawn_blocking(move || inner.associate(&src_id, &dst_id, &rel_type, w))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map_err(|e| napi::Error::from_reason(format!("Associate failed: {}", e)))
    }

    #[napi]
    pub async fn expand(
        &self,
        entity_id: String,
        hops: Option<i64>,
        edge_types: Option<Vec<String>>,
    ) -> napi::Result<Vec<JsMemoryEntity>> {
        let inner = self.inner.clone();
        let h = hops.unwrap_or(1).max(1) as u32;
        let et: Vec<String> = edge_types
            .unwrap_or_else(|| vec!["Relates".to_string()]);

        tokio::task::spawn_blocking(move || {
            let et_refs: Vec<&str> = et.iter().map(|s| s.as_str()).collect();
            inner.expand(&entity_id, h, &et_refs)
        })
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|entities| entities.into_iter().map(JsMemoryEntity::from_core).collect())
            .map_err(|e| napi::Error::from_reason(format!("Expand failed: {}", e)))
    }

    #[napi]
    pub async fn forget(&self, entity_id: String) -> napi::Result<bool> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || inner.forget(&entity_id))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map_err(|e| napi::Error::from_reason(format!("Forget failed: {}", e)))
    }

    #[napi]
    pub async fn decay(&self) -> napi::Result<i64> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || inner.decay())
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|count| count as i64)
            .map_err(|e| napi::Error::from_reason(format!("Decay failed: {}", e)))
    }

    #[napi]
    pub async fn store_batch(&self, entities: Vec<JsMemoryEntity>) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let rust_entities: Vec<MemoryEntity> = entities
            .into_iter()
            .map(|e| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                MemoryEntity {
                    id: e.id,
                    entity_type: e.entity_type,
                    content: e.content,
                    created_at: if e.created_at == 0 { now } else { e.created_at },
                    last_accessed: if e.last_accessed == 0 { now } else { e.last_accessed },
                    access_count: e.access_count.max(1),
                    ttl_seconds: e.ttl_seconds,
                    metadata: if e.metadata.is_empty() { "{}".to_string() } else { e.metadata },
                    valid_from: if e.valid_from == 0 { now } else { e.valid_from },
                    valid_until: e.valid_until,
                }
            })
            .collect();

        tokio::task::spawn_blocking(move || inner.store_batch(rust_entities))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(|count| count as i64)
            .map_err(|e| napi::Error::from_reason(format!("Store batch failed: {}", e)))
    }

    #[napi]
    pub async fn rag_query(
        &self,
        query: String,
        top_k: Option<i64>,
    ) -> napi::Result<JsRagResult> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(5).max(1) as usize;
        let embedding: Vec<f32> = Vec::new();

        tokio::task::spawn_blocking(move || inner.rag_query(&query, &embedding, k))
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(JsRagResult::from)
            .map_err(|e| napi::Error::from_reason(format!("RAG query failed: {}", e)))
    }

    #[napi]
    pub async fn consolidate(&self) -> napi::Result<JsConsolidationReport> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || inner.consolidate())
            .await
            .map_err(|e| napi::Error::from_reason(format!("Task failed: {}", e)))?
            .map(JsConsolidationReport::from)
            .map_err(|e| napi::Error::from_reason(format!("Consolidate failed: {}", e)))
    }

    #[napi]
    pub fn query_stream(&self, query: String) -> napi::Result<JsQueryStream> {
        let rx = self
            .inner
            .query_stream(&query)
            .map_err(|e| napi::Error::from_reason(format!("Query stream failed: {}", e)))?;
        Ok(JsQueryStream::new(rx))
    }

    #[napi]
    pub fn subscribe_changes(&self) -> napi::Result<JsChangeStream> {
        let rx = self
            .inner
            .subscribe_changes()
            .map_err(|e| napi::Error::from_reason(format!("Subscribe changes failed: {}", e)))?;
        let crossbeam_rx = self.convert_mpsc_to_crossbeam(rx);
        Ok(JsChangeStream::new(crossbeam_rx))
    }

    #[napi]
    pub fn recall_stream(
        &self,
        query: String,
        top_k: Option<i64>,
    ) -> napi::Result<JsRecallStream> {
        let inner = self.inner.clone();
        let k = top_k.unwrap_or(10).max(1) as usize;
        let embedding: Vec<f32> = Vec::new();

        let rx = inner
            .recall_stream(&query, &embedding, k)
            .map_err(|e| napi::Error::from_reason(format!("Recall stream failed: {}", e)))?;
        Ok(JsRecallStream::new(rx))
    }
}

impl JsMemoryStore {
    fn convert_mpsc_to_crossbeam(
        &self,
        rx: std::sync::mpsc::Receiver<lightning_core::memory::ChangeEvent>,
    ) -> crossbeam::channel::Receiver<lightning_core::memory::ChangeEvent> {
        let (cb_tx, cb_rx) = crossbeam::channel::unbounded();
        std::thread::spawn(move || {
            while let Ok(event) = rx.recv() {
                if cb_tx.send(event).is_err() {
                    break;
                }
            }
        });
        cb_rx
    }
}
