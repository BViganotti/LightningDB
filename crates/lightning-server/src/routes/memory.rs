use axum::Json;
use lightning_core::memory::MemoryEntity;

use crate::error::AppError;
use crate::extract::{AppStore, RequestId};
use crate::models::request::{
    ConsolidateRequest, EntityHistoryRequest, ForgetRequest, RecallByTypeRequest,
    RecallRecentRequest, RecallRequest, StoreBatchRequest, StoreRequest,
};
use crate::models::response::{
    ApiResponse, ConsolidationReportResponse, DecayResponse, EntitiesResponse,
    EntityHistoryResponse, EntityItem, ForgetResponse, RecallResponse, ResponseMeta,
    SearchResultItem, StoreBatchResponse,
};

fn entity_to_item(e: &MemoryEntity) -> EntityItem {
    EntityItem {
        id: e.id.clone(),
        entity_type: e.entity_type.clone(),
        content: e.content.clone(),
        metadata: e.metadata.clone(),
        created_at: e.created_at,
        last_accessed: e.last_accessed,
        access_count: e.access_count,
        ttl_seconds: e.ttl_seconds,
        valid_from: e.valid_from,
        valid_until: e.valid_until,
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_else(|e| {
            tracing::warn!("SystemTime before UNIX_EPOCH: {e}");
            0
        })
}

pub async fn store_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<StoreRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();
    let now = now_micros();

    let entity = MemoryEntity {
        id: req.id,
        entity_type: req.entity_type,
        content: req.content,
        created_at: req.created_at.unwrap_or(now),
        last_accessed: req.last_accessed.unwrap_or(now),
        access_count: req.access_count.unwrap_or(1),
        ttl_seconds: req.ttl_seconds.unwrap_or(0),
        metadata: req.metadata,
        valid_from: req.valid_from.unwrap_or(now),
        valid_until: req.valid_until.unwrap_or(i64::MAX),
        embedding: req.embedding.unwrap_or_default(),
    };

    store.store(entity).map_err(AppError::from)?;

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(request_id = %request_id, duration_ms = duration, "Entity stored");

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn store_batch_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<StoreBatchRequest>,
) -> Result<Json<ApiResponse<StoreBatchResponse>>, AppError> {
    let start = std::time::Instant::now();
    let now = now_micros();

    let entities: Vec<MemoryEntity> = req
        .entities
        .into_iter()
        .map(|e| MemoryEntity {
            id: e.id,
            entity_type: e.entity_type,
            content: e.content,
            created_at: e.created_at.unwrap_or(now),
            last_accessed: e.last_accessed.unwrap_or(now),
            access_count: e.access_count.unwrap_or(1),
            ttl_seconds: e.ttl_seconds.unwrap_or(0),
            metadata: e.metadata,
            valid_from: e.valid_from.unwrap_or(now),
            valid_until: e.valid_until.unwrap_or(i64::MAX),
            embedding: e.embedding.unwrap_or_default(),
        })
        .collect();

    let count = store.store_batch(entities).map_err(AppError::from)?;
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, stored = count, "Batch store completed");

    Ok(Json(ApiResponse {
        data: StoreBatchResponse { stored: count },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn recall_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<RecallRequest>,
) -> Result<Json<ApiResponse<RecallResponse>>, AppError> {
    let start = std::time::Instant::now();

    let results = store
        .recall(
            req.query.as_deref().unwrap_or(""),
            req.embedding.as_deref().unwrap_or(&[]),
            req.top_k,
        )
        .map_err(AppError::from)?;

    let items: Vec<SearchResultItem> = results
        .into_iter()
        .map(|r| SearchResultItem {
            id: r.entity.id,
            content: r.entity.content,
            entity_type: r.entity.entity_type,
            score: r.score,
            metadata: r.entity.metadata,
        })
        .collect();

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(request_id = %request_id, duration_ms = duration, results = items.len(), "Recall completed");

    Ok(Json(ApiResponse {
        data: RecallResponse { results: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn recall_recent_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<RecallRecentRequest>,
) -> Result<Json<ApiResponse<EntitiesResponse>>, AppError> {
    let start = std::time::Instant::now();

    let entities = store.recall_recent(req.top_k).map_err(AppError::from)?;

    let items: Vec<EntityItem> = entities.iter().map(entity_to_item).collect();
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, count = items.len(), "Recall recent completed");

    Ok(Json(ApiResponse {
        data: EntitiesResponse { entities: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn recall_by_type_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<RecallByTypeRequest>,
) -> Result<Json<ApiResponse<EntitiesResponse>>, AppError> {
    let start = std::time::Instant::now();

    let entities = store
        .recall_by_type(&req.entity_type, req.top_k)
        .map_err(AppError::from)?;

    let items: Vec<EntityItem> = entities.iter().map(entity_to_item).collect();
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, entity_type = %req.entity_type, count = items.len(), "Recall by type completed");

    Ok(Json(ApiResponse {
        data: EntitiesResponse { entities: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn forget_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<ForgetRequest>,
) -> Result<Json<ApiResponse<ForgetResponse>>, AppError> {
    let start = std::time::Instant::now();

    let deleted = store.forget(&req.id).map_err(AppError::from)?;
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, entity_id = %req.id, deleted = deleted, "Forget completed");

    Ok(Json(ApiResponse {
        data: ForgetResponse { deleted },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn decay_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<DecayResponse>>, AppError> {
    let start = std::time::Instant::now();

    let expired = store.decay().map_err(AppError::from)?;
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, expired = expired, "Decay completed");

    Ok(Json(ApiResponse {
        data: DecayResponse { expired },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn entity_history_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<EntityHistoryRequest>,
) -> Result<Json<ApiResponse<EntityHistoryResponse>>, AppError> {
    let start = std::time::Instant::now();

    let versions = store.entity_history(&req.id).map_err(AppError::from)?;

    let items: Vec<EntityItem> = versions.iter().map(entity_to_item).collect();
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(request_id = %request_id, duration_ms = duration, entity_id = %req.id, versions = items.len(), "Entity history retrieved");

    Ok(Json(ApiResponse {
        data: EntityHistoryResponse { versions: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn consolidate_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<ConsolidateRequest>,
) -> Result<Json<ApiResponse<ConsolidationReportResponse>>, AppError> {
    let start = std::time::Instant::now();

    let config = if req.similarity_threshold.is_some()
        || req.contradiction_jaccard_max.is_some()
        || req.contradiction_cosine_min.is_some()
        || req.contradiction_length_sim_min.is_some()
        || req.max_comparisons_per_entity.is_some()
    {
        Some(lightning_core::memory::ConsolidationConfig {
            similarity_threshold: req.similarity_threshold.ok_or_else(|| {
                AppError::BadRequest("similarity_threshold is required".into())
            })?,
            contradiction_jaccard_max: req.contradiction_jaccard_max.ok_or_else(|| {
                AppError::BadRequest("contradiction_jaccard_max is required".into())
            })?,
            contradiction_cosine_min: req.contradiction_cosine_min.ok_or_else(|| {
                AppError::BadRequest("contradiction_cosine_min is required".into())
            })?,
            contradiction_length_sim_min: req.contradiction_length_sim_min.ok_or_else(|| {
                AppError::BadRequest("contradiction_length_sim_min is required".into())
            })?,
            max_comparisons_per_entity: req.max_comparisons_per_entity.ok_or_else(|| {
                AppError::BadRequest("max_comparisons_per_entity is required".into())
            })?,
        })
    } else {
        None
    };
    let report = store.inner().consolidate(config).map_err(AppError::from)?;

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(
        request_id = %request_id,
        duration_ms = duration,
        links_created = report.links_created,
        contradictions = report.contradictions_found,
        "Consolidation completed"
    );

    Ok(Json(ApiResponse {
        data: ConsolidationReportResponse {
            links_created: report.links_created,
            contradictions_found: report.contradictions_found,
            total_entities: report.total_entities,
            warnings: report.warnings,
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}
