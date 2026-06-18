use axum::Json;

use crate::error::AppError;
use crate::extract::{AppStore, RequestId};
use crate::models::request::{AssociateRequest, ExpandRequest};
use crate::models::response::{ApiResponse, EntityItem, ExpandResponse, ResponseMeta};

pub async fn associate_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<AssociateRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    store
        .associate(&req.src_id, &req.dst_id, &req.rel_type, req.weight)
        .map_err(AppError::from)?;

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(
        request_id = %request_id,
        duration_ms = duration,
        src_id = %req.src_id,
        dst_id = %req.dst_id,
        rel_type = %req.rel_type,
        "Association created"
    );

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn expand_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<ExpandRequest>,
) -> Result<Json<ApiResponse<ExpandResponse>>, AppError> {
    req.validate().map_err(AppError::BadRequest)?;
    let start = std::time::Instant::now();

    let edge_types: Vec<&str> = req
        .edge_types
        .as_deref()
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();

    let entities = store
        .expand(&req.entity_id, req.hops, &edge_types)
        .map_err(AppError::from)?;

    let items: Vec<EntityItem> = entities
        .iter()
        .map(|e| EntityItem {
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
        })
        .collect();

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(
        request_id = %request_id,
        duration_ms = duration,
        entity_id = %req.entity_id,
        hops = req.hops,
        neighbors = items.len(),
        "Graph expansion completed"
    );

    Ok(Json(ApiResponse {
        data: ExpandResponse { entities: items },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}
