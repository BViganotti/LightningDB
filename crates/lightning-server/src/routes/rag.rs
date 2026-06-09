use axum::Json;

use crate::error::AppError;
use crate::extract::{AppStore, RequestId};
use crate::models::request::RagRequest;
use crate::models::response::{ApiResponse, RagResponse, ResponseMeta, SourceRef};

pub async fn rag_query_handler(
    AppStore(store): AppStore,
    RequestId(request_id): RequestId,
    Json(req): Json<RagRequest>,
) -> Result<Json<ApiResponse<RagResponse>>, AppError> {
    let start = std::time::Instant::now();

    let config = lightning_core::memory::RagConfig {
        expansion_depth: req.expansion_depth.unwrap_or(3),
        search_weight: req.search_weight.unwrap_or(2.0),
        recency_weight: req.recency_weight.unwrap_or(0.3),
        degree_weight: req.degree_weight.unwrap_or(0.0),
        max_context_tokens: req.max_tokens.unwrap_or(4096),
        ..Default::default()
    };

    let result = store
        .inner()
        .rag_query_with_config(
            &req.query,
            req.embedding.as_deref().unwrap_or(&[]),
            req.top_k,
            &config,
        )
        .map_err(AppError::from)?;

    let sources: Vec<SourceRef> = result
        .source_details
        .into_iter()
        .map(|s| SourceRef {
            id: s.id,
            score: s.score,
            entity_type: s.entity_type,
            excerpt: s.excerpt,
        })
        .collect();

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(
        request_id = %request_id,
        duration_ms = duration,
        total_sources = result.total_sources,
        "RAG query completed"
    );

    Ok(Json(ApiResponse {
        data: RagResponse {
            context: result.context,
            sources,
            total_sources: result.total_sources,
            warnings: result.warnings,
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}
