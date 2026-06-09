use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::State;
use axum::Json;

use crate::error::AppError;
use crate::extract::RequestId;
use crate::models::response::{ApiResponse, ResponseMeta};
use crate::server::AppState;

pub async fn checkpoint_handler(
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    state.db.checkpoint().map_err(AppError::from)?;

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(request_id = %request_id, duration_ms = duration, "Checkpoint completed");

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn vacuum_handler(
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = std::time::Instant::now();

    state.db.vacuum().map_err(AppError::from)?;

    let duration = start.elapsed().as_millis() as u64;
    tracing::info!(request_id = %request_id, duration_ms = duration, "Vacuum completed");

    Ok(Json(ApiResponse {
        data: (),
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn metrics_handler(
    State(state): State<Arc<AppState>>,
) -> (axum::http::StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    let db_metrics = state.db.metrics();

    let total_queries = db_metrics.total_queries.load(Ordering::Relaxed);
    let total_checkpoints = db_metrics.total_checkpoints.load(Ordering::Relaxed);
    let buffer_hit_rate = db_metrics.buffer_hit_rate();
    let uptime_secs = state.config.uptime_secs();
    let request_count = state.request_counter.load(Ordering::Relaxed);

    let metrics = format!(
        r#"# HELP lightning_queries_total Total queries executed
# TYPE lightning_queries_total counter
lightning_queries_total {}

# HELP lightning_checkpoints_total Total checkpoints performed
# TYPE lightning_checkpoints_total counter
lightning_checkpoints_total {}

# HELP lightning_buffer_hit_rate Buffer pool hit rate (0.0 to 1.0)
# TYPE lightning_buffer_hit_rate gauge
lightning_buffer_hit_rate {:.4}

# HELP lightning_http_requests_total Total HTTP requests processed
# TYPE lightning_http_requests_total counter
lightning_http_requests_total {}

# HELP lightning_uptime_seconds Server uptime in seconds
# TYPE lightning_uptime_seconds gauge
lightning_uptime_seconds {}

"#,
        total_queries, total_checkpoints, buffer_hit_rate, request_count, uptime_secs,
    );

    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        metrics,
    )
}
