use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use futures::stream::StreamExt;
use lightning::types::TypedQueryResult;

use crate::error::AppError;
use crate::extract::{DbConnection, RequestId};
use crate::models::request::QueryRequest;
use crate::models::response::{ApiResponse, QueryResponse, ResponseMeta};
use crate::server::AppState;

pub async fn query_handler(
    DbConnection(conn): DbConnection,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResponse>>, AppError> {
    let start = std::time::Instant::now();

    let query_str = req.query.clone();
    let timeout_ms = if req.timeout_ms > 0 {
        req.timeout_ms
    } else {
        state.config.query_timeout_ms.unwrap_or(30_000)
    };
    let timeout_dur = std::time::Duration::from_millis(timeout_ms);

    let params = req.params.map(|p| {
        p.into_iter()
            .map(|(k, v)| (k, lightning_core::Value::from_json(&v)))
            .collect::<std::collections::HashMap<_, _>>()
    });

    let result = tokio::time::timeout(timeout_dur, tokio::task::spawn_blocking(move || {
        if let Some(ts) = req.snapshot_ts {
            conn.execute_at(&req.query, ts, params)
        } else {
            conn.execute(&req.query, params)
        }
    }))
    .await
    .map_err(|_| AppError::Timeout(timeout_ms))?
    .map_err(|e| AppError::Internal(e.to_string()))?
    .map_err(AppError::from)?;

    let typed = TypedQueryResult::from(result);
    let duration = start.elapsed().as_millis() as u64;

    tracing::info!(
        request_id = %request_id,
        query = %query_str,
        duration_ms = duration,
        num_rows = typed.num_rows,
        "Query executed"
    );

    Ok(Json(ApiResponse {
        data: QueryResponse {
            columns: typed.columns,
            rows: typed.rows,
            num_rows: typed.num_rows,
        },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

pub async fn query_stream_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Result<
    axum::response::sse::Sse<impl futures::stream::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>>,
    AppError,
> {
    use axum::response::sse::{Event, KeepAlive};
    
    

    let db = state.db.clone();
    let query = req.query.clone();

    let params = req.params.map(|p| {
        p.into_iter()
            .map(|(k, v)| (k, lightning_core::Value::from_json(&v)))
            .collect::<std::collections::HashMap<_, _>>()
    });

    let stream = crate::streaming::build_query_stream(db, query, params);

    let sse_stream = stream.map(|result| match result {
        Ok(row) => Ok(Event::default().json_data(row).unwrap()),
        Err(e) => Ok(Event::default()
            .json_data(serde_json::json!({"error": e}))
            .unwrap()),
    });

    let final_stream =
        futures::stream::once(async { Ok(Event::default().json_data(serde_json::json!({"done": true})).unwrap()) });

    let combined = sse_stream.chain(final_stream);

    Ok(axum::response::sse::Sse::new(combined).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}
