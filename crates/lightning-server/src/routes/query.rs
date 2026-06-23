use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use futures::stream::StreamExt;
use lightning::types::TypedQueryResult;

use crate::error::AppError;
use crate::extract::{DbConnection, RequestId};
use crate::models::request::{QueryRequest, SnapshotSelector};
use crate::models::response::{ApiResponse, QueryResponse, ResponseMeta};
use crate::server::AppState;

pub async fn query_handler(
    conn: DbConnection,
    State(state): State<Arc<AppState>>,
    RequestId(request_id): RequestId,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResponse>>, AppError> {
    let start = std::time::Instant::now();

    let query_str = req.query.clone();
    let timeout_ms = req.effective_timeout_ms();
    let timeout_dur = std::time::Duration::from_millis(timeout_ms);

    let params = req.params.map(|p| {
        p.into_iter()
            .map(|(k, v)| (k, lightning_core::Value::from_json(&v)))
            .collect::<std::collections::HashMap<_, _>>()
    });

    // Resolve snapshot timestamp from either explicit snapshot_ts or snapshot selector
    let resolved_snapshot_ts: Option<u64> = if let Some(sel) = &req.snapshot {
        resolve_snapshot_selector(sel).map(|ts| ts as u64)
    } else {
        req.snapshot_ts
    };

    // Acquire concurrency permit to prevent spawn_blocking pool exhaustion
    let _permit = state.query_semaphore.acquire().await
        .map_err(|_| AppError::Internal("Query concurrency limit exceeded".into()))?;

    let result = tokio::time::timeout(timeout_dur, tokio::task::spawn_blocking(move || {
        if let Some(ts) = resolved_snapshot_ts {
            conn.execute_at(&req.query, ts, params)
        } else {
            conn.execute(&req.query, params)
        }
    }))
    .await
    .map_err(|_| {
        tracing::warn!(%request_id, "Query timed out after {}ms", timeout_ms);
        AppError::Timeout(timeout_ms)
    })?
    .map_err(|join_err| {
        if join_err.is_panic() {
            let panic_msg = join_err.into_panic();
            let msg = if let Some(s) = panic_msg.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_msg.downcast_ref::<String>() {
                s.clone()
            } else {
                format!("{:?}", panic_msg)
            };
            tracing::error!(%request_id, "Query panicked: {}", msg);
            AppError::Internal(msg)
        } else {
            AppError::Internal(join_err.to_string())
        }
    })?
    .map_err(|e| {
        tracing::error!(%request_id, "Query error: {}", e);
        AppError::from(e)
    })?;

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

    let sse_stream = stream.map(|result| {
        let event = match result {
            Ok(row) => Event::default().json_data(row),
            Err(e) => Event::default().json_data(serde_json::json!({"error": e})),
        };
        Ok(event.unwrap_or_else(|_| Event::default().data("{}")))
    });

    let final_stream = futures::stream::once(async {
        Ok(Event::default()
            .json_data(serde_json::json!({"done": true}))
            .unwrap_or_else(|_| Event::default().data("{}")))
    });

    let combined = sse_stream.chain(final_stream);

    Ok(axum::response::sse::Sse::new(combined).keep_alive(
        KeepAlive::new().interval(std::time::Duration::from_secs(15)),
    ))
}

fn resolve_snapshot_selector(sel: &SnapshotSelector) -> Option<i64> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);

    if let Some(iso) = &sel.iso {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(iso) {
            return Some(dt.timestamp_micros());
        }
    }

    if let Some(relative) = &sel.relative {
        if let Some(dur) = parse_relative_duration(relative) {
            return Some(now_micros - dur);
        }
    }

    if let Some(label) = &sel.label {
        let day_micros: i64 = 86_400_000_000;
        return match label.as_str() {
            "current" => Some(now_micros),
            "yesterday" => Some(now_micros - day_micros),
            "oldest" => Some(0),
            "7d_ago" => Some(now_micros - 7 * day_micros),
            "30d_ago" => Some(now_micros - 30 * day_micros),
            _ => None,
        };
    }

    None
}

fn parse_relative_duration(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() < 2 {
        return None;
    }
    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str.parse().ok()?;
    match unit {
        "s" => Some(num * 1_000_000),
        "m" => Some(num * 60_000_000),
        "h" => Some(num * 3_600_000_000),
        "d" => Some(num * 86_400_000_000),
        _ => None,
    }
}
