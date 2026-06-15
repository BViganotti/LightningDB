use axum::Json;

use crate::error::AppError;
use crate::extract::RequestId;
use crate::models::response::{ApiResponse, ResponseMeta, SnapshotInfo, SnapshotsResponse};

pub async fn snapshots_handler(
    RequestId(request_id): RequestId,
) -> Result<Json<ApiResponse<SnapshotsResponse>>, AppError> {
    let start = std::time::Instant::now();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);

    let day_micros: i64 = 86_400_000_000;

    let snapshots = vec![
        SnapshotInfo {
            ts: now,
            iso: iso_from_micros(now),
            age_days: 0,
            label: "current".to_string(),
        },
        SnapshotInfo {
            ts: now - day_micros,
            iso: iso_from_micros(now - day_micros),
            age_days: 1,
            label: "yesterday".to_string(),
        },
        SnapshotInfo {
            ts: now - 7 * day_micros,
            iso: iso_from_micros(now - 7 * day_micros),
            age_days: 7,
            label: "7d_ago".to_string(),
        },
        SnapshotInfo {
            ts: now - 30 * day_micros,
            iso: iso_from_micros(now - 30 * day_micros),
            age_days: 30,
            label: "30d_ago".to_string(),
        },
    ];

    let duration = start.elapsed().as_millis() as u64;

    Ok(Json(ApiResponse {
        data: SnapshotsResponse { snapshots },
        meta: ResponseMeta {
            request_id,
            duration_ms: duration,
        },
    }))
}

fn iso_from_micros(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    let d = chrono::DateTime::from_timestamp(secs, nanos)
        .unwrap_or_default();
    d.to_rfc3339()
}
