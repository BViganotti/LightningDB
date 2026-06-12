use axum::extract::State;
use axum::Json;
use std::sync::Arc;

use crate::server::AppState;

pub async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    // Verify database connectivity by attempting a simple query
    let db_ok = state.db.connect().query("RETURN 1").is_ok();

    let status = if db_ok { "ok" } else { "degraded" };
    let response = serde_json::json!({
        "status": status,
        "version": env!("CARGO_PKG_VERSION"),
        "database": if db_ok { "connected" } else { "error" },
    });
    Json(response)
}
