use axum::Json;

pub async fn health_handler() -> Json<serde_json::Value> {
    let response = serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    });
    Json(response)
}
