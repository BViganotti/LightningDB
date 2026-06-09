use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::models::response::ErrorResponse;

#[derive(Debug)]
pub struct AppError(pub lightning_core::LightningError);

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<lightning_core::LightningError> for AppError {
    fn from(e: lightning_core::LightningError) -> Self {
        AppError(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self.0 {
            lightning_core::LightningError::Query(msg) => {
                if msg.contains("not found") || msg.contains("does not exist") {
                    (StatusCode::NOT_FOUND, Some("NOT_FOUND".into()))
                } else if msg.contains("already exists") {
                    (StatusCode::CONFLICT, Some("ALREADY_EXISTS".into()))
                } else if msg.contains("syntax") || msg.contains("parse") {
                    (StatusCode::BAD_REQUEST, Some("SYNTAX_ERROR".into()))
                } else {
                    (StatusCode::BAD_REQUEST, Some("QUERY_ERROR".into()))
                }
            }
            lightning_core::LightningError::Config(_) => {
                (StatusCode::BAD_REQUEST, Some("CONFIG_ERROR".into()))
            }
            lightning_core::LightningError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Some("INTERNAL_ERROR".into()))
            }
            lightning_core::LightningError::Database(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, Some("DATABASE_ERROR".into()))
            }
            lightning_core::LightningError::Io(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Some("IO_ERROR".into()))
            }
        };

        let error_response = ErrorResponse {
            error: self.0.to_string(),
            code,
            details: None,
            request_id: None,
        };

        (status, Json(error_response)).into_response()
    }
}
