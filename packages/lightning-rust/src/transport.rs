use crate::error::{Error, LightningError};
use reqwest::RequestBuilder;
use serde::de::DeserializeOwned;

#[derive(serde::Deserialize)]
struct ApiResponse<T> {
    data: Option<T>,
    #[allow(dead_code)]
    meta: Option<ResponseMeta>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct ResponseMeta {
    #[serde(rename = "requestId")]
    request_id: Option<String>,
    #[serde(rename = "durationMs")]
    duration_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct ErrorResponse {
    error: String,
    #[serde(default)]
    code: String,
    details: Option<serde_json::Value>,
    #[serde(rename = "requestId")]
    request_id: Option<String>,
}

pub async fn execute_and_unwrap<T: DeserializeOwned>(
    builder: RequestBuilder,
    max_content_bytes: u64,
) -> Result<T, Error> {
    let response = builder.send().await.map_err(Error::Http)?;
    let status = response.status();
    let content_length = response.content_length().unwrap_or(0);
    let body_bytes = if content_length > max_content_bytes {
        return Err(Error::Validation(format!(
            "response content length {} exceeds max {}",
            content_length, max_content_bytes
        )));
    } else {
        response.bytes().await.map_err(Error::Http)?
    };

    if !status.is_success() {
        let err: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap_or(ErrorResponse {
            error: String::from_utf8_lossy(&body_bytes).to_string(),
            code: status.to_string(),
            details: None,
            request_id: None,
        });
        return Err(Error::Lightning(LightningError {
            error: err.error,
            code: err.code,
            details: err.details,
            request_id: err.request_id,
            status: status.as_u16(),
        }));
    }

    let api_resp: ApiResponse<T> = serde_json::from_slice(&body_bytes)
        .map_err(|e| Error::Custom(format!("failed to parse response: {}", e)))?;

    match api_resp.data {
        Some(data) => Ok(data),
        None => {
            let raw: T = serde_json::from_slice(&body_bytes)
                .map_err(|e| Error::Custom(format!("failed to deserialize response: {}", e)))?;
            Ok(raw)
        }
    }
}

pub fn execute_and_unwrap_blocking<T: DeserializeOwned>(
    builder: reqwest::blocking::RequestBuilder,
    max_content_bytes: u64,
) -> Result<T, Error> {
    let response = builder.send().map_err(Error::Http)?;
    let status = response.status();
    let content_length = response.content_length().unwrap_or(0);
    let body_bytes = if content_length > max_content_bytes {
        return Err(Error::Validation(format!(
            "response content length {} exceeds max {}",
            content_length, max_content_bytes
        )));
    } else {
        response.bytes().map_err(Error::Http)?
    };

    if !status.is_success() {
        let err: ErrorResponse = serde_json::from_slice(&body_bytes).unwrap_or(ErrorResponse {
            error: String::from_utf8_lossy(&body_bytes).to_string(),
            code: status.to_string(),
            details: None,
            request_id: None,
        });
        return Err(Error::Lightning(LightningError {
            error: err.error,
            code: err.code,
            details: err.details,
            request_id: err.request_id,
            status: status.as_u16(),
        }));
    }

    let api_resp: ApiResponse<T> = serde_json::from_slice(&body_bytes)
        .map_err(|e| Error::Custom(format!("failed to parse response: {}", e)))?;

    match api_resp.data {
        Some(data) => Ok(data),
        None => {
            let raw: T = serde_json::from_slice(&body_bytes)
                .map_err(|e| Error::Custom(format!("failed to deserialize response: {}", e)))?;
            Ok(raw)
        }
    }
}
