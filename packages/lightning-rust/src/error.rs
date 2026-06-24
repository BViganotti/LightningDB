use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct LightningError {
    pub error: String,
    #[serde(default)]
    pub code: String,
    pub details: Option<serde_json::Value>,
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
    pub status: u16,
}

impl fmt::Display for LightningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.status, self.code, self.error)
    }
}

impl std::error::Error for LightningError {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("LightningDB server error: {0}")]
    Lightning(#[from] LightningError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Circuit breaker open: {0}")]
    CircuitBreakerOpen(String),

    #[error("Max retries exceeded ({0} attempts): {1}")]
    MaxRetriesExceeded(usize, String),

    #[error("SSE stream error: {0}")]
    Stream(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("{0}")]
    Custom(String),
}

impl Error {
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Lightning(e) => matches!(e.status, 429 | 500 | 502 | 503 | 504),
            Error::Http(e) => e.is_timeout() || e.is_connect(),
            _ => false,
        }
    }

    pub fn status_code(&self) -> Option<u16> {
        match self {
            Error::Lightning(e) => Some(e.status),
            Error::Http(e) => e.status().map(|s| s.as_u16()),
            _ => None,
        }
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Custom(s)
    }
}
