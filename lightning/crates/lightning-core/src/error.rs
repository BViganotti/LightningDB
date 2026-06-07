use thiserror::Error;

#[derive(Error, Debug)]
pub enum LightningError {
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Query error: {0}")]
    Query(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<arrow::error::ArrowError> for LightningError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Internal(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, LightningError>;
