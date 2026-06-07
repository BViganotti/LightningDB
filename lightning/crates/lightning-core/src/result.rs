use arrow::record_batch::RecordBatch;
use lightning_types::LogicalType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    pub column_names: Vec<String>,
    pub column_types: Vec<LogicalType>,
    #[serde(skip)]
    pub batches: Vec<RecordBatch>,
    pub error: Option<String>,
}

impl QueryResult {
    pub fn new_arrow(
        column_names: Vec<String>,
        column_types: Vec<LogicalType>,
        batches: Vec<RecordBatch>,
    ) -> Self {
        Self {
            column_names,
            column_types,
            batches,
            error: None,
        }
    }
    pub fn new_error(msg: String) -> Self {
        Self {
            column_names: vec![],
            column_types: vec![],
            batches: vec![],
            error: Some(msg),
        }
    }
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
    pub fn error_message(&self) -> Option<String> {
        self.error.clone()
    }
}
