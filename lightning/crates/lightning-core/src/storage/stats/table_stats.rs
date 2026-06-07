use crate::storage::stats::ColumnStats;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub cardinality: u64,
    pub column_stats: Vec<ColumnStats>,
}

impl TableStats {
    pub fn new(num_columns: usize) -> Self {
        Self {
            cardinality: 0,
            column_stats: vec![ColumnStats::new(); num_columns],
        }
    }
}
