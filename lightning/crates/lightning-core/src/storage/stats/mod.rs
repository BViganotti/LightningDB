pub mod column_stats;
pub mod table_stats;

pub use column_stats::*;
pub use table_stats::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    // For now, let's keep it simple.
}
