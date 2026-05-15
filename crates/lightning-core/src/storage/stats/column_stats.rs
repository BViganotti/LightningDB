use crate::processor::Value;
use crate::storage::compression::CompressionMetadata;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: u64,
    pub num_values: u64,
    pub distinct_count: u64,
    pub compression_meta: Option<CompressionMetadata>,
    /// Achieved compression ratio (uncompressed_size / compressed_size).
    /// Higher is better. None means not yet measured.
    pub compression_ratio: Option<f64>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnStats {
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,
            null_count: 0,
            num_values: 0,
            distinct_count: 0,
            compression_meta: None,
            compression_ratio: None,
        }
    }

    pub fn update(&mut self, val: &Value) {
        self.num_values += 1;
        match val {
            Value::Null => self.null_count += 1,
            _ => {
                // Update min
                if let Some(ref current_min) = self.min {
                    if val < current_min {
                        self.min = Some(val.clone());
                    }
                } else {
                    self.min = Some(val.clone());
                }

                // Update max
                if let Some(ref current_max) = self.max {
                    if val > current_max {
                        self.max = Some(val.clone());
                    }
                } else {
                    self.max = Some(val.clone());
                }
            }
        }
    }
}
