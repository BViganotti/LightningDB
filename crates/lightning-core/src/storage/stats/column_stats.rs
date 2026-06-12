use crate::processor::Value;
use crate::storage::compression::CompressionMetadata;
use lightning_types::LogicalType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PageBounds {
    pub min: Value,
    pub max: Value,
}

impl PageBounds {
    pub fn value_can_be_in_page(&self, query: &Value, logical_type: &LogicalType) -> bool {
        match logical_type {
            LogicalType::Int64
            | LogicalType::Int32
            | LogicalType::Int16
            | LogicalType::Int8 => {
                let q = query.as_number();
                let lo = self.min.as_number();
                let hi = self.max.as_number();
                q >= lo && q <= hi
            }
            LogicalType::Uint64
            | LogicalType::Uint32
            | LogicalType::Uint16
            | LogicalType::Uint8
            | LogicalType::Node(_) => {
                let q = query.as_node();
                let lo = self.min.as_node();
                let hi = self.max.as_node();
                q >= lo && q <= hi
            }
            LogicalType::Double | LogicalType::Float => {
                let q = query.as_number();
                let lo = self.min.as_number();
                let hi = self.max.as_number();
                q >= lo && q <= hi
            }
            LogicalType::String => match (query, &self.min, &self.max) {
                (Value::String(q), Value::String(lo), Value::String(hi)) => q >= lo && q <= hi,
                _ => true,
            },
            LogicalType::Bool => true,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: u64,
    pub num_values: u64,
    pub distinct_count: u64,
    pub compression_meta: Option<CompressionMetadata>,
    pub compression_ratio: Option<f64>,
    #[serde(default)]
    pub page_bounds: Vec<Option<PageBounds>>,
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
            page_bounds: Vec::new(),
        }
    }

    pub fn update(&mut self, val: &Value) {
        self.num_values += 1;
        match val {
            Value::Null => self.null_count += 1,
            _ => {
                if let Some(ref current_min) = self.min {
                    if val < current_min {
                        self.min = Some(val.clone());
                    }
                } else {
                    self.min = Some(val.clone());
                }

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

    pub fn invalidate_page_bounds(&mut self) {
        self.page_bounds.clear();
    }

    pub fn update_page_bounds(&mut self, page_idx: usize, val: &Value) {
        if val == &Value::Null {
            return;
        }
        // Cap page_bounds growth to prevent unbounded memory from corrupted page_idx
        const MAX_PAGE_BOUNDS: usize = 1_000_000;
        if page_idx >= MAX_PAGE_BOUNDS {
            return;
        }
        while self.page_bounds.len() <= page_idx {
            self.page_bounds.push(None);
        }
        if let Some(ref mut bounds) = self.page_bounds[page_idx] {
            if let Some(std::cmp::Ordering::Less) = val.partial_cmp(&bounds.min) {
                bounds.min = val.clone();
            }
            if let Some(std::cmp::Ordering::Greater) = val.partial_cmp(&bounds.max) {
                bounds.max = val.clone();
            }
        } else {
            self.page_bounds[page_idx] = Some(PageBounds {
                min: val.clone(),
                max: val.clone(),
            });
        }
    }
}
