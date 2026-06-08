use crate::processor::Value;
use crate::storage::compression::{
    CompressionAlg, CompressionMetadata, CompressionType, Uncompressed,
};
use crate::Result;
use lightning_types::LogicalType;

pub struct CompressionAnalyzer;

impl CompressionAnalyzer {
    pub fn analyze_integer_chunk(values: &[Value], data_type: &LogicalType) -> CompressionMetadata {
        if values.is_empty() {
            return CompressionMetadata::new(
                Value::Null,
                Value::Null,
                CompressionType::Uncompressed,
                0,
            );
        }

        let mut min = values[0].clone();
        let mut max = values[0].clone();
        let mut all_same = true;
        let mut count_same = 0;
        let mut prev = &values[0];

        for val in values {
            if val != &values[0] {
                all_same = false;
            }
            if val == prev {
                count_same += 1;
            }
            prev = val;
            if val < &min {
                min = val.clone();
            }
            if val > &max {
                max = val.clone();
            }
        }

        if all_same {
            return CompressionMetadata::new(min, max, CompressionType::Constant, 0);
        }

        if count_same > values.len() * 3 / 4 && values.len() > 64 {
            return CompressionMetadata::new(min, max, CompressionType::Rle, 0);
        }

        // Count distinct values for Dictionary encoding
        use std::collections::HashSet;
        let mut distinct = HashSet::new();
        for val in values {
            distinct.insert(val);
        }
        if distinct.len() < values.len() / 4 && values.len() > 64 {
            // Low cardinality, Dictionary might be good
            return CompressionMetadata::new(min, max, CompressionType::Dict, 0);
        }

        match (min.clone(), max.clone()) {
            (Value::Node(v1), Value::Node(v2)) => {
                let range = v2 - v1;
                let bit_width = Self::calculate_bit_width(range);
                if bit_width < 64 {
                    if v1 > 0 {
                        return CompressionMetadata::new(
                            min,
                            max,
                            CompressionType::FixedFrameOfReference,
                            bit_width as u8,
                        );
                    } else {
                        return CompressionMetadata::new(
                            min,
                            max,
                            CompressionType::IntegerBitpacking,
                            bit_width as u8,
                        );
                    }
                }
            }
            (Value::Number(v1), Value::Number(v2)) => {
                if *data_type == LogicalType::Int64 || *data_type == LogicalType::Int32 {
                    let min_val = v1 as i64;
                    let max_val = v2 as i64;
                    let range = (max_val as i128 - min_val as i128) as u64;
                    let bit_width = Self::calculate_bit_width(range);
                    if bit_width < 64 {
                        return CompressionMetadata::new(
                            min,
                            max,
                            CompressionType::FixedFrameOfReference,
                            bit_width as u8,
                        );
                    }
                }
            }
            _ => (),
        };

        CompressionMetadata::new(min, max, CompressionType::Uncompressed, 0)
    }

    pub fn analyze_float_chunk(values: &[Value]) -> CompressionMetadata {
        if values.is_empty() {
            return CompressionMetadata::new(
                Value::Null,
                Value::Null,
                CompressionType::Uncompressed,
                0,
            );
        }

        let mut min = values[0].clone();
        let mut max = values[0].clone();

        for val in values {
            if val < &min {
                min = val.clone();
            }
            if val > &max {
                max = val.clone();
            }
        }

        // For now, ALP is always a candidate for floats if they aren't uncompressed
        // In a real implementation we'd check if ALP actually saves space
        CompressionMetadata::new(min, max, CompressionType::Alp, 0)
    }

    pub fn analyze_string_chunk(values: &[Value]) -> CompressionMetadata {
        if values.is_empty() {
            return CompressionMetadata::new(
                Value::Null,
                Value::Null,
                CompressionType::Uncompressed,
                0,
            );
        }

        let mut all_same = true;
        let p = &values[0];
        for v in values {
            if v != p {
                all_same = false;
                break;
            }
        }

        if all_same {
            return CompressionMetadata::new(p.clone(), p.clone(), CompressionType::Constant, 0);
        }

        use std::collections::HashSet;
        let mut distinct = HashSet::new();
        for val in values {
            distinct.insert(val);
        }
        if distinct.len() < values.len() / 8 && values.len() > 16 {
            return CompressionMetadata::new(Value::Null, Value::Null, CompressionType::Dict, 0);
        }

        CompressionMetadata::new(Value::Null, Value::Null, CompressionType::Uncompressed, 0)
    }

    pub fn analyze_column(
        _col: &crate::storage::column::Column,
        _bm: &crate::storage::buffer_manager::BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<(Box<dyn CompressionAlg>, CompressionMetadata)> {
        // Simplified analysis for the port
        // In a real implementation, we'd sample the column data
        let meta =
            CompressionMetadata::new(Value::Null, Value::Null, CompressionType::Uncompressed, 0);
        Ok((Box::new(Uncompressed { element_size: 8 }), meta))
    }

    fn calculate_bit_width(range: u64) -> u32 {
        if range == 0 {
            return 0;
        }
        64 - range.leading_zeros()
    }
}
