use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int64Array, StringArray, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::stream::Stream;

pub fn arrow_row_to_json(batch: &RecordBatch, row_idx: usize) -> serde_json::Value {
    let schema = batch.schema();
    let mut map = serde_json::Map::new();
    for col_idx in 0..batch.num_columns() {
        let col_name = schema.field(col_idx).name().to_string();
        let arr = batch.column(col_idx);
        let value = if arr.is_null(row_idx) {
            serde_json::Value::Null
        } else {
            match arr.data_type() {
                DataType::Int64 => {
                    arr.as_any().downcast_ref::<Int64Array>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                DataType::UInt64 => {
                    arr.as_any().downcast_ref::<UInt64Array>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                DataType::Float32 => {
                    arr.as_any().downcast_ref::<Float32Array>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                DataType::Float64 => {
                    arr.as_any().downcast_ref::<Float64Array>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                DataType::Boolean => {
                    arr.as_any().downcast_ref::<BooleanArray>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    arr.as_any().downcast_ref::<StringArray>()
                        .map(|c| serde_json::json!(c.value(row_idx)))
                        .unwrap_or(serde_json::Value::Null)
                }
                _ => {
                    // Fallback: try StringArray
                    if let Some(c) = arr.as_any().downcast_ref::<StringArray>() {
                        serde_json::json!(c.value(row_idx))
                    } else {
                        serde_json::Value::Null
                    }
                }
            }
        };
        map.insert(col_name, value);
    }
    serde_json::Value::Object(map)
}

pub fn build_query_stream(
    db: Arc<lightning::Database>,
    query: String,
    params: Option<std::collections::HashMap<String, lightning_core::Value>>,
) -> impl Stream<Item = Result<serde_json::Value, String>> {
    let stream = async_stream::stream! {
        let conn = db.connect();
        let rx = match conn.execute_stream(&query, params) {
            Ok(rx) => rx,
            Err(e) => {
                yield Err(e.to_string());
                return;
            }
        };
        while let Ok(result) = rx.recv() {
            match result {
                Ok(chunk) => {
                    let batch = &chunk.batch;
                    for row_idx in 0..batch.num_rows() {
                        let row = arrow_row_to_json(batch, row_idx);
                        yield Ok(row);
                    }
                }
                Err(e) => {
                    yield Err(e.to_string());
                    return;
                }
            }
        }
    };
    stream
}
