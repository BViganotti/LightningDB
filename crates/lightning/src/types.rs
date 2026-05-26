use arrow::array::*;
use arrow::record_batch::RecordBatch;
use serde::Serialize;

pub use lightning_core::{
    DatabaseMetrics, LightningError, QueryResult, Result, SyncMode, SystemConfig, Value,
};

/// A single row of query results represented as a map of column name to JSON value.
pub type Row = serde_json::Map<String, serde_json::Value>;

/// Typed query result containing deserialized rows.
#[derive(Debug, Clone)]
pub struct TypedQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub num_rows: usize,
}

impl TypedQueryResult {
    /// Build a TypedQueryResult from Arrow record batches.
    pub fn from_batches(batches: &[RecordBatch]) -> Self {
        use serde_json::json;

        let mut rows: Vec<Row> = Vec::new();
        let col_names: Vec<String> = batches
            .first()
            .map(|b| {
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect()
            })
            .unwrap_or_default();

        for batch in batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for col_idx in 0..batch.num_columns() {
                    let col_name = schema.field(col_idx).name();
                    let col = batch.column(col_idx);
                    let arr = col.as_ref();

                    let value: serde_json::Value = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        match arr.data_type() {
                            t if t == &arrow::datatypes::DataType::Int8 => {
                                let c = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Int16 => {
                                let c = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Int32 => {
                                let c = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Int64 => {
                                let c = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::UInt64 => {
                                let c = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Float32 => {
                                let c = arr.as_any().downcast_ref::<Float32Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Float64 => {
                                let c = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Boolean => {
                                let c = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                                json!(c.value(row_idx))
                            }
                            t if t == &arrow::datatypes::DataType::Utf8
                                || t == &arrow::datatypes::DataType::LargeUtf8 =>
                            {
                                let c = arr.as_any().downcast_ref::<StringArray>().unwrap();
                                json!(c.value(row_idx))
                            }
                            _ => {
                                let c = arr.as_any().downcast_ref::<StringArray>();
                                match c {
                                    Some(s) => json!(s.value(row_idx)),
                                    None => serde_json::Value::Null,
                                }
                            }
                        }
                    };
                    row.insert(col_name.to_string(), value);
                }
                rows.push(row);
            }
        }

        let num_rows = rows.len();
        Self {
            columns: col_names,
            rows,
            num_rows,
        }
    }

    /// Serialize to a JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

impl Serialize for TypedQueryResult {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("columns", &self.columns)?;
        map.serialize_entry("rows", &self.rows)?;
        map.serialize_entry("num_rows", &self.num_rows)?;
        map.end()
    }
}

/// Convert a core QueryResult into a TypedQueryResult.
impl From<QueryResult> for TypedQueryResult {
    fn from(result: QueryResult) -> Self {
        Self::from_batches(&result.batches)
    }
}
