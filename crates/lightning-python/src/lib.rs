use lightning_core::memory::{DEFAULT_EMBEDDING_DIM, MemoryEntity, MemoryStore as CoreMemoryStore};
use lightning_core::{Database, LightningError, SystemConfig, SyncMode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

fn to_py_err(e: LightningError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn entity_to_pydict(py: Python<'_>, e: &MemoryEntity) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("id", &e.id)?;
    dict.set_item("content", &e.content)?;
    dict.set_item("type", &e.entity_type)?;
    dict.set_item("metadata", &e.metadata)?;
    dict.set_item("created_at", e.created_at)?;
    dict.set_item("last_accessed", e.last_accessed)?;
    dict.set_item("access_count", e.access_count)?;
    dict.set_item("ttl_seconds", e.ttl_seconds)?;
    dict.set_item("valid_from", e.valid_from)?;
    dict.set_item("valid_until", e.valid_until)?;
    dict.set_item("embedding", e.embedding.clone())?;
    Ok(dict.into())
}

fn search_result_to_pydict(py: Python<'_>, r: &lightning_core::memory::SearchResult) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("id", &r.entity.id)?;
    dict.set_item("content", &r.entity.content)?;
    dict.set_item("type", &r.entity.entity_type)?;
    dict.set_item("score", r.score)?;
    dict.set_item("metadata", &r.entity.metadata)?;
    dict.set_item("embedding", r.entity.embedding.clone())?;
    Ok(dict.into())
}

fn extract_str(dict: &Bound<'_, PyDict>, key: &str) -> String {
    dict.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default()
}

fn extract_i64(dict: &Bound<'_, PyDict>, key: &str, default: i64) -> i64 {
    dict.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| v.extract::<i64>().ok())
        .unwrap_or(default)
}

fn extract_embedding(dict: &Bound<'_, PyDict>, key: &str) -> Vec<f32> {
    dict.get_item(key)
        .ok()
        .flatten()
        .and_then(|v| {
            v.downcast::<PyList>()
                .ok()
                .map(|list| list.iter().filter_map(|item| item.extract::<f32>().ok()).collect())
        })
        .unwrap_or_default()
}

#[pyclass]
struct LightningDatabase {
    db: Arc<Database>,
}

#[pymethods]
impl LightningDatabase {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let config = SystemConfig {
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = Database::new(path, config).map_err(to_py_err)?;
        Ok(Self { db })
    }

    fn execute(&self, query: &str) -> PyResult<String> {
        let conn = self.db.connect();
        let result = conn.query(query).map_err(to_py_err)?;
        let col_names: Vec<String> = result
            .batches
            .first()
            .map(|b| b.schema().fields().iter().map(|f| f.name().to_string()).collect())
            .unwrap_or_default();
        let mut rows: Vec<serde_json::Value> = Vec::new();
        for batch in &result.batches {
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for col_idx in 0..batch.num_columns() {
                    let col_name = schema.field(col_idx).name();
                    let arr = batch.column(col_idx);
                    let value: serde_json::Value = if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        use arrow::array::*;
                        macro_rules! extract {
                            ($ty:ident, $method:ident) => {{
                                arr.as_any().downcast_ref::<$ty>()
                                    .map(|c| serde_json::json!(c.value(row_idx)))
                                    .unwrap_or(serde_json::Value::Null)
                            }};
                        }
                        match arr.data_type() {
                            t if t == &arrow::datatypes::DataType::Int8 => extract!(Int8Array, value),
                            t if t == &arrow::datatypes::DataType::Int16 => extract!(Int16Array, value),
                            t if t == &arrow::datatypes::DataType::Int32 => extract!(Int32Array, value),
                            t if t == &arrow::datatypes::DataType::Int64 => extract!(Int64Array, value),
                            t if t == &arrow::datatypes::DataType::Float32 => extract!(Float32Array, value),
                            t if t == &arrow::datatypes::DataType::Float64 => extract!(Float64Array, value),
                            t if t == &arrow::datatypes::DataType::Boolean => extract!(BooleanArray, value),
                            t if t == &arrow::datatypes::DataType::Utf8 || t == &arrow::datatypes::DataType::LargeUtf8 => extract!(StringArray, value),
                            _ => serde_json::Value::Null,
                        }
                    };
                    row.insert(col_name.to_string(), value);
                }
                rows.push(serde_json::Value::Object(row));
            }
        }
        let response = serde_json::json!({
            "columns": col_names,
            "rows": rows,
            "num_rows": rows.len(),
        });
        serde_json::to_string(&response)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization failed: {}", e)))
    }
}

#[pyclass]
struct PyMemoryStore {
    inner: CoreMemoryStore,
}

#[pymethods]
impl PyMemoryStore {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let config = SystemConfig {
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = Database::new(path, config).map_err(to_py_err)?;
        let conn = db.connect();
        Ok(Self { inner: CoreMemoryStore::new(conn, DEFAULT_EMBEDDING_DIM) })
    }

    #[staticmethod]
    fn with_embedding_dim(path: &str, dim: usize) -> PyResult<Self> {
        let config = SystemConfig {
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = Database::new(path, config).map_err(to_py_err)?;
        let conn = db.connect();
        Ok(Self { inner: CoreMemoryStore::new(conn, dim) })
    }

    #[staticmethod]
    fn now_micros_for_test() -> i64 {
        CoreMemoryStore::now_micros_for_test()
    }

    fn embedding_dim(&self) -> usize {
        self.inner.embedding_dim()
    }

    #[pyo3(signature = (id, content, entity_type, metadata=None, embedding=None))]
    fn store(&self, id: &str, content: &str, entity_type: &str, metadata: Option<&str>, embedding: Option<Vec<f32>>) -> PyResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let entity = MemoryEntity {
            id: id.to_string(),
            entity_type: entity_type.to_string(),
            content: content.to_string(),
            created_at: now,
            last_accessed: now,
            access_count: 0,
            ttl_seconds: 0,
            metadata: metadata.unwrap_or("{}").to_string(),
            valid_from: now,
            valid_until: 0,
            embedding: embedding.unwrap_or_default(),
        };
        self.inner.store(entity).map_err(to_py_err)
    }

    #[pyo3(signature = (query, top_k=None))]
    fn recall(&self, query: &str, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let results = self.inner.recall(query, &[], k).map_err(to_py_err)?;
        Python::with_gil(|py| results.iter().map(|r| search_result_to_pydict(py, r)).collect::<PyResult<Vec<_>>>())
    }

    #[pyo3(signature = (query, embedding, top_k=None))]
    fn recall_with_embedding(&self, query: &str, embedding: Vec<f32>, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let results = self.inner.recall(query, &embedding, k).map_err(to_py_err)?;
        Python::with_gil(|py| results.iter().map(|r| search_result_to_pydict(py, r)).collect::<PyResult<Vec<_>>>())
    }

    #[pyo3(signature = (top_k=None))]
    fn recall_recent(&self, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self.inner.recall_recent(k).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    #[pyo3(signature = (entity_type, top_k=None))]
    fn recall_by_type(&self, entity_type: &str, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self.inner.recall_by_type(entity_type, k).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    #[pyo3(signature = (at_micros, top_k=None))]
    fn recall_at_time(&self, at_micros: i64, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self.inner.recall_at_time(at_micros, k).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    #[pyo3(signature = (start, end, top_k=None))]
    fn recall_by_time(&self, start: i64, end: i64, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self.inner.recall_by_time(start, end, k).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    fn recall_stream(&self, query: &str, embedding: Vec<f32>, top_k: usize) -> PyResult<Vec<PyObject>> {
        let rx = self.inner.recall_stream(query, &embedding, top_k).map_err(to_py_err)?;
        Python::with_gil(|py| {
            let mut results = Vec::new();
            while let Ok(Ok(r)) = rx.recv() {
                results.push(search_result_to_pydict(py, &r)?);
            }
            Ok(results)
        })
    }

    #[pyo3(signature = (query, embedding, top_k=None))]
    fn rag_query(&self, query: &str, embedding: Vec<f32>, top_k: Option<usize>) -> PyResult<PyObject> {
        let k = top_k.unwrap_or(5);
        let result = self.inner.rag_query(query, &embedding, k).map_err(to_py_err)?;
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("context", result.context)?;
            dict.set_item("sources", result.sources)?;
            dict.set_item("total_sources", result.total_sources)?;
            dict.set_item("query", result.query)?;
            Ok(dict.into())
        })
    }

    #[pyo3(signature = (src_id, dst_id, rel_type, weight=None))]
    fn associate(&self, src_id: &str, dst_id: &str, rel_type: &str, weight: Option<f64>) -> PyResult<()> {
        self.inner.associate(src_id, dst_id, rel_type, weight.unwrap_or(1.0)).map_err(to_py_err)
    }

    #[pyo3(signature = (entity_id, hops=None))]
    fn expand(&self, entity_id: &str, hops: Option<u32>) -> PyResult<Vec<PyObject>> {
        let h = hops.unwrap_or(1);
        let entities = self.inner.expand(entity_id, h, &["Relates"]).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    fn forget(&self, entity_id: &str) -> PyResult<bool> {
        self.inner.forget(entity_id).map_err(to_py_err)
    }

    fn decay(&self) -> PyResult<usize> {
        self.inner.decay().map_err(to_py_err)
    }

    fn entity_history(&self, entity_id: &str) -> PyResult<Vec<PyObject>> {
        let entities = self.inner.entity_history(entity_id).map_err(to_py_err)?;
        Python::with_gil(|py| entities.iter().map(|e| entity_to_pydict(py, e)).collect::<PyResult<Vec<_>>>())
    }

    fn consolidate(&self) -> PyResult<PyObject> {
        let report = self.inner.consolidate().map_err(to_py_err)?;
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("links_created", report.links_created)?;
            dict.set_item("contradictions_found", report.contradictions_found)?;
            dict.set_item("total_entities", report.total_entities)?;
            Ok(dict.into())
        })
    }

    fn execute_at(&self, query: &str, snapshot_micros: u64) -> PyResult<PyObject> {
        let result = self.inner.execute_at(query, snapshot_micros).map_err(to_py_err)?;
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("column_names", result.column_names.clone())?;
            if let Some(ref err) = result.error {
                dict.set_item("error", err)?;
            } else {
                dict.set_item("num_rows", result.batches.iter().map(|b| b.num_rows()).sum::<usize>())?;
            }
            Ok(dict.into())
        })
    }

    fn query_stream(&self, query: &str) -> PyResult<Vec<PyObject>> {
        let rx = self.inner.query_stream(query).map_err(to_py_err)?;
        Python::with_gil(|py| {
            let mut chunks = Vec::new();
            while let Ok(Ok(chunk)) = rx.recv() {
                let dict = PyDict::new(py);
                dict.set_item("num_rows", chunk.num_rows())?;
                chunks.push(dict.into());
            }
            Ok(chunks)
        })
    }

    fn subscribe_changes(&self) -> PyResult<Vec<PyObject>> {
        let rx = self.inner.subscribe_changes().map_err(to_py_err)?;
        Python::with_gil(|py| {
            let mut events = Vec::new();
            loop {
                match rx.recv_timeout(std::time::Duration::from_millis(500)) {
                    Ok(event) => {
                        let dict = PyDict::new(py);
                        dict.set_item("timestamp", event.timestamp)?;
                        dict.set_item("bytes_written", event.bytes_written)?;
                        dict.set_item("total_wal_bytes", event.total_wal_bytes)?;
                        dict.set_item("entity_id", event.entity_id.clone())?;
                        dict.set_item("operation_type", event.operation_type.clone())?;
                        events.push(dict.into());
                        if events.len() >= 100 { break; }
                    }
                    Err(_) => break,
                }
            }
            Ok(events)
        })
    }

    fn store_batch(&self, entities: Vec<PyObject>) -> PyResult<usize> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let rust_entities: Vec<MemoryEntity> = entities.into_iter().map(|py_entity| {
            Python::with_gil(|py| {
                let dict = py_entity.downcast_bound::<PyDict>(py)
                    .map_err(|_| PyRuntimeError::new_err("store_batch: each item must be a dict"))?;
                let meta = extract_str(&dict, "metadata");
                Ok(MemoryEntity {
                    id: extract_str(&dict, "id"),
                    entity_type: extract_str(&dict, "type"),
                    content: extract_str(&dict, "content"),
                    created_at: extract_i64(&dict, "created_at", now),
                    last_accessed: extract_i64(&dict, "last_accessed", now),
                    access_count: extract_i64(&dict, "access_count", 0),
                    ttl_seconds: extract_i64(&dict, "ttl_seconds", 0),
                    metadata: if meta.is_empty() { "{}".to_string() } else { meta },
                    valid_from: extract_i64(&dict, "valid_from", now),
                    valid_until: extract_i64(&dict, "valid_until", 0),
                    embedding: extract_embedding(&dict, "embedding"),
                })
            })
        }).collect::<PyResult<Vec<MemoryEntity>>>()?;
        self.inner.store_batch(rust_entities).map_err(to_py_err)
    }
}

#[pymodule]
fn _native(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMemoryStore>()?;
    m.add_class::<LightningDatabase>()?;
    Ok(())
}
