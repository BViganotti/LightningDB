use lightning_core::memory::{MemoryEntity, MemoryStore as CoreMemoryStore};
use lightning_core::{Database, SystemConfig, SyncMode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

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
        let db = Database::new(path, config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {}", e)))?;
        Ok(Self { db })
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
        let db = Database::new(path, config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {}", e)))?;
        let conn = db.connect();
        Ok(Self {
            inner: CoreMemoryStore::new(conn),
        })
    }

    fn store(
        &self,
        id: &str,
        content: &str,
        entity_type: &str,
        metadata: Option<&str>,
    ) -> PyResult<()> {
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
        };

        self.inner
            .store(entity)
            .map_err(|e| PyRuntimeError::new_err(format!("Store failed: {}", e)))
    }

    fn recall(&self, query: &str, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let embedding: Vec<f32> = Vec::new();

        let results = self
            .inner
            .recall(query, &embedding, k)
            .map_err(|e| PyRuntimeError::new_err(format!("Recall failed: {}", e)))?;

        Python::with_gil(|py| {
            let py_results: Vec<PyObject> = results
                .into_iter()
                .map(|r| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("id", r.entity.id).unwrap();
                    dict.set_item("content", r.entity.content).unwrap();
                    dict.set_item("type", r.entity.entity_type).unwrap();
                    dict.set_item("score", r.score).unwrap();
                    dict.set_item("metadata", r.entity.metadata).unwrap();
                    dict.into()
                })
                .collect();
            Ok(py_results)
        })
    }

    fn recall_with_embedding(
        &self,
        query: &str,
        embedding: Vec<f32>,
        top_k: Option<usize>,
    ) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);

        let results = self
            .inner
            .recall(query, &embedding, k)
            .map_err(|e| PyRuntimeError::new_err(format!("Recall failed: {}", e)))?;

        Python::with_gil(|py| {
            let py_results: Vec<PyObject> = results
                .into_iter()
                .map(|r| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("id", r.entity.id).unwrap();
                    dict.set_item("content", r.entity.content).unwrap();
                    dict.set_item("type", r.entity.entity_type).unwrap();
                    dict.set_item("score", r.score).unwrap();
                    dict.set_item("metadata", r.entity.metadata).unwrap();
                    dict.into()
                })
                .collect();
            Ok(py_results)
        })
    }

    fn recall_recent(&self, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self
            .inner
            .recall_recent(k)
            .map_err(|e| PyRuntimeError::new_err(format!("Recall recent failed: {}", e)))?;

        Python::with_gil(|py| {
            let py_results: Vec<PyObject> = entities
                .into_iter()
                .map(|e| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("id", e.id).unwrap();
                    dict.set_item("content", e.content).unwrap();
                    dict.set_item("type", e.entity_type).unwrap();
                    dict.set_item("metadata", e.metadata).unwrap();
                    dict.into()
                })
                .collect();
            Ok(py_results)
        })
    }

    fn recall_by_type(&self, entity_type: &str, top_k: Option<usize>) -> PyResult<Vec<PyObject>> {
        let k = top_k.unwrap_or(10);
        let entities = self
            .inner
            .recall_by_type(entity_type, k)
            .map_err(|e| PyRuntimeError::new_err(format!("Recall by type failed: {}", e)))?;

        Python::with_gil(|py| {
            let py_results: Vec<PyObject> = entities
                .into_iter()
                .map(|e| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("id", e.id).unwrap();
                    dict.set_item("content", e.content).unwrap();
                    dict.set_item("type", e.entity_type).unwrap();
                    dict.set_item("metadata", e.metadata).unwrap();
                    dict.into()
                })
                .collect();
            Ok(py_results)
        })
    }

    fn associate(
        &self,
        src_id: &str,
        dst_id: &str,
        rel_type: &str,
        weight: Option<f64>,
    ) -> PyResult<()> {
        self.inner
            .associate(src_id, dst_id, rel_type, weight.unwrap_or(1.0))
            .map_err(|e| PyRuntimeError::new_err(format!("Associate failed: {}", e)))
    }

    fn expand(&self, entity_id: &str, hops: Option<u32>) -> PyResult<Vec<PyObject>> {
        let h = hops.unwrap_or(1);
        let edge_types = vec!["Relates"];

        let entities = self
            .inner
            .expand(entity_id, h, &edge_types)
            .map_err(|e| PyRuntimeError::new_err(format!("Expand failed: {}", e)))?;

        Python::with_gil(|py| {
            let py_results: Vec<PyObject> = entities
                .into_iter()
                .map(|e| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("id", e.id).unwrap();
                    dict.set_item("content", e.content).unwrap();
                    dict.set_item("type", e.entity_type).unwrap();
                    dict.set_item("metadata", e.metadata).unwrap();
                    dict.into()
                })
                .collect();
            Ok(py_results)
        })
    }

    fn forget(&self, entity_id: &str) -> PyResult<bool> {
        self.inner
            .forget(entity_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Forget failed: {}", e)))
    }

    fn decay(&self) -> PyResult<usize> {
        self.inner
            .decay()
            .map_err(|e| PyRuntimeError::new_err(format!("Decay failed: {}", e)))
    }

    fn store_batch(&self, entities: Vec<PyObject>) -> PyResult<usize> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let rust_entities: Vec<MemoryEntity> = entities
            .into_iter()
            .map(|py_entity| {
                Python::with_gil(|py| {
                    let dict = py_entity.downcast_bound::<PyDict>(py).unwrap();
                    fn get_str(dict: &Bound<'_, PyDict>, key: &str) -> String {
                        dict.get_item(key).ok().flatten().and_then(|v| v.extract::<String>().ok()).unwrap_or_default()
                    }
                    fn get_i64(dict: &Bound<'_, PyDict>, key: &str, default: i64) -> i64 {
                        dict.get_item(key).ok().flatten().and_then(|v| v.extract::<i64>().ok()).unwrap_or(default)
                    }
                    let meta = get_str(&dict, "metadata");
                    MemoryEntity {
                        id: get_str(&dict, "id"),
                        entity_type: get_str(&dict, "type"),
                        content: get_str(&dict, "content"),
                        created_at: get_i64(&dict, "created_at", now),
                        last_accessed: get_i64(&dict, "last_accessed", now),
                        access_count: get_i64(&dict, "access_count", 0),
                        ttl_seconds: get_i64(&dict, "ttl_seconds", 0),
                        metadata: if meta.is_empty() { "{}".to_string() } else { meta },
                    }
                })
            })
            .collect();

        self.inner
            .store_batch(rust_entities)
            .map_err(|e| PyRuntimeError::new_err(format!("Store batch failed: {}", e)))
    }
}

#[pymodule]
fn lightning(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMemoryStore>()?;
    m.add_class::<LightningDatabase>()?;
    Ok(())
}
