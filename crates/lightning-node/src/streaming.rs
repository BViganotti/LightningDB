use std::sync::{Arc, Mutex};

use crossbeam::channel::Receiver;
use lightning_core::memory::{ChangeEvent, SearchResult};
use lightning_core::processor::DataChunk;
use lightning_core::LightningError;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::types::{JsChangeEvent, JsSearchResult};

pub struct NextChunk {
    rx: Arc<Mutex<Receiver<std::result::Result<DataChunk, LightningError>>>>,
}

#[napi]
impl Task for NextChunk {
    type Output = Option<Vec<Vec<String>>>;
    type JsValue = JsChunkResult;

    fn compute(&mut self) -> Result<Self::Output> {
        // Clone the receiver out of the mutex so recv() doesn't hold the lock
        let rx = self
            .rx
            .lock()
            .map_err(|e| napi::Error::from_reason(format!("Lock error: {}", e)))?
            .clone();
        match rx.recv() {
            Ok(Ok(chunk)) => {
                let batch = &chunk.batch;
                let num_rows = batch.num_rows();
                let num_cols = batch.num_columns();
                let mut rows = Vec::with_capacity(num_rows);
                for row_idx in 0..num_rows {
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        let col = batch.column(col_idx);
                        let val = if col.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            lightning_core::processor::Value::from_arrow(col, row_idx).to_string()
                        };
                        row.push(val);
                    }
                    rows.push(row);
                }
                Ok(Some(rows))
            }
            Ok(Err(e)) => Err(napi::Error::from_reason(format!("Query error: {}", e))),
            Err(_) => Ok(None),
        }
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(JsChunkResult {
            done: output.is_none(),
            rows: output.unwrap_or_default(),
        })
    }
}

pub struct NextChange {
    rx: Arc<Mutex<Receiver<ChangeEvent>>>,
}

#[napi]
impl Task for NextChange {
    type Output = Option<ChangeEvent>;
    type JsValue = Option<JsChangeEvent>;

    fn compute(&mut self) -> Result<Self::Output> {
        let rx = self
            .rx
            .lock()
            .map_err(|e| napi::Error::from_reason(format!("Lock error: {}", e)))?
            .clone();
        match rx.recv() {
            Ok(event) => Ok(Some(event)),
            Err(_) => Ok(None),
        }
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output.map(JsChangeEvent::from))
    }
}

pub struct NextRecall {
    rx: Arc<Mutex<Receiver<std::result::Result<SearchResult, LightningError>>>>,
}

#[napi]
impl Task for NextRecall {
    type Output = Option<SearchResult>;
    type JsValue = Option<JsSearchResult>;

    fn compute(&mut self) -> Result<Self::Output> {
        let rx = self
            .rx
            .lock()
            .map_err(|e| napi::Error::from_reason(format!("Lock error: {}", e)))?
            .clone();
        match rx.recv() {
            Ok(Ok(result)) => Ok(Some(result)),
            Ok(Err(e)) => Err(napi::Error::from_reason(format!("Recall error: {}", e))),
            Err(_) => Ok(None),
        }
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output.map(|r| {
            JsSearchResult::from_parts(
                r.entity.id,
                r.entity.content,
                r.entity.entity_type,
                r.score,
                r.entity.metadata,
                r.entity.embedding.iter().map(|&v| v as f64).collect(),
            )
        }))
    }
}

#[napi]
pub struct JsQueryStream {
    rx: Arc<Mutex<Receiver<std::result::Result<DataChunk, LightningError>>>>,
}

#[napi]
impl JsQueryStream {
    #[napi]
    pub fn next(&self) -> AsyncTask<NextChunk> {
        AsyncTask::new(NextChunk { rx: self.rx.clone() })
    }
}

impl JsQueryStream {
    pub fn new(rx: Receiver<std::result::Result<DataChunk, LightningError>>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

#[napi(object)]
pub struct JsChunkResult {
    pub done: bool,
    pub rows: Vec<Vec<String>>,
}

#[napi]
pub struct JsChangeStream {
    rx: Arc<Mutex<Receiver<ChangeEvent>>>,
}

#[napi]
impl JsChangeStream {
    #[napi]
    pub fn next(&self) -> AsyncTask<NextChange> {
        AsyncTask::new(NextChange { rx: self.rx.clone() })
    }
}

impl JsChangeStream {
    pub fn new(rx: Receiver<ChangeEvent>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

#[napi]
pub struct JsRecallStream {
    rx: Arc<Mutex<Receiver<std::result::Result<SearchResult, LightningError>>>>,
}

#[napi]
impl JsRecallStream {
    #[napi]
    pub fn next(&self) -> AsyncTask<NextRecall> {
        AsyncTask::new(NextRecall { rx: self.rx.clone() })
    }
}

impl JsRecallStream {
    pub fn new(rx: Receiver<std::result::Result<SearchResult, LightningError>>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}
