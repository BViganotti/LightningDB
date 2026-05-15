use std::sync::Arc;

use lightning_core::{Database as CoreDatabase, SystemConfig, SyncMode};
use napi_derive::napi;

#[napi]
pub struct JsDatabase {
    db: Arc<CoreDatabase>,
}

#[napi]
impl JsDatabase {
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let config = SystemConfig {
            sync_mode: SyncMode::Normal,
            ..Default::default()
        };
        let db = CoreDatabase::new(&path, config)
            .map_err(|e| napi::Error::from_reason(format!("Failed to open database: {}", e)))?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl JsDatabase {
    pub fn inner(&self) -> Arc<CoreDatabase> {
        self.db.clone()
    }
}
