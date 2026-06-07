use crate::error::{LightningError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Normal,
    Off,
}

impl Default for SyncMode {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub buffer_pool_size: u64,
    pub max_num_threads: u32,
    pub read_only: bool,
    pub sync_mode: SyncMode,
    pub vacuum_interval_ms: u64,
    pub prefetch_enabled: bool,
    pub prefetch_depth: usize,
    pub prefetch_confidence: f64,
    pub slow_query_threshold_ms: u64,
    pub copy_base_dir: Option<std::path::PathBuf>,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 1024 * 1024 * 1024,
            max_num_threads: 0,
            read_only: false,
            sync_mode: SyncMode::Normal,
            vacuum_interval_ms: 1000,
            prefetch_enabled: true,
            prefetch_depth: 2,
            prefetch_confidence: 0.15,
            slow_query_threshold_ms: 100,
            copy_base_dir: None,
        }
    }
}

impl SystemConfig {
    pub fn validate(&self) -> Result<()> {
        if self.buffer_pool_size == 0 {
            return Err(LightningError::Config(
                "buffer_pool_size must be greater than 0".into(),
            ));
        }
        if self.buffer_pool_size < 1024 * 1024 {
            return Err(LightningError::Config(
                "buffer_pool_size must be at least 1MB".into(),
            ));
        }
        if self.vacuum_interval_ms < 100 {
            return Err(LightningError::Config(
                "vacuum_interval_ms must be at least 100ms".into(),
            ));
        }
        if self.prefetch_depth > 100 {
            return Err(LightningError::Config(
                "prefetch_depth must be <= 100".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.prefetch_confidence) {
            return Err(LightningError::Config(
                "prefetch_confidence must be between 0.0 and 1.0".into(),
            ));
        }
        Ok(())
    }
}
