use crate::LightningError;
use crate::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Tracks memory usage during query execution and enforces a configurable quota.
/// When the tracked bytes exceed the quota, further allocations are rejected.
///
/// This is an approximate tracker suitable for detecting runaway queries.
/// It does not track every individual allocation — it tracks batches and
/// major column allocations.
#[derive(Clone)]
pub struct MemoryTracker {
    quota: u64,
    used: Arc<AtomicU64>,
}

impl MemoryTracker {
    pub fn new(quota: u64) -> Self {
        Self {
            quota,
            used: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record an allocation of `bytes` and check if the quota is exceeded.
    /// Returns `Ok(())` if within budget, `Err` if the quota would be exceeded.
    pub fn record_allocation(&self, bytes: u64) -> Result<()> {
        if self.quota == 0 {
            return Ok(());
        }
        let prev = self.used.fetch_add(bytes, Ordering::Relaxed);
        let new_used = prev + bytes;
        if new_used > self.quota {
            Err(LightningError::Internal(format!(
                "Memory quota exceeded: used {} bytes, limit {} bytes",
                new_used, self.quota
            )))
        } else {
            Ok(())
        }
    }

    /// Record that `bytes` have been freed.
    pub fn record_free(&self, bytes: u64) {
        if self.quota == 0 {
            return;
        }
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn quota(&self) -> u64 {
        self.quota
    }
}
