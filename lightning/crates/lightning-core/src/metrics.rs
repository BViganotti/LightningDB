use std::sync::atomic::{AtomicU64, Ordering};

pub struct DatabaseMetrics {
    pub total_queries: AtomicU64,
    pub total_checkpoints: AtomicU64,
    pub checkpoint_duration_us: AtomicU64,
    pub wal_bytes_written: AtomicU64,
    pub wal_fsync_count: AtomicU64,
    pub eviction_count: AtomicU64,
    pub buffer_miss_count: AtomicU64,
    pub buffer_hit_count: AtomicU64,
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            total_checkpoints: AtomicU64::new(0),
            checkpoint_duration_us: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
            wal_fsync_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            buffer_miss_count: AtomicU64::new(0),
            buffer_hit_count: AtomicU64::new(0),
        }
    }

    pub fn record_query(&self) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self, duration_us: u64) {
        self.total_checkpoints.fetch_add(1, Ordering::Relaxed);
        self.checkpoint_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_wal_write(&self, bytes: u64) {
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wal_fsync(&self) {
        self.wal_fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.eviction_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_buffer_access(&self, hit: bool) {
        if hit {
            self.buffer_hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.buffer_miss_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn buffer_hit_rate(&self) -> f64 {
        let hits = self.buffer_hit_count.load(Ordering::Relaxed);
        let misses = self.buffer_miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn avg_checkpoint_duration_ms(&self) -> f64 {
        let count = self.total_checkpoints.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let total_us = self.checkpoint_duration_us.load(Ordering::Relaxed);
        (total_us / count) as f64 / 1000.0
    }
}
