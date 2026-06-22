use std::collections::HashSet;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter_factor: f64,
    pub retry_statuses: HashSet<u16>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        let mut statuses = HashSet::new();
        statuses.insert(429);
        statuses.insert(502);
        statuses.insert(503);
        statuses.insert(504);
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            jitter_factor: 0.1,
            retry_statuses: statuses,
        }
    }
}

pub fn compute_backoff(attempt: usize, config: &RetryConfig) -> Duration {
    let delay = config.base_delay.as_millis() as f64 * 2u64.pow(attempt as u32) as f64;
    let delay = delay.min(config.max_delay.as_millis() as f64);
    let jitter = if config.jitter_factor > 0.0 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        delay * config.jitter_factor * (rng.gen::<f64>() * 2.0 - 1.0)
    } else {
        0.0
    };
    let total = (delay + jitter).max(1.0) as u64;
    Duration::from_millis(total)
}

pub fn is_status_retryable(status: u16, config: &RetryConfig) -> bool {
    config.retry_statuses.contains(&status)
}
