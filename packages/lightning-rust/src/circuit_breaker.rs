use std::time::{Duration, Instant};
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,
    pub recovery_timeout: Duration,
    pub half_open_max_requests: usize,
    pub success_threshold: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(30),
            half_open_max_requests: 3,
            success_threshold: 2,
        }
    }
}

struct Inner {
    state: CircuitState,
    failure_count: usize,
    success_count: usize,
    half_open_requests: usize,
    last_failure_time: Option<Instant>,
}

pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                half_open_requests: 0,
                last_failure_time: None,
            }),
        }
    }

    pub fn allow_request(&self) -> bool {
        let mut inner = self.inner.lock();
        match inner.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(t) = inner.last_failure_time {
                    if t.elapsed() >= self.config.recovery_timeout {
                        inner.state = CircuitState::HalfOpen;
                        inner.half_open_requests = 0;
                        inner.success_count = 0;
                        inner.half_open_requests += 1;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => {
                if inner.half_open_requests < self.config.half_open_max_requests {
                    inner.half_open_requests += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn on_success(&self) {
        let mut inner = self.inner.lock();
        match inner.state {
            CircuitState::Closed => {
                inner.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                inner.success_count += 1;
                if inner.success_count >= self.config.success_threshold {
                    inner.state = CircuitState::Closed;
                    inner.failure_count = 0;
                    inner.success_count = 0;
                    inner.half_open_requests = 0;
                }
            }
            CircuitState::Open => {}
        }
    }

    pub fn on_failure(&self) {
        let mut inner = self.inner.lock();
        match inner.state {
            CircuitState::Closed => {
                inner.failure_count += 1;
                inner.last_failure_time = Some(Instant::now());
                if inner.failure_count >= self.config.failure_threshold {
                    inner.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                inner.last_failure_time = Some(Instant::now());
                inner.state = CircuitState::Open;
            }
            CircuitState::Open => {
                inner.last_failure_time = Some(Instant::now());
            }
        }
    }

    pub fn state(&self) -> CircuitState {
        self.inner.lock().state
    }

    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }
}
