use std::sync::Arc;
use std::time::Duration;
use crate::circuit_breaker::CircuitBreakerConfig;
use crate::retry::RetryConfig;
use std::path::PathBuf;

pub type TelemetryCallback = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;
pub type TelemetryCallbackResult = Arc<dyn Fn(&str, &str, &str, u16, f64) + Send + Sync>;
pub type TelemetryCallbackError = Arc<dyn Fn(&str, &str, &str, &crate::error::Error) + Send + Sync>;
pub type TelemetryCallbackRetry = Arc<dyn Fn(&str, &str, &str, usize, f64) + Send + Sync>;
pub type TelemetryCallbackState = Arc<dyn Fn(&str, &str) + Send + Sync>;

#[derive(Clone)]
pub struct TelemetryHooks {
    pub on_request_start: Option<TelemetryCallback>,
    pub on_request_end: Option<TelemetryCallbackResult>,
    pub on_error: Option<TelemetryCallbackError>,
    pub on_retry: Option<TelemetryCallbackRetry>,
    pub on_circuit_breaker: Option<TelemetryCallbackState>,
}

impl std::fmt::Debug for TelemetryHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryHooks").finish()
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub verify: bool,
    pub ca_bundle_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub server_name_override: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub auth_token: Option<String>,
    pub default_timeout: Duration,
    pub retry: RetryConfig,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
    pub tls: Option<TlsConfig>,
    pub telemetry: Option<TelemetryHooks>,
    pub max_connections: usize,
    pub max_keepalive: usize,
    pub keepalive_timeout: Duration,
    pub follow_redirects: bool,
    pub max_content_bytes: u64,
    pub max_batch_entities: usize,
    pub max_top_k: usize,
    pub user_agent: String,
}

impl ClientConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            auth_token: None,
            default_timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
            circuit_breaker: None,
            tls: None,
            telemetry: None,
            max_connections: 10,
            max_keepalive: 5,
            keepalive_timeout: Duration::from_secs(60),
            follow_redirects: false,
            max_content_bytes: 10 * 1024 * 1024,
            max_batch_entities: 1000,
            max_top_k: 1000,
            user_agent: "lightning-client-rust/0.1.0".to_string(),
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry = config;
        self
    }

    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(config);
        self
    }

    pub fn with_tls(mut self, config: TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }
}
