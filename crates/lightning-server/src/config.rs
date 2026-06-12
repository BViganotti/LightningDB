use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use lightning_core::SystemConfig;

#[derive(Parser, Debug, Clone)]
#[command(name = "lightning-server", version, about = "LightningDB HTTP server — graph+vector+RAG database for AI agent memory")]
pub struct CliArgs {
    /// Path to the database directory
    #[arg(long, default_value = "./lightning-data", env = "LIGHTNING_DB_PATH")]
    pub db_path: PathBuf,

    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1", env = "LIGHTNING_HOST")]
    pub host: String,

    /// Port to listen on
    #[arg(short = 'p', long, default_value_t = 8080, env = "LIGHTNING_PORT")]
    pub port: u16,

    /// Buffer pool size in megabytes
    #[arg(long, default_value_t = 1024, env = "LIGHTNING_BUFFER_POOL_MB")]
    pub buffer_pool_mb: u64,

    /// Open database in read-only mode
    #[arg(long, env = "LIGHTNING_READ_ONLY")]
    pub read_only: bool,

    /// Vacuum interval in milliseconds (minimum 100)
    #[arg(long, default_value_t = 1000, env = "LIGHTNING_VACUUM_INTERVAL_MS")]
    pub vacuum_interval_ms: u64,

    /// Log filter directive (e.g. "lightning_server=debug")
    #[arg(long, env = "LIGHTNING_LOG")]
    pub log: Option<String>,

    /// Maximum number of concurrent connections
    #[arg(long, default_value_t = 100, env = "LIGHTNING_MAX_CONNECTIONS")]
    pub max_connections: u32,

    /// Embedding dimension for vector search
    #[arg(long, default_value_t = 768, env = "LIGHTNING_EMBEDDING_DIM")]
    pub embedding_dim: usize,

    /// Enable SSL/TLS
    #[arg(long, env = "LIGHTNING_TLS_ENABLED")]
    pub tls_enabled: bool,

    /// Path to TLS certificate file
    #[arg(long, env = "LIGHTNING_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to TLS key file
    #[arg(long, env = "LIGHTNING_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Comma-separated list of allowed CORS origins (e.g. "http://localhost:3000,https://app.example.com").
    /// If not set, defaults to allowing only localhost origins.
    #[arg(long, env = "LIGHTNING_CORS_ORIGINS")]
    pub cors_allowed_origins: Option<String>,

    /// API token for authentication (Authorization: Bearer <token>).
    /// If set, all endpoints (except /health) require this token.
    /// If not set, authentication is disabled (open access).
    #[arg(long, env = "LIGHTNING_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Query timeout in milliseconds (default: 30000)
    #[arg(long, default_value_t = 30000, env = "LIGHTNING_QUERY_TIMEOUT_MS")]
    pub query_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub buffer_pool_size: u64,
    pub read_only: bool,
    pub vacuum_interval_ms: u64,
    pub max_connections: u32,
    pub db_path: PathBuf,
    pub embedding_dim: usize,
    pub tls_enabled: bool,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub startup_time: std::time::Instant,
    pub cors_allowed_origins: Vec<String>,
    /// Shared auth token — single allocation shared across all clones
    /// to reduce copies in memory.
    pub auth_token: Option<Arc<str>>,
    pub query_timeout_ms: Option<u64>,
}

impl ServerConfig {
    pub fn from_args(args: &CliArgs) -> Self {
        let buffer_pool_size = (args.buffer_pool_mb as u64) * 1024 * 1024;
        let cors_allowed_origins = args
            .cors_allowed_origins
            .as_ref()
            .map(|s| {
                s.split(',')
                    .map(|part| part.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                vec![
                    "http://localhost:3000".to_string(),
                    "http://localhost:8080".to_string(),
                    "http://127.0.0.1:3000".to_string(),
                    "http://127.0.0.1:8080".to_string(),
                ]
            });
        Self {
            host: args.host.clone(),
            port: args.port,
            buffer_pool_size,
            read_only: args.read_only,
            vacuum_interval_ms: args.vacuum_interval_ms.max(100),
            max_connections: args.max_connections,
            db_path: args.db_path.clone(),
            embedding_dim: args.embedding_dim,
            tls_enabled: args.tls_enabled,
            tls_cert: args.tls_cert.clone(),
            tls_key: args.tls_key.clone(),
            startup_time: std::time::Instant::now(),
            cors_allowed_origins,
            auth_token: args.auth_token.as_deref().map(|t| Arc::from(t)),
            query_timeout_ms: Some(args.query_timeout_ms),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.startup_time.elapsed().as_secs()
    }

    pub fn system_config(&self) -> SystemConfig {
        SystemConfig {
            buffer_pool_size: self.buffer_pool_size,
            read_only: self.read_only,
            vacuum_interval_ms: self.vacuum_interval_ms,
            embedding_dim: self.embedding_dim,
            ..Default::default()
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_pool_size < 1024 * 1024 {
            return Err("buffer_pool_size must be at least 1MB".into());
        }
        if self.vacuum_interval_ms < 100 {
            return Err("vacuum_interval_ms must be at least 100ms".into());
        }
        if self.max_connections == 0 {
            return Err("max_connections must be > 0".into());
        }
        if self.embedding_dim == 0 {
            return Err("embedding_dim must be > 0".into());
        }
        if self.tls_enabled {
            if self.tls_cert.is_none() {
                return Err("tls_cert is required when tls_enabled is true".into());
            }
            if self.tls_key.is_none() {
                return Err("tls_key is required when tls_enabled is true".into());
            }
        }
        Ok(())
    }
}
