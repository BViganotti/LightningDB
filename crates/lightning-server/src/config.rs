use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use lightning_core::SystemConfig;

use crate::auth::models::AuthMode;

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

    /// Minimum TLS version (e.g. "1.2", "1.3")
    #[arg(long, default_value = "1.2", env = "LIGHTNING_TLS_MIN_VERSION")]
    pub tls_min_version: String,

    /// Maximum TLS version (e.g. "1.2", "1.3")
    #[arg(long, default_value = "1.3", env = "LIGHTNING_TLS_MAX_VERSION")]
    pub tls_max_version: String,

    /// Enable mutual TLS (requires client certificate)
    #[arg(long, env = "LIGHTNING_MTLS_ENABLED")]
    pub mtls_enabled: bool,

    /// CA certificate file for mTLS client verification
    #[arg(long, env = "LIGHTNING_MTLS_CA")]
    pub mtls_ca: Option<PathBuf>,

    /// Comma-separated list of allowed CORS origins (e.g. "http://localhost:3000,https://app.example.com").
    /// If not set, defaults to allowing only localhost origins.
    #[arg(long, env = "LIGHTNING_CORS_ORIGINS")]
    pub cors_allowed_origins: Option<String>,

    /// API token for authentication (Authorization: Bearer <token>).
    /// If set, auth mode defaults to 'token' unless --auth-mode is explicitly set.
    #[arg(long, env = "LIGHTNING_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Authentication mode: "none" (open access), "token" (shared bearer token), "jwt" (multi-user JWT + RBAC).
    /// Default: "none" if --auth-token is not set, "token" if --auth-token is set.
    #[arg(long, env = "LIGHTNING_AUTH_MODE")]
    pub auth_mode: Option<String>,

    /// Initial admin username for JWT auth mode (used on first start)
    #[arg(long, default_value = "admin", env = "LIGHTNING_ADMIN_USERNAME")]
    pub admin_username: String,

    /// Initial admin password for JWT auth mode. If not set and no users exist,
    /// a random password is generated and printed to stderr.
    #[arg(long, env = "LIGHTNING_ADMIN_PASSWORD")]
    pub admin_password: Option<String>,

    /// JWT signing secret. Must be at least 32 bytes when hex- or base64-decoded.
    /// If not set, a random secret is generated (tokens invalidated on restart).
    /// Prefix with @ to load from a file (e.g. @/path/to/secret).
    #[arg(long, env = "LIGHTNING_JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// JWT access token TTL in seconds (default: 900 = 15 minutes)
    #[arg(long, default_value_t = 900, env = "LIGHTNING_JWT_ACCESS_TTL")]
    pub jwt_access_ttl_secs: u64,

    /// JWT refresh token TTL in seconds (default: 604800 = 7 days)
    #[arg(long, default_value_t = 604800, env = "LIGHTNING_JWT_REFRESH_TTL")]
    pub jwt_refresh_ttl_secs: u64,

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
    pub tls_min_version: String,
    pub tls_max_version: String,
    pub mtls_enabled: bool,
    pub mtls_ca: Option<PathBuf>,
    pub startup_time: std::time::Instant,
    pub cors_allowed_origins: Vec<String>,
    pub auth_token: Option<Arc<str>>,
    pub auth_mode: AuthMode,
    pub admin_username: String,
    #[allow(dead_code)]
    pub admin_password: Option<String>,
    #[allow(dead_code)]
    pub jwt_secret: Option<String>,
    pub jwt_access_ttl_secs: u64,
    pub jwt_refresh_ttl_secs: u64,
    #[allow(dead_code)]
    pub query_timeout_ms: Option<u64>,
}

impl ServerConfig {
    pub fn from_args(args: &CliArgs) -> Self {
        let buffer_pool_size = args.buffer_pool_mb * 1024 * 1024;

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

        let auth_mode = match args.auth_mode.as_deref() {
            Some(m) => m.parse::<AuthMode>().unwrap_or_else(|_| {
                if args.auth_token.is_some() {
                    AuthMode::Token
                } else {
                    AuthMode::None
                }
            }),
            None => {
                if args.auth_token.is_some() {
                    AuthMode::Token
                } else {
                    AuthMode::None
                }
            }
        };

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
            tls_min_version: args.tls_min_version.clone(),
            tls_max_version: args.tls_max_version.clone(),
            mtls_enabled: args.mtls_enabled,
            mtls_ca: args.mtls_ca.clone(),
            startup_time: std::time::Instant::now(),
            cors_allowed_origins,
            auth_token: args.auth_token.as_deref().map(Arc::from),
            auth_mode,
            admin_username: args.admin_username.clone(),
            admin_password: args.admin_password.clone(),
            jwt_secret: args.jwt_secret.clone(),
            jwt_access_ttl_secs: args.jwt_access_ttl_secs,
            jwt_refresh_ttl_secs: args.jwt_refresh_ttl_secs,
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
        const MAX_BUFFER_POOL: u64 = 1024 * 1024 * 1024 * 1024;
        if self.buffer_pool_size > MAX_BUFFER_POOL {
            return Err(format!("buffer_pool_size cannot exceed {}TB", MAX_BUFFER_POOL / (1024 * 1024 * 1024 * 1024)));
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
            let _ = tls_protocol_versions(&self.tls_min_version, &self.tls_max_version)
                .map_err(|e| format!("TLS version configuration error: {e}"))?;
            if self.mtls_enabled && self.mtls_ca.is_none() {
                return Err("mtls_ca is required when mtls_enabled is true".into());
            }
        }
        if self.auth_mode == AuthMode::Token {
            match &self.auth_token {
                None => return Err("auth_token is required when auth_mode is 'token'".into()),
                Some(t) if t.is_empty() => return Err("auth_token must not be empty when auth_mode is 'token'".into()),
                _ => {}
            }
        }
        Ok(())
    }
}

fn tls_version_rank(version: &str) -> Result<u8, String> {
    match version {
        "1.2" => Ok(2),
        "1.3" => Ok(3),
        _ => Err(format!("unsupported TLS version '{version}'; supported: 1.2, 1.3")),
    }
}

pub fn tls_protocol_versions(
    min_str: &str,
    max_str: &str,
) -> Result<Vec<&'static rustls::SupportedProtocolVersion>, String> {
    let min_rank = tls_version_rank(min_str)?;
    let max_rank = tls_version_rank(max_str)?;
    if min_rank > max_rank {
        return Err("tls_max_version must be >= tls_min_version".to_string());
    }
    let mut versions = Vec::new();
    if min_rank <= 2 && 2 <= max_rank {
        versions.push(&rustls::version::TLS12);
    }
    if min_rank <= 3 && 3 <= max_rank {
        versions.push(&rustls::version::TLS13);
    }
    if versions.is_empty() {
        return Err("no TLS versions match the configured range".to_string());
    }
    Ok(versions)
}
