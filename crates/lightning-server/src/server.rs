use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use lightning::memory::MemoryStore;
use lightning::Database;
use parking_lot::Mutex;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::compression::CompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tracing::Level;

use rustls::pki_types::CertificateDer;

use crate::auth::middleware::{auth_middleware, require_admin_role, require_reader_role, require_writer_role};
use crate::auth::models::AuthMode;
use crate::auth::store::AuthStore;
use crate::config::{self as server_config, ServerConfig};
use crate::extract::{ConnectionPool, RequestIdExtension};
use crate::routes;

struct RateLimiter {
    windows: Mutex<HashMap<IpAddr, SlidingWindow>>,
    max_requests: u32,
    window: Duration,
}

struct SlidingWindow {
    timestamps: VecDeque<Instant>,
}

impl RateLimiter {
    fn new(max_requests: u32, window_secs: u64) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            max_requests,
            window: Duration::from_secs(window_secs),
        }
    }

    fn check(&self, ip: IpAddr) -> bool {
        let mut windows = self.windows.lock();
        let now = Instant::now();

        // Always prune stale entries to prevent memory exhaustion.
        // Evict entries that haven't been active within 2x the window.
        if windows.len() > 1000 {
            let stale_threshold = self.window * 2;
            windows.retain(|_, sw| {
                sw.timestamps.back().map_or(false, |t| now.duration_since(*t) < stale_threshold)
            });
        }

        let sw = windows.entry(ip).or_insert(SlidingWindow {
            timestamps: VecDeque::new(),
        });

        while sw.timestamps.front().map_or(false, |t| now.duration_since(*t) > self.window) {
            sw.timestamps.pop_front();
        }

        if sw.timestamps.len() < self.max_requests as usize {
            sw.timestamps.push_back(now);
            true
        } else {
            false
        }
    }
}

pub struct AppState {
    pub db: Arc<Database>,
    pub store: Arc<MemoryStore>,
    pub config: ServerConfig,
    pub request_counter: AtomicU64,
    pub connection_pool: Arc<ConnectionPool>,
    pub auth_store: Arc<AuthStore>,
    pub auth_mode: AuthMode,
    rate_limiter: Arc<RateLimiter>,
    pub query_semaphore: Arc<tokio::sync::Semaphore>,
}

const MAX_CONCURRENT_QUERIES: usize = 64;

impl AppState {
    pub fn new(
        db: Arc<Database>,
        store: MemoryStore,
        config: ServerConfig,
        auth_store: Arc<AuthStore>,
    ) -> Self {
        let auth_mode = config.auth_mode;
        Self {
            db: Arc::clone(&db),
            store: Arc::new(store),
            config,
            request_counter: AtomicU64::new(0),
            connection_pool: Arc::new(ConnectionPool::new(Arc::clone(&db))),
            auth_store,
            auth_mode,
            rate_limiter: Arc::new(RateLimiter::new(100, 1)),
            query_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES)),
        }
    }
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            store: Arc::clone(&self.store),
            config: self.config.clone(),
            request_counter: AtomicU64::new(self.request_counter.load(Ordering::Relaxed)),
            connection_pool: Arc::clone(&self.connection_pool),
            auth_store: Arc::clone(&self.auth_store),
            auth_mode: self.auth_mode,
            rate_limiter: Arc::clone(&self.rate_limiter),
            query_semaphore: Arc::clone(&self.query_semaphore),
        }
    }
}

async fn request_id_middleware(
    mut req: Request,
    next: Next,
) -> Response {
    const MAX_REQUEST_ID_LEN: usize = 256;
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.len() <= MAX_REQUEST_ID_LEN)
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    req.extensions_mut()
        .insert(RequestIdExtension(request_id.clone()));

    let mut response = next.run(req).await;
    response.headers_mut().insert(
        header::HeaderName::from_static("x-request-id"),
        header::HeaderValue::from_str(&request_id)
            .unwrap_or_else(|_| header::HeaderValue::from_static("fallback-request-id")),
    );
    response
}

pub struct Server {
    state: Arc<AppState>,
}

impl Server {
    pub fn new(state: AppState) -> Self {
        Self {
            state: Arc::new(state),
        }
    }

    async fn rate_limit_middleware(
        state: axum::extract::State<Arc<AppState>>,
        req: Request,
        next: Next,
    ) -> Response {
        let client_ip = req.extensions()
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|ci| ci.0.ip())
            .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

        if !state.rate_limiter.check(client_ip) {
            let mut resp = (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response();
            resp.headers_mut().insert(
                header::RETRY_AFTER,
                header::HeaderValue::from_static("1"),
            );
            return resp;
        }
        next.run(req).await
    }

    pub async fn run(self) {
        let state = self.state;

        // Build CORS layer from configured allowed origins.
        let cors_allowed = if state.config.cors_allowed_origins.is_empty() {
            vec![
                "http://localhost:3000".to_string(),
                "http://localhost:8080".to_string(),
                "http://127.0.0.1:3000".to_string(),
                "http://127.0.0.1:8080".to_string(),
            ]
        } else {
            state.config.cors_allowed_origins.clone()
        };
        let origins: Vec<axum::http::HeaderValue> = cors_allowed
            .iter()
            .filter_map(|o| axum::http::HeaderValue::from_str(o).ok())
            .collect();
        let cors_layer = if origins.is_empty() {
            tracing::warn!("No valid CORS origins configured; CORS will deny all cross-origin requests");
            CorsLayer::new()
                .allow_origin(AllowOrigin::predicate(|_origin, _parts| false))
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::OPTIONS,
                ])
                .allow_headers([
                    axum::http::header::CONTENT_TYPE,
                ])
        } else {
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(origins))
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::PUT,
                    axum::http::Method::DELETE,
                    axum::http::Method::OPTIONS,
                ])
                .allow_headers([
                    axum::http::header::CONTENT_TYPE,
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::HeaderName::from_static("x-request-id"),
                ])
        };

        // Public routes (no auth required)
        let app = Router::new()
            .route("/health", get(routes::health::health_handler))
            .route("/metrics", get(routes::admin::metrics_handler))
            .route("/v1/auth/login", post(routes::auth::login_handler))

            // Reader-guarded routes (any authenticated user with at least Reader role)
            .merge(
                Router::new()
                    .route("/v1/query", post(routes::query::query_handler))
                    .route("/v1/query/stream", post(routes::query::query_stream_handler))
                    .route("/v1/memory/recall", post(routes::memory::recall_handler))
                    .route("/v1/memory/recall-recent", post(routes::memory::recall_recent_handler))
                    .route("/v1/memory/recall-by-type", post(routes::memory::recall_by_type_handler))
                    .route("/v1/memory/entity-history", post(routes::memory::entity_history_handler))
                    .route("/v1/graph/expand", post(routes::graph::expand_handler))
                    .route("/v1/rag/query", post(routes::rag::rag_query_handler))
                    .route("/v1/subscribe", get(routes::subscribe::subscribe_handler))
                    .route("/v1/snapshots", get(routes::snapshots::snapshots_handler))
                    .route("/v1/auth/refresh", post(routes::auth::refresh_handler))
                    .route("/v1/auth/logout", post(routes::auth::logout_handler))
                    .route("/v1/auth/me", get(routes::auth::me_handler))
                    .layer(middleware::from_fn(require_reader_role)),
            )

            // Writer-guarded routes
            .merge(
                Router::new()
                    .route("/v1/memory/store", post(routes::memory::store_handler))
                    .route("/v1/memory/store-batch", post(routes::memory::store_batch_handler))
                    .route("/v1/memory/forget", post(routes::memory::forget_handler))
                    .route("/v1/memory/decay", post(routes::memory::decay_handler))
                    .route("/v1/memory/consolidate", post(routes::memory::consolidate_handler))
                    .route("/v1/graph/associate", post(routes::graph::associate_handler))
                    .layer(middleware::from_fn(require_writer_role)),
            )

            // Admin-guarded routes
            .merge(
                Router::new()
                    .route("/v1/admin/checkpoint", post(routes::admin::checkpoint_handler))
                    .route("/v1/admin/vacuum", post(routes::admin::vacuum_handler))
                    .route("/v1/admin/users", get(routes::admin_users::list_users_handler))
                    .route("/v1/admin/users", post(routes::admin_users::create_user_handler))
                    .route("/v1/admin/users/{id}", post(routes::admin_users::update_user_handler))
                    .route("/v1/admin/users/{id}", delete(routes::admin_users::delete_user_handler))
                    .route("/v1/admin/users/{id}/reset-password", post(routes::admin_users::reset_password_handler))
                    .route("/v1/admin/users/{id}/keys", get(routes::admin_users::list_api_keys_handler))
                    .route("/v1/admin/users/{id}/keys", post(routes::admin_users::create_api_key_handler))
                    .route("/v1/admin/users/{user_id}/keys/{key_id}", delete(routes::admin_users::delete_api_key_handler))
                    .layer(middleware::from_fn(require_admin_role)),
            )

            // Auth middleware (checks JWT/API key/Token, populates AuthenticatedUser)
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .layer(middleware::from_fn(request_id_middleware))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .layer(cors_layer)
            .layer(CompressionLayer::new())
            .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                Self::rate_limit_middleware,
            ))
            .with_state(state.clone());

        let addr = format!("{}:{}", state.config.host, state.config.port);
        tracing::info!(
            "Lightning server starting on {} (read_only={}, buffer_pool_mb={}, tls={}, auth={})",
            addr,
            state.config.read_only,
            state.config.buffer_pool_size / (1024 * 1024),
            state.config.tls_enabled,
            state.auth_mode,
        );

        if state.config.tls_enabled {
            let cert_path = state
                .config
                .tls_cert
                .as_ref()
                .expect("tls_cert required when tls_enabled=true");
            let key_path = state
                .config
                .tls_key
                .as_ref()
                .expect("tls_key required when tls_enabled=true");
            let addr: std::net::SocketAddr = addr.parse().expect("Invalid address");

            let versions = server_config::tls_protocol_versions(
                &state.config.tls_min_version,
                &state.config.tls_max_version,
            )
            .expect("invalid TLS version configuration");

            if state.config.mtls_enabled {
                let ca_path = state
                    .config
                    .mtls_ca
                    .as_ref()
                    .expect("mtls_ca required when mtls_enabled=true");
                run_mtls_server(app, addr, cert_path, key_path, ca_path, &versions).await;
            } else {
                let certs = load_pem_certs(cert_path);
                let key = load_pem_key(key_path);

                let server_config = rustls::ServerConfig::builder_with_protocol_versions(&versions)
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .expect("failed to set server certificate");

                let tls_config = axum_server::tls_rustls::RustlsConfig::from_config(
                    std::sync::Arc::new(server_config),
                );

                let server = axum_server::bind_rustls(addr, tls_config);
                let serve_future = server.serve(app.into_make_service());
                tokio::select! {
                    result = serve_future => {
                        result.expect("Server exited with error");
                    }
                    _ = shutdown_signal() => {
                        tracing::info!("Shutdown signal received, stopping server...");
                    }
                }
            }
        } else {
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .expect("Failed to bind address");

            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .expect("Server exited with error");
        }

        tracing::info!("Shutdown complete");
    }
}

async fn run_mtls_server(
    app: Router,
    addr: std::net::SocketAddr,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    ca_path: &std::path::Path,
    versions: &[&'static rustls::SupportedProtocolVersion],
) {
    use rustls::server::WebPkiClientVerifier;

    let certs = load_pem_certs(cert_path);
    let key = load_pem_key(key_path);
    let ca_certs = load_pem_certs(ca_path);

    let mut root_store = rustls::RootCertStore::empty();
    for cert in &ca_certs {
        root_store
            .add(cert.clone())
            .expect("failed to add CA certificate");
    }

    let client_verifier = WebPkiClientVerifier::builder(std::sync::Arc::new(root_store))
        .build()
        .expect("failed to build mTLS client verifier");

    let server_config = rustls::ServerConfig::builder_with_protocol_versions(versions)
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .expect("failed to set server certificate");

    let tls_config =
        axum_server::tls_rustls::RustlsConfig::from_config(std::sync::Arc::new(server_config));

    let server = axum_server::bind_rustls(addr, tls_config);
    let serve_future = server.serve(app.into_make_service());
    tokio::select! {
        result = serve_future => {
            result.expect("Server exited with error");
        }
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received, stopping server...");
        }
    }
}

fn load_pem_certs(path: &std::path::Path) -> Vec<CertificateDer<'static>> {
    let data = std::fs::read(path).expect("failed to read cert file");
    rustls_pemfile::certs(&mut data.as_ref())
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse PEM certs")
}

fn load_pem_key(path: &std::path::Path) -> rustls::pki_types::PrivateKeyDer<'static> {
    let data = std::fs::read(path).expect("failed to read key file");
    let mut reader = data.as_ref();
    for item in rustls_pemfile::read_all(&mut reader) {
        match item.expect("failed to parse PEM") {
            rustls_pemfile::Item::Pkcs1Key(key) => {
                return key.into();
            }
            rustls_pemfile::Item::Pkcs8Key(key) => {
                return key.into();
            }
            rustls_pemfile::Item::Sec1Key(key) => {
                return key.into();
            }
            _ => continue,
        }
    }
    panic!("no private key found in {}", path.display());
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received SIGINT, shutting down...");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down...");
        }
    }
}
