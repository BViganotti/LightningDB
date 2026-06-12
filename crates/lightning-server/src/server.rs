use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use lightning::memory::MemoryStore;
use lightning::Database;
use parking_lot::Mutex;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::compression::CompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tracing::Level;

use crate::config::ServerConfig;
use crate::extract::{ConnectionPool, RequestIdExtension};
use crate::routes;

struct RateLimiter {
    buckets: Mutex<HashMap<String, (u32, Instant)>>,
    max_requests: u32,
    window: Duration,
}

impl RateLimiter {
    fn new(max_requests: u32, window_secs: u64) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            max_requests,
            window: Duration::from_secs(window_secs),
        }
    }

    fn check(&self, key: &str) -> bool {
        let mut buckets = self.buckets.lock();
        let now = Instant::now();
        let entry = buckets.entry(key.to_string()).or_insert((0, now));
        if now.duration_since(entry.1) > self.window {
            *entry = (1, now);
            true
        } else if entry.0 < self.max_requests {
            entry.0 += 1;
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
    rate_limiter: Arc<RateLimiter>,
    /// Semaphore to limit concurrent query execution and prevent
    /// spawn_blocking pool exhaustion from a single client.
    pub query_semaphore: Arc<tokio::sync::Semaphore>,
}

const MAX_CONCURRENT_QUERIES: usize = 64;

impl AppState {
    pub fn new(db: Database, store: MemoryStore, config: ServerConfig) -> Self {
        let db_arc = Arc::new(db);
        Self {
            db: Arc::clone(&db_arc),
            store: Arc::new(store),
            config,
            request_counter: AtomicU64::new(0),
            connection_pool: Arc::new(ConnectionPool::new(Arc::clone(&db_arc))),
            rate_limiter: Arc::new(RateLimiter::new(100, 1)), // 100 req/sec per IP
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
            rate_limiter: Arc::clone(&self.rate_limiter),
            query_semaphore: Arc::clone(&self.query_semaphore),
        }
    }
}

async fn request_id_middleware(
    mut req: Request,
    next: Next,
) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
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
        // Rate limit by the direct client IP (socket address).
        // Do NOT use x-forwarded-for for rate limiting — it is trivially
        // spoofable and allows attackers to bypass rate limits entirely.
        let client_ip = req.extensions()
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|ci| ci.0.ip().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        if !state.rate_limiter.check(&client_ip) {
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
        // When no --cors-allowed-origins is specified, defaults to localhost-only.
        // NEVER fall back to CorsLayer::permissive() — that allows any origin,
        // which is a security risk for a database HTTP server.
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
            // Only reachable if ALL configured origins failed to parse as HeaderValue.
            // Log a warning and provide a restricted layer that allows nothing.
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

        // Build auth layer from config (if auth_token is set)
        let auth_token = state.config.auth_token.clone();
        let auth_layer = axum::middleware::from_fn(move |req: Request, next: Next| {
            let expected = auth_token.clone();
            async move {
                if let Some(ref expected) = expected {
                    let provided = req
                        .headers()
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "))
                        .map(|s| s.trim().to_string());
                    match provided {
                        Some(token) if token.as_str() == expected.as_ref() => {}
                        _ => {
                            return Err(axum::http::StatusCode::UNAUTHORIZED);
                        }
                    }
                }
                Ok(next.run(req).await)
            }
        });

        let app = Router::new()
            // Health (no auth required)
            .route("/health", get(routes::health::health_handler))
            // Query
            .route("/v1/query", post(routes::query::query_handler))
            .route("/v1/query/stream", post(routes::query::query_stream_handler))
            // Memory
            .route("/v1/memory/store", post(routes::memory::store_handler))
            .route("/v1/memory/store-batch", post(routes::memory::store_batch_handler))
            .route("/v1/memory/recall", post(routes::memory::recall_handler))
            .route("/v1/memory/recall-recent", post(routes::memory::recall_recent_handler))
            .route("/v1/memory/recall-by-type", post(routes::memory::recall_by_type_handler))
            .route("/v1/memory/forget", post(routes::memory::forget_handler))
            .route("/v1/memory/decay", post(routes::memory::decay_handler))
            .route("/v1/memory/entity-history", post(routes::memory::entity_history_handler))
            .route("/v1/memory/consolidate", post(routes::memory::consolidate_handler))
            // Graph
            .route("/v1/graph/associate", post(routes::graph::associate_handler))
            .route("/v1/graph/expand", post(routes::graph::expand_handler))
            // RAG
            .route("/v1/rag/query", post(routes::rag::rag_query_handler))
            // Admin
            .route("/v1/admin/checkpoint", post(routes::admin::checkpoint_handler))
            .route("/v1/admin/vacuum", post(routes::admin::vacuum_handler))
            // Metrics
            .route("/metrics", get(routes::admin::metrics_handler))
            // CDC
            .route("/v1/subscribe", get(routes::subscribe::subscribe_handler))
            // Middleware (auth BEFORE all protected routes)
            .layer(auth_layer)
            .layer(middleware::from_fn(request_id_middleware))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .layer(cors_layer)
            .layer(CompressionLayer::new())
            .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024)) // 10MB max body
            .layer(middleware::from_fn_with_state(
                state.clone(),
                Self::rate_limit_middleware,
            ))
            .with_state(state.clone());

        let addr = format!("{}:{}", state.config.host, state.config.port);
        tracing::info!(
            "Lightning server starting on {} (read_only={}, buffer_pool_mb={}, tls={})",
            addr,
            state.config.read_only,
            state.config.buffer_pool_size / (1024 * 1024),
            state.config.tls_enabled,
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

            let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
                .await
                .expect("Failed to configure TLS");

            let addr: std::net::SocketAddr = format!("{}:{}", state.config.host, state.config.port)
                .parse()
                .expect("Invalid address");

            axum_server::bind_rustls(addr, tls_config)
                .serve(app.into_make_service())
                .await
                .expect("Server exited with error");
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
