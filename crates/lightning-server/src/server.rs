use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::Request;
use axum::http::header;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use lightning::memory::MemoryStore;
use lightning::Database;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::compression::CompressionLayer;
use tracing::Level;

use crate::config::ServerConfig;
use crate::extract::RequestIdExtension;
use crate::routes;

pub struct AppState {
    pub db: Arc<Database>,
    pub store: Arc<MemoryStore>,
    pub config: ServerConfig,
    pub request_counter: AtomicU64,
}

impl AppState {
    pub fn new(db: Database, store: MemoryStore, config: ServerConfig) -> Self {
        Self {
            db: Arc::new(db),
            store: Arc::new(store),
            config,
            request_counter: AtomicU64::new(0),
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
        header::HeaderValue::from_str(&request_id).unwrap(),
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

    pub async fn run(self) {
        let state = self.state;

        // Build CORS layer from configured allowed origins.
        // Defaults to localhost-only when no --cors-allowed-origins is specified.
        let cors_layer = if state.config.cors_allowed_origins.is_empty() {
            CorsLayer::permissive()
        } else {
            let origins: Vec<axum::http::HeaderValue> = state
                .config
                .cors_allowed_origins
                .iter()
                .filter_map(|o| axum::http::HeaderValue::from_str(o).ok())
                .collect();
            if origins.is_empty() {
                CorsLayer::permissive()
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
            }
        };

        let app = Router::new()
            // Health
            .route("/health", get(routes::health::health_handler))
            // Query
            .route("/v1/query", post(routes::query::query_handler))
            .route("/v1/query/stream", get(routes::query::query_stream_handler))
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
            // Middleware
            .layer(middleware::from_fn(request_id_middleware))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
            )
            .layer(cors_layer)
            .layer(CompressionLayer::new())
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
