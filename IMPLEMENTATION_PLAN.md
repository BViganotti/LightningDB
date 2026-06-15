# Lightning HTTP Server — Implementation Plan

## Overview

Lightning is a graph+vector+RAG database for AI agents. This plan adds a
lightweight HTTP server on top of the existing engine, enabling `docker run` usage
and language-agnostic access. The existing Rust library stays available for direct use.

```ascii
  curl / Python requests / any HTTP client
         │
         ▼  HTTP/1.1 + JSON (port 8080)
  ┌──────────────────────────┐
  │   lightning-server       │  ← new binary crate (axum + tokio)
  │  ┌────────────────────┐  │
  │  │  Router            │  │  ← 8-10 endpoints
  │  │  /v1/query         │  │
  │  │  /v1/memory/*      │  │
  │  │  /v1/graph/*       │  │
  │  │  /health           │  │
  │  │  /metrics          │  │
  │  ├────────────────────┤  │
  │  │  MemoryStore       │  │  ← existing Rust struct (unchanged)
  │  │  Connection        │  │  ← existing Rust struct (unchanged)
  │  ├────────────────────┤  │
  │  │  Database (engine) │  │  ← existing, 1 instance, shared via Arc
  │  └────────────────────┘  │
  └──────────────────────────┘
```

**Framework choice: axum** (built by the tokio team on top of hyper and tower).

| Criterion | axum | actix-web |
|---|---|---|
| Async runtime | tokio (native) | tokio (adapted) |
| Middleware ecosystem | tower (std) | actix-specific |
| Community / docs | Largest | Large |
| Performance | ~1M req/s | ~1.2M req/s (marginally faster) |
| Compile time | Fast | Slower |
| Error handling | Type-safe via `Result<T, AppError>` | Type-safe via `HttpResponse` |
| Why choose | Ecosystem, maintainability, "batteries included but replaceable" | Raw throughput |

axum wins because reliability and ecosystem matter more than 20% throughput gains for a
database server where queries take 1-100ms anyway.

---

## Phase 0: Project Scaffolding (1 day)

### 0.1 New crate: `crates/lightning-server`

```toml
# crates/lightning-server/Cargo.toml
[package]
name = "lightning-server"
version.workspace = true
edition.workspace = true

[[bin]]
name = "lightning-server"
path = "src/main.rs"

[dependencies]
# Engine
lightning-core.workspace = true

# HTTP framework
axum = { version = "0.8", features = ["json", "macros"] }
tokio = { version = "1", features = ["full"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "trace", "compression-gzip", "metrics"] }

# Serialization
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }

# Observability
tracing = { workspace = true }
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }

# CLI & config
clap = { version = "4", features = ["derive"] }

# Async
crossbeam = { workspace = true }
parking_lot = { workspace = true }

# Static assets / OpenAPI
utoipa = { version = "5", features = ["axum", "serde_json"] }
utoipa-swagger-ui = { version = "8", features = ["axum"] }
```

Register in workspace `Cargo.toml`:
```toml
members = [
    ...
    "crates/lightning-server",
]
```

### 0.2 Module structure

```
crates/lightning-server/src/
├── main.rs            # entry point, CLI, init
├── server.rs          # axum router, middleware stack, graceful shutdown
├── config.rs          # CLI args, config file parsing
├── extract.rs         # custom axum extractors (DbConnection, etc.)
├── routes/
│   ├── mod.rs
│   ├── health.rs      # GET /health
│   ├── query.rs       # POST /v1/query
│   ├── memory.rs      # POST /v1/memory/{store,recall,forget,...}
│   ├── graph.rs       # POST /v1/graph/{associate,expand}
│   └── admin.rs       # POST /v1/checkpoint, /v1/vacuum
├── models/
│   ├── mod.rs
│   ├── request.rs     # all request types
│   └── response.rs    # all response types, error format
└── streaming.rs       # SSE streaming for query_stream / subscribe_changes
```

### 0.3 Binary entry point

```rust
// crates/lightning-server/src/main.rs
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("lightning_server=info".parse().unwrap())
        )
        .json()
        .init();

    let args = CliArgs::parse();
    let config = ServerConfig::from_args(&args);

    let db = Database::open(&config.db_path)
        .expect("Failed to open database");
    let store = MemoryStore::new(db.connect(), DEFAULT_EMBEDDING_DIM);

    let state = AppState::new(db, store, config).await;

    let server = Server::new(state);
    server.run().await;
}
```

---

## Phase 1: Core Server (1 week)

### 1.1 App State

```rust
// server.rs
pub struct AppState {
    pub db: Arc<Database>,
    pub store: Arc<MemoryStore>,
    pub config: ServerConfig,
}

impl AppState {
    pub async fn new(db: Database, store: MemoryStore, config: ServerConfig) -> Self {
        store.ensure_schema().expect("Failed to initialize schema");
        Self {
            db: Arc::new(db),
            store: Arc::new(store),
            config,
        }
    }
}
```

### 1.2 Router + Middleware

```rust
// server.rs
pub struct Server {
    state: Arc<AppState>,
}

impl Server {
    pub fn new(state: AppState) -> Self {
        Self { state: Arc::new(state) }
    }

    pub async fn run(self) {
        let state = self.state;

        let app = Router::new()
            // Health
            .route("/health", get(health::health_handler))

            // Cypher Query
            .route("/v1/query", post(query::query_handler))
            .route("/v1/query/stream", get(query::query_stream_handler))

            // Memory operations
            .route("/v1/memory/store", post(memory::store_handler))
            .route("/v1/memory/recall", post(memory::recall_handler))
            .route("/v1/memory/forget", post(memory::forget_handler))
            .route("/v1/memory/decay", post(memory::decay_handler))

            // Graph operations
            .route("/v1/graph/associate", post(graph::associate_handler))
            .route("/v1/graph/expand", post(graph::expand_handler))

            // RAG
            .route("/v1/rag/query", post(memory::rag_query_handler))

            // Admin
            .route("/v1/admin/checkpoint", post(admin::checkpoint_handler))
            .route("/v1/admin/vacuum", post(admin::vacuum_handler))

            // Metrics
            .route("/metrics", get(admin::metrics_handler))

            // OpenAPI docs
            .route("/docs", get(|| async {
                axum::response::Redirect::temporary("/swagger-ui/")
            }))
            .merge(SwaggerUi::new("/swagger-ui").url("/api/openapi.json", openapi()))

            // Middleware stack
            .layer((
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(DefaultOnResponse::new().level(Level::INFO)),
                CorsLayer::permissive(),
                CompressionLayer::new(),
                RequestIdLayer::new(),  // auto-generates x-request-id
            ))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(
            (self.state.config.host.clone(), self.state.config.port)
        ).await.unwrap();

        tracing::info!("Lightning server listening on {}:{}",
            self.state.config.host, self.state.config.port);

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("Shutdown signal received, draining connections...");
}
```

### 1.3 Custom Extractors

```rust
// extract.rs
/// Axum extractor that provides a fresh Connection for each request.
/// Each HTTP request gets its own Connection (lightweight, thread-safe).
pub struct DbConnection(pub Connection);

impl<S> FromRequestParts<S> for DbConnection
where
    S: Send + Sync,
    Arc<AppState>: FromRef<S>,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        parts: &mut request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let state = Arc::<AppState>::from_ref(state);
        let conn = state.db.connect();
        Ok(DbConnection(conn))
    }
}

/// Axum extractor for the MemoryStore.
pub struct AppStore(pub Arc<MemoryStore>);

impl<S> FromRequestParts<S> for AppStore
where
    S: Send + Sync,
    Arc<AppState>: FromRef<S>,
{
    type Rejection = (StatusCode, Json<ErrorResponse>);

    async fn from_request_parts(
        parts: &mut request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let state = Arc::<AppState>::from_ref(state);
        Ok(AppStore(state.store.clone()))
    }
}
```

---

## Phase 2: Shared Error Model (2 days)

### 2.1 Unified error type

```rust
// models/response.rs
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

// Map any engine error to HTTP 500 with structured JSON
impl From<LightningError> for ErrorResponse {
    fn from(e: LightningError) -> Self {
        let code = match &e {
            LightningError::Query(_) => Some("QUERY_ERROR".into()),
            LightningError::Config(_) => Some("CONFIG_ERROR".into()),
            LightningError::Internal(_) => Some("INTERNAL_ERROR".into()),
            LightningError::Database(_) => Some("DATABASE_ERROR".into()),
            LightningError::Io(_) => Some("IO_ERROR".into()),
        };
        ErrorResponse {
            error: e.to_string(),
            code,
            details: None,
            request_id: None,
        }
    }
}

/// All API responses follow this shape:
/// { "data": ..., "meta": { "request_id": "...", "duration_ms": 12 } }
#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub data: T,
    pub meta: ResponseMeta,
}

#[derive(Serialize)]
pub struct ResponseMeta {
    pub request_id: String,
    pub duration_ms: u64,
}
```

### 2.2 Global error handler

```rust
// Axum's IntoResponse for any Result<T, LightningError>
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            LightningError::Query(s) if s.contains("not found") => StatusCode::NOT_FOUND,
            LightningError::Query(s) if s.contains("already exists") => StatusCode::CONFLICT,
            LightningError::Config(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(ErrorResponse::from(self.0))).into_response()
    }
}
```

---

## Phase 3: Endpoints (1-2 weeks)

### 3.1 Health

```rust
// routes/health.rs
/// Simple health check. Returns 200 OK when the server is ready.
#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, description = "Server is healthy"))
)]
pub async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.config.uptime().as_secs(),
        "db_path": state.config.db_path.to_string_lossy(),
    })
}
```

### 3.2 Cypher Query

```rust
// routes/query.rs
#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default)]
    pub params: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub snapshot_ts: Option<u64>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 { 30000 }

/// Execute an arbitrary Cypher query.
///
/// Accepts `MATCH`, `CREATE`, `SET`, `DELETE`, `CALL`, DDL, etc.
/// Returns rows as an array of JSON objects with column names as keys.
#[utoipa::path(
    post,
    path = "/v1/query",
    request_body = QueryRequest,
    responses((status = 200, description = "Query results"))
)]
pub async fn query_handler(
    DbConnection(conn): DbConnection,
    Json(req): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResponse>>, AppError> {
    let start = Instant::now();

    let params = req.params.map(|p| {
        p.into_iter()
            .map(|(k, v)| (k, Value::from_json(&v)))
            .collect::<HashMap<_, _>>()
    });

    let result = if let Some(ts) = req.snapshot_ts {
        conn.execute_at(&req.query, ts, params)
    } else {
        conn.execute(&req.query, params)
    }.map_err(|e| AppError(e))?;

    let typed = TypedQueryResult::from(result);
    let duration = start.elapsed().as_millis() as u64;

    Ok(Json(ApiResponse {
        data: QueryResponse {
            columns: typed.columns,
            rows: typed.rows,
            num_rows: typed.num_rows,
        },
        meta: ResponseMeta {
            request_id: String::new(),  // set by middleware
            duration_ms: duration,
        },
    }))
}
```

### 3.3 Streaming Query (SSE)

```rust
/// Execute a Cypher query and stream results via Server-Sent Events.
///
/// Useful for large result sets. Each chunk is sent as a separate SSE event.
/// The connection stays open until the query completes or the client disconnects.
pub async fn query_stream_handler(
    DbConnection(conn): DbConnection,
    Query(req): Query<QueryRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, AppError> {
    let rx = conn.query_stream(&req.query)
        .map_err(|e| AppError(e))?;

    let stream = async_stream::stream! {
        let mut row_count = 0u64;
        while let Ok(result) = rx.recv() {
            match result {
                Ok(chunk) => {
                    // Convert Arrow chunk to JSON rows
                    let batch = &chunk.batch;
                    let schema = batch.schema();
                    for row_idx in 0..batch.num_rows() {
                        let mut row = serde_json::Map::new();
                        for col_idx in 0..batch.num_columns() {
                            let col_name = schema.field(col_idx).name();
                            let value = arrow_row_to_json(batch, row_idx, col_idx);
                            row.insert(col_name.to_string(), value);
                        }
                        row_count += 1;
                        yield Ok(Event::default()
                            .json_data(row)
                            .unwrap());
                    }
                }
                Err(e) => {
                    yield Ok(Event::default()
                        .json_data(serde_json::json!({"error": e.to_string()}))
                        .unwrap());
                    break;
                }
            }
        }
        yield Ok(Event::default()
            .json_data(serde_json::json!({"done": true, "total_rows": row_count}))
            .unwrap());
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(Duration::from_secs(15))
    ))
}
```

### 3.4 Memory Store

```rust
// routes/memory.rs
/// Store a single memory entity.
#[utoipa::path(
    post,
    path = "/v1/memory/store",
    request_body = StoreRequest,
    responses((status = 200, description = "Entity stored"))
)]
pub async fn store_handler(
    AppStore(store): AppStore,
    Json(req): Json<StoreRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = Instant::now();
    let entity = MemoryEntity {
        id: req.id,
        entity_type: req.entity_type.unwrap_or("memory".into()),
        content: req.content,
        created_at: req.created_at.unwrap_or_else(now_micros),
        metadata: req.metadata.unwrap_or_else(|| "{}".into()),
        embedding: req.embedding.unwrap_or_default(),
        ttl_seconds: req.ttl_seconds.unwrap_or(0),
        ..Default::default()
    };
    store.store(entity).map_err(AppError)?;
    Ok(Json(ApiResponse {
        data: (),
        meta: meta(start),
    }))
}

#[derive(Deserialize)]
pub struct StoreRequest {
    pub id: String,
    pub content: String,
    pub entity_type: Option<String>,
    pub metadata: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub created_at: Option<i64>,
    pub ttl_seconds: Option<i64>,
}
```

### 3.5 Batch Store

```rust
/// Store multiple entities in a single batch. Significantly faster than
/// individual store calls for bulk operations.
pub async fn store_batch_handler(
    AppStore(store): AppStore,
    Json(req): Json<StoreBatchRequest>,
) -> Result<Json<ApiResponse<StoreBatchResponse>>, AppError> {
    let start = Instant::now();
    let now = now_micros();

    let entities: Vec<MemoryEntity> = req.entities.into_iter().map(|e| MemoryEntity {
        id: e.id,
        entity_type: e.entity_type.unwrap_or("memory".into()),
        content: e.content,
        created_at: e.created_at.unwrap_or(now),
        metadata: e.metadata.unwrap_or_else(|| "{}".into()),
        embedding: e.embedding.unwrap_or_default(),
        ttl_seconds: e.ttl_seconds.unwrap_or(0),
        ..Default::default()
    }).collect();

    let count = store.store_batch(entities).map_err(AppError)?;
    Ok(Json(ApiResponse {
        data: StoreBatchResponse { stored: count },
        meta: meta(start),
    }))
}
```

### 3.6 Recall (Hybrid Search)

```rust
/// Hybrid search: FTS + vector similarity with RRF fusion.
///
/// Provide `query` for full-text search, `embedding` for vector search,
/// or both for hybrid search with reciprocal rank fusion.
pub async fn recall_handler(
    AppStore(store): AppStore,
    Json(req): Json<RecallRequest>,
) -> Result<Json<ApiResponse<RecallResponse>>, AppError> {
    let start = Instant::now();

    let results = store.recall(
        &req.query.unwrap_or_default(),
        req.embedding.as_deref().unwrap_or(&[]),
        req.top_k.unwrap_or(10),
    ).map_err(AppError)?;

    let items: Vec<SearchResultItem> = results.into_iter().map(|r| SearchResultItem {
        id: r.entity.id,
        content: r.entity.content,
        entity_type: r.entity.entity_type,
        score: r.score,
        metadata: r.entity.metadata,
    }).collect();

    Ok(Json(ApiResponse {
        data: RecallResponse { results: items },
        meta: meta(start),
    }))
}

#[derive(Deserialize)]
pub struct RecallRequest {
    pub query: Option<String>,
    pub embedding: Option<Vec<f32>>,
    pub top_k: Option<usize>,
}
```

### 3.7 RAG Query

```rust
/// Full RAG pipeline: hybrid search → graph expansion → reranking → context assembly.
///
/// Returns assembled context string ready for LLM consumption, plus source metadata.
/// Optionally accepts configuration for expansion depth, weights, and cross-encoder.
pub async fn rag_query_handler(
    AppStore(store): AppStore,
    Json(req): Json<RagRequest>,
) -> Result<Json<ApiResponse<RagResponse>>, AppError> {
    let start = Instant::now();

    let config = RagConfig {
        expansion_depth: req.expansion_depth.unwrap_or(3),
        search_weight: req.search_weight.unwrap_or(2.0),
        recency_weight: req.recency_weight.unwrap_or(0.3),
        degree_weight: req.degree_weight.unwrap_or(0.0),
        max_context_tokens: req.max_tokens.unwrap_or(4096),
        ..Default::default()
    };

    let result = store.rag_query_with_config(
        &req.query,
        req.embedding.as_deref().unwrap_or(&[]),
        req.top_k.unwrap_or(5),
        &config,
    ).map_err(AppError)?;

    Ok(Json(ApiResponse {
        data: RagResponse {
            context: result.context,
            sources: result.source_details.into_iter().map(|s| SourceRef {
                id: s.id,
                score: s.score,
                entity_type: s.entity_type,
                excerpt: s.excerpt,
            }).collect(),
            total_sources: result.total_sources,
            warnings: result.warnings,
        },
        meta: meta(start),
    }))
}
```

### 3.8 Graph Operations

```rust
// routes/graph.rs
/// Create a relationship between two entities.
pub async fn associate_handler(
    AppStore(store): AppStore,
    Json(req): Json<AssociateRequest>,
) -> Result<Json<ApiResponse<()>>, AppError> {
    let start = Instant::now();
    store.associate(&req.src_id, &req.dst_id, &req.rel_type, req.weight.unwrap_or(1.0))
        .map_err(AppError)?;
    Ok(Json(ApiResponse { data: (), meta: meta(start) }))
}

/// Expand from an entity via graph edges up to N hops.
/// Returns all reachable entities within the hop radius.
pub async fn expand_handler(
    AppStore(store): AppStore,
    Json(req): Json<ExpandRequest>,
) -> Result<Json<ApiResponse<ExpandResponse>>, AppError> {
    let start = Instant::now();
    let edge_types: Vec<&str> = req.edge_types.as_deref()
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();

    let entities = store.expand(&req.entity_id, req.hops.unwrap_or(1), &edge_types)
        .map_err(AppError)?;

    let items: Vec<EntityItem> = entities.into_iter().map(|e| EntityItem {
        id: e.id,
        entity_type: e.entity_type,
        content: e.content,
        metadata: e.metadata,
    }).collect();

    Ok(Json(ApiResponse {
        data: ExpandResponse { entities: items },
        meta: meta(start),
    }))
}
```

### 3.9 CDC via SSE

```rust
/// Subscribe to real-time change events via Server-Sent Events.
///
/// Each event is a JSON object with `entity_id`, `operation_type`, and `timestamp`.
/// The connection stays open; dropped when the client disconnects.
pub async fn subscribe_handler(
    AppStore(store): AppStore,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, AppError> {
    let rx = store.subscribe_changes().map_err(AppError)?;

    let stream = async_stream::stream! {
        while let Ok(event) = rx.recv() {
            // Use recv() (blocking) on a background thread, not recv_async
            yield Ok(Event::default()
                .json_data(event)
                .unwrap());
        }
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(Duration::from_secs(15))
    ))
}
```

---

## Phase 4: Configuration (2 days)

### 4.1 CLI

```rust
#[derive(clap::Parser)]
#[command(name = "lightning-server", version)]
pub struct CliArgs {
    #[arg(short, long, default_value = "./lightning-data", env = "LIGHTNING_DB_PATH")]
    pub db_path: PathBuf,

    #[arg(short, long, default_value = "0.0.0.0", env = "LIGHTNING_HOST")]
    pub host: String,

    #[arg(short, long, default_value_t = 8080, env = "LIGHTNING_PORT")]
    pub port: u16,

    #[arg(long, default_value_t = 1024, env = "LIGHTNING_BUFFER_POOL_MB")]
    pub buffer_pool_mb: u64,

    #[arg(long, env = "LIGHTNING_READ_ONLY")]
    pub read_only: bool,

    #[arg(long, default_value_t = 1000, env = "LIGHTNING_VACUUM_INTERVAL_MS")]
    pub vacuum_interval_ms: u64,

    #[arg(long, env = "LIGHTNING_LOG")]
    pub log: Option<String>,
}
```

### 4.2 Usage

```bash
# Quick start
docker run -p 8080:8080 -v ./data:/data lightning-db/lightning

# With config
docker run -p 8080:8080 \
  -e LIGHTNING_DB_PATH=/data \
  -e LIGHTNING_BUFFER_POOL_MB=4096 \
  -e LIGHTNING_READ_ONLY=false \
  -v ./data:/data \
  lightning-db/lightning

# Check it works
curl http://localhost:8080/health
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE NODE TABLE Person (name STRING, age INT64, PRIMARY KEY (name))"}'
curl -X POST http://localhost:8080/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:Person) RETURN n.name, n.age"}'
```

---

## Phase 5: Docker & Deployment (2 days)

### 5.1 Dockerfile

```dockerfile
# syntax=docker/dockerfile:1
FROM rust:1.82-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p lightning-server && \
    strip target/release/lightning-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lightning-server /usr/local/bin/
EXPOSE 8080
VOLUME /data
HEALTHCHECK --interval=5s --timeout=1s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
USER nobody
ENTRYPOINT ["lightning-server"]
CMD ["--db-path", "/data", "--host", "0.0.0.0"]
```

### 5.2 Docker Compose

```yaml
version: "3.9"
services:
  lightning:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - lightning-data:/data
    environment:
      LIGHTNING_DB_PATH: /data
      LIGHTNING_BUFFER_POOL_MB: "2048"
      LIGHTNING_LOG: "lightning_server=debug"
      RUST_LOG: "info"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 5s
      timeout: 1s
      retries: 5

volumes:
  lightning-data:
```

### 5.3 Multi-arch Build

```makefile
docker-build:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-t lightning-db/lightning:latest \
		--push .
```

---

## Phase 6: Observability (1 week)

### 6.1 Tracing

Every request gets a trace with:
- `x-request-id` header (auto-generated or forwarded)
- Duration
- Query type (inferred from endpoint)
- Error details

```rust
// In middleware stack:
.layer(TraceLayer::new_for_http()
    .make_span_with(|req: &Request<Body>| {
        let request_id = req.headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        info_span!("http_request",
            method = %req.method(),
            uri = %req.uri(),
            request_id = %request_id,
        )
    })
    .on_response(|response: &Response, _latency: Duration, span: &Span| {
        span.record("status", response.status().as_u16());
    })
)
```

### 6.2 Prometheus Metrics

```rust
// Use tower-http's metrics or opentelemetry-prometheus

// Exposed at GET /metrics
pub async fn metrics_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db_metrics = state.db.metrics();
    let metrics = format!(
        r#"# HELP lightning_queries_total Total queries executed
# TYPE lightning_queries_total counter
lightning_queries_total {}

# HELP lightning_checkpoints_total Total checkpoints
# TYPE lightning_checkpoints_total counter
lightning_checkpoints_total {}

# HELP lightning_buffer_hit_rate Buffer pool hit rate
# TYPE lightning_buffer_hit_rate gauge
lightning_buffer_hit_rate {}

# HELP lightning_http_requests_total Total HTTP requests
# TYPE lightning_http_requests_total counter
lightning_http_requests_total {}
"#,
        db_metrics.total_queries.load(Ordering::Relaxed),
        db_metrics.total_checkpoints.load(Ordering::Relaxed),
        db_metrics.buffer_hit_rate(),
        0, // replace with actual counter
    );
    (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], metrics)
}
```

### 6.3 OpenAPI Docs

Auto-generated via `utoipa`:

```rust
#[derive(OpenApi)]
#[openapi(
    paths(
        health::health_handler,
        query::query_handler,
        memory::store_handler,
        memory::recall_handler,
        memory::rag_query_handler,
        graph::associate_handler,
        graph::expand_handler,
        admin::checkpoint_handler,
    ),
    components(
        schemas(
            QueryRequest, QueryResponse,
            StoreRequest, StoreBatchRequest, StoreBatchResponse,
            RecallRequest, RecallResponse, SearchResultItem,
            RagRequest, RagResponse, SourceRef,
            AssociateRequest, ExpandRequest, ExpandResponse, EntityItem,
            ErrorResponse, ApiResponse<()>,
        )
    ),
    tags(
        (name = "query", description = "Cypher query execution"),
        (name = "memory", description = "Memory entity CRUD & search"),
        (name = "graph", description = "Graph relationship operations"),
        (name = "rag", description = "RAG pipeline"),
        (name = "admin", description = "Database administration"),
    )
)]
pub struct ApiDoc;
```

---

## Phase 7: Testing (ongoing)

### 7.1 Unit tests

```rust
#[tokio::test]
async fn test_store_and_recall() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let store = MemoryStore::new(db.connect(), DEFAULT_EMBEDDING_DIM);
    let state = AppState::new(db, store, ServerConfig::test()).await;
    let app = test_app(state).await;

    // Store an entity
    let resp = app
        .post("/v1/memory/store")
        .json(&serde_json::json!({
            "id": "test-1",
            "content": "Alice likes Python",
            "entity_type": "fact",
        }))
        .await;
    assert_eq!(resp.status(), 200);

    // Recall it
    let resp = app
        .post("/v1/memory/recall")
        .json(&serde_json::json!({
            "query": "Python",
            "top_k": 5,
        }))
        .await;
    assert_eq!(resp.status(), 200);
    let body: ApiResponse<RecallResponse> = resp.json().await;
    assert!(!body.data.results.is_empty());
    assert_eq!(body.data.results[0].id, "test-1");
}
```

### 7.2 Integration tests

```bash
# Start server in background
./target/release/lightning-server --db-path /tmp/test-db --port 8081 &
SERVER_PID=$!
sleep 1

# Test health
curl -f http://localhost:8081/health

# Test Cypher DDL
curl -s -X POST http://localhost:8081/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE NODE TABLE IF NOT EXISTS Person (name STRING, age INT64, PRIMARY KEY (name))"}'

# Test Cypher DML
curl -s -X POST http://localhost:8081/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:Person) RETURN n.name, n.age"}'

# Test MemoryStore
curl -s -X POST http://localhost:8081/v1/memory/store \
  -H "Content-Type: application/json" \
  -d '{"id": "u1", "content": "Alice likes Python", "entity_type": "fact"}'

curl -s -X POST http://localhost:8081/v1/memory/recall \
  -H "Content-Type: application/json" \
  -d '{"query": "Python", "top_k": 5}'

# Test RAG
curl -s -X POST http://localhost:8081/v1/rag/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What does Alice like?", "top_k": 5}'

# Test Graph
curl -s -X POST http://localhost:8081/v1/graph/associate \
  -H "Content-Type: application/json" \
  -d '{"src_id": "u1", "dst_id": "u2", "rel_type": "RelatedTo", "weight": 0.9}'

curl -s -X POST http://localhost:8081/v1/graph/expand \
  -H "Content-Type: application/json" \
  -d '{"entity_id": "u1", "hops": 2}'

# Test CDC subscribe (SSE)
curl -s -N http://localhost:8081/v1/subscribe

# Cleanup
kill $SERVER_PID
```

### 7.3 Performance baseline

```python
# bench.py — keep this in the repo
import requests, time, statistics

BASE = "http://localhost:8081"

def bench(name, fn, n=100):
    latencies = []
    for _ in range(n):
        start = time.time()
        fn()
        latencies.append((time.time() - start) * 1000)
    print(f"{name}: p50={statistics.median(latencies):.1f}ms "
          f"p99={sorted(latencies)[int(n*0.99)]:.1f}ms "
          f"avg={statistics.mean(latencies):.1f}ms")

# Warmup
for _ in range(10):
    requests.post(f"{BASE}/v1/memory/recall", json={"query": "warmup", "top_k": 1})

bench("recall (hybrid)", lambda: requests.post(
    f"{BASE}/v1/memory/recall",
    json={"query": "python programming", "top_k": 5}
))
bench("cypher query", lambda: requests.post(
    f"{BASE}/v1/query",
    json={"query": "MATCH (e:Entity) RETURN e.id, e.content LIMIT 10"}
))
```

---

## Endpoint Reference (full surface)

| Method | Path | Purpose | Response |
|---|---|---|---|
| `GET` | `/health` | Readiness check | `{"status":"ok","version":"0.1.0"}` |
| `POST` | `/v1/query` | Any Cypher query | `{data:{columns,rows,num_rows},meta}` |
| `GET` | `/v1/query/stream` | Streaming (SSE) | `text/event-stream` |
| `POST` | `/v1/memory/store` | Store one entity | `{data:null,meta}` |
| `POST` | `/v1/memory/store-batch` | Store many entities | `{data:{stored:N},meta}` |
| `POST` | `/v1/memory/recall` | Hybrid search | `{data:{results:[{id,content,score}]},meta}` |
| `POST` | `/v1/memory/recall-recent` | Recent entities | `{data:{entities:[]},meta}` |
| `POST` | `/v1/memory/recall-by-type` | Filter by type | `{data:{entities:[]},meta}` |
| `POST` | `/v1/memory/forget` | Soft-delete entity | `{data:{deleted:bool},meta}` |
| `POST` | `/v1/memory/decay` | Expire TTL entities | `{data:{expired:N},meta}` |
| `POST` | `/v1/memory/entity-history` | Full version history | `{data:{versions:[]},meta}` |
| `POST` | `/v1/graph/associate` | Create relationship | `{data:null,meta}` |
| `POST` | `/v1/graph/expand` | Graph traversal | `{data:{entities:[]},meta}` |
| `POST` | `/v1/rag/query` | RAG pipeline | `{data:{context,sources},meta}` |
| `POST` | `/v1/admin/checkpoint` | Force checkpoint | `{data:null,meta}` |
| `POST` | `/v1/admin/vacuum` | Reclaim space | `{data:null,meta}` |
| `GET` | `/v1/admin/metrics` | Prometheus metrics | `text/plain` |
| `GET` | `/v1/subscribe` | CDC (SSE) | `text/event-stream` |
| `GET` | `/docs` | Swagger UI | `text/html` |

---

## Implementation Timeline

| Week | Deliverable | What it unblocks |
|---|---|---|
| **1** | Project scaffold, axum router, `/health`, `/v1/query` | curl "Hello World" |
| **2** | Memory endpoints (`store`, `recall`, `forget`, `decay`) | AI agent can use the server |
| **3** | Graph endpoints (`associate`, `expand`), RAG endpoint | Full memory stack via HTTP |
| **4** | Docker, CI, `/metrics`, tracing, error model | Ship it |
| **5** | Streaming (SSE for `query_stream` + `subscribe_changes`) | Large result sets, CDC |
| **6** | OpenAPI docs, integration tests, benchmark | Ready for users |

**Total time to "docker run" with working agent memory: 3-4 weeks.**

---

## What this enables

```python
# Day 1: Any language can use Lightning
import requests

BASE = "http://localhost:8081"

# Store agent memories
requests.post(f"{BASE}/v1/memory/store", json={
    "id": "session-1",
    "content": "User asked about vector databases",
    "entity_type": "conversation",
}).raise_for_status()

# Hybrid search
resp = requests.post(f"{BASE}/v1/memory/recall", json={
    "query": "vector databases",
    "top_k": 5,
}).json()
for result in resp["data"]["results"]:
    print(f"  [{result['score']:.2f}] {result['content']}")

# Full RAG query
rag = requests.post(f"{BASE}/v1/rag/query", json={
    "query": "What did the user ask about?",
    "top_k": 5,
}).json()
llm_input = rag["data"]["context"]  # Ready to send to GPT/Claude

# Arbitrary Cypher
resp = requests.post(f"{BASE}/v1/query", json={
    "query": "MATCH (e:Entity) WHERE e.type = 'conversation' RETURN e.id, e.content LIMIT 10"
}).json()
for row in resp["data"]["rows"]:
    print(row)
```
