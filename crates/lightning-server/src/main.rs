use clap::Parser;
use lightning::memory::MemoryStore;
use lightning::Database;
use tracing_subscriber::EnvFilter;

mod config;
mod error;
mod extract;
mod models;
mod routes;
mod server;
mod streaming;

pub use error::AppError;

#[tokio::main]
async fn main() {
    let args = config::CliArgs::parse();

    let log_filter = args
        .log
        .clone()
        .unwrap_or_else(|| "lightning_server=info,lightning_core=warn".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .parse(&log_filter)
                .unwrap_or_else(|e| {
                    eprintln!("Warning: invalid log filter '{}': {}. Using default.", log_filter, e);
                    EnvFilter::from_default_env()
                }),
        )
        .json()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    let config = config::ServerConfig::from_args(&args);
    if let Err(e) = config.validate() {
        tracing::error!("Configuration error: {}", e);
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

    let db_path = config.db_path.clone();

    tracing::info!(
        db_path = %db_path.display(),
        host = %config.host,
        port = config.port,
        buffer_pool_mb = config.buffer_pool_size / (1024 * 1024),
        read_only = config.read_only,
        embedding_dim = config.embedding_dim,
        "Opening Lightning database"
    );

    let db = Database::open_with_config(&db_path, config.system_config())
        .unwrap_or_else(|e| {
            eprintln!("Failed to open database at '{}': {}", db_path.display(), e);
            std::process::exit(1);
        });

    let conn = db.connect();
    let store = MemoryStore::new(conn, config.embedding_dim);

    store
        .ensure_schema()
        .unwrap_or_else(|e| {
            eprintln!("Failed to initialize memory schema: {}", e);
            std::process::exit(1);
        });

    let state = server::AppState::new(db, store, config);

    let server = server::Server::new(state);
    server.run().await;
}
