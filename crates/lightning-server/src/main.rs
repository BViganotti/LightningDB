use std::sync::Arc;

use clap::Parser;
use lightning::memory::MemoryStore;
use lightning::Database;
use tracing_subscriber::EnvFilter;
use rand::RngCore;

mod auth;
mod config;
mod error;
mod extract;
mod models;
mod routes;
mod server;
mod streaming;

pub use error::AppError;

fn generate_jwt_secret() -> Vec<u8> {
    let mut secret = vec![0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    secret
}

fn resolve_jwt_secret(args: &config::CliArgs) -> Vec<u8> {
    let secret = match &args.jwt_secret {
        Some(raw) => {
            if let Some(file_path) = raw.strip_prefix('@') {
                let content = std::fs::read_to_string(file_path)
                    .unwrap_or_else(|e| {
                        eprintln!("Failed to read JWT secret file '{}': {}", file_path, e);
                        std::process::exit(1);
                    });
                let trimmed = content.trim().to_string();
                trimmed.into_bytes()
            } else {
                raw.as_bytes().to_vec()
            }
        }
        None => {
            let secret = generate_jwt_secret();
            eprintln!("WARNING: No --jwt-secret provided. Generated random secret.");
            eprintln!("  All JWT tokens will be invalidated on server restart.");
            eprintln!("  Set LIGHTNING_JWT_SECRET or --jwt-secret for stable tokens.");
            secret
        }
    };
    if secret.len() < 32 {
        eprintln!("ERROR: JWT secret must be at least 32 bytes (got {})", secret.len());
        eprintln!("  Generate a secure secret with: openssl rand -base64 32");
        std::process::exit(1);
    }
    secret
}

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
        auth_mode = %config.auth_mode,
        "Opening Lightning database"
    );

    let db = Arc::new(
        Database::open_with_config(&db_path, config.system_config())
            .unwrap_or_else(|e| {
                eprintln!("Failed to open database at '{}': {}", db_path.display(), e);
                std::process::exit(1);
            }),
    );

    let jwt_secret = resolve_jwt_secret(&args);
    let auth_store = Arc::new(
        auth::store::AuthStore::new(
            Arc::clone(&db),
            jwt_secret,
            config.jwt_access_ttl_secs,
            config.jwt_refresh_ttl_secs,
        )
        .unwrap_or_else(|e| {
            eprintln!("Failed to initialize auth store: {}", e);
            std::process::exit(1);
        }),
    );

    // Bootstrap admin user in JWT mode if no users exist
    if config.auth_mode == auth::models::AuthMode::Jwt && auth_store.list_users().is_empty() {
        let admin_password = args
            .admin_password
            .clone()
            .unwrap_or_else(generate_random_password);

        match auth_store.bootstrap_admin(
            &config.admin_username,
            &admin_password,
            auth::models::Role::Admin,
        ) {
            Ok(user) => {
                if args.admin_password.is_none() {
                    eprintln!();
                    eprintln!("=====================================================================");
                    eprintln!("  LightningDB Admin Credentials (generated)");
                    eprintln!("  Username: {}", user.username);
                    eprintln!("  Password: {}", admin_password);
                    eprintln!("  Set LIGHTNING_ADMIN_PASSWORD or --admin-password to suppress this.");
                    eprintln!("=====================================================================");
                    eprintln!();
                }
                tracing::info!(
                    username = %user.username,
                    "Admin user bootstrapped"
                );
            }
            Err(e) => {
                tracing::warn!("Admin bootstrap skipped: {}", e);
            }
        }

    }

    // Start background GC for expired tokens
    let gc_store = Arc::clone(&auth_store);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(e) = gc_store.purge_expired() {
                tracing::warn!("Token GC failed: {}", e);
            }
        }
    });

    let conn = db.connect();
    let store = MemoryStore::new(conn, config.embedding_dim);

    store
        .ensure_schema()
        .unwrap_or_else(|e| {
            eprintln!("Failed to initialize memory schema: {}", e);
            std::process::exit(1);
        });

    let state = server::AppState::new(db, store, config, auth_store);

    let server = server::Server::new(state);
    server.run().await;
}

fn generate_random_password() -> String {
    let mut bytes = [0u8; 24];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    use base64::Engine;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    engine.encode(bytes)
}
