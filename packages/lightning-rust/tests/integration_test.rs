use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;
use std::net::TcpStream;

use lightning_client::*;

struct ServerGuard(Mutex<Option<Child>>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.0.lock() {
            if let Some(mut child) = guard.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        let _ = std::fs::remove_dir_all("/tmp/lightning-rust-test-db");
    }
}

static SERVER_GUARD: std::sync::OnceLock<ServerGuard> = std::sync::OnceLock::new();

fn server_guard() -> &'static ServerGuard {
    SERVER_GUARD.get_or_init(|| ServerGuard(Mutex::new(None)))
}

fn ensure_server() -> String {
    let url = std::env::var("LIGHTNING_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());

    if is_server_ready(&url) {
        return url;
    }

    let mut guard = server_guard().0.lock().unwrap();
    if guard.is_some() {
        return url;
    }

    let child = Command::new("cargo")
        .args([
            "run", "-p", "lightning-server", "--",
            "--db-path", "/tmp/lightning-rust-test-db",
            "--port", "8080",
            "--log", "lightning_server=error",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|_| {
            // Try with the compiled binary as fallback
            Command::new("./target/debug/lightning-server")
                .args([
                    "--db-path", "/tmp/lightning-rust-test-db",
                    "--port", "8080",
                    "--log", "lightning_server=error",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect(
                    "failed to start lightning-server. Build it first:\n  \
                     cargo build -p lightning-server\n  \
                     or start manually:\n  \
                     cargo run -p lightning-server -- --db-path /tmp/lightning-rust-test-db --port 8080",
                )
        });

    *guard = Some(child);

    for _ in 0..30 {
        if is_server_ready(&url) {
            return url;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    panic!(
        "lightning-server did not start within 15s. \
         Start it manually: cargo run -p lightning-server -- --db-path /tmp/lightning-rust-test-db --port 8080"
    );
}

fn is_server_ready(url: &str) -> bool {
    let host = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');
    TcpStream::connect_timeout(&host.parse().unwrap(), Duration::from_secs(1)).is_ok()
}

fn create_client(url: &str) -> Client {
    let config = ClientConfig::new(url)
        .with_timeout(Duration::from_secs(10));
    Client::new(config).expect("failed to create client")
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_endpoint() {
    let url = ensure_server();
    let client = create_client(&url);

    let health = client.health().await.expect("health check failed");
    assert!(health.is_object(), "health should return an object");
}

// ---------------------------------------------------------------------------
// Memory: Store & Recall
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_store_and_recall() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "Rust integration test memory".into(),
        entity_type: "test".into(),
        metadata: r#"{"source":"rust-integration"}"#.into(),
        embedding: None,
        ttl_seconds: Some(3600),
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let results = client
        .recall("Rust integration test", None, 5)
        .await
        .expect("recall failed");

    assert!(!results.is_empty(), "should find stored memory");
    assert!(
        results.iter().any(|r| r.id == id),
        "stored memory should be in results"
    );
}

#[tokio::test]
async fn test_store_with_embedding() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "entity with embedding".into(),
        entity_type: "embed_test".into(),
        embedding: Some(vec![0.1, 0.2, 0.3, 0.4, 0.5]),
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store with embedding failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = client
        .recall("entity with embedding", Some(&[0.1, 0.2, 0.3, 0.4, 0.5]), 5)
        .await
        .expect("recall with embedding failed");
    assert!(!results.is_empty(), "should find by embedding recall");
}

#[tokio::test]
async fn test_store_batch() {
    let url = ensure_server();
    let client = create_client(&url);

    let entities: Vec<StoreRequest> = (0..3)
        .map(|i| StoreRequest {
            id: uuid::Uuid::new_v4().to_string(),
            content: format!("batch entity {}", i),
            entity_type: "batch_test".into(),
            embedding: None,
            metadata: "{}".into(),
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        })
        .collect();

    let stored = client
        .store_batch(entities)
        .await
        .expect("store_batch failed");
    assert_eq!(stored, 3, "should store all 3 entities");
}

#[tokio::test]
async fn test_forget() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "to be forgotten".into(),
        entity_type: "test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let deleted = client.forget(&id).await.expect("forget failed");
    assert!(deleted, "forget should return true");
}

#[tokio::test]
async fn test_recall_by_type() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "type-specific memory".into(),
        entity_type: "rust_test_type".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = client
        .recall_by_type("rust_test_type", 10)
        .await
        .expect("recall_by_type failed");
    assert!(!results.is_empty(), "should find memories of this type");
    assert!(
        results.iter().any(|e| e.id == id),
        "stored entity should be in results"
    );
}

#[tokio::test]
async fn test_recall_recent() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "recent memory".into(),
        entity_type: "test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = client
        .recall_recent(10)
        .await
        .expect("recall_recent failed");
    assert!(!results.is_empty(), "should find recent memories");
}

#[tokio::test]
async fn test_entity_history() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "history test v1".into(),
        entity_type: "history_test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };
    client.store(req).await.expect("store v1 failed");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let req2 = StoreRequest {
        id: id.clone(),
        content: "history test v2".into(),
        entity_type: "history_test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };
    client.store(req2).await.expect("store v2 failed");

    let history = client
        .entity_history(&id)
        .await
        .expect("entity_history failed");
    assert!(!history.is_empty(), "history should have entries");
}

#[tokio::test]
async fn test_consolidate() {
    let url = ensure_server();
    let client = create_client(&url);

    let result = client
        .consolidate(ConsolidateRequest {
            similarity_threshold: Some(0.95),
            contradiction_jaccard_max: None,
            contradiction_cosine_min: None,
            contradiction_length_sim_min: None,
            max_comparisons_per_entity: Some(100),
        })
        .await;
    assert!(result.is_ok(), "consolidate should succeed");
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_associate_and_expand() {
    let url = ensure_server();
    let client = create_client(&url);

    let src_id = uuid::Uuid::new_v4().to_string();
    let dst_id = uuid::Uuid::new_v4().to_string();

    async fn store_entity(client: &Client, id: &str) -> Result<(), Error> {
        let req = StoreRequest {
            id: id.to_string(),
            content: format!("graph node {}", id),
            entity_type: "graph_test".into(),
            embedding: None,
            metadata: "{}".into(),
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        };
        client.store(req).await
    }

    store_entity(&client, &src_id).await.expect("store src failed");
    store_entity(&client, &dst_id).await.expect("store dst failed");

    client
        .associate(&src_id, &dst_id, "knows", 1.0)
        .await
        .expect("associate failed");

    let results = client
        .expand(&src_id, 1, None)
        .await
        .expect("expand failed");
    assert!(
        results.iter().any(|e| e.id == dst_id),
        "expand should find associated entity"
    );
}

// ---------------------------------------------------------------------------
// RAG
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rag_query() {
    let url = ensure_server();
    let client = create_client(&url);

    let result = client
        .rag_query("test query", None, 5, None)
        .await;
    assert!(result.is_ok(), "RAG query should succeed: {:?}", result.err());
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_raw_query() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "query test data".into(),
        entity_type: "query_test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let query = format!(
        "MATCH (n:query_test) WHERE n.id = '{}' RETURN n.id, n.content",
        id
    );
    let result = client
        .query(&query, None, None, 5000)
        .await
        .expect("query failed");

    assert!(result.num_rows > 0, "query should return results");
    assert!(!result.columns.is_empty(), "query should return columns");
}

// ---------------------------------------------------------------------------
// Auth (only works with --auth-mode none which is default)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_me() {
    let url = ensure_server();
    let client = create_client(&url);

    let result = client.me().await;
    assert!(
        result.is_ok(),
        "me() should work in no-auth mode: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_admin_checkpoint_and_vacuum() {
    let url = ensure_server();
    let client = create_client(&url);

    client.checkpoint().await.expect("checkpoint should succeed");
    client.vacuum().await.expect("vacuum should succeed");
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics_endpoint() {
    let url = ensure_server();
    let client = create_client(&url);

    let metrics = client.metrics().await.expect("metrics failed");
    assert!(!metrics.is_empty(), "metrics should not be empty");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_validation_errors_returned_properly() {
    let url = ensure_server();
    let client = create_client(&url);

    let req = StoreRequest {
        id: "".into(),
        content: "".into(),
        entity_type: "".into(),
        metadata: "{}".into(),
        embedding: None,
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    let result = client.store(req).await;
    assert!(result.is_err(), "empty id/content should fail validation");
    match result {
        Err(Error::Validation(_)) => {}
        _ => panic!("expected Validation error, got {:?}", result),
    }
}

#[tokio::test]
async fn test_forget_nonexistent() {
    let url = ensure_server();
    let client = create_client(&url);

    let result = client.forget("nonexistent-entity-id-12345").await;
    assert!(result.is_ok(), "forget on non-existent should return ok");
    assert!(!result.unwrap(), "should return false for non-existent");
}

#[tokio::test]
async fn test_store_with_ttl_seconds() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "ttl test".into(),
        entity_type: "test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: Some(1),
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store with ttl failed");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = client
        .recall("ttl test", None, 5)
        .await
        .expect("recall failed");
    assert!(
        results.iter().any(|r| r.id == id),
        "ttl entity should be found"
    );
}

// ---------------------------------------------------------------------------
// Blocking API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_blocking_api() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "blocking API test".into(),
        entity_type: "test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client
        .blocking_store(req)
        .expect("blocking store failed");

    let results = client
        .blocking_recall("blocking API test", None, 5)
        .expect("blocking recall failed");
    assert!(!results.is_empty(), "blocking recall should find data");
    assert!(
        results.iter().any(|r| r.id == id),
        "blocking recall should find stored entity"
    );
}

#[tokio::test]
async fn test_blocking_store_batch() {
    let url = ensure_server();
    let client = create_client(&url);

    let entities: Vec<StoreRequest> = (0..2)
        .map(|i| StoreRequest {
            id: uuid::Uuid::new_v4().to_string(),
            content: format!("blocking batch {}", i),
            entity_type: "batch_test".into(),
            metadata: "{}".into(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        })
        .collect();

    let stored = client
        .blocking_store_batch(entities)
        .expect("blocking store_batch failed");
    assert_eq!(stored, 2, "should store 2 entities");
}

#[tokio::test]
async fn test_blocking_forget() {
    let url = ensure_server();
    let client = create_client(&url);

    let id = uuid::Uuid::new_v4().to_string();
    let req = StoreRequest {
        id: id.clone(),
        content: "blocking forget".into(),
        entity_type: "test".into(),
        embedding: None,
        metadata: "{}".into(),
        ttl_seconds: None,
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.blocking_store(req).expect("store failed");

    let deleted = client.blocking_forget(&id).expect("forget failed");
    assert!(deleted, "forget should succeed");
}

#[tokio::test]
async fn test_blocking_graph() {
    let url = ensure_server();
    let client = create_client(&url);

    let src_id = uuid::Uuid::new_v4().to_string();
    let dst_id = uuid::Uuid::new_v4().to_string();

    for id in [&src_id, &dst_id] {
        client
            .blocking_store(StoreRequest {
                id: id.to_string(),
                content: format!("bg {}", id),
                entity_type: "bg_test".into(),
                embedding: None,
                metadata: "{}".into(),
                ttl_seconds: None,
                created_at: None,
                last_accessed: None,
                access_count: None,
                valid_from: None,
                valid_until: None,
            })
            .expect("store failed");
    }

    client
        .blocking_associate(&src_id, &dst_id, "connected", 1.0)
        .expect("associate failed");

    let results = client
        .blocking_expand(&src_id, 1, None)
        .expect("expand failed");
    assert!(
        results.iter().any(|e| e.id == dst_id),
        "should find associated node"
    );
}

#[tokio::test]
async fn test_blocking_health() {
    let url = ensure_server();
    let client = create_client(&url);

    let health = client.blocking_health().expect("health failed");
    assert!(health.is_object());
}
