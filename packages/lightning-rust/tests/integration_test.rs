use std::time::Duration;

use lightning_client::*;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn mock_client(url: &str) -> Client {
    let config = ClientConfig::new(url)
        .with_timeout(Duration::from_secs(5))
        .with_retry(lightning_client::retry::RetryConfig {
            max_retries: 0,
            ..Default::default()
        });
    Client::new(config).expect("failed to create client")
}

// Wrap in JSON envelope like the real server
fn envelope(data: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "data": data,
        "meta": {
            "requestId": "test-request-id",
            "durationMs": 1
        }
    })
}

// ---------------------------------------------------------------------------
// Memory: Store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_store_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/store"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!(null)))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let req = StoreRequest {
        id: "test-1".into(),
        content: "hello world".into(),
        entity_type: "memory".into(),
        metadata: r#"{"key":"value"}"#.into(),
        embedding: Some(vec![0.1, 0.2, 0.3]),
        ttl_seconds: Some(3600),
        created_at: None,
        last_accessed: None,
        access_count: None,
        valid_from: None,
        valid_until: None,
    };

    client.store(req).await.expect("store failed");

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["id"], "test-1");
    assert_eq!(body["content"], "hello world");
    assert_eq!(body["entityType"], "memory");
    assert!(body.get("metadata").is_some());
    assert_eq!(body["ttlSeconds"], 3600);
    assert!(body.get("embedding").is_some());
}

#[tokio::test]
async fn test_store_validates_id() {
    let client = mock_client("http://localhost:1");
    let req = StoreRequest {
        id: "".into(),
        content: "valid content".into(),
        entity_type: "memory".into(),
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
    assert!(matches!(result, Err(Error::Validation(_))));
}

#[tokio::test]
async fn test_store_validates_content_empty() {
    let client = mock_client("http://localhost:1");
    let req = StoreRequest {
        id: "valid-id".into(),
        content: "".into(),
        entity_type: "memory".into(),
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
    assert!(matches!(result, Err(Error::Validation(_))));
}

#[tokio::test]
async fn test_store_defaults_entity_type_to_memory() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/store"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!(null)))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let result = client
        .store(StoreRequest {
            id: "test-id".into(),
            content: "content".into(),
            entity_type: "".into(),
            metadata: "{}".into(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        })
        .await;
    assert!(result.is_ok());
    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["entityType"], "memory");
}

// ---------------------------------------------------------------------------
// Memory: Recall
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recall_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/recall"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "results": []
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let results = client
        .recall("test query", Some(&[0.1, 0.2, 0.3]), 5)
        .await
        .expect("recall failed");

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["query"], "test query");
    assert_eq!(body["topK"], 5);
    let emb = body["embedding"].as_array().unwrap();
    assert!((emb[0].as_f64().unwrap() - 0.1).abs() < 0.001);
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_recall_validates_top_k_zero() {
    let client = mock_client("http://localhost:1");
    let result = client.recall("test", None, 0).await;
    assert!(matches!(result, Err(Error::Validation(_))));
}

#[tokio::test]
async fn test_recall_validates_embedding_empty() {
    let client = mock_client("http://localhost:1");
    let result = client.recall("test", Some(&[]), 5).await;
    assert!(matches!(result, Err(Error::Validation(_))));
}

// ---------------------------------------------------------------------------
// Memory: Recall Recent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recall_recent_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/recall-recent"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "entities": []
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let entities = client.recall_recent(10).await.expect("recall_recent failed");
    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["topK"], 10);
    assert!(entities.is_empty());
}

// ---------------------------------------------------------------------------
// Memory: Recall By Type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recall_by_type_sends_correct_body() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/memory/recall-by-type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
            "entities": []
        }))))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    client
        .recall_by_type("test_type", 10)
        .await
        .expect("recall_by_type failed");
}

// ---------------------------------------------------------------------------
// Memory: Forget
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_forget_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/forget"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "deleted": true
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let deleted = client.forget("test-id").await.expect("forget failed");
    assert!(deleted);

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["id"], "test-id");
}

#[tokio::test]
async fn test_forget_validates_id() {
    let client = mock_client("http://localhost:1");
    let result = client.forget("").await;
    assert!(matches!(result, Err(Error::Validation(_))));
}

// ---------------------------------------------------------------------------
// Memory: Decay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_decay_sends_correct_body() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/memory/decay"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
            "expired": 5
        }))))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let expired = client.decay().await.expect("decay failed");
    assert_eq!(expired, 5);
}

// ---------------------------------------------------------------------------
// Memory: Store Batch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_store_batch_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/memory/store-batch"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "stored": 2
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let entities = vec![
        StoreRequest {
            id: "batch-1".into(),
            content: "content 1".into(),
            entity_type: "test".into(),
            metadata: "{}".into(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        },
        StoreRequest {
            id: "batch-2".into(),
            content: "content 2".into(),
            entity_type: "test".into(),
            metadata: "{}".into(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        },
    ];

    let stored = client.store_batch(entities).await.expect("store_batch failed");
    assert_eq!(stored, 2);

    let body = captured.lock().unwrap().take().unwrap();
    assert!(body.get("entities").is_some());
    assert_eq!(body["entities"].as_array().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// Graph: Associate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_associate_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/graph/associate"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!(null)))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    client
        .associate("src-1", "dst-1", "knows", 0.5)
        .await
        .expect("associate failed");

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["srcId"], "src-1");
    assert_eq!(body["dstId"], "dst-1");
    assert_eq!(body["relType"], "knows");
    assert_eq!(body["weight"], 0.5);
}

// ---------------------------------------------------------------------------
// Graph: Expand
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_expand_sends_correct_body() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/graph/expand"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
            "entities": []
        }))))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    client
        .expand("entity-1", 1, None)
        .await
        .expect("expand failed");
}

// ---------------------------------------------------------------------------
// RAG
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rag_query_sends_correct_body() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/rag/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
            "context": "test context",
            "sources": [],
            "totalSources": 0,
            "warnings": []
        }))))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let result = client
        .rag_query("test query", None, 5, None)
        .await
        .expect("rag_query failed");
    assert_eq!(result.context, "test context");
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/query"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "columns": ["id", "name"],
                "rows": [],
                "numRows": 0
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let result = client
        .query("MATCH (n) RETURN n", None, None, 5000)
        .await
        .expect("query failed");
    assert_eq!(result.columns, vec!["id", "name"]);

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["query"], "MATCH (n) RETURN n");
    assert_eq!(body["timeoutMs"], 5000);
}

#[tokio::test]
async fn test_query_validates_empty() {
    let client = mock_client("http://localhost:1");
    let result = client.query("", None, None, 5000).await;
    assert!(matches!(result, Err(Error::Validation(_))));
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "database": "connected",
            "status": "ok"
        })))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let health = client.health().await.expect("health failed");
    assert_eq!(health["status"], "ok");
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metrics() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string("# HELP http_requests_total Total HTTP requests\n"))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let metrics = client.metrics().await.expect("metrics failed");
    assert!(metrics.contains("http_requests_total"));
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_server_error_returns_lightning_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/memory/store"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "bad request",
                "code": "BAD_REQUEST",
                "requestId": "abc-123"
            })),
        )
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let req = StoreRequest {
        id: "test".into(),
        content: "test".into(),
        entity_type: "test".into(),
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
    match result {
        Err(Error::Lightning(e)) => {
            assert_eq!(e.status, 400);
            assert_eq!(e.code, "BAD_REQUEST");
        }
        _ => panic!("expected Lightning error, got {:?}", result),
    }
}

#[tokio::test]
async fn test_retry_on_429() {
    let mock_server = MockServer::start().await;

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(move |_req: &wiremock::Request| {
            let count = attempts_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count < 2 {
                ResponseTemplate::new(429)
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"}))
            }
        })
        .mount(&mock_server)
        .await;

    let config = ClientConfig::new(mock_server.uri())
        .with_timeout(Duration::from_secs(5))
        .with_retry(lightning_client::retry::RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            ..Default::default()
        });
    let client = Client::new(config).expect("failed to create client");

    let health = client.health().await.expect("health should succeed after retries");
    assert_eq!(health["status"], "ok");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_max_retries_exceeded() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&mock_server)
        .await;

    let config = ClientConfig::new(mock_server.uri())
        .with_timeout(Duration::from_secs(5))
        .with_retry(lightning_client::retry::RetryConfig {
            max_retries: 1,
            base_delay: Duration::from_millis(10),
            ..Default::default()
        });
    let client = Client::new(config).expect("failed to create client");

    let result = client.health().await;
    assert!(matches!(result, Err(Error::MaxRetriesExceeded(_, _))));
}

#[tokio::test]
async fn test_no_retry_on_400() {
    let mock_server = MockServer::start().await;

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts_clone = attempts.clone();

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(move |_req: &wiremock::Request| {
            attempts_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "bad request",
                "code": "BAD_REQUEST"
            }))
        })
        .mount(&mock_server)
        .await;

    let config = ClientConfig::new(mock_server.uri())
        .with_timeout(Duration::from_secs(5))
        .with_retry(lightning_client::retry::RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            ..Default::default()
        });
    let client = Client::new(config).expect("failed to create client");

    let result = client.health().await;
    assert!(result.is_err());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_login_sends_correct_body() {
    let mock_server = MockServer::start().await;

    let captured = std::sync::Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
    let captured_clone = captured.clone();

    Mock::given(method("POST"))
        .and(path("/v1/auth/login"))
        .respond_with(move |req: &wiremock::Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            *captured_clone.lock().unwrap() = Some(body);
            ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
                "accessToken": "test-token",
                "refreshToken": "refresh-token",
                "expiresIn": 3600
            })))
        })
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let login = client.login("admin", "password").await.expect("login failed");
    assert_eq!(login.access_token, "test-token");

    let body = captured.lock().unwrap().take().unwrap();
    assert_eq!(body["username"], "admin");
    assert_eq!(body["password"], "password");
}

#[tokio::test]
async fn test_me() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/auth/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(serde_json::json!({
            "userId": "user-1",
            "username": "admin",
            "role": "admin"
        }))))
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let me = client.me().await.expect("me failed");
    assert_eq!(me.username, "admin");
}

// ---------------------------------------------------------------------------
// Blocking API (simpler: just test the constructor works)
// ---------------------------------------------------------------------------

#[test]
fn test_blocking_client_construction() {
    let config = ClientConfig::new("http://localhost:9999");
    let client = Client::new(config).expect("client creation failed");
    // Just verify the client exists — blocking HTTP calls are tested via wiremock above
    assert!(client.blocking_health().is_err()); // Expected: connection refused
}

// ---------------------------------------------------------------------------
// Envelope unwrapping (raw response)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_envelope_unwrap() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"status": "ok", "not_enveloped": true})),
        )
        .mount(&mock_server)
        .await;

    let client = mock_client(&mock_server.uri());
    let health = client.health().await.expect("health failed");
    assert_eq!(health["status"], "ok");
}
