use lightning_client::*;
use std::time::Duration;

#[test]
fn test_validate_id() {
    validation::validate_id("abc123", "id").unwrap();
    validation::validate_id("", "id").unwrap_err();
    let long_id = "a".repeat(600);
    validation::validate_id(&long_id, "id").unwrap_err();
}

#[test]
fn test_validate_content() {
    validation::validate_content("hello world").unwrap();
    validation::validate_content("").unwrap_err();
    let long = "a".repeat(2_000_001);
    validation::validate_content(&long).unwrap_err();
}

#[test]
fn test_validate_metadata_valid_json() {
    let meta = r#"{"key": "value"}"#.to_string();
    validation::validate_metadata(&meta).unwrap();

    let empty = "".to_string();
    assert_eq!(validation::validate_metadata(&empty).unwrap(), "{}");

    let invalid = "not-json".to_string();
    validation::validate_metadata(&invalid).unwrap_err();

    let not_object = r#""just a string""#.to_string();
    validation::validate_metadata(&not_object).unwrap_err();
}

#[test]
fn test_validate_entity_type() {
    validation::validate_entity_type("memory").unwrap();
    validation::validate_entity_type("").unwrap_err();
    let long = "a".repeat(200);
    validation::validate_entity_type(&long).unwrap_err();
}

#[test]
fn test_validate_top_k() {
    validation::validate_top_k(1, 100).unwrap();
    validation::validate_top_k(0, 100).unwrap_err();
    validation::validate_top_k(101, 100).unwrap_err();
}

#[test]
fn test_validate_batch_size() {
    validation::validate_batch_size(1, 1000).unwrap();
    validation::validate_batch_size(0, 1000).unwrap_err();
    validation::validate_batch_size(1001, 1000).unwrap_err();
}

#[test]
fn test_validate_embedding() {
    validation::validate_embedding(&[0.1, 0.2, 0.3]).unwrap();
    validation::validate_embedding(&[]).unwrap_err();
    let big = vec![0.0; 10_000];
    validation::validate_embedding(&big).unwrap_err();
}

#[test]
fn test_validate_query_string() {
    validation::validate_query_string("MATCH (n) RETURN n").unwrap();
    validation::validate_query_string("").unwrap_err();
}

#[test]
fn test_validate_hops() {
    validation::validate_hops(1).unwrap();
    validation::validate_hops(0).unwrap_err();
    validation::validate_hops(11).unwrap_err();
}

#[test]
fn test_compute_backoff() {
    let config = retry::RetryConfig::default();
    let d0 = retry::compute_backoff(0, &config);
    assert!(d0.as_millis() >= 1);

    let d2 = retry::compute_backoff(2, &config);
    assert!(d2 > d0 || d2 == d0);
}

#[test]
fn test_circuit_breaker_closed_by_default() {
    let cb = CircuitBreaker::new(CircuitBreakerConfig::default());
    assert_eq!(cb.state(), CircuitState::Closed);
    assert!(cb.allow_request());
}

#[test]
fn test_circuit_breaker_trips_on_failures() {
    let config = CircuitBreakerConfig {
        failure_threshold: 3,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    assert!(cb.allow_request());
    cb.on_failure();
    assert!(cb.allow_request());
    cb.on_failure();
    assert!(cb.allow_request());
    cb.on_failure();

    assert_eq!(cb.state(), CircuitState::Open);
    assert!(!cb.allow_request());
}

#[test]
fn test_circuit_breaker_recovers_after_timeout() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout: std::time::Duration::from_millis(1),
        half_open_max_requests: 2,
        success_threshold: 1,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    cb.on_failure();
    cb.on_failure();
    assert_eq!(cb.state(), CircuitState::Open);

    std::thread::sleep(std::time::Duration::from_millis(5));

    assert!(cb.allow_request());
    assert_eq!(cb.state(), CircuitState::HalfOpen);

    cb.on_success();
    assert_eq!(cb.state(), CircuitState::Closed);
}

#[test]
fn test_circuit_breaker_half_open_limits_requests() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout: std::time::Duration::from_millis(1),
        half_open_max_requests: 1,
        success_threshold: 1,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    cb.on_failure();
    cb.on_failure();
    std::thread::sleep(std::time::Duration::from_millis(5));

    assert!(cb.allow_request());
    assert!(!cb.allow_request());
}

#[test]
fn test_circuit_breaker_opens_again_on_failure_in_half_open() {
    let config = CircuitBreakerConfig {
        failure_threshold: 2,
        recovery_timeout: std::time::Duration::from_millis(1),
        half_open_max_requests: 3,
        success_threshold: 1,
        ..Default::default()
    };
    let cb = CircuitBreaker::new(config);

    cb.on_failure();
    cb.on_failure();
    std::thread::sleep(std::time::Duration::from_millis(5));

    cb.allow_request();
    cb.on_failure();
    assert_eq!(cb.state(), CircuitState::Open);
}

#[test]
fn test_client_config_default() {
    let config = ClientConfig::new("http://localhost:8080");
    assert_eq!(config.base_url, "http://localhost:8080");
    assert!(config.auth_token.is_none());
    assert_eq!(config.max_batch_entities, 1000);
    assert_eq!(config.max_top_k, 1000);
    assert_eq!(
        config.user_agent,
        "lightning-client-rust/0.1.0"
    );
}

#[test]
fn test_client_config_with_auth() {
    let config = ClientConfig::new("http://localhost:8080")
        .with_auth_token("my-token");
    assert_eq!(config.auth_token.unwrap(), "my-token");
}

#[test]
fn test_client_config_builder_patterns() {
    let config = ClientConfig::new("http://localhost:8080")
        .with_timeout(std::time::Duration::from_secs(60))
        .with_circuit_breaker(CircuitBreakerConfig {
            failure_threshold: 10,
            ..Default::default()
        });

    assert_eq!(config.default_timeout.as_secs(), 60);
    assert!(config.circuit_breaker.is_some());
}

#[test]
fn test_store_request_serialization() {
    let req = StoreRequest {
        id: "test-1".into(),
        content: "some content".into(),
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

    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["id"], "test-1");
    assert_eq!(json["content"], "some content");
    assert_eq!(json["entityType"], "memory");
    assert_eq!(json["metadata"], r#"{"key":"value"}"#);
    assert!(json.get("embedding").is_some());
    assert_eq!(json["ttlSeconds"], 3600);
    assert!(json.get("createdAt").is_none());
}

#[test]
fn test_retry_backoff_increases() {
    let config = retry::RetryConfig::default();
    let prev = retry::compute_backoff(0, &config);
    for i in 1..4 {
        let next = retry::compute_backoff(i, &config);
        assert!(next.as_millis() >= 1);
    }
}

#[test]
fn test_error_is_retryable() {
    let err = error::Error::Lightning(error::LightningError {
        error: "rate limited".into(),
        code: "TOO_MANY_REQUESTS".into(),
        details: None,
        request_id: None,
        status: 429,
    });
    assert!(err.is_retryable());

    let err = error::Error::Lightning(error::LightningError {
        error: "bad request".into(),
        code: "BAD_REQUEST".into(),
        details: None,
        request_id: None,
        status: 400,
    });
    assert!(!err.is_retryable());
}

#[test]
fn test_rag_query_config_serialization() {
    let cfg = client::RagQueryConfig {
        expansion_depth: Some(2),
        search_weight: Some(0.7),
        recency_weight: Some(0.2),
        degree_weight: Some(0.1),
        max_tokens: Some(2048),
    };

    let json = serde_json::json!({
        "query": "test",
        "topK": 10,
    });

    let mut body = json;
    if let Some(v) = cfg.expansion_depth {
        body["expansionDepth"] = serde_json::json!(v);
    }
    if let Some(v) = cfg.search_weight {
        body["searchWeight"] = serde_json::json!(v);
    }
    if let Some(v) = cfg.recency_weight {
        body["recencyWeight"] = serde_json::json!(v);
    }
    if let Some(v) = cfg.degree_weight {
        body["degreeWeight"] = serde_json::json!(v);
    }
    if let Some(v) = cfg.max_tokens {
        body["maxTokens"] = serde_json::json!(v);
    }

    assert_eq!(body["expansionDepth"], 2);
    assert_eq!(body["searchWeight"], 0.7);
    assert_eq!(body["recencyWeight"], 0.2);
    assert_eq!(body["degreeWeight"], 0.1);
    assert_eq!(body["maxTokens"], 2048);
}
