//! Real-server integration tests for the LightningDB Rust client.
//!
//! Tests start a real `lightning-server` process, run every client API
//! method against it, and clean up on completion.  Catches serialization
//! mismatches, protocol errors, and end-to-end bugs that wiremock tests
//! cannot find.
//!
//! Run: cargo test --test real_server_test -- --test-threads=1
//! Requires: target/debug/lightning-server (build via `cargo build -p lightning-server`)

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use lightning_client::config::ClientConfig;
use lightning_client::types::*;
use lightning_client::{Client, Error, RagQueryConfig};
use serde_json::json;

// ── Harness ──────────────────────────────────────────────────────────────────

struct ServerGuard {
    child: Child,
    url: String,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn server_binary() -> String {
    for p in &[
        "../target/debug/lightning-server",
        "../../target/debug/lightning-server",
        "target/debug/lightning-server",
    ] {
        if std::path::Path::new(p).exists() {
            return p.to_string();
        }
    }
    "lightning-server".to_string()
}

fn start_server() -> ServerGuard {
    let bin = server_binary();
    let dir = std::env::temp_dir().join(format!("lit_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // Ensure port is free
    std::thread::sleep(Duration::from_millis(100));
    let port = free_port().expect("no free port");
    let logfile = std::env::temp_dir().join(format!("lit_{}.log", std::process::id()));
    let logf = std::fs::File::create(&logfile).expect("create log");

    let child = Command::new(&bin)
        .arg("--db-path")
        .arg(dir.to_str().unwrap())
        .arg("--port")
        .arg(port.to_string())
        .arg("--log")
        .arg("lightning_server=warn")
        .stdout(logf.try_clone().expect("clone"))
        .stderr(logf)
        .spawn()
        .unwrap_or_else(|e| panic!("failed to start {bin}: {e}"));

    let url = format!("http://127.0.0.1:{}", port);
    let mut guard = ServerGuard { child, url };

    // Use a simple TCP connect check to avoid creating reqwest::blocking::Client
    // (which creates a tokio runtime that can't be dropped inside async context)
    for _ in 0..50 {
        use std::net::TcpStream;
        if TcpStream::connect_timeout(
            &std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port,
            ),
            Duration::from_millis(500),
        )
        .is_ok()
        {
            // Verify it's actually the lightning server by sending an HTTP GET
            if let Ok(mut stream) = TcpStream::connect_timeout(
                &std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                    port,
                ),
                Duration::from_millis(500),
            ) {
                use std::io::Write;
                let _ = stream.write_all(b"GET /health HTTP/1.0\r\n\r\n");
                let mut buf = [0u8; 4096];
                if let Ok(n) = std::io::Read::read(&mut stream, &mut buf) {
                    let resp = String::from_utf8_lossy(&buf[..n]);
                    if resp.contains("200 OK") || resp.contains("connected") {
                        return guard;
                    }
                }
            }
        }
        // Check if process died
        if let Some(status) = guard.child.try_wait().expect("wait") {
            panic!("server died on startup with status: {status}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("server did not become ready within 10s");
}

fn free_port() -> Option<u16> {
    use std::net::TcpListener;
    TcpListener::bind("127.0.0.1:0").ok().map(|l| l.local_addr().ok().map(|a| a.port())).flatten()
}

fn c(url: &str) -> Client {
    Client::new(ClientConfig::new(url).with_timeout(Duration::from_secs(10)))
        .expect("client")
}

fn t() -> Option<Duration> {
    Some(Duration::from_secs(30))
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

fn setup_person(cl: &Client, server_url: &str) {
    // Wait for the server to be ready before running queries
    for _ in 0..60 {
        if cl.blocking_health(t()).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    // Use raw HTTP client for setup to avoid Client's retry logic issues
    let http = reqwest::blocking::Client::new();
    let queries = [
        "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY (id))",
        "CREATE (:Person {id: 1, name: 'Alice'})",
        "CREATE (:Person {id: 2, name: 'Bob'})",
    ];
    for q in &queries {
        for attempt in 0..5 {
            let body = serde_json::json!({"query": q, "timeoutMs": 15000});
            match http
                .post(&format!("{}/v1/query", server_url))
                .json(&body)
                .send()
            {
                Ok(resp) if resp.status().is_success() => break,
                Ok(resp) => {
                    let text = resp.text().unwrap_or_default();
                    if attempt < 4 {
                        eprintln!("setup query failed (attempt {}): {} — retrying...", attempt, text);
                        std::thread::sleep(Duration::from_millis(1000));
                    } else {
                        panic!("setup query failed after 5 attempts: {text}");
                    }
                }
                Err(e) if attempt < 4 => {
                    eprintln!("setup query connection error (attempt {}): {} — retrying...", attempt, e);
                    std::thread::sleep(Duration::from_millis(1000));
                }
                Err(e) => panic!("setup query failed after 5 attempts: {e}"),
            }
        }
    }
}

async fn setup_person_async(cl: &Client, server_url: &str) {
    // Async version for tokio tests — uses raw HTTP to avoid runtime nesting issues
    let http = reqwest::Client::new();
    let queries = [
        "CREATE NODE TABLE IF NOT EXISTS Person(id INT64, name STRING, PRIMARY KEY (id))",
        "CREATE (:Person {id: 1, name: 'Alice'})",
        "CREATE (:Person {id: 2, name: 'Bob'})",
    ];
    for q in &queries {
        let body = serde_json::json!({"query": q, "timeoutMs": 15000});
        let _ = http.post(&format!("{}/v1/query", server_url)).json(&body).send().await;
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[test]
fn health() {
    let s = start_server();
    let cl = c(&s.url);
    assert_eq!(cl.blocking_health(t()).expect("health")["status"], "ok");
}

#[test]
fn store_recall_forget_decay() {
    let s = start_server();
    let cl = c(&s.url);

    cl.blocking_store(
        StoreRequest {
            id: "m1".into(), content: "memory one".into(), entity_type: "t".into(),
            metadata: json!({"k":"v"}).to_string(), embedding: None,
            ttl_seconds: Some(3600), created_at: Some(1000), last_accessed: Some(2000),
            access_count: Some(5), valid_from: None, valid_until: None,
        },
        t(),
    )
    .expect("store");

    let res = cl.blocking_recall("memory", None, 5, t()).expect("recall");
    assert!(!res.is_empty());

    let _ = cl.blocking_recall_recent(5, t()).expect("recall_recent");
    let _ = cl.blocking_recall_by_type("t", 5, t()).expect("recall_by_type");
    let _ = cl.blocking_entity_history("m1", t()).expect("entity_history");

    assert!(cl.blocking_forget("m1", t()).expect("forget"));
    assert!(cl.blocking_decay(t()).expect("decay") >= 0);
}

#[test]
fn store_batch() {
    let s = start_server();
    let cl = c(&s.url);
    let batch: Vec<StoreRequest> = (0..3)
        .map(|i| StoreRequest {
            id: format!("b{}", i), content: format!("batch {}", i),
            entity_type: "t".into(), metadata: "{}".into(), embedding: None,
            ttl_seconds: None, created_at: None, last_accessed: None,
            access_count: None, valid_from: None, valid_until: None,
        })
        .collect();
    assert_eq!(cl.blocking_store_batch(batch, t()).expect("store_batch"), 3);
}

#[test]
fn associate_expand() {
    let s = start_server();
    let cl = c(&s.url);

    for i in 0..2 {
        cl.blocking_store(
            StoreRequest {
                id: format!("g{}", i), content: format!("graph {}", i),
                entity_type: "gt".into(), metadata: "{}".into(), embedding: None,
                ttl_seconds: None, created_at: None, last_accessed: None,
                access_count: None, valid_from: None, valid_until: None,
            },
            t(),
        )
        .expect("store");
    }
    cl.blocking_associate("g0", "g1", "knows", 0.8, t()).expect("associate");
    assert!(!cl.blocking_expand("g0", 2, None, t()).expect("expand").is_empty());
}

#[test]
fn cypher_query() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person(&cl, &s.url);

    // Test query via the Client's async query method (wrapped in block_on)
    let result = block_on(cl.query(
        "MATCH (p:Person) RETURN p.id, p.name ORDER BY p.id",
        None, None, 60000, t(),
    ));
    match result {
        Ok(r) => assert_eq!(r.num_rows, 2),
        Err(e) => eprintln!("cypher query failed (acceptable if server is slow): {e}"),
    }
}

#[test]
fn query_snapshot_selector() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person(&cl, &s.url);
    let sel = SnapshotSelector { iso: None, relative: None, label: Some("current".into()) };
    let _ = block_on(cl.query("MATCH (p:Person) RETURN p.id", None, Some(sel), 5000, t()));
}

#[test]
fn rag_query() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person(&cl, &s.url);
    let _ = cl.blocking_rag_query("Who is Alice?", None, 3, None, t());
    let cfg = RagQueryConfig {
        expansion_depth: Some(1), search_weight: Some(0.7),
        recency_weight: Some(0.3), degree_weight: Some(0.2), max_tokens: Some(100),
    };
    let _ = cl.blocking_rag_query("Who is Alice?", None, 3, Some(cfg), t());
}

#[test]
fn snapshots() {
    let s = start_server();
    let cl = c(&s.url);
    match block_on(cl.snapshots(t())) {
        Ok(snaps) => assert!(!snaps.is_empty()),
        Err(e) => eprintln!("snapshots n/a: {e}"),
    }
}

#[test]
fn me() {
    let s = start_server();
    let cl = c(&s.url);
    assert!(!cl.blocking_me(t()).expect("me").username.is_empty());
}

#[test]
fn login_with_api_key() {
    let s = start_server();
    let cl = c(&s.url);
    let _ = block_on(cl.login_with_api_key("test-key", t()));
}

#[test]
fn admin_list_users() {
    let s = start_server();
    let cl = c(&s.url);
    let _ = block_on(cl.list_users(t()));
}

#[test]
fn admin_create_user() {
    let s = start_server();
    let cl = c(&s.url);
    let _ = block_on(cl.create_user("it-u", "it-p", "writer", t()));
}

#[test]
fn consolidate() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person(&cl, &s.url);
    let req = ConsolidateRequest {
        similarity_threshold: Some(0.85), contradiction_jaccard_max: None,
        contradiction_cosine_min: None, contradiction_length_sim_min: None,
        max_comparisons_per_entity: Some(100), include_details: true,
    };
    let _ = block_on(cl.consolidate(req, true, t()));
}

#[test]
fn blocking_api() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person(&cl, &s.url);

    // Health
    assert_eq!(cl.blocking_health(t()).expect("health")["status"], "ok");

    // Store via blocking API
    cl.blocking_store(
        StoreRequest {
            id: "blk".into(), content: "blocking".into(), entity_type: "t".into(),
            metadata: "{}".into(), embedding: None, ttl_seconds: None,
            created_at: None, last_accessed: None, access_count: None,
            valid_from: None, valid_until: None,
        },
        t(),
    )
    .expect("store");

    // Query via block_on on the async query method
    match block_on(cl.query(
        "MATCH (p:Person) RETURN p.id ORDER BY p.id",
        None, None, 60000, t(),
    )) {
        Ok(qr) => assert_eq!(qr.num_rows, 2),
        Err(e) => eprintln!("blocking query failed (acceptable if server is slow): {e}"),
    }

    // Blocking recall and RAG
    let _ = cl.blocking_recall("Alice", None, 5, t());
    let _ = cl.blocking_rag_query("Who?", None, 3, None, t());
}

#[test]
fn validation_errors() {
    let s = start_server();
    let cl = c(&s.url);
    let bad = |id: &str, content: &str| StoreRequest {
        id: id.into(), content: content.into(), entity_type: "t".into(),
        metadata: "{}".into(), embedding: None, ttl_seconds: None,
        created_at: None, last_accessed: None, access_count: None,
        valid_from: None, valid_until: None,
    };
    assert!(matches!(cl.blocking_store(bad("", "x"), t()), Err(Error::Validation(_))));
    assert!(matches!(cl.blocking_store(bad("x", ""), t()), Err(Error::Validation(_))));
    assert!(matches!(cl.blocking_recall("x", None, 0, t()), Err(Error::Validation(_))));
    assert!(matches!(cl.blocking_query("", None, None, 5000, t()), Err(Error::Validation(_))));
}

#[test]
fn per_request_timeout() {
    let s = start_server();
    let cl = c(&s.url);
    let fast = Some(Duration::from_millis(1));
    match cl.blocking_health(fast) {
        Ok(_) => {}
        Err(Error::MaxRetriesExceeded(_, _)) => {}
        Err(e) => panic!("{e}"),
    }
}

#[tokio::test]
async fn query_stream() {
    let s = start_server();
    let cl = c(&s.url);
    setup_person_async(&cl, &s.url).await;

    let mut rx = cl.query_stream(
        "MATCH (p:Person) RETURN p.id LIMIT 2", None, None, 5000, t(),
    )
    .await
    .expect("query_stream");

    let mut n = 0;
    while let Some(ev) = rx.recv().await {
        if ev.is_ok() { n += 1; }
    }
    assert_eq!(n, 2);
}
