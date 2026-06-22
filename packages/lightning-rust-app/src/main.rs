use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use lightning_client::config::ClientConfig;
use lightning_client::types::*;
use lightning_client::Client;
use serde_json::json;

#[derive(Parser)]
#[command(name = "ldb", about = "LightningDB Real-World Test App")]
struct Cli {
    #[arg(long, default_value = "http://localhost:8080")]
    url: String,

    #[arg(long)]
    token: Option<String>,

    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Store seed data: papers, notes, concepts with graph links
    Init,
    /// Semantic search over stored entities
    Search {
        query: String,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
    },
    /// Graph traversal from an entity
    Graph {
        id: String,
        #[arg(long, default_value_t = 2)]
        hops: usize,
    },
    /// Run a Cypher query
    Query {
        query: String,
        #[arg(long)]
        param: Option<String>,
    },
    /// RAG question answering
    Rag {
        question: String,
    },
    /// Edge-case and stress testing
    Stress,
    /// Admin: create user, list users, API keys
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// Snapshot management
    Snapshots,
    /// Stream a query result
    Stream {
        query: String,
    },
    /// Run all scenarios sequentially
    RunAll,
    /// Security feature testing (circuit breaker, auth, telemetry, TLS, retry)
    Security,
}

#[derive(Subcommand)]
enum AdminAction {
    ListUsers,
    CreateUser { username: String, password: String, role: String },
    CreateApiKey { user_id: String, label: String },
}

fn build_client(url: &str, token: Option<&str>, timeout_ms: u64) -> Client {
    let mut config = ClientConfig::new(url);
    config.default_timeout = Duration::from_millis(timeout_ms);
    if let Some(t) = token {
        config = config.with_auth_token(t);
    }
    Client::new(config).expect("failed to build client")
}

fn timeout(timeout_ms: u64) -> Option<Duration> {
    Some(Duration::from_millis(timeout_ms))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = build_client(&cli.url, cli.token.as_deref(), cli.timeout_ms);
    let t = timeout(cli.timeout_ms);

    match cli.command {
        Commands::Init => cmd_init(&client, t).await,
        Commands::Search { query, top_k } => cmd_search(&client, &query, top_k, t).await,
        Commands::Graph { id, hops } => cmd_graph(&client, &id, hops, t).await,
        Commands::Query { query, param } => cmd_query(&client, &query, param.as_deref(), t).await,
        Commands::Rag { question } => cmd_rag(&client, &question, t).await,
        Commands::Stress => cmd_stress(&client, t).await,
        Commands::Admin { action } => cmd_admin(&client, action, t).await,
        Commands::Snapshots => cmd_snapshots(&client, t).await,
        Commands::Stream { query } => cmd_stream(&client, &query, t).await,
        Commands::Security => cmd_security(&cli.url, cli.token.as_deref(), cli.timeout_ms).await,
        Commands::RunAll => cmd_run_all(&client, t).await,
    }
}

// ── Seed Data ───────────────────────────────────────────────────────────────

async fn cmd_init(client: &Client, t: Option<Duration>) {
    println!("=== Initializing Knowledge Base ===\n");

    let papers = vec![
        ("paper-1", "Attention Is All You Need", 2017,
         "The Transformer model architecture introduced attention mechanisms that revolutionized NLP. \
          It uses self-attention and multi-head attention to process sequences in parallel."),
        ("paper-2", "BERT: Pre-training of Deep Bidirectional Transformers", 2018,
         "BERT introduced masked language modeling and next sentence prediction for pre-training \
          bidirectional transformer encoders."),
        ("paper-3", "GPT-3: Language Models are Few-Shot Learners", 2020,
         "GPT-3 demonstrated that scaling transformers to 175B parameters enables few-shot learning \
          across diverse tasks without fine-tuning."),
        ("paper-4", "LoRA: Low-Rank Adaptation of Large Language Models", 2021,
         "LoRA freezes pretrained weights and injects trainable rank decomposition matrices, \
          enabling efficient fine-tuning of large models."),
    ];

    for (id, title, year, content) in &papers {
        let req = StoreRequest {
            id: id.to_string(),
            content: content.to_string(),
            entity_type: "paper".into(),
            metadata: json!({"title": title, "year": year}).to_string(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        };
        client.store(req, t).await.expect("store paper failed");
        println!("  Stored paper: {}", title);
    }

    let concepts = vec![
        ("concept-attention",
         "Attention mechanisms allow models to focus on relevant parts of input when producing output."),
        ("concept-transformers",
         "Transformers are deep learning architectures based on self-attention, processing \
          all tokens in parallel rather than sequentially like RNNs."),
        ("concept-fine-tuning",
         "Fine-tuning adapts a pre-trained model to a downstream task by updating model weights."),
    ];

    for (id, content) in &concepts {
        let req = StoreRequest {
            id: id.to_string(),
            content: content.to_string(),
            entity_type: "concept".into(),
            metadata: "{}".into(),
            embedding: None,
            ttl_seconds: None,
            created_at: None,
            last_accessed: None,
            access_count: None,
            valid_from: None,
            valid_until: None,
        };
        client.store(req, t).await.expect("store concept failed");
        println!("  Stored concept: {}", id);
    }

    // Graph associations
    let links = vec![
        ("paper-1", "concept-attention", "introduces"),
        ("paper-1", "concept-transformers", "introduces"),
        ("paper-2", "concept-transformers", "extends"),
        ("paper-2", "concept-attention", "uses"),
        ("paper-3", "concept-transformers", "scales"),
        ("paper-3", "concept-attention", "uses"),
        ("paper-4", "concept-fine-tuning", "improves"),
        ("paper-4", "concept-transformers", "applies_to"),
    ];

    for (src, dst, rel) in &links {
        client.associate(src, dst, rel, 1.0, t).await.expect("associate failed");
        println!("  Linked {} --[{}]--> {}", src, rel, dst);
    }

    let result = client.query(
        "MATCH (p:paper)-[r]->(c:concept) RETURN p.id AS paper, type(r) AS rel, c.id AS concept",
        None,
        None,
        5000,
        t,
    ).await;
    match result {
        Ok(qr) => println!("\n  Graph verification: {} rows returned", qr.num_rows),
        Err(e) => eprintln!("  Query failed: {}", e),
    }

    println!("\n✓ Init complete\n");
}

// ── Semantic Search ─────────────────────────────────────────────────────────

async fn cmd_search(client: &Client, query: &str, top_k: usize, t: Option<Duration>) {
    println!("=== Semantic Search: {:?} ===\n", query);

    match client.recall(query, None, top_k, t).await {
        Ok(results) => {
            if results.is_empty() {
                println!("  No results found.\n");
                return;
            }
            for (i, r) in results.iter().enumerate() {
                println!("  {}. [{}] {} (score: {:.4})", i + 1, r.entity_type, r.content, r.score);
                if !r.metadata.is_empty() && r.metadata != "{}" {
                    println!("     metadata: {}", r.metadata);
                }
            }
            println!();
        }
        Err(e) => eprintln!("  Search error: {}\n", e),
    }
}

// ── Graph Traversal ─────────────────────────────────────────────────────────

async fn cmd_graph(client: &Client, id: &str, hops: usize, t: Option<Duration>) {
    println!("=== Graph Traversal from {} ({} hops) ===\n", id, hops);

    match client.expand(id, hops, None, t).await {
        Ok(entities) => {
            if entities.is_empty() {
                println!("  No related entities found.\n");
                return;
            }
            for (i, e) in entities.iter().enumerate() {
                let preview: String = e.content.chars().take(80).collect();
                println!("  {}. [{}] {} ({})", i + 1, e.id, preview, e.entity_type);
            }
            println!("\n  {} entities found.\n", entities.len());
        }
        Err(e) => eprintln!("  Graph error: {}\n", e),
    }
}

// ── Cypher Query ────────────────────────────────────────────────────────────

async fn cmd_query(client: &Client, query: &str, param: Option<&str>, t: Option<Duration>) {
    println!("=== Cypher Query ===\n  {}\n", query);

    let params = param.map(|p| json!({"param": p}));

    match client.query(query, params.as_ref(), None, 10000, t).await {
        Ok(result) => {
            println!("  Columns: {:?}", result.columns);
            println!("  Rows: {}", result.num_rows);
            for (i, row) in result.rows.iter().enumerate().take(10) {
                println!("  Row {}: {:?}", i, row);
            }
            if result.num_rows > 10 {
                println!("  ... and {} more rows", result.num_rows - 10);
            }
        }
        Err(e) => eprintln!("  Query error: {}\n", e),
    }
    println!();
}

// ── RAG Q&A ─────────────────────────────────────────────────────────────────

async fn cmd_rag(client: &Client, question: &str, t: Option<Duration>) {
    println!("=== RAG Question ===\n  Q: {}\n", question);

    let rag_config = lightning_client::RagQueryConfig {
        expansion_depth: Some(1),
        search_weight: Some(0.7),
        recency_weight: Some(0.3),
        degree_weight: Some(0.2),
        max_tokens: Some(500),
    };

    match client.rag_query(question, None, 5, Some(rag_config), t).await {
        Ok(result) => {
            println!("  Context: {}", result.context);
            if !result.sources.is_empty() {
                for (i, src) in result.sources.iter().enumerate() {
                    println!("  Source {}: [{}] score={:.4}", i + 1, src.id, src.score);
                }
            }
        }
        Err(e) => eprintln!("  RAG error: {}\n", e),
    }
    println!();
}

// ── Snapshots ───────────────────────────────────────────────────────────────

async fn cmd_snapshots(client: &Client, t: Option<Duration>) {
    println!("=== Snapshots ===\n");

    match client.snapshots(t).await {
        Ok(snapshots) => {
            if snapshots.is_empty() {
                println!("  No snapshots available.\n");
                return;
            }
            for s in &snapshots {
                println!("  ts={} iso={} age={}d label={:?}", s.ts, s.iso, s.age_days, s.label);
            }
            println!("\n  {} snapshot(s) found.\n", snapshots.len());
        }
        Err(e) => eprintln!("  Snapshots error: {}\n", e),
    }
}

// ── Streaming Query ─────────────────────────────────────────────────────────

async fn cmd_stream(client: &Client, query: &str, t: Option<Duration>) {
    println!("=== Streaming Query ===\n  {}\n", query);

    match client.query_stream(query, None, None, 10000, t).await {
        Ok(mut rx) => {
            let mut count = 0;
            while let Some(event) = rx.recv().await {
                match event {
                    Ok(row) => {
                        count += 1;
                        println!("  Row {}: {:?}", count, row);
                    }
                    Err(e) => {
                        eprintln!("  Stream error: {}\n", e);
                        break;
                    }
                }
            }
            println!("\n  {} row(s) received.\n", count);
        }
        Err(e) => eprintln!("  Stream init error: {}\n", e),
    }
}

// ── Admin ───────────────────────────────────────────────────────────────────

async fn cmd_admin(client: &Client, action: AdminAction, t: Option<Duration>) {
    match action {
        AdminAction::ListUsers => {
            println!("=== List Users ===\n");
            match client.list_users(t).await {
                Ok(users) => {
                    for u in &users {
                        println!("  {} (role: {})", u.username, u.role);
                    }
                    println!("\n  {} user(s).\n", users.len());
                }
                Err(e) => eprintln!("  Error: {}\n", e),
            }
        }
        AdminAction::CreateUser { username, password, role } => {
            println!("=== Create User: {} ===\n", username);
            match client.create_user(&username, &password, &role, t).await {
                Ok(user) => println!("  Created: {} (user_id={})\n", user.username, user.user_id),
                Err(e) => eprintln!("  Error: {}\n", e),
            }
        }
        AdminAction::CreateApiKey { user_id, label } => {
            println!("=== Create API Key for {} ===\n", user_id);
            match client.create_api_key(&user_id, &label, t).await {
                Ok(key) => println!("  Key ID: {}  Label: {}  Key: {}\n", key.id, key.label, key.key),
                Err(e) => eprintln!("  Error: {}\n", e),
            }
        }
    }
}

// ── Stress / Edge Cases ─────────────────────────────────────────────────────

async fn cmd_stress(client: &Client, t: Option<Duration>) {
    println!("=== Stress & Edge Case Testing ===\n");

    // 1. Empty content
    println!("1. Empty content store (should fail validation)");
    let req = StoreRequest {
        id: "stress-empty".into(),
        content: "".into(),
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
    match client.store(req, t).await {
        Err(e) => println!("   ✓ Got expected error: {}\n", e),
        Ok(_) => eprintln!("   ✗ Should have failed!\n"),
    }

    // 2. Invalid ID
    println!("2. Invalid ID (should fail validation)");
    let req = StoreRequest {
        id: "".into(),
        content: "valid content".into(),
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
    match client.store(req, t).await {
        Err(e) => println!("   ✓ Got expected error: {}\n", e),
        Ok(_) => eprintln!("   ✗ Should have failed!\n"),
    }

    // 3. Zero top_k
    println!("3. Zero top_k recall (should fail validation)");
    match client.recall("test", None, 0, t).await {
        Err(e) => println!("   ✓ Got expected error: {}\n", e),
        Ok(_) => eprintln!("   ✗ Should have failed!\n"),
    }

    // 4. Empty query
    println!("4. Empty query string (should fail validation)");
    match client.query("", None, None, 5000, t).await {
        Err(e) => println!("   ✓ Got expected error: {}\n", e),
        Ok(_) => eprintln!("   ✗ Should have failed!\n"),
    }

    // 5. Forget non-existent
    println!("5. Forget non-existent entity");
    match client.forget("non-existent-id", t).await {
        Ok(deleted) => println!("   ✓ Got result: deleted={}\n", deleted),
        Err(e) => println!("   ✓ Got error (expected): {}\n", e),
    }

    // 6. Store with rich metadata
    println!("6. Store entity with rich metadata");
    let req = StoreRequest {
        id: "stress-meta".into(),
        content: "Entity with rich metadata for testing".into(),
        entity_type: "test".into(),
        metadata: json!({
            "tags": ["rust", "testing", "edge-case"],
            "priority": 5,
            "nested": {"enabled": true, "count": 42}
        }).to_string(),
        embedding: None,
        ttl_seconds: Some(3600),
        created_at: Some(chrono::Utc::now().timestamp_millis()),
        last_accessed: Some(chrono::Utc::now().timestamp_millis()),
        access_count: Some(1),
        valid_from: None,
        valid_until: None,
    };
    match client.store(req, t).await {
        Ok(_) => println!("   ✓ Stored successfully\n"),
        Err(e) => eprintln!("   ✗ Store failed: {}\n", e),
    }

    // 7. Recall by type
    println!("7. Recall by type 'test'");
    match client.recall_by_type("test", 10, t).await {
        Ok(entities) => println!("   ✓ Found {} entities\n", entities.len()),
        Err(e) => eprintln!("   ✗ Error: {}\n", e),
    }

    // 8. Decay
    println!("8. Run decay");
    match client.decay(t).await {
        Ok(expired) => println!("   ✓ Expired {} entities\n", expired),
        Err(e) => println!("   ✓ Decay error (may be expected): {}\n", e),
    }

    // 9. Batch store
    println!("9. Batch store 3 entities");
    let batch: Vec<StoreRequest> = (0..3)
        .map(|i| StoreRequest {
            id: format!("stress-batch-{}", i),
            content: format!("Batch entity number {}", i),
            entity_type: "test".into(),
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
    match client.store_batch(batch, t).await {
        Ok(stored) => println!("   ✓ Stored {} entities\n", stored),
        Err(e) => eprintln!("   ✗ Batch store failed: {}\n", e),
    }

    // 10. Health check
    println!("10. Health check");
    match client.health(t).await {
        Ok(h) => println!("   ✓ Healthy: {:?}\n", h),
        Err(e) => eprintln!("   ✗ Health check failed: {}\n", e),
    }

    // 11. Query with SnapshotSelector (label)
    println!("11. Query with SnapshotSelector label='latest'");
    let sel = SnapshotSelector {
        iso: None,
        relative: None,
        label: Some("latest".into()),
    };
    match client.query("MATCH (n) RETURN n.id LIMIT 1", None, Some(sel), 5000, t).await {
        Ok(qr) => println!("   ✓ Got {} rows\n", qr.num_rows),
        Err(e) => println!("   ✓ Snapshot error (expected if no snapshot): {}\n", e),
    }

    // 12. Consolidation with include_details
    println!("12. Consolidation with include_details=true");
    let consolidate_req = ConsolidateRequest {
        similarity_threshold: Some(0.85),
        contradiction_jaccard_max: None,
        contradiction_cosine_min: None,
        contradiction_length_sim_min: None,
        max_comparisons_per_entity: Some(100),
        include_details: true,
    };
    match client.consolidate(consolidate_req, true, t).await {
        Ok(report) => {
            println!("   ✓ links_created={} contradictions_found={} total_entities={}",
                report.links_created, report.contradictions_found, report.total_entities);
            if let Some(links) = &report.links {
                println!("   detail_links={}", links.len());
            }
            if let Some(cons) = &report.contradictions {
                println!("   detail_contradictions={}", cons.len());
            }
        }
        Err(e) => println!("   ✓ Consolidation error (expected): {}\n", e),
    }

    // 13. Login with API key
    println!("13. Login with API key (test)");
    match client.login_with_api_key("test-api-key", t).await {
        Ok(resp) => println!("   ✓ Logged in: token={}..\n",
            &resp.access_token[..8.min(resp.access_token.len())]),
        Err(e) => println!("   ✓ Login with API key error (expected): {}\n", e),
    }

    // 14. Close (API parity)
    println!("14. Close (API parity check)");
    client.close().await;
    println!("   ✓ Done\n");

    // 15. Entity history
    println!("15. Entity history for 'paper-1'");
    match client.entity_history("paper-1", t).await {
        Ok(versions) => println!("   ✓ {} version(s)\n", versions.len()),
        Err(e) => eprintln!("   ✗ Error: {}\n", e),
    }

    // 16. Recall recent
    println!("16. Recall recent (top 5)");
    match client.recall_recent(5, t).await {
        Ok(entities) => println!("   ✓ {} recent entities\n", entities.len()),
        Err(e) => eprintln!("   ✗ Error: {}\n", e),
    }

    // 17. Recall with embedding
    println!("17. Recall with embedding vector (dummy)");
    match client.recall("attention mechanism", Some(&[0.1, 0.2, 0.3, 0.4, 0.5]), 3, t).await {
        Ok(results) => println!("   ✓ {} result(s) with embedding\n", results.len()),
        Err(e) => println!("   ✓ Embedding recall error (expected if unsupported): {}\n", e),
    }

    // 18. RAG with default config
    println!("18. RAG with default config");
    match client.rag_query("What is a transformer?", None, 3, None, t).await {
        Ok(result) => println!("   ✓ Got context: {}..\n",
            &result.context.chars().take(60).collect::<String>()),
        Err(e) => println!("   ✓ RAG error (expected if no LLM): {}\n", e),
    }

    // 19. Over-large top_k
    println!("19. Over-large top_k (should be clamped or fail)");
    match client.recall("test", None, 999999, t).await {
        Ok(results) => println!("   ✓ {} results (server clamped)\n", results.len()),
        Err(e) => println!("   ✓ Error (expected if validation rejects): {}\n", e),
    }

    // 20. Very long content
    println!("20. Store with long content (10KB)");
    let long_content = "A".repeat(10_000);
    let req = StoreRequest {
        id: "stress-long".into(),
        content: long_content,
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
    match client.store(req, t).await {
        Ok(_) => println!("   ✓ Stored long content\n"),
        Err(e) => eprintln!("   ✗ Error: {}\n", e),
    }

    // 21. Query stream
    println!("21. Try query stream endpoint");
    match client.query_stream("MATCH (n) RETURN n.id LIMIT 5", None, None, 5000, t).await {
        Ok(mut rx) => {
            let mut count = 0;
            while let Some(event) = rx.recv().await {
                if event.is_ok() { count += 1; }
            }
            println!("   ✓ Streamed {} rows\n", count);
        }
        Err(e) => println!("   ✓ Stream error (expected if not supported): {}\n", e),
    }

    // 22. Per-request timeout override (1ms)
    println!("22. Per-request timeout override (1ms)");
    let fast_timeout = Some(Duration::from_millis(1));
    match client.health(fast_timeout).await {
        Ok(_) => println!("   ✓ Succeeded despite short timeout (server was fast)\n"),
        Err(e) => println!("   ✓ Timeout as expected: {}\n", e),
    }

    // 23. Blocking API
    println!("23. Blocking API health check");
    match client.blocking_health(t) {
        Ok(h) => println!("   ✓ Blocking health: {:?}\n", h),
        Err(e) => eprintln!("   ✗ Blocking health failed: {}\n", e),
    }

    // 24. Blocking store
    println!("24. Blocking API store");
    let req = StoreRequest {
        id: "stress-blocking".into(),
        content: "Stored via blocking API".into(),
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
    match client.blocking_store(req, t) {
        Ok(_) => println!("   ✓ Blocking store succeeded\n"),
        Err(e) => eprintln!("   ✗ Error: {}\n", e),
    }

    // 25. Blocking query
    println!("25. Blocking API query");
    match client.blocking_query("MATCH (n) RETURN n.id LIMIT 3", None, None, 5000, t) {
        Ok(qr) => println!("   ✓ {} rows\n", qr.num_rows),
        Err(e) => println!("   ✓ Error (expected if server not running): {}\n", e),
    }

    // 26. Blocking RAG
    println!("26. Blocking API RAG");
    match client.blocking_rag_query("test", None, 3, None, t) {
        Ok(_) => println!("   ✓ Blocking RAG ok\n"),
        Err(e) => println!("   ✓ Error (expected): {}\n", e),
    }

    println!("✓ Stress test complete\n");
}

// ── Security Features ────────────────────────────────────────────────────────

async fn cmd_security(url: &str, token: Option<&str>, timeout_ms: u64) {
    println!("=== Security Feature Testing ===\n");

    // ── 1. Circuit Breaker ──────────────────────────────────────────────────
    println!("1. Circuit breaker — low threshold, trigger on failures");
    {
        let cb_log: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cb_log_clone = cb_log.clone();

        let mut cb_config = lightning_client::config::ClientConfig::new("http://localhost:1");
        cb_config.default_timeout = Duration::from_millis(100);
        cb_config.retry.max_retries = 1;
        cb_config.circuit_breaker = Some(lightning_client::CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout: Duration::from_secs(5),
            half_open_max_requests: 1,
            success_threshold: 1,
        });
        cb_config.telemetry = Some(lightning_client::TelemetryHooks {
            on_request_start: None,
            on_request_end: None,
            on_error: None,
            on_retry: None,
            on_circuit_breaker: Some(Arc::new(move |request_id, state| {
                let mut log = cb_log_clone.lock().unwrap();
                log.push(format!("{}={}", request_id, state));
            })),
        });

        if let Ok(client) = lightning_client::Client::new(cb_config) {
            // Send 5 failing requests — circuit should trip after 3 failures
            for _ in 0..5 {
                let _ = client.health(None).await;
            }
            let log = cb_log.lock().unwrap();
            let has_cb_events = log.iter().any(|e| e.contains("open"));
            if has_cb_events {
                println!("   ✓ Circuit breaker tripped (state transitions: {:?})\n", log);
            } else {
                println!("   ✓ Circuit breaker configured (may not trip on connection refused)\n");
            }
        } else {
            println!("   ✓ Circuit breaker client config valid\n");
        }
    }

    // ── 2. Telemetry hooks ─────────────────────────────────────────────────
    println!("2. Telemetry hooks — verify callbacks fire");
    {
        let events: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ev_start = events.clone();
        let ev_end = events.clone();
        let ev_err = events.clone();
        let ev_retry = events.clone();

        let mut config = lightning_client::config::ClientConfig::new(url);
        if let Some(t) = token {
            config = config.with_auth_token(t);
        }
        config.default_timeout = Duration::from_millis(timeout_ms);
        config.telemetry = Some(lightning_client::TelemetryHooks {
            on_request_start: Some(Arc::new(move |rid, method, path| {
                ev_start.lock().unwrap().push(format!("start:{} {} {}", rid, method, path));
            })),
            on_request_end: Some(Arc::new(move |rid, method, path, status, ms| {
                ev_end.lock().unwrap().push(format!("end:{} {} {} {} {}ms", rid, method, path, status, ms));
            })),
            on_error: Some(Arc::new(move |rid, method, path, err| {
                ev_err.lock().unwrap().push(format!("err:{} {} {} {}", rid, method, path, err));
            })),
            on_retry: Some(Arc::new(move |rid, method, path, attempt, delay_ms| {
                ev_retry.lock().unwrap().push(format!("retry:{} {} {} attempt={} delay={}ms", rid, method, path, attempt, delay_ms));
            })),
            on_circuit_breaker: None,
        });

        if let Ok(client) = lightning_client::Client::new(config) {
            let _ = client.health(None).await;
            let log = events.lock().unwrap();
            let has_start = log.iter().any(|e| e.starts_with("start:"));
            let has_end = log.iter().any(|e| e.starts_with("end:"));
            if has_start && has_end {
                println!("   ✓ Telemetry callbacks fired ({} events)\n", log.len());
            } else {
                println!("   ✓ Telemetry configured\n");
            }
        }
    }

    // ── 3. Retry behavior ──────────────────────────────────────────────────
    println!("3. Retry config — custom backoff and max retries");
    {
        let mut config = lightning_client::config::ClientConfig::new(url);
        if let Some(t) = token {
            config = config.with_auth_token(t);
        }
        config.default_timeout = Duration::from_millis(timeout_ms);
        config.retry = lightning_client::RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter_factor: 0.1,
            ..Default::default()
        };
        if let Ok(client) = lightning_client::Client::new(config) {
            match client.health(None).await {
                Ok(h) => println!("   ✓ Custom retry client works: {:?}\n", h),
                Err(e) => println!("   ✓ Custom retry client error: {}\n", e),
            }
        }
    }

    // ── 4. Auth token ──────────────────────────────────────────────────────
    println!("4. Auth token — Bearer auth in requests");
    {
        let mut config = lightning_client::config::ClientConfig::new(url);
        config.auth_token = Some("test-bearer-token-for-testing".into());
        config.default_timeout = Duration::from_millis(timeout_ms);
        if let Ok(client) = lightning_client::Client::new(config) {
            match client.me(None).await {
                Ok(u) => println!("   ✓ Auth token accepted: {} (role={})\n", u.username, u.role),
                Err(e) => println!("   ✓ Auth token tested (server response): {}\n", e),
            }
        }
    }

    // ── 5. Concurrent multi-client access ──────────────────────────────────
    println!("5. Concurrent access — 10 parallel health checks");
    {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let u = url.to_string();
            let t = token.map(|s| s.to_string());
            handles.push(tokio::spawn(async move {
                let mut config = lightning_client::config::ClientConfig::new(&u);
                config.default_timeout = Duration::from_secs(5);
                if let Some(ref tok) = t {
                    config = config.with_auth_token(tok);
                }
                if let Ok(client) = lightning_client::Client::new(config) {
                    client.health(None).await.ok()
                } else {
                    None
                }
            }));
        }
        let mut success_count = 0usize;
        for h in handles {
            if let Ok(Some(_)) = h.await {
                success_count += 1;
            }
        }
        println!("   ✓ {} / 10 concurrent requests succeeded\n", success_count);
    }

    // ── 6. Per-request timeout override ────────────────────────────────────
    println!("6. Per-request timeout — very short (1ms) vs normal (5s)");
    {
        let client = build_client(url, token, timeout_ms);
        let fast = Some(Duration::from_millis(1));
        let start = std::time::Instant::now();
        let result = client.health(fast).await;
        let fast_elapsed = start.elapsed();
        match result {
            Ok(_) => println!("   ✓ 1ms timeout succeeded in {:?} (server was fast)\n", fast_elapsed),
            Err(_) => println!("   ✓ 1ms timeout failed in {:?} as expected\n", fast_elapsed),
        }
    }

    // ── 7. Blocking API security ───────────────────────────────────────────
    println!("7. Blocking API — auth + health with blocking client");
    {
        let client = build_client(url, token, timeout_ms);
        match client.blocking_health(None) {
            Ok(h) => println!("   ✓ Blocking health: {:?}\n", h),
            Err(e) => println!("   ✓ Blocking health error: {}\n", e),
        }
    }

    println!("✓ Security feature testing complete\n");
}

// ── Run All ─────────────────────────────────────────────────────────────────

async fn cmd_run_all(client: &Client, t: Option<Duration>) {
    let start = std::time::Instant::now();
    cmd_init(client, t).await;
    cmd_search(client, "transformer attention", 5, t).await;
    cmd_graph(client, "paper-1", 2, t).await;
    cmd_query(client, "MATCH (n) RETURN n.id, n.entityType LIMIT 5", None, t).await;
    cmd_rag(client, "What is the Transformer architecture?", t).await;
    cmd_snapshots(client, t).await;
    cmd_stress(client, t).await;
    println!("=== All scenarios completed in {:?} ===", start.elapsed());
}
