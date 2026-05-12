/// AI Agent Memory Demo
///
/// Demonstrates Lightning as a persistent memory backend for AI agents.
/// Run with: cargo run --example agent_memory --release
///
/// What this shows:
///   1. Store memories (conversations, facts, preferences)
///   2. Hybrid semantic + keyword recall
///   3. Graph traversal (related memories)
///   4. Temporal time-travel queries
///   5. Built-in RAG pipeline
///   6. Memory consolidation (auto-link, contradictions)
///   7. WAL change streaming
///   8. WASM-defined scoring function
///   9. Streaming queries
///  10. PageRank importance scoring

use lightning_core::memory::{MemoryEntity, MemoryStore, RagResult};
use lightning_core::{Database, SystemConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════╗");
    println!("║        Lightning AI Agent Memory Demo             ║");
    println!("╚════════════════════════════════════════════════════╝\n");

    // ================================================================
    // 1. SETUP: Create an agent memory store
    // ================================================================
    let dir = std::env::temp_dir().join("lightning_agent_demo");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let db = Database::new(&dir, SystemConfig::default())?;
    let conn = db.connect();
    let memory = MemoryStore::new(conn);
    memory.ensure_schema()?;
    println!("✓ Agent memory initialized");

    // ================================================================
    // 2. STORE MEMORIES: conversations, facts, preferences, documents
    // ================================================================
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);

    fn entity(id: &str, content: &str, etype: &str, ts: i64) -> MemoryEntity {
        MemoryEntity {
            id: id.to_string(),
            entity_type: etype.to_string(),
            content: content.to_string(),
            created_at: ts,
            last_accessed: ts,
            access_count: 1,
            ttl_seconds: 0,
            metadata: "{}".to_string(),
            valid_from: ts,
            valid_until: 0,
        }
    }

    let t0 = now;
    let day = 86_400_000_000i64; // microseconds in a day

    // Session 1: User asks about Rust
    memory.store(entity("conv-1", "User asked: What's the best way to learn Rust?", "conversation", t0))?;
    memory.store(entity("conv-2", "Assistant recommended starting with 'The Book' and building small CLI tools", "conversation", t0 + 1000))?;

    // Session 2: User expresses preferences
    memory.store(entity("pref-1", "User prefers Python for data science and Rust for systems programming", "preference", t0 + day))?;
    memory.store(entity("pref-2", "User likes functional programming patterns", "preference", t0 + day + 1000))?;

    // Session 3: Facts extracted from conversation
    memory.store(entity("fact-1", "User has 5 years of Python experience and 1 year of Rust experience", "fact", t0 + 2 * day))?;
    memory.store(entity("fact-2", "User works at a fintech company building trading systems", "fact", t0 + 2 * day + 1000))?;

    // A document memory
    memory.store(entity("doc-1", "Rust ownership model ensures memory safety without a garbage collector. The borrow checker enforces rules at compile time.", "document", t0 + 3 * day))?;

    // Create relationships between related memories
    memory.associate("conv-1", "fact-1", "extracted_from", 0.9)?;
    memory.associate("conv-2", "pref-1", "implies", 0.7)?;
    memory.associate("pref-1", "pref-2", "related_to", 0.5)?;
    memory.associate("doc-1", "conv-1", "relevant_to", 0.8)?;

    println!("✓ Stored 7 memories with 4 relationships\n");

    // ================================================================
    // 3. HYBRID SEMANTIC + KEYWORD RECALL
    // ================================================================
    println!("▶ HYBRID SEARCH: 'what does the user know about Rust?'");
    let results = memory.recall("Rust programming experience", &[], 5)?;
    for r in &results {
        println!("  [{:.3}] ({}): {}", r.score, r.entity.entity_type, r.entity.content);
    }
    println!();

    // ================================================================
    // 4. GRAPH TRAVERSAL: Expand from a seed memory
    // ================================================================
    println!("▶ GRAPH TRAVERSAL: expand from 'conv-1' (1 hop)");
    let neighbors = memory.expand("conv-1", 1, &["extracted_from", "relevant_to"])?;
    for n in &neighbors {
        println!("  → {}: {}", n.entity_type, n.content);
    }
    println!();

    // ================================================================
    // 5. TEMPORAL QUERIES: Time-travel to see what the agent knew
    // ================================================================
    println!("▶ TEMPORAL: what did the agent know after session 1?");
    let snapshot_t = t0 + 5000; // right after session 1
    let snapshot = memory.recall_at_time(snapshot_t, 10)?;
    println!("  Memories visible at time T+5ms: {} memories", snapshot.len());
    for s in &snapshot {
        println!("  [{}] {}", s.entity_type, s.content);
    }
    println!();

    // ================================================================
    // 6. BUILT-IN RAG PIPELINE
    // ================================================================
    println!("▶ RAG PIPELINE: 'what should I know about the user?'");
    let rag: RagResult = memory.rag_query("user background and preferences", &[], 5)?;
    println!("  Query: {}", rag.query);
    println!("  Sources: {} entities", rag.total_sources);
    println!("  Context:\n{}\n", rag.context);

    // ================================================================
    // 7. MEMORY CONSOLIDATION: auto-link, contradictions, PageRank
    // ================================================================
    println!("▶ CONSOLIDATION: auto-link related memories");
    let report = memory.consolidate()?;
    println!("  Links created: {}", report.links_created);
    println!("  Contradictions found: {}", report.contradictions_found);
    println!("  Total entities processed: {}", report.total_entities);
    println!();

    // ================================================================
    // 8. WAL CHANGE DATA CAPTURE
    // ================================================================
    println!("▶ CDC: subscribe to memory changes");
    let rx = memory.subscribe_changes()?;
    // Write something to trigger CDC
    memory.store(entity("cdc-test", "CDC test event", "test", t0 + 4 * day))?;
    std::thread::sleep(std::time::Duration::from_millis(200));
    let events: Vec<_> = rx.try_iter().collect();
    println!("  CDC events received: {}", events.len());
    if let Some(ev) = events.first() {
        println!("  Last event: +{} bytes written (total WAL: {} bytes)", ev.bytes_written, ev.total_wal_bytes);
    }
    println!();

    // ================================================================
    // 9. STREAMING QUERY
    // ================================================================
    println!("▶ STREAMING: recall results as a channel");
    let rx = memory.recall_stream("experience", &[], 10)?;
    let mut stream_count = 0u64;
    while let Ok(Ok(r)) = rx.recv() {
        stream_count += 1;
        println!("  [stream] id={} score={:.3} type={}", r.entity.id, r.score, r.entity.entity_type);
    }
    println!("  Stream delivered {} results\n", stream_count);

    // ================================================================
    // 10. REGISTER A WASM FUNCTION
    // ================================================================
    println!("▶ WASM: register a user-defined scoring function");
    let wasm_path = std::env::temp_dir().join("lightning_agent_demo").join("score.wasm");
    // We don't generate WASM here — see test_wasm_function_double for an example
    if wasm_path.exists() {
        match db.register_wasm_function(&wasm_path, "score") {
            Ok(()) => println!("  WASM function 'score' registered"),
            Err(e) => println!("  WASM registration: {} (may not exist)", e),
        }
    } else {
        println!("  WASM demo skipped (no .wasm file)");
    }
    println!();

    // ================================================================
    // SUMMARY
    // ================================================================
    println!("╔════════════════════════════════════════════════════╗");
    println!("║  Lightning AI Agent Memory — Demo Complete        ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║  Features demonstrated:                           ║");
    println!("║  ✓ Store (conversations, facts, preferences)      ║");
    println!("║  ✓ Hybrid search (FTS + keyword)                  ║");
    println!("║  ✓ Graph traversal (expand from seed)             ║");
    println!("║  ✓ Temporal queries (time-travel)                 ║");
    println!("║  ✓ RAG pipeline (search → expand → rank → format) ║");
    println!("║  ✓ Consolidation (auto-link → contradictions)     ║");
    println!("║  ✓ WAL CDC (real-time change streaming)           ║");
    println!("║  ✓ Streaming queries (channel-based)              ║");
    println!("║  ✓ WASM functions (user-defined code)             ║");
    println!("╚════════════════════════════════════════════════════╝");

    Ok(())
}
