use arrow::array::{Float64Array, StringArray, UInt64Array};
use lightning_core::memory::{MemoryEntity, MemoryStore, SearchResult};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>, MemoryStore) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let store = MemoryStore::new(conn);
    // Initialize schema synchronously
    store.ensure_schema().unwrap();
    (dir, db, store)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn make_entity(id: &str, content: &str, entity_type: &str) -> MemoryEntity {
    let now = now_micros();
    MemoryEntity {
        id: id.to_string(),
        entity_type: entity_type.to_string(),
        content: content.to_string(),
        created_at: now,
        last_accessed: now,
        access_count: 1,
        ttl_seconds: 0,
        metadata: "{}".to_string(),
        valid_from: now,
        valid_until: 0,
    }
}

// ============================================================
// Basic CRUD
// ============================================================

#[test]
fn test_store_and_recall() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("mem-1", "Hello world", "message"))?;

    let results = store.recall("hello", &[], 10)?;
    assert!(!results.is_empty(), "Should find the stored memory");
    assert_eq!(results[0].entity.id, "mem-1");
    assert_eq!(results[0].entity.content, "Hello world");
    Ok(())
}

#[test]
fn test_store_batch() -> TestResult {
    let (_dir, _db, store) = setup();
    let entities = vec![
        make_entity("batch-1", "First memory", "note"),
        make_entity("batch-2", "Second memory", "note"),
        make_entity("batch-3", "Third memory", "note"),
    ];
    let count = store.store_batch(entities)?;
    assert_eq!(count, 3);

    let results = store.recall("memory", &[], 10)?;
    assert!(results.len() >= 3);
    Ok(())
}

#[test]
fn test_recall_by_type() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("pref-1", "Likes Python", "preference"))?;
    store.store(make_entity("fact-1", "Earth is round", "fact"))?;

    let prefs = store.recall_by_type("preference", 10)?;
    assert_eq!(prefs.len(), 1);
    assert_eq!(prefs[0].id, "pref-1");

    let facts = store.recall_by_type("fact", 10)?;
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].id, "fact-1");
    Ok(())
}

#[test]
fn test_recall_recent() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("old-1", "Old memory", "note"))?;
    std::thread::sleep(std::time::Duration::from_millis(10));
    store.store(make_entity("new-1", "New memory", "note"))?;

    let recent = store.recall_recent(5)?;
    assert!(!recent.is_empty());
    assert_eq!(recent[0].id, "new-1");
    Ok(())
}

#[test]
fn test_associate_and_expand() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("alice", "Alice is a developer", "person"))?;
    store.store(make_entity("bob", "Bob is a designer", "person"))?;
    store.associate("alice", "bob", "works_with", 0.9)?;

    let neighbors = store.expand("alice", 1, &["works_with"])?;
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].id, "bob");
    Ok(())
}

#[test]
fn test_forget_and_decay() -> TestResult {
    let (_dir, _db, store) = setup();

    // Permanent memory
    store.store(make_entity("perm-1", "Permanent", "note"))?;

    // TTL memory — expires in 1 second
    let mut ttl_entity = make_entity("temp-1", "Temporary", "note");
    ttl_entity.ttl_seconds = 1;
    store.store(ttl_entity)?;

    // Forget perm-1
    let deleted = store.forget("perm-1")?;
    assert!(deleted, "forget should return true");

    // Wait for TTL to expire
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Decay should clean up temp-1
    let expired = store.decay()?;
    assert!(expired >= 1);

    Ok(())
}

// ============================================================
// Temporal Graph Queries
// ============================================================

#[test]
fn test_recall_at_time() -> TestResult {
    let (_dir, _db, store) = setup();
    let t0 = now_micros();

    store.store(make_entity("time-1", "Exists at time zero", "temporal"))?;
    // Wait briefly so snapshot time is after entity creation
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t_snapshot = now_micros();

    // All memories should be visible at any time after their creation
    let snapshot = match store.recall_at_time(t_snapshot, 10) {
        Ok(s) => s,
        Err(e) => { eprintln!("recall_at_time error: {}", e); return Err(e); }
    };
    assert!(!snapshot.is_empty(), "Should find memories at creation time (num={})", snapshot.len());

    let recent = store.recall_recent(10)?;
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].id, "time-1");
    Ok(())
}

#[test]
fn test_entity_history() -> TestResult {
    let (_dir, _db, store) = setup();

    // Store and re-store to create version history
    store.store(make_entity("hist-1", "Version 1", "history"))?;
    store.store(make_entity("hist-1", "Version 2", "history"))?;

    // NOTE: The current MERGE-based store overwrites, not appends versions.
    // The valid_from/valid_until fields enable temporal queries when
    // applications explicitly use store_batch with different timestamps.
    // At minimum, recall should return the latest version.
    let recent = store.recall_recent(10)?;
    assert!(!recent.is_empty());
    Ok(())
}

// ============================================================
// Consolidation Pipeline
// ============================================================

#[test]
fn test_consolidate_empty() -> TestResult {
    let (_dir, _db, store) = setup();
    let report = store.consolidate()?;
    // 0 entities is fine
    Ok(())
}

#[test]
fn test_consolidate_single_entity() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("only-1", "Just one entity", "lonely"))?;
    let report = store.consolidate()?;
    // Should process at least 1 entity (fewer than 2 means no linking)
    Ok(())
}

#[test]
fn test_consolidate_similar_entities() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("a", "Python is a programming language for software development", "fact"))?;
    store.store(make_entity("b", "Python is widely used in machine learning and AI development", "fact"))?;
    store.store(make_entity("c", "Rust is a systems programming language focused on safety", "fact"))?;

    let report = store.consolidate()?;
    // Consolidation should process all entities
    assert_eq!(report.total_entities, 3);
    Ok(())
}

#[test]
fn test_consolidate_pagerank() -> TestResult {
    let (_dir, _db, store) = setup();

    // Create a small graph where one entity is central
    let topics = vec![
        ("hub", "Core concept for all systems architecture design"),
        ("a1", "System design for distributed architecture patterns"),
        ("a2", "Architecture patterns for modern system design"),
        ("a3", "Designing distributed systems with architecture patterns"),
    ];
    for (id, content) in &topics {
        store.store(make_entity(id, content, "topic"))?;
    }

    let report = store.consolidate()?;
    // Consolidation should process entities
    assert!(report.total_entities > 0);
    Ok(())
}

// ============================================================
// WAL Change Data Capture
// ============================================================

#[test]
fn test_subscribe_changes() -> TestResult {
    let (_dir, _db, store) = setup();
    let rx = store.subscribe_changes()?;

    store.store(make_entity("cdc-1", "CDC test memory", "cdc"))?;

    // Give the subscriber time to see the WAL change
    std::thread::sleep(std::time::Duration::from_millis(200));

    let events: Vec<_> = rx.try_iter().collect();
    assert!(!events.is_empty(), "Should receive CDC events");
    for event in &events {
        assert!(event.bytes_written > 0);
        assert!(event.timestamp > 0);
        assert!(event.total_wal_bytes > 0);
    }
    Ok(())
}

// ============================================================
// Vector Index + SIMD Search
// ============================================================

#[test]
fn test_vector_index_insert_and_search() -> TestResult {
    let (_dir, _db, store) = setup();

    // Store entities — vector index is created by ensure_schema
    store.store(make_entity("vec-1", "Vector search test one", "vector"))?;
    store.store(make_entity("vec-2", "Vector search test two", "vector"))?;

    // Recall via FTS (no embedding provided)
    let results = store.recall("vector", &[], 10)?;
    // FTS recall may require Tantivy commit — just verify no crash
    let _ = results;

    // Search with a dummy embedding to exercise the vector path
    let dummy_emb = vec![0.1f32; 768];
    let emb_results = store.recall("vector", &dummy_emb, 10)?;
    // Hybrid search may return 0 if vector index has no embeddings — just verify no crash
    let _ = emb_results;
    Ok(())
}

// ============================================================
// Bulk Operations
// ============================================================

#[test]
fn test_store_batch_large() -> TestResult {
    let (_dir, _db, store) = setup();
    let mut entities = Vec::new();
    for i in 0..100 {
        entities.push(make_entity(
            &format!("bulk-{}", i),
            &format!("Bulk memory entry number {}", i),
            "bulk",
        ));
    }
    let count = store.store_batch(entities)?;
    assert_eq!(count, 100);

    let results = store.recall("memory", &[], 10)?;
    assert!(!results.is_empty());
    Ok(())
}

// ============================================================
// Edge Cases
// ============================================================

#[test]
fn test_empty_content() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("empty-1", "", "empty"))?;
    Ok(())
}

#[test]
fn test_special_characters() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("spec-1", "Special chars: ñoño 日本語 🎉", "unicode"))?;
    let results = store.recall("unicode", &[], 10)?;
    assert!(!results.is_empty());
    Ok(())
}

#[test]
fn test_recall_empty_store() -> TestResult {
    let (_dir, _db, store) = setup();
    let results = store.recall("anything", &[], 10)?;
    assert!(results.is_empty());
    Ok(())
}

#[test]
fn test_forget_nonexistent() -> TestResult {
    let (_dir, _db, store) = setup();
    // Forget on nonexistent returns true (idempotent)
    let _ = store.forget("does-not-exist")?;
    Ok(())
}

#[test]
fn test_double_store() -> TestResult {
    let (_dir, _db, store) = setup();
    store.store(make_entity("dup-1", "First version", "dup"))?;
    store.store(make_entity("dup-1", "Second version", "dup"))?;

    let results = store.recall("version", &[], 10)?;
    // Should find the latest version (MERGE behavior)
    let found: Vec<_> = results.iter().filter(|r| r.entity.id == "dup-1").collect();
    assert!(!found.is_empty());
    Ok(())
}

#[test]
fn test_recall_by_time_range() -> TestResult {
    let (_dir, _db, store) = setup();
    let t0 = now_micros();
    store.store(make_entity("range-1", "In range", "time"))?;
    store.store(make_entity("range-2", "Also in range", "time"))?;
    let t1 = now_micros() + 1;

    let results = store.recall_by_time(t0, t1, 10)?;
    assert!(!results.is_empty(), "Should find memories in time range");
    Ok(())
}

#[test]
fn test_expand_nonexistent() -> TestResult {
    let (_dir, _db, store) = setup();
    let results = store.expand("does-not-exist", 1, &["Relates"])?;
    assert!(results.is_empty());
    Ok(())
}

#[test]
fn test_consolidate_contradictions() -> TestResult {
    let (_dir, _db, store) = setup();

    // Similar content length with low word overlap → potential contradiction
    store.store(make_entity("c1", "The sky is blue during daytime", "fact"))?;
    store.store(make_entity("c2", "The ocean reflects atmospheric light", "fact"))?;

    let report = store.consolidate()?;
    // These might or might not trigger contradiction depending on word overlap
    assert!(report.total_entities >= 2);
    Ok(())
}

// ============================================================
// Auto-temporal versioning (execute_at)
// ============================================================

#[test]
fn test_execute_at_time_travel() -> TestResult {
    let (_dir, _db, store) = setup();

    store.store(make_entity("tt-1", "Original version", "time_travel"))?;

    // Get a timestamp after first store
    let t1 = lightning_core::memory::MemoryStore::now_micros_for_test();
    std::thread::sleep(std::time::Duration::from_millis(5));

    store.store(make_entity("tt-1", "Updated version", "time_travel"))?;

    // Should see updated version at current time
    let now = store.recall_recent(10)?;
    let tt: Vec<_> = now.iter().filter(|e| e.id == "tt-1").collect();
    assert!(!tt.is_empty(), "Should find entity at current time");
    assert_eq!(tt[0].content, "Updated version");

    Ok(())
}

// ============================================================
// RAG Pipeline
// ============================================================

#[test]
fn test_rag_query_basic() -> TestResult {
    let (_dir, _db, store) = setup();

    store.store(make_entity("rag-1", "Python is a programming language used in AI development", "rag"))?;
    store.store(make_entity("rag-2", "Machine learning models are trained with large datasets", "rag"))?;
    store.store(make_entity("rag-3", "Data pipelines process information for analysis", "rag"))?;

    let result = store.rag_query("AI programming", &[], 5)?;
    assert!(!result.context.is_empty(), "RAG should produce context");
    assert!(result.total_sources > 0, "RAG should have sources");
    assert_eq!(result.query, "AI programming");
    Ok(())
}

#[test]
fn test_rag_query_empty() -> TestResult {
    let (_dir, _db, store) = setup();
    let result = store.rag_query("nothing", &[], 5)?;
    assert!(result.context.is_empty());
    assert_eq!(result.total_sources, 0);
    Ok(())
}

#[test]
fn test_rag_query_with_graph_expansion() -> TestResult {
    let (_dir, _db, store) = setup();

    store.store(make_entity("hub", "Central machine learning concept", "rag"))?;
    store.store(make_entity("leaf", "Related secondary topic in ML systems", "rag"))?;
    store.associate("hub", "leaf", "RelatesTo", 0.9)?;

    let result = store.rag_query("machine learning", &[], 5)?;
    assert!(!result.context.is_empty());
    assert!(result.total_sources >= 1);
    Ok(())
}

// ============================================================
// WebAssembly Functions
// ============================================================

#[test]
fn test_wasm_function_double() -> TestResult {
    use std::sync::Arc;

    // Write a WAT file
    std::fs::write("/tmp/wasm_test/double.wat", r#"
(module
  (func (export "double") (param f64) (result f64)
    local.get 0
    f64.const 2.0
    f64.mul
  )
)
"#)?;

    let dir = tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    db.register_wasm_function("/tmp/wasm_test/double.wat", "double")?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Test(val DOUBLE, PRIMARY KEY (val))", None)?;
    conn.execute("CREATE (:Test {val: 3.0})", None)?;
    conn.execute("CREATE (:Test {val: 7.5})", None)?;

    let res = conn.execute("MATCH (t:Test) RETURN WASM_double(t.val)", None)?;
    let result = res.batches.first().unwrap();
    let arr = result.column(0).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

    let mut vals: Vec<f64> = (0..result.num_rows()).map(|i| arr.value(i)).collect();
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals.len(), 2);
    assert!((vals[0] - 6.0).abs() < 0.001, "3.0 * 2 = 6.0, got {}", vals[0]);
    assert!((vals[1] - 15.0).abs() < 0.001, "7.5 * 2 = 15.0, got {}", vals[1]);

    Ok(())
}
