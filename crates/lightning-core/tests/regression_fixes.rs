//! Regression tests for the engine fixes made during the hardening pass.
//!
//! Each test names the bug it guards. The three P0 bugs (delete/reopen data
//! loss, int-equality after delete, rollback) live in `engine_audit.rs`; the
//! tests here cover the remaining fixes.

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn new_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn row_count(db: &Arc<Database>, cypher: &str) -> usize {
    db.connect()
        .execute(cypher, None)
        .unwrap()
        .batches
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

// --- Scan/projection fixes -------------------------------------------------

/// A row whose projected property is NULL must still be returned. The scan used
/// to test `batch.column(0)` as if it were `_id`, but projection pushdown made
/// column 0 an arbitrary property, dropping rows with a null value there.
#[test]
fn scan_keeps_null_property_rows_under_projection() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE T(id INT64, x INT64, y STRING)", None)
        .unwrap();
    c.execute("CREATE (:T {id: 1, x: 1, y: 'a'})", None).unwrap();
    c.execute("CREATE (:T {id: 2, x: null, y: null})", None)
        .unwrap();
    c.execute("CREATE (:T {id: 3, x: 3, y: 'c'})", None).unwrap();

    assert_eq!(row_count(&db, "MATCH (t:T) RETURN t.x"), 3, "null x row kept");
    assert_eq!(row_count(&db, "MATCH (t:T) RETURN t.y"), 3, "null y row kept");
}

/// Deleting a row must exclude it even when `_id` is not projected (the
/// projection used to hide that the wrong column was examined).
#[test]
fn scan_delete_excluded_under_projection() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE T(id INT64, x INT64, PRIMARY KEY (id))", None)
        .unwrap();
    for i in 0..5i64 {
        c.execute(&format!("CREATE (:T {{id: {i}, x: {i}0}})"), None).unwrap();
    }
    c.execute("MATCH (t:T {id: 2}) DELETE t", None).unwrap();
    assert_eq!(row_count(&db, "MATCH (t:T) RETURN t.x"), 4);
}

// --- Storage optimizer deadlock -------------------------------------------

/// `optimize` followed by `commit` must not deadlock. `Table::optimize` took
/// `&mut self` (forcing the storage write lock) while `commit` takes the read
/// lock, which is non-reentrant.
#[test]
fn optimize_then_commit_does_not_deadlock() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE P(id INT64, age INT64, dept STRING)", None)
        .unwrap();
    for i in 0..200i64 {
        c.execute(
            &format!("CREATE (:P {{id: {i}, age: 25, dept: 'Engineering'}})"),
            None,
        )
        .unwrap();
    }

    let bm = db.buffer_manager();
    let tx = db.transaction_manager().begin(false).unwrap();
    {
        let storage = db.storage_manager().read();
        let table = storage.node_tables.get("P").unwrap();
        table.optimize(&bm, &tx).unwrap();
    }
    db.transaction_manager().commit(&tx, &bm, &db).unwrap();

    assert_eq!(row_count(&db, "MATCH (p:P) RETURN p.id"), 200);
}

// --- Commit pin leak -------------------------------------------------------

/// Committing must not leak buffer-pool pins. `commit` pinned frames with
/// `pin_page_by_key` and released them with `drop`, which never decrements the
/// pin count; under a small pool repeated commits exhausted the pool.
#[test]
fn commit_does_not_leak_buffer_pins() {
    let dir = tempdir().unwrap();
    let config = SystemConfig {
        buffer_pool_size: 256 * 4096, // small pool; drives constant eviction
        prefetch_enabled: false,
        ..Default::default()
    };
    let db = Database::new(dir.path(), config).unwrap();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE S(id INT64, v INT64, data STRING, PRIMARY KEY (id))",
        None,
    )
    .unwrap();

    // Pre-fix this fails with "Buffer pool exhausted" part-way through.
    for i in 0..500i64 {
        c.execute(
            &format!("CREATE (:S {{id: {i}, v: {}, data: 'row_{i}'}})", i * 3 % 1000),
            None,
        )
        .unwrap();
    }
    assert_eq!(row_count(&db, "MATCH (s:S) RETURN s.id"), 500);
}

// --- Function semantics ----------------------------------------------------

/// `COALESCE` with a leading NULL must use the first non-null argument's type;
/// previously the NULL literal's type won and the string result became a float.
#[test]
fn coalesce_with_leading_null_returns_string() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE Person(name STRING, PRIMARY KEY (name))", None)
        .unwrap();
    c.execute("CREATE (:Person {name: 'Alice'})", None).unwrap();

    let res = c
        .execute("MATCH (p:Person) RETURN COALESCE(NULL, p.name, 'default')", None)
        .unwrap();
    let col = res.batches[0].column(0);
    let s = col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("COALESCE must resolve to a string");
    assert_eq!(s.value(0), "Alice");
}

/// `length(string)` must compute the length, not return the string itself.
#[test]
fn length_returns_int64() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE L(val STRING)", None).unwrap();
    c.execute("CREATE (:L {val: 'hello'})", None).unwrap();

    let res = c.execute("MATCH (l:L) RETURN length(l.val)", None).unwrap();
    let col = res.batches[0].column(0);
    let n = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("length must be Int64");
    assert_eq!(n.value(0), 5);
}

/// `CASE WHEN <comparison>` must parse and evaluate.
#[test]
fn case_when_comparison() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE C(val INT64)", None).unwrap();
    c.execute("CREATE (:C {val: 3})", None).unwrap();

    let res = c
        .execute(
            "MATCH (c:C) RETURN CASE WHEN c.val < 5 THEN 'small' ELSE 'big' END",
            None,
        )
        .unwrap();
    let s = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("CASE must return a string");
    assert_eq!(s.value(0), "small");
}

/// Unary minus on a property must parse and evaluate.
#[test]
fn unary_minus_on_property() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE N(val INT64)", None).unwrap();
    c.execute("CREATE (:N {val: 5})", None).unwrap();

    let res = c.execute("MATCH (n:N) RETURN -n.val", None).unwrap();
    let n = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("unary minus must be numeric");
    assert_eq!(n.value(0), -5);
}

// --- DDL -------------------------------------------------------------------

/// `UINT64`/`UINT32` are advertised by the parser and must create unsigned
/// columns (they previously silently became STRING).
#[test]
fn uint64_ddl_creates_unsigned_column() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE U(id UINT64, name STRING, PRIMARY KEY (id))", None)
        .unwrap();
    c.execute("CREATE (:U {id: 7, name: 'seven'})", None).unwrap();

    let res = c.execute("MATCH (u:U) RETURN u.id", None).unwrap();
    let id_col = res.batches[0].column(0);
    let ids = id_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("UINT64 column must be unsigned");
    assert_eq!(ids.value(0), 7);
}

/// `MERGE` on a non-PK pattern must match an existing node, not duplicate it.
#[test]
fn merge_matches_existing_node_by_non_pk_property() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE Mg(id INT64, val INT64)", None).unwrap();
    c.execute("CREATE (:Mg {id: 1, val: 10})", None).unwrap();

    c.execute("MERGE (m:Mg {id: 1}) ON MATCH SET m.val = 999", None)
        .unwrap();

    assert_eq!(row_count(&db, "MATCH (m:Mg) RETURN m.id"), 1, "no duplicate");
    let res = c.execute("MATCH (m:Mg) RETURN m.val", None).unwrap();
    let v = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("val is Int64");
    assert_eq!(v.value(0), 999, "ON MATCH SET applied");
}

// --- MemoryStore / FTS -----------------------------------------------------

use lightning_core::memory::{MemoryEntity, MemoryStore};

fn embedding(seed: f32, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| (seed + i as f32).sin()).collect()
}

/// Deferred FTS commits must still be visible to a subsequent search.
#[test]
fn memory_store_recall_sees_just_stored_entity() {
    let (_d, db) = new_db();
    let store = MemoryStore::new(db.connect(), 384);
    store
        .store(MemoryEntity {
            id: "mem_1".into(),
            entity_type: "note".into(),
            content: "the quick brown zanzibar fox".into(),
            embedding: embedding(1.0, 384),
            ..Default::default()
        })
        .unwrap();

    let hits = store.recall("zanzibar", &embedding(1.0, 384), 10).unwrap();
    assert!(
        hits.iter().any(|h| h.entity.id == "mem_1"),
        "search must flush deferred FTS writes"
    );
}

/// Deferred FTS writes must be durable across a checkpoint + reopen.
#[test]
fn memory_store_fts_survives_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::new(&path, SystemConfig::default()).unwrap();
        let store = MemoryStore::new(db.connect(), 384);
        store
            .store(MemoryEntity {
                id: "persist_1".into(),
                entity_type: "note".into(),
                content: "durable macadamia content".into(),
                embedding: embedding(2.0, 384),
                ..Default::default()
            })
            .unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::new(&path, SystemConfig::default()).unwrap();
    let store = MemoryStore::new(db.connect(), 384);
    let hits = store.recall("macadamia", &embedding(2.0, 384), 10).unwrap();
    assert!(
        hits.iter().any(|h| h.entity.id == "persist_1"),
        "FTS writes must survive checkpoint + reopen"
    );
}

/// Concurrent MemoryStore setup must not fail the schema-creation race, and
/// all writes must succeed.
#[test]
fn memory_store_concurrent_writes_no_setup_errors() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::new(dir.path(), SystemConfig::default()).unwrap());
    let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let handles: Vec<_> = (0..4u64)
        .map(|t| {
            let db = Arc::clone(&db);
            let errors = Arc::clone(&errors);
            std::thread::spawn(move || {
                let store = MemoryStore::new(db.connect(), 384);
                for i in 0..10u64 {
                    let e = MemoryEntity {
                        id: format!("concurrent_{t}_{i}"),
                        entity_type: "n".into(),
                        content: format!("thread {t} entity {i}"),
                        embedding: embedding((t * 10 + i) as f32, 384),
                        ..Default::default()
                    };
                    if store.store(e).is_err() {
                        errors.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        errors.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "concurrent schema setup and writes must all succeed"
    );
}

/// SET assignment expressions must be remapped to the projection-pushed batch
/// layout, not left as binder indices (which go out of bounds).
#[test]
fn set_expression_remapped_under_projection_pushdown() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE E(id STRING, type STRING, content STRING, a INT64, b INT64, c INT64, PRIMARY KEY (id))",
        None,
    )
    .unwrap();
    c.execute("CREATE (:E {id: 'x', type: 't', content: 'c', a: 1, b: 2, c: 3})", None)
        .unwrap();

    // WHERE touches only id/c; SET reads `a` (not in the WHERE), which forces
    // projection pushdown to include and remap `a`.
    c.execute(
        "MATCH (e:E) WHERE e.id = 'x' AND e.c > 0 SET e.a = e.a + 10",
        None,
    )
    .unwrap();

    let res = c.execute("MATCH (e:E) RETURN e.a", None).unwrap();
    let v = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("a is Int64");
    assert_eq!(v.value(0), 11);
}

/// The vector index must support embeddings too large for a single 4096-byte
/// page (e.g. OpenAI's 1536 dims), which previously stored nothing and made
/// vector-only recall return zero results.
#[test]
fn vector_index_supports_1536_dimensions() {
    let (_d, db) = new_db();
    let dim = 1536usize;
    let store = MemoryStore::new(db.connect(), dim);
    for i in 0..5u64 {
        store
            .store(MemoryEntity {
                id: format!("hd_{i}"),
                entity_type: "hd".into(),
                content: format!("entity {i}"),
                embedding: embedding(i as f32, dim),
                ..Default::default()
            })
            .unwrap();
    }

    // Vector-only recall (empty text => FTS contributes nothing).
    let hits = store.recall("", &embedding(0.0, dim), 5).unwrap();
    assert_eq!(hits.len(), 5, "high-dim vector recall must return all vectors");
    for h in &hits {
        assert_eq!(h.entity.embedding.len(), dim, "returned embedding dimension");
    }

    // Delete one and confirm it disappears from vector recall.
    assert!(store.forget("hd_0").unwrap());
    let hits = store.recall("", &embedding(0.0, dim), 10).unwrap();
    assert_eq!(hits.len(), 4, "deleted vector must not be returned");
}

/// `OFFSET` is accepted as a synonym for `SKIP`, in either clause order.
#[test]
fn offset_clause_is_supported() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE T(x INT64)", None).unwrap();
    for i in 0..5i64 {
        c.execute(&format!("CREATE (:T {{x: {i}}})"), None).unwrap();
    }
    let res = c
        .execute("MATCH (t:T) RETURN t.x ORDER BY t.x LIMIT 2 OFFSET 1", None)
        .unwrap();
    let xs: Vec<i64> = res
        .batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(xs, vec![1, 2], "LIMIT 2 OFFSET 1 should yield rows 1,2");
}

/// Adding a column after rows were written must not panic on flush (write
/// buffers created before the ALTER have fewer columns than the table).
#[test]
fn alter_add_column_after_writes_then_set() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE E(id INT64, name STRING, PRIMARY KEY (id))", None)
        .unwrap();
    for i in 0..100i64 {
        c.execute(&format!("CREATE (:E {{id: {i}, name: 'base_{i}'}})"), None)
            .unwrap();
    }
    c.execute("ALTER TABLE E ADD COLUMN score DOUBLE", None).unwrap();
    c.execute("MATCH (e:E) SET e.score = 0.5", None).unwrap();
    assert_eq!(row_count(&db, "MATCH (e:E) RETURN e.score"), 100);

    c.execute("ALTER TABLE E ADD COLUMN active BOOL", None).unwrap();
    c.execute("MATCH (e:E) SET e.active = TRUE", None).unwrap();
    assert_eq!(row_count(&db, "MATCH (e:E {active: TRUE}) RETURN e.id"), 100);
}

/// An unlabeled relationship pattern is resolved from the endpoint node labels
/// when exactly one rel table connects them.
#[test]
fn unlabeled_relationship_pattern_resolves_single_table() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE X(id INT64, PRIMARY KEY (id))", None).unwrap();
    c.execute("CREATE NODE TABLE Y(id INT64, PRIMARY KEY (id))", None).unwrap();
    c.execute("CREATE REL TABLE R1(FROM X TO Y)", None).unwrap();
    c.execute("CREATE (:X {id: 1})", None).unwrap();
    c.execute("CREATE (:Y {id: 1})", None).unwrap();
    c.execute("MATCH (x:X {id: 1}), (y:Y {id: 1}) CREATE (x)-[:R1]->(y)", None)
        .unwrap();

    // No relationship label: inferred from X -> Y (only R1 connects them).
    assert_eq!(row_count(&db, "MATCH (x:X)-[r]->(y:Y) RETURN r"), 1);
}

/// DDL must invalidate cached plans: re-running the same query string after a
/// schema change must re-bind against the new schema, not reuse the old plan.
#[test]
fn ddl_invalidates_plan_cache() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE X(id INT64, PRIMARY KEY (id))", None).unwrap();
    c.execute("CREATE NODE TABLE Y(id INT64, PRIMARY KEY (id))", None).unwrap();
    c.execute("CREATE REL TABLE R1(FROM X TO Y)", None).unwrap();

    // Cache a plan for this exact query string (single candidate => works).
    assert_eq!(row_count(&db, "MATCH (x:X)-[r]->(y:Y) RETURN count(*)"), 1);

    // Adding a second rel table changes what this query means; the cached plan
    // must be dropped so the ambiguity is reported instead of a stale result.
    c.execute("CREATE REL TABLE R2(FROM X TO Y)", None).unwrap();
    let after = c.execute("MATCH (x:X)-[r]->(y:Y) RETURN count(*)", None);
    assert!(
        after.is_err(),
        "plan cache must be invalidated by CREATE REL TABLE"
    );
}

/// UNION must execute every arm (the parser previously kept only the last
/// statement) and UNION (distinct) must deduplicate.
#[test]
fn union_executes_all_arms_and_dedups() {
    let (_d, db) = new_db();
    let c = db.connect();
    c.execute("CREATE NODE TABLE P(name STRING, PRIMARY KEY (name))", None)
        .unwrap();
    for n in ["Alice", "Bob", "Charlie"] {
        c.execute(&format!("CREATE (:P {{name: '{n}'}})"), None).unwrap();
    }

    // UNION ALL of two different single-row arms => 2 rows.
    assert_eq!(
        row_count(
            &db,
            "MATCH (p:P) WHERE p.name = 'Alice' RETURN p.name AS name \
             UNION ALL MATCH (p:P) WHERE p.name = 'Bob' RETURN p.name AS name"
        ),
        2
    );
    // UNION of the same row twice => 1 (deduplicated).
    assert_eq!(
        row_count(
            &db,
            "MATCH (p:P) WHERE p.name = 'Alice' RETURN p.name AS name \
             UNION MATCH (p:P) WHERE p.name = 'Alice' RETURN p.name AS name"
        ),
        1
    );
    // Trailing LIMIT applies to the whole union.
    assert_eq!(
        row_count(
            &db,
            "MATCH (p:P) RETURN p.name AS name UNION MATCH (p:P) RETURN p.name AS name LIMIT 2"
        ),
        2
    );
}

/// `UNWIND $list` must evaluate a list-typed query parameter.
#[test]
fn list_parameter_unwinds() {
    let (_d, db) = new_db();
    let c = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert(
        "ids".to_string(),
        lightning_core::Value::List(vec![
            lightning_core::Value::Number(1.0),
            lightning_core::Value::Number(2.0),
            lightning_core::Value::Number(3.0),
        ]),
    );
    let res = c.execute("UNWIND $ids AS id RETURN id", Some(params)).unwrap();
    assert_eq!(
        res.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        3
    );
}
