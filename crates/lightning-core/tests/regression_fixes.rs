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
