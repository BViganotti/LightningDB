//! Invariant probes for the storage engine. Each test exercises a property a
//! column store must guarantee; failures point at real defects.
use arrow::array::{Array, Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use std::collections::HashMap;
use std::sync::Arc;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db: Arc<Database> = Database::new(&path, SystemConfig::default()).unwrap();
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, s STRING, n INT64, PRIMARY KEY (id))",
            None,
        )
        .unwrap();
    (dir, db)
}

fn s(v: &str) -> HashMap<String, lightning_core::processor::Value> {
    let mut p = HashMap::new();
    p.insert("p".into(), lightning_core::processor::Value::String(v.into()));
    p
}

fn get_s(db: &Arc<Database>, id: i64) -> Option<String> {
    let res = db
        .connect()
        .execute(&format!("MATCH (t:T {{id: {id}}}) RETURN t.s"), None)
        .unwrap();
    if res.batches.is_empty() || res.batches[0].num_rows() == 0 {
        return None;
    }
    let a = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    if a.is_null(0) {
        None
    } else {
        Some(a.value(0).to_string())
    }
}

/// NULL, empty string and a set string must all round-trip distinctly.
#[test]
fn null_empty_and_set_are_distinct() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 1})", None).unwrap();
    assert_eq!(get_s(&db, 1), None, "unset string must read as NULL");

    conn.execute("MATCH (t:T {id: 1}) SET t.s = ''", None).unwrap();
    assert_eq!(get_s(&db, 1).as_deref(), Some(""), "empty string must be stored");

    conn.execute("MATCH (t:T {id: 1}) SET t.s = 'x'", None).unwrap();
    assert_eq!(get_s(&db, 1).as_deref(), Some("x"));

    conn.execute("MATCH (t:T {id: 1}) SET t.s = null", None).unwrap();
    assert_eq!(get_s(&db, 1), None, "SET null must clear the value");
}

/// Strings across the inline/overflow boundary must round-trip after updates.
#[test]
fn string_length_boundaries_round_trip() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 1})", None).unwrap();
    for len in [0usize, 1, 63, 64, 65, 200, 5000] {
        let val = "a".repeat(len);
        conn.execute("MATCH (t:T {id: 1}) SET t.s = $p", Some(s(&val)))
            .unwrap_or_else(|e| panic!("SET len={len} failed: {e}"));
        assert_eq!(
            get_s(&db, 1).as_deref(),
            Some(val.as_str()),
            "round-trip failed at len={len}"
        );
    }
}

/// Values must survive a reopen (persistence, not just buffer visibility).
#[test]
fn values_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db: Arc<Database> = Database::new(&path, SystemConfig::default()).unwrap();
        let conn = db.connect();
        conn.execute("CREATE NODE TABLE T(id INT64, s STRING, n INT64, PRIMARY KEY (id))", None).unwrap();
        conn.execute("CREATE (:T {id: 1, s: 'hello', n: 7})", None).unwrap();
        conn.execute("CREATE (:T {id: 2, s: 'world'})", None).unwrap();
        db.checkpoint().unwrap();
    }
    let db: Arc<Database> = Database::new(&path, SystemConfig::default()).unwrap();
    assert_eq!(get_s(&db, 1).as_deref(), Some("hello"));
    assert_eq!(get_s(&db, 2).as_deref(), Some("world"));
}

/// A scan over several rows with mixed null/non-null strings must be correct.
#[test]
fn multi_row_null_scan() {
    let (_d, db) = setup();
    let conn = db.connect();
    for id in 1..=6 {
        conn.execute(&format!("CREATE (:T {{id: {id}}})"), None).unwrap();
    }
    for id in [1i64, 3, 5] {
        conn.execute(&format!("MATCH (t:T {{id: {id}}}) SET t.s = 'v{id}'"), None).unwrap();
    }
    let res = conn
        .execute("MATCH (t:T) RETURN t.id, t.s ORDER BY t.id", None)
        .unwrap();
    let ids = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let vals = res.batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.len(), 6);
    for i in 0..6 {
        let id = ids.value(i);
        if [1, 3, 5].contains(&id) {
            assert_eq!(vals.value(i), format!("v{id}"), "row {id} value");
        } else {
            assert!(vals.is_null(i), "row {id} must be null");
        }
    }
}

/// Updating a string to a different length must not leave stale bytes.
#[test]
fn update_changes_length_cleanly() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 1, s: 'averylonginitialvalue'})", None).unwrap();
    conn.execute("MATCH (t:T {id: 1}) SET t.s = 'short'", None).unwrap();
    assert_eq!(get_s(&db, 1).as_deref(), Some("short"), "shrink must not leave tail");
    conn.execute("MATCH (t:T {id: 1}) SET t.s = 'this is much longer than before'", None).unwrap();
    assert_eq!(get_s(&db, 1).as_deref(), Some("this is much longer than before"));
}

/// UPDATE must not affect other rows (row addressing correctness).
#[test]
fn update_is_row_local() {
    let (_d, db) = setup();
    let conn = db.connect();
    for id in 1..=5 {
        conn.execute(&format!("CREATE (:T {{id: {id}, s: 'orig{id}'}})"), None).unwrap();
    }
    conn.execute("MATCH (t:T {id: 3}) SET t.s = 'changed'", None).unwrap();
    for id in 1..=5 {
        let expected = if id == 3 { "changed".to_string() } else { format!("orig{id}") };
        assert_eq!(get_s(&db, id).as_deref(), Some(expected.as_str()), "row {id}");
    }
}

/// Delete then re-create: counts and reads must stay consistent.
#[test]
fn delete_then_reinsert() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 1, s: 'a'})", None).unwrap();
    conn.execute("CREATE (:T {id: 2, s: 'b'})", None).unwrap();
    conn.execute("MATCH (t:T {id: 1}) DELETE t", None).unwrap();
    assert_eq!(get_s(&db, 1), None, "deleted row must not be visible");
    conn.execute("CREATE (:T {id: 3, s: 'c'})", None).unwrap();
    assert_eq!(get_s(&db, 3).as_deref(), Some("c"));
    assert_eq!(get_s(&db, 2).as_deref(), Some("b"));
}
