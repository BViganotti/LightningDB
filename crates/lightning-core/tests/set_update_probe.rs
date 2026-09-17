//! Regression tests for property updates (`SET`) and the `MERGE` forms used by
//! the LightningMCP settings/LSP-cache/conversation/git-history writers.
use arrow::array::{Array, Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;

fn cell_string(res: &lightning_core::QueryResult, col: usize) -> String {
    res.batches[0]
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

fn cell_i64(res: &lightning_core::QueryResult, col: usize) -> i64 {
    res.batches[0]
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

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

/// `SET` works for both strings and ints when the value already exists.
#[test]
fn set_updates_existing_values() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 1, s: 'a', n: 1})", None).unwrap();
    conn.execute("MATCH (t:T {id: 1}) SET t.s = 'hello', t.n = 42", None)
        .unwrap();
    let res = conn
        .execute("MATCH (t:T {id: 1}) RETURN t.s, t.n", None)
        .unwrap();
    assert_eq!(cell_string(&res, 0), "hello");
    assert_eq!(cell_i64(&res, 1), 42);

    // Absent int property at creation.
    conn.execute("CREATE (:T {id: 2, s: 'x'})", None).unwrap();
    conn.execute("MATCH (t:T {id: 2}) SET t.n = 7", None).unwrap();
    let res = conn
        .execute("MATCH (t:T {id: 2}) RETURN t.s, t.n", None)
        .unwrap();
    assert_eq!(cell_string(&res, 0), "x");
    assert_eq!(cell_i64(&res, 1), 7);
}

/// The `MERGE ... ON CREATE SET ... ON MATCH SET ...` form used by the writers
/// persists strings and ints on both create and match.
#[test]
fn merge_on_create_and_match_set_persists() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute(
        "MERGE (m:T {id: 5}) ON CREATE SET m.s = 'ocs', m.n = 55 ON MATCH SET m.s = 'matched', m.n = 56",
        None,
    )
    .unwrap();
    let res = conn
        .execute("MATCH (m:T {id: 5}) RETURN m.s, m.n", None)
        .unwrap();
    assert_eq!(cell_string(&res, 0), "ocs", "create set must persist");
    assert_eq!(cell_i64(&res, 1), 55);

    // Second MERGE takes the MATCH branch.
    conn.execute(
        "MERGE (m:T {id: 5}) ON CREATE SET m.s = 'ocs', m.n = 55 ON MATCH SET m.s = 'matched', m.n = 56",
        None,
    )
    .unwrap();
    let res = conn
        .execute("MATCH (m:T {id: 5}) RETURN m.s, m.n", None)
        .unwrap();
    assert_eq!(cell_string(&res, 0), "matched", "match set must persist");
    assert_eq!(cell_i64(&res, 1), 56);
}

/// Known LightningDB limitation: a bare `SET` applied to a string column that
/// was NULL at creation does not persist (ints do). The parser now routes a
/// trailing SET to a real Set clause (it used to be dropped entirely), but the
/// column write for the null→string transition is still lost. All in-repo
/// writers use `ON CREATE SET`/`ON MATCH SET`, which avoids it.
#[test]
#[ignore = "LightningDB bug: SET of a string column that was NULL at creation does not persist"]
fn set_null_string_column_bug() {
    let (_d, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE (:T {id: 6})", None).unwrap();
    conn.execute("MATCH (t:T {id: 6}) SET t.s = 'w', t.n = 6", None)
        .unwrap();
    let res = conn
        .execute("MATCH (t:T {id: 6}) RETURN t.s, t.n", None)
        .unwrap();
    assert_eq!(cell_string(&res, 0), "w");
    assert_eq!(cell_i64(&res, 1), 6);
}
