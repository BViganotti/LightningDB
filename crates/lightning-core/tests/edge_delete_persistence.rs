//! Relationship deletion must be durable: `DELETE r` / `DETACH DELETE n` null
//! out a relationship row's endpoints, and that tombstone has to survive a
//! reopen. If it does not, deleted edges reappear after restart and any
//! "repair by deleting rows" strategy is silently a no-op.
use arrow::array::{Array, Int64Array};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;

fn edge_count(db: &Arc<Database>) -> i64 {
    let conn = db.connect();
    let res = conn
        .execute("MATCH (a:N)-[r:E]->(b:N) RETURN count(r)", None)
        .unwrap_or_else(|e| panic!("count query failed: {e}"));
    res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

fn setup(dir: &std::path::Path) -> Arc<Database> {
    let db = Database::new(dir, SystemConfig::default()).expect("open db");
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )
    .expect("node table");
    conn.execute("CREATE REL TABLE E(FROM N TO N)", None)
        .expect("rel table");
    conn.execute("CREATE (:N {id: 1, name: 'A'})", None)
        .expect("node a");
    conn.execute("CREATE (:N {id: 2, name: 'B'})", None)
        .expect("node b");
    conn.execute(
        "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E]->(b)",
        None,
    )
    .expect("edge");
    db
}

#[test]
fn edge_delete_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = setup(&path);
        assert_eq!(edge_count(&db), 1, "edge should exist after creation");
        let conn = db.connect();
        conn.execute("MATCH (a:N)-[r:E]->(b:N) DELETE r", None)
            .expect("delete edge");
        assert_eq!(edge_count(&db), 0, "edge should be gone immediately after DELETE");
        db.checkpoint().expect("checkpoint");
    }

    let db = Database::new(&path, SystemConfig::default()).expect("reopen");
    assert_eq!(edge_count(&db), 0, "edge deletion must persist across reopen");
}

#[test]
fn detach_delete_endpoint_removes_edge_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    {
        let db = setup(&path);
        let conn = db.connect();
        conn.execute("MATCH (a:N {id: 1}) DETACH DELETE a", None)
            .expect("detach delete");
        assert_eq!(edge_count(&db), 0, "detached endpoint should leave no edge");
        db.checkpoint().expect("checkpoint");
    }

    let db = Database::new(&path, SystemConfig::default()).expect("reopen");
    assert_eq!(
        edge_count(&db),
        0,
        "DETACH DELETE must not leave the edge behind after reopen"
    );
}
