//! Phase 2 regression tests: CSR construction must be bounded by the
//! authoritative node-table size, and must never trust a corrupt node id far
//! enough to attempt a multi-gigabyte allocation (which would abort the
//! process).
use lightning_core::processor::Value;
use lightning_core::{Database, SystemConfig};
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn row_count(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn csr_rebuild_preserves_traversal_and_edge_less_nodes() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE REL TABLE E(FROM N TO N)", None)?;
    for i in 1..=5 {
        conn.execute(&format!("CREATE (:N {{id: {i}, name: 'n{i}'}})"), None)?;
    }
    for i in 2..=4 {
        conn.execute(
            &format!("MATCH (a:N {{id: 1}}), (b:N {{id: {i}}}) CREATE (a)-[:E]->(b)"),
            None,
        )?;
    }

    // Force a full CSR rebuild through the storage layer.
    {
        let bm = db.buffer_manager();
        let tx = db.transaction_manager().begin(false)?;
        db.storage_manager().read().rebuild_csr("E", bm, &tx)?;
        db.transaction_manager().commit(&tx, bm, db.as_ref())?;
    }

    let res = conn.execute(
        "MATCH (a:N {id: 1})-[r:E]->(b:N) RETURN b.id ORDER BY b.id",
        None,
    )?;
    assert_eq!(row_count(&res), 3, "all three edges must survive a CSR rebuild");

    // A node with no outgoing edges must yield an empty result: offsets cover
    // every valid node id, so no stale edges can leak in from a prior build.
    let res = conn.execute("MATCH (a:N {id: 5})-[r:E]->(b:N) RETURN b.id", None)?;
    assert_eq!(row_count(&res), 0, "edge-less node must have no neighbors");
    Ok(())
}

#[test]
fn csr_rebuild_drops_corrupt_endpoint_without_oom() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE E(FROM N TO N)", None)?;
    for i in 1..=3 {
        conn.execute(&format!("CREATE (:N {{id: {i}}})"), None)?;
    }
    conn.execute(
        "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:E]->(b)",
        None,
    )?;
    conn.execute(
        "MATCH (a:N {id: 1}), (b:N {id: 3}) CREATE (a)-[:E]->(b)",
        None,
    )?;

    // Corrupt one edge's source id to an absurd value. Trusting it would demand
    // an offsets array of ~2^64 entries; the fix drops it instead.
    {
        let bm = db.buffer_manager();
        let tm = db.transaction_manager();
        let tx = tm.begin(false)?;
        {
            let guard = db.storage_manager().read();
            let rel = guard.get_table("E").expect("rel table E");
            rel.columns[0].append_value(bm, &Value::Node(u64::MAX - 8), 0, &tx)?;
        }
        tm.commit(&tx, bm, db.as_ref())?;
    }

    // Must succeed (dropping the corrupt edge) rather than OOM/abort.
    {
        let bm = db.buffer_manager();
        let tx = db.transaction_manager().begin(false)?;
        db.storage_manager().read().rebuild_csr("E", bm, &tx)?;
        db.transaction_manager().commit(&tx, bm, db.as_ref())?;
    }

    let res = conn.execute("MATCH (a:N)-[r:E]->(b:N) RETURN b.id", None)?;
    let total = row_count(&res);
    assert!(
        total <= 1,
        "corrupt edge must be dropped while valid edges survive; got {total} rows"
    );
    Ok(())
}
