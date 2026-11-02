//! Performance regression gates. Measures a few representative operations and
//! asserts conservative lower bounds so a change that makes the engine
//! dramatically slower fails CI. Thresholds are set well below observed
//! throughput to tolerate CI noise while still catching order-of-magnitude
//! regressions.
//!
//! Run with: cargo test -p lightning-core --release --test bench_gates_test -- --nocapture

use lightning_core::{Database, SyncMode, SystemConfig};
use std::sync::Arc;
use std::time::Instant;

fn open() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    (dir, db)
}

fn bulk_nodes(db: &Arc<Database>, table: &str, n: i64) {
    let ids: arrow::array::Int64Array = (0..n).map(Some).collect();
    let batch =
        arrow::record_batch::RecordBatch::try_from_iter(vec![("id", Arc::new(ids) as _)]).unwrap();
    db.connect().bulk_insert_batch(table, &batch).unwrap();
}

fn rate(name: &str, ops: u64, secs: f64, min_ops_per_sec: f64) {
    let per_sec = ops as f64 / secs;
    eprintln!(
        "GATE {name}: {ops} ops in {secs:.3}s = {per_sec:.0} ops/s (min {min_ops_per_sec:.0})"
    );
    assert!(
        per_sec >= min_ops_per_sec,
        "{name} regression: {per_sec:.0} ops/s < gate {min_ops_per_sec:.0} ops/s"
    );
}

#[test]
fn gate_bulk_insert() {
    let (_d, db) = open();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE Item(id INT64, val STRING, category INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    let n = 20_000usize;
    let ids: arrow::array::Int64Array = (0..n as i64).map(Some).collect();
    let vals: arrow::array::StringArray = (0..n).map(|i| Some(format!("val_{i}"))).collect();
    let cats: arrow::array::Int64Array = (0..n as i64).map(|i| Some(i % 20)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("val", Arc::new(vals) as _),
        ("category", Arc::new(cats) as _),
    ])
    .unwrap();
    let t = Instant::now();
    c.bulk_insert_batch("Item", &batch).unwrap();
    rate("bulk_insert", n as u64, t.elapsed().as_secs_f64(), 5_000.0);
}

#[test]
fn gate_point_lookup_and_scan() {
    let (_d, db) = open();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE Item(id INT64, v INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    let n = 5_000i64;
    let ids: arrow::array::Int64Array = (0..n).map(Some).collect();
    let vs: arrow::array::Int64Array = (0..n).map(Some).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("v", Arc::new(vs) as _),
    ])
    .unwrap();
    c.bulk_insert_batch("Item", &batch).unwrap();
    // point lookups
    let lookups = 2_000u64;
    let t = Instant::now();
    for k in 0..lookups {
        let id = (k as i64) % n;
        let r = c
            .execute(&format!("MATCH (t:Item {{id: {id}}}) RETURN t.v"), None)
            .unwrap();
        assert_eq!(r.batches[0].num_rows(), 1);
    }
    rate("point_lookup", lookups, t.elapsed().as_secs_f64(), 500.0);

    // full scan
    let scans = 200u64;
    let t = Instant::now();
    for _ in 0..scans {
        let r = c.execute("MATCH (t:Item) RETURN t.v", None).unwrap();
        assert!(!r.batches.is_empty());
    }
    rate("full_scan", scans, t.elapsed().as_secs_f64(), 50.0);
}

#[test]
fn gate_traversal() {
    let (_d, db) = open();
    let c = db.connect();
    c.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE NODE TABLE B(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE REL TABLE R(FROM A TO B)", None).unwrap();
    let n = 2_000i64;
    bulk_nodes(&db, "A", n);
    bulk_nodes(&db, "B", n);
    for i in 0..n {
        c.execute(
            &format!("MATCH (a:A {{id: {i}}}), (b:B {{id: {i}}}) CREATE (a)-[:R]->(b)"),
            None,
        )
        .unwrap();
    }
    let hops = 500u64;
    let t = Instant::now();
    for _ in 0..hops {
        let r = c
            .execute("MATCH (a:A)-[:R]->(b:B) RETURN count(*)", None)
            .unwrap();
        assert!(!r.batches.is_empty());
    }
    rate("one_hop_traversal", hops, t.elapsed().as_secs_f64(), 20.0);
}

#[test]
fn gate_vector_search() {
    let (_d, db) = open();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE V(id INT64, emb LIST(FLOAT), PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    db.storage_manager()
        .write()
        .create_vector_index("V", 8)
        .unwrap();
    let vec = db
        .storage_manager()
        .read()
        .vector_indexes
        .get("V")
        .cloned()
        .unwrap();
    let bm = db.buffer_manager().clone();
    let n = 2_000i64;
    let tx = db.transaction_manager().begin(false).unwrap();
    for i in 0..n {
        let emb: Vec<f32> = (0..8).map(|d| ((i * 8 + d) % 100) as f32).collect();
        vec.insert(i as u64, &emb, &bm, &tx).unwrap();
    }
    db.transaction_manager().commit(&tx, &bm, &db).unwrap();

    let rtx = db.transaction_manager().begin(true).unwrap();
    let q: Vec<f32> = (0..8).map(|d| d as f32).collect();
    let searches = 2_000u64;
    let t = Instant::now();
    for _ in 0..searches {
        let r = vec.search(&q, 10, &bm, &rtx).unwrap();
        assert!(!r.is_empty());
    }
    db.transaction_manager().rollback(&db, &rtx).unwrap();
    rate("vector_search", searches, t.elapsed().as_secs_f64(), 200.0);
}

#[test]
fn gate_fts_search() {
    let (_d, db) = open();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE D(id INT64, body STRING, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    db.storage_manager().write().create_fts_index("D").unwrap();
    let fts = db
        .storage_manager()
        .read()
        .fts_indexes
        .get("D")
        .cloned()
        .unwrap();
    let bm = db.buffer_manager().clone();
    let n = 2_000i64;
    for i in 0..n {
        let body = format!("document number {i} about widgets");
        fts.insert_multi_field(i as u64, &[("body".to_string(), body.as_str())])
            .unwrap();
    }
    fts.commit().unwrap();

    let rtx = db.transaction_manager().begin(true).unwrap();
    let searches = 2_000u64;
    let t = Instant::now();
    for _ in 0..searches {
        let r = fts.search("widgets", 10, &bm, &rtx).unwrap();
        assert!(!r.is_empty());
    }
    db.transaction_manager().rollback(&db, &rtx).unwrap();
    rate("fts_search", searches, t.elapsed().as_secs_f64(), 200.0);
}
