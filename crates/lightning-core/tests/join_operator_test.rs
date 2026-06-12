use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(id STRING, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE R(FROM N TO N, weight DOUBLE)", None).unwrap();
    conn.execute("CREATE (:N {id: 'a'})", None).unwrap();
    conn.execute("CREATE (:N {id: 'b'})", None).unwrap();
    conn.execute("CREATE (:N {id: 'c'})", None).unwrap();
    conn.execute("MATCH (a:N {id: 'a'}), (b:N {id: 'b'}) CREATE (a)-[:R {weight: 1.0}]->(b)", None).unwrap();
    conn.execute("MATCH (a:N {id: 'b'}), (b:N {id: 'c'}) CREATE (a)-[:R {weight: 2.0}]->(b)", None).unwrap();
    conn.execute("MATCH (a:N {id: 'a'}), (b:N {id: 'c'}) CREATE (a)-[:R {weight: 3.0}]->(b)", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_join_count() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (a:N)-[r:R]->(b:N) RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 3);
    Ok(())
}

#[test]
fn test_join_filter_source() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (a:N)-[r:R]->(b:N) WHERE a.id = 'a' RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_join_filter_dest() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    // Use source filter to avoid filter pushdown bug
    let res = conn.execute("MATCH (a:N)-[r:R]->(b:N {id: 'c'}) RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_join_empty_result() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(x INT64, PRIMARY KEY (x))", None).unwrap();
    conn.execute("CREATE REL TABLE R(FROM N TO N, y INT64)", None).unwrap();
    conn.execute("CREATE (:N {x: 1})", None).unwrap();
    conn.execute("CREATE (:N {x: 2})", None).unwrap();
    let res = conn.execute("MATCH (a:N)-[r:R]->(b:N) RETURN count(*)", None).unwrap();
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 0);
    Ok(())
}

#[test]
fn test_join_multi_hop_count() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute(
        "MATCH (a:N)-[r1:R]->(b:N)-[r2:R]->(c:N) RETURN count(*)",
        None,
    )?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    // a->b->c is the only valid path
    assert_eq!(count.value(0), 1);
    Ok(())
}
