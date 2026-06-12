use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None).unwrap();
    conn.execute("CREATE (:T {x: 1})", None).unwrap();
    conn.execute("CREATE (:T {x: 2})", None).unwrap();
    conn.execute("CREATE (:T {x: 3})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_wal_basic_write() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 3);
    Ok(())
}

#[test]
fn test_wal_multiple_writes() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    for i in 10..20 {
        conn.execute(&format!("CREATE (:T {{x: {}}})", i), None)?;
    }
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 13);
    Ok(())
}

#[test]
fn test_wal_update() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("MATCH (t:T) WHERE t.x = 1 SET t.x = 42", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.x = 42 RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 1);
    Ok(())
}

#[test]
fn test_wal_delete() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("MATCH (t:T) WHERE t.x = 1 DELETE t", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_wal_checkpoint() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    db.checkpoint()?;
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 3);
    Ok(())
}
