use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64, y STRING)", None).unwrap();
    conn.execute("CREATE (:T {x: 1, y: 'hello'})", None).unwrap();
    conn.execute("CREATE (:T {x: 2, y: 'world'})", None).unwrap();
    conn.execute("CREATE (:T {x: 3, y: 'test'})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_memory_store_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_memory_store_content_filter() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) WHERE t.y CONTAINS 'hello' RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_memory_store_update() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("MATCH (t:T) WHERE t.x = 1 SET t.y = 'updated'", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.x = 1 RETURN t.y", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(val.value(0), "updated");
    Ok(())
}

#[test]
fn test_memory_store_delete() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("MATCH (t:T) WHERE t.x = 1 DELETE t", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}
