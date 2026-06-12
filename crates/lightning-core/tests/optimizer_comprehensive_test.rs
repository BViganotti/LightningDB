use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None).unwrap();
    for i in 0..50 {
        conn.execute(&format!("CREATE (:T {{x: {}}})", i), None).unwrap();
    }
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_optimizer_simple_select() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 50);
    Ok(())
}

#[test]
fn test_optimizer_filter_pushdown() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) WHERE t.x > 25 RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 24);
    Ok(())
}

#[test]
fn test_optimizer_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.x LIMIT 5", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_optimizer_skip_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.x SKIP 5 LIMIT 3", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_optimizer_empty_result() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) WHERE t.x > 100 RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_optimizer_projection() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.x * 2 AS doubled", None)?;
    assert_eq!(count_rows(&res), 50);
    Ok(())
}

#[test]
fn test_optimizer_count() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 50);
    Ok(())
}
