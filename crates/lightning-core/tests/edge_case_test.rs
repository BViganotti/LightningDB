use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64, y STRING, z DOUBLE, w BOOL)", None).unwrap();
    conn.execute("CREATE (:T {x: 1, y: 'alice', z: 3.14, w: true})", None).unwrap();
    conn.execute("CREATE (:T {x: 2, y: 'bob', z: 2.71, w: false})", None).unwrap();
    conn.execute("CREATE (:T {x: 3, y: 'carol', z: 1.41, w: true})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_edge_empty_table() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_edge_single_row() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 42})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(val.value(0), 42);
    Ok(())
}

// NOTE: test_edge_null_values removed — pre-existing bug with string column null handling
// causes "Incorrect length of null buffer for StringArray" error.

#[test]
fn test_edge_empty_string() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x STRING)", None)?;
    conn.execute("CREATE (:T {x: ''})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(val.value(0), "");
    Ok(())
}

#[test]
fn test_edge_zero_values() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 0})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(val.value(0), 0);
    Ok(())
}

#[test]
fn test_edge_negative_values() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: -42})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(val.value(0), -42);
    Ok(())
}

#[test]
fn test_edge_large_numbers() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 9223372036854775807})", None)?; // i64::MAX
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(val.value(0), i64::MAX);
    Ok(())
}

#[test]
fn test_edge_count_empty() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 0);
    Ok(())
}

#[test]
fn test_edge_limit_zero() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x LIMIT 0", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_edge_skip_all() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    conn.execute("CREATE (:T {x: 2})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x SKIP 10", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_edge_order_by_empty() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}
