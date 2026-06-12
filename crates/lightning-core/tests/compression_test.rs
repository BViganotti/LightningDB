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
    conn.execute("CREATE (:T {x: 4, y: 'dave', z: 1.73, w: false})", None).unwrap();
    conn.execute("CREATE (:T {x: 5, y: 'eve', z: 2.23, w: true})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_compression_integers() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 5);
    Ok(())
}

#[test]
fn test_compression_strings() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.y", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_compression_doubles() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) RETURN t.z", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_compression_booleans() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) WHERE t.w = true RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 3); // alice, carol, eve
    Ok(())
}

#[test]
fn test_compression_repeated_values() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None).unwrap();
    for _ in 0..100 {
        conn.execute("CREATE (:T {x: 42})", None).unwrap();
    }
    let res = conn.execute("MATCH (t:T) RETURN count(t.x)", None).unwrap();
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 100);
    Ok(())
}

#[test]
fn test_compression_null_values() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64, y STRING)", None).unwrap();
    conn.execute("CREATE (:T {x: 1, y: 'hello'})", None).unwrap();
    conn.execute("CREATE (:T {x: null, y: null})", None).unwrap();
    conn.execute("CREATE (:T {x: 3, y: 'world'})", None).unwrap();
    let res = conn.execute("MATCH (t:T) RETURN t.x, t.y", None).unwrap();
    assert_eq!(count_rows(&res), 3);
    Ok(())
}
