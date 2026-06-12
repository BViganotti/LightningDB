use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64, y STRING, PRIMARY KEY (x))", None).unwrap();
    conn.execute("CREATE (:T {x: 1, y: 'alice'})", None).unwrap();
    conn.execute("CREATE (:T {x: 2, y: 'bob'})", None).unwrap();
    conn.execute("CREATE (:T {x: 3, y: 'carol'})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_create_node_table() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.x", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_match_with_index_scan() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T {x: 1}) RETURN t.y", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(val.value(0), "alice");
    Ok(())
}

#[test]
fn test_match_with_where() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (t:T) WHERE t.x > 1 RETURN count(t.x)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_delete_node() -> TestResult {
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
fn test_set_property() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("MATCH (t:T) WHERE t.x = 1 SET t.y = 'updated'", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.x = 1 RETURN t.y", None)?;
    let val = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(val.value(0), "updated");
    Ok(())
}
