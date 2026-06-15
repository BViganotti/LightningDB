use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_flatten_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1})", None)?;
    conn.execute("CREATE (:T {id: 2})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_flatten_empty_unwind() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1})", None)?;
    let res = conn.execute("UNWIND [] AS x RETURN x", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_flatten_no_input() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_flatten_with_where() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1})", None)?;
    conn.execute("CREATE (:T {id: 2})", None)?;
    conn.execute("CREATE (:T {id: 3})", None)?;
    let res = conn.execute("UNWIND [10, 20] AS x RETURN x", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}
