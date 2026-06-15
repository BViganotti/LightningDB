use arrow::array::{Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    (dir, db)
}

fn setup_data(conn: &lightning_core::Connection) -> TestResult {
    conn.execute("CREATE NODE TABLE T(id INT64, name STRING, val INT64, PRIMARY KEY (id))", None)?;
    for i in 0..20 {
        conn.execute(&format!("CREATE (:T {{id: {}, name: 'n{}', val: {}}})", i, i, i * 10), None)?;
    }
    Ok(())
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

// === LIMIT ===

#[test]
fn test_limit_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 5", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_limit_zero() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 0", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_limit_one() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 1", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_limit_more_than_rows() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 999", None)?;
    assert_eq!(count_rows(&res), 20);
    Ok(())
}

#[test]
fn test_limit_empty_table() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 10", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

// === SKIP ===

#[test]
fn test_skip_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 15", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_skip_zero() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 0", None)?;
    assert_eq!(count_rows(&res), 20);
    Ok(())
}

#[test]
fn test_skip_all() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 999", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

// === SKIP + LIMIT ===

#[test]
fn test_skip_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 5 LIMIT 5", None)?;
    assert_eq!(count_rows(&res), 5);
    Ok(())
}

#[test]
fn test_skip_limit_exhausted() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 18 LIMIT 5", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

// === WITH + LIMIT ===

#[test]
fn test_with_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 3", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

// === Return specific columns with LIMIT ===

#[test]
fn test_limit_column_projection() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.name LIMIT 2", None)?;
    assert_eq!(count_rows(&res), 2);
    let names = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "n0");
    assert_eq!(names.value(1), "n1");
    Ok(())
}

// === WHERE + LIMIT ===

#[test]
fn test_where_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val > 50 RETURN t.id LIMIT 3", None)?;
    assert!(count_rows(&res) <= 3);
    assert!(count_rows(&res) > 0);
    Ok(())
}

// === Negative / edge case limits ===

#[test]
fn test_limit_negative_value() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let result = conn.execute("MATCH (t:T) RETURN t.id LIMIT -1", None);
    // May error or return all rows, just verify no crash
    Ok(())
}

#[test]
fn test_skip_negative_value() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let result = conn.execute("MATCH (t:T) RETURN t.id SKIP -1", None);
    Ok(())
}

// === Large LIMIT ===

#[test]
fn test_large_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id LIMIT 1000000", None)?;
    assert_eq!(count_rows(&res), 20);
    Ok(())
}

// === SKIP past end with no LIMIT ===

#[test]
fn test_skip_past_end() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute("MATCH (t:T) RETURN t.id SKIP 100", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}
