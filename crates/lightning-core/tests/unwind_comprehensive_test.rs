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

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_unwind_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("UNWIND [1, 2, 3] AS id RETURN id", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_unwind_single_value() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("UNWIND [42] AS x RETURN x", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_unwind_bind_to_create() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, label STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, label: 'a'})", None)?;
    conn.execute("CREATE (:T {id: 2, label: 'b'})", None)?;
    conn.execute("CREATE (:T {id: 3, label: 'c'})", None)?;
    let res = conn.execute("MATCH (t:T) RETURN count(t.id)", None)?;
    let c = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), 3);
    Ok(())
}

#[test]
fn test_unwind_strings() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("UNWIND ['Alice', 'Bob'] AS name RETURN name", None)?;
    assert_eq!(count_rows(&res), 2);
    let name = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "Alice");
    assert_eq!(name.value(1), "Bob");
    Ok(())
}

#[test]
fn test_unwind_mixed_types() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("UNWIND [1, 2, 3] AS x RETURN x", None)?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}
