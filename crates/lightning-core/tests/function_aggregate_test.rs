use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY (name))", None).unwrap();
    conn.execute("CREATE (:Person {name: 'alice', age: 30, city: 'NYC'})", None).unwrap();
    conn.execute("CREATE (:Person {name: 'bob', age: 25, city: 'LA'})", None).unwrap();
    conn.execute("CREATE (:Person {name: 'carol', age: 35, city: 'NYC'})", None).unwrap();
    conn.execute("CREATE (:Person {name: 'dave', age: 25, city: 'LA'})", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_count_all() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 4);
    Ok(())
}

#[test]
fn test_count_with_filter() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.city = 'NYC' RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_group_by() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) RETURN p.city, count(*)", None)?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_contains_function() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1); // alice
    Ok(())
}

#[test]
fn test_starts_with_function() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.name STARTS WITH 'a' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1); // alice
    Ok(())
}

#[test]
fn test_ends_with_function() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.name ENDS WITH 'ol' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1); // carol
    Ok(())
}

#[test]
fn test_comparison_operators() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.age > 25 RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2); // alice(30), carol(35)
    Ok(())
}

#[test]
fn test_in_operator() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    let res = conn.execute("MATCH (p:Person) WHERE p.city IN ['NYC', 'LA'] RETURN count(*)", None)?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 4);
    Ok(())
}
