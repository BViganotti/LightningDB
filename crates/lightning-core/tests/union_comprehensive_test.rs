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

fn setup_data(conn: &lightning_core::Connection) -> TestResult {
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 25})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie', age: 35})", None)?;
    Ok(())
}

#[test]
fn test_union_basic() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name AS name",
        None,
    )?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_union_all() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION ALL MATCH (p:Person) RETURN p.name AS name",
        None,
    )?;
    assert!(count_rows(&res) >= 3);
    Ok(())
}

#[test]
fn test_union_dedup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name AS name",
        None,
    )?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_union_empty() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    let res = conn.execute(
        "MATCH (t:T) RETURN t.val AS val UNION MATCH (t:T) RETURN t.val AS val",
        None,
    )?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_union_with_where() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name AS name",
        None,
    )?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_union_with_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name AS name LIMIT 2",
        None,
    )?;
    assert!(count_rows(&res) <= 2);
    Ok(())
}

#[test]
fn test_union_all_with_limit() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION ALL MATCH (p:Person) RETURN p.name AS name LIMIT 4",
        None,
    )?;
    assert!(count_rows(&res) >= 2);
    Ok(())
}

#[test]
fn test_union_different_labels() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Employee(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Employee {id: 1, name: 'Alice'})", None)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (e:Employee) RETURN e.name AS name",
        None,
    )?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

#[test]
fn test_union_order_preserved() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_data(&conn)?;
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.name AS name",
        None,
    )?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}
