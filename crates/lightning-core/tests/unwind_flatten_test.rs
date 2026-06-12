use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64, y STRING, z DOUBLE, w BOOL, PRIMARY KEY (x))", None).unwrap();
    conn.execute("CREATE REL TABLE R(weight DOUBLE)", None).unwrap();
    conn.execute("CREATE (:T {x: 1, y: 'alice', z: 3.14, w: true})", None).unwrap();
    conn.execute("CREATE (:T {x: 2, y: 'bob', z: 2.71, w: false})", None).unwrap();
    conn.execute("CREATE (:T {x: 3, y: 'carol', z: 1.41, w: true})", None).unwrap();
    conn.execute("MATCH (a:T {x: 1}), (b:T {x: 2}) CREATE (a)-[:R {weight: 1.0}]->(b)", None).unwrap();
    conn.execute("MATCH (a:T {x: 2}), (b:T {x: 3}) CREATE (a)-[:R {weight: 2.0}]->(b)", None).unwrap();
    conn.execute("MATCH (a:T {x: 1}), (b:T {x: 3}) CREATE (a)-[:R {weight: 3.0}]->(b)", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_unwind_basic() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert("ids".to_string(), lightning_core::Value::List(vec![
        lightning_core::Value::Number(1.0),
        lightning_core::Value::Number(2.0),
        lightning_core::Value::Number(3.0),
    ]));
    let res = conn.execute("UNWIND $ids AS id RETURN id", Some(params))?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_unwind_empty_list() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert("ids".to_string(), lightning_core::Value::List(vec![]));
    let res = conn.execute("UNWIND $ids AS id RETURN id", Some(params))?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_unwind_with_match() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(x INT64)", None)?;
    conn.execute("CREATE (:T {x: 1})", None)?;
    conn.execute("CREATE (:T {x: 2})", None)?;
    let mut params = std::collections::HashMap::new();
    params.insert("ids".to_string(), lightning_core::Value::List(vec![
        lightning_core::Value::Number(1.0),
        lightning_core::Value::Number(2.0),
    ]));
    let res = conn.execute("UNWIND $ids AS id MATCH (t:T) WHERE t.x = id RETURN t.x", Some(params))?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_unwind_with_string_list() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert("names".to_string(), lightning_core::Value::List(vec![
        lightning_core::Value::String("alice".to_string()),
        lightning_core::Value::String("bob".to_string()),
    ]));
    let res = conn.execute("UNWIND $names AS name RETURN name", Some(params))?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}

#[test]
fn test_unwind_with_order_by() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert("ids".to_string(), lightning_core::Value::List(vec![
        lightning_core::Value::Number(3.0),
        lightning_core::Value::Number(1.0),
        lightning_core::Value::Number(2.0),
    ]));
    let res = conn.execute("UNWIND $ids AS id RETURN id", Some(params))?;
    assert_eq!(count_rows(&res), 3);
    Ok(())
}

#[test]
fn test_unwind_with_limit() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    let mut params = std::collections::HashMap::new();
    params.insert("ids".to_string(), lightning_core::Value::List(vec![
        lightning_core::Value::Number(1.0),
        lightning_core::Value::Number(2.0),
        lightning_core::Value::Number(3.0),
    ]));
    let res = conn.execute("UNWIND $ids AS id RETURN id LIMIT 2", Some(params))?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}
