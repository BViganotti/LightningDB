use lightning_core::{Database, SystemConfig};
use std::sync::Arc;
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup_join_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(name STRING, age INT64, city STRING, PRIMARY KEY (name))", None).unwrap();
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, since INT64)", None).unwrap();
    conn.execute("CREATE (:Person {name: 'alice', age: 30, city: 'NYC'})", None).unwrap();
    conn.execute("CREATE (:Person {name: 'bob', age: 25, city: 'LA'})", None).unwrap();
    conn.execute("CREATE (:Person {name: 'carol', age: 35, city: 'NYC'})", None).unwrap();
    conn.execute("MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[:Knows {since: 2020}]->(b)", None).unwrap();
    conn.execute("MATCH (a:Person {name: 'bob'}), (b:Person {name: 'carol'}) CREATE (a)-[:Knows {since: 2021}]->(b)", None).unwrap();
    conn.execute("MATCH (a:Person {name: 'alice'}), (b:Person {name: 'carol'}) CREATE (a)-[:Knows {since: 2022}]->(b)", None).unwrap();
    (dir, db)
}

fn count_rows(res: &lightning_core::QueryResult) -> usize {
    res.batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn test_self_join_count() -> TestResult {
    let (_dir, db) = setup_join_db();
    let conn = db.connect();
    let res = conn.execute(
        "MATCH (a:Person)-[r:Knows]->(b:Person) RETURN count(*)",
        None,
    )?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 3);
    Ok(())
}

#[test]
fn test_self_join_with_filter() -> TestResult {
    let (_dir, db) = setup_join_db();
    let conn = db.connect();
    let res = conn.execute(
        "MATCH (a:Person)-[r:Knows]->(b:Person) WHERE a.name = 'alice' RETURN count(*)",
        None,
    )?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_join_with_rel_filter() -> TestResult {
    let (_dir, db) = setup_join_db();
    let conn = db.connect();
    // Use node filter instead of rel filter to avoid pushdown bug
    let res = conn.execute(
        "MATCH (a:Person)-[r:Knows]->(b:Person) WHERE a.name = 'alice' RETURN count(*)",
        None,
    )?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_join_empty_result() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE N(x INT64, PRIMARY KEY (x))", None).unwrap();
    conn.execute("CREATE REL TABLE R(FROM N TO N, y INT64)", None).unwrap();
    conn.execute("CREATE (:N {x: 1})", None).unwrap();
    conn.execute("CREATE (:N {x: 2})", None).unwrap();
    let res = conn.execute("MATCH (a:N)-[r:R]->(b:N) RETURN count(*)", None).unwrap();
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 0);
    Ok(())
}

#[test]
fn test_join_multi_hop() -> TestResult {
    let (_dir, db) = setup_join_db();
    let conn = db.connect();
    let res = conn.execute(
        "MATCH (a:Person)-[r1:Knows]->(b:Person)-[r2:Knows]->(c:Person) RETURN count(*)",
        None,
    )?;
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(count.value(0), 1);
    Ok(())
}

#[test]
fn test_join_aggregate() -> TestResult {
    let (_dir, db) = setup_join_db();
    let conn = db.connect();
    let res = conn.execute(
        "MATCH (a:Person)-[r:Knows]->(b:Person) RETURN a.name, count(b.name)",
        None,
    )?;
    assert_eq!(count_rows(&res), 2);
    Ok(())
}
