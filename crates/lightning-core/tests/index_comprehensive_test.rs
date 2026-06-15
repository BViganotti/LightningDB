use arrow::array::Int64Array;
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

fn setup_person_index(conn: &lightning_core::Connection) -> TestResult {
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, email STRING, age INT64, city STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice', email: 'alice@test.com', age: 30, city: 'NYC'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob', email: 'bob@test.com', age: 25, city: 'LA'})", None)?;
    conn.execute("CREATE (:Person {id: 3, name: 'Charlie', email: 'charlie@test.com', age: 35, city: 'NYC'})", None)?;
    conn.execute("CREATE (:Person {id: 4, name: 'Diana', email: 'diana@test.com', age: 28, city: 'Chicago'})", None)?;
    conn.execute("CREATE (:Person {id: 5, name: 'Eve', email: 'eve@test.com', age: 45, city: 'LA'})", None)?;
    Ok(())
}

// === Primary key lookup (implicit index) ===

#[test]
fn test_index_pk_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 3 RETURN p.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(name.value(0), "Charlie");
    Ok(())
}

#[test]
fn test_index_pk_lookup_nonexistent() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.id = 999 RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_index_pk_string_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id STRING, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 'key1', val: 100})", None)?;
    conn.execute("CREATE (:T {id: 'key2', val: 200})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.id = 'key1' RETURN t.val", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val.value(0), 100);
    Ok(())
}

// === Index scan (property equality) ===

#[test]
fn test_index_scan_equality() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.email", None)?;
    let email = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(email.value(0), "alice@test.com");
    Ok(())
}

#[test]
fn test_index_scan_no_match() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name = 'NonExistent' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

#[test]
fn test_index_scan_partial_string() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === Hash index with multiple matches ===

#[test]
fn test_index_multiple_matches() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.city = 'NYC' RETURN count(p.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 2);
    Ok(())
}

#[test]
fn test_index_multiple_matches_large() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, group_id INT64, PRIMARY KEY (id))", None)?;
    for i in 0..100 {
        conn.execute(&format!("CREATE (:T {{id: {}, group_id: {}}})", i, i % 5), None)?;
    }
    let res = conn.execute("MATCH (t:T) WHERE t.group_id = 3 RETURN count(t.id)", None)?;
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count.value(0), 20);
    Ok(())
}

// === No index (scan should still work) ===

#[test]
fn test_index_no_index_scan() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 42})", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.val = 42 RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 1);
    Ok(())
}

// === FTS index via CONTAINS ===

#[test]
fn test_index_contains_search() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Doc(id INT64, content STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:Doc {id: 1, content: 'the quick brown fox jumps over the lazy dog'})", None)?;
    conn.execute("CREATE (:Doc {id: 2, content: 'a quick brown rabbit'})", None)?;
    conn.execute("CREATE (:Doc {id: 3, content: 'lazy dog sleeping'})", None)?;
    let res = conn.execute("MATCH (d:Doc) WHERE d.content CONTAINS 'quick' RETURN count(d.id)", None)?;
    assert!(count_rows(&res) > 0);
    Ok(())
}

// === Relationship index ===

#[test]
fn test_index_relationship_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64)", None)?;
    conn.execute("CREATE (:Person {id: 1, name: 'Alice'})", None)?;
    conn.execute("CREATE (:Person {id: 2, name: 'Bob'})", None)?;
    conn.execute("MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS {since: 2020}]->(b)", None)?;
    let res = conn.execute("MATCH (a:Person {id: 1})-[r:KNOWS]->(b:Person) RETURN b.name", None)?;
    let name = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(name.value(0), "Bob");
    Ok(())
}

// === Index with UPDATE ===

#[test]
fn test_index_update_preserves_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.name = 'Alice' SET p.name = 'Alicia'", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name = 'Alicia' RETURN p.email", None)?;
    let email = res.batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(email.value(0), "alice@test.com");
    Ok(())
}

// === Index with DELETE ===

#[test]
fn test_index_delete_removes_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    setup_person_index(&conn)?;
    conn.execute("MATCH (p:Person) WHERE p.name = 'Bob' DELETE p", None)?;
    let res = conn.execute("MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}

// === Large PK index ===

#[test]
fn test_index_large_insert_lookup() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    for i in 0..500 {
        conn.execute(&format!("CREATE (:T {{id: {}, val: {}}})", i, i * 2), None)?;
    }
    let res = conn.execute("MATCH (t:T) WHERE t.id = 499 RETURN t.val", None)?;
    let val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(val.value(0), 998);
    Ok(())
}

// === Index on empty table ===

#[test]
fn test_index_empty_table_scan() -> TestResult {
    let (_dir, db) = setup();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    let res = conn.execute("MATCH (t:T) WHERE t.name = 'anything' RETURN t.id", None)?;
    assert_eq!(count_rows(&res), 0);
    Ok(())
}
