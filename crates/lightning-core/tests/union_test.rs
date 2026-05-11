use lightning_core::Database;
use lightning_core::SystemConfig;
use std::sync::Arc;

fn setup_db() -> Arc<Database> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::new(temp_dir.path(), SystemConfig::default()).unwrap();
    
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))",
        None,
    )
    .unwrap();
    conn.query("CREATE (p:Person {name: 'Alice', age: 30})").unwrap();
    conn.query("CREATE (p:Person {name: 'Bob', age: 25})").unwrap();
    conn.query("CREATE (p:Person {name: 'Charlie', age: 35})").unwrap();
    
    db
}

#[test]
fn test_union_all() {
    let db = setup_db();
    let conn = db.connect();
    
    // UNION ALL should return all rows from both sides (1 + 1 = 2)
    let result = conn.query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name UNION ALL MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name AS name").unwrap();
    assert!(result.is_success());
    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
    
    // UNION ALL with same rows
    let result = conn.query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name UNION ALL MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name").unwrap();
    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[test]
fn test_union_deduplicate() {
    let db = setup_db();
    let conn = db.connect();
    
    // UNION (distinct) should deduplicate
    let result = conn.query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name UNION MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name").unwrap();
    assert!(result.is_success());
    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[test]
fn test_union_multiple() {
    let db = setup_db();
    let conn = db.connect();
    
    let result = conn.query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name AS name UNION MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name AS name UNION MATCH (p:Person) WHERE p.name = 'Charlie' RETURN p.name AS name").unwrap();
    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn test_union_schema_mismatch() {
    let db = setup_db();
    let conn = db.connect();
    
    // Column name mismatch
    let result = conn.query("MATCH (p:Person) RETURN p.name AS name UNION MATCH (p:Person) RETURN p.age AS age");
    assert!(result.is_err());
    let err = match result { Ok(_) => panic!("Expected error"), Err(e) => e };
    assert!(err.to_string().contains("Column name mismatch"));
    
    // Column count mismatch
    let result = conn.query("MATCH (p:Person) RETURN p.name AS name, p.age AS age UNION MATCH (p:Person) RETURN p.name AS name");
    assert!(result.is_err());
    let err = match result { Ok(_) => panic!("Expected error"), Err(e) => e };
    assert!(err.to_string().contains("same number of columns"));
}
