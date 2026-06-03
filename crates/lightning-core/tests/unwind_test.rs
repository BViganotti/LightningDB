use lightning_core::{Database, SystemConfig};
use lightning_core::catalog::PropertyDefinition;
use lightning_types::LogicalType;
use tempfile::tempdir;
use lightning_core::processor::Value;

#[test]
fn test_baseline_match() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table("Person".into(), vec![
            PropertyDefinition { name: "name".into(), type_: LogicalType::String },
        ], None).unwrap();
        let mut storage = db.storage_manager.write();
        storage.create_table("Person".into(), vec![("name".into(), LogicalType::String)], false, None).unwrap();
    }
    conn.query("CREATE (p:Person {name: 'Alice'})").unwrap();

    let query = "MATCH (p:Person) RETURN p.name";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    assert_eq!(result.batches[0].num_rows(), 1);
}

#[test]
fn test_unwind_simple_literal() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    let query = "UNWIND 1 AS x RETURN x";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    assert_eq!(result.batches[0].num_rows(), 1);
    assert_eq!(Value::from_arrow(result.batches[0].column(0), 0), Value::Number(1.0));
}

#[test]
fn test_unwind_constant_list() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // Constant list unwinding
    let query = "UNWIND [1, 2, 3] AS x RETURN x";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    let batches = result.batches;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
    
    // Check values
    assert_eq!(Value::from_arrow(batches[0].column(0), 0), Value::Number(1.0));
    assert_eq!(Value::from_arrow(batches[0].column(0), 1), Value::Number(2.0));
    assert_eq!(Value::from_arrow(batches[0].column(0), 2), Value::Number(3.0));
}

#[test]
fn test_unwind_with_match() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // 1. Setup Data
    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table("Person".into(), vec![
            PropertyDefinition { name: "name".into(), type_: LogicalType::String },
        ], None).unwrap();

        let mut storage = db.storage_manager.write();
        storage.create_table("Person".into(), vec![
            ("name".into(), LogicalType::String),
        ], false, None).unwrap();
    }

    conn.query("CREATE (p:Person {name: 'Alice'})").unwrap();
    conn.query("CREATE (p:Person {name: 'Bob'})").unwrap();

    // 2. Query: UNWIND [1, 2] for each person
    let query = "MATCH (p:Person) UNWIND [1, 2] AS x RETURN p.name, x";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    let batches = result.batches;
    // Total rows should be 2 (people) * 2 (unwind elements) = 4
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[test]
fn test_unwind_empty_list() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // Setup Data
    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table("Person".into(), vec![
            PropertyDefinition { name: "name".into(), type_: LogicalType::String },
        ], None).unwrap();

        let mut storage = db.storage_manager.write();
        storage.create_table("Person".into(), vec![
            ("name".into(), LogicalType::String),
        ], false, None).unwrap();
    }

    conn.query("CREATE (p:Person {name: 'Alice'})").unwrap();

    // UNWIND an empty list should result in 0 rows
    let query = "MATCH (p:Person) UNWIND [] AS x RETURN p.name, x";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}
