use lightning_core::{Database, SystemConfig};
use lightning_core::catalog::PropertyDefinition;
use lightning_types::LogicalType;
use tempfile::tempdir;

#[test]
fn test_optimizer_filter_pushdown_e2e() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // 1. Setup Catalog and Storage
    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table("Person".into(), vec![
            PropertyDefinition { name: "id".into(), type_: LogicalType::Int64 },
            PropertyDefinition { name: "age".into(), type_: LogicalType::Int64 },
            PropertyDefinition { name: "name".into(), type_: LogicalType::String },
        ], None).unwrap();
        
        catalog.add_rel_table("Knows".into(), "Person".into(), "Person".into(), vec![]).unwrap();

        let mut storage = db.storage_manager.write();
        storage.create_table("Person".into(), vec![
            ("id".into(), LogicalType::Int64),
            ("age".into(), LogicalType::Int64),
            ("name".into(), LogicalType::String),
        ], false, None).unwrap();
        
        storage.create_table("Knows".into(), vec![], true, None).unwrap();
    }

    // 2. Insert Data
    conn.query("CREATE (p:Person {id: 1, age: 30, name: 'Alice'})").unwrap();
    conn.query("CREATE (p:Person {id: 2, age: 15, name: 'Bob'})").unwrap();
    conn.query("CREATE (p:Person {id: 3, age: 25, name: 'Charlie'})").unwrap();

    // 3. Query
    let query = "MATCH (a:Person), (b:Person) WHERE a.age > 20 RETURN a.name, b.name";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    let batches = result.batches;
    assert_eq!(batches.len(), 1); 
    assert_eq!(batches[0].num_rows(), 6);
}
