use lightning_core::Database;
use lightning_types::LogicalType;
use tempfile::TempDir;

#[test]
fn test_projection_pushdown() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::new(temp_dir.path(), lightning_core::SystemConfig::default()).unwrap();
    let conn = db.connect();

    // Create a table with many columns
    db.create_node_table(
        "Person".to_string(),
        vec![
            lightning_core::catalog::PropertyDefinition { name: "id".to_string(), type_: LogicalType::Int32 },
            lightning_core::catalog::PropertyDefinition { name: "name".to_string(), type_: LogicalType::String },
            lightning_core::catalog::PropertyDefinition { name: "age".to_string(), type_: LogicalType::Int32 },
            lightning_core::catalog::PropertyDefinition { name: "city".to_string(), type_: LogicalType::String },
            lightning_core::catalog::PropertyDefinition { name: "salary".to_string(), type_: LogicalType::Int32 },
        ],
        Some("id".to_string()),
    ).unwrap();

    // Insert some data
    conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30, city: 'NY', salary: 100000})").unwrap();

    // Query only name and age
    let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();
    
    assert!(result.success);
    assert_eq!(result.column_names, vec!["name".to_string(), "age".to_string()]);
    assert_eq!(result.batches.len(), 1);
    let batch = &result.batches[0];
    assert_eq!(batch.num_columns(), 2);
    
    // Verify data correctness - this ensures column indices were resolved correctly in the projected batch
    let names = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    let ages = batch.column(1).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(ages.value(0), 30);

    // Test with IndexScan (id = 1)
    let result_idx = conn.query("MATCH (p:Person {id: 1}) RETURN p.salary, p.city").unwrap();
    assert!(result_idx.success);
    let batch_idx = &result_idx.batches[0];
    assert_eq!(batch_idx.num_columns(), 2);
    let salaries = batch_idx.column(0).as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
    let cities = batch_idx.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(salaries.value(0), 100000);
    assert_eq!(cities.value(0), "NY");
}
