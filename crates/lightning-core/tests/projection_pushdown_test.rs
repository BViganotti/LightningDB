use lightning_core::Database;
use tempfile::TempDir;

#[test]
fn test_projection_pushdown() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::new(temp_dir.path(), lightning_core::SystemConfig::default()).unwrap();
    let conn = db.connect();

    // Create a table with many columns
    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, city STRING, salary INT64, PRIMARY KEY(id))",
        None,
    ).unwrap();

    // Insert some data
    conn.query("CREATE (p:Person {id: 1, name: 'Alice', age: 30, city: 'NY', salary: 100000})").unwrap();

    // Query only name and age
    let result = conn.query("MATCH (p:Person) RETURN p.name, p.age").unwrap();
    
    assert!(result.is_success());
    assert_eq!(result.batches.len(), 1);
    let batch = &result.batches[0];
    assert_eq!(batch.num_columns(), 2);
    
    // Verify data correctness - this ensures column indices were resolved correctly in the projected batch
    let names = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    let ages = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(ages.value(0), 30);

    // Test with IndexScan (id = 1)
    let result_idx = conn.query("MATCH (p:Person {id: 1}) RETURN p.salary, p.city").unwrap();
    assert!(result_idx.is_success());
    let batch_idx = &result_idx.batches[0];
    assert_eq!(batch_idx.num_columns(), 2);
    let salaries = batch_idx.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    let cities = batch_idx.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
    assert_eq!(salaries.value(0), 100000);
    assert_eq!(cities.value(0), "NY");
}
