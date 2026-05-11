use lightning_core::Database;
use lightning_core::SystemConfig;
use lightning_core::catalog::PropertyDefinition;
use lightning_types::LogicalType;
use arrow::array::{Float64Array, StringArray};
use std::sync::Arc;
use tempfile::tempdir;

fn setup_db() -> Arc<Database> {
    let dir = tempdir().unwrap();
    let config = SystemConfig::default();
    let db = Database::new(dir.path(), config).unwrap();
    
    db.create_node_table(
        "Person".to_string(),
        vec![
            PropertyDefinition { name: "name".to_string(), type_: LogicalType::String },
            PropertyDefinition { name: "age".to_string(), type_: LogicalType::Double },
            PropertyDefinition { name: "height".to_string(), type_: LogicalType::Double },
        ],
        Some("name".to_string()),
    ).unwrap();

    let conn = db.connect();
    conn.query("CREATE (:Person {name: 'Alice', age: 25.6, height: 1.65})").unwrap();
    conn.query("CREATE (:Person {name: 'Bob', age: 42.1, height: 1.88})").unwrap();
    conn.query("CREATE (:Person {name: 'Charlie', age: -10.5, height: 1.75})").unwrap();
    
    db
}

#[test]
fn test_arithmetic_functions() {
    let db = setup_db();
    let conn = db.connect();

    let res = conn.query("MATCH (p:Person) RETURN ABS(p.age) AS abs_age, CEIL(p.age) AS ceil_age, FLOOR(p.age) AS floor_age, ROUND(p.age) AS round_age").unwrap();
    assert!(res.is_success());
    
    let mut total_rows = 0;
    for batch in res.batches {
        total_rows += batch.num_rows();
        let abs_col = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let ceil_col = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        
        for i in 0..batch.num_rows() {
            let age: f64 = if i == 0 { 25.6 } else if i == 1 { 42.1 } else { -10.5 };
            assert_eq!(abs_col.value(i), age.abs());
            assert_eq!(ceil_col.value(i), age.ceil());
        }
    }
    assert_eq!(total_rows, 3);
}

#[test]
fn test_string_functions() {
    let db = setup_db();
    let conn = db.connect();

    let res = conn.query("MATCH (p:Person) RETURN CONCAT(p.name, ' suffix') AS concatenated, SUBSTRING(p.name, 0, 2) AS sub, REPLACE(p.name, 'A', 'X') AS replaced").unwrap();
    assert!(res.is_success());
    
    for batch in res.batches {
        let concat_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let sub_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let replace_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        
        for i in 0..batch.num_rows() {
            let name = if i == 0 { "Alice" } else if i == 1 { "Bob" } else { "Charlie" };
            assert_eq!(concat_col.value(i), format!("{} suffix", name));
            assert_eq!(sub_col.value(i), &name[0..2]);
            assert_eq!(replace_col.value(i), name.replace("A", "X"));
        }
    }
}

#[test]
fn test_coalesce_function() {
    let db = setup_db();
    let conn = db.connect();

    let res = conn.query("MATCH (p:Person) RETURN COALESCE(NULL, p.name, 'default') AS coal").unwrap();
    assert!(res.is_success());
    
    for batch in res.batches {
        let coal_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            let name = if i == 0 { "Alice" } else if i == 1 { "Bob" } else { "Charlie" };
            assert_eq!(coal_col.value(i), name);
        }
    }
}
