use lightning_core::{Database, SystemConfig};
use lightning_core::catalog::PropertyDefinition;
use lightning_types::LogicalType;
use tempfile::tempdir;

#[test]
fn test_explain_query() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table("Person".into(), vec![
            PropertyDefinition { name: "name".into(), type_: LogicalType::String },
        ], None);
    }

    let query = "EXPLAIN MATCH (a:Person), (b:Person) RETURN a.name";
    let result = conn.query(query).unwrap();
    
    assert!(result.is_success());
    assert_eq!(result.column_names[0], "Plan");
    let batches = result.batches;
    assert_eq!(batches.len(), 1);
    let plan = batches[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(0);
    assert!(plan.contains("Join") || plan.contains("Scan"));
    println!("Plan: {}", plan);
}
