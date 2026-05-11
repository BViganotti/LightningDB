use lightning_core::processor::Value;
use lightning_core::Database;
use lightning_types::LogicalType;

use arrow::array::Array;
use tempfile::tempdir;

#[test]
fn test_semijoin_pushdown_optimization() {
    let dir = tempdir().unwrap();
    let db = Database::new(
        dir.path().to_path_buf(),
        lightning_core::SystemConfig::default(),
    )
    .unwrap();

    // 1. Create Schema
    {
        db.create_node_table(
            "User".to_string(),
            vec![lightning_core::catalog::PropertyDefinition {
                name: "name".to_string(),
                type_: LogicalType::String,
            }],
            Some("name".to_string()),
        )
        .unwrap();
        db.create_rel_table(
            "Follows".to_string(),
            "User".to_string(),
            "User".to_string(),
            vec![],
        )
        .unwrap();

        // Mock set num_rows in catalog (usually updated during appends, but here we mock it)
        let mut catalog = db.catalog.write();
        catalog.get_node_table_mut("User").unwrap().num_rows = 4;
        catalog.get_rel_table_mut("Follows").unwrap().num_rows = 5;
    }

    // 2. Insert Data
    {
        let tx = db.transaction_manager.begin(false).unwrap();
        let bm = &db.buffer_manager;
        let storage = db.storage_manager.read();
        let user_table = storage.get_table("User").unwrap();
        let follows_table = storage.get_table("Follows").unwrap();

        // Users: [(_id, name)]
        user_table
            .append_row(
                bm,
                &[Value::Node(0), Value::String("Alice".to_string())],
                0,
                &tx,
            )
            .unwrap();
        user_table
            .append_row(
                bm,
                &[Value::Node(1), Value::String("Bob".to_string())],
                1,
                &tx,
            )
            .unwrap();
        user_table
            .append_row(
                bm,
                &[Value::Node(2), Value::String("Charlie".to_string())],
                2,
                &tx,
            )
            .unwrap();
        user_table
            .append_row(
                bm,
                &[Value::Node(3), Value::String("David".to_string())],
                3,
                &tx,
            )
            .unwrap();

        // Follows: [(_src, _dst)]
        follows_table
            .append_row(bm, &[Value::Node(0), Value::Node(2)], 0, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(1), Value::Node(0)], 1, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(1), Value::Node(2)], 2, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(2), Value::Node(3)], 3, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(3), Value::Node(0)], 4, &tx)
            .unwrap();

        // Build Index
        let index = storage.get_index("User").unwrap();
        index
            .insert(bm, &Value::String("Alice".to_string()), 0, &tx)
            .unwrap();
        index
            .insert(bm, &Value::String("Bob".to_string()), 1, &tx)
            .unwrap();
        index
            .insert(bm, &Value::String("Charlie".to_string()), 2, &tx)
            .unwrap();
        index
            .insert(bm, &Value::String("David".to_string()), 3, &tx)
            .unwrap();

        db.transaction_manager.commit(&tx, bm, &db).unwrap();
    }

    // 3. Query: MATCH (a:User)-[e:Follows]->(b:User) WHERE b.name = 'Alice' RETURN a.name
    let query = "MATCH (a:User)-[e:Follows]->(b:User) WHERE b.name = 'Alice' RETURN a.name";
    let conn = db.connect();
    let query_result = conn.query(query).unwrap();

    let mut results = Vec::new();
    for batch in query_result.batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..col.len() {
            results.push(col.value(i).to_string());
        }
    }

    // Results should be "Bob" and "David"
    results.sort();
    assert_eq!(results, vec!["Bob", "David"]);
}
