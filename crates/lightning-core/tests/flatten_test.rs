use lightning_core::catalog::PropertyDefinition;
use lightning_core::planner::logical_plan::LogicalOperator;
use lightning_core::processor::physical_plan::PhysicalPlanner;
use lightning_core::processor::DataChunk;
use lightning_core::processor::Value;
use lightning_core::Database;
use lightning_core::Result;
use lightning_core::SystemConfig;
use std::sync::Arc;

#[test]
fn test_flatten_operator() -> Result<()> {
    let test_dir = std::env::temp_dir().join("lightning_test_flatten_final");
    if test_dir.exists() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    std::fs::create_dir_all(&test_dir).unwrap();

    let db = Database::new(&test_dir, SystemConfig::default())?;
    let sm = db.storage_manager.clone();

    // Setup: 1 table with 3 rows
    {
        let tx = db.transaction_manager.begin(false)?;
        // 1. Add to Catalog
        {
            let mut cat = db.catalog.write();
            cat.add_node_table(
                "User".to_string(),
                vec![PropertyDefinition {
                    name: "name".to_string(),
                    type_: lightning_types::LogicalType::String,
                }],
                Some("name".to_string()),
            ).unwrap();
            cat.node_tables.get_mut("User").unwrap().num_rows = 3;
        }

        // 2. Create table in StorageManager
        let mut sm_write = sm.write();
        sm_write
            .create_table(
                "User".to_string(),
                vec![("name".to_string(), lightning_types::LogicalType::String)],
                false,
                None,
            )
            .unwrap();

        // 3. Append rows
        let table = sm_write.node_tables.get("User").unwrap();
        table
            .append_row(
                &db.buffer_manager,
                &[Value::Node(1), Value::String("Alice".to_string())],
                0,
                &tx,
            )
            .unwrap();
        table
            .append_row(
                &db.buffer_manager,
                &[Value::Node(2), Value::String("Bob".to_string())],
                1,
                &tx,
            )
            .unwrap();
        table
            .append_row(
                &db.buffer_manager,
                &[Value::Node(3), Value::String("Charlie".to_string())],
                2,
                &tx,
            )
            .unwrap();

        db.transaction_manager
            .commit(&tx, &db.buffer_manager, &db)?;
    }

    // Manually create a plan with Flatten
    // Scan(User) -> Flatten
    let scan = LogicalOperator::Scan("User".to_string(), "u".to_string(), None, None, None);
    let plan = LogicalOperator::Flatten(Box::new(scan));

    let tx = db.transaction_manager.begin(true)?;
    let undo_buffer = Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());
    let mut planner = PhysicalPlanner::new(db.clone(), tx.tx_id, tx.read_ts, undo_buffer);
    let mut physical_plan = planner.plan(plan)?;

    // Expected: 3 chunks, each with 1 row
    let mut count = 0;
    while let Some(chunk) = physical_plan.get_next(&db, &tx, None)? {
        assert_eq!(
            chunk.batch.num_rows(),
            1,
            "Chunk {} should have 1 row",
            count
        );
        count += 1;
    }
    assert_eq!(count, 3, "Should have returned 3 chunks");

    // Cleanup
    std::fs::remove_dir_all(&test_dir).unwrap();

    Ok(())
}
