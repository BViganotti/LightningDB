use lightning_core::parser::parse;
use lightning_core::planner::logical_plan::LogicalPlanner;
use lightning_core::planner::Binder;
use lightning_core::processor::physical_plan::PhysicalPlanner;
use lightning_core::Database;
use lightning_core::SystemConfig;
use tempfile::tempdir;

#[test]
fn test_merge_basic() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();

    // Create a node table
    {
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))",
            None,
        )
        .unwrap();
    }

    // 1. MERGE (n:Person {id: 1, name: 'Alice'}) - Should CREATE
    let query = "MERGE (n:Person {id: 1, name: 'Alice'}) RETURN count(*)";
    let statement = parse(query)
        .unwrap()
        .union_queries
        .into_iter()
        .next()
        .unwrap()
        .statement;

    let bound_statement = {
        let catalog = db.catalog.read();
        let mut binder = Binder::new(&catalog, &db.function_registry);
        binder.bind(&statement).unwrap()
    };

    let logical_plan = LogicalPlanner::plan(bound_statement).unwrap();
    let undo_buffer = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());

    let tx = db.transaction_manager.begin(false).unwrap();
    let mut physical_planner = PhysicalPlanner::new(db.clone(), tx.tx_id, tx.read_ts, undo_buffer);
    let mut physical_plan = physical_planner.plan(logical_plan).unwrap();

    let result = physical_plan.get_next(&db, &tx, None).unwrap().unwrap();
    let count = result
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1.0);

    db.transaction_manager
        .commit(&tx, &db.buffer_manager, &db)
        .unwrap();

    // Verify Alice exists
    {
        assert_eq!(
            db.catalog.read().get_node_table("Person").unwrap().num_rows,
            1
        );
    }

    // 2. MERGE (n:Person {id: 1, name: 'Alice'}) - Should MATCH (no change)
    let statement2 = parse(query)
        .unwrap()
        .union_queries
        .into_iter()
        .next()
        .unwrap()
        .statement;
    let bound_statement2 = {
        let catalog = db.catalog.read();
        let mut binder = Binder::new(&catalog, &db.function_registry);
        binder.bind(&statement2).unwrap()
    };
    let logical_plan2 = LogicalPlanner::plan(bound_statement2).unwrap();

    let tx2 = db.transaction_manager.begin(false).unwrap();
    let undo_buffer2 = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());
    let mut physical_planner2 =
        PhysicalPlanner::new(db.clone(), tx2.tx_id, tx2.read_ts, undo_buffer2);
    let mut physical_plan2 = physical_planner2.plan(logical_plan2).unwrap();

    let result2 = physical_plan2.get_next(&db, &tx2, None).unwrap().unwrap();
    let count2 = result2
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count2, 1.0);

    db.transaction_manager
        .commit(&tx2, &db.buffer_manager, &db)
        .unwrap();
    assert_eq!(
        db.catalog.read().get_node_table("Person").unwrap().num_rows,
        1
    );
}

#[test]
fn test_merge_on_create_on_match() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();

    {
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, created BOOL, matched BOOL, PRIMARY KEY(id))",
            None,
        )
        .unwrap();
    }

    // 1. MERGE ... ON CREATE SET created = TRUE
    let query_create =
        "MERGE (n:Person {id: 1, name: 'Alice'}) ON CREATE SET n.created = TRUE RETURN count(*)";
    let statement = parse(query_create)
        .unwrap()
        .union_queries
        .into_iter()
        .next()
        .unwrap()
        .statement;
    let bound_statement = {
        let catalog = db.catalog.read();
        let mut binder = Binder::new(&catalog, &db.function_registry);
        binder.bind(&statement).unwrap()
    };
    let logical_plan = LogicalPlanner::plan(bound_statement).unwrap();
    let undo_buffer = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());

    let tx = db.transaction_manager.begin(false).unwrap();
    let mut physical_planner = PhysicalPlanner::new(db.clone(), tx.tx_id, tx.read_ts, undo_buffer);
    let mut physical_plan = physical_planner.plan(logical_plan).unwrap();
    physical_plan.get_next(&db, &tx, None).unwrap();

    db.transaction_manager
        .commit(&tx, &db.buffer_manager, &db)
        .unwrap();

    // Verify created = TRUE
    {
        assert_eq!(
            db.catalog.read().get_node_table("Person").unwrap().num_rows,
            1
        );
    }

    // 2. MERGE ... ON MATCH SET matched = TRUE
    let query_match =
        "MERGE (n:Person {id: 1, name: 'Alice'}) ON MATCH SET n.matched = TRUE RETURN count(*)";
    let statement2 = parse(query_match)
        .unwrap()
        .union_queries
        .into_iter()
        .next()
        .unwrap()
        .statement;
    let bound_statement2 = {
        let catalog = db.catalog.read();
        let mut binder = Binder::new(&catalog, &db.function_registry);
        binder.bind(&statement2).unwrap()
    };
    let logical_plan2 = LogicalPlanner::plan(bound_statement2).unwrap();

    let tx2 = db.transaction_manager.begin(false).unwrap();
    let undo_buffer2 = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());
    let mut physical_planner2 =
        PhysicalPlanner::new(db.clone(), tx2.tx_id, tx2.read_ts, undo_buffer2);
    let mut physical_plan2 = physical_planner2.plan(logical_plan2).unwrap();
    let result2 = physical_plan2.get_next(&db, &tx2, None).unwrap().unwrap();
    let count2 = result2
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count2, 1.0);

    db.transaction_manager
        .commit(&tx2, &db.buffer_manager, &db)
        .unwrap();
}
