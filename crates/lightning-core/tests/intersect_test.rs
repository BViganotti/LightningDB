use arrow::array::UInt64Array;
use lightning_core::processor::DataChunk;
use lightning_core::processor::PhysicalOperator;
use lightning_core::processor::Value;
use lightning_core::Database;
use lightning_types::LogicalType;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_intersect_operator() {
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

        // Manual catalog update for tests
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

        // Users
        user_table
            .append_row(bm, &[Value::String("Alice".to_string())], 0, &tx)
            .unwrap();
        user_table
            .append_row(bm, &[Value::String("Bob".to_string())], 1, &tx)
            .unwrap();
        user_table
            .append_row(bm, &[Value::String("Charlie".to_string())], 2, &tx)
            .unwrap();
        user_table
            .append_row(bm, &[Value::String("David".to_string())], 3, &tx)
            .unwrap();

        // Follows: (src, dst)
        // 0 -> 2, 0 -> 3
        // 1 -> 2, 1 -> 3, 1 -> 4
        follows_table
            .append_row(bm, &[Value::Node(0), Value::Node(2)], 0, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(0), Value::Node(3)], 1, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(1), Value::Node(2)], 2, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(1), Value::Node(3)], 3, &tx)
            .unwrap();
        follows_table
            .append_row(bm, &[Value::Node(1), Value::Node(4)], 4, &tx)
            .unwrap();

        db.transaction_manager.commit(&tx, bm, &db).unwrap();
    }

    // 3. Test Physical Intersect directly
    use lightning_core::processor::operators::PhysicalIntersect;
    use lightning_core::processor::operators::PhysicalScan;

    let storage = db.storage_manager.read();
    let follows_table = storage.get_table("Follows").unwrap().clone();

    // Build 1: Scan Follows
    let scan1 = Box::new(PhysicalScan::new(
        follows_table.clone(),
        "e1".to_string(),
        db.buffer_manager.clone(),
        5,
        100,
    ));
    // Build 2: Scan Follows
    let scan2 = Box::new(PhysicalScan::new(
        follows_table,
        "e2".to_string(),
        db.buffer_manager.clone(),
        5,
        100,
    ));

    // Probe side: provides [(0, 1)] (one row, two columns: a=0, b=1)
    #[derive(Clone)]
    struct MockProbe {
        done: bool,
    }
    impl PhysicalOperator for MockProbe {
        fn clone_box(&self) -> Box<dyn PhysicalOperator> {
            Box::new((*self).clone())
        }
        fn get_next(
            &mut self,
            _database: &lightning_core::Database,
            _tx: &lightning_core::transaction::transaction_manager::Transaction,
            _params: Option<&std::collections::HashMap<String, lightning_core::processor::Value>>,
        ) -> lightning_core::Result<Option<DataChunk>> {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let a = Arc::new(UInt64Array::from(vec![0]));
            let b = Arc::new(UInt64Array::from(vec![1]));
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::UInt64, false),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::UInt64, false),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![a, b]).unwrap();
            Ok(Some(DataChunk { batch }))
        }
    }

    let probe = Box::new(MockProbe { done: false });

    // Intersect:
    // probe keys: [0, 1] (column indices in probe side)
    // build keys: [0, 0] (column indices in each build side, i.e., 'src')
    // build intersect indices: [1, 1] (column indices in each build side for the common var, i.e., 'dst')
    // intersect_var: 'c'

    let mut intersect = PhysicalIntersect::new(
        probe,
        vec![0, 1], // a and b
        vec![scan1, scan2],
        vec![0, 0], // src
        vec![1, 1], // dst
        "c".to_string(),
    );

    let tx = db.transaction_manager.begin(false).unwrap();
    let result = intersect
        .get_next(&db, &tx, None)
        .unwrap()
        .expect("Should have results");
    let batch = result.batch;

    // Schema should be [a, b, c]
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 2); // 2 and 3 are common followers

    let c_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let mut values: Vec<u64> = c_col.values().to_vec();
    values.sort();
    assert_eq!(values, vec![2, 3]);
}
