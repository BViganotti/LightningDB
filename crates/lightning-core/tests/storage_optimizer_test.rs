use arrow::array::{Array, Int64Array, StringArray};
use lightning_core::{Database, SystemConfig};
use tempfile::tempdir;

#[test]
fn test_storage_compression_rewrite_roundtrip() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // 1. Create table and insert highly compressible data (repeated values)
    // 1. Setup Catalog and Storage
    {
        let mut catalog = db.catalog.write();
        catalog.add_node_table(
            "Person".into(),
            vec![
                lightning_core::catalog::PropertyDefinition {
                    name: "id".into(),
                    type_: lightning_types::LogicalType::Int64,
                },
                lightning_core::catalog::PropertyDefinition {
                    name: "age".into(),
                    type_: lightning_types::LogicalType::Int64,
                },
                lightning_core::catalog::PropertyDefinition {
                    name: "dept".into(),
                    type_: lightning_types::LogicalType::String,
                },
            ],
            None,
        );

        let mut storage = db.storage_manager.write();
        storage
            .create_table(
                "Person".into(),
                vec![
                    ("id".into(), lightning_types::LogicalType::Int64),
                    ("age".into(), lightning_types::LogicalType::Int64),
                    ("dept".into(), lightning_types::LogicalType::String),
                ],
                false,
            )
            .unwrap();
    }

    // 1000 rows with same department and same age to trigger RLE/Constant/Bitpacking
    for i in 0..1000 {
        conn.query(&format!(
            "CREATE (p:Person {{id: {}, age: 25, dept: 'Engineering'}})",
            i
        ))
        .unwrap();
    }

    // 2. Run optimization manual
    {
        let mut storage = db.storage_manager.write();
        let table = storage.node_tables.get_mut("Person").unwrap();
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).unwrap();
        table.optimize(bm, &tx).unwrap();
        db.transaction_manager.commit(&tx, bm, &db).unwrap();
    }

    // 3. Verify compression detected
    {
        let storage = db.storage_manager.read();
        let table = storage.node_tables.get("Person").unwrap();
        // Check "age" column (index 2, 0 is _id, 1 is id, 2 is age)
        let age_col = &table.columns[2];
        let stats = age_col.stats.read();
        let meta = stats
            .compression_meta
            .as_ref()
            .expect("Should have compression meta");
        println!("Detected compression for age: {:?}", meta.compression);
        // It should be Constant or RLE for age=25
    }

    // 4. Query data back to ensure round-trip correctness
    let result = conn
        .query("MATCH (p:Person) WHERE p.id = 500 RETURN p.age, p.dept")
        .unwrap();
    assert!(result.is_success());
    let batches = result.batches;
    let age_arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(age_arr.value(0), 25);
    let dept_arr = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(dept_arr.value(0), "Engineering");
}
