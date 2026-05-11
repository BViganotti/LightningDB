use lightning_core::Database;
use lightning_core::SystemConfig;
use tempfile::tempdir;

#[test]
#[ignore]
fn test_constant_compression_integration() {
    let dir = tempdir().unwrap();
    let config = SystemConfig::default();
    let db = Database::new(dir.path(), config).unwrap();
    
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE User(age INT64)",
        None,
    ).unwrap();
    // Insert 100 rows with same age
    for _ in 0..100 {
        conn.query("CREATE (:User {age: 25})").unwrap();
    }

    // Optimize table to trigger compression detection
    {
        let sm = db.storage_manager.read();
        let table = sm.get_table("User").unwrap();
        // table.optimize(&db.buffer_manager).unwrap();
    }

    // Verify compression meta is Constant
    {
        let sm = db.storage_manager.read();
        let table = sm.get_table("User").unwrap();
        let age_col = &table.columns[1]; // 0 is _id
        let stats = age_col.stats.read();
        assert!(stats.compression_meta.is_some());
        let meta = stats.compression_meta.as_ref().unwrap();
        assert_eq!(meta.compression, lightning_core::storage::compression::CompressionType::Constant);
    }

    // Query back to ensure transparency
    let res = conn.query("MATCH (u:User) RETURN u.age").unwrap();
    assert!(res.is_success());
    assert_eq!(res.batches[0].num_rows(), 100);
}
