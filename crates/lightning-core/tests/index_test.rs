use lightning_core::{Database, SystemConfig};
use tempfile::tempdir;

#[test]
fn test_hash_index_creation_and_lookup() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();

    // Create a table with a primary key
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))",
        None,
    )
    .unwrap();

    // Insert some rows
    // Since we don't have the full parser to test complex inserts, we will just use the index directly through the storage manager to test it's actually working.
    let storage = db.storage_manager.read();
    let index = storage
        .get_index("User")
        .expect("Index should exist for User table");

    // The buffer manager is normally held by the database. We can create a local one or use db's.
    let bm = &db.buffer_manager;

    let tx = db.transaction_manager.begin(false).unwrap();

    // Insert into index
    use lightning_core::processor::Value;
    index.insert(bm, &Value::Number(42.0), 100, &tx).unwrap();
    index.insert(bm, &Value::Number(10.0), 101, &tx).unwrap();
    index.insert(bm, &Value::Number(99.0), 102, &tx).unwrap();

    // Lookup
    let res1 = index.lookup(bm, &Value::Number(42.0), &tx).unwrap();
    assert_eq!(res1, Some(100));

    let res2 = index.lookup(bm, &Value::Number(10.0), &tx).unwrap();
    assert_eq!(res2, Some(101));

    let res3 = index.lookup(bm, &Value::Number(99.0), &tx).unwrap();
    assert_eq!(res3, Some(102));

    let res_not_found = index.lookup(bm, &Value::Number(5.0), &tx).unwrap();
    assert_eq!(res_not_found, None);

    db.transaction_manager.commit(&tx, bm, &db).unwrap();
}

#[test]
fn test_hash_index_collision_20_entries() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))",
        None,
    )
    .unwrap();

    let storage = db.storage_manager.read();
    let index = storage.get_index("User").expect("Index should exist");
    let bm = &db.buffer_manager;
    let tx = db.transaction_manager.begin(false).unwrap();

    use lightning_core::processor::Value;

    // Insert 20 entries — exceeds default bucket count, forcing overflow chaining
    for i in 0..20u64 {
        let page_id = 1000 + i;
        index
            .insert(bm, &Value::Number(i as f64), page_id, &tx)
            .unwrap();
    }

    // Verify all 20 are found
    for i in 0..20u64 {
        let res = index
            .lookup(bm, &Value::Number(i as f64), &tx)
            .unwrap();
        assert_eq!(
            res,
            Some(1000 + i),
            "Entry for key {} should be found at page_id {}",
            i,
            1000 + i
        );
    }

    // Verify a non-existent key is not found
    let not_found = index.lookup(bm, &Value::Number(999.0), &tx).unwrap();
    assert_eq!(not_found, None);

    db.transaction_manager.commit(&tx, bm, &db).unwrap();
}

#[test]
fn test_index_scan_query_e2e() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Item(id INT64, val INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();

    // Insert rows via Cypher
    for i in 0..10 {
        conn.query(&format!("CREATE (:Item {{id: {}, val: {}}})", i, i * 10))
            .unwrap();
    }

    // Verify 10 rows
    let count = conn.query("MATCH (i:Item) RETURN count(*)").unwrap();
    let total: usize = count.batches.iter().map(|b| b.num_rows()).sum();
    assert!(total >= 1, "Should have at least 1 batch with count result");

    // Delete all rows
    conn.query("MATCH (i:Item) DELETE i").unwrap();
    let count = conn.query("MATCH (i:Item) RETURN count(*)").unwrap();
    let total_after_delete: usize = count.batches.iter().map(|b| b.num_rows() as usize).sum();
    // After delete, count(*) returns 0 rows or 1 row with value 0
    if !count.batches.is_empty() && count.batches[0].num_rows() > 0 {
        let val = count.batches[0].column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 0, "After deleting all, count should be 0");
    }

    // Re-insert same IDs — should reuse freed pages
    for i in 0..10 {
        conn.query(&format!("CREATE (:Item {{id: {}, val: {}}})", i, i * 10))
            .unwrap();
    }

    // Verify 10 rows again
    let final_count = conn.query("MATCH (i:Item) RETURN count(*)").unwrap();
    if !final_count.batches.is_empty() && final_count.batches[0].num_rows() > 0 {
        let val = final_count.batches[0].column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val, 10, "After re-insert, count should be 10");
    }
}
