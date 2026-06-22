use lightning_core::{Database, SystemConfig};
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

#[test]
fn test_committed_data_survives_clean_shutdown() -> TestResult {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    let config = SystemConfig::default();
    let db = Database::new(&db_path, config.clone())?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 100})", None)?;
    conn.execute("CREATE (:T {id: 2, val: 200})", None)?;

    db.checkpoint()?;
    drop(conn);
    drop(db);

    let db2 = Database::new(&db_path, config)?;
    let conn2 = db2.connect();
    let r = conn2.execute("MATCH (t:T) RETURN t.id, t.val ORDER BY t.id", None)?;
    let ids = r.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    let vals = r.batches[0].column(1)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
    assert_eq!(ids.len(), 2, "Both committed rows should survive");
    assert_eq!(ids.value(0), 1);
    assert_eq!(vals.value(0), 100);
    assert_eq!(ids.value(1), 2);
    assert_eq!(vals.value(1), 200);
    Ok(())
}

#[test]
fn test_wal_replay_with_corrupt_records() -> TestResult {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    let config = SystemConfig::default();
    let db = Database::new(&db_path, config)?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 100})", None)?;
    conn.execute("CREATE (:T {id: 2, val: 200})", None)?;

    drop(conn);
    drop(db);

    let wal_path = db_path.join("wal.ltng");
    if wal_path.exists() {
        let wal_data = std::fs::read(&wal_path).unwrap();
        if wal_data.len() > 20 {
            let mut modified = wal_data.clone();
            let corrupt_pos = 5 + 15;
            if corrupt_pos < modified.len() {
                modified[corrupt_pos] ^= 0xFF;
            }
            std::fs::write(&wal_path, &modified).unwrap();
        }
    }

    let db2 = Database::new(&db_path, SystemConfig::default())?;
    let conn2 = db2.connect();
    let r = conn2.execute("MATCH (t:T) RETURN count(*) as cnt", None)?;
    let count = r.batches[0].column(0)
        .as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
    assert_eq!(count, 2, "Valid records should survive WAL replay despite corruption");
    Ok(())
}

#[test]
fn test_wal_header_validated_on_open() -> TestResult {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    let db = Database::new(&db_path, SystemConfig::default())?;
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE (:T {id: 1, val: 100})", None)?;
    drop(conn);
    drop(db);

    let wal_path = db_path.join("wal.ltng");
    if wal_path.exists() {
        let mut wal_data = std::fs::read(&wal_path).unwrap();
        if wal_data.len() >= 5 {
            wal_data[0] ^= 0xFF;
            std::fs::write(&wal_path, &wal_data).unwrap();
        }
    }

    let result = Database::new(&db_path, SystemConfig::default());
    assert!(result.is_err(), "Should reject WAL with corrupted header magic");
    if let Err(e) = result {
        let msg = format!("{}", e);
        assert!(msg.contains("LNIW"), "Error should mention expected magic: {}", msg);
    }
    Ok(())
}

#[test]
fn test_rolled_back_data_not_leaked_to_data_files() -> TestResult {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    let config = SystemConfig::default();
    let db = Database::new(&db_path, config.clone())?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE T(id INT64, val INT64, PRIMARY KEY (id))", None)?;

    conn.begin()?;
    conn.execute("CREATE (:T {id: 1, val: 10})", None)?;
    conn.execute("CREATE (:T {id: 2, val: 20})", None)?;
    conn.rollback()?;

    // After rollback, the data is still in the WAL but uncommitted.
    // The next checkpoint should NOT write uncommitted pages to data files.
    // Verify by checking data file sizes directly before and after checkpoint.

    // Get data file paths
    let entries = std::fs::read_dir(&db_path).unwrap();
    let data_files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("data_") && name.ends_with(".bin")
        })
        .collect();

    let sizes_before: Vec<(String, u64)> = data_files
        .iter()
        .map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let size = e.metadata().map(|m| m.len()).unwrap_or(0);
            (name, size)
        })
        .collect();

    // Force a checkpoint which should skip uncommitted pages
    db.checkpoint()?;

    // Check data file sizes after checkpoint — they should NOT have grown
    // (uncommitted pages should never be flushed)
    for (name, size_before) in &sizes_before {
        let path = db_path.join(name);
        let size_after = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        assert!(
            size_after <= *size_before,
            "Data file '{}' grew from {} to {} after checkpoint with only uncommitted data. \
             UNCOMMITTED_BIT check in checkpoint is not working.",
            name,
            size_before,
            size_after
        );
    }

    drop(conn);
    drop(db);

    Ok(())
}
