use lightning_core::processor::Value;
use lightning_core::Database;
use lightning_core::SystemConfig;
use tempfile::tempdir;

/// Read back (file_path, hash, workspace_id) for a FileHash node table using the
/// connection-level query API. Returns one tuple per row.
fn read_file_hashes(db: &std::sync::Arc<Database>) -> Vec<(String, String, String)> {
    let conn = db.connect();
    let res = conn
        .query("MATCH (f:FileHash) RETURN f.file_path, f.hash, f.workspace_id")
        .unwrap();
    let mut out = Vec::new();
    for batch in &res.batches {
        let c0 = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let c1 = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let c2 = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            out.push((
                c0.value(i).to_string(),
                c1.value(i).to_string(),
                c2.value(i).to_string(),
            ));
        }
    }
    out
}

/// Regression test for the MERGE vanishing-write bug: PhysicalMerge previously
/// appended rows through a plan-time Table clone whose write_buffer was private
/// and never flushed, so MERGE-created nodes had all-empty property columns on
/// readback. This test asserts the actual stored VALUES (not just row counts).
#[test]
fn test_merge_persists_property_values() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();

    {
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE FileHash (file_path STRING, hash STRING, workspace_id STRING, PRIMARY KEY (file_path))",
            None,
        )
        .unwrap();
    }

    let merge_upsert = "MERGE (n:FileHash {file_path: $path}) \
                        ON CREATE SET n.hash = $hash, n.workspace_id = $ws \
                        ON MATCH SET n.hash = $hash, n.workspace_id = $ws";

    // Insert path: first MERGE creates the node.
    {
        let conn = db.connect();
        let mut params = std::collections::HashMap::new();
        params.insert("path".to_string(), Value::String("src/a.rs".to_string()));
        params.insert("hash".to_string(), Value::String("HASH_V1".to_string()));
        params.insert("ws".to_string(), Value::String("ws-1".to_string()));
        conn.execute(merge_upsert, Some(params)).unwrap();
    }

    let rows = read_file_hashes(&db);
    assert_eq!(rows.len(), 1, "one row after first MERGE, got {rows:?}");
    assert_eq!(
        rows[0],
        ("src/a.rs".to_string(), "HASH_V1".to_string(), "ws-1".to_string()),
        "MERGE-created node must persist the pattern PK and ON CREATE SET values"
    );

    // Update path: MERGE the same PK with a new hash updates in place.
    {
        let conn = db.connect();
        let mut params = std::collections::HashMap::new();
        params.insert("path".to_string(), Value::String("src/a.rs".to_string()));
        params.insert("hash".to_string(), Value::String("HASH_V2".to_string()));
        params.insert("ws".to_string(), Value::String("ws-1".to_string()));
        conn.execute(merge_upsert, Some(params)).unwrap();
    }

    let rows = read_file_hashes(&db);
    assert_eq!(rows.len(), 1, "still one row after re-MERGE (no duplicate), got {rows:?}");
    assert_eq!(
        rows[0].1, "HASH_V2",
        "ON MATCH SET must update the existing node's hash in place, got {rows:?}"
    );
}

/// Count nodes in a table via the connection API (`MATCH ... RETURN count`).
fn match_count(db: &std::sync::Arc<Database>, table: &str) -> i64 {
    let conn = db.connect();
    let res = conn
        .query(&format!("MATCH (n:{table}) RETURN count(n)"))
        .unwrap();
    res.batches
        .first()
        .and_then(|b| {
            let col = b.column(0);
            if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                Some(a.value(0))
            } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                Some(a.value(0) as i64)
            } else {
                col.as_any().downcast_ref::<arrow::array::Float64Array>().map(|a| a.value(0) as i64)
            }
        })
        .expect("count(n) should return a row")
}

/// Read a Person node's name by id.
fn person_name(db: &std::sync::Arc<Database>, id: i64) -> Option<String> {
    let conn = db.connect();
    let res = conn
        .query(&format!("MATCH (n:Person {{id: {id}}}) RETURN n.name"))
        .unwrap();
    res.batches.first().and_then(|b| {
        if b.num_rows() == 0 {
            return None;
        }
        b.column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .map(|a| a.value(0).to_string())
    })
}

#[test]
fn test_merge_basic() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path().to_path_buf(), SystemConfig::default()).unwrap();

    {
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))",
            None,
        )
        .unwrap();
        conn.execute("MERGE (n:Person {id: 1, name: 'Alice'})", None).unwrap();
    }

    // First MERGE creates Alice.
    assert_eq!(match_count(&db, "Person"), 1, "first MERGE creates one node");
    assert_eq!(person_name(&db, 1).as_deref(), Some("Alice"));

    // Second identical MERGE matches instead of duplicating.
    {
        let conn = db.connect();
        conn.execute("MERGE (n:Person {id: 1, name: 'Alice'})", None).unwrap();
    }
    assert_eq!(
        match_count(&db, "Person"),
        1,
        "second identical MERGE must not create a duplicate"
    );
    assert_eq!(person_name(&db, 1).as_deref(), Some("Alice"));
}

/// Read a Person node's (created, matched) booleans by id.
fn person_flags(db: &std::sync::Arc<Database>, id: i64) -> Option<(bool, bool)> {
    let conn = db.connect();
    let res = conn
        .query(&format!("MATCH (n:Person {{id: {id}}}) RETURN n.created, n.matched"))
        .unwrap();
    res.batches.first().and_then(|b| {
        if b.num_rows() == 0 {
            return None;
        }
        let created = b.column(0).as_any().downcast_ref::<arrow::array::BooleanArray>()?;
        let matched = b.column(1).as_any().downcast_ref::<arrow::array::BooleanArray>()?;
        Some((created.value(0), matched.value(0)))
    })
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
        // First MERGE creates the node and sets created = TRUE.
        conn.execute(
            "MERGE (n:Person {id: 1, name: 'Alice'}) ON CREATE SET n.created = TRUE",
            None,
        )
        .unwrap();
    }

    assert_eq!(match_count(&db, "Person"), 1, "ON CREATE MERGE creates one node");
    let (created, matched) = person_flags(&db, 1).expect("node exists");
    assert!(created, "created must be TRUE after ON CREATE SET");
    assert!(!matched, "matched must remain FALSE before any ON MATCH SET");

    {
        let conn = db.connect();
        // Second MERGE on the same PK matches and sets matched = TRUE.
        conn.execute(
            "MERGE (n:Person {id: 1, name: 'Alice'}) ON MATCH SET n.matched = TRUE",
            None,
        )
        .unwrap();
    }

    assert_eq!(match_count(&db, "Person"), 1, "ON MATCH MERGE must not duplicate");
    let (created, matched) = person_flags(&db, 1).expect("node exists");
    assert!(created, "created stays TRUE from the create");
    assert!(matched, "matched must be TRUE after ON MATCH SET");
}
