use arrow::array::{Float64Array, StringArray};
use lightning_core::{Database, Result, SystemConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_extreme_concurrency_stress() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;

    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Counter(id INT64, val DOUBLE, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute("CREATE (:Counter {id: 1, val: 0.0})", None)?;

    let num_threads = 4;
    let iterations = 5;

    let mut handles = vec![];
    for i in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let conn = db_clone.connect();
            for _iter in 0..iterations {
                if i % 2 == 0 {
                    let mut retries = 0;
                    loop {
                        let query = "MATCH (c:Counter) WHERE c.id = 1 SET c.val = c.val + 1";
                        match conn.execute(query, None) {
                            Ok(res) if res.success => {
                                break;
                            }
                            Err(e) if format!("{:?}", e).contains("Write-Write Conflict") => {
                                thread::sleep(Duration::from_millis(2));
                                retries += 1;
                            }
                            Err(e) => {
                                panic!("Unexpected error: {:?}", e);
                            }
                            _ => {}
                        }
                        if retries > 50 {
                            panic!("Too many retries");
                        }
                    }
                } else {
                    let _ = conn.execute("MATCH (c:Counter) WHERE c.id = 1 RETURN c.val", None);
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let conn = db.connect();
    let res = conn.execute("MATCH (c:Counter) WHERE c.id = 1 RETURN c.val", None)?;
    let val = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);

    assert_eq!(val, (num_threads / 2 * iterations) as f64);

    Ok(())
}

#[test]
fn test_transaction_isolation_snapshot() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;

    let conn1 = db.connect();
    conn1.execute(
        "CREATE NODE TABLE Account(id INT64, balance DOUBLE, PRIMARY KEY (id))",
        None,
    )?;
    conn1.execute("CREATE (:Account {id: 1, balance: 1000.0})", None)?;

    // Start a transaction in conn1
    conn1.execute("BEGIN", None)?;

    let conn2 = db.connect();
    // Update in conn2 (should create a new version)
    conn2.execute(
        "MATCH (a:Account) WHERE a.id = 1 SET a.balance = 2000.0",
        None,
    )?;

    // conn1 should still see 1000.0 because it's in a transaction started before the update
    let res = conn1.execute("MATCH (a:Account) WHERE a.id = 1 RETURN a.balance", None)?;
    let bal = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bal, 1000.0);

    conn1.execute("COMMIT", None)?;

    // Now conn1 should see 2000.0
    let res2 = conn1.execute("MATCH (a:Account) WHERE a.id = 1 RETURN a.balance", None)?;
    let bal2 = res2.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bal2, 2000.0);

    Ok(())
}

#[test]
fn test_crash_recovery_wal() -> Result<()> {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::new(&path, SystemConfig::default())?;
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE Data(id INT64, msg STRING, PRIMARY KEY (id))",
            None,
        )?;
        conn.execute("CREATE (:Data {id: 1, msg: 'Hello'})", None)?;
        conn.execute("CREATE (:Data {id: 2, msg: 'World'})", None)?;
        // No checkpoint here! WAL should have the data.
    }

    // Re-open the database. It should replay the WAL.
    {
        let db = Database::new(&path, SystemConfig::default())?;
        let conn = db.connect();
        let res = conn.execute("MATCH (d:Data) RETURN d.msg ORDER BY d.id", None)?;
        assert_eq!(res.batches.len(), 1);
        assert_eq!(res.batches[0].num_rows(), 2);
        let msg1 = res.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        let msg2 = res.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1);
        assert_eq!(msg1, "Hello");
        assert_eq!(msg2, "World");
    }

    Ok(())
}

#[test]
fn test_large_data_joins() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Users(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute(
        "CREATE NODE TABLE Posts(id INT64, author_id INT64, title STRING, PRIMARY KEY (id))",
        None,
    )?;

    // Insert data
    for i in 0..10 {
        let query = format!("CREATE (:Users {{id: {}, name: 'User{}'}})", i, i);
        eprintln!("DEBUG: Creating user {}: {}", i, query);
        conn.execute(&query, None)?;
    }
    for i in 0..10 {
        for j in 0..5 {
            conn.execute(
                &format!(
                    "CREATE (:Posts {{id: {}, author_id: {}, title: 'Post{}-{}'}})",
                    i * 10 + j,
                    i,
                    i,
                    j
                ),
                None,
            )?;
        }
    }

    // Verify data integrity
    let user_count_res = conn.execute("MATCH (u:Users) RETURN count(*)", None)?;
    let user_count = user_count_res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(user_count, 10, "Expected 10 users");

    let post_count_res = conn.execute("MATCH (p:Posts) RETURN count(*)", None)?;
    let post_count = post_count_res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(post_count, 50, "Expected 50 posts");

    // Verify user names
    let users_res = conn.execute("MATCH (u:Users) RETURN u.id, u.name ORDER BY u.id", None)?;
    eprintln!("DEBUG: Users batches: {}", users_res.batches.len());
    if !users_res.batches.is_empty() {
        let batch = &users_res.batches[0];
        eprintln!(
            "DEBUG: Users batch has {} columns, {} rows",
            batch.num_columns(),
            batch.num_rows()
        );
        for i in 0..batch.num_columns() {
            eprintln!(
                "DEBUG: Column {} type: {:?}",
                i,
                batch.column(i).data_type()
            );
        }
    }
    eprintln!("DEBUG: Verifying user names:");
    for batch in &users_res.batches {
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for j in 0..batch.num_rows() {
            let name = names.value(j);
            eprintln!("  User row {}: name='{}'", j, name);
        }
    }

    // Correct join syntax for nodes: comma-separated MATCH
    let res = conn.execute(
        "MATCH (u:Users), (p:Posts) WHERE u.id = p.author_id RETURN u.name, p.title ORDER BY u.name, p.title",
        None,
    )?;
    assert!(res.success);
    let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();

    if total_rows != 50 {
        eprintln!("DEBUG: Expected 50 rows but got {}", total_rows);

        // Count posts per user
        let mut user_post_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for batch in &res.batches {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for j in 0..batch.num_rows() {
                *user_post_counts
                    .entry(names.value(j).to_string())
                    .or_insert(0) += 1;
            }
        }
        eprintln!("DEBUG: Posts per user: {:?}", user_post_counts);

        // Check individual tables
        let users = conn.execute("MATCH (u:Users) RETURN u.id, u.name", None)?;
        eprintln!("DEBUG: Users query has {} batches", users.batches.len());
        if !users.batches.is_empty() {
            eprintln!(
                "DEBUG: Users batch 0 has {} rows",
                users.batches[0].num_rows()
            );
        }
    }

    assert_eq!(total_rows, 50);

    Ok(())
}
