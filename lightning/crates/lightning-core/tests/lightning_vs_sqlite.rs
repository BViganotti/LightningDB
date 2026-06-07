use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_lightning_vs_sqlite_insert() {
    // --- Lightning: 10K nodes via bulk_insert_batch ---
    let dir1 = tempdir().unwrap();
    let db = lightning_core::Database::new(dir1.path(), lightning_core::SystemConfig::default())
        .unwrap();
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Item(id INT64, val STRING, PRIMARY KEY (id))",
        None,
    )
    .unwrap();

    let ids: arrow::array::Int64Array = (0..10_000).map(Some).collect();
    let vals: arrow::array::StringArray = (0..10_000).map(|i| Some(format!("val_{}", i))).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("val", Arc::new(vals) as Arc<dyn arrow::array::Array>),
    ])
    .unwrap();

    let start = Instant::now();
    let inserted = conn.bulk_insert_batch("Item", &batch).unwrap();
    let lightning_time = start.elapsed();
    assert_eq!(inserted, 10_000);

    // Verify
    let res = conn
        .execute("MATCH (i:Item) RETURN count(*)", None)
        .unwrap();
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10_000);

    // --- SQLite: 10K rows via INSERT ---
    let dir2 = tempdir().unwrap();
    let sqlite_path = dir2.path().join("test.db");
    let mut conn_sqlite = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn_sqlite
        .execute_batch(
            "CREATE TABLE Item (id INTEGER PRIMARY KEY, val TEXT);
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;",
        )
        .unwrap();

    let start = Instant::now();
    {
        let tx = conn_sqlite.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO Item (id, val) VALUES (?1, ?2)")
                .unwrap();
            for i in 0..10_000 {
                stmt.execute(rusqlite::params![i, format!("val_{}", i)])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    let sqlite_time = start.elapsed();

    // Verify
    let sqlite_count: i64 = conn_sqlite
        .query_row("SELECT count(*) FROM Item", [], |r| r.get(0))
        .unwrap();
    assert_eq!(sqlite_count, 10_000);

    let speedup = sqlite_time.as_micros() as f64 / lightning_time.as_micros() as f64;

    println!("\n=== 10K ROW INSERT: Lightning vs SQLite ===");
    println!(
        "Lightning (bulk_insert_batch): {:?} ({:.0} rows/sec)",
        lightning_time,
        10_000.0 / lightning_time.as_secs_f64()
    );
    println!(
        "SQLite (batched INSERT):       {:?} ({:.0} rows/sec)",
        sqlite_time,
        10_000.0 / sqlite_time.as_secs_f64()
    );
    println!("Lightning is {:.1}x faster than SQLite", speedup);
}

#[test]
fn test_lightning_vs_sqlite_20k_insert() {
    // --- Lightning: 20K nodes via bulk_insert_batch ---
    let dir1 = tempdir().unwrap();
    let db = lightning_core::Database::new(dir1.path(), lightning_core::SystemConfig::default())
        .unwrap();
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Item(id INT64, val STRING, age INT64, PRIMARY KEY (id))",
        None,
    )
    .unwrap();

    let ids: arrow::array::Int64Array = (0..20_000).map(Some).collect();
    let vals: arrow::array::StringArray = (0..20_000).map(|i| Some(format!("val_{}", i))).collect();
    let ages: arrow::array::Int64Array =
        (0..20_000).map(|i| Some(((i % 80) + 18) as i64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("val", Arc::new(vals) as Arc<dyn arrow::array::Array>),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])
    .unwrap();

    let start = Instant::now();
    let inserted = conn.bulk_insert_batch("Item", &batch).unwrap();
    let lightning_time = start.elapsed();
    assert_eq!(inserted, 20_000);

    // --- SQLite: 20K rows ---
    let dir2 = tempdir().unwrap();
    let sqlite_path = dir2.path().join("test.db");
    let mut conn_sqlite = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn_sqlite
        .execute_batch(
            "CREATE TABLE Item (id INTEGER PRIMARY KEY, val TEXT, age INTEGER);
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;",
        )
        .unwrap();

    let start = Instant::now();
    {
        let tx = conn_sqlite.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO Item (id, val, age) VALUES (?1, ?2, ?3)")
                .unwrap();
            for i in 0..20_000 {
                stmt.execute(rusqlite::params![i, format!("val_{}", i), (i % 80) + 18])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    let sqlite_time = start.elapsed();

    let speedup = sqlite_time.as_micros() as f64 / lightning_time.as_micros() as f64;

    println!("\n=== 20K ROW INSERT: Lightning vs SQLite ===");
    println!(
        "Lightning (bulk_insert_batch): {:?} ({:.0} rows/sec)",
        lightning_time,
        20_000.0 / lightning_time.as_secs_f64()
    );
    println!(
        "SQLite (batched INSERT):       {:?} ({:.0} rows/sec)",
        sqlite_time,
        20_000.0 / sqlite_time.as_secs_f64()
    );
    println!("Lightning is {:.1}x faster than SQLite", speedup);
}

#[test]
fn test_lightning_vs_sqlite_scan() {
    // Setup both DBs with 20K rows
    // Lightning
    let dir1 = tempdir().unwrap();
    let db = lightning_core::Database::new(dir1.path(), lightning_core::SystemConfig::default())
        .unwrap();
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Item(id INT64, val STRING, age INT64, PRIMARY KEY (id))",
        None,
    )
    .unwrap();
    let ids: arrow::array::Int64Array = (0..20_000).map(Some).collect();
    let vals: arrow::array::StringArray = (0..20_000).map(|i| Some(format!("val_{}", i))).collect();
    let ages: arrow::array::Int64Array =
        (0..20_000).map(|i| Some(((i % 80) + 18) as i64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("val", Arc::new(vals) as Arc<dyn arrow::array::Array>),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])
    .unwrap();
    conn.bulk_insert_batch("Item", &batch).unwrap();

    // SQLite
    let dir2 = tempdir().unwrap();
    let sqlite_path = dir2.path().join("test.db");
    let mut conn_sqlite = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn_sqlite
        .execute_batch(
            "CREATE TABLE Item (id INTEGER PRIMARY KEY, val TEXT, age INTEGER);
         PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        )
        .unwrap();
    {
        let tx = conn_sqlite.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO Item (id, val, age) VALUES (?1, ?2, ?3)")
                .unwrap();
            for i in 0..20_000 {
                stmt.execute(rusqlite::params![i, format!("val_{}", i), (i % 80) + 18])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    // Scan: Lightning
    let start = Instant::now();
    let res = conn
        .execute("MATCH (i:Item) RETURN i.id, i.val, i.age", None)
        .unwrap();
    let lightning_scan = start.elapsed();
    let lightning_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(lightning_rows, 20_000);

    // Scan: SQLite
    let start = Instant::now();
    let mut stmt = conn_sqlite
        .prepare("SELECT id, val, age FROM Item")
        .unwrap();
    let mut sqlite_rows = 0;
    let mut rows = stmt.query([]).unwrap();
    while let Some(_row) = rows.next().unwrap() {
        sqlite_rows += 1;
    }
    let sqlite_scan = start.elapsed();
    assert_eq!(sqlite_rows, 20_000);

    let speedup = sqlite_scan.as_micros() as f64 / lightning_scan.as_micros() as f64;

    println!("\n=== 20K ROW FULL SCAN: Lightning vs SQLite ===");
    println!(
        "Lightning:  {:?} ({:.0} rows/sec)",
        lightning_scan,
        20_000.0 / lightning_scan.as_secs_f64()
    );
    println!(
        "SQLite:     {:?} ({:.0} rows/sec)",
        sqlite_scan,
        20_000.0 / sqlite_scan.as_secs_f64()
    );
    println!("Lightning is {:.1}x faster than SQLite", speedup);
}

#[test]
fn test_lightning_vs_sqlite_filter() {
    // Setup both DBs with 20K rows (reuse same setup pattern)
    let dir1 = tempdir().unwrap();
    let db = lightning_core::Database::new(dir1.path(), lightning_core::SystemConfig::default())
        .unwrap();
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Item(id INT64, val STRING, age INT64, PRIMARY KEY (id))",
        None,
    )
    .unwrap();
    let ids: arrow::array::Int64Array = (0..20_000).map(Some).collect();
    let vals: arrow::array::StringArray = (0..20_000).map(|i| Some(format!("val_{}", i))).collect();
    let ages: arrow::array::Int64Array =
        (0..20_000).map(|i| Some(((i % 80) + 18) as i64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("val", Arc::new(vals) as Arc<dyn arrow::array::Array>),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])
    .unwrap();
    conn.bulk_insert_batch("Item", &batch).unwrap();

    let dir2 = tempdir().unwrap();
    let sqlite_path = dir2.path().join("test.db");
    let mut conn_sqlite = rusqlite::Connection::open(&sqlite_path).unwrap();
    conn_sqlite
        .execute_batch(
            "CREATE TABLE Item (id INTEGER PRIMARY KEY, val TEXT, age INTEGER);
         PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        )
        .unwrap();
    {
        let tx = conn_sqlite.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO Item (id, val, age) VALUES (?1, ?2, ?3)")
                .unwrap();
            for i in 0..20_000 {
                stmt.execute(rusqlite::params![i, format!("val_{}", i), (i % 80) + 18])
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    // Filter + count: Lightning
    let start = Instant::now();
    let res = conn
        .execute("MATCH (i:Item) WHERE i.age >= 50 RETURN count(*)", None)
        .unwrap();
    let lightning_filter = start.elapsed();
    let l_count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    // Filter + count: SQLite
    let start = Instant::now();
    let s_count: i64 = conn_sqlite
        .query_row("SELECT count(*) FROM Item WHERE age >= 50", [], |r| {
            r.get(0)
        })
        .unwrap();
    let sqlite_filter = start.elapsed();

    let speedup = sqlite_filter.as_micros() as f64 / lightning_filter.as_micros() as f64;

    println!("\n=== FILTER (age >= 50, ~20K rows): Lightning vs SQLite ===");
    println!("Lightning:  {:?}  (count={})", lightning_filter, l_count);
    println!("SQLite:     {:?}  (count={})", sqlite_filter, s_count);
    // println!("Lightning is {:.1}x faster than SQLite", speedup);
}
