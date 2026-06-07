use arrow::array::StringArray;
use lightning_core::{Database, Result, SystemConfig, Value};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn create_person_table(conn: &lightning_core::Connection) -> Result<lightning_core::QueryResult> {
    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, content STRING, age INT64, PRIMARY KEY (id))",
        None,
    )
}

fn bulk_insert_names(
    conn: &lightning_core::Connection,
    count: usize,
    name_prefix: &str,
) -> Result<usize> {
    let ids: arrow::array::Int64Array = (0..count as i64).map(Some).collect();
    let names: StringArray = (0..count)
        .map(|i| Some(format!("{}{}", name_prefix, i)))
        .collect();
    let contents: StringArray = (0..count)
        .map(|i| Some(format!("content_{}_some_text_here_with_common_words", i)))
        .collect();
    let ages: arrow::array::Int64Array = (0..count as i64).map(Some).collect();

    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names) as Arc<dyn arrow::array::Array>),
        (
            "content",
            Arc::new(contents) as Arc<dyn arrow::array::Array>,
        ),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])?;

    conn.bulk_insert_batch("Person", &batch)?;
    Ok(count)
}

#[test]
fn test_insert_100_nodes() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;

    let start = Instant::now();
    for i in 0..100 {
        conn.execute(
            &format!("CREATE (:Person {{id: {}, name: 'Person{}'}})", i, i),
            None,
        )?;
    }
    let elapsed = start.elapsed();
    println!("\n=== 100 nodes (per-statement autocommit) ===");
    println!("Total:  {:?}", elapsed);
    println!(
        "Per node: {:.2} ms",
        elapsed.as_micros() as f64 / 100.0 / 1000.0
    );
    println!("Rate:   {:.0} nodes/sec", 100.0 / elapsed.as_secs_f64());

    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 100);
    Ok(())
}

#[test]
fn test_insert_1000_nodes() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;

    let start = Instant::now();
    for i in 0..1000 {
        conn.execute(
            &format!("CREATE (:Person {{id: {}, name: 'Person{}'}})", i, i),
            None,
        )?;
    }
    let elapsed = start.elapsed();
    println!("\n=== 1000 nodes (per-statement autocommit) ===");
    println!("Total:  {:?}", elapsed);
    println!(
        "Per node: {:.2} ms",
        elapsed.as_micros() as f64 / 1000.0 / 1000.0
    );
    println!("Rate:   {:.0} nodes/sec", 1000.0 / elapsed.as_secs_f64());

    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1000);
    Ok(())
}

#[test]
fn test_bulk_insert_via_api() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;

    // Build Arrow batch directly
    let ids: arrow::array::Int64Array = (0..10_000).map(Some).collect();
    let names: arrow::array::StringArray =
        (0..10_000).map(|i| Some(format!("Person{}", i))).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names) as Arc<dyn arrow::array::Array>),
    ])?;

    let start = Instant::now();
    let inserted = conn.bulk_insert_batch("Person", &batch)?;
    let elapsed = start.elapsed();
    println!("\n=== 10K nodes (bulk_insert_batch API) ===");
    println!("Inserted: {}", inserted);
    println!("Total:    {:?}", elapsed);
    println!("Per node: {:.2} μs", elapsed.as_micros() as f64 / 10_000.0);
    println!(
        "Rate:     {:.0} nodes/sec",
        10_000.0 / elapsed.as_secs_f64()
    );

    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10_000);
    Ok(())
}

#[test]
fn test_scan_and_query_performance() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY (id))",
        None,
    )?;

    // Use bulk insert for 20K nodes
    let ids: arrow::array::Int64Array = (0..20_000).map(Some).collect();
    let names: arrow::array::StringArray =
        (0..20_000).map(|i| Some(format!("Person{}", i))).collect();
    let ages: arrow::array::Int64Array =
        (0..20_000).map(|i| Some(((i % 80) + 18) as i64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names) as Arc<dyn arrow::array::Array>),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])?;

    let _ = conn.bulk_insert_batch("Person", &batch)?;

    // 1. Full table scan
    let start = Instant::now();
    let res = conn.execute("MATCH (p:Person) RETURN p.id, p.name, p.age", None)?;
    let scan_time = start.elapsed();
    let total_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    println!("\n=== FULL TABLE SCAN (20K rows) ===");
    println!("Rows returned: {}", total_rows);
    println!("Total time:    {:?}", scan_time);
    println!(
        "Rate:          {:.0} rows/sec",
        20_000.0 / scan_time.as_secs_f64()
    );
    assert_eq!(total_rows, 20_000);

    // 2. PK lookup (1000 iterations)
    let start = Instant::now();
    for i in 0..1000 {
        let id = (i * 19) % 20_000;
        let _ = conn.execute(
            &format!("MATCH (p:Person) WHERE p.id = {} RETURN p.name", id),
            None,
        );
    }
    let pk_time = start.elapsed();
    println!("\n=== PK LOOKUP (1000 iterations) ===");
    println!("Total time:    {:?}", pk_time);
    println!(
        "Per lookup:    {:.2} μs",
        pk_time.as_micros() as f64 / 1000.0
    );

    // 3. Filter + aggregation
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.age >= 30 RETURN count(*), avg(p.age)",
        None,
    )?;
    let filter_time = start.elapsed();
    println!("\n=== FILTER + AGG (age >= 30) ===");
    println!("Total time:    {:?}", filter_time);

    // 4. ORDER BY + LIMIT
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 100",
        None,
    )?;
    let order_time = start.elapsed();
    let order_rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    println!("\n=== ORDER BY + LIMIT 100 ===");
    println!("Rows returned: {}", order_rows);
    println!("Total time:    {:?}", order_time);

    Ok(())
}

#[test]
fn test_contains_query_performance() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    create_person_table(&conn)?;

    // Bulk insert 20K nodes with varied content
    let ids: arrow::array::Int64Array = (0..20_000i64).map(Some).collect();
    let names: StringArray = (0..20_000).map(|i| Some(format!("Person_{}", i))).collect();
    let contents: StringArray = (0..20_000)
        .map(|i| {
            if i % 3 == 0 {
                Some(format!("test_data_{}_compression_utils_text", i))
            } else if i % 5 == 0 {
                Some(format!("parser_code_{}_module_info", i))
            } else {
                Some(format!("node_content_{}", i))
            }
        })
        .collect();
    let ages: arrow::array::Int64Array = (0..20_000i64).map(Some).collect();

    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names) as Arc<dyn arrow::array::Array>),
        (
            "content",
            Arc::new(contents) as Arc<dyn arrow::array::Array>,
        ),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])?;

    let start = Instant::now();
    let _ = conn.bulk_insert_batch("Person", &batch)?;
    let insert_time = start.elapsed();
    println!("\n=== TRIGRAM INDEX BENCHMARK (20K nodes with async indexing) ===");
    println!("Bulk insert time: {:?}", insert_time);
    println!(
        "Insert rate: {:.0} nodes/sec",
        20_000.0 / insert_time.as_secs_f64()
    );

    // Short pattern - should skip index due to short pattern penalty
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.content CONTAINS 'e_' RETURN count(*)",
        None,
    )?;
    let short_time = start.elapsed();
    let count: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("\n--- SHORT PATTERN (2 chars: 'e_') - index skipped ---");
    println!("Results: {}", count);
    println!("Time:    {:?}", short_time);

    // Medium pattern - normal trigram lookup
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.content CONTAINS 'test_' RETURN count(*)",
        None,
    )?;
    let medium_time = start.elapsed();
    let count: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("\n--- MEDIUM PATTERN (5 chars: 'test_') - uses trigram index ---");
    println!("Results: {}", count);
    println!("Time:    {:?}", medium_time);

    // Common pattern - should skip index due to common trigrams
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.name CONTAINS 'o_' RETURN count(*)",
        None,
    )?;
    let common_time = start.elapsed();
    let count: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("\n--- COMMON PATTERN - index skipped via is_common heuristic ---");
    println!("Results: {}", count);
    println!("Time:    {:?}", common_time);

    // Rare pattern - specific trigram intersection
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.content CONTAINS 'compression_utils' RETURN count(*)",
        None,
    )?;
    let rare_time = start.elapsed();
    let count: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("\n--- RARE PATTERN (17 chars: 'compression_utils') - full trigram index ---");
    println!("Results: {}", count);
    println!("Time:    {:?}", rare_time);

    // Multiple CONTAINS with AND
    let start = Instant::now();
    let res = conn.execute(
        "MATCH (p:Person) WHERE p.content CONTAINS 'test_' AND p.content CONTAINS 'code' RETURN count(*)",
        None,
    )?;
    let and_time = start.elapsed();
    println!("\n--- CONTAINS AND - dual index with AND intersection ---");
    println!("Time:    {:?}", and_time);

    Ok(())
}

#[test]
fn test_batch_transaction_performance() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    create_person_table(&conn)?;

    println!("\n=== BATCH TRANSACTION vs AUTOCOMMIT ===\n");

    // Test 1: Autocommit (old behavior - 500 separate transactions)
    let start = Instant::now();
    for i in 0..500 {
        conn.execute(
            &format!(
                "CREATE (:Person {{id: {}, name: 'AutoCommit{}', content: 'content_{}'}})",
                i, i, i
            ),
            None,
        )?;
    }
    let autocommit_time = start.elapsed();
    println!("500 inserts with AUTOCOMMIT (separate tx each):");
    println!("  Total:  {:?}", autocommit_time);
    println!(
        "  Per op: {:.2} ms",
        autocommit_time.as_millis() as f64 / 500.0
    );
    println!(
        "  Rate:   {:.0} ops/sec",
        500.0 / autocommit_time.as_secs_f64()
    );

    // Test 2: Batch transaction (new behavior - single transaction)
    let start = Instant::now();
    conn.begin()?;
    for i in 0..1000 {
        conn.execute(
            &format!(
                "CREATE (:Person {{id: {}, name: 'Batch{}', content: 'batch_{}'}})",
                i + 10000,
                i,
                i
            ),
            None,
        )?;
    }
    conn.commit()?;
    let batch_time = start.elapsed();
    println!("\n1000 inserts with BATCH TRANSACTION (single tx):");
    println!("  Total:  {:?}", batch_time);
    println!("  Per op: {:.2} ms", batch_time.as_millis() as f64 / 1000.0);
    println!("  Rate:   {:.0} ops/sec", 1000.0 / batch_time.as_secs_f64());

    let speedup = autocommit_time.as_secs_f64() / batch_time.as_secs_f64() * (500.0 / 1000.0);
    println!("\nBatch vs Autocommit speedup: {:.1}x", speedup);

    // Verify counts
    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let total: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("Total nodes: {} (expected 1500)", total);
    assert_eq!(total, 1500);

    Ok(())
}

#[test]
fn test_fast_insert_1000_nodes() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;

    let start = Instant::now();
    for i in 0..1000 {
        conn.fast_insert(
            "Person",
            vec![vec![
                ("id".to_string(), Value::Number(i as f64)),
                ("name".to_string(), Value::String(format!("Person{}", i))),
            ]],
        )?;
    }
    let elapsed = start.elapsed();
    println!("\n=== 1000 nodes (fast_insert per row) ===");
    println!("Total:  {:?}", elapsed);
    println!(
        "Per node: {:.2} ms",
        elapsed.as_micros() as f64 / 1000.0 / 1000.0
    );
    println!("Rate:   {:.0} nodes/sec", 1000.0 / elapsed.as_secs_f64());

    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1000);
    Ok(())
}

#[test]
fn test_fast_insert_batch_1000_nodes() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    )?;

    let mut all_rows = Vec::with_capacity(1000);
    for i in 0..1000 {
        all_rows.push(vec![
            ("id".to_string(), Value::Number(i as f64)),
            ("name".to_string(), Value::String(format!("Person{}", i))),
        ]);
    }

    let start = Instant::now();
    conn.fast_insert("Person", all_rows)?;
    let elapsed = start.elapsed();
    println!("\n=== 1000 nodes (fast_insert batch of 1000) ===");
    println!("Total:  {:?}", elapsed);
    println!("Per node: {:.2} μs", elapsed.as_micros() as f64 / 1000.0);
    println!("Rate:   {:.0} nodes/sec", 1000.0 / elapsed.as_secs_f64());

    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let count = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1000);
    Ok(())
}

#[test]
fn test_trigram_index_population_overhead() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    create_person_table(&conn)?;

    // Insert 10K nodes with both name and content fields
    println!("\n=== TRIGRAM INDEX POPULATION OVERHEAD ===");
    println!("Testing async worker queue vs synchronous indexing\n");

    // Single row inserts - tests worker queue batching
    let start = Instant::now();
    for i in 0..500 {
        conn.execute(
            &format!("CREATE (:Person {{id: {}, name: 'Node{}', content: 'some_text_data_for_node_{}'}})", i, i, i),
            None,
        )?;
    }
    let single_time = start.elapsed();
    println!("500 single-row inserts:");
    println!("  Total:  {:?}", single_time);
    println!("  Per op: {:.2} ms", single_time.as_millis() as f64 / 500.0);
    println!("  Rate:   {:.0} ops/sec", 500.0 / single_time.as_secs_f64());

    // Bulk insert - tests efficient batch processing
    let start = Instant::now();
    bulk_insert_names(&conn, 10_000, "Bulk")?;
    let bulk_time = start.elapsed();
    println!("\n10K bulk insert (async batch processing):");
    println!("  Total:  {:?}", bulk_time);
    println!(
        "  Per node: {:.2} μs",
        bulk_time.as_micros() as f64 / 10_000.0
    );
    println!(
        "  Rate:   {:.0} nodes/sec",
        10_000.0 / bulk_time.as_secs_f64()
    );

    // Verify total count
    let res = conn.execute("MATCH (p:Person) RETURN count(*)", None)?;
    let total: i64 = res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("\nTotal nodes in DB: {}", total);
    assert_eq!(total, 10_500);

    Ok(())
}

#[test]
fn test_adaptive_threshold_decisions() -> Result<()> {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default())?;
    let conn = db.connect();

    create_person_table(&conn)?;

    // Insert 15K nodes with deterministic patterns
    println!("\n=== ADAPTIVE THRESHOLD DECISION LOGGING ===");

    let ids: arrow::array::Int64Array = (0..15_000i64).map(Some).collect();
    let names: StringArray = (0..15_000)
        .map(|i| {
            let s = if i % 100 < 20 {
                format!("Common_{}", i % 100)
            } else {
                format!("Unique_{}", i)
            };
            Some(s)
        })
        .collect();
    let contents: StringArray = (0..15_000)
        .map(|i| Some(format!("content_file_{}_data_here", i)))
        .collect();
    let ages: arrow::array::Int64Array = (0..15_000i64).map(Some).collect();

    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn arrow::array::Array>),
        ("name", Arc::new(names) as Arc<dyn arrow::array::Array>),
        (
            "content",
            Arc::new(contents) as Arc<dyn arrow::array::Array>,
        ),
        ("age", Arc::new(ages) as Arc<dyn arrow::array::Array>),
    ])?;

    let _ = conn.bulk_insert_batch("Person", &batch)?;

    // Test various pattern types
    let patterns = vec![
        ("_", "1-char pattern (should skip)"),
        ("Co", "2-char pattern (should skip if >10K rows)"),
        ("Common", "prefix with common trigram"),
        ("Unique_12345", "rare specific pattern"),
        ("file", "medium common substring"),
    ];

    for (pattern, desc) in patterns {
        let start = Instant::now();
        let res = conn.execute(
            &format!(
                "MATCH (p:Person) WHERE p.name CONTAINS '{}' RETURN count(*)",
                pattern
            ),
            None,
        )?;
        let elapsed = start.elapsed();
        let count: i64 = res.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        println!("\nPattern '{}' - {}", pattern, desc);
        println!("  Results: {}", count);
        println!("  Time:   {:?}", elapsed);
        println!(
            "  Rate:   {:.0} results/sec",
            count as f64 / elapsed.as_secs_f64()
        );
    }

    Ok(())
}
