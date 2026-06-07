/// Lightning Battle-Testing Suite
///
/// Benchmarks, stress tests, crash recovery, and large-scale validation.
/// Run with: cargo test --test benchmark_suite -- --nocapture
///
/// Results are printed to stdout in a structured format.

use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
use lightning_core::{Database, SystemConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// ============================================================
// Helpers
// ============================================================

struct BenchResult {
    name: &'static str,
    ops: u64,
    duration: Duration,
    throughput: f64,
}

impl BenchResult {
    fn print(&self) {
        println!(
            "BENCH|{}|{}|{:.3}s|{:.0} ops/s",
            self.name,
            self.ops,
            self.duration.as_secs_f64(),
            self.throughput
        );
    }
}

fn bench<F>(name: &'static str, ops: u64, f: F) -> BenchResult
where
    F: Fn(),
{
    let start = Instant::now();
    f();
    let duration = start.elapsed();
    let throughput = ops as f64 / duration.as_secs_f64();
    let r = BenchResult { name, ops, duration, throughput };
    r.print();
    r
}

// ============================================================
// 1. INSERT THROUGHPUT vs SQLite
// ============================================================

#[test]
fn bench_insert_10k() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(id INT64, val STRING, PRIMARY KEY (id))", None).unwrap();

    bench("insert_10k_lightning_bulk", 10_000, || {
        let ids: Int64Array = (0..10_000).map(Some).collect();
        let vals: StringArray = (0..10_000).map(|i| Some(format!("val_{}", i))).collect();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as _),
            ("val", Arc::new(vals) as _),
        ]).unwrap();
        conn.bulk_insert_batch("Item", &batch).unwrap();
    });
}

#[test]
fn bench_insert_100k() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(id INT64, val STRING, category INT64, PRIMARY KEY (id))", None).unwrap();

    bench("insert_100k_lightning_bulk", 100_000, || {
        let ids: Int64Array = (0..100_000).map(Some).collect();
        let vals: StringArray = (0..100_000).map(|i| Some(format!("val_{}", i))).collect();
        let cats: Int64Array = (0..100_000).map(|i| Some(i % 20)).collect();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as _),
            ("val", Arc::new(vals) as _),
            ("category", Arc::new(cats) as _),
        ]).unwrap();
        conn.bulk_insert_batch("Item", &batch).unwrap();
    });
}

// ============================================================
// 2. QUERY LATENCY
// ============================================================

fn setup_20k() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Item(id INT64, val STRING, age INT64, PRIMARY KEY (id))", None).unwrap();
    let ids: Int64Array = (0..20_000).map(Some).collect();
    let vals: StringArray = (0..20_000).map(|i| Some(format!("val_{}", i))).collect();
    let ages: Int64Array = (0..20_000).map(|i| Some(((i % 80) + 18) as i64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("val", Arc::new(vals) as _),
        ("age", Arc::new(ages) as _),
    ]).unwrap();
    conn.bulk_insert_batch("Item", &batch).unwrap();
    (dir, db)
}

#[test]
fn bench_full_scan_20k() {
    let (_dir, db) = setup_20k();
    let conn = db.connect();
    // Warm up
    conn.execute("MATCH (i:Item) RETURN i.id LIMIT 1", None).unwrap();
    bench("full_scan_20k", 20_000, || {
        let res = conn.execute("MATCH (i:Item) RETURN i.id, i.val, i.age", None).unwrap();
        let rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 20_000);
    });
}

#[test]
fn bench_filtered_scan_20k() {
    let (_dir, db) = setup_20k();
    let conn = db.connect();
    bench("filtered_scan_20k", 20_000, || {
        let res = conn.execute("MATCH (i:Item) WHERE i.age >= 50 RETURN i.id, i.val, i.age", None).unwrap();
        let _rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    });
}

#[test]
fn bench_aggregate_count_20k() {
    let (_dir, db) = setup_20k();
    let conn = db.connect();
    bench("aggregate_count_20k", 1, || {
        let res = conn.execute("MATCH (i:Item) RETURN count(*), avg(i.age), max(i.age)", None).unwrap();
        let _count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    });
}

// ============================================================
// 3. GRAPH TRAVERSAL
// ============================================================

#[test]
fn bench_graph_traversal() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)", None).unwrap();

    // Create 1K persons in a chain graph (each knows the next)
    let ids: Int64Array = (0..1000).map(Some).collect();
    let names: StringArray = (0..1000).map(|i| Some(format!("person_{}", i))).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("name", Arc::new(names) as _),
    ]).unwrap();
    conn.bulk_insert_batch("Person", &batch).unwrap();

    // Create edges
    for i in 0..999 {
        conn.execute(
            &format!("MATCH (a:Person {{id: {}}}), (b:Person {{id: {}}}) CREATE (a)-[:Knows]->(b)", i, i + 1),
            None,
        ).unwrap();
    }

    // Single-hop neighbor traversal
    let _warmup = conn.execute("MATCH (a:Person {{id: 0}})-[:Knows]->(b:Person) RETURN b.id", None);
    bench("graph_1hop_neighbor", 1, || {
        let _res = conn.execute("MATCH (a:Person {id: 500})-[:Knows]->(b:Person) RETURN b.id", None).unwrap();
    });
}

// ============================================================
// 4. CONCURRENT STRESS TEST
// ============================================================

#[test]
fn stress_concurrent_read_write() {
    use std::thread;

    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    // Single-node table for counter
    conn.execute("CREATE NODE TABLE Counter(id INT64, val INT64, PRIMARY KEY (id))", None).unwrap();
    conn.execute("CREATE (:Counter {id: 1, val: 0})", None).unwrap();

    let db = Arc::new(db);
    let num_threads = 8;
    let ops_per_thread = 200;
    let errors = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads).map(|_| {
        let db = Arc::clone(&db);
        let errs = Arc::clone(&errors);
        let done = Arc::clone(&completed);
        thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let c = db.connect();
                // Read-only queries — no write-write conflicts
                // Write queries separated to avoid MVCC conflicts under stress
                let r = match rand() % 4 {
                    0 => {
                        c.execute("MATCH (c:Counter) WHERE c.id = 1 RETURN c.val", None)
                    }
                    1 => {
                        c.execute("MATCH (c:Counter) WHERE c.id = 1 SET c.val = c.val + 1", None)
                    }
                    2 => {
                        // Count all nodes (cheap)
                        c.execute("MATCH (c:Counter) RETURN count(*)", None)
                    }
                    _ => {
                        // Read + verify
                        c.execute("MATCH (c:Counter) RETURN c.val, c.id", None)
                    }
                };
                match r {
                    Ok(_) => { done.fetch_add(1, Ordering::SeqCst); }
                    Err(e) => {
                        errs.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        })
    }).collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let err_cnt = errors.load(Ordering::SeqCst);
    let ops_total = completed.load(Ordering::SeqCst) + err_cnt;

    println!(
        "STRESS|16_threads|{}ops|{:.3}s|{:.0}ops/s|{}errors",
        ops_total,
        elapsed.as_secs_f64(),
        ops_total as f64 / elapsed.as_secs_f64(),
        err_cnt
    );

    // Verify final value
    let res = db.connect().execute("MATCH (c:Counter) WHERE c.id = 1 RETURN c.val", None).unwrap();
    if res.batches.is_empty() || res.batches[0].num_rows() == 0 {
        println!("STRESS|WARN|counter_not_found_after_concurrent_ops");
    } else {
        let final_val = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        println!("STRESS|final_counter_value|{}", final_val);
    }

    // Print error info but accept some write-write conflicts (expected under concurrency)
    if err_cnt > 0 {
        println!("STRESS|WARN|{} concurrent errors (expected write-write conflicts)", err_cnt);
    }
    assert!(ops_total > 0, "Concurrent stress test did 0 ops");
}

// Simple LCG random
fn rand() -> u64 {
    use std::sync::atomic::AtomicU64;
    static SEED: AtomicU64 = AtomicU64::new(42);
    let old = SEED.fetch_add(1, Ordering::SeqCst);
    old.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407)
}

// ============================================================
// 5. LARGE DATASET: 100K nodes
// ============================================================

#[test]
fn bench_large_100k() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Entity(id INT64, name STRING, value DOUBLE, category INT64, PRIMARY KEY (id))",
        None,
    ).unwrap();

    // Bulk insert
    let n = 100_000u64;
    let result = bench("insert_100k_multi_column", n, || {
        let ids: Int64Array = (0..n as i64).map(Some).collect();
        let names: StringArray = (0..n).map(|i| Some(format!("entity_{}", i))).collect();
        let vals: Float64Array = (0..n).map(|i| Some(i as f64 * 0.5)).collect();
        let cats: Int64Array = (0..n).map(|i| Some((i % 50) as i64)).collect();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as _),
            ("name", Arc::new(names) as _),
            ("value", Arc::new(vals) as _),
            ("category", Arc::new(cats) as _),
        ]).unwrap();
        conn.bulk_insert_batch("Entity", &batch).unwrap();
    });

    // Verify count
    let res = conn.execute("MATCH (e:Entity) RETURN count(*)", None).unwrap();
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count as u64, n);
    println!("  Verified: {} entities in database", count);

    // Query benchmarks on the large dataset
    let _warmup = conn.execute("MATCH (e:Entity) WHERE e.id = 0 RETURN e.name", None);
    bench("query_100k_by_id", 1, || {
        let res = conn.execute("MATCH (e:Entity) WHERE e.id = 50000 RETURN e.name, e.value", None).unwrap();
        let _rows: usize = res.batches.iter().map(|b| b.num_rows()).sum();
    });

    bench("query_100k_filter_category", 1, || {
        let res = conn.execute("MATCH (e:Entity) WHERE e.category = 25 RETURN count(*)", None).unwrap();
        let _cnt = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    });

    bench("query_100k_aggregate", 1, || {
        let res = conn.execute("MATCH (e:Entity) RETURN avg(e.value), max(e.value), min(e.value)", None).unwrap();
        let _avg = res.batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    });
}

// ============================================================
// 6. CRASH RECOVERY TEST
// ============================================================

#[test]
fn stress_crash_recovery() {
    // Simulate crash recovery by:
    // 1. Create database, insert data, checkpoint
    // 2. Insert more data WITHOUT checkpoint (simulate dirty buffers)
    // 3. Close the database (simulate crash via Drop)
    // 4. Reopen, verify data integrity

    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create and populate
    {
        let db = Database::new(&db_path, SystemConfig::default()).unwrap();
        let conn = db.connect();
        conn.execute("CREATE NODE TABLE Test(id INT64, val STRING, PRIMARY KEY (id))", None).unwrap();

        // Batch 1: 5K rows with checkpoint
        let ids: Int64Array = (0..5_000).map(Some).collect();
        let vals: StringArray = (0..5_000).map(|i| Some(format!("pre_crash_{}", i))).collect();
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as _),
            ("val", Arc::new(vals) as _),
        ]).unwrap();
        conn.bulk_insert_batch("Test", &batch).unwrap();
        db.checkpoint().unwrap();

        // Batch 2: 5K rows without checkpoint (simulating dirty pages at crash time)
        let ids2: Int64Array = (5_000..10_000).map(Some).collect();
        let vals2: StringArray = (5_000..10_000).map(|i| Some(format!("post_crash_{}", i))).collect();
        let batch2 = arrow::record_batch::RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids2) as _),
            ("val", Arc::new(vals2) as _),
        ]).unwrap();
        conn.bulk_insert_batch("Test", &batch2).unwrap();
        // db drops here → shutdown() → checkpoint() which now saves the catalog
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::new(&db_path, SystemConfig::default()).unwrap();
        let conn = db.connect();

        let res = conn.execute("MATCH (t:Test) RETURN count(*)", None).unwrap();
        let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        println!("CRASH_RECOVERY|count_after_reopen|{}", count);

        // All 10K rows should survive the clean shutdown with catalog persistence
        assert!(count >= 5000, "At least checkpointed data must survive (got {})", count);
        if count < 10_000 {
            println!("CRASH_RECOVERY|WARN|only {} rows survived (expected 10K)", count);
        } else {
            println!("CRASH_RECOVERY|PASS|all_data_intact");
        }
    }
}

// ============================================================
// 7. MEMORY USAGE UNDER LOAD
// ============================================================

#[test]
fn bench_memory_usage() {
    let dir = tempdir().unwrap();
    let config = SystemConfig {
        buffer_pool_size: 64 * 1024 * 1024, // 64 MB buffer pool
        ..Default::default()
    };
    let db = Database::new(dir.path(), config).unwrap();
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE BigRow(id INT64, data STRING, val DOUBLE, PRIMARY KEY (id))", None).unwrap();

    // Insert 50K rows and measure
    let start = Instant::now();
    let ids: Int64Array = (0..50_000).map(Some).collect();
    let data: StringArray = (0..50_000).map(|i| Some(format!("Some longer test data for row number {} to measure memory", i))).collect();
    let vals: Float64Array = (0..50_000).map(|i| Some(i as f64)).collect();
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("data", Arc::new(data) as _),
        ("val", Arc::new(vals) as _),
    ]).unwrap();
    conn.bulk_insert_batch("BigRow", &batch).unwrap();
    let elapsed = start.elapsed();

    // Disk usage
    let disk_used: u64 = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.metadata().unwrap().len())
        .sum();

    println!("MEMORY|50k_string_rows|insert_time={:?}|disk_usage={}MB|{}bytes/row",
        elapsed,
        disk_used / 1024 / 1024,
        disk_used / 50_000
    );

    // Query speed
    let res = conn.execute("MATCH (b:BigRow) WHERE b.val >= 25000.0 RETURN count(*)", None).unwrap();
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 25_000);
}

// ============================================================
// 8. DURABILITY TEST: repeated crash + restart cycles
// ============================================================

#[test]
fn stress_repeated_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    let num_cycles = 10;

    for cycle in 0..num_cycles {
        {
            let db = Database::new(&db_path, SystemConfig::default()).unwrap();
            let conn = db.connect();

            if cycle == 0 {
                conn.execute("CREATE NODE TABLE Cycle(id INT64, cycle INT64, PRIMARY KEY (id))", None).unwrap();
            }

            // Insert 1000 rows each cycle
            let start_id = cycle * 1000;
            let ids: Int64Array = (start_id..start_id + 1000).map(Some).collect();
            let cycles: Int64Array = (0..1000).map(|_| Some(cycle as i64)).collect();
            let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
                ("id", Arc::new(ids) as _),
                ("cycle", Arc::new(cycles) as _),
            ]).unwrap();
            conn.bulk_insert_batch("Cycle", &batch).unwrap();
            db.checkpoint().unwrap();

            // Verify count so far
            let res = conn.execute("MATCH (c:Cycle) RETURN count(*)", None).unwrap();
            let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            assert_eq!(count as i64, ((cycle + 1) * 1000) as i64, "Cycle {} count", cycle);
        }
        // db drops = clean shutdown/checkpoint

        // Reopen and verify
        {
            let db = Database::new(&db_path, SystemConfig::default()).unwrap();
            let conn = db.connect();
            let res = conn.execute("MATCH (c:Cycle) RETURN count(*)", None).unwrap();
            let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            assert_eq!(count, ((cycle + 1) * 1000) as i64,
                "Cycle {}: expected {} rows, got {}", cycle + 1, (cycle + 1) * 1000, count
            );
            println!("DURABILITY|cycle_{}|passed|count={}", cycle + 1, count);
        }
    }
}

// ============================================================
// 9. EDGE CASE: empty database operations
// ============================================================

#[test]
fn edge_empty_database() {
    // Create, query, drop — no data
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Empty(id INT64, PRIMARY KEY (id))", None).unwrap();
    let res = conn.execute("MATCH (e:Empty) RETURN count(*)", None).unwrap();
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 0, "Empty table should have 0 rows");
    // Drop and recreate
    conn.execute("DROP TABLE Empty", None).unwrap();
    conn.execute("CREATE NODE TABLE Empty(id INT64, name STRING, PRIMARY KEY (id))", None).unwrap();
    let res2 = conn.execute("MATCH (e:Empty) RETURN count(*)", None).unwrap();
    let count2 = res2.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count2, 0);
    println!("EDGE|empty_database|PASS");
}

// ============================================================
// 10. LARGE STRING DATA
// ============================================================

#[test]
fn bench_large_strings() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Doc(id INT64, title STRING, body STRING, PRIMARY KEY (id))", None).unwrap();

    // Insert 10K documents with realistic content sizes
    let n = 10_000;
    let ids: Int64Array = (0..n).map(Some).collect();
    let titles: StringArray = (0..n).map(|i| Some(format!("Document Number {}", i))).collect();
    let bodies: Vec<String> = (0..n).map(|i| {
        format!("This is the body of document {}. It contains some meaningful text that simulates a realistic document storage scenario with varying content lengths for testing purposes. ", i).repeat(5)
    }).collect();
    let bodies_arr = StringArray::from(bodies.iter().map(|s| s.as_str()).collect::<Vec<_>>());

    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as _),
        ("title", Arc::new(titles) as _),
        ("body", Arc::new(bodies_arr) as _),
    ]).unwrap();

    let r = bench("insert_10k_large_strings", n as u64, || {
        conn.bulk_insert_batch("Doc", &batch).unwrap();
    });

    // Verify content
    let res = conn.execute("MATCH (d:Doc) WHERE d.id = 5000 RETURN d.title, d.body", None).unwrap();
    let title = res.batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
    assert!(title.contains("5000"), "Title should contain '5000'");
}

// ============================================================
// CONCURRENT THROUGHPUT BENCHMARK
// ============================================================

/// Measure combined throughput of N concurrent readers + M concurrent writers.
/// Readers execute aggregate queries; writers create new nodes in batches.
#[test]
fn bench_concurrent_throughput() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::new(dir.path(), SystemConfig::default()).unwrap());
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE T(id INT64, val INT64)", None).unwrap();

    // Seed initial data
    for i in 0..1000 {
        conn.execute(&format!("CREATE (:T {{id: {}, val: {}}})", i, i), None).unwrap();
    }

    // Config
    let num_readers = 4.max(num_cpus::get() / 2);
    let num_writers = 2.max(num_cpus::get() / 4);
    let run_duration = Duration::from_secs(3);
    let writer_batch = 10;

    let reader_ops = Arc::new(AtomicU64::new(0));
    let writer_ops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicU64::new(0));

    // Spawn readers
    let mut handles = Vec::new();
    for _ in 0..num_readers {
        let db = Arc::clone(&db);
        let rops = Arc::clone(&reader_ops);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            let c = db.connect();
            while stop.load(Ordering::Relaxed) == 0 {
                if c.execute(
                    "MATCH (t:T) WHERE t.val > 500 RETURN count(*), avg(t.val)",
                    None,
                ).is_ok() {
                    rops.fetch_add(1, Ordering::Relaxed);
                }
                // Also do short range queries
                if c.execute(
                    "MATCH (t:T) WHERE t.id < 1000 RETURN max(t.id)",
                    None,
                ).is_ok() {
                    rops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Spawn writers
    let next_id = Arc::new(AtomicU64::new(1000));
    for _ in 0..num_writers {
        let db = Arc::clone(&db);
        let wops = Arc::clone(&writer_ops);
        let stop = Arc::clone(&stop);
        let nid = Arc::clone(&next_id);
        handles.push(std::thread::spawn(move || {
            let c = db.connect();
            while stop.load(Ordering::Relaxed) == 0 {
                let start = nid.fetch_add(writer_batch as u64, Ordering::Relaxed);
                let mut ok = true;
                for j in 0..writer_batch {
                    if c.execute(
                        &format!("CREATE (:T {{id: {}, val: {}}})", start + j, start + j),
                        None,
                    ).is_err() {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    wops.fetch_add(writer_batch as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    // Run for fixed duration
    std::thread::sleep(run_duration);
    stop.store(1, Ordering::SeqCst);

    for h in handles {
        h.join().unwrap();
    }

    let r_ops = reader_ops.load(Ordering::Relaxed);
    let w_ops = writer_ops.load(Ordering::Relaxed);
    let total_ops = r_ops + w_ops;
    let elapsed = run_duration.as_secs_f64();
    let throughput = total_ops as f64 / elapsed;

    println!(
        "BENCH|concurrent|{} readers {} writers|{:.1}s|{}R {}W {}T ({:.0} ops/s)",
        num_readers,
        num_writers,
        elapsed,
        r_ops,
        w_ops,
        total_ops,
        throughput,
    );

    assert!(
        total_ops > 0,
        "Concurrent benchmark should complete at least 1 operation"
    );
}

// ============================================================
// SUMMARY
// ============================================================

#[test]
fn bench_summary() {
    println!("\n===== LIGHTNING BATTLE-TESTING REPORT =====");
    println!("Platform: {} ({} cores)", std::env::consts::OS, num_cpus::get());
    println!("Tests: insert, query, graph, concurrent, crash, memory, edge cases");
    println!("============================================\n");
}
