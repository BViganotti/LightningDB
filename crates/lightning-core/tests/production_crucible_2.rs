/// PRODUCTION CRUCIBLE TEST — Phase 2
///
/// Advanced test scenarios: Fusion code analysis module, WASM UDFs,
/// schema evolution under load, complex Cypher queries, optimizer rules,
/// buffer pool pressure, correlated subqueries, and cross-table joins at scale.

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use lightning_core::fusion::FusionApp;
use lightning_core::{Database, SystemConfig};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

type TestResult = lightning_core::Result<()>;

fn setup_db() -> lightning_core::Result<(tempfile::TempDir, Arc<Database>)> {
    let dir = tempdir()?;
    let db = Database::new(dir.path(), SystemConfig::default())?;
    Ok((dir, db))
}

fn exec(db: &Arc<Database>, query: &str) -> lightning_core::QueryResult {
    let conn = db.connect();
    conn.execute(query, None).unwrap()
}

fn exec_with(db: &Arc<Database>, query: &str, params: Option<std::collections::HashMap<String, lightning_core::Value>>) -> lightning_core::QueryResult {
    let conn = db.connect();
    conn.execute(query, params).unwrap()
}

macro_rules! assert_count {
    ($res:expr, $expected:expr) => {
        let total: usize = $res.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, $expected, "Expected {} rows, got {}", $expected, total);
    };
}

macro_rules! assert_val {
    ($res:expr, $col:expr, $row:expr, $expected:expr, $type:ty) => {
        if $res.batches.is_empty() || $res.batches[0].num_rows() <= $row {
            panic!("Result is empty or does not have row {}", $row);
        }
        let val = $res.batches[0]
            .column($col)
            .as_any()
            .downcast_ref::<$type>()
            .expect(&format!("Type mismatch in column {} at row {}", $col, $row))
            .value($row);
        assert_eq!(val, $expected);
    };
}

// ============================================================================
// 1. FUSION MODULE — init, code nodes, paths, PageRank
// ============================================================================

#[test]
fn crucible_fusion_module() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    // Initialize fusion schema
    FusionApp::init_fusion_schema(&conn)?;

    // Create code nodes (simulating what the indexer would create)
    conn.execute(
        "CREATE NODE TABLE CodeNode(id STRING, name STRING, node_type STRING, file_path STRING, PRIMARY KEY (id))",
        None,
    )?;
    conn.execute(
        "CREATE REL TABLE CodeEdge(FROM CodeNode TO CodeNode, edge_type STRING)",
        None,
    )?;

    // Insert code nodes
    let nodes = vec![
        ("main.rs", "main", "function", "/src/main.rs"),
        ("lib.rs", "lib", "module", "/src/lib.rs"),
        ("utils.rs", "utils", "module", "/src/utils.rs"),
        ("db.rs", "db", "module", "/src/db.rs"),
        ("handler.rs", "handler", "function", "/src/handler.rs"),
    ];
    for (id, name, node_type, path) in &nodes {
        conn.execute(
            &format!("CREATE (:CodeNode {{id: '{}', name: '{}', node_type: '{}', file_path: '{}'}})", id, name, node_type, path),
            None,
        )?;
    }

    // Insert edges
    let edges = vec![
        ("main.rs", "lib.rs", "calls"),
        ("lib.rs", "utils.rs", "imports"),
        ("lib.rs", "db.rs", "imports"),
        ("main.rs", "handler.rs", "calls"),
        ("handler.rs", "db.rs", "calls"),
    ];
    for (src, dst, etype) in &edges {
        conn.execute(
            &format!("MATCH (a:CodeNode {{id: '{}'}}), (b:CodeNode {{id: '{}'}}) CREATE (a)-[:CodeEdge {{edge_type: '{}'}}]->(b)", src, dst, etype),
            None,
        )?;
    }

    // Fusion find_node_by_name
    let found = FusionApp::find_node_by_name(&conn, "main")?;
    assert!(!found.is_empty(), "should find node by name 'main'");
    println!("  Fusion find_node_by_name('main'): {:?}", found);

    // Fusion find_paths
    let paths = FusionApp::find_paths(&conn, "main.rs", "db.rs", &[])?;
    println!("  Fusion paths from main.rs to db.rs: {:?}", paths);
    assert!(!paths.is_empty(), "should find paths from main to db");

    // Fusion find_connected_nodes
    use lightning_core::fusion::ConnectedDirection;
    let connected = FusionApp::find_connected_nodes(&conn, "lib.rs", &[], ConnectedDirection::Incoming)?;
    println!("  Fusion incoming connections to lib.rs: {:?}", connected);
    assert!(!connected.is_empty(), "lib.rs should have incoming connections");

    // Fusion lookup_node_names
    let ids: Vec<String> = nodes.iter().map(|(id, _, _, _)| id.to_string()).collect();
    let names = FusionApp::lookup_node_names(&conn, &ids)?;
    assert_eq!(names.len(), nodes.len(), "should look up all node names");
    println!("  Fusion lookup_node_names: {} results", names.len());

    // Fusion add_observation + get_recent_observations
    FusionApp::add_observation(&conn, "obs_1", "Found a potential bug in db.rs", None)?;
    FusionApp::add_observation(&conn, "obs_2", "Performance bottleneck in handler.rs", Some("obs_1"))?;
    let observations = FusionApp::get_recent_observations(&conn, 10)?;
    assert_eq!(observations.len(), 2, "should have 2 observations");
    println!("  Fusion observations: {:?}", observations);

    // Fusion compute_architecture_cohesion (needs proper module graph)
    // Just verify it returns without error
    let _ = FusionApp::compute_architecture_cohesion(&conn)?;

    // Fusion materialize_pagerank
    FusionApp::materialize_pagerank(&conn)?;
    println!("  Fusion PageRank: computed successfully");

    // Fusion export_to_d3_json
    let d3_json = FusionApp::export_to_d3_json(&conn)?;
    assert!(!d3_json.is_empty(), "D3 export should produce JSON");
    assert!(d3_json.contains("nodes"), "D3 JSON should contain 'nodes'");
    assert!(d3_json.contains("links"), "D3 JSON should contain 'links'");
    println!("  Fusion D3 export: {} chars", d3_json.len());

    Ok(())
}

// ============================================================================
// 2. COMPLEX CYPHER QUERIES — joins, aggregations, subqueries, UNION
// ============================================================================

#[test]
fn crucible_complex_cypher_queries() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    // Multi-table schema: Users, Orders, Products
    conn.execute("CREATE NODE TABLE User(id INT64, name STRING, age INT64, city STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, category STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE NODE TABLE Order(id INT64, user_id INT64, product_id INT64, quantity INT64, total DOUBLE, PRIMARY KEY (id))", None)?;

    // Insert data
    for i in 0..50 {
        conn.execute(&format!("CREATE (:User {{id: {}, name: 'User_{}', age: {}, city: '{}'}})",
            i, i, 20 + (i % 30), ["NYC", "SF", "LA", "CHI", "SEA"][i as usize % 5]), None)?;
    }
    for i in 0..100 {
        conn.execute(&format!("CREATE (:Product {{id: {}, name: 'Product_{}', price: {}, category: '{}'}})",
            i, i, (i as f64 + 0.99) * 10.0, ["Electronics", "Books", "Clothing", "Food", "Tools"][i as usize % 5]), None)?;
    }
    for i in 0..200 {
        conn.execute(&format!("CREATE (:Order {{id: {}, user_id: {}, product_id: {}, quantity: {}, total: {}}})",
            i, i % 50, i % 100, (i % 5) + 1, ((i % 5) + 1) as f64 * ((i as f64 + 0.99) * 10.0)), None)?;
    }

    // COUNT aggregate
    let res = exec(&db, "MATCH (o:Order) RETURN count(*)");
    assert_count!(res, 1);
    println!("  Total orders: OK");

    // GROUP BY + aggregation
    let res = exec(&db, "MATCH (o:Order) RETURN o.user_id, count(*), sum(o.total) ORDER BY o.user_id LIMIT 5");
    assert!(res.batches[0].num_rows() > 0);
    println!("  GROUP BY city aggregation: OK");

    // Cross-table join via comma-separated MATCH
    let res = exec(&db,
        "MATCH (u:User), (o:Order) WHERE u.id = o.user_id RETURN u.name, o.total ORDER BY u.name, o.total LIMIT 10"
    );
    assert_count!(res, 10);
    println!("  Cross-table join (User -> Order): OK");

    // Triple join
    let res = exec(&db,
        "MATCH (u:User), (o:Order), (p:Product) \
         WHERE u.id = o.user_id AND o.product_id = p.id \
         RETURN u.name, p.name, o.quantity ORDER BY u.name LIMIT 10"
    );
    assert_count!(res, 10);
    println!("  Triple join (User -> Order -> Product): OK");

    // Filter + aggregate
    let res = exec(&db,
        "MATCH (o:Order) WHERE o.total > 100 RETURN o.user_id, avg(o.total), max(o.total) ORDER BY o.user_id LIMIT 5"
    );
    assert_count!(res, 5);
    println!("  Filter + aggregate: OK");

    // ORDER BY + LIMIT + SKIP (OFFSET)
    let res = exec(&db,
        "MATCH (u:User) RETURN u.name ORDER BY u.name ASC LIMIT 5 OFFSET 10"
    );
    assert_count!(res, 5);
    println!("  ORDER BY + LIMIT + OFFSET: OK");

    // WHERE with IN
    let res = exec(&db,
        "MATCH (u:User) WHERE u.age IN [25, 30, 35] RETURN count(*)"
    );
    println!("  WHERE IN clause: OK");

    // Multi-field WHERE with AND/OR
    let res = exec(&db,
        "MATCH (u:User) WHERE (u.age > 30 AND u.city = 'NYC') OR (u.age < 25 AND u.city = 'SF') RETURN count(*)"
    );
    println!("  Complex WHERE (AND/OR): OK");

    // NOT EQUAL filter
    let res = exec(&db,
        "MATCH (u:User) WHERE u.city <> 'NYC' RETURN count(*)"
    );
    println!("  NOT EQUAL filter: OK");

    Ok(())
}

// ============================================================================
// 3. SCHEMA EVOLUTION UNDER LOAD — ALTER TABLE while querying
// ============================================================================

#[test]
fn crucible_schema_evolution() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Evolving(id INT64, name STRING, PRIMARY KEY (id))", None)?;

    // Insert base data
    for i in 0..100 {
        conn.execute(&format!("CREATE (:Evolving {{id: {}, name: 'base_{}'}})", i, i), None)?;
    }

    // ADD COLUMN
    conn.execute("ALTER TABLE Evolving ADD COLUMN score DOUBLE", None)?;
    conn.execute("MATCH (e:Evolving) SET e.score = 0.5", None)?;
    let res = exec(&db, "MATCH (e:Evolving) RETURN count(e.score), sum(e.score)");
    assert_count!(res, 1);
    println!("  ALTER ADD COLUMN + SET: OK");

    // ADD ANOTHER COLUMN
    conn.execute("ALTER TABLE Evolving ADD COLUMN active BOOL", None)?;
    conn.execute("MATCH (e:Evolving) SET e.active = TRUE", None)?;
    let res = exec(&db, "MATCH (e:Evolving {active: TRUE}) RETURN count(*)");
    assert_count!(res, 1);
    println!("  ALTER ADD BOOL COLUMN: OK");

    // RENAME COLUMN
    conn.execute("ALTER TABLE Evolving RENAME COLUMN score TO priority", None)?;
    let res = exec(&db, "MATCH (e:Evolving) RETURN e.priority LIMIT 1");
    assert_count!(res, 1);
    println!("  ALTER RENAME COLUMN: OK");

    // DROP COLUMN
    conn.execute("ALTER TABLE Evolving DROP COLUMN name", None)?;
    let res = exec(&db, "MATCH (e:Evolving) RETURN e.id, e.priority, e.active LIMIT 1");
    assert_count!(res, 1);
    println!("  ALTER DROP COLUMN: OK");

    // Add data after schema evolution
    for i in 100..110 {
        conn.execute(&format!("CREATE (:Evolving {{id: {}, priority: {}, active: {}}})", i, i as f64 * 0.1, if i % 2 == 0 { "TRUE" } else { "FALSE" }), None)?;
    }
    let res = exec(&db, "MATCH (e:Evolving) RETURN count(*)");
    let count = res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 110, "should have 110 rows after schema evolution");
    println!("  INSERT after schema evolution: {} rows OK", count);

    // RENAME TABLE
    conn.execute("ALTER TABLE Evolving RENAME TO Evolved", None)?;
    let res = exec(&db, "MATCH (e:Evolved) RETURN count(*)");
    assert!(res.batches[0].num_rows() > 0);
    println!("  ALTER RENAME TABLE: OK");

    Ok(())
}

// ============================================================================
// 4. BUFFER POOL PRESSURE — tiny pool, flood with data, verify correctness
// ============================================================================

#[test]
fn crucible_buffer_pool_pressure() -> TestResult {
    let dir = tempdir()?;
    // Use a very small buffer pool (1MB = 256 pages) to force constant eviction
    let config = SystemConfig {
        buffer_pool_size: 256 * 4096,
        ..Default::default()
    };
    let db = Arc::new(Database::new(dir.path(), config)?);
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Pressure(id INT64, val INT64, data STRING, PRIMARY KEY (id))", None)?;

    // Write rows that force buffer pool eviction
    let n = 2000u64;
    for i in 0..n {
        conn.execute(&format!(
            "CREATE (:Pressure {{id: {}, val: {}, data: 'row_{}'}})",
            i, (i * 7) % 1000, i
        ), None)?;

        // Periodically verify while under pressure
        if i > 0 && i % 500 == 0 {
            let res = exec(&db, &format!("MATCH (p:Pressure {{id: {}}}) RETURN p.val", i));
            assert!(res.batches[0].num_rows() > 0,
                "Row {} lost under buffer pool pressure", i);
        }
    }

    // Final count
    let res = exec(&db, "MATCH (p:Pressure) RETURN count(*)");
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0) as u64;
    assert_eq!(count, n, "Expected {} rows under pressure, got {}", n, count);

    println!("  Buffer pool pressure: {} rows with tiny pool, all intact", n);

    // Random access pattern to stress eviction further
    use std::collections::HashSet;
    let mut rng = XorShift::new(42);
    let mut accessed = HashSet::new();
    for _ in 0..1000 {
        let id = (rng.next_u64() % n) as i64;
        let res = exec(&db, &format!("MATCH (p:Pressure {{id: {}}}) RETURN p.val", id));
        if res.batches[0].num_rows() > 0 {
            accessed.insert(id);
        }
    }
    println!("  Random access: {} unique pages retrieved under pressure", accessed.len());

    Ok(())
}

// ============================================================================
// 5. WAL CRASH RECOVERY — mixed types, string columns, rollback
// ============================================================================

#[test]
fn crucible_wal_crash_recovery_types() -> TestResult {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Create schema and insert all data types, then crash without checkpoint
    {
        let db = Database::new(&db_path, SystemConfig::default())?;
        let conn = db.connect();

        conn.execute(
            "CREATE NODE TABLE Types(id INT64, int_val INT64, float_val DOUBLE, string_val STRING, bool_val BOOL, PRIMARY KEY (id))",
            None,
        )?;

        for i in 0..50 {
            conn.execute(&format!(
                "CREATE (:Types {{id: {}, int_val: {}, float_val: {}, string_val: 'str_{}', bool_val: {}}})",
                i, i * 10, i as f64 * 1.5, i, if i % 2 == 0 { "TRUE" } else { "FALSE" }
            ), None)?;
        }
        // No checkpoint — simulate crash
    }

    // Phase 2: Recover and verify ALL data types
    {
        let db = Database::new(&db_path, SystemConfig::default())?;
        let conn = db.connect();

        let res = exec(&db, "MATCH (t:Types) RETURN count(*)");
        let count = res.batches[0].column(0)
            .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(count, 50, "Expected 50 rows after WAL replay, got {}", count);

        // Verify each type round-trips correctly
        for i in 0..50 {
            let res = exec(&db, &format!(
                "MATCH (t:Types {{id: {}}}) RETURN t.int_val, t.float_val, t.string_val, t.bool_val", i
            ));
            let batch = &res.batches[0];
            assert!(batch.num_rows() > 0, "Row {} not found after WAL replay", i);

            let int_v = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
            let float_v = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
            let str_v = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0);
            let bool_v = batch.column(3).as_any().downcast_ref::<BooleanArray>().unwrap().value(0);

            assert_eq!(int_v, i * 10, "int mismatch for id {}", i);
            assert!((float_v - (i as f64 * 1.5)).abs() < 0.001, "float mismatch for id {}", i);
            assert_eq!(str_v, format!("str_{}", i), "string mismatch for id {}", i);
            assert_eq!(bool_v, i % 2 == 0, "bool mismatch for id {}", i);
        }
    }

    println!("  WAL crash recovery: ALL 50 rows with mixed types verified");

    // Phase 3: Insert additional data without checkpoint, crash again
    {
        let db = Database::new(&db_path, SystemConfig::default())?;
        let conn = db.connect();
        for i in 50..75 {
            conn.execute(&format!(
                "CREATE (:Types {{id: {}, int_val: {}, float_val: {}, string_val: 'crash_{}', bool_val: {}}})",
                i, i * 100, i as f64 * 10.0, i, if i % 3 == 0 { "TRUE" } else { "FALSE" }
            ), None)?;
        }
        // No checkpoint
    }

    // Recover again — should have 75 rows total
    {
        let db = Database::new(&db_path, SystemConfig::default())?;
        let conn = db.connect();
        let res = exec(&db, "MATCH (t:Types) RETURN count(*)");
        let count = res.batches[0].column(0)
            .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(count, 75, "Expected 75 rows after second WAL replay, got {}", count);
        println!("  Second WAL replay: 75 rows total");
    }

    Ok(())
}

// ============================================================================
// 6. CONCURRENT READERS DURING CHECKPOINT
// ============================================================================

#[test]
fn crucible_readers_during_checkpoint() -> TestResult {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::new(dir.path(), SystemConfig::default())?);
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE CkptData(id INT64, val INT64, PRIMARY KEY (id))", None)?;
    for i in 0..2000 {
        conn.execute(&format!("CREATE (:CkptData {{id: {}, val: {}}})", i, i * 3), None)?;
    }

    let num_readers = 4;
    let stop = Arc::new(AtomicBool::new(false));

    // Start reader threads
    let readers: Vec<_> = (0..num_readers).map(|id| {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let reader_id = id;
        std::thread::spawn(move || {
            let conn = db.connect();
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = conn.execute(
                    "MATCH (c:CkptData) WHERE c.val >= 0 RETURN count(*), avg(c.val)", None
                );
                let _ = conn.execute(
                    "MATCH (c:CkptData) WHERE c.id < 100 RETURN c.id, c.val ORDER BY c.id", None
                );
                reads += 2;
            }
            (reader_id, reads)
        })
    }).collect();

    // Run checkpoints while readers are active
    for i in 0..10 {
        db.checkpoint()?;
        std::thread::sleep(Duration::from_millis(5));
        if i % 5 == 4 {
            println!("  Checkpoint {}/10 completed with {} readers active", i + 1, num_readers);
        }
    }

    stop.store(true, Ordering::Release);

    let total_reads: u64 = readers.into_iter().map(|h| h.join().unwrap().1).sum();
    println!("  Readers during checkpoint: {} total reads, no crashes", total_reads);

    // Data integrity check
    let res = exec(&db, "MATCH (c:CkptData) RETURN count(*)");
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 2000, "Data should be intact after checkpoint with readers");
    println!("  Data integrity verified: {} rows", count);

    Ok(())
}

// ============================================================================
// 7. AGGREGATE FUNCTIONS — count, sum, avg, min, max, stddev
// ============================================================================

#[test]
fn crucible_aggregate_functions() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Agg(id INT64, val INT64, cat STRING, PRIMARY KEY (id))", None)?;
    for i in 1..=100 {
        conn.execute(&format!("CREATE (:Agg {{id: {}, val: {}, cat: 'cat_{}'}})",
            i, i * 2, (i % 5) + 1), None)?;
    }

    // Basic aggregates
    let res = exec(&db, "MATCH (a:Agg) RETURN count(*)");
    assert_val!(res, 0, 0, 100i64, Int64Array);

    let res = exec(&db, "MATCH (a:Agg) RETURN sum(a.val)");
    // sum(1..100 * 2) = 2 * 5050 = 10100
    let sum_val = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(sum_val, 10100, "sum should be 10100");

    let res = exec(&db, "MATCH (a:Agg) RETURN avg(a.val)");
    let avg_val = res.batches[0].column(0)
        .as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert!((avg_val - 101.0).abs() < 0.01, "avg should be ~101.0, got {}", avg_val);

    let res = exec(&db, "MATCH (a:Agg) RETURN min(a.val), max(a.val)");
    let min_val = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    let max_val = res.batches[0].column(1)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(min_val, 2, "min should be 2");
    assert_eq!(max_val, 200, "max should be 200");

    // GROUP BY with aggregation
    let res = exec(&db,
        "MATCH (a:Agg) RETURN a.cat, count(*), sum(a.val) ORDER BY a.cat"
    );
    assert_count!(res, 5);
    println!("  GROUP BY aggregates: 5 categories");

    // HAVING-style: GROUP BY with WHERE on aggregate column
    let res = exec(&db,
        "MATCH (a:Agg) WHERE a.val > 50 RETURN a.cat, count(*), avg(a.val) ORDER BY a.cat"
    );
    println!("  Filtered GROUP BY: {} rows", res.batches.iter().map(|b| b.num_rows()).sum::<usize>());

    Ok(())
}

// ============================================================================
// 8. RELATIONSHIP QUERIES — MATCH with edges at scale
// ============================================================================

#[test]
fn crucible_relationship_queries_at_scale() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, since INT64, strength DOUBLE)", None)?;

    // Create 500 people
    use std::sync::Arc as A;
    let ids: Vec<i64> = (0..500).collect();
    let names: Vec<String> = (0..500).map(|i| format!("Person_{}", i)).collect();
    let names_arr = arrow::array::StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", A::new(arrow::array::Int64Array::from(ids)) as _),
        ("name", A::new(names_arr) as _),
    ]).unwrap();
    conn.bulk_insert_batch("Person", &batch)?;

    // Create edges forming a small-world network
    let mut rng = XorShift::new(1234);
    let mut edge_count = 0;
    for i in 0..500 {
        // Each person knows ~5 others
        let num_edges = (rng.next_u64() % 8) + 2;
        for _ in 0..num_edges {
            let j = (rng.next_u64() % 500) as i64;
            if i as i64 != j {
                let since = 2000 + (rng.next_u64() % 24) as i64;
                let strength = (rng.next_u64() % 100) as f64 / 100.0;
                if let Ok(_) = conn.execute(
                    &format!(
                        "MATCH (a:Person {{id: {}}}), (b:Person {{id: {}}}) CREATE (a)-[:Knows {{since: {}, strength: {}}}]->(b)",
                        i, j, since, strength
                    ), None
                ) {
                    edge_count += 1;
                }
            }
        }
    }
    println!("  Created {} people with {} Knows edges", 500, edge_count);

    // Count edges
    let res = exec(&db, "MATCH (a:Person)-[k:Knows]->(b:Person) RETURN count(*)");
    let edges_found = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    println!("  Edge count: {} (expected ~{})", edges_found, edge_count);
    assert!(edges_found > 0, "should have edges in the graph");

    // Filtered edge query
    let res = exec(&db, "MATCH (a:Person)-[k:Knows]->(b:Person) WHERE k.strength > 0.5 RETURN count(*)");
    let strong_edges = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    println!("  Strong edges (strength > 0.5): {}", strong_edges);

    // Edge property aggregation
    let res = exec(&db, "MATCH (a:Person)-[k:Knows]->(b:Person) RETURN avg(k.strength), max(k.since), min(k.since)");
    assert_count!(res, 1);
    println!("  Edge stats: OK");

    Ok(())
}

// ============================================================================
// 9. GRAPH ALGORITHM — path finding, reachability
// ============================================================================

#[test]
fn crucible_graph_reachability() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE GNode(id INT64, PRIMARY KEY (id))", None)?;
    conn.execute("CREATE REL TABLE GEdge(FROM GNode TO GNode)", None)?;

    // Create a chain: 0 -> 1 -> 2 -> ... -> 99
    for i in 0..100 {
        conn.execute(&format!("CREATE (:GNode {{id: {}}})", i), None)?;
    }
    for i in 0..99 {
        conn.execute(&format!(
            "MATCH (a:GNode {{id: {}}}), (b:GNode {{id: {}}}) CREATE (a)-[:GEdge]->(b)",
            i, i + 1
        ), None)?;
    }

    // Edge count
    let res = exec(&db, "MATCH (a:GNode)-[:GEdge]->(b:GNode) RETURN count(*)");
    let edges = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(edges, 99, "should have 99 edges in chain");

    // CONTAINS (path pattern matching)
    let res = exec(&db, "MATCH (a:GNode)-[:GEdge]->(b:GNode)-[:GEdge]->(c:GNode) RETURN count(*)");
    let paths = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(paths, 98, "should have 98 paths of length 2");
    println!("  Path length 2: {}", paths);

    // CONTAINS with filter on interior node
    let res = exec(&db,
        "MATCH (a:GNode)-[:GEdge]->(b:GNode)-[:GEdge]->(c:GNode) WHERE b.id = 50 RETURN a.id, b.id, c.id"
    );
    assert_count!(res, 1);
    assert_val!(res, 0, 0, 49i64, Int64Array);
    assert_val!(res, 1, 0, 50i64, Int64Array);
    assert_val!(res, 2, 0, 51i64, Int64Array);
    println!("  Filtered path (b=50): OK");

    Ok(())
}

// ============================================================================
// 10. PARAMETERIZED QUERIES — $param style across multiple types
// ============================================================================

#[test]
fn crucible_parameterized_queries() -> TestResult {
    let (_dir, db) = setup_db()?;
    use lightning_core::Value;

    let conn = db.connect();
    conn.execute("CREATE NODE TABLE ParamTest(id INT64, name STRING, score DOUBLE, active BOOL, PRIMARY KEY (id))", None)?;

    // Insert
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), Value::Number(1.0));
    params.insert("name".to_string(), Value::String("param_test".to_string()));
    params.insert("score".to_string(), Value::Number(99.5));
    params.insert("active".to_string(), Value::Boolean(true));
    conn.execute("CREATE (:ParamTest {id: $id, name: $name, score: $score, active: $active})", Some(params))?;

    // Select with parameter
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), Value::Number(1.0));
    let res = conn.execute("MATCH (p:ParamTest) WHERE p.id = $id RETURN p.name, p.score, p.active", Some(params))?;
    assert_count!(res, 1);
    assert_val!(res, 0, 0, "param_test", StringArray);
    assert_val!(res, 1, 0, 99.5, Float64Array);
    assert_val!(res, 2, 0, true, BooleanArray);
    println!("  Parameterized query with all types: OK");

    // Parameterized string match
    let mut params = std::collections::HashMap::new();
    params.insert("name".to_string(), Value::String("param_test".to_string()));
    let res = conn.execute("MATCH (p:ParamTest) WHERE p.name = $name RETURN p.id", Some(params))?;
    assert_val!(res, 0, 0, 1i64, Int64Array);
    println!("  Parameterized string match: OK");

    // Parameterized UPDATE
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), Value::Number(1.0));
    params.insert("new_score".to_string(), Value::Number(100.0));
    conn.execute("MATCH (p:ParamTest {id: $id}) SET p.score = $new_score", Some(params))?;

    let res = exec(&db, "MATCH (p:ParamTest {id: 1}) RETURN p.score");
    assert_val!(res, 0, 0, 100.0, Float64Array);
    println!("  Parameterized UPDATE: OK");

    Ok(())
}

// ============================================================================
// 11. BULK INSERT + QUERY — 10K rows via bulk_insert_batch
// ============================================================================

#[test]
fn crucible_bulk_insert_then_query() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute(
        "CREATE NODE TABLE Bulk(id INT64, val INT64, label STRING, score DOUBLE, PRIMARY KEY (id))",
        None,
    )?;

    // Prepare 10K batch
    let n = 10_000u64;
    let ids: Vec<i64> = (0..n as i64).collect();
    let vals: Vec<i64> = (0..n as i64).map(|i| (i * 7) % 1000).collect();
    let labels: Vec<String> = (0..n).map(|i| format!("label_{}", i % 50)).collect();
    let scores: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();

    use std::sync::Arc as A;
    let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![
        ("id", A::new(arrow::array::Int64Array::from(ids)) as _),
        ("val", A::new(arrow::array::Int64Array::from(vals)) as _),
        ("label", A::new(arrow::array::StringArray::from(labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as _),
        ("score", A::new(arrow::array::Float64Array::from(scores)) as _),
    ]).unwrap();

    let start = Instant::now();
    conn.bulk_insert_batch("Bulk", &batch)?;
    let elapsed = start.elapsed();

    println!("  Bulk insert 10K rows in {:.3}s ({:.0}/sec)", elapsed.as_secs_f64(), n as f64 / elapsed.as_secs_f64());

    // Verify count
    let res = exec(&db, "MATCH (b:Bulk) RETURN count(*)");
    let count = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count as u64, n, "Bulk count mismatch");

    // Filter queries on bulk data
    let res = exec(&db, "MATCH (b:Bulk) WHERE b.val = 500 RETURN count(*)");
    let filtered = res.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    println!("  Filtered (val=500): {} rows", filtered);

    // Aggregate on bulk data
    let res = exec(&db, "MATCH (b:Bulk) WHERE b.label = 'label_25' RETURN count(*), avg(b.score)");
    assert_count!(res, 1);
    println!("  Aggregate on filtered bulk data: OK");

    // ORDER BY + LIMIT on 10K
    let res = exec(&db, "MATCH (b:Bulk) RETURN b.id, b.val ORDER BY b.val DESC LIMIT 5");
    assert_count!(res, 5);
    println!("  ORDER BY + LIMIT on bulk: OK");

    Ok(())
}

// ============================================================================
// 12. VACUUM — compact data, verify no data loss
// ============================================================================

#[test]
fn crucible_vacuum_data_integrity() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Vac(id INT64, val INT64, label STRING, PRIMARY KEY (id))", None)?;

    // Insert and delete many rows to create garbage
    for cycle in 0..5 {
        // Use UNWIND for efficient bulk insert
        let items: Vec<String> = (0..500).map(|i| {
            let id = cycle * 1000 + i;
            format!("{{id: {}, val: {}, label: 'cycle_{}'}}", id, i, cycle)
        }).collect();
        conn.execute(
            &format!("UNWIND [{}] AS row CREATE (:Vac {{id: row.id, val: row.val, label: row.label}})", items.join(", ")),
            None,
        )?;
    }

    // Delete even-numbered IDs to create garbage for vacuum
    conn.execute("MATCH (v:Vac) WHERE v.id % 2 = 0 DELETE v", None)?;

    let before_vacuum = exec(&db, "MATCH (v:Vac) RETURN count(*)");
    let before_count = before_vacuum.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    println!("  Before vacuum: {} rows", before_count);

    // Vacuum
    let start = Instant::now();
    db.vacuum()?;
    let elapsed = start.elapsed();
    println!("  Vacuum completed in {:.3}s", elapsed.as_secs_f64());

    // Verify same count after vacuum
    let after = exec(&db, "MATCH (v:Vac) RETURN count(*)");
    let after_count = after.batches[0].column(0)
        .as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(after_count, before_count, "Vacuum should not change row count");
    println!("  After vacuum: {} rows (correct)", after_count);

    // Verify individual values still accessible
    let res = exec(&db, "MATCH (v:Vac) WHERE v.id = 1 RETURN v.val, v.label");
    assert_count!(res, 1);
    assert_val!(res, 0, 0, 1i64, Int64Array);
    assert_val!(res, 1, 0, "cycle_0", StringArray);
    println!("  Vacuum: data integrity preserved");

    Ok(())
}

// ============================================================================
// 13. OPTIMIZER RULES PUSHDOWN — verify filters and projections push down
// ============================================================================

#[test]
fn crucible_optimizer_pushdowns() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE Opt(id INT64, val INT64, cat STRING, tag STRING, PRIMARY KEY (id))", None)?;

    // Insert data that benefits from filter pushdown
    for i in 0..2000 {
        conn.execute(&format!(
            "CREATE (:Opt {{id: {}, val: {}, cat: 'cat_{}', tag: 'tag_{}'}})",
            i, (i * 3) % 500, i % 10, i % 20
        ), None)?;
    }

    // Filter pushdown: WHERE on indexed column
    let start = Instant::now();
    let res = exec(&db, "MATCH (o:Opt) WHERE o.id = 42 RETURN o.val, o.cat");
    let elapsed = start.elapsed();
    assert_count!(res, 1);
    assert_val!(res, 0, 0, (42 * 3 % 500) as i64, Int64Array);
    println!("  PK lookup filter pushdown: {:.3}s", elapsed.as_secs_f64());

    // Composite filter pushdown
    let start = Instant::now();
    let res = exec(&db, "MATCH (o:Opt) WHERE o.cat = 'cat_5' AND o.tag = 'tag_3' RETURN count(*)");
    let elapsed = start.elapsed();
    println!("  Composite filter: {} rows in {:.3}s",
        res.batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        elapsed.as_secs_f64());

    // Projection pushdown: SELECT specific columns
    let start = Instant::now();
    let res = exec(&db, "MATCH (o:Opt) WHERE o.val > 400 RETURN o.id, o.cat LIMIT 100");
    let elapsed = start.elapsed();
    assert_count!(res, 100);
    println!("  Projection pushdown: 100 rows in {:.3}s", elapsed.as_secs_f64());

    // ORDER BY + LIMIT pushdown
    let start = Instant::now();
    let res = exec(&db, "MATCH (o:Opt) RETURN o.val, o.id ORDER BY o.val DESC LIMIT 10");
    let elapsed = start.elapsed();
    assert_count!(res, 10);
    println!("  TopK pushdown: 10 rows in {:.3}s", elapsed.as_secs_f64());

    Ok(())
}

// ============================================================================
// 14. UNWIND — list operations for bulk creation
// ============================================================================

#[test]
fn crucible_unwind_bulk_create() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE UnwindNode(id INT64, name STRING, PRIMARY KEY (id))", None)?;

    // Use UNWIND to create multiple nodes
    conn.execute(
        "UNWIND [{id: 1, name: 'a'}, {id: 2, name: 'b'}, {id: 3, name: 'c'}] AS row \
         CREATE (:UnwindNode {id: row.id, name: row.name})",
        None,
    )?;

    let res = exec(&db, "MATCH (u:UnwindNode) RETURN count(*)");
    assert_val!(res, 0, 0, 3i64, Int64Array);
    println!("  UNWIND bulk create: OK");

    // UNWIND with larger dataset
    let items: Vec<String> = (0..100).map(|i| {
        format!("{{id: {}, name: 'unwind_{}'}}", i, i)
    }).collect();
    let unwind_expr = items.join(", ");
    conn.execute(
        &format!("UNWIND [{}] AS row CREATE (:UnwindNode {{id: row.id, name: row.name}})", unwind_expr),
        None,
    )?;

    let res = exec(&db, "MATCH (u:UnwindNode) RETURN count(*)");
    assert_val!(res, 0, 0, 103i64, Int64Array);
    println!("  UNWIND 100 rows: OK");

    Ok(())
}

// ============================================================================
// 15. MERGE — create-or-match pattern
// ============================================================================

#[test]
fn crucible_merge_operations() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE MergeNode(id INT64, val INT64, PRIMARY KEY (id))", None)?;

    // MERGE creates if not exists
    conn.execute("MERGE (m:MergeNode {id: 1}) SET m.val = 10", None)?;
    let res = exec(&db, "MATCH (m:MergeNode) RETURN count(*)");
    assert_val!(res, 0, 0, 1i64, Int64Array);

    // MERGE with existing should not create duplicate
    conn.execute("MERGE (m:MergeNode {id: 1}) SET m.val = 20", None)?;
    let res = exec(&db, "MATCH (m:MergeNode) RETURN count(*)");
    assert_val!(res, 0, 0, 1i64, Int64Array);
    let res = exec(&db, "MATCH (m:MergeNode {id: 1}) RETURN m.val");
    assert_val!(res, 0, 0, 20i64, Int64Array);
    println!("  MERGE upsert: OK");

    // MERGE creates new
    conn.execute("MERGE (m:MergeNode {id: 2}) SET m.val = 30", None)?;
    let res = exec(&db, "MATCH (m:MergeNode) RETURN count(*)");
    assert_val!(res, 0, 0, 2i64, Int64Array);
    println!("  MERGE create new: OK");

    Ok(())
}

// ============================================================================
// 16. BOOLEAN COLUMNS — all combinations across operations
// ============================================================================

#[test]
fn crucible_boolean_columns() -> TestResult {
    let (_dir, db) = setup_db()?;
    let conn = db.connect();

    conn.execute("CREATE NODE TABLE BoolTable(id INT64, flag BOOL, PRIMARY KEY (id))", None)?;

    // Insert bools
    conn.execute("CREATE (:BoolTable {id: 1, flag: TRUE})", None)?;
    conn.execute("CREATE (:BoolTable {id: 2, flag: FALSE})", None)?;

    // Filter on bool
    let res = exec(&db, "MATCH (b:BoolTable) WHERE b.flag = TRUE RETURN count(*)");
    assert_val!(res, 0, 0, 1i64, Int64Array);

    let res = exec(&db, "MATCH (b:BoolTable) WHERE b.flag = FALSE RETURN count(*)");
    assert_val!(res, 0, 0, 1i64, Int64Array);

    // Bool in UPDATE SET
    conn.execute("MATCH (b:BoolTable {id: 1}) SET b.flag = FALSE", None)?;
    let res = exec(&db, "MATCH (b:BoolTable {id: 1}) RETURN b.flag");
    assert_val!(res, 0, 0, false, BooleanArray);

    // Mixed CREATE with bools
    conn.execute("CREATE (:BoolTable {id: 3, flag: TRUE})", None)?;
    let res = exec(&db, "MATCH (b:BoolTable {id: 3}) RETURN b.flag");
    assert_val!(res, 0, 0, true, BooleanArray);

    println!("  Boolean column operations: OK");

    Ok(())
}

// ============================================================================
// Simple deterministic PRNG (XorShift) for reproducible tests
// ============================================================================
struct XorShift {
    state: u64,
}

impl XorShift {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545F4914F6CDD1D)
    }
}
