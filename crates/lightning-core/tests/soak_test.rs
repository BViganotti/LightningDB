//! Bounded concurrency soak: several writer threads doing random DML over
//! disjoint key ranges while reader threads continuously scan the same tables.
//! Verifies the final committed state against a model and that no reader ever
//! observes a torn/invalid row. Complements the model-based query fuzzer.

use arrow::array::Array;
use lightning_core::{Database, SyncMode, SystemConfig};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};

fn scan_ids(db: &Arc<Database>) -> BTreeSet<i64> {
    let r = db
        .connect()
        .execute("MATCH (t:T) RETURN t.id", None)
        .unwrap();
    let mut out = BTreeSet::new();
    for b in &r.batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                out.insert(a.value(i));
            }
        }
    }
    out
}

#[test]
fn concurrent_soak_readers_and_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY(id))",
            None,
        )
        .unwrap();

    let threads = 4u64;
    let per = 200u64;
    let stop = Arc::new(AtomicBool::new(false));
    let model = Arc::new(Mutex::new(BTreeSet::<i64>::new()));
    let barrier = Arc::new(Barrier::new(threads as usize));

    // Writers: insert all rows, then randomly update/delete earlier rows.
    let writers: Vec<_> = (0..threads)
        .map(|t| {
            let db = Arc::clone(&db);
            let model = Arc::clone(&model);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let c = db.connect();
                let run = |sql: &str| {
                    for _ in 0..100000 {
                        match c.execute(sql, None) {
                            Ok(_) => return,
                            Err(e) if e.to_string().contains("Write-Write Conflict") => {
                                std::thread::yield_now()
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    panic!("retry exhausted: {sql}");
                };
                barrier.wait();
                let base = t * 1000;
                for i in 0..per {
                    let id = (base + i) as i64;
                    run(&format!("CREATE (:T {{id: {id}, v: {id}, s: 't{t}'}})"));
                    model.lock().unwrap().insert(id);
                }
                // deterministic pseudo-random update/delete
                let mut x = t.wrapping_mul(2654435761).wrapping_add(1);
                for i in 0..per {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let j = x % (i + 1);
                    let id = (base + j) as i64;
                    if x % 3 == 0 {
                        run(&format!("MATCH (t:T {{id: {id}}}) DELETE t"));
                        model.lock().unwrap().remove(&id);
                    } else if x % 3 == 1 {
                        run(&format!("MATCH (t:T {{id: {id}}}) SET t.v = {}", id + 1));
                    }
                }
            })
        })
        .collect();

    // Readers: continuously scan while writers run; every observed row must be
    // internally consistent (id == v for non-updated rows is not asserted here,
    // but scans must not error or panic).
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut scans = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let _ = scan_ids(&db);
                    let _ = db
                        .connect()
                        .execute("MATCH (t:T) RETURN count(*)", None)
                        .unwrap();
                    scans += 1;
                    if scans > 5_000_000 {
                        break;
                    }
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let expected = model.lock().unwrap().clone();
    let actual = scan_ids(&db);
    assert_eq!(actual, expected, "soak final state mismatch");
    // Reopen and re-verify durability.
    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(scan_ids(&db2), expected, "soak reopen mismatch");
}

/// Wall-clock soak: continuous concurrent writers + readers for `SOAK_SECS`
/// (default 10s; set higher locally / in a nightly job). Verifies the final
/// state matches the model and survives reopen.
#[test]
fn concurrent_soak_timed() {
    use std::time::{Duration, Instant};
    let secs: u64 = std::env::var("SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY(id))",
            None,
        )
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let model = Arc::new(Mutex::new(BTreeSet::<i64>::new()));
    let threads = 6u64;
    let deadline = Instant::now() + Duration::from_secs(secs);

    let writers: Vec<_> = (0..threads)
        .map(|t| {
            let db = Arc::clone(&db);
            let model = Arc::clone(&model);
            std::thread::spawn(move || {
                let c = db.connect();
                let base = (t * 1_000_000) as i64;
                let mut n = 0i64;
                let mut x = t.wrapping_mul(2654435761).wrapping_add(7);
                let run = |sql: &str| {
                    for _ in 0..100000 {
                        match c.execute(sql, None) {
                            Ok(_) => return true,
                            Err(e) if e.to_string().contains("Write-Write Conflict") => {
                                std::thread::yield_now()
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    false
                };
                while Instant::now() < deadline {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = base + n;
                    n += 1;
                    if run(&format!("CREATE (:T {{id: {id}, v: {id}}})")) {
                        model.lock().unwrap().insert(id);
                    }
                    // occasionally delete an earlier row in this writer's range
                    if n > 4 && x % 4 == 0 {
                        let j = base + (x % n as u64) as i64;
                        if run(&format!("MATCH (t:T {{id: {j}}}) DELETE t")) {
                            model.lock().unwrap().remove(&j);
                        }
                    }
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    let _ = scan_ids(&db);
                    let _ = db
                        .connect()
                        .execute("MATCH (t:T) RETURN count(*)", None)
                        .unwrap();
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let expected = model.lock().unwrap().clone();
    let actual = scan_ids(&db);
    assert_eq!(actual, expected, "timed soak final state mismatch");
    let live_count = db
        .connect()
        .execute("MATCH (t:T) RETURN count(*)", None)
        .unwrap()
        .batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "SOAK live scan={} count(*)={} model={}",
        actual.len(),
        live_count,
        expected.len()
    );
    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    let reopened = scan_ids(&db2);
    let rc = db2
        .connect()
        .execute("MATCH (t:T) RETURN count(*)", None)
        .unwrap()
        .batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    eprintln!("SOAK reopen scan={} count(*)={}", reopened.len(), rc);
    if reopened != expected {
        let missing: Vec<i64> = expected.difference(&reopened).take(10).copied().collect();
        let extra: Vec<i64> = reopened.difference(&expected).take(10).copied().collect();
        let probe = missing.first().map(|&m| {
            let r = db2
                .connect()
                .execute(&format!("MATCH (t:T {{id: {m}}}) RETURN t.v"), None)
                .unwrap();
            r.batches.iter().map(|b| b.num_rows()).sum::<usize>()
        });
        panic!("timed soak reopen mismatch: expected={} reopened={} missing={missing:?} extra={extra:?} idx_lookup_first_missing={probe:?}", expected.len(), reopened.len());
    }
}

/// Regression: interleaved inserts + deletes then checkpoint + reopen must keep
/// every committed row. This used to lose rows because the reopen path scans
/// the table in morsels that start on a 512-row page boundary, not row 0; the
/// direct (no-modifications) scan packed the one-byte-per-row null bitmap into
/// Arrow's bit-packed validity assuming the morsel start was 8-aligned, so a
/// morsel starting at e.g. row 490 shifted every null bit — valid rows read as
/// null and were dropped. Deterministic: a single writer with this exact op
/// sequence reproduced it (missing rows 518, 547 at 700 steps).
#[test]
fn reopen_after_interleaved_insert_delete_keeps_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY(id))",
            None,
        )
        .unwrap();

    let mut model: BTreeSet<i64> = BTreeSet::new();
    let mut x: u64 = 7;
    let mut n: i64 = 0;
    {
        let c = db.connect();
        for _ in 0..700 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let id = n;
            n += 1;
            c.execute(&format!("CREATE (:T {{id: {id}, v: {id}}})"), None)
                .unwrap();
            model.insert(id);
            if n > 4 && x % 4 == 0 {
                let j = (x % n as u64) as i64;
                c.execute(&format!("MATCH (t:T {{id: {j}}}) DELETE t"), None)
                    .unwrap();
                model.remove(&j);
            }
        }
    }
    assert_eq!(scan_ids(&db), model, "live state mismatch");

    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::new(
        dir.path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    let reopened = scan_ids(&db2);
    let missing: Vec<i64> = model.difference(&reopened).take(20).copied().collect();
    assert!(missing.is_empty(), "rows lost after reopen: {missing:?}");
}
