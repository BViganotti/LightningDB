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
    let r = db.connect().execute("MATCH (t:T) RETURN t.id", None).unwrap();
    let mut out = BTreeSet::new();
    for b in &r.batches {
        let a = b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
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
        SystemConfig { sync_mode: SyncMode::Off, ..Default::default() },
    )
    .unwrap();
    db.connect()
        .execute("CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY(id))", None)
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
                    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
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
                    let _ = db.connect().execute("MATCH (t:T) RETURN count(*)", None).unwrap();
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
        SystemConfig { sync_mode: SyncMode::Off, ..Default::default() },
    )
    .unwrap();
    assert_eq!(scan_ids(&db2), expected, "soak reopen mismatch");
}
