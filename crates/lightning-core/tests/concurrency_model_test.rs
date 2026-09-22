use arrow::array::Array;
use lightning_core::{Database, SystemConfig};
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};

fn scan_ids(db: &Arc<Database>) -> BTreeSet<i64> {
    let res = db.connect().execute("MATCH (t:T) RETURN t.id", None).unwrap();
    let mut out = BTreeSet::new();
    for b in &res.batches {
        let a = b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                out.insert(a.value(i));
            }
        }
    }
    out
}

fn run_once(mode: u8, threads: u64, per: u64, seed: u64) -> (BTreeSet<i64>, BTreeSet<i64>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
    db.connect()
        .execute("CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY (id))", None)
        .unwrap();
    let barrier = Arc::new(Barrier::new(threads as usize));
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let db = Arc::clone(&db);
            let b = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let c = db.connect();
                let mut exp = BTreeSet::new();
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
                b.wait();
                for i in 0..per {
                    let id = (t * 1000 + i) as i64;
                    run(&format!("CREATE (:T {{id: {id}, v: {id}}})"));
                    exp.insert(id);
                }
                if mode >= 1 {
                    for i in (0..per).step_by(2) {
                        let id = (t * 1000 + i) as i64;
                        run(&format!("MATCH (t:T {{id: {id}}}) DELETE t"));
                        exp.remove(&id);
                    }
                }
                if mode >= 2 {
                    for i in (1..per).step_by(2) {
                        let id = (t * 1000 + i) as i64;
                        run(&format!("MATCH (t:T {{id: {id}}}) SET t.v = {}", id + 7));
                    }
                }
                exp
            })
        })
        .collect();
    let mut expected = BTreeSet::new();
    for h in hs {
        expected.extend(h.join().unwrap());
    }
    let actual = scan_ids(&db);
    let _ = seed;
    (expected, actual)
}

fn check(mode: u8, iters: u64, threads: u64, per: u64) {
    for it in 0..iters {
        let (exp, act) = run_once(mode, threads, per, it);
        if exp != act {
            let missing: Vec<i64> = exp.difference(&act).copied().collect();
            let extra: Vec<i64> = act.difference(&exp).copied().collect();
            panic!("mode={mode} it={it} expected={} actual={} missing={missing:?} extra={extra:?}", exp.len(), act.len());
        }
    }
}

#[test]
fn concurrent_single_insert() {
    check(0, 40, 2, 1);
}

#[test]
fn concurrent_inserts_disjoint() {
    check(0, 15, 4, 20);
}

#[test]
fn concurrent_inserts_deletes() {
    check(1, 15, 4, 20);
}

#[test]
fn concurrent_inserts_updates() {
    check(2, 15, 4, 20);
}

#[test]
fn concurrent_bulk_inserts_unwind() {
    let threads = 4u64;
    let per = 500i64;
    for it in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::new(dir.path(), SystemConfig::default()).unwrap();
        db.connect()
            .execute("CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY (id))", None)
            .unwrap();
        let barrier = Arc::new(Barrier::new(threads as usize));
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let db = Arc::clone(&db);
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let c = db.connect();
                    let base = (t as i64) * 100000;
                    b.wait();
                    let sql = format!(
                        "UNWIND range(0, {}) AS x CREATE (:T {{id: {base} + x, v: x}})",
                        per - 1
                    );
                    for _ in 0..100000 {
                        match c.execute(&sql, None) {
                            Ok(_) => break,
                            Err(e) if e.to_string().contains("Write-Write Conflict") => {
                                std::thread::yield_now()
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let actual = scan_ids(&db);
        let mut expected = BTreeSet::new();
        for t in 0..threads {
            for i in 0..per {
                expected.insert(t as i64 * 100000 + i);
            }
        }
        assert_eq!(actual, expected, "it={it} bulk insert mismatch");
    }
}

#[test]
fn uncommitted_writes_invisible_to_other_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::new(dir.path(), SystemConfig::default()).unwrap());
    db.connect()
        .execute("CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY (id))", None)
        .unwrap();
    let count = |db: &Arc<Database>| {
        let r = db.connect().execute("MATCH (t:T) RETURN count(*)", None).unwrap();
        r.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };
    let a = db.connect();
    a.execute("BEGIN TRANSACTION", None).unwrap();
    a.execute("CREATE (:T {id: 1, v: 1})", None).unwrap();
    assert_eq!(count(&db), 0, "uncommitted insert visible to another connection");
    a.execute("COMMIT TRANSACTION", None).unwrap();
    assert_eq!(count(&db), 1);
}
