//! Model-based invariant tests for the storage engine.
//!
//! These drive the engine with randomized DML (insert / update / delete,
//! reopen, explicit transactions) against a simple in-memory reference model
//! and assert the engine's observable results always agree with the model.
//! Unlike hand-written tests, they explore interleavings of operations that
//! would be tedious to enumerate by hand.

use arrow::array::Array;
use lightning_core::{Database, SyncMode, SystemConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

// ── Deterministic RNG (xorshift) ───────────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ── Engine helpers ─────────────────────────────────────────────────────────

fn open(path: &std::path::Path) -> Arc<Database> {
    // Default config: the background vacuum thread runs.
    // These are correctness tests, not durability tests: skip per-commit
    // fsync (SyncMode::Off) so they run in seconds instead of minutes.
    let config = SystemConfig {
        sync_mode: SyncMode::Off,
        ..Default::default()
    };
    Database::new(path, config).unwrap()
}

fn one_i64(db: &Arc<Database>, q: &str) -> i64 {
    let res = db.connect().execute(q, None).unwrap();
    res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64 result")
        .value(0)
}

fn scan_ids(db: &Arc<Database>) -> BTreeSet<i64> {
    let res = db
        .connect()
        .execute("MATCH (t:T) RETURN t.id", None)
        .unwrap();
    let mut out = BTreeSet::new();
    for b in &res.batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id Int64");
        for i in 0..a.len() {
            if a.is_valid(i) {
                out.insert(a.value(i));
            }
        }
    }
    out
}

fn value_of(db: &Arc<Database>, id: i64) -> Option<(i64, String)> {
    let res = db
        .connect()
        .execute(
            &format!("MATCH (t:T) WHERE t.id = {id} RETURN t.v, t.s"),
            None,
        )
        .unwrap();
    if res.batches.is_empty() || res.batches.iter().map(|b| b.num_rows()).sum::<usize>() == 0 {
        return None;
    }
    let b = &res.batches[0];
    let v = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let s = b
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    Some((v.value(0), s.value(0).to_string()))
}

fn check(
    db: &Arc<Database>,
    model: &BTreeMap<i64, (i64, String)>,
    ctx: &str,
) -> Result<(), String> {
    let scanned = scan_ids(db);
    let expected: BTreeSet<i64> = model.keys().copied().collect();
    if scanned != expected {
        let extra: Vec<i64> = scanned.difference(&expected).take(16).copied().collect();
        let missing: Vec<i64> = expected.difference(&scanned).take(16).copied().collect();
        return Err(format!(
            "[{ctx}] scan set mismatch: extra_in_engine={extra:?} missing_from_engine={missing:?}"
        ));
    }

    let cnt = one_i64(db, "MATCH (t:T) RETURN count(*)");
    if cnt != model.len() as i64 {
        return Err(format!(
            "[{ctx}] count mismatch: engine={cnt} model={}",
            model.len()
        ));
    }

    for id in 0..64i64 {
        let got = one_i64(db, &format!("MATCH (t:T {{id: {id}}}) RETURN count(*)"));
        let want = if model.contains_key(&id) { 1 } else { 0 };
        if got != want {
            return Err(format!(
                "[{ctx}] equality lookup for id={id}: engine={got} model={want}"
            ));
        }
    }

    for (id, (v, s)) in model {
        let got = value_of(db, *id);
        if got != Some((*v, s.clone())) {
            return Err(format!(
                "[{ctx}] value mismatch id={id}: engine={got:?} model={:?}",
                (*v, s.clone())
            ));
        }
    }
    Ok(())
}

// ── Autocommit random DML with reopens ─────────────────────────────────────

fn run_autocommit(seed: u64, vacuum_each_step: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut db = open(&path);
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY (id))",
            None,
        )
        .unwrap();
    let mut model: BTreeMap<i64, (i64, String)> = BTreeMap::new();
    let mut rng = Rng::new(seed);
    let mut log: Vec<String> = Vec::new();

    for step in 0..400 {
        if step != 0 && step % 83 == 0 {
            // Checkpoint + reopen to exercise persistence/recovery.
            db.checkpoint().unwrap();
            drop(db);
            db = open(&path);
            if let Err(e) = check(&db, &model, &format!("seed={seed} step={step} reopen")) {
                panic!(
                    "REOPEN: {e}\nops written to /tmp/reopen_ops.txt ({} ops)",
                    log.len()
                );
            }
        }
        let c = db.connect();
        match rng.below(10) {
            0..=3 => {
                // insert
                let id = rng.below(64) as i64;
                log.push(format!("step {step}: insert id={id}"));
                if !model.contains_key(&id) {
                    let v = rng.below(10_000) as i64;
                    let s = format!("v{v}");
                    let sql = format!("CREATE (:T {{id: {id}, v: {v}, s: '{s}'}})");
                    c.execute(&sql, None).unwrap();
                    model.insert(id, (v, s));
                }
            }
            4..=7 => {
                // update
                let id = rng.below(64) as i64;
                log.push(format!("step {step}: update id={id}"));
                if let Some(e) = model.get_mut(&id) {
                    let v = rng.below(10_000) as i64;
                    let s = format!("u{v}");
                    let sql = format!("MATCH (t:T {{id: {id}}}) SET t.v = {v}, t.s = '{s}'");
                    c.execute(&sql, None).unwrap();
                    *e = (v, s);
                }
            }
            8 => {
                // delete
                let id = rng.below(64) as i64;
                log.push(format!("step {step}: delete id={id}"));
                if model.remove(&id).is_some() {
                    let sql = format!("MATCH (t:T {{id: {id}}}) DELETE t");
                    c.execute(&sql, None).unwrap();
                }
            }
            _ => {
                // delete-all-and-reinsert occasionally
                if rng.below(20) == 0 {
                    log.push(format!("step {step}: delete_all"));
                    c.execute("MATCH (t:T) DELETE t", None).unwrap();
                    model.clear();
                }
            }
        }
        if vacuum_each_step {
            // Deterministically exercise reclamation every step.
            let min_ts = db.transaction_manager().get_min_active_read_ts();
            db.buffer_manager()
                .reclaim_expired_versions(min_ts)
                .unwrap();
        }
        if let Err(e) = check(&db, &model, &format!("seed={seed} step={step}")) {
            let tail = &log[log.len().saturating_sub(40)..];
            panic!("{e}\nlast ops: {tail:#?}");
        }
    }
}

#[test]
fn model_autocommit_random_dml() {
    for seed in 1..=8u64 {
        run_autocommit(seed, false);
    }
}

/// Same model, but reclamation is forced after every step so the vacuum
/// invariant (never evict the newest committed version) is exercised
/// deterministically (regression for deleted rows resurfacing).
#[test]
fn model_autocommit_vacuum_each_step() {
    for seed in 1..=4u64 {
        run_autocommit(seed, true);
    }
}

// ── Explicit transactions with commit/rollback ─────────────────────────────

fn run_transactions(seed: u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let db = open(&path);
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY (id))",
            None,
        )
        .unwrap();
    let mut model: BTreeMap<i64, (i64, String)> = BTreeMap::new();
    let mut rng = Rng::new(seed);

    for step in 0..200 {
        let c = db.connect();
        // A transaction performs 1..4 operations, then commits or rolls back.
        let ops = 1 + rng.below(4);
        let mut overlay: BTreeMap<i64, Option<(i64, String)>> = BTreeMap::new();
        c.execute("BEGIN TRANSACTION", None).unwrap();
        for _ in 0..ops {
            let id = rng.below(64) as i64;
            let cur = overlay
                .get(&id)
                .cloned()
                .unwrap_or_else(|| model.get(&id).cloned());
            match rng.below(6) {
                0..=2 => {
                    if cur.is_none() {
                        let v = rng.below(10_000) as i64;
                        let s = format!("v{v}");
                        let sql = format!("CREATE (:T {{id: {id}, v: {v}, s: '{s}'}})");
                        c.execute(&sql, None).unwrap();
                        overlay.insert(id, Some((v, s)));
                    }
                }
                3..=4 => {
                    if let Some((_, _)) = cur {
                        let v = rng.below(10_000) as i64;
                        let s = format!("u{v}");
                        let sql = format!("MATCH (t:T {{id: {id}}}) SET t.v = {v}, t.s = '{s}'");
                        c.execute(&sql, None).unwrap();
                        overlay.insert(id, Some((v, s)));
                    }
                }
                _ => {
                    if cur.is_some() {
                        let sql = format!("MATCH (t:T {{id: {id}}}) DELETE t");
                        c.execute(&sql, None).unwrap();
                        overlay.insert(id, None);
                    }
                }
            }
        }
        let commit = rng.below(2) == 0;
        if commit {
            c.execute("COMMIT TRANSACTION", None).unwrap();
            for (id, st) in overlay {
                match st {
                    Some(v) => {
                        model.insert(id, v);
                    }
                    None => {
                        model.remove(&id);
                    }
                }
            }
        } else {
            c.execute("ROLLBACK TRANSACTION", None).unwrap();
        }
        if let Err(e) = check(
            &db,
            &model,
            &format!("tx seed={seed} step={step} commit={commit}"),
        ) {
            panic!("{e}");
        }
    }
}

#[test]
fn model_explicit_transactions() {
    for seed in 1..=4u64 {
        run_transactions(seed);
    }
}

#[test]
fn repro_delete_nullbit_lost() {
    for iter in 0..500u64 {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let c = db.connect();
        c.execute(
            "CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY (id))",
            None,
        )
        .unwrap();
        for id in 0..10i64 {
            c.execute(&format!("CREATE (:T {{id: {id}, v: {}}})", id * 10), None)
                .unwrap();
        }
        let victim = 5i64;
        c.execute(&format!("MATCH (t:T {{id: {victim}}}) DELETE t"), None)
            .unwrap();
        let ids = scan_ids(&db);
        let expected: BTreeSet<i64> = (0..10).filter(|x| *x != victim).collect();
        assert_eq!(
            ids, expected,
            "iter {iter}: deleted row leaked (null bit lost)"
        );
    }
}

#[test]
fn repro_vacuum_loses_committed_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY (id))",
        None,
    )
    .unwrap();
    for id in 0..10i64 {
        c.execute(&format!("CREATE (:T {{id: {id}, v: {}}})", id * 10), None)
            .unwrap();
    }
    c.execute("MATCH (t:T {id: 5}) DELETE t", None).unwrap();
    // Force a full vacuum (reclaim every committed, unpinned version).
    db.buffer_manager()
        .reclaim_expired_versions(u64::MAX)
        .unwrap();
    let ids = scan_ids(&db);
    let expected: BTreeSet<i64> = (0..10).filter(|x| *x != 5).collect();
    assert_eq!(
        ids, expected,
        "vacuum lost committed data / resurrected a deleted row"
    );
}

/// Clean shutdown (drop) must persist the physical row count: the tail of the
/// data must not vanish on the next open. No explicit checkpoint is taken here.
#[test]
fn shutdown_recovery_preserves_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let before;
    {
        let db = open(&path);
        let c = db.connect();
        c.execute(
            "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY (id))",
            None,
        )
        .unwrap();
        for i in 0..100i64 {
            c.execute(
                &format!("CREATE (:T {{id: {i}, v: {}, s: 'x{i}'}})", i * 3),
                None,
            )
            .unwrap();
        }
        for i in (0..100i64).step_by(7) {
            c.execute(&format!("MATCH (t:T {{id: {i}}}) DELETE t"), None)
                .unwrap();
        }
        for i in (0..100i64).step_by(5) {
            c.execute(
                &format!("MATCH (t:T {{id: {i}}}) SET t.v = {}", i + 1000),
                None,
            )
            .unwrap();
        }
        before = scan_ids(&db);
        // drop -> clean shutdown, no checkpoint()
    }
    let db = open(&path);
    assert_eq!(scan_ids(&db), before, "shutdown+reopen lost rows");
}

/// KNOWN BUG: concurrent writers on disjoint key ranges produce a final set
/// that differs from the expected union (lost deletes / resurrected rows), even
/// when every write-write conflict is retried. Reproduces reliably with 8
/// threads x 40 rows.
#[test]
fn concurrent_writers_disjoint_ranges() {
    use std::sync::Barrier;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let db = open(&path);
    db.connect()
        .execute(
            "CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY (id))",
            None,
        )
        .unwrap();

    let threads = 8u64;
    let per = 40u64;
    let barrier = std::sync::Arc::new(Barrier::new(threads as usize));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let db = std::sync::Arc::clone(&db);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                let c = db.connect();
                let base = t * 1000;
                let mut rng = Rng::new(t + 1);
                let mut expected = BTreeSet::new();
                // Concurrent writers may observe write-write conflicts, which are
                // retryable (a fresh transaction re-reads a newer snapshot).
                let run = |sql: &str| {
                    for _ in 0..1000 {
                        match c.execute(sql, None) {
                            Ok(_) => return,
                            Err(e) if e.to_string().contains("Write-Write Conflict") => {
                                std::thread::yield_now();
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                    panic!("conflict retries exhausted: {sql}");
                };
                barrier.wait();
                for i in 0..per {
                    let id = (base + i) as i64;
                    run(&format!("CREATE (:T {{id: {id}, v: {}, s: 't{t}'}})", id));
                    expected.insert(id);
                    // Randomly update or delete earlier rows in this thread's range.
                    match rng.below(4) {
                        0 => {
                            let j = rng.below(i + 1);
                            let jid = (base + j) as i64;
                            run(&format!("MATCH (t:T {{id: {jid}}}) SET t.v = {jid}"));
                        }
                        1 => {
                            let j = rng.below(i + 1);
                            let jid = (base + j) as i64;
                            run(&format!("MATCH (t:T {{id: {jid}}}) DELETE t"));
                            expected.remove(&jid);
                        }
                        _ => {}
                    }
                }
                expected
            })
        })
        .collect();

    let mut expected = BTreeSet::new();
    for h in handles {
        expected.extend(h.join().unwrap());
    }

    let actual = scan_ids(&db);
    if actual != expected {
        let missing: Vec<i64> = expected.difference(&actual).take(20).copied().collect();
        let extra: Vec<i64> = actual.difference(&expected).take(20).copied().collect();
        panic!(
            "concurrent writers mismatch: expected={} actual={} missing={:?} extra={:?}",
            expected.len(),
            actual.len(),
            missing,
            extra
        );
    }
    assert_eq!(
        one_i64(&db, "MATCH (t:T) RETURN count(*)"),
        expected.len() as i64
    );
}
