//! Deterministic model-based fuzzing: build a random graph with a mix of column
//! types and DML, keep a simple in-memory model, then run a battery of queries
//! whose expected answers are computed from the model. This is the harness that
//! would have caught the correlated-EXISTS, projection-pushdown and multi-hop
//! cartesian bugs; it compares *results*, not just that queries don't error.

use lightning_core::{Database, SyncMode, SystemConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(6364136223846793005).wrapping_add(1))
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

fn one_i64(db: &Arc<Database>, q: &str) -> i64 {
    let r = db.connect().execute(q, None).unwrap();
    if r.batches.is_empty() || r.batches[0].num_rows() == 0 {
        return 0;
    }
    let a = r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    a.value(0)
}

fn run_seed(seed: u64) {
    let db = Database::new(
        tempfile::tempdir().unwrap().into_path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE A(id INT64, v INT64, s STRING, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    c.execute(
        "CREATE NODE TABLE B(id INT64, v INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    c.execute("CREATE NODE TABLE C(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE REL TABLE R(FROM A TO B)", None).unwrap();
    c.execute("CREATE REL TABLE R2(FROM B TO C)", None).unwrap();

    let mut rng = Rng::new(seed);
    let mut a: BTreeMap<i64, (i64, String)> = BTreeMap::new();
    let mut b: BTreeMap<i64, i64> = BTreeMap::new();
    let mut cc: BTreeSet<i64> = BTreeSet::new();
    let mut r: BTreeSet<(i64, i64)> = BTreeSet::new(); // a_id -> b_id
    let mut r2: BTreeSet<(i64, i64)> = BTreeSet::new(); // b_id -> c_id
    let mut next_id = 0i64;
    let retry = |sql: &str| {
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

    for _ in 0..60 {
        match rng.below(10) {
            0..=3 => {
                // insert A
                let id = next_id;
                next_id += 1;
                let v = rng.below(10) as i64;
                let s = format!("s{}", rng.below(5));
                retry(&format!("CREATE (:A {{id: {id}, v: {v}, s: '{s}'}})"));
                a.insert(id, (v, s));
            }
            4..=5 => {
                // insert B
                let id = next_id;
                next_id += 1;
                let v = rng.below(10) as i64;
                retry(&format!("CREATE (:B {{id: {id}, v: {v}}})"));
                b.insert(id, v);
            }
            6 => {
                // insert C
                let id = next_id;
                next_id += 1;
                retry(&format!("CREATE (:C {{id: {id}}})"));
                cc.insert(id);
            }
            7 => {
                // edge A->B
                if let Some(&aid) = a.keys().nth(rng.below(a.len().max(1) as u64) as usize) {
                    if let Some(&bid) = b.keys().nth(rng.below(b.len().max(1) as u64) as usize) {
                        if !r.contains(&(aid, bid)) {
                            retry(&format!("MATCH (x:A {{id: {aid}}}), (y:B {{id: {bid}}}) CREATE (x)-[:R]->(y)"));
                            r.insert((aid, bid));
                        }
                    }
                }
            }
            8 => {
                // edge B->C
                if let Some(&bid) = b.keys().nth(rng.below(b.len().max(1) as u64) as usize) {
                    if let Some(&cid) = cc.iter().nth(rng.below(cc.len().max(1) as u64) as usize) {
                        if !r2.contains(&(bid, cid)) {
                            retry(&format!("MATCH (x:B {{id: {bid}}}), (y:C {{id: {cid}}}) CREATE (x)-[:R2]->(y)"));
                            r2.insert((bid, cid));
                        }
                    }
                }
            }
            _ => {
                // update / delete A
                if a.is_empty() {
                    continue;
                }
                let aid = *a.keys().nth(rng.below(a.len() as u64) as usize).unwrap();
                if rng.below(2) == 0 {
                    let v = rng.below(10) as i64;
                    retry(&format!("MATCH (x:A {{id: {aid}}}) SET x.v = {v}"));
                    a.get_mut(&aid).unwrap().0 = v;
                } else {
                    retry(&format!("MATCH (x:A {{id: {aid}}}) DETACH DELETE x"));
                    a.remove(&aid);
                    r.retain(|(x, _)| *x != aid);
                }
            }
        }
    }

    // ---- Queries vs the model ----
    assert_eq!(
        one_i64(&db, "MATCH (n:A) RETURN count(*)"),
        a.len() as i64,
        "seed {seed}: A count"
    );
    assert_eq!(
        one_i64(&db, "MATCH (n:B) RETURN count(*)"),
        b.len() as i64,
        "seed {seed}: B count"
    );

    let x = rng.below(10) as i64;
    assert_eq!(
        one_i64(&db, &format!("MATCH (n:A) WHERE n.v > {x} RETURN count(*)")),
        a.values().filter(|(v, _)| *v > x).count() as i64,
        "seed {seed}: A filter"
    );

    // join A->B (one row per distinct edge)
    assert_eq!(
        one_i64(&db, "MATCH (p:A)-[:R]->(q:B) RETURN count(*)"),
        r.len() as i64,
        "seed {seed}: A-R->B join"
    );

    // multi-hop A->B->C
    let multi: usize = r
        .iter()
        .map(|(_, bid)| r2.iter().filter(|(b2, _)| b2 == bid).count())
        .sum();
    assert_eq!(
        one_i64(
            &db,
            "MATCH (p:A)-[:R]->(q:B), (q)-[:R2]->(c:C) RETURN count(*)"
        ),
        multi as i64,
        "seed {seed}: multi-hop join"
    );

    // correlated EXISTS
    let with_edge = a
        .keys()
        .filter(|aid| r.iter().any(|(x, _)| x == *aid))
        .count();
    assert_eq!(
        one_i64(
            &db,
            "MATCH (p:A) WHERE EXISTS { MATCH (p)-[:R]->(q:B) } RETURN count(*)"
        ),
        with_edge as i64,
        "seed {seed}: correlated EXISTS"
    );

    // string filter
    assert_eq!(
        one_i64(&db, "MATCH (n:A) WHERE n.s = 's2' RETURN count(*)"),
        a.values().filter(|(_, s)| s == "s2").count() as i64,
        "seed {seed}: string filter"
    );
}

#[test]
fn model_random_queries() {
    for seed in 1..=60u64 {
        run_seed(seed);
    }
}
