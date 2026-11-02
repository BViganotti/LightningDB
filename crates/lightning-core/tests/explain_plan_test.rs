use arrow::array::{Array, StringArray};
use lightning_core::{Database, SyncMode, SystemConfig};
use std::sync::Arc;

fn setup() -> Arc<Database> {
    let db = Database::new(
        tempfile::tempdir().unwrap().into_path(),
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap();
    let c = db.connect();
    c.execute("CREATE NODE TABLE P(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE NODE TABLE M(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))", None)
        .unwrap();
    c.execute("CREATE REL TABLE LIKES(FROM P TO M)", None)
        .unwrap();
    c.execute("CREATE REL TABLE TAGGED(FROM M TO T)", None)
        .unwrap();
    db
}

fn explain(db: &Arc<Database>, q: &str) -> String {
    let r = db.connect().execute(&format!("EXPLAIN {q}"), None).unwrap();
    let mut out = String::new();
    for b in &r.batches {
        for col in b.columns() {
            if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..a.len() {
                    out.push_str(a.value(i));
                }
            }
        }
    }
    out
}

/// A node re-referenced across comma-separated patterns must join on the
/// existing binding, not be re-scanned into a cartesian product.
#[test]
fn multi_hop_match_has_no_cartesian_product() {
    let db = setup();
    let plan = explain(
        &db,
        "MATCH (p:P)-[:LIKES]->(m), (m)-[:TAGGED]->(t) RETURN p",
    );
    println!("{plan}");
    assert!(
        !plan.contains("CrossJoin"),
        "multi-hop MATCH must not produce a cartesian product:\n{plan}"
    );
    assert_eq!(
        plan.matches("Scan M AS m").count(),
        1,
        "node m must be scanned exactly once:\n{plan}"
    );
}

/// The subquery/EXISTS path must also avoid the cartesian product.
#[test]
fn exists_subquery_has_no_cartesian_product() {
    let db = setup();
    let plan = explain(
        &db,
        "MATCH (p:P) WHERE EXISTS { MATCH (p)-[:LIKES]->(m), (m)-[:TAGGED]->(t) } RETURN p",
    );
    println!("{plan}");
    assert!(
        !plan.contains("CrossJoin"),
        "EXISTS subquery must not cartesian:\n{plan}"
    );
}

/// Genuinely disconnected patterns are still a (legitimate) cross product.
#[test]
fn disconnected_pattern_is_cartesian() {
    let db = setup();
    let plan = explain(&db, "MATCH (p:P), (m:M) RETURN p, m");
    println!("{plan}");
    assert!(
        plan.contains("CrossJoin"),
        "disconnected pattern should cross join:\n{plan}"
    );
}

/// EXPLAIN dumps the plan without executing (empty tables still yield a plan).
#[test]
fn explain_returns_plan_without_executing() {
    let db = setup();
    let plan = explain(&db, "MATCH (t:T) RETURN t");
    assert!(
        plan.contains("Scan T"),
        "expected to see the scan in:\n{plan}"
    );
}

fn rows(db: &Arc<Database>, q: &str) -> Vec<Vec<i64>> {
    let r = db.connect().execute(q, None).unwrap();
    let mut out = Vec::new();
    for b in &r.batches {
        for i in 0..b.num_rows() {
            let mut row = Vec::new();
            for col in b.columns() {
                if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    row.push(if a.is_valid(i) { a.value(i) } else { -1 });
                }
            }
            out.push(row);
        }
    }
    out
}

/// The re-referenced-variable fix must not change query results.
#[test]
fn multi_hop_returns_correct_rows() {
    let db = setup();
    let c = db.connect();
    c.execute("CREATE (:P {id: 1})", None).unwrap();
    c.execute("CREATE (:M {id: 10})", None).unwrap();
    c.execute("CREATE (:T {id: 100})", None).unwrap();
    c.execute(
        "MATCH (p:P {id: 1}), (m:M {id: 10}) CREATE (p)-[:LIKES]->(m)",
        None,
    )
    .unwrap();
    c.execute(
        "MATCH (m:M {id: 10}), (t:T {id: 100}) CREATE (m)-[:TAGGED]->(t)",
        None,
    )
    .unwrap();

    let r = rows(
        &db,
        "MATCH (p:P)-[:LIKES]->(m), (m)-[:TAGGED]->(t) RETURN p.id, m.id, t.id",
    );
    assert_eq!(r, vec![vec![1, 10, 100]], "multi-hop result wrong: {r:?}");
}

/// Pre-existing bug (not introduced here): a correlated `EXISTS` semi-join
/// returns no rows at execution time even though the plan now carries the
/// correlation (`p.0 = __sub_p.0`). The semi-join probe does not match the
/// build key under projection pushdown. Tracked separately.
#[test]
fn exists_semi_join_returns_rows() {
    let db = setup();
    let c = db.connect();
    c.execute("CREATE (:P {id: 1})", None).unwrap();
    c.execute("CREATE (:M {id: 10})", None).unwrap();
    c.execute("CREATE (:T {id: 100})", None).unwrap();
    c.execute(
        "MATCH (p:P {id: 1}), (m:M {id: 10}) CREATE (p)-[:LIKES]->(m)",
        None,
    )
    .unwrap();
    c.execute(
        "MATCH (m:M {id: 10}), (t:T {id: 100}) CREATE (m)-[:TAGGED]->(t)",
        None,
    )
    .unwrap();
    let r2 = rows(
        &db,
        "MATCH (p:P) WHERE EXISTS { MATCH (p)-[:LIKES]->(m), (m)-[:TAGGED]->(t) } RETURN p.id",
    );
    assert_eq!(r2, vec![vec![1]], "EXISTS result wrong: {r2:?}");
}
