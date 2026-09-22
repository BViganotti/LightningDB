use lightning_core::{Database, SyncMode, SystemConfig};
use std::sync::Arc;

fn open(dir: &std::path::Path) -> Arc<Database> {
    Database::new(
        dir,
        SystemConfig {
            sync_mode: SyncMode::Off,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn fts_mutations_are_transactional() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE T(id INT64, body STRING, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    db.storage_manager().write().create_fts_index("T").unwrap();
    let fts = db
        .storage_manager()
        .read()
        .fts_indexes
        .get("T")
        .cloned()
        .unwrap();
    let bm = db.buffer_manager().clone();
    let search = |q: &str| -> Vec<u64> {
        let tx = db.transaction_manager().begin(true).unwrap();
        let r = fts.search(q, 10, &bm, &tx).unwrap();
        db.transaction_manager().rollback(&db, &tx).unwrap();
        r.into_iter().map(|(id, _)| id).collect()
    };

    // committed batch insert -> indexed
    c.execute(
        "UNWIND [1] AS x CREATE (:T {id: x, body: 'alpha committed'})",
        None,
    )
    .unwrap();
    assert!(!search("alpha").is_empty(), "committed insert should be indexed");

    // rolled-back batch insert -> must NOT be indexed
    c.execute("BEGIN TRANSACTION", None).unwrap();
    c.execute(
        "UNWIND [2] AS x CREATE (:T {id: x, body: 'beta rolledback'})",
        None,
    )
    .unwrap();
    c.execute("ROLLBACK TRANSACTION", None).unwrap();
    assert!(
        search("beta").is_empty(),
        "rolled-back insert must not be indexed (FTS not transactional)"
    );

    // later commit still works
    c.execute(
        "UNWIND [3] AS x CREATE (:T {id: x, body: 'gamma committed'})",
        None,
    )
    .unwrap();
    assert!(!search("gamma").is_empty(), "later commit should be indexed");
}

