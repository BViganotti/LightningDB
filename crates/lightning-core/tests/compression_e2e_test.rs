use arrow::array::Array;
use lightning_core::{Database, SyncMode, SystemConfig};
use std::sync::Arc;

fn scan_grp(db: &Arc<Database>) -> Vec<i64> {
    let r = db.connect().execute("MATCH (t:T) RETURN t.grp", None).unwrap();
    let mut v = Vec::new();
    for b in &r.batches {
        let a = b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) { v.push(a.value(i)); }
        }
    }
    v.sort();
    v
}

#[test]
fn compression_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig { sync_mode: SyncMode::Off, ..Default::default() };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute("CREATE NODE TABLE T(id INT64, grp INT64, PRIMARY KEY(id))", None).unwrap();
    // 400 rows in four long runs => RLE-friendly.
    for i in 0..400i64 {
        c.execute(&format!("CREATE (:T {{id: {i}, grp: {}}})", i / 100), None).unwrap();
    }
    let before = scan_grp(&db);
    assert_eq!(before.len(), 400);

    db.vacuum().unwrap();

    let compressed = db
        .storage_manager()
        .read()
        .get_table("T")
        .map(|t| t.columns.iter().filter(|col| col.is_compressed()).count())
        .unwrap_or(0);
    println!("compressed columns = {compressed}");

    let after = scan_grp(&db);
    println!("before[0..8]={:?}", &before[0..8]);
    println!("after len={} first16={:?}", after.len(), &after[..after.len().min(16)]);
    let mut hist = std::collections::BTreeMap::new();
    for v in &after { *hist.entry(*v).or_insert(0i32) += 1; }
    println!("after hist={:?}", hist);
    let mism: Vec<usize> = (0..before.len()).filter(|&i| before[i] != after[i]).collect();
    println!("mismatch count={} first={:?}", mism.len(), mism.iter().take(5).map(|&i| (i, before[i], after[i])).collect::<Vec<_>>());
    assert_eq!(after, before, "read mismatch on compressed column");

    // Reopen: metadata must persist so the compressed layout is still decoded.
    db.checkpoint().unwrap();
    drop(c);
    drop(db);
    let db2 = Database::new(dir.path(), cfg()).unwrap();
    let reopened = scan_grp(&db2);
    let re_compressed = db2.storage_manager().read().get_table("T").map(|t| t.columns.iter().filter(|c| c.is_compressed()).count()).unwrap_or(0);
    let mut rh = std::collections::BTreeMap::new();
    for v in &reopened { *rh.entry(*v).or_insert(0i32) += 1; }
    println!("reopen compressed_cols={re_compressed} hist={rh:?} len={}", reopened.len());
    assert_eq!(reopened, before, "read mismatch after reopen (compression metadata not persisted?)");

    assert!(compressed > 0, "expected at least one column to be compressed (codec not selected?)");
}

#[test]
fn compressed_column_rejects_write_until_decompressed() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig { sync_mode: SyncMode::Off, ..Default::default() };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute("CREATE NODE TABLE T(id INT64, grp INT64, PRIMARY KEY(id))", None).unwrap();
    for i in 0..400i64 {
        c.execute(&format!("CREATE (:T {{id: {i}, grp: {}}})", i / 100), None).unwrap();
    }
    db.vacuum().unwrap();
    assert!(
        db.storage_manager().read().get_table("T").map(|t| t.columns.iter().any(|col| col.is_compressed())).unwrap_or(false),
        "expected compression after vacuum"
    );

    // The compressed column is an immutable generation: writes are refused.
    let w = c.execute("CREATE (:T {id: 9999, grp: 1})", None);
    assert!(w.is_err(), "write to a compressed column must be rejected");

    // Decompressing restores writability.
    db.decompress().unwrap();
    c.execute("CREATE (:T {id: 9999, grp: 1})", None)
        .unwrap_or_else(|e| panic!("write after decompress failed: {e}"));
    let vals = scan_grp(&db);
    assert_eq!(vals.len(), 401);
    assert!(vals.contains(&1));
}
