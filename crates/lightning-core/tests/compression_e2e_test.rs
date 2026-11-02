use arrow::array::Array;
use lightning_core::{Database, SyncMode, SystemConfig};
use std::sync::Arc;

fn scan_grp(db: &Arc<Database>) -> Vec<i64> {
    let r = db
        .connect()
        .execute("MATCH (t:T) RETURN t.grp", None)
        .unwrap();
    let mut v = Vec::new();
    for b in &r.batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                v.push(a.value(i));
            }
        }
    }
    v.sort();
    v
}

#[test]
fn compression_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig {
        sync_mode: SyncMode::Off,
        ..Default::default()
    };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE T(id INT64, grp INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    // 400 rows in four long runs => RLE-friendly.
    for i in 0..400i64 {
        c.execute(&format!("CREATE (:T {{id: {i}, grp: {}}})", i / 100), None)
            .unwrap();
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
    println!(
        "after len={} first16={:?}",
        after.len(),
        &after[..after.len().min(16)]
    );
    let mut hist = std::collections::BTreeMap::new();
    for v in &after {
        *hist.entry(*v).or_insert(0i32) += 1;
    }
    println!("after hist={:?}", hist);
    let mism: Vec<usize> = (0..before.len())
        .filter(|&i| before[i] != after[i])
        .collect();
    println!(
        "mismatch count={} first={:?}",
        mism.len(),
        mism.iter()
            .take(5)
            .map(|&i| (i, before[i], after[i]))
            .collect::<Vec<_>>()
    );
    assert_eq!(after, before, "read mismatch on compressed column");

    // Reopen: metadata must persist so the compressed layout is still decoded.
    db.checkpoint().unwrap();
    drop(c);
    drop(db);
    let db2 = Database::new(dir.path(), cfg()).unwrap();
    let reopened = scan_grp(&db2);
    let re_compressed = db2
        .storage_manager()
        .read()
        .get_table("T")
        .map(|t| t.columns.iter().filter(|c| c.is_compressed()).count())
        .unwrap_or(0);
    let mut rh = std::collections::BTreeMap::new();
    for v in &reopened {
        *rh.entry(*v).or_insert(0i32) += 1;
    }
    println!(
        "reopen compressed_cols={re_compressed} hist={rh:?} len={}",
        reopened.len()
    );
    assert_eq!(
        reopened, before,
        "read mismatch after reopen (compression metadata not persisted?)"
    );

    assert!(
        compressed > 0,
        "expected at least one column to be compressed (codec not selected?)"
    );
}

#[test]
fn compressed_column_write_auto_decompresses() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig {
        sync_mode: SyncMode::Off,
        ..Default::default()
    };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE T(id INT64, grp INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    for i in 0..400i64 {
        c.execute(&format!("CREATE (:T {{id: {i}, grp: {}}})", i / 100), None)
            .unwrap();
    }
    db.vacuum().unwrap();
    assert!(
        db.storage_manager()
            .read()
            .get_table("T")
            .map(|t| t.columns.iter().any(|col| col.is_compressed()))
            .unwrap_or(false),
        "expected compression after vacuum"
    );

    // A write on the idle database transparently decompresses the column and
    // succeeds (compressed columns are an immutable generation, migrated on the
    // next write).
    c.execute("CREATE (:T {id: 9999, grp: 1})", None)
        .unwrap_or_else(|e| panic!("write on compressed db failed: {e}"));
    let vals = scan_grp(&db);
    assert_eq!(vals.len(), 401);
    assert!(vals.contains(&1));
    assert!(
        !db.has_compressed_columns(),
        "writing should have decompressed the columns"
    );
}

fn scan_v(db: &Arc<Database>) -> Vec<i64> {
    let r = db
        .connect()
        .execute("MATCH (n:N) RETURN n.v", None)
        .unwrap();
    let mut v = Vec::new();
    for b in &r.batches {
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..a.len() {
            if a.is_valid(i) {
                v.push(a.value(i));
            }
        }
    }
    v.sort();
    v
}

/// A narrow/sequential integer column (delta/bitpacking-friendly) must
/// round-trip through compression and reopen.
#[test]
fn compression_bitpacked_integers_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig {
        sync_mode: SyncMode::Off,
        ..Default::default()
    };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute(
        "CREATE NODE TABLE N(id INT64, v INT64, PRIMARY KEY(id))",
        None,
    )
    .unwrap();
    for i in 0..500i64 {
        c.execute(&format!("CREATE (:N {{id: {i}, v: {}}})", i * 3), None)
            .unwrap();
    }
    let before = scan_v(&db);
    assert_eq!(before.len(), 500);

    db.vacuum().unwrap();
    let compressed = db
        .storage_manager()
        .read()
        .get_table("N")
        .map(|t| t.columns.iter().filter(|col| col.is_compressed()).count())
        .unwrap_or(0);
    println!("compressed columns = {compressed}");
    assert_eq!(
        scan_v(&db),
        before,
        "read mismatch on compressed integer column"
    );

    // Point reads (index lookup) across the compressed column.
    for id in [0i64, 1, 100, 250, 499] {
        let pr = db
            .connect()
            .execute(&format!("MATCH (n:N {{id: {id}}}) RETURN n.v"), None)
            .unwrap();
        let got = pr.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            got,
            id * 3,
            "point read on compressed column wrong for id {id}"
        );
    }

    db.checkpoint().unwrap();
    drop(c);
    drop(db);
    let db2 = Database::new(dir.path(), cfg()).unwrap();
    assert_eq!(scan_v(&db2), before, "read mismatch after reopen");
    assert!(compressed > 0, "expected the v column to be compressed");
}

fn read_cols(db: &Arc<Database>) -> Vec<Vec<i64>> {
    let r = db
        .connect()
        .execute("MATCH (d:D) RETURN d.seq, d.narrow, d.noisy, d.rep", None)
        .unwrap();
    let mut rows = Vec::new();
    for b in &r.batches {
        for i in 0..b.num_rows() {
            let mut row = Vec::new();
            for c in b.columns() {
                let a = c
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                row.push(if a.is_valid(i) { a.value(i) } else { -1 });
            }
            rows.push(row);
        }
    }
    rows.sort();
    rows
}

#[test]
fn compression_various_distributions_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || SystemConfig {
        sync_mode: SyncMode::Off,
        ..Default::default()
    };
    let db = Database::new(dir.path(), cfg()).unwrap();
    let c = db.connect();
    c.execute("CREATE NODE TABLE D(id INT64, seq INT64, narrow INT64, noisy INT64, rep INT64, PRIMARY KEY(id))", None).unwrap();
    for i in 0..800i64 {
        c.execute(
            &format!(
                "CREATE (:D {{id: {i}, seq: {}, narrow: {}, noisy: {}, rep: {}}})",
                i * 1000,
                1000 + (i % 64),
                (i.wrapping_mul(2_654_435_761)) % 100_000,
                i / 100,
            ),
            None,
        )
        .unwrap();
    }
    let before = read_cols(&db);
    assert_eq!(before.len(), 800);

    db.vacuum().unwrap();
    let compressed = db
        .storage_manager()
        .read()
        .get_table("D")
        .map(|t| t.columns.iter().filter(|col| col.is_compressed()).count())
        .unwrap_or(0);
    println!("compressed columns = {compressed}");
    assert_eq!(read_cols(&db), before, "read mismatch after compression");

    db.checkpoint().unwrap();
    drop(c);
    drop(db);
    let db2 = Database::new(dir.path(), cfg()).unwrap();
    assert_eq!(read_cols(&db2), before, "read mismatch after reopen");
    assert!(
        compressed >= 3,
        "expected several columns compressed, got {compressed}"
    );
}
