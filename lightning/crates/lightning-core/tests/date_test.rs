use lightning_core::Database;
use lightning_core::SystemConfig;
use arrow::array::{Date32Array, TimestampMicrosecondArray, Array};
use std::sync::Arc;
use tempfile::tempdir;

fn setup_db() -> (Arc<Database>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let config = SystemConfig::default();
    let db = Database::new(dir.path(), config).unwrap();
    
    let conn = db.connect();
    conn.execute(
        "CREATE NODE TABLE Event(name STRING, date DATE, ts TIMESTAMP, PRIMARY KEY(name))",
        None,
    ).unwrap();

    (db, dir)
}

#[test]
fn test_date_functions() {
    let (db, _dir) = setup_db();
    let conn = db.connect();

    // Test date() without args
    let res = conn.query("RETURN date() AS d").unwrap();
    assert!(res.is_success());
    let batch = &res.batches[0];
    let d_col = batch.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    assert!(!d_col.is_null(0));

    // Test date(string)
    let res2 = conn.query("RETURN date('2023-05-15') AS d").unwrap();
    assert!(res2.is_success());
    let d_col2 = res2.batches[0].column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    // 2023-05-15 is 19492 days since epoch (roughly)
    assert!(d_col2.value(0) > 19000);
}

#[test]
fn test_timestamp_function() {
    let (db, _dir) = setup_db();
    let conn = db.connect();

    let res = conn.query("RETURN timestamp() AS ts").unwrap();
    assert!(res.is_success());
    let ts_col = res.batches[0].column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    assert!(ts_col.value(0) > 0);
}

#[test]
fn test_storage_date_timestamp() {
    let (db, _dir) = setup_db();
    let conn = db.connect();

    conn.query("CREATE (:Event {name: 'Party', date: date('2023-12-31'), ts: timestamp()})").unwrap();
    
    let res = conn.query("MATCH (e:Event) RETURN e.name, e.date, e.ts").unwrap();
    assert!(res.is_success());
    let batch = &res.batches[0];
    assert_eq!(batch.num_rows(), 1);
    
    let date_col = batch.column(1).as_any().downcast_ref::<Date32Array>().unwrap();
    let ts_col = batch.column(2).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    
    assert!(!date_col.is_null(0));
    assert!(!ts_col.is_null(0));
}
