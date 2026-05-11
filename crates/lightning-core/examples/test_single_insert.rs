use lightning_core::{Database, SystemConfig};
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let db = Database::new(path, SystemConfig::default()).unwrap();

    // Create table first
    println!("=== Session 1: Creating table ===");
    let conn = db.connect();
    conn.execute("CREATE NODE TABLE Test(val INT64)", None)
        .unwrap();

    println!("=== Session 1: Inserting row ===");
    conn.execute("CREATE (:Test {val: 1})", None).unwrap();

    println!("=== Session 1: Querying (should work if buffer pool has data) ===");
    let res = conn.execute("MATCH (t:Test) RETURN t.val", None).unwrap();
    if let Some(batch) = res.batches.first() {
        if let Some(arr) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            println!("  val values: {:?}", arr.values());
        }
    }

    // Try a second insert in the same session
    println!("\n=== Session 1: Second insert ===");
    conn.execute("CREATE (:Test {val: 2})", None).unwrap();

    let res = conn.execute("MATCH (t:Test) RETURN t.val", None).unwrap();
    if let Some(batch) = res.batches.first() {
        if let Some(arr) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        {
            println!("  val values after second insert: {:?}", arr.values());
        }
    }

    println!("\n=== Files at end of session 1 ===");
    for entry in std::fs::read_dir(path).unwrap() {
        let e = entry.unwrap();
        let metadata = e.metadata().unwrap();
        if metadata.len() > 0 {
            println!("  {}: {} bytes", e.file_name().display(), metadata.len());
        }
    }
}
