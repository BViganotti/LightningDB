//! Helper for the recovery regression test.
//!
//! `prep` writes 200 rows and checkpoints (advancing the durable recovery
//! watermark), then exits cleanly. `crash` reopens the database, writes 200
//! more committed rows, and calls `abort()` WITHOUT a checkpoint or a clean
//! drop — simulating a process crash. The test then reopens and asserts both
//! batches survive and the transaction counter was restored.
use lightning_core::{Database, SystemConfig};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("usage: crash_writer <db_dir> <prep|crash>");
    let mode = args.get(2).map(String::as_str).unwrap_or("prep");

    std::fs::create_dir_all(dir)?;
    let db = Database::new(Path::new(dir), SystemConfig::default())?;
    let conn = db.connect();
    // Idempotent for the second process (table already exists).
    let _ = conn.execute(
        "CREATE NODE TABLE T(id INT64, name STRING, PRIMARY KEY (id))",
        None,
    );

    match mode {
        "prep" => {
            for i in 0..200i64 {
                conn.execute(
                    &format!("CREATE (:T {{id: {i}, name: 'n{i}'}})"),
                    None,
                )?;
            }
            db.checkpoint()?;
            Ok(())
        }
        "crash" => {
            for i in 1000..1200i64 {
                conn.execute(
                    &format!("CREATE (:T {{id: {i}, name: 'm{i}'}})"),
                    None,
                )?;
            }
            // Simulate a crash: no checkpoint, no clean drop.
            std::process::abort();
        }
        other => Err(format!("unknown mode: {other}").into()),
    }
}
