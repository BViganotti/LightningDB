//! Recovery regression tests.
//!
//! Root bug: `next_tx_id`/`current_ts` reset to 1 on every open, while the WAL
//! is keyed by transaction id and replay was gated on a *commit-clock*
//! timestamp. Committed transactions written after the last checkpoint could be
//! silently discarded on restart. The fix persists and restores the counters and
//! gates replay on a durable transaction-id watermark.
use arrow::array::{Array, Int64Array};
use lightning_core::{Database, SystemConfig};
use std::process::Command;

fn count_rows(db: &std::sync::Arc<Database>, table: &str) -> i64 {
    let res = db
        .connect()
        .execute(&format!("MATCH (t:{table}) RETURN count(*)"), None)
        .expect("count query");
    res.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

/// Deterministic root-cause regression: transaction counters and the recovery
/// watermark must persist across a clean reopen rather than resetting to 1.
#[test]
fn counters_and_watermark_persist_across_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let next_before;
    let rows = 100i64;
    {
        let db = Database::new(dir.path(), SystemConfig::default()).expect("create");
        let conn = db.connect();
        conn.execute(
            "CREATE NODE TABLE T(id INT64, name STRING, PRIMARY KEY (id))",
            None,
        )
        .expect("create table");
        for i in 0..rows {
            conn.execute(&format!("CREATE (:T {{id: {i}, name: 'n{i}'}})"), None)
                .expect("insert");
        }
        db.checkpoint().expect("checkpoint");
        next_before = db.transaction_manager().next_tx_id();
        assert!(
            next_before > rows as u64,
            "counter should have advanced, got {next_before}"
        );
    }

    let db = Database::new(dir.path(), SystemConfig::default()).expect("reopen");
    let next_after = db.transaction_manager().next_tx_id();
    assert!(
        next_after > rows as u64,
        "next_tx_id must be restored across reopen (got {next_after}); a reset to 1 is the bug"
    );
    assert!(
        db.header().read().last_checkpoint_tx > rows as u64,
        "tx watermark must be persisted"
    );
    assert_eq!(count_rows(&db, "T"), rows, "rows must survive reopen");
}

/// Data survival across an abrupt process crash (no checkpoint, no clean drop).
#[test]
fn committed_rows_survive_process_abort() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bin = env!("CARGO_BIN_EXE_crash_writer");

    let prep = Command::new(bin)
        .arg(dir.path())
        .arg("prep")
        .status()
        .expect("run prep");
    assert!(prep.success(), "prep run should succeed");

    // Crash run: writes 200 more committed rows then aborts.
    let crash = Command::new(bin)
        .arg(dir.path())
        .arg("crash")
        .status()
        .expect("run crash");
    assert!(!crash.success(), "crash run should terminate abnormally");

    let db = Database::new(dir.path(), SystemConfig::default()).expect("reopen");
    assert_eq!(
        count_rows(&db, "T"),
        400,
        "both committed batches must survive the crash"
    );
    // The counter must not have been reset to 1 by the restart.
    assert!(
        db.transaction_manager().next_tx_id() > 100,
        "next_tx_id must not reset to 1 after a crash+reopen"
    );
}
