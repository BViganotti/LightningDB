//! Real crash-durability test: a child process writes single-row autocommit
//! transactions in `SyncMode::Normal`, printing `ACK <id>` after each commit.
//! The parent SIGKILLs it mid-stream, reopens the database and checks that
//! every acknowledged (i.e. committed) row survived — no committed data may be
//! lost and the database must reopen without corruption.

use lightning_core::{Database, SystemConfig};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Child entry point. Only does work when launched as a subprocess.
#[test]
fn crash_writer_child() {
    let Ok(path) = std::env::var("CRASH_DB") else {
        return; // normal `cargo test` run: nothing to do
    };
    let rows: i64 = std::env::var("CRASH_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let start: i64 = std::env::var("CRASH_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let db = Database::new(&path, SystemConfig::default()).unwrap();
    let c = db.connect();
    for i in start..start + rows {
        c.execute(&format!("CREATE (:T {{id: {i}, v: {i}}})"), None)
            .unwrap();
        println!("ACK {i}");
        std::io::stdout().flush().unwrap();
    }
    std::process::exit(0);
}

fn count_rows(db: &std::sync::Arc<Database>) -> i64 {
    let r = db
        .connect()
        .execute("MATCH (t:T) RETURN count(*)", None)
        .unwrap();
    r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn sigkill_preserves_all_committed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::new(&path, SystemConfig::default()).unwrap();
        db.connect()
            .execute("CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY(id))", None)
            .unwrap();
    }

    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args(["crash_writer_child", "--nocapture", "--test-threads=1"])
        .env("CRASH_DB", &path)
        .env("CRASH_ROWS", "1000000")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    // Consume ACKs for a short window, then hard-kill mid-write.
    let mut last_ack: i64 = -1;
    {
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let start = Instant::now();
        let mut line = String::new();
        while start.elapsed() < Duration::from_secs(3) {
            line.clear();
            if reader.read_line(&mut line).unwrap() == 0 {
                break;
            }
            if let Some(rest) = line.strip_prefix("ACK ") {
                last_ack = rest.trim().parse().unwrap_or(last_ack);
            }
        }
    }
    // SIGKILL: no clean shutdown, no Drop, no checkpoint.
    unsafe {
        libc::kill(child.id() as i32, libc::SIGKILL);
    }
    let _ = child.wait();

    assert!(last_ack >= 0, "child never committed anything");

    // Reopen (WAL replay) and verify durable data.
    let db = Database::new(&path, SystemConfig::default()).unwrap();
    let recovered = count_rows(&db);
    println!("acked={last_ack} recovered={recovered}");
    assert!(
        recovered >= last_ack + 1,
        "committed data lost across SIGKILL: acked through {last_ack} ({} rows) but recovered {recovered}",
        last_ack + 1
    );

    // Data must be intact, not just present: every recovered v == id.
    let r = db
        .connect()
        .execute("MATCH (t:T) WHERE t.v <> t.id RETURN count(*)", None)
        .unwrap();
    let bad = r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(bad, 0, "{bad} rows corrupted after crash recovery");
}

/// Repeated kill/recover cycles under write load: each cycle the child writes
/// from where the previous recovery left off, is SIGKILLed mid-stream, and the
/// reopened database must contain every acknowledged row with intact data. This
/// exercises WAL replay across many crashes, not just one.
#[test]
fn sigkill_repeated_cycles_preserve_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::new(&path, SystemConfig::default()).unwrap();
        db.connect()
            .execute("CREATE NODE TABLE T(id INT64, v INT64, PRIMARY KEY(id))", None)
            .unwrap();
    }
    let exe = std::env::current_exe().unwrap();
    let mut start = 0i64;
    for cycle in 0..4 {
        let mut child = Command::new(&exe)
            .args(["crash_writer_child", "--nocapture", "--test-threads=1"])
            .env("CRASH_DB", &path)
            .env("CRASH_START", start.to_string())
            .env("CRASH_ROWS", "1000000")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut last_ack = start - 1;
        {
            let mut reader = BufReader::new(child.stdout.take().unwrap());
            let begin = Instant::now();
            let mut line = String::new();
            while begin.elapsed() < Duration::from_secs(2) {
                line.clear();
                if reader.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                if let Some(rest) = line.strip_prefix("ACK ") {
                    last_ack = rest.trim().parse().unwrap_or(last_ack);
                }
            }
        }
        unsafe {
            libc::kill(child.id() as i32, libc::SIGKILL);
        }
        let _ = child.wait();

        let db = Database::new(&path, SystemConfig::default()).unwrap();
        let recovered = count_rows(&db);
        assert!(
            recovered >= last_ack + 1,
            "cycle {cycle}: lost committed rows (acked through {last_ack}, recovered {recovered})"
        );
        let bad = db
            .connect()
            .execute("MATCH (t:T) WHERE t.v <> t.id RETURN count(*)", None)
            .unwrap()
            .batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(bad, 0, "cycle {cycle}: {bad} corrupted rows after recovery");
        start = recovered;
    }
}
