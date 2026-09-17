//! The database directory is single-writer: a second open of the same path must
//! fail fast instead of corrupting state.
use lightning_core::{Database, SystemConfig};

#[test]
fn second_open_of_same_path_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");

    let first = Database::new(&path, SystemConfig::default()).expect("first open");
    let second = Database::new(&path, SystemConfig::default());
    assert!(
        second.is_err(),
        "a second open of the same database path must be rejected"
    );
    let msg = second.err().unwrap().to_string();
    assert!(
        msg.contains("already open"),
        "error should explain the conflict, got: {msg}"
    );

    // Releasing the first handle releases the lock.
    drop(first);
    let third = Database::new(&path, SystemConfig::default());
    assert!(third.is_ok(), "the lock must be released when the DB is dropped");
}
