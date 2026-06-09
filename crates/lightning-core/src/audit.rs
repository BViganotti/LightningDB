use crate::{LightningError, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Simple append-only audit logger for database operations.
/// Records each query with timestamp, duration, and status.
///
/// The audit log is written to a file in the database directory
/// and is never read back by the database itself — it exists for
/// external security and compliance tooling.
pub struct AuditLogger {
    file: Mutex<File>,
    enabled: bool,
}

impl AuditLogger {
    pub fn new(db_path: &Path, enabled: bool) -> Result<Self> {
        if !enabled {
            return Ok(Self {
                file: Mutex::new(File::open("/dev/null").unwrap_or_else(|_| {
                    // Fallback: create a temp file if /dev/null fails (Windows)
                    File::create(db_path.join("audit_null.lbug")).unwrap_or_else(|_| {
                        panic!("Cannot create audit log file")
                    })
                })),
                enabled: false,
            });
        }

        let audit_path = db_path.join("audit.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .read(false)
            .open(&audit_path)
            .map_err(|e| LightningError::Database(format!("Failed to create audit log: {e}")))?;

        Ok(Self {
            file: Mutex::new(file),
            enabled: true,
        })
    }

    /// Record a query execution to the audit log.
    /// Format: ISO8601 timestamp | user | query | duration_ms | status
    pub fn record_query(
        &self,
        query: &str,
        duration_ms: u64,
        success: bool,
    ) {
        if !self.enabled {
            return;
        }

        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        let status = if success { "OK" } else { "ERROR" };
        // Truncate query to 4096 chars to prevent log abuse
        let truncated: String = query.chars().take(4096).collect();
        let escaped = truncated.replace('|', "/");

        let line = format!("{} | {} | {} | {}\n", timestamp, escaped, duration_ms, status);

        if let Ok(mut file) = self.file.lock() {
            let _ = writeln!(file, "{}", line.trim());
            let _ = file.flush();
        }
    }
}
