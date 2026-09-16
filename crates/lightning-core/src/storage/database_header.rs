use crate::Result;
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// On-disk database header.
///
/// V2 adds a durable, CRC-protected recovery watermark. The previous format
/// stored only a commit-clock timestamp (`last_checkpoint_ts`) and no
/// transaction counter, so on restart `next_tx_id` reset to 1 while the WAL is
/// keyed by transaction id — replay then dropped committed transactions written
/// after the last checkpoint. See `last_checkpoint_tx`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseHeader {
    pub magic: [u8; 8],
    pub version: u32,
    /// Transaction-id watermark. Every transaction with `tx_id <= this` is
    /// resolved (committed or rolled back) and its committed data is on disk.
    /// WAL replay applies only records with `tx_id > last_checkpoint_tx`.
    pub last_checkpoint_tx: u64,
    /// Next transaction id to hand out. Restored on open so ids never collide
    /// with (and are never gated below) transactions from a previous run.
    pub next_tx_id: u64,
    /// Commit clock used for MVCC commit timestamps.
    pub current_ts: u64,
    /// Legacy commit-clock watermark, retained for backward compatibility.
    pub last_checkpoint_ts: u64,
}

/// V1 header layout (magic + version + commit-clock timestamp, no CRC).
#[derive(Serialize, Deserialize, Debug, Clone)]
struct DatabaseHeaderV1 {
    magic: [u8; 8],
    version: u32,
    last_checkpoint_ts: u64,
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseHeader {
    pub const MAGIC: [u8; 8] = *b"LIGHTNIN";
    pub const VERSION: u32 = 2;

    pub fn new() -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            last_checkpoint_tx: 0,
            next_tx_id: 1,
            current_ts: 1,
            last_checkpoint_ts: 0,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.magic != Self::MAGIC {
            return Err(crate::LightningError::Database(format!(
                "Invalid magic number: got {:#x?}, expected {:#x?}",
                self.magic,
                Self::MAGIC
            )));
        }
        if self.version > Self::VERSION {
            return Err(crate::LightningError::Database(format!(
                "Database version {} is newer than this software (v{}); upgrade required",
                self.version,
                Self::VERSION
            )));
        }
        if self.version == 0 {
            return Err(crate::LightningError::Database(
                "Database version 0 is invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // V2: bincode payload followed by a 4-byte crc32 over the payload.
        if buf.len() > 4 {
            let (payload, crc_bytes) = buf.split_at(buf.len() - 4);
            let expected = u32::from_le_bytes(
                crc_bytes
                    .try_into()
                    .map_err(|_| crate::LightningError::Database("bad header crc length".into()))?,
            );
            let mut hasher = Hasher::new();
            hasher.update(payload);
            if hasher.finalize() == expected {
                if let Ok(header) = bincode::deserialize::<DatabaseHeader>(payload) {
                    header.validate()?;
                    return Ok(header);
                }
            }
        }

        // V1 fallback: migrate a legacy header. Its watermark was a commit
        // clock, not a tx id, so it cannot be trusted as a replay gate; use 0
        // (replay everything) — over-replay is idempotent, under-replay loses
        // data.
        if let Ok(v1) = bincode::deserialize::<DatabaseHeaderV1>(&buf) {
            if v1.magic == Self::MAGIC {
                tracing::warn!(
                    "Migrating legacy database header (v{}) to v{}: recovery watermark reset",
                    v1.version,
                    Self::VERSION
                );
                return Ok(Self {
                    magic: Self::MAGIC,
                    version: Self::VERSION,
                    last_checkpoint_tx: 0,
                    next_tx_id: 1,
                    current_ts: 1,
                    last_checkpoint_ts: v1.last_checkpoint_ts,
                });
            }
        }

        Err(crate::LightningError::Database(
            "database header is corrupt (bad magic, version, or CRC)".into(),
        ))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let buf =
            bincode::serialize(self).map_err(|e| crate::LightningError::Database(e.to_string()))?;
        let mut hasher = Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();

        // Write to a temporary file first, then atomically rename so a crash
        // mid-write cannot leave a torn header.
        let tmp_path = path.with_extension("header.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&buf)?;
            file.write_all(&crc.to_le_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, path)?;

        // Fsync the containing directory so the rename itself is durable; a
        // header that "exists" only in the page cache can be lost on power
        // failure, silently shifting the recovery window.
        if let Some(dir) = path.parent() {
            if let Ok(dir_file) = File::open(dir) {
                let _ = dir_file.sync_all();
            }
        }
        Ok(())
    }
}
