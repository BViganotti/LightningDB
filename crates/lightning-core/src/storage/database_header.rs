use crate::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub last_checkpoint_ts: u64,
}

impl DatabaseHeader {
    pub const MAGIC: [u8; 8] = *b"LIGHTNIG"; // Misspelled just like Ladybug (wait, no, let's use LIGHTNIN)
    pub const VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            last_checkpoint_ts: 0,
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let header: DatabaseHeader = bincode::deserialize(&buf)
            .map_err(|e| crate::LightningError::Database(e.to_string()))?;
        if header.magic != Self::MAGIC {
            return Err(crate::LightningError::Database(
                "Invalid magic number".into(),
            ));
        }
        Ok(header)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let buf =
            bincode::serialize(self).map_err(|e| crate::LightningError::Database(e.to_string()))?;
        let mut file = File::create(path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
}
