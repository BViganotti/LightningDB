use crate::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub struct FreeSpaceManager {
    pub(crate) free_pages: RwLock<HashMap<u64, VecDeque<u64>>>,
}

impl FreeSpaceManager {
    pub fn new() -> Self {
        Self {
            free_pages: RwLock::new(HashMap::new()),
        }
    }

    pub fn record_free_page(&self, file_id: u64, page_idx: u64) {
        let mut map = self.free_pages.write();
        map.entry(file_id).or_default().push_back(page_idx);
    }

    pub fn get_free_page(&self, file_id: u64) -> Option<u64> {
        let mut map = self.free_pages.write();
        map.get_mut(&file_id)?.pop_front()
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let map = self.free_pages.read();
        if map.is_empty() {
            return Ok(());
        }
        let buf =
            bincode::serialize(&*map).map_err(|e| crate::LightningError::Database(e.to_string()))?;
        let mut file = File::create(path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(Self::new()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let map: HashMap<u64, VecDeque<u64>> = bincode::deserialize(&buf)
            .map_err(|e| crate::LightningError::Database(e.to_string()))?;
        Ok(Self {
            free_pages: RwLock::new(map),
        })
    }
}
