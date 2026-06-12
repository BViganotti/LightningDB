use crate::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub struct FreeSpaceManager {
    pub(crate) free_pages: RwLock<HashMap<u64, VecDeque<u64>>>,
}

impl Default for FreeSpaceManager {
    fn default() -> Self {
        Self::new()
    }
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
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(Self::new());
                }
                return Err(crate::LightningError::Io(e));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_save_load_roundtrip() {
        let fsm = FreeSpaceManager::new();
        fsm.record_free_page(1, 100);
        fsm.record_free_page(1, 101);
        fsm.record_free_page(2, 200);

        let dir = tempdir().expect("internal invariant violated");
        let path = dir.path().join("fsm.bin");

        fsm.save(&path).expect("internal invariant violated");

        let loaded = FreeSpaceManager::load(&path).expect("internal invariant violated");
        assert_eq!(loaded.get_free_page(1), Some(100));
        assert_eq!(loaded.get_free_page(1), Some(101));
        assert_eq!(loaded.get_free_page(1), None);
        assert_eq!(loaded.get_free_page(2), Some(200));
        assert_eq!(loaded.get_free_page(2), None);
    }

    #[test]
    fn test_save_empty_then_load() {
        let fsm = FreeSpaceManager::new();
        let dir = tempdir().expect("internal invariant violated");
        let path = dir.path().join("empty.bin");

        fsm.save(&path).expect("internal invariant violated");
        // Empty map skips writing; load should return fresh manager
        assert!(!path.exists());

        let loaded = FreeSpaceManager::load(&path).expect("internal invariant violated");
        assert_eq!(loaded.get_free_page(1), None);
    }

    #[test]
    fn test_load_no_file() {
        let dir = tempdir().expect("internal invariant violated");
        let path = dir.path().join("nonexistent.bin");
        let fsm = FreeSpaceManager::load(&path).expect("internal invariant violated");
        assert_eq!(fsm.get_free_page(1), None);
    }

    #[test]
    fn test_record_and_reuse_order() {
        let fsm = FreeSpaceManager::new();
        fsm.record_free_page(1, 10);
        fsm.record_free_page(1, 20);
        fsm.record_free_page(1, 30);

        assert_eq!(fsm.get_free_page(1), Some(10));
        assert_eq!(fsm.get_free_page(1), Some(20));
        assert_eq!(fsm.get_free_page(1), Some(30));
        assert_eq!(fsm.get_free_page(1), None);
    }

    #[test]
    fn test_multiple_file_ids() {
        let fsm = FreeSpaceManager::new();
        fsm.record_free_page(5, 500);
        fsm.record_free_page(3, 300);
        fsm.record_free_page(5, 501);

        assert_eq!(fsm.get_free_page(3), Some(300));
        assert_eq!(fsm.get_free_page(3), None);
        assert_eq!(fsm.get_free_page(5), Some(500));
        assert_eq!(fsm.get_free_page(5), Some(501));
        assert_eq!(fsm.get_free_page(5), None);
    }
}
