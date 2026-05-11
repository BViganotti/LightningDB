use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

pub struct FreeSpaceManager {
    // file_id -> queue of free page indices
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
}
