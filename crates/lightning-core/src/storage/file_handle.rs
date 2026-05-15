use crate::storage::buffer_manager::PAGE_SIZE;
use crate::storage::free_space_manager::FreeSpaceManager;
use crate::storage::page_state::PageState;
use crate::Result;
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

pub struct FileHandle {
    pub file_id: u64,
    file: Arc<File>,
    num_pages: RwLock<u64>,
    pub(crate) page_states: RwLock<Vec<PageState>>,
    free_space_manager: RwLock<Option<Arc<FreeSpaceManager>>>,
}

impl FileHandle {
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let size = metadata.len();
        // Ensure file is a multiple of PAGE_SIZE
        if size % PAGE_SIZE as u64 != 0 {
            file.set_len((size / PAGE_SIZE as u64 + 1) * PAGE_SIZE as u64)?;
        }
        let num_pages = size.div_ceil(PAGE_SIZE as u64);

        let mut page_states = Vec::new();
        for _ in 0..num_pages {
            page_states.push(PageState::new());
        }

        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        // Use ONLY the filename for hash, not the full path
        // This ensures file_id is consistent across database sessions
        if let Some(filename) = path.file_name() {
            filename.hash(&mut hasher);
        } else {
            path.hash(&mut hasher);
        }
        let file_id = hasher.finish();

        Ok(Self {
            file_id,
            file: Arc::new(file),
            num_pages: RwLock::new(num_pages),
            page_states: RwLock::new(page_states),
            free_space_manager: RwLock::new(None),
        })
    }

    pub fn read_page(&self, page_idx: u64, buffer: &mut [u8]) -> Result<()> {
        let offset = page_idx * PAGE_SIZE as u64;
        let file_len = self.file.metadata()?.len();

        if offset >= file_len {
            buffer.fill(0);
            return Ok(());
        }

        let to_read = std::cmp::min(PAGE_SIZE as u64, file_len - offset) as usize;
        self.file.read_exact_at(&mut buffer[..to_read], offset)?;
        if to_read < PAGE_SIZE {
            buffer[to_read..].fill(0);
        }

        Ok(())
    }

    pub fn read_pages(&self, start_page: u64, num_pages: u64, buffer: &mut [u8]) -> Result<()> {
        let offset = start_page * PAGE_SIZE as u64;
        let expected_bytes = (num_pages as usize) * PAGE_SIZE;
        let file_len = self.file.metadata()?.len();

        if offset >= file_len {
            buffer[..expected_bytes].fill(0);
            return Ok(());
        }

        let to_read = std::cmp::min(expected_bytes as u64, file_len - offset) as usize;
        self.file.read_exact_at(&mut buffer[..to_read], offset)?;
        if to_read < expected_bytes {
            buffer[to_read..expected_bytes].fill(0);
        }

        Ok(())
    }

    pub fn write_page(&self, page_idx: u64, buffer: &[u8]) -> Result<()> {
        self.file
            .write_all_at(buffer, page_idx * PAGE_SIZE as u64)?;
        // No sync_all here — WAL provides durability, checkpoint handles persistence
        Ok(())
    }

    pub fn write_bytes_at(&self, offset: u64, buffer: &[u8]) -> Result<()> {
        self.file.write_all_at(buffer, offset)?;
        Ok(())
    }

    pub fn set_free_space_manager(&self, fsm: Arc<FreeSpaceManager>) {
        *self.free_space_manager.write() = Some(fsm);
    }

    pub fn add_new_page(&self) -> Result<u64> {
        {
            let fsm = self.free_space_manager.read();
            if let Some(ref fsm) = *fsm {
                if let Some(freed_page) = fsm.get_free_page(self.file_id) {
                    let mut page_states = self.page_states.write();
                    if (freed_page as usize) < page_states.len() {
                        page_states[freed_page as usize] = PageState::new();
                    }
                    return Ok(freed_page);
                }
            }
        }
        let mut num_pages = self.num_pages.write();
        let mut page_states = self.page_states.write();

        let new_idx = *num_pages;
        *num_pages += 1;
        page_states.push(PageState::new());

        Ok(new_idx)
    }

    pub fn get_num_pages(&self) -> u64 {
        *self.num_pages.read()
    }

    pub fn get_file_size(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    pub fn get_page_state(&self, page_idx: u64) -> Option<u64> {
        let states = self.page_states.read();
        states
            .get(page_idx as usize)
            .map(|s| s.get_state_and_version())
    }
    pub fn truncate(&self) -> Result<()> {
        let mut num_pages = self.num_pages.write();
        let mut page_states = self.page_states.write();
        *num_pages = 0;
        page_states.clear();
        self.file.set_len(0)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn free_page(&self, page_idx: u64) {
        {
            let fsm = self.free_space_manager.read();
            if let Some(ref fsm) = *fsm {
                fsm.record_free_page(self.file_id, page_idx);
            }
        }
    }

    pub fn reset_page_state(&self, page_idx: u64) {
        let mut page_states = self.page_states.write();
        if (page_idx as usize) < page_states.len() {
            page_states[page_idx as usize] = PageState::new();
        }
    }

    pub fn truncate_last_pages(&self, keep_count: u64) -> Result<()> {
        let mut num_pages = self.num_pages.write();
        let mut page_states = self.page_states.write();
        if keep_count >= *num_pages {
            return Ok(());
        }
        for page_idx in keep_count..*num_pages {
            self.free_page(page_idx);
        }
        let new_len = keep_count * PAGE_SIZE as u64;
        self.file.set_len(new_len)?;
        page_states.truncate(keep_count as usize);
        *num_pages = keep_count;
        Ok(())
    }
}
