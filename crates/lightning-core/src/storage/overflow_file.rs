use crate::storage::buffer_manager::BufferManager;
use crate::storage::file_handle::FileHandle;
use crate::Result;
use std::sync::Arc;

pub struct OverflowFile {
    file_handle: Arc<FileHandle>,
    buffer_manager: Arc<BufferManager>,
}

impl OverflowFile {
    pub fn new(file_handle: Arc<FileHandle>, buffer_manager: Arc<BufferManager>) -> Self {
        Self {
            file_handle,
            buffer_manager,
        }
    }

    pub fn read_string(
        &self,
        page_idx: u32,
        offset: u16,
        len: u32,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<String> {
        const PAGE_SIZE: usize = 4096;
        const NEXT_PTR_SIZE: usize = 4;
        const USABLE_SIZE: usize = PAGE_SIZE - NEXT_PTR_SIZE;

        if len == 0 {
            return Ok(String::new());
        }

        let mut result = String::with_capacity(len as usize);
        let mut current_page_idx = page_idx;
        let mut current_offset = offset as usize;
        let mut remaining = len as usize;

        while remaining > 0 {
            if current_offset > USABLE_SIZE {
                return Err(LightningError::Internal(format!(
                    "read_string: offset {} exceeds usable page size {}", current_offset, USABLE_SIZE
                )));
            }

            let page = self.buffer_manager.pin_page(
                self.file_handle.clone(),
                current_page_idx as u64,
                tx,
            )?;
            let page_data = page.as_slice();
            if page_data.len() < PAGE_SIZE {
                return Err(LightningError::Internal(format!(
                    "read_string: page {} too short: {} bytes", current_page_idx, page_data.len()
                )));
            }

            let to_read = std::cmp::min(remaining, USABLE_SIZE - current_offset);
            let end = current_offset + to_read;
            if end > page_data.len() {
                return Err(LightningError::Internal(format!(
                    "read_string: slice end {} exceeds page size {} on page {}",
                    end, page_data.len(), current_page_idx
                )));
            }
            let slice = &page_data[current_offset..end];
            result.push_str(
                std::str::from_utf8(slice)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?,
            );

            remaining -= to_read;
            if remaining > 0 {
                let next_ptr_end = USABLE_SIZE + NEXT_PTR_SIZE;
                if next_ptr_end > page_data.len() {
                    return Err(LightningError::Internal(format!(
                        "read_string: page {} too short for next pointer", current_page_idx
                    )));
                }
                let next_page_bytes = &page_data[USABLE_SIZE..next_ptr_end];
                let next_page_idx = u32::from_le_bytes(
                    next_page_bytes.try_into()
                        .map_err(|_| LightningError::Internal("read_string: invalid next page pointer".into()))?
                );
                current_page_idx = next_page_idx;
                current_offset = 0;
            }
        }

        Ok(result)
    }

    pub fn write_string(
        &self,
        s: &str,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<(u32, u16)> {
        const PAGE_SIZE: usize = 4096;
        const USABLE_SIZE: usize = PAGE_SIZE - 4;

        let data = s.as_bytes();
        let data_len = data.len();
        if data_len == 0 {
            return Ok((0, 0));
        }

        let first_page_idx = self.file_handle.add_new_page()? as u32;
        let mut current_page_idx = first_page_idx;
        let mut offset = 0;

        while offset < data_len {
            let frame = self.buffer_manager.create_new_version(
                self.file_handle.clone(),
                current_page_idx as u64,
                tx,
            )?;

            let to_write = std::cmp::min(data_len - offset, USABLE_SIZE);
            unsafe {
                let ptr = frame.as_ptr();
                std::ptr::copy_nonoverlapping(data.as_ptr().add(offset), ptr, to_write);
            }

            offset += to_write;

            if offset < data_len {
                let next_page_idx = self.file_handle.add_new_page()? as u32;
                let next_bytes = next_page_idx.to_le_bytes();
                unsafe {
                    let ptr = frame.as_ptr();
                    std::ptr::copy_nonoverlapping(next_bytes.as_ptr(), ptr.add(USABLE_SIZE), 4);
                }
                self.buffer_manager.log_page_update(
                    self.file_handle.file_id,
                    current_page_idx as u64,
                    frame.as_slice(),
                )?;
                self.buffer_manager.unpin_page(&self.file_handle, current_page_idx as u64, frame);
                current_page_idx = next_page_idx;
            } else {
                self.buffer_manager.log_page_update(
                    self.file_handle.file_id,
                    current_page_idx as u64,
                    frame.as_slice(),
                )?;
                self.buffer_manager.unpin_page(&self.file_handle, current_page_idx as u64, frame);
            }
        }

        Ok((first_page_idx, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_manager::BufferManager;
    use crate::storage::file_handle::FileHandle;
    use crate::storage::wal::WAL;
    use crate::transaction::TransactionManager;
    use crate::SyncMode;

    const PAGE_SIZE: usize = 4096;
    const USABLE_SIZE: usize = PAGE_SIZE - 4;

    fn setup() -> (Arc<FileHandle>, Arc<BufferManager>, Arc<TransactionManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow_test.lbug");
        let fh = Arc::new(FileHandle::open(&path).unwrap());
        let wal = Arc::new(WAL::new(dir.path(), SyncMode::Normal).unwrap());
        let tm = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        tm.set_self_weak(Arc::downgrade(&tm));
        let bm = Arc::new(BufferManager::new(256, Some(wal), false, 0, 0.0));
        (fh, bm, tm, dir)
    }

    #[test]
    fn test_write_and_read_short_string() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "Hello, Overflow!";
        let (page_idx, offset) = of.write_string(s, &tx).unwrap();
        assert_eq!(page_idx, 0, "first allocated page should be 0");
        assert_eq!(offset, 0, "offset should be 0 for single-page writes");

        let roundtrip = of.read_string(page_idx, offset, s.len() as u32, &tx).unwrap();
        assert_eq!(roundtrip, s);
    }

    #[test]
    fn test_write_and_read_empty_string() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let (page_idx, offset) = of.write_string("", &tx).unwrap();
        assert_eq!(page_idx, 0, "empty string returns (0,0)");
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_write_and_read_exact_one_page() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "x".repeat(USABLE_SIZE);
        let (page_idx, offset) = of.write_string(&s, &tx).unwrap();
        assert_eq!(page_idx, 0);
        assert_eq!(offset, 0);

        let roundtrip = of.read_string(page_idx, offset, s.len() as u32, &tx).unwrap();
        assert_eq!(roundtrip.len(), USABLE_SIZE);
        assert_eq!(roundtrip, s);
    }

    #[test]
    fn test_write_and_read_multi_page() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "y".repeat(USABLE_SIZE + 100);
        let (page_idx, offset) = of.write_string(&s, &tx).unwrap();
        assert_eq!(page_idx, 0);
        assert_eq!(offset, 0);

        let roundtrip = of.read_string(page_idx, offset, s.len() as u32, &tx).unwrap();
        assert_eq!(roundtrip.len(), s.len());
        assert_eq!(roundtrip, s);
    }

    #[test]
    fn test_write_and_read_three_pages() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "z".repeat(USABLE_SIZE * 2 + USABLE_SIZE / 2);
        let (page_idx, offset) = of.write_string(&s, &tx).unwrap();
        assert_eq!(page_idx, 0);

        let roundtrip = of.read_string(page_idx, offset, s.len() as u32, &tx).unwrap();
        assert_eq!(roundtrip, s);
    }

    #[test]
    fn test_write_and_read_unicode() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "🚀 Rust 🔥 Lightning ⚡ DB 🌩️ 引擎 🦀";
        let (page_idx, offset) = of.write_string(s, &tx).unwrap();
        assert_eq!(page_idx, 0);

        let roundtrip = of.read_string(page_idx, offset, s.len() as u32, &tx).unwrap();
        assert_eq!(roundtrip, s);
    }

    #[test]
    fn test_multiple_writes_return_distinct_pages() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let (p1, _) = of.write_string("first", &tx).unwrap();
        let (p2, _) = of.write_string("second", &tx).unwrap();
        let (p3, _) = of.write_string("third", &tx).unwrap();

        assert_ne!(p1, p2, "each write needs its own page");
        assert_ne!(p2, p3, "each write needs its own page");

        assert_eq!(of.read_string(p1, 0, 5, &tx).unwrap(), "first");
        assert_eq!(of.read_string(p2, 0, 6, &tx).unwrap(), "second");
        assert_eq!(of.read_string(p3, 0, 5, &tx).unwrap(), "third");
    }

    #[test]
    fn test_read_from_non_existent_page_returns_garbage() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(fh, bm);
        let tx = Arc::new(tm.begin(false).unwrap());

        // BM returns zeroed pages for non-existent pages instead of erroring
        let result = of.read_string(9999, 0, 10, &tx);
        assert!(result.is_ok(), "BM returns zeroed page for non-existent page");
        // A zeroed page reads as null bytes (valid UTF-8)
        assert_eq!(result.unwrap(), "\0\0\0\0\0\0\0\0\0\0");
    }

    #[test]
    fn test_read_beyond_string_len_panics_or_truncates() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        of.write_string("hi", &tx).unwrap();
        // Requesting more bytes than the string has will read from the
        // next page or past the string — this is caller's responsibility.
        let result = of.read_string(0, 0, 100, &tx);
        assert!(result.is_ok(), "read beyond string succeeds but produces garbage");
    }

    #[test]
    fn test_write_string_with_offset_parameter() {
        let (fh, bm, tm, _dir) = setup();
        let of = OverflowFile::new(Arc::clone(&fh), Arc::clone(&bm));
        let tx = Arc::new(tm.begin(false).unwrap());

        let s = "offset_test_string";
        let (page_idx, _) = of.write_string(s, &tx).unwrap();
        // Read starting at a non-zero offset within the page
        let partial = of.read_string(page_idx, 7, 4, &tx).unwrap();
        assert_eq!(partial, "test");
    }
}
