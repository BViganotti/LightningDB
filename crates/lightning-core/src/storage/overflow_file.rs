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
        let mut result = String::with_capacity(len as usize);
        let mut current_page_idx = page_idx;
        let mut current_offset = offset as usize;
        let mut remaining = len as usize;

        while remaining > 0 {
            let page = self.buffer_manager.pin_page(
                self.file_handle.clone(),
                current_page_idx as u64,
                tx,
            )?;
            // In kuzu/ladybug, each overflow page has a pointer to the next page at the end.
            // Page size is 4KB.
            let page_size = 4096;
            let usable_size = page_size - 4;
            let page_data = page.as_slice();

            let to_read = std::cmp::min(remaining, usable_size - current_offset);
            let slice = &page_data[current_offset..current_offset + to_read];
            result.push_str(
                std::str::from_utf8(slice)
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?,
            );

            remaining -= to_read;
            if remaining > 0 {
                let next_page_bytes = &page_data[usable_size..page_size];
                let next_page_idx = u32::from_le_bytes(next_page_bytes.try_into().expect("4-byte array"));
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
