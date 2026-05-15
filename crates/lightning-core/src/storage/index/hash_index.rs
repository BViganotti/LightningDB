use crate::processor::Value;
use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::LightningError;
use crate::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

pub struct HashIndex {
    file_handle: Arc<FileHandle>,
    num_buckets: u64,
}

const HEADER_PAGE_IDX: u64 = 0;
const MAX_VAL_SIZE: usize = 256;
const ENTRY_SIZE: usize = 8 + MAX_VAL_SIZE + 8;
const MAX_ENTRIES_PER_PAGE: usize = (PAGE_SIZE - 16) / ENTRY_SIZE;
const DELETED_BIT: u64 = 1 << 63;

impl HashIndex {
    pub fn open_or_create(path: &Path) -> Result<Self> {
        Self::open_or_create_with_buckets(path, 64)
    }

    pub fn open_or_create_with_buckets(path: &Path, initial_buckets: u64) -> Result<Self> {
        let is_new = !path.exists();
        let file_handle = Arc::new(FileHandle::open(path)?);
        let num_buckets = if is_new {
            initial_buckets.max(1)
        } else {
            // Read existing bucket count from header
            let header_data = std::fs::read(path)?;
            if header_data.len() >= 8 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&header_data[0..8]);
                u64::from_le_bytes(bytes).max(1)
            } else {
                initial_buckets.max(1)
            }
        };
        let index = Self { file_handle, num_buckets };
        if is_new {
            index.initialize_header()?;
        }
        Ok(index)
    }

    pub fn buckets(&self) -> u64 {
        self.num_buckets
    }

    fn initialize_header(&self) -> Result<()> {
        let header_idx = self.file_handle.add_new_page()?;
        let mut header_data = [0u8; PAGE_SIZE];
        header_data[0..8].copy_from_slice(&self.num_buckets.to_le_bytes());
        header_data[8..16].copy_from_slice(&0u64.to_le_bytes());
        self.file_handle.write_page(header_idx, &header_data)?;
        for _ in 0..self.num_buckets {
            let bucket_idx = self.file_handle.add_new_page()?;
            let mut bucket_data = [0u8; PAGE_SIZE];
            bucket_data[0..8].copy_from_slice(&0u64.to_le_bytes());
            bucket_data[8..16].copy_from_slice(&0u64.to_le_bytes());
            self.file_handle.write_page(bucket_idx, &bucket_data)?;
        }
        Ok(())
    }

    fn compute_hash(val: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        match val {
            Value::Number(n) => n.to_bits().hash(&mut hasher),
            Value::String(s) => s.hash(&mut hasher),
            _ => format!("{val:?}").hash(&mut hasher),
        };
        hasher.finish()
    }
    fn serialize_value(val: &Value, buf: &mut [u8]) -> Result<()> {
        match val {
            Value::Number(n) => {
                buf[0..8].copy_from_slice(&n.to_le_bytes());
                Ok(())
            }
            Value::String(s) => {
                let bytes = s.as_bytes();
                if bytes.len() > MAX_VAL_SIZE - 4 {
                    return Err(LightningError::Internal("String too long".into()));
                }
                let len = bytes.len() as u32;
                buf[0..4].copy_from_slice(&len.to_le_bytes());
                buf[4..4 + bytes.len()].copy_from_slice(bytes);
                Ok(())
            }
            Value::Boolean(b) => {
                buf[0] = if *b { 1 } else { 0 };
                Ok(())
            }
            Value::Node(id) => {
                buf[0..8].copy_from_slice(&id.to_le_bytes());
                Ok(())
            }
            _ => Err(LightningError::Internal(
                "Unsupported index value type".into(),
            )),
        }
    }
    fn deserialize_value(val_template: &Value, buf: &[u8]) -> Result<Value> {
        match val_template {
            Value::Number(_) => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[0..8]);
                Ok(Value::Number(f64::from_le_bytes(bytes)))
            }
            Value::String(_) => {
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&buf[0..4]);
                let len = u32::from_le_bytes(len_bytes) as usize;
                Ok(Value::String(
                    String::from_utf8_lossy(&buf[4..4 + len]).into_owned(),
                ))
            }
            Value::Boolean(_) => Ok(Value::Boolean(buf[0] != 0)),
            Value::Node(_) => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[0..8]);
                Ok(Value::Node(u64::from_le_bytes(bytes)))
            }
            _ => Err(LightningError::Internal(
                "Unsupported index value type".into(),
            )),
        }
    }

    fn scan_bucket_page(
        data: &[u8],
        hash: u64,
        key: &Value,
        limit: Option<usize>,
        results: &mut Vec<u64>,
    ) -> Result<()> {
        let num_entries = u64::from_le_bytes(data[8..16].try_into().expect("fixed-size array conversion (8 bytes)"));
        for i in 0..num_entries as usize {
            if let Some(l) = limit {
                if results.len() >= l {
                    return Ok(());
                }
            }
            let offset = 16 + i * ENTRY_SIZE;
            let stored_hash = u64::from_le_bytes(data[offset..offset + 8].try_into().expect("fixed-size array conversion (8 bytes)"));
            if stored_hash & DELETED_BIT != 0 {
                continue;
            }
            if stored_hash == hash {
                let stored_val =
                    Self::deserialize_value(key, &data[offset + 8..offset + 8 + MAX_VAL_SIZE])?;
                if stored_val == *key {
                    results.push(u64::from_le_bytes(
                        data[offset + 8 + MAX_VAL_SIZE..offset + 16 + MAX_VAL_SIZE]
                            .try_into()
                            .expect("fixed-size array conversion (8 bytes)"),
                    ));
                }
            }
        }
        Ok(())
    }

    fn allocate_overflow_page(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<u64> {
        let new_idx = self.file_handle.get_num_pages();
        let frame = bm.create_new_version(Arc::clone(&self.file_handle), new_idx, tx)?;
        let ptr = frame.as_ptr();
        unsafe {
            ptr.write_bytes(0, PAGE_SIZE);
            (0u64.to_le_bytes()).as_ptr().copy_to(ptr, 8);
            (1u64.to_le_bytes()).as_ptr().copy_to(ptr.add(8), 8);
        }
        bm.log_page_update(self.file_handle.file_id, new_idx, frame.as_slice())?;
        bm.unpin_page(&self.file_handle, new_idx, frame);
        Ok(new_idx)
    }

    fn write_entry_to_page(
        data_ptr: *mut u8,
        num_entries: u64,
        hash: u64,
        key: &Value,
        row_id: u64,
    ) -> Result<()> {
        let offset = 16 + (num_entries as usize) * ENTRY_SIZE;
        unsafe {
            std::ptr::copy_nonoverlapping(hash.to_le_bytes().as_ptr(), data_ptr.add(offset), 8);
        }
        let mut val_bytes = vec![0u8; MAX_VAL_SIZE];
        Self::serialize_value(key, &mut val_bytes)?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                val_bytes.as_ptr(),
                data_ptr.add(offset + 8),
                MAX_VAL_SIZE,
            );
            std::ptr::copy_nonoverlapping(
                row_id.to_le_bytes().as_ptr(),
                data_ptr.add(offset + 8 + MAX_VAL_SIZE),
                8,
            );
            std::ptr::copy_nonoverlapping(
                (num_entries + 1).to_le_bytes().as_ptr(),
                data_ptr.add(8),
                8,
            );
        }
        Ok(())
    }

    pub fn insert(
        &self,
        bm: &BufferManager,
        key: &Value,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let hash = Self::compute_hash(key);
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = {
            let hdr_data = header_frame.as_slice();
            u64::from_le_bytes(hdr_data[0..8].try_into().expect("fixed-size array conversion (8 bytes)"))
        };
        if num_buckets == 0 {
            return Err(LightningError::Internal("HashIndex header corrupted: num_buckets=0".into()));
        }
        let target_bucket = 1 + (hash % num_buckets);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
            let data_ptr = frame.as_ptr();
            let num_entries = u64::from_le_bytes(unsafe { *data_ptr.add(8).cast::<[u8; 8]>() });

            if (num_entries as usize) < MAX_ENTRIES_PER_PAGE {
                Self::write_entry_to_page(data_ptr, num_entries, hash, key, row_id)?;
                bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, frame.as_slice())?;
                bm.unpin_page(self.fh(), current_page, frame);
                break;
            }

            let next_page = u64::from_le_bytes(unsafe { *data_ptr.cast::<[u8; 8]>() });
            bm.unpin_page(self.fh(), current_page, frame);

            if next_page == 0 {
                let new_page = self.allocate_overflow_page(bm, tx)?;
                let bucket_frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        new_page.to_le_bytes().as_ptr(),
                        bucket_frame.as_ptr(),
                        8,
                    );
                }
                bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, bucket_frame.as_slice())?;
                bm.unpin_page(self.fh(), current_page, bucket_frame);
                current_page = new_page;
            } else {
                current_page = next_page;
            }
        }

        Ok(())
    }

    pub fn delete(
        &self,
        bm: &BufferManager,
        key: &Value,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<bool> {
        let hash = Self::compute_hash(key);
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = {
            let hdr_data = header_frame.as_slice();
            u64::from_le_bytes(hdr_data[0..8].try_into().expect("fixed-size array conversion (8 bytes)"))
        };
        if num_buckets == 0 {
            return Ok(false);
        }
        let target_bucket = 1 + (hash % num_buckets);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
            let num_entries;
            let next_page;
            let mut found = false;
            unsafe {
                let data = &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>();
                num_entries = u64::from_le_bytes(data[8..16].try_into().expect("fixed-size array conversion (8 bytes)"));
                next_page = u64::from_le_bytes(data[0..8].try_into().expect("fixed-size array conversion (8 bytes)"));
                for i in 0..num_entries as usize {
                    let offset = 16 + i * ENTRY_SIZE;
                    let stored_hash =
                        u64::from_le_bytes(data[offset..offset + 8].try_into().expect("fixed-size array conversion (8 bytes)"));
                    if stored_hash & DELETED_BIT != 0 {
                        continue;
                    }
                    if stored_hash == hash {
                        let stored_val = Self::deserialize_value(
                            key,
                            &data[offset + 8..offset + 8 + MAX_VAL_SIZE],
                        )?;
                        if stored_val == *key {
                            let stored_row_id = u64::from_le_bytes(
                                data[offset + 8 + MAX_VAL_SIZE..offset + 16 + MAX_VAL_SIZE]
                                    .try_into()
                                    .expect("fixed-size array conversion (8 bytes)"),
                            );
                            if stored_row_id == row_id {
                                let ptr = frame.as_ptr();
                                let tombstone = (hash | DELETED_BIT).to_le_bytes();
                                std::ptr::copy_nonoverlapping(
                                    tombstone.as_ptr(),
                                    ptr.add(offset),
                                    8,
                                );
                                found = true;
                                break;
                            }
                        }
                    }
                }
            }
            if found {
                bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, frame.as_slice())?;
                bm.unpin_page(self.fh(), current_page, frame);
                return Ok(true);
            }
            bm.unpin_page(self.fh(), current_page, frame);
            if next_page == 0 {
                break;
            }
            current_page = next_page;
        }

        Ok(false)
    }

    pub fn lookup(
        &self,
        bm: &BufferManager,
        key: &Value,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Option<u64>> {
        Ok(self.lookup_multi(bm, key, Some(1), tx)?.first().cloned())
    }

    pub fn lookup_multi(
        &self,
        bm: &BufferManager,
        key: &Value,
        limit: Option<usize>,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<u64>> {
        let hash = Self::compute_hash(key);
        let mut results = Vec::new();
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = {
            let hdr_data = header_frame.as_slice();
            u64::from_le_bytes(hdr_data[0..8].try_into().expect("fixed-size array conversion (8 bytes)"))
        };
        if num_buckets == 0 {
            return Ok(Vec::new());
        }
        let target_bucket = 1 + (hash % num_buckets);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
            let data = unsafe { &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>() };
            Self::scan_bucket_page(data, hash, key, limit, &mut results)?;
            let next_page = u64::from_le_bytes(data[0..8].try_into().expect("fixed-size array conversion (8 bytes)"));
            bm.unpin_page(self.fh(), current_page, frame);
            if next_page == 0 {
                break;
            }
            current_page = next_page;
        }

        Ok(results)
    }

    fn fh(&self) -> &Arc<FileHandle> {
        &self.file_handle
    }
}
