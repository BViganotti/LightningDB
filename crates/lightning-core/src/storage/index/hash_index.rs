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
}

const HEADER_PAGE_IDX: u64 = 0;
const INITIAL_BUCKETS: u64 = 64;
const MAX_VAL_SIZE: usize = 256;
const ENTRY_SIZE: usize = 8 + MAX_VAL_SIZE + 8; // hash(8) + max_value_size(256) + row_id(8) = 272 bytes
const MAX_ENTRIES_PER_PAGE: usize = (PAGE_SIZE - 16) / ENTRY_SIZE;

impl HashIndex {
    pub fn open_or_create(path: &Path) -> Result<Self> {
        let is_new = !path.exists();
        let file_handle = Arc::new(FileHandle::open(path)?);
        let index = Self { file_handle };
        if is_new {
            index.initialize_header()?;
        }
        Ok(index)
    }

    fn initialize_header(&self) -> Result<()> {
        let header_idx = self.file_handle.add_new_page()?;
        let mut header_data = [0u8; PAGE_SIZE];
        header_data[0..8].copy_from_slice(&INITIAL_BUCKETS.to_le_bytes());
        header_data[8..16].copy_from_slice(&0u64.to_le_bytes());
        self.file_handle.write_page(header_idx, &header_data)?;
        for _ in 0..INITIAL_BUCKETS {
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
            _ => format!("{:?}", val).hash(&mut hasher),
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

    pub fn insert(
        &self,
        bm: &BufferManager,
        key: &Value,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let hash = Self::compute_hash(key);
        let header_frame = bm.pin_page(Arc::clone(&self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = u64::from_le_bytes(header_frame.data[0..8].try_into().unwrap());
        // Simplified insert: for now we don't handle overflow pages in MVCC perfectly
        let target_bucket = 1 + (hash % num_buckets);
        let frame = bm.pin_page(Arc::clone(&self.fh()), target_bucket, tx)?;
        unsafe {
            let data_ptr = frame.data.as_ptr() as *mut u8;
            let num_entries = u64::from_le_bytes(frame.data[8..16].try_into().unwrap());
            if (num_entries as usize) < MAX_ENTRIES_PER_PAGE {
                let offset = 16 + (num_entries as usize) * ENTRY_SIZE;
                std::ptr::copy_nonoverlapping(hash.to_le_bytes().as_ptr(), data_ptr.add(offset), 8);
                let mut val_bytes = vec![0u8; MAX_VAL_SIZE];
                Self::serialize_value(key, &mut val_bytes)?;
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
        }
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);
        bm.unpin_page(self.fh(), target_bucket, frame);
        Ok(())
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
        let header_frame = bm.pin_page(Arc::clone(&self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = u64::from_le_bytes(header_frame.data[0..8].try_into().unwrap());
        let target_bucket = 1 + (hash % num_buckets);
        let frame = bm.pin_page(Arc::clone(&self.fh()), target_bucket, tx)?;
        let data = &frame.data;
        let num_entries = u64::from_le_bytes(data[8..16].try_into().unwrap());
        for i in 0..num_entries as usize {
            let offset = 16 + i * ENTRY_SIZE;
            let stored_hash = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            if stored_hash == hash {
                let stored_val =
                    Self::deserialize_value(key, &data[offset + 8..offset + 8 + MAX_VAL_SIZE])?;
                if stored_val == *key {
                    results.push(u64::from_le_bytes(
                        data[offset + 8 + MAX_VAL_SIZE..offset + 16 + MAX_VAL_SIZE]
                            .try_into()
                            .unwrap(),
                    ));
                    if let Some(l) = limit {
                        if results.len() >= l {
                            break;
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    fn fh(&self) -> &Arc<FileHandle> {
        &self.file_handle
    }
}
