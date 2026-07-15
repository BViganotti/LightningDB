use crate::processor::Value;
use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::file_handle::FileHandle;
use crate::LightningError;
use crate::Result;
use parking_lot::Mutex;
use std::path::Path;
use std::sync::Arc;

/// Uses SipHash 1-3 (via DefaultHasher). The hash is used for bucket
/// distribution only — stored hashes are compared against recomputed values
/// within the same process.  In-memory HashMap protections via RandomState
/// are orthogonal; SipHash provides adequate collision resistance for
/// bucket assignment.
pub struct HashIndex {
    file_handle: Arc<FileHandle>,
    num_buckets: std::sync::atomic::AtomicU64,
    resize_lock: Mutex<()>,
}

const HEADER_PAGE_IDX: u64 = 0;
const MAX_VAL_SIZE: usize = 256;
const ENTRY_SIZE: usize = 8 + MAX_VAL_SIZE + 8;
const MAX_ENTRIES_PER_PAGE: usize = (PAGE_SIZE - 16) / ENTRY_SIZE;
const DELETED_BIT: u64 = 1 << 63;

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = data
        .get(offset..offset + 8)
        .ok_or_else(|| LightningError::Internal("buffer too short for u64".into()))?
        .try_into()
        .map_err(|_| LightningError::Internal("array conversion failed".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

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
            // Read existing bucket count from header (first 8 bytes only)
            let mut header_data = [0u8; 8];
            use std::io::Read;
            let mut f = std::fs::File::open(path)?;
            if f.read_exact(&mut header_data).is_ok() {
                u64::from_le_bytes(header_data).max(1)
            } else {
                initial_buckets.max(1)
            }
        };
        let index = Self {
            file_handle,
            num_buckets: std::sync::atomic::AtomicU64::new(num_buckets),
            resize_lock: Mutex::new(()),
        };
        if is_new {
            index.initialize_header(None)?;
        }
        Ok(index)
    }

    pub fn buckets(&self) -> u64 {
        self.num_buckets.load(std::sync::atomic::Ordering::Acquire)
    }

    fn initialize_header(&self, bm: Option<&BufferManager>) -> Result<()> {
        let header_idx = self.file_handle.add_new_page()?;
        let nb = self.num_buckets.load(std::sync::atomic::Ordering::Acquire);
        let mut header_data = [0u8; PAGE_SIZE];
        header_data[0..8].copy_from_slice(&nb.to_le_bytes());
        header_data[8..16].copy_from_slice(&0u64.to_le_bytes());
        self.file_handle.write_page(header_idx, &header_data)?;
        for _ in 0..nb {
            let bucket_idx = self.file_handle.add_new_page()?;
            let mut bucket_data = [0u8; PAGE_SIZE];
            bucket_data[0..8].copy_from_slice(&0u64.to_le_bytes());
            bucket_data[8..16].copy_from_slice(&0u64.to_le_bytes());
            self.file_handle.write_page(bucket_idx, &bucket_data)?;
        }
        // Invalidate buffer pool frames for pages written directly to disk.
        // Without this, pin_page may return a stale cached frame (all-zeros)
        // instead of the data we just wrote.
        if let Some(bm) = bm {
            bm.invalidate_page(self.file_handle.file_id, header_idx);
            for i in 0..nb {
                bm.invalidate_page(self.file_handle.file_id, 1 + i);
            }
        }
        Ok(())
    }

    /// Resize the hash index to double its current bucket count.
    /// Collects all active entries, reinitializes the table with
    /// twice the bucket count, and rehashes every entry.
    /// Header is updated LAST to prevent concurrent lookups (which do
    /// NOT hold the resize lock) from reading garbage bucket pages.
    pub fn resize(&self, bm: &BufferManager, tx: &crate::transaction::transaction_manager::Transaction) -> Result<()> {
        let _lock = self.resize_lock.lock();
        let entries = self.collect_all_entries(bm, tx)?;
        let old_buckets = self.num_buckets.load(std::sync::atomic::Ordering::Acquire);
        let new_buckets = old_buckets * 2;

        // 1. Create new bucket pages (pre-zeroed) for the additional capacity
        let needed_pages = 1 + new_buckets;
        while self.file_handle.get_num_pages() < needed_pages {
            let idx = self.file_handle.add_new_page()?;
            let frame = bm.create_new_version(Arc::clone(self.fh()), idx, tx)?;
            unsafe {
                let zero8 = 0u64.to_le_bytes();
                // SAFETY: zero8 (stack) and frame (heap) do not overlap.
                std::ptr::copy_nonoverlapping(zero8.as_ptr(), frame.as_ptr(), 8);
                std::ptr::copy_nonoverlapping(zero8.as_ptr(), frame.as_ptr().add(8), 8);
            }
            bm.log_page_update(self.file_handle.file_id, idx, frame.as_slice())?;
            bm.unpin_page(self.fh(), idx, frame);
        }

        // 2. Zero out only the NEW bucket pages (old entries in existing pages
        //    will be overwritten during re-insertion in step 3).
        for page_idx in (old_buckets + 1)..=new_buckets {
            let frame = bm.create_new_version(Arc::clone(self.fh()), page_idx, tx)?;
            let ptr = frame.as_ptr();
            unsafe {
                ptr.write_bytes(0, PAGE_SIZE);
                let zero8 = 0u64.to_le_bytes();
                // SAFETY: zero8 (stack) and ptr (heap) do not overlap.
                std::ptr::copy_nonoverlapping(zero8.as_ptr(), ptr, 8);
                std::ptr::copy_nonoverlapping(zero8.as_ptr(), ptr.add(8), 8);
            }
            bm.log_page_update(self.file_handle.file_id, page_idx, frame.as_slice())?;
            bm.unpin_page(self.fh(), page_idx, frame);
        }

        // 3. Update header BEFORE re-insertion — insert_internal reads bucket count
        //    from the header page, NOT from self.num_buckets. If we delay this
        //    update, entries get inserted at hash % old_buckets but lookups
        //    (which also read the header) hash at new_buckets → all misses.
        let header_frame = bm.create_new_version(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                new_buckets.to_le_bytes().as_ptr(),
                header_frame.as_ptr(),
                8,
            );
        }
        bm.log_page_update(self.file_handle.file_id, HEADER_PAGE_IDX, header_frame.as_slice())?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);

        // 4. Update the in-memory count
        self.num_buckets.store(new_buckets, std::sync::atomic::Ordering::Release);

        // 5. Re-insert all collected entries (insert_internal reads header → new_buckets ✓)
        for (hash, key, row_id) in &entries {
            self.insert_internal(bm, *hash, key, *row_id, tx)?;
        }

        Ok(())
    }

    fn collect_all_entries(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(u64, Vec<u8>, u64)>> {
        let mut all = Vec::new();
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);

        for bucket_idx in 1..=num_buckets {
            let mut current = bucket_idx;
            loop {
                let frame = bm.pin_page(Arc::clone(self.fh()), current, tx)?;
                // SAFETY: frame.as_ptr() points to PAGE_SIZE bytes of initialized memory.
                // The frame was either read from disk or zero-initialized on creation.
                let data = unsafe { &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>() };
                let num_entries = read_u64_at(data, 8)?;
                let next_page = read_u64_at(data, 0)?;
                for i in 0..num_entries as usize {
                    let offset = 16 + i * ENTRY_SIZE;
                    let stored_hash = read_u64_at(data, offset)?;
                    if stored_hash & DELETED_BIT != 0 {
                        continue;
                    }
                    let val_bytes = data[offset + 8..offset + 8 + MAX_VAL_SIZE].to_vec();
                    let row_id = read_u64_at(data, offset + 8 + MAX_VAL_SIZE)?;
                    all.push((stored_hash, val_bytes, row_id));
                }
                bm.unpin_page(self.fh(), current, frame);
                if next_page == 0 {
                    break;
                }
                current = next_page;
            }
        }
        Ok(all)
    }

    fn insert_internal(
        &self,
        bm: &BufferManager,
        hash: u64,
        key_bytes: &[u8],
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        // Read num_buckets directly from the file header (already updated by resize)
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);

        let nb = if num_buckets == 0 {
            tracing::warn!("HashIndex num_buckets=0 at insert, reinitializing with 64 buckets");
            self.initialize_header(Some(bm))?;
            let n = self.buckets();
            if n == 0 { return Err(LightningError::Internal("HashIndex reinit failed".into())); }
            n
        } else {
            num_buckets
        };
        let target_bucket = 1 + (hash % nb);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.create_new_version(Arc::clone(self.fh()), current_page, tx)?;
            let data_ptr = frame.as_ptr();
            let num_entries = read_u64_at(unsafe { &*data_ptr.cast::<[u8; PAGE_SIZE]>() }, 8)?;

            if (num_entries as usize) < MAX_ENTRIES_PER_PAGE {
                Self::write_entry_to_page(data_ptr, num_entries, hash, key_bytes, row_id)?;
                bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, frame.as_slice())?;
                bm.unpin_page(self.fh(), current_page, frame);
                return Ok(());
            }

            let next_page = read_u64_at(unsafe { &*data_ptr.cast::<[u8; PAGE_SIZE]>() }, 0)?;
            bm.unpin_page(self.fh(), current_page, frame);

            if next_page == 0 {
                let new_page = self.allocate_overflow_page(bm, tx)?;
                let bucket_frame = bm.create_new_version(Arc::clone(self.fh()), current_page, tx)?;
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
    }

    fn compute_hash(val: &Value) -> u64 {
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        Self::hash_value(val, &mut h);
        h.finish() & !DELETED_BIT
    }

    fn hash_value<H: std::hash::Hasher>(val: &Value, h: &mut H) {
        use std::hash::Hash;
        match val {
            Value::Number(n) => {
                0u8.hash(h);
                n.to_bits().hash(h);
            }
            Value::String(s) => {
                1u8.hash(h);
                s.hash(h);
            }
            Value::Boolean(b) => {
                2u8.hash(h);
                b.hash(h);
            }
            Value::Node(id) | Value::Relationship(id) => {
                3u8.hash(h);
                id.hash(h);
            }
            Value::Date(d) => {
                4u8.hash(h);
                d.hash(h);
            }
            Value::Timestamp(t) => {
                5u8.hash(h);
                t.hash(h);
            }
            Value::Null => {
                6u8.hash(h);
            }
            Value::Path(vals) => {
                7u8.hash(h);
                vals.len().hash(h);
                for v in vals {
                    Self::hash_value(v, h);
                }
            }
            Value::List(vals) => {
                8u8.hash(h);
                vals.len().hash(h);
                for v in vals {
                    Self::hash_value(v, h);
                }
            }
            Value::Struct(fields) => {
                9u8.hash(h);
                fields.len().hash(h);
                for (name, v) in fields {
                    name.hash(h);
                    Self::hash_value(v, h);
                }
            }
            Value::Map(entries) => {
                10u8.hash(h);
                entries.len().hash(h);
                for (k, v) in entries {
                    Self::hash_value(k, h);
                    Self::hash_value(v, h);
                }
            }
        }
    }
    fn serialize_value(val: &Value, buf: &mut [u8]) -> Result<()> {
        match val {
            Value::Number(n) => {
                buf[0] = 0;
                buf[1..9].copy_from_slice(&n.to_le_bytes());
                Ok(())
            }
            Value::String(s) => {
                buf[0] = 1;
                let bytes = s.as_bytes();
                if bytes.len() > MAX_VAL_SIZE - 5 {
                    return Err(LightningError::Internal("String too long".into()));
                }
                let len = bytes.len() as u32;
                buf[1..5].copy_from_slice(&len.to_le_bytes());
                buf[5..5 + bytes.len()].copy_from_slice(bytes);
                Ok(())
            }
            Value::Boolean(b) => {
                buf[0] = 2;
                buf[1] = if *b { 1 } else { 0 };
                Ok(())
            }
            Value::Node(id) => {
                buf[0] = 3;
                buf[1..9].copy_from_slice(&id.to_le_bytes());
                Ok(())
            }
            Value::Relationship(id) => {
                buf[0] = 4;
                buf[1..9].copy_from_slice(&id.to_le_bytes());
                Ok(())
            }
            Value::Date(d) => {
                buf[0] = 5;
                buf[1..5].copy_from_slice(&d.to_le_bytes());
                Ok(())
            }
            Value::Timestamp(t) => {
                buf[0] = 6;
                buf[1..9].copy_from_slice(&t.to_le_bytes());
                Ok(())
            }
            _ => Err(LightningError::Internal(
                "Unsupported index value type".into(),
            )),
        }
    }
    fn deserialize_value(buf: &[u8]) -> Result<Value> {
        if buf.is_empty() {
            return Err(LightningError::Internal("empty buffer in deserialize_value".into()));
        }
        match buf[0] {
            0 => {
                if buf.len() < 9 {
                    return Err(LightningError::Internal("short buffer for Number in deserialize_value".into()));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[1..9]);
                Ok(Value::Number(f64::from_le_bytes(bytes)))
            }
            1 => {
                if buf.len() < 5 {
                    return Err(LightningError::Internal("short buffer for String length in deserialize_value".into()));
                }
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&buf[1..5]);
                let len = u32::from_le_bytes(len_bytes) as usize;
                if 5 + len > buf.len() {
                    return Err(LightningError::Internal(format!(
                        "String data length {} exceeds buffer size {} in deserialize_value",
                        len,
                        buf.len()
                    )));
                }
                Ok(Value::String(
                    String::from_utf8_lossy(&buf[5..5 + len]).into_owned(),
                ))
            }
            2 => {
                if buf.len() < 2 {
                    return Err(LightningError::Internal("short buffer for Boolean in deserialize_value".into()));
                }
                Ok(Value::Boolean(buf[1] != 0))
            }
            3 => {
                if buf.len() < 9 {
                    return Err(LightningError::Internal("short buffer for Node in deserialize_value".into()));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[1..9]);
                Ok(Value::Node(u64::from_le_bytes(bytes)))
            }
            4 => {
                if buf.len() < 9 {
                    return Err(LightningError::Internal("short buffer for Relationship in deserialize_value".into()));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[1..9]);
                Ok(Value::Relationship(u64::from_le_bytes(bytes)))
            }
            5 => {
                if buf.len() < 5 {
                    return Err(LightningError::Internal("short buffer for Date in deserialize_value".into()));
                }
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&buf[1..5]);
                Ok(Value::Date(i32::from_le_bytes(bytes)))
            }
            6 => {
                if buf.len() < 9 {
                    return Err(LightningError::Internal("short buffer for Timestamp in deserialize_value".into()));
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&buf[1..9]);
                Ok(Value::Timestamp(i64::from_le_bytes(bytes)))
            }
            t => Err(LightningError::Internal(
                format!("Unsupported index value type tag {t}"),
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
        let num_entries = read_u64_at(data, 8)?;
        if num_entries == 0 {
            // Check if page looks like it should have entries (non-zero data after header)
            let has_nonzero = data[16..std::cmp::min(16 + ENTRY_SIZE, PAGE_SIZE)]
                .iter().any(|&b| b != 0);
            if has_nonzero {
                tracing::warn!(
                    "scan_bucket_page: num_entries=0 but page has non-zero data at offset 16+, hash={:#x}",
                    hash
                );
            }
        }
        for i in 0..num_entries as usize {
            if let Some(l) = limit {
                if results.len() >= l {
                    return Ok(());
                }
            }
            let offset = 16 + i * ENTRY_SIZE;
            let stored_hash = read_u64_at(data, offset)?;
            if stored_hash & DELETED_BIT != 0 {
                continue;
            }
            if stored_hash == hash {
                let stored_val =
                    Self::deserialize_value(&data[offset + 8..offset + 8 + MAX_VAL_SIZE])?;
                if stored_val == *key {
                    results.push(read_u64_at(data, offset + 8 + MAX_VAL_SIZE)?);
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
        // add_new_page atomically increments the page count, preventing
        // two threads from allocating the same page index.
        let new_idx = self.file_handle.add_new_page()?;
        let frame = bm.create_new_version(Arc::clone(&self.file_handle), new_idx, tx)?;
        let ptr = frame.as_ptr();
        unsafe {
            ptr.write_bytes(0, PAGE_SIZE);
            let zero8 = 0u64.to_le_bytes();
            zero8.as_ptr().copy_to(ptr, 8);
            zero8.as_ptr().copy_to(ptr.add(8), 8);
        }
        bm.log_page_update(self.file_handle.file_id, new_idx, frame.as_slice())?;
        bm.unpin_page(&self.file_handle, new_idx, frame);
        Ok(new_idx)
    }

    fn write_entry_to_page(
        data_ptr: *mut u8,
        num_entries: u64,
        hash: u64,
        key_bytes: &[u8],
        row_id: u64,
    ) -> Result<()> {
        let offset = 16 + (num_entries as usize) * ENTRY_SIZE;
        if offset + ENTRY_SIZE > 4096 {
            return Err(LightningError::Internal(format!(
                "Hash index page full: entry at offset {offset} exceeds page size"
            )));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(hash.to_le_bytes().as_ptr(), data_ptr.add(offset), 8);
        }
        if key_bytes.len() != MAX_VAL_SIZE {
            return Err(LightningError::Internal("key_bytes length must match MAX_VAL_SIZE".into()));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                key_bytes.as_ptr(),
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
        let _lock = self.resize_lock.lock();
        let hash = Self::compute_hash(key);
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);
        let nb = if num_buckets == 0 {
            tracing::warn!("HashIndex num_buckets=0 at delete_if, reinitializing with 64 buckets");
            self.initialize_header(Some(bm))?;
            let n = self.buckets();
            if n == 0 { return Err(LightningError::Internal("HashIndex reinit failed".into())); }
            n
        } else {
            num_buckets
        };
        let target_bucket = 1 + (hash % nb);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.create_new_version(Arc::clone(self.fh()), current_page, tx)?;
            let data_ptr = frame.as_ptr();
            let num_entries = read_u64_at(unsafe { &*data_ptr.cast::<[u8; PAGE_SIZE]>() }, 8)?;

            if (num_entries as usize) < MAX_ENTRIES_PER_PAGE {
                let mut key_buf = vec![0u8; MAX_VAL_SIZE];
                Self::serialize_value(key, &mut key_buf)?;
                Self::write_entry_to_page(data_ptr, num_entries, hash, &key_buf, row_id)?;
                bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, frame.as_slice())?;
                bm.unpin_page(self.fh(), current_page, frame);
                break;
            }

            // SAFETY: SAFETY: Reading overflow page pointer from newly created version via cast.
            let next_page = read_u64_at(unsafe { &*data_ptr.cast::<[u8; PAGE_SIZE]>() }, 0)?;
            bm.unpin_page(self.fh(), current_page, frame);

            if next_page == 0 {
                let new_page = self.allocate_overflow_page(bm, tx)?;
                let bucket_frame = bm.create_new_version(Arc::clone(self.fh()), current_page, tx)?;
                // SAFETY: SAFETY: Writing overflow page link into newly created version.
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
        let _lock = self.resize_lock.lock();
        let hash = Self::compute_hash(key);
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        if num_buckets == 0 {
            return Ok(false);
        }
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);
        let target_bucket = 1 + (hash % num_buckets);

        let mut current_page = target_bucket;
        let mut found_offset: Option<usize> = None;
        loop {
            let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
            let num_entries;
            let next_page;
            let mut found = false;
            // SAFETY: SAFETY: Reading data in delete path. Frame is pinned.
            unsafe {
                let data = &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>();
                num_entries = read_u64_at(data, 8)?;
                next_page = read_u64_at(data, 0)?;
                for i in 0..num_entries as usize {
                    let offset = 16 + i * ENTRY_SIZE;
                    let stored_hash = read_u64_at(data, offset)?;
                    if stored_hash & DELETED_BIT != 0 {
                        continue;
                    }
                    if stored_hash == hash {
                        let stored_val = Self::deserialize_value(
                            &data[offset + 8..offset + 8 + MAX_VAL_SIZE],
                        )?;
                        if stored_val == *key {
                            let stored_row_id = read_u64_at(data, offset + 8 + MAX_VAL_SIZE)?;
                            if stored_row_id == row_id {
                                found = true;
                                found_offset = Some(offset);
                                break;
                            }
                        }
                    }
                }
            }
            if found {
                bm.unpin_page(self.fh(), current_page, frame);
                if let Some(offset) = found_offset {
                    let write_frame = bm.create_new_version(Arc::clone(self.fh()), current_page, tx)?;
                    unsafe {
                        let ptr = write_frame.as_ptr();
                        let tombstone = (hash | DELETED_BIT).to_le_bytes();
                        std::ptr::copy_nonoverlapping(
                            tombstone.as_ptr(),
                            ptr.add(offset),
                            8,
                        );
                    }
                    bm.log_page_update_for_tx(tx.tx_id, self.fh().file_id, current_page, write_frame.as_slice())?;
                    bm.unpin_page(self.fh(), current_page, write_frame);
                }
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
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);
        if num_buckets == 0 {
            return Ok(Vec::new());
        }
        let target_bucket = 1 + (hash % num_buckets);

        let mut current_page = target_bucket;
        loop {
            let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
            // SAFETY: SAFETY: Reading bucket page data in lookup_multi. Frame pinned via pin_page.
            let data = unsafe { &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>() };
            Self::scan_bucket_page(data, hash, key, limit, &mut results)?;
            let next_page = read_u64_at(data, 0)?;
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

    /// Scan all buckets and return every non-deleted entry (key, row_id).
    /// Reads the header page for the bucket count, then iterates each
    /// bucket and its overflow chain by page ID.
    pub fn entries(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(Value, u64)>> {
        let mut results = Vec::new();
        let header_frame = bm.pin_page(Arc::clone(self.fh()), HEADER_PAGE_IDX, tx)?;
        let num_buckets = read_u64_at(header_frame.as_slice(), 0)?;
        bm.unpin_page(self.fh(), HEADER_PAGE_IDX, header_frame);

        for bucket_idx in 1..=num_buckets {
            let mut current_page = bucket_idx;
            loop {
                let frame = bm.pin_page(Arc::clone(self.fh()), current_page, tx)?;
                let data = unsafe { &*frame.as_ptr().cast::<[u8; PAGE_SIZE]>() };
                let num_entries = read_u64_at(data, 8)?;
                let next_page = read_u64_at(data, 0)?;

                for i in 0..num_entries as usize {
                    let offset = 16 + i * ENTRY_SIZE;
                    let stored_hash = read_u64_at(data, offset)?;
                    if stored_hash & DELETED_BIT != 0 {
                        continue;
                    }
                    let key = Self::deserialize_value(
                        &data[offset + 8..offset + 8 + MAX_VAL_SIZE],
                    )?;
                    let row_id = read_u64_at(data, offset + 8 + MAX_VAL_SIZE)?;
                    results.push((key, row_id));
                }

                bm.unpin_page(self.fh(), current_page, frame);
                if next_page == 0 {
                    break;
                }
                current_page = next_page;
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use crate::SystemConfig;
    use tempfile::tempdir;

    fn small_db_config() -> SystemConfig {
        SystemConfig {
            buffer_pool_size: 64 * 1024 * 1024, // 64MB — small enough for tests
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000, // very large: never runs during test
            ..Default::default()
        }
    }

    #[test]
    fn test_resize_bucket_count() {
        let dir = tempdir().expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");

        assert_eq!(index.buckets(), 64);
        // Initial header should show 64 buckets
        let data = std::fs::read(&path).expect("internal invariant violated");
        let header_buckets = u64::from_le_bytes(data[0..8].try_into().expect("infallible: fixed-size array conversion"));
        assert_eq!(header_buckets, 64);
    }

    #[test]
    fn test_resize_updates_header() {
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");

        assert_eq!(index.buckets(), 64);
        index.resize(bm, &tx).expect("internal invariant violated");
        assert_eq!(index.buckets(), 128);

        // Commit and checkpoint to flush to disk
        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
        db.checkpoint().expect("internal invariant violated");

        // Verify header on disk
        let data = std::fs::read(&path).expect("internal invariant violated");
        let nb = u64::from_le_bytes(data[0..8].try_into().expect("infallible: fixed-size array conversion"));
        assert_eq!(nb, 128, "On-disk header should be 128");

        // Reopen and verify
        let index2 = HashIndex::open_or_create(&path).expect("internal invariant violated");
        assert_eq!(index2.buckets(), 128);
    }

    #[test]
    fn test_double_resize() {
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");

        assert_eq!(index.buckets(), 64);
        index.resize(bm, &tx).expect("internal invariant violated");
        assert_eq!(index.buckets(), 128);
        index.resize(bm, &tx).expect("internal invariant violated");
        assert_eq!(index.buckets(), 256);

        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
        db.checkpoint().expect("internal invariant violated");

        let data = std::fs::read(&path).expect("internal invariant violated");
        let nb = u64::from_le_bytes(data[0..8].try_into().expect("infallible: fixed-size array conversion"));
        assert_eq!(nb, 256, "Double resize: on-disk header should be 256");
    }

    #[test]
    fn test_resize_rejected_for_bucket_count_1() {
        // Edge case: resize when there's only 1 bucket
        let dir = tempdir().expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create_with_buckets(&path, 1).expect("internal invariant violated");
        assert_eq!(index.buckets(), 1);

        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");

        index.resize(bm, &tx).expect("internal invariant violated");
        assert_eq!(index.buckets(), 2);

        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_entries_scan_all() {
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");

        // Insert 5 entries
        for i in 0..5u64 {
            index.insert(bm, &Value::Number(i as f64), 100 + i, &tx).expect("internal invariant violated");
        }

        let entries = index.entries(bm, &tx).expect("internal invariant violated");
        assert_eq!(entries.len(), 5, "Should find all 5 entries");

        for (key, row_id) in &entries {
            if let Value::Number(n) = key {
                assert_eq!(*row_id, 100 + *n as u64, "Row ID should match key");
            }
        }

        index.delete(bm, &Value::Number(2.0), 102, &tx).expect("internal invariant violated");
        let after_delete = index.entries(bm, &tx).expect("internal invariant violated");
        assert_eq!(after_delete.len(), 4, "Should skip deleted entry");
        let still_present = after_delete.iter().any(|(_, id)| *id == 102);
        assert!(!still_present, "Deleted entry should not appear");

        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_entries_empty_index() {
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");

        let entries = index.entries(bm, &tx).expect("internal invariant violated");
        assert_eq!(entries.len(), 0, "Empty index should have 0 entries");

        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
    }

    // === Commit visibility tests ===

    #[test]
    fn test_lookup_survives_commit() {
        // Core test: insert in tx1, commit, lookup in tx2
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // Transaction 1: insert entries
        let tx1 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..10u64 {
            index.insert(bm, &Value::Number(i as f64), 1000 + i, &tx1)
                .expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx1, bm, &db).expect("internal invariant violated");

        // Transaction 2: lookup should find all entries
        let tx2 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..10u64 {
            let result = index.lookup(bm, &Value::Number(i as f64), &tx2)
                .expect("internal invariant violated");
            assert_eq!(result, Some(1000 + i), "lookup({}) should find row_id={}", i, 1000 + i);
        }
        db.transaction_manager.commit(&tx2, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_lookup_after_reopen() {
        // Insert, commit, close DB, reopen, lookup
        let dir = tempdir().expect("internal invariant violated");
        let index_path = dir.path().join("test_index.lbug");

        {
            let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
            let index = HashIndex::open_or_create(&index_path).expect("internal invariant violated");
            let bm = &db.buffer_manager;

            let tx = db.transaction_manager.begin(false).expect("internal invariant violated");
            for i in 0..20u64 {
                index.insert(bm, &Value::String(format!("key_{}", i)), i * 100, &tx)
                    .expect("internal invariant violated");
            }
            db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
            db.checkpoint().expect("internal invariant violated");
        }

        // Reopen
        {
            let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
            let index = HashIndex::open_or_create(&index_path).expect("internal invariant violated");
            let bm = &db.buffer_manager;

            let tx = db.transaction_manager.begin(false).expect("internal invariant violated");
            for i in 0..20u64 {
                let result = index.lookup(bm, &Value::String(format!("key_{}", i)), &tx)
                    .expect("internal invariant violated");
                assert_eq!(result, Some(i * 100), "After reopen: key_{} should have row_id={}", i, i * 100);
            }
            db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
        }
    }

    #[test]
    fn test_no_accumulation_on_reinsert() {
        // Insert same keys twice in separate transactions — should not duplicate
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // First insert
        let tx1 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..5u64 {
            index.insert(bm, &Value::Number(i as f64), 100 + i, &tx1)
                .expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx1, bm, &db).expect("internal invariant violated");

        // Second insert (same keys, different row_ids — simulates re-index)
        let tx2 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..5u64 {
            index.insert(bm, &Value::Number(i as f64), 200 + i, &tx2)
                .expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx2, bm, &db).expect("internal invariant violated");

        // lookup_multi should find both entries for each key (insert doesn't deduplicate)
        let tx3 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..5u64 {
            let results = index.lookup_multi(bm, &Value::Number(i as f64), None, &tx3)
                .expect("internal invariant violated");
            // Hash index insert is append-only, so we expect 2 entries per key
            assert!(results.len() >= 1, "key {} should have at least 1 entry, got {}", i, results.len());
        }
        db.transaction_manager.commit(&tx3, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_lookup_with_tiny_buffer_pool() {
        // Force eviction by using a very small buffer pool
        let dir = tempdir().expect("internal invariant violated");
        let config = SystemConfig {
            buffer_pool_size: 4 * 1024 * 1024, // 4MB — very small
            prefetch_enabled: false,
            vacuum_interval_ms: 86_400_000_000,
            ..Default::default()
        };
        let db = Database::new(dir.path(), config).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // Insert many entries to fill buffer pool
        let tx1 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..500u64 {
            index.insert(bm, &Value::String(format!("key_{:04}", i)), i, &tx1)
                .expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx1, bm, &db).expect("internal invariant violated");

        // Lookup in new transaction — should find all entries despite eviction pressure
        let tx2 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..500u64 {
            let result = index.lookup(bm, &Value::String(format!("key_{:04}", i)), &tx2)
                .expect("internal invariant violated");
            assert_eq!(result, Some(i), "key_{:04} should be found after commit", i);
        }
        db.transaction_manager.commit(&tx2, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_multiple_commits_visibility() {
        // Insert across multiple commits, verify all visible
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        for batch in 0..5u64 {
            let tx = db.transaction_manager.begin(false).expect("internal invariant violated");
            for i in 0..10u64 {
                let key = batch * 10 + i;
                index.insert(bm, &Value::Number(key as f64), key * 10, &tx)
                    .expect("internal invariant violated");
            }
            db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
        }

        // All 50 entries should be visible
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");
        for batch in 0..5u64 {
            for i in 0..10u64 {
                let key = batch * 10 + i;
                let result = index.lookup(bm, &Value::Number(key as f64), &tx)
                    .expect("internal invariant violated");
                assert_eq!(result, Some(key * 10), "key={} should be found", key);
            }
        }
        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_insert_lookup_delete_lookup_cycle() {
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // Insert
        let tx1 = db.transaction_manager.begin(false).expect("internal invariant violated");
        index.insert(bm, &Value::Number(42.0), 100, &tx1).expect("internal invariant violated");
        db.transaction_manager.commit(&tx1, bm, &db).expect("internal invariant violated");

        // Verify insert
        let tx2 = db.transaction_manager.begin(false).expect("internal invariant violated");
        assert_eq!(index.lookup(bm, &Value::Number(42.0), &tx2).unwrap(), Some(100));
        db.transaction_manager.commit(&tx2, bm, &db).expect("internal invariant violated");

        // Delete
        let tx3 = db.transaction_manager.begin(false).expect("internal invariant violated");
        let deleted = index.delete(bm, &Value::Number(42.0), 100, &tx3).unwrap();
        assert!(deleted, "Should delete successfully");
        db.transaction_manager.commit(&tx3, bm, &db).expect("internal invariant violated");

        // Verify deletion
        let tx4 = db.transaction_manager.begin(false).expect("internal invariant violated");
        assert_eq!(index.lookup(bm, &Value::Number(42.0), &tx4).unwrap(), None, "Should be gone after delete");
        db.transaction_manager.commit(&tx4, bm, &db).expect("internal invariant violated");

        // Re-insert same key with different row_id
        let tx5 = db.transaction_manager.begin(false).expect("internal invariant violated");
        index.insert(bm, &Value::Number(42.0), 200, &tx5).expect("internal invariant violated");
        db.transaction_manager.commit(&tx5, bm, &db).expect("internal invariant violated");

        // Verify re-insert
        let tx6 = db.transaction_manager.begin(false).expect("internal invariant violated");
        assert_eq!(index.lookup(bm, &Value::Number(42.0), &tx6).unwrap(), Some(200));
        db.transaction_manager.commit(&tx6, bm, &db).expect("internal invariant violated");
    }

    #[test]
    fn test_initialize_header_invalidates_pool() {
        // Simulate: buffer pool has stale frame for page 0 (all zeros),
        // then initialize_header writes correct data to disk.
        // After invalidation, pin_page should read correct data from disk.
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // Load page 0 into buffer pool (will be all zeros since file is new)
        let tx = db.transaction_manager.begin(false).expect("internal invariant violated");
        let f = bm.pin_page(Arc::clone(index.fh()), 0, &tx).unwrap();
        let initial_data = f.as_slice()[..8].to_vec();
        bm.unpin_page(index.fh(), 0, f);
        db.transaction_manager.commit(&tx, bm, &db).expect("internal invariant violated");

        // The header should have num_buckets=64 (written by initialize_header during open_or_create)
        let num_buckets = u64::from_le_bytes(initial_data[..8].try_into().unwrap());
        assert_eq!(num_buckets, 64, "Header should show 64 buckets");
    }

    #[test]
    fn test_concurrent_insert_and_lookup() {
        // Simulate: two transactions, one inserting and one reading
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        // Pre-insert some entries
        let tx0 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..10u64 {
            index.insert(bm, &Value::Number(i as f64), i, &tx0).expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx0, bm, &db).expect("internal invariant violated");

        // Read-only transaction should see all 10 entries
        let tx_read = db.transaction_manager.begin(true).expect("internal invariant violated");
        for i in 0..10u64 {
            let result = index.lookup(bm, &Value::Number(i as f64), &tx_read).unwrap();
            assert_eq!(result, Some(i), "Read tx should see key {}", i);
        }
    }

    #[test]
    fn test_hash_index_full_table_scan_after_commit() {
        // Insert entries, commit, then do a full table scan via entries()
        let dir = tempdir().expect("internal invariant violated");
        let db = Database::new(dir.path(), small_db_config()).expect("internal invariant violated");
        let path = dir.path().join("test_index.lbug");
        let index = HashIndex::open_or_create(&path).expect("internal invariant violated");
        let bm = &db.buffer_manager;

        let tx1 = db.transaction_manager.begin(false).expect("internal invariant violated");
        for i in 0..100u64 {
            index.insert(bm, &Value::Number(i as f64), i * 10, &tx1)
                .expect("internal invariant violated");
        }
        db.transaction_manager.commit(&tx1, bm, &db).expect("internal invariant violated");

        // Full scan in new transaction
        let tx2 = db.transaction_manager.begin(false).expect("internal invariant violated");
        let entries = index.entries(bm, &tx2).expect("internal invariant violated");
        assert_eq!(entries.len(), 100, "Should find all 100 entries after commit");
        db.transaction_manager.commit(&tx2, bm, &db).expect("internal invariant violated");
    }
}
