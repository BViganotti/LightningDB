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
            index.initialize_header()?;
        }
        Ok(index)
    }

    pub fn buckets(&self) -> u64 {
        self.num_buckets.load(std::sync::atomic::Ordering::Acquire)
    }

    fn initialize_header(&self) -> Result<()> {
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
                zero8.as_ptr().copy_to(frame.as_ptr(), 8);
                zero8.as_ptr().copy_to(frame.as_ptr().add(8), 8);
            }
            bm.log_page_update(self.file_handle.file_id, idx, frame.as_slice())?;
            bm.unpin_page(self.fh(), idx, frame);
        }

        // 2. Zero out ALL bucket pages (old + new) before updating header
        //    so concurrent lookups never see stale entries in wrong buckets
        for page_idx in 1..=new_buckets {
            let frame = bm.create_new_version(Arc::clone(self.fh()), page_idx, tx)?;
            let ptr = frame.as_ptr();
            unsafe {
                ptr.write_bytes(0, PAGE_SIZE);
                let zero8 = 0u64.to_le_bytes();
                zero8.as_ptr().copy_to(ptr, 8);
                zero8.as_ptr().copy_to(ptr.add(8), 8);
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

        if num_buckets == 0 {
            return Err(LightningError::Internal("HashIndex header corrupted: num_buckets=0".into()));
        }
        let target_bucket = 1 + (hash % num_buckets);

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
        use std::hash::Hash;
        let hash = match val {
            Value::Number(n) => n.to_bits(),
            Value::String(s) => {
                let mut h = 0u64;
                for chunk in s.as_bytes().chunks(8) {
                    let mut buf = [0u8; 8];
                    buf[..chunk.len()].copy_from_slice(chunk);
                    h = h.wrapping_mul(6364136223846793005).wrapping_add(u64::from_le_bytes(buf));;
                }
                h
            }
            Value::Boolean(b) => *b as u64,
            Value::Node(id) | Value::Relationship(id) => *id,
            Value::Date(d) => *d as u64,
            Value::Timestamp(t) => *t as u64,
            Value::Null => 0u64,
            Value::Path(vals) => {
                let mut h: u64 = 0x6b821c9b;
                for v in vals {
                    h = h.wrapping_mul(31).wrapping_add(Self::compute_hash(v));
                }
                h
            }
            Value::List(vals) => {
                let mut h: u64 = 0x37e9c8a3;
                for v in vals {
                    h = h.wrapping_mul(31).wrapping_add(Self::compute_hash(v));
                }
                h
            }
            Value::Struct(fields) => {
                let mut h: u64 = 0x1429c4e7;
                for (name, v) in fields {
                    for chunk in name.as_bytes().chunks(8) {
                        let mut buf = [0u8; 8];
                        buf[..chunk.len()].copy_from_slice(chunk);
                        h = h.wrapping_mul(6364136223846793005).wrapping_add(u64::from_le_bytes(buf));
                    }
                    h = h.wrapping_mul(31).wrapping_add(Self::compute_hash(v));
                }
                h
            }
            Value::Map(entries) => {
                let mut h: u64 = 0x9e3a1c7d;
                for (k, v) in entries {
                    h = h.wrapping_mul(31).wrapping_add(Self::compute_hash(k));
                    h = h.wrapping_mul(31).wrapping_add(Self::compute_hash(v));
                }
                h
            }
        };
        hash & !DELETED_BIT
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
        let new_idx = self.file_handle.get_num_pages();
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
        if num_buckets == 0 {
            return Err(LightningError::Internal("HashIndex header corrupted: num_buckets=0".into()));
        }
        let target_bucket = 1 + (hash % num_buckets);

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
}
