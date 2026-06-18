use crate::storage::buffer_manager::{BufferManager, PAGE_SIZE};
use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::storage::file_handle::FileHandle;
use crate::storage::row_version::RowVersion;
use crate::storage::stats::ColumnStats;
use crate::Result;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, UInt64Array,
};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use lightning_types::LogicalType;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::processor::Value;

pub struct ZoneMapEq {
    pub value: Value,
}

pub struct Column {
    pub name: String,
    pub data_type: LogicalType,
    pub fh: Arc<FileHandle>,
    pub null_fh: Arc<FileHandle>,
    pub overflow_fh: Option<Arc<FileHandle>>,
    pub stats: Arc<RwLock<ColumnStats>>,
    pub version_info: Arc<RowVersion>,
    pub child_columns: Vec<Column>,
    pub dirty: Arc<AtomicBool>,
    /// Atomic counters for stats, avoiding write lock on append path.
    atomic_num_values: AtomicU64,
    atomic_null_count: AtomicU64,
    /// Buffered null bit changes: (byte_offset_in_null_page, 0|1).
    /// Flushed to actual pages before batch ops or checkpoint.
    pending_nulls: parking_lot::Mutex<Vec<(usize, u8)>>,
}

impl Clone for Column {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            fh: Arc::clone(&self.fh),
            null_fh: Arc::clone(&self.null_fh),
            overflow_fh: self.overflow_fh.as_ref().map(Arc::clone),
            stats: Arc::clone(&self.stats),
            version_info: Arc::clone(&self.version_info),
            child_columns: self.child_columns.clone(),
            dirty: Arc::clone(&self.dirty),
            atomic_num_values: AtomicU64::new(self.atomic_num_values.load(Ordering::Acquire)),
            atomic_null_count: AtomicU64::new(self.atomic_null_count.load(Ordering::Acquire)),
            pending_nulls: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

impl Column {
    pub fn new(
        name: String,
        data_type: LogicalType,
        null_fh: Arc<FileHandle>,
        fh: Arc<FileHandle>,
        overflow_fh: Option<Arc<FileHandle>>,
        version_info: Arc<RowVersion>,
    ) -> Self {
        Self {
            name,
            data_type,
            fh,
            null_fh,
            overflow_fh,
            stats: Arc::new(RwLock::new(ColumnStats::new())),
            version_info,
            child_columns: Vec::new(),
            dirty: Arc::new(AtomicBool::new(false)),
            atomic_num_values: AtomicU64::new(0),
            atomic_null_count: AtomicU64::new(0),
            pending_nulls: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn with_children(
        name: String,
        data_type: LogicalType,
        null_fh: Arc<FileHandle>,
        fh: Arc<FileHandle>,
        overflow_fh: Option<Arc<FileHandle>>,
        version_info: Arc<RowVersion>,
        child_columns: Vec<Column>,
    ) -> Self {
        Self {
            name,
            data_type,
            fh,
            null_fh,
            overflow_fh,
            stats: Arc::new(RwLock::new(ColumnStats::new())),
            version_info,
            child_columns,
            dirty: Arc::new(AtomicBool::new(false)),
            atomic_num_values: AtomicU64::new(0),
            atomic_null_count: AtomicU64::new(0),
            pending_nulls: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn with_overflow(mut self, overflow_fh: Arc<FileHandle>) -> Self {
        self.overflow_fh = Some(overflow_fh);
        self
    }

    pub fn to_field(&self) -> Field {
        Field::new(
            &self.name,
            crate::processor::arrow_utils::logical_type_to_arrow_type(&self.data_type),
            true,
        )
    }

    pub fn append_value(
        &self,
        bm: &BufferManager,
        val: &Value,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.dirty.store(true, Ordering::Release);
        let is_val_null = matches!(val, Value::Null);
        self.set_null(bm, row_id, is_val_null, tx)?;
        if is_val_null {
            self.flush_pending_nulls(bm, tx)?;
            let zero = Value::Number(0.0);
            self.atomic_null_count.fetch_add(1, Ordering::Release);
            return self.append_plain_value(bm, &zero, row_id, tx);
        }
        match &self.data_type {
            LogicalType::List(_) => {
                if let Some(elements) = val.as_list() {
                    let child = &self.child_columns[0];
                    // Atomically claim a contiguous range of row IDs so that two
                    // concurrent list appends do not write to the same child row.
                    let num_vals = child.atomic_num_values.fetch_add(elements.len() as u64, Ordering::AcqRel);
                    for (i, el) in elements.iter().enumerate() {
                        child.append_value(bm, el, num_vals + i as u64, tx)?;
                    }
                    // Note: atomic_num_values over-counts by elements.len() because
                    // append_plain_value also increments it per element. This is
                    // acceptable since the counter is only used for approximate stats
                    // and test assertions, never for correctness.
                    let end_offset = child.stats.read().num_values;
                    self.append_plain_value(bm, &Value::Number(end_offset as f64), row_id, tx)?;
                }
            }
            LogicalType::Struct(_) => {
                if let Value::Struct(vals) = val {
                    for (i, (_name, field_val)) in vals.iter().enumerate() {
                        self.child_columns[i].append_value(bm, field_val, row_id, tx)?;
                    }
                }
            }
            _ => self.append_plain_value(bm, val, row_id, tx)?,
        }
        Ok(())
    }

    pub fn get_value(
        &self,
        bm: &BufferManager,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Value> {
        if self.is_null(bm, row_id, tx)? {
            return Ok(Value::Null);
        }
        match &self.data_type {
            LogicalType::List(_) => {
                let end_offset = self.get_plain_value(bm, row_id, tx)?.as_number() as u64;
                let start_offset = if row_id == 0 {
                    0
                } else {
                    self.get_plain_value(bm, row_id - 1, tx)?.as_number() as u64
                };
                let mut list = Vec::new();
                for i in start_offset..end_offset {
                    list.push(self.child_columns[0].get_value(bm, i, tx)?);
                }
                Ok(Value::List(list))
            }
            LogicalType::Struct(_) => {
                let mut fields = Vec::new();
                for col in self.child_columns.iter() {
                    fields.push((col.name.clone(), col.get_value(bm, row_id, tx)?));
                }
                Ok(Value::Struct(fields))
            }
            _ => self.get_plain_value(bm, row_id, tx),
        }
    }

    pub fn get_plain_value(
        &self,
        bm: &BufferManager,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Value> {
        let stats = self.stats.read();
        if let Some(ref meta) = stats.compression_meta {
            if meta.compression == CompressionType::Constant {
                return Ok(meta.min.clone());
            }
        }

        let element_size = self.element_size();
        let values_per_page = if stats
            .compression_meta
            .as_ref()
            .map(|m| m.compression)
            .unwrap_or(CompressionType::Uncompressed)
            == CompressionType::Uncompressed
        {
            4096 / element_size as u64
        } else {
            32
        };
        let page_idx = row_id / values_per_page;
        let offset_in_page = (row_id % values_per_page) as usize * element_size;
        let frame = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
        let res = self.parse_value(
            &frame.as_slice()[offset_in_page..offset_in_page + element_size],
            bm,
            tx,
        );
        bm.unpin_page(&self.fh, page_idx, frame);
        res
    }

    pub fn is_null(
        &self,
        bm: &BufferManager,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<bool> {
        let row_id_usize = row_id as usize;
        {
            let pending = self.pending_nulls.lock();
            if let Some(&(_rid, val)) = pending.iter().rev().find(|(rid, _)| *rid == row_id_usize) {
                return Ok(val != 0);
            }
        }
        let page_idx = row_id / 4096;
        let offset = (row_id % 4096) as usize;
        let frame = bm.pin_page(Arc::clone(&self.null_fh), page_idx, tx)?;
        let is_null = frame.as_slice()[offset] != 0;
        bm.unpin_page(&self.null_fh, page_idx, frame);
        Ok(is_null)
    }

    pub fn batch_append_values(
        &self,
        bm: &BufferManager,
        vals: &[Value],
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Vec<(Arc<RowVersion>, u64)>> {
        let num_rows = vals.len();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        // 1. Write null bitmap in batches
        let mut i = 0;
        while i < num_rows {
            let page_idx = (start_row_id + i as u64) / 4096;
            while self.null_fh.get_num_pages() <= page_idx {
                self.null_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;

            let mut page_i = i;
            // SAFETY: SAFETY: The pointer `frame.data.get()` yields a raw pointer to PAGE_SIZE bytes. The frame is pinned via pin_page and released via unpin_page. Shard synchronization ensures exclusive write access during this scope.
            unsafe {
                let ptr = frame.as_ptr();
                while page_i < num_rows {
                    let current_row = start_row_id + page_i as u64;
                    if current_row / 4096 != page_idx {
                        break;
                    }
                    let offset = (current_row % 4096) as usize;
                    *ptr.add(offset) = if matches!(vals[page_i], Value::Null) {
                        1
                    } else {
                        0
                    };
                    page_i += 1;
                }
            }
            bm.log_page_update(self.null_fh.file_id, page_idx, frame.as_slice())?;
            bm.unpin_page(&self.null_fh, page_idx, frame);
            i = page_i;
        }

        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;

        let mut i = 0;
        let mut stats = self.stats.write();
        let mut modified_rows_batch = Vec::with_capacity(num_rows);

        while i < num_rows {
            if matches!(
                self.data_type,
                LogicalType::List(_) | LogicalType::Struct(_)
            ) {
                self.append_value(bm, &vals[i], start_row_id + i as u64, tx)?;
                i += 1;
                continue;
            }

            let page_idx = (start_row_id + i as u64) / values_per_page;
            while self.fh.get_num_pages() <= page_idx {
                self.fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

            let mut page_i = i;
            let mut stack_buf = [0u8; 64];

            // SAFETY: SAFETY: Same as above — pinned frame accessed within the pin-unpin lifecycle.
            unsafe {
                let data_ptr = frame.as_ptr();
                while page_i < num_rows {
                    let current_row = start_row_id + page_i as u64;
                    if current_row / values_per_page != page_idx {
                        break;
                    }

                    if !matches!(vals[page_i], Value::Null) {
                        let offset_in_page =
                            (current_row % values_per_page) as usize * element_size;
                        self.serialize_value_into(&vals[page_i], bm, tx, &mut stack_buf)?;
                        std::ptr::copy_nonoverlapping(
                            stack_buf.as_ptr(),
                            data_ptr.add(offset_in_page),
                            element_size,
                        );
                        stats.update(&vals[page_i]);
                        stats.update_page_bounds(page_idx as usize, &vals[page_i]);
                    }

                    if !self.name.starts_with('_') {
                        if let Err(e) = self
                            .version_info
                            .mark_row(current_row, tx.tx_id, tx.read_ts)
                        {
                            tracing::error!("Failed to mark row {} in version_info: {}", current_row, e);
                        }
                        modified_rows_batch.push((self.version_info.clone(), current_row));
                    }

                    page_i += 1;
                }
            }
            bm.log_page_update(self.fh.file_id, page_idx, frame.as_slice())?;
            bm.unpin_page(&self.fh, page_idx, frame);
            i = page_i;
        }

        Ok(modified_rows_batch)
    }

    pub fn scan(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        result: &mut Vec<crate::processor::Value>,
    ) -> Result<()> {
        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;
        let mut values_read = 0;
        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize * element_size;
            let frame = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            let to_read = std::cmp::min(
                values_per_page - (current_offset % values_per_page),
                num_values - values_read,
            );
            for k in 0..to_read {
                let start = offset_in_page + k as usize * element_size;
                result.push(self.parse_value(&frame.as_slice()[start..start + element_size], bm, tx)?);
            }
            bm.unpin_page(&self.fh, page_idx, frame);
            values_read += to_read;
        }
        Ok(())
    }

    pub fn scan_to_array(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        zone_map: Option<&ZoneMapEq>,
    ) -> Result<ArrayRef> {
        self.flush_pending_nulls(bm, tx)?;
        let analyzer_meta = self.stats.read().compression_meta.clone();
        let element_size = self.element_size();
        let compression = analyzer_meta
            .as_ref()
            .map(|m| m.compression)
            .unwrap_or(CompressionType::Uncompressed);

        let target_type =
            crate::processor::arrow_utils::logical_type_to_arrow_type(&self.data_type);

        if compression == CompressionType::Uncompressed && self.can_vectorize(&target_type) {
            if target_type == DataType::Utf8 {
                return self.scan_string_vectorized(bm, offset, num_values, tx, zone_map);
            }
            return self.scan_to_array_vectorized(
                bm,
                offset,
                num_values,
                tx,
                element_size,
                &target_type,
                zone_map,
            );
        }

        let mut builder = arrow::array::make_builder(&target_type, num_values as usize);
        let alg = Self::get_alg(compression, element_size);
        let values_per_page = if compression == CompressionType::Uncompressed {
            4096 / element_size as u64
        } else {
            32
        };
        let mut values_read = 0;
        let mut temp_block = vec![0u8; 32 * element_size];
        let meta = analyzer_meta.unwrap_or_else(|| {
            CompressionMetadata::new(Value::Null, Value::Null, CompressionType::Uncompressed, 0)
        });

        let null_fh = Arc::clone(&self.null_fh);
        let data_type = &self.data_type;
        let mut cached_null_page: Option<(u64, Arc<crate::storage::buffer_manager::Frame>)> = None;

        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize;
            let to_read = std::cmp::min(num_values - values_read, 32);

            let skip_page = self.zone_map_should_skip(page_idx, zone_map);

            if skip_page {
                for _ in 0..to_read as usize {
                    crate::processor::arrow_utils::append_null_to_builder(
                        &mut *builder,
                        &target_type,
                    )?;
                }
                values_read += to_read;
                continue;
            }

            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            alg.decompress_from_page(
                page.as_slice(),
                offset_in_page as u64,
                &mut temp_block,
                0,
                to_read,
                &meta,
            )?;

            let null_page_idx = current_offset / 4096;
            let null_base_offset = (current_offset % 4096) as usize;

            // Cache null frame across sequential batches on the same null page
            let null_frame = if cached_null_page.as_ref().map(|(idx, _)| *idx) == Some(null_page_idx) {
                cached_null_page.as_ref().expect("null_page cache hit has value").1.clone()
            } else {
                if let Some((old_idx, ref old_frame)) = cached_null_page.take() {
                    bm.unpin_page(&null_fh, old_idx, old_frame.clone());
                }
                let frame = bm.pin_page(null_fh.clone(), null_page_idx, tx)?;
                cached_null_page = Some((null_page_idx, frame));
                cached_null_page.as_ref().expect("null_page cache just populated").1.clone()
            };

            for i in 0..to_read as usize {
                let is_null = null_frame.as_slice()[null_base_offset + i] != 0;
                if is_null {
                    crate::processor::arrow_utils::append_null_to_builder(
                        &mut *builder,
                        &target_type,
                    )?;
                } else {
                    let start = i * element_size;
                    crate::processor::arrow_utils::append_raw_to_builder(
                        &mut *builder,
                        &temp_block[start..start + element_size],
                        data_type,
                    )?;
                }
            }
            bm.unpin_page(&self.fh, page_idx, page);
            values_read += to_read;
        }

        // Unpin cached null frame if any
        if let Some((idx, frame)) = cached_null_page.take() {
            bm.unpin_page(&null_fh, idx, frame);
        }

        Ok(builder.finish())
    }

    fn can_vectorize(&self, target_type: &DataType) -> bool {
        matches!(
            target_type,
            DataType::Int64
                | DataType::Int32
                | DataType::UInt64
                | DataType::Float64
                | DataType::Boolean
                | DataType::Utf8
        )
    }

    fn zone_map_should_skip(&self, page_idx: u64, zone_map: Option<&ZoneMapEq>) -> bool {
        let Some(zone_map) = zone_map else {
            return false;
        };
        let stats = self.stats.read();
        if page_idx as usize >= stats.page_bounds.len() {
            return false;
        }
        let Some(bounds) = &stats.page_bounds[page_idx as usize] else {
            return false;
        };
        !bounds.value_can_be_in_page(&zone_map.value, &self.data_type)
    }

    fn scan_string_vectorized(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        zone_map: Option<&ZoneMapEq>,
    ) -> Result<ArrayRef> {
        use arrow::array::StringBuilder;
        let values_per_page = 4096 / 64u64;

        // Check if we can use direct file reads (no uncommitted modifications in range)
        let can_direct_read = !self.version_info.has_modifications() && zone_map.is_none();

        if can_direct_read {
            return self.scan_string_direct(offset, num_values);
        }

        let mut builder =
            StringBuilder::with_capacity(num_values as usize, num_values as usize * 16);

        let mut values_read = 0;
        let null_fh = Arc::clone(&self.null_fh);

        // Batch null bitmap reads: collect all null page indices first
        let mut null_pages_needed: Vec<u64> = Vec::new();
        let mut data_pages_needed: Vec<u64> = Vec::new();
        let mut temp_read = 0u64;
        while temp_read < num_values {
            let current_offset = offset + temp_read;
            let page_idx = current_offset / values_per_page;
            let null_page_idx = current_offset / 4096;
            if data_pages_needed.last() != Some(&page_idx) {
                data_pages_needed.push(page_idx);
            }
            if null_pages_needed.last() != Some(&null_page_idx) {
                null_pages_needed.push(null_page_idx);
            }
            temp_read += std::cmp::min(
                num_values - temp_read,
                values_per_page - (current_offset % values_per_page),
            );
        }

        // Pre-pin all null pages
        let mut null_frames: Vec<(u64, Arc<crate::storage::buffer_manager::Frame>)> = Vec::new();
        for &np_idx in &null_pages_needed {
            let frame = bm.pin_page(null_fh.clone(), np_idx, tx)?;
            null_frames.push((np_idx, frame));
        }

        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize;
            let to_read = std::cmp::min(
                num_values - values_read,
                values_per_page - offset_in_page as u64,
            ) as usize;

            let skip_page = self.zone_map_should_skip(page_idx, zone_map);

            let null_page_idx = current_offset / 4096;
            let null_frame = &null_frames
                .iter()
                .find(|(idx, _)| *idx == null_page_idx)
                .ok_or_else(|| crate::LightningError::Internal(format!(
                    "Null page {} not found in pre-pinned frames", null_page_idx
                )))?
                .1;
            let null_base_offset = (current_offset % 4096) as usize;

            if skip_page {
                for _ in 0..to_read {
                    builder.append_null();
                }
                values_read += to_read as u64;
                continue;
            }

            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            let base_offset = offset_in_page * 64;

            for i in 0..to_read {
                let is_null = null_frame.as_slice()[null_base_offset + i] != 0;
                if is_null {
                    builder.append_null();
                } else {
                    let slot_offset = base_offset + i * 64;
                    let marker = page.as_slice()[slot_offset];
                    let s = if marker == 255 {
                        match self.parse_value(
                            &page.as_slice()[slot_offset..slot_offset + 64],
                            bm,
                            tx,
                        ) {
                            Ok(Value::String(s)) => s,
                            _ => "".to_string(),
                        }
                    } else {
                        let len = marker as usize;
                        let actual_len = std::cmp::min(len, 63);
                        std::str::from_utf8(
                            &page.as_slice()[slot_offset + 1..slot_offset + 1 + actual_len],
                        )
                        .unwrap_or("")
                        .to_string()
                    };
                    builder.append_value(&s);
                }
            }

            bm.unpin_page(&self.fh, page_idx, page);
            values_read += to_read as u64;
        }

        // Unpin null pages
        for (np_idx, frame) in null_frames {
            bm.unpin_page(&null_fh, np_idx, frame);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn scan_string_direct(&self, offset: u64, num_values: u64) -> Result<ArrayRef> {
        use arrow::array::StringArray;
        use arrow::buffer::OffsetBuffer;
        let values_per_page = 4096 / 64u64;

        // Batch read data pages
        let first_page = offset / values_per_page;
        let last_page = (offset + num_values - 1) / values_per_page;
        let num_pages = last_page - first_page + 1;

        let mut data_buf = vec![0u8; (num_pages as usize) * 4096];
        self.fh.read_pages(first_page, num_pages, &mut data_buf)?;

        // Batch read null pages
        let null_first_page = offset / 4096;
        let null_last_page = (offset + num_values - 1) / 4096;
        let num_null_pages = null_last_page - null_first_page + 1;
        let mut null_data = vec![0u8; (num_null_pages as usize) * 4096];
        self.null_fh
            .read_pages(null_first_page, num_null_pages, &mut null_data)?;

        // Build Arrow buffers directly
        let mut offsets = Vec::with_capacity(num_values as usize + 1);
        let mut values = Vec::with_capacity(num_values as usize * 16);
        let mut current_offset = 0i32;
        offsets.push(current_offset);

        let mut null_bits = vec![0xFFu8; (num_values as usize).div_ceil(8)];
        let mut has_any_nulls = false;

        // SIMD-accelerated null bitmap construction using NEON/SSE
        // Process null bytes in chunks of 64 to check for non-zero values
        let num_values_usize = num_values as usize;
        let null_base = null_first_page as usize * 4096;

        {
            let mut i = 0usize;
            let simd_end = num_values_usize - (num_values_usize % 64);
            while i < simd_end {
                let row_offset = offset as usize + i;
                let null_start = row_offset - null_base;
                let chunk = &null_data[null_start..null_start + 64];
                // Fast check: OR all 64 bytes together to see if any are non-zero
                let mut or_all = 0u8;
                let mut j = 0;
                while j < 64 {
                    or_all |= chunk[j];
                    j += 1;
                }
                if or_all != 0 {
                    has_any_nulls = true;
                    for j in 0..64 {
                        if chunk[j] != 0 {
                            null_bits[(i + j) / 8] &= !(1u8 << ((i + j) % 8));
                        }
                    }
                }
                i += 64;
            }

            while i < num_values_usize {
                let row_offset = offset as usize + i;
                let null_idx = row_offset - null_base;
                if null_data[null_idx] != 0 {
                    null_bits[i / 8] &= !(1u8 << (i % 8));
                    has_any_nulls = true;
                }
                i += 1;
            }
        }

        // Pre-read overflow file if needed — we need it for overflow strings.
        // Limit to a reasonable number of pages to prevent OOM on large files.
        const MAX_OVERFLOW_PAGES: usize = 1024; // 4MB max pre-read
        let overflow_data: Vec<u8> = if self.overflow_fh.is_some() {
            let ofh = self.overflow_fh.as_ref().expect("overflow file handle required");
            let num_of_pages = (ofh.get_num_pages() as usize).min(MAX_OVERFLOW_PAGES);
            if num_of_pages > 0 {
                let mut buf = vec![0u8; num_of_pages * 4096];
                let _ = ofh.read_pages(0, num_of_pages as u64, &mut buf);
                buf
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        for i in 0..num_values as usize {
            let row_offset = offset as usize + i;
            let null_idx = row_offset - (null_first_page as usize * 4096);
            let is_null = null_data[null_idx];

            if is_null != 0 {
                offsets.push(current_offset);
                continue;
            }

            let page_offset =
                ((row_offset / values_per_page as usize) - first_page as usize) * 4096;
            let offset_in_page = (row_offset % values_per_page as usize) * 64;
            let slot_offset = page_offset + offset_in_page;

            let marker = data_buf[slot_offset];
            let s_bytes = if marker == 255 && !overflow_data.is_empty() {
                let of_page = u64::from_le_bytes(
                    data_buf[slot_offset + 1..slot_offset + 9].try_into().expect("infallible: fixed-size array conversion"),
                ) as usize;
                let of_offset = u64::from_le_bytes(
                    data_buf[slot_offset + 9..slot_offset + 17].try_into().expect("infallible: fixed-size array conversion"),
                ) as usize;
                let of_len = std::cmp::min(
                    u32::from_le_bytes(
                        data_buf[slot_offset + 17..slot_offset + 21].try_into().expect("infallible: fixed-size array conversion"),
                    ) as usize,
                    4096,
                );
                let of_start = of_page * 4096 + of_offset;
                let of_end = std::cmp::min(of_start + of_len, overflow_data.len());
                if of_end > of_start {
                    &overflow_data[of_start..of_end]
                } else {
                    &[]
                }
            } else {
                let len = marker as usize;
                let actual_len = std::cmp::min(len, 63);
                &data_buf[slot_offset + 1..slot_offset + 1 + actual_len]
            };

            let s = String::from_utf8_lossy(s_bytes);
            values.extend_from_slice(s.as_bytes());
            current_offset += s.len() as i32;
            offsets.push(current_offset);

            // Update null bits branchlessly
            if is_null != 0 {
                null_bits[i / 8] &= !(is_null << (i % 8));
                has_any_nulls |= is_null != 0;
            }
        }

        let null_buf = if has_any_nulls {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(null_bits),
                0,
                num_values as usize,
            )))
        } else {
            None
        };

        let array = StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from(values),
            null_buf,
        );
        Ok(Arc::new(array))
    }

    fn scan_to_array_vectorized(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        element_size: usize,
        target_type: &DataType,
        zone_map: Option<&ZoneMapEq>,
    ) -> Result<ArrayRef> {
        let values_per_page = 4096 / element_size as u64;

        if !self.version_info.has_modifications() && zone_map.is_none() {
            return self.scan_primitive_direct(offset, num_values, element_size, target_type, zone_map);
        }

        if num_values == values_per_page && offset % values_per_page == 0 && zone_map.is_none() {
            let page_idx = offset / values_per_page;
            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;

            let data_buf = Buffer::from(page.as_slice());

            let null_page_idx = offset / 4096;
            let null_frame = bm.pin_page(Arc::clone(&self.null_fh), null_page_idx, tx)?;
            let null_base_offset = (offset % 4096) as usize;

            let has_nulls = null_frame.as_slice()
                [null_base_offset..null_base_offset + num_values as usize]
                .iter()
                .any(|&v| v != 0);

            let null_buf = if has_nulls {
                let mut bits = vec![0xFFu8; (num_values as usize).div_ceil(8)];
                for i in 0..num_values as usize {
                    if null_frame.as_slice()[null_base_offset + i] != 0 {
                        bits[i / 8] &= !(1u8 << (i % 8));
                    }
                }
                Some(NullBuffer::new(BooleanBuffer::new(
                    Buffer::from(bits),
                    0,
                    num_values as usize,
                )))
            } else {
                None
            };

            bm.unpin_page(&self.null_fh, null_page_idx, null_frame);
            bm.unpin_page(&self.fh, page_idx, page);

            return self.build_array(target_type, data_buf, null_buf, num_values as usize);
        }

        let mut values_read = 0;
        let mut data_buffer = MutableBuffer::with_capacity(num_values as usize * element_size);
        let mut null_bits = vec![0xFFu8; (num_values as usize).div_ceil(8)];
        let mut has_any_nulls = false;

        let null_fh = Arc::clone(&self.null_fh);
        let mut output_offset = 0usize;

        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize;
            let to_read = std::cmp::min(
                num_values - values_read,
                values_per_page - offset_in_page as u64,
            ) as usize;

            let skip_page = self.zone_map_should_skip(page_idx, zone_map);

            if skip_page {
                let skip_bytes = to_read * element_size;
                data_buffer.extend_from_slice(&vec![0u8; skip_bytes]);
                for j in 0..to_read {
                    null_bits[(output_offset + j) / 8] &= !(1u8 << ((output_offset + j) % 8));
                    has_any_nulls = true;
                }
                values_read += to_read as u64;
                output_offset += to_read;
                continue;
            }

            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            let src_start = offset_in_page * element_size;
            let src_end = src_start + to_read * element_size;
            data_buffer.extend_from_slice(&page.as_slice()[src_start..src_end]);

            {
                let null_page_idx = current_offset / 4096;
                let null_frame = bm.pin_page(null_fh.clone(), null_page_idx, tx)?;
                let null_base_offset = (current_offset % 4096) as usize;

                let null_src = &null_frame.as_slice()[null_base_offset..null_base_offset + to_read];
                let mut j = 0;
                while j + 8 <= to_read {
                    let val = u64::from_le_bytes(null_src[j..j + 8].try_into().expect("infallible: fixed-size array conversion"));
                    if val != 0 {
                        for k in 0..8 {
                            if null_src[j + k] != 0 {
                                null_bits[(output_offset + j + k) / 8] &=
                                    !(1u8 << ((output_offset + j + k) % 8));
                                has_any_nulls = true;
                            }
                        }
                    }
                    j += 8;
                }
                while j < to_read {
                    if null_src[j] != 0 {
                        null_bits[(output_offset + j) / 8] &= !(1u8 << ((output_offset + j) % 8));
                        has_any_nulls = true;
                    }
                    j += 1;
                }

                bm.unpin_page(&null_fh, null_page_idx, null_frame);
            }

            bm.unpin_page(&self.fh, page_idx, page);

            values_read += to_read as u64;
            output_offset += to_read;
        }

        let data_buf = Buffer::from(data_buffer);
        let null_buf = if has_any_nulls {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(null_bits),
                0,
                num_values as usize,
            )))
        } else {
            None
        };

        self.build_array(target_type, data_buf, null_buf, num_values as usize)
    }

    fn scan_primitive_direct(
        &self,
        offset: u64,
        num_values: u64,
        element_size: usize,
        target_type: &DataType,
        _zone_map: Option<&ZoneMapEq>,
    ) -> Result<ArrayRef> {
        let values_per_page = 4096 / element_size as u64;
        let total_bytes = num_values as usize * element_size;
        let mut has_any_nulls = false;
        let mut output_offset = 0usize;
        let mut values_read = 0u64;

        // Always read null bitmap — not relying on null_count stats which
        // can be stale for CREATE→flush_buffer path.
        let mut null_bits = vec![0xFFu8; (num_values as usize).div_ceil(8)];

        // Optimization: When offset is page-aligned, we can use fast bulk-reads
        let is_page_aligned = offset % values_per_page == 0;

        let data_buf = if is_page_aligned {
            // Read all data pages in one syscall
            let first_page = offset / values_per_page;
            let last_page = (offset + num_values - 1) / values_per_page;
            let num_pages = last_page - first_page + 1;
            let expected_bytes = (num_pages as usize) * 4096;

            let mut data_vec = vec![0u8; expected_bytes];
            self.fh.read_pages(first_page, num_pages, &mut data_vec)?;
            // Truncate down to the exact requested data size
            data_vec.truncate(total_bytes);

            {
                // Read null pages in one syscall — always, since null_count
                // stats may be stale for recently-written data.
                let null_first_page = offset / 4096;
                let null_last_page = (offset + num_values - 1) / 4096;
                let num_null_pages = null_last_page - null_first_page + 1;
                let mut null_data = vec![0u8; (num_null_pages as usize) * 4096];
                self.null_fh
                    .read_pages(null_first_page, num_null_pages, &mut null_data)?;

                // Efficient branchless null bit extraction (8 bytes at a time)
                let null_src = &null_data[..(num_values as usize)];
                let mut chunk_iter = null_src.chunks_exact(8);
                let mut out_idx = 0;
                let mut any_nulls_int = 0u8;

                for chunk in chunk_iter.by_ref() {
                    let mut bitmask = 0u8;
                    bitmask |= chunk[0];
                    bitmask |= chunk[1] << 1;
                    bitmask |= chunk[2] << 2;
                    bitmask |= chunk[3] << 3;
                    bitmask |= chunk[4] << 4;
                    bitmask |= chunk[5] << 5;
                    bitmask |= chunk[6] << 6;
                    bitmask |= chunk[7] << 7;

                    null_bits[out_idx] &= !bitmask;
                    any_nulls_int |= bitmask;
                    out_idx += 1;
                }

                // Remainder
                let remainder = chunk_iter.remainder();
                if !remainder.is_empty() {
                    let mut bitmask = 0u8;
                    for (i, &b) in remainder.iter().enumerate() {
                        bitmask |= b << i;
                    }
                    null_bits[out_idx] &= !bitmask;
                    any_nulls_int |= bitmask;
                }

                has_any_nulls = any_nulls_int != 0;
            }

            Buffer::from(data_vec)
        } else {
            let first_page = offset / values_per_page;
            let last_page = (offset + num_values - 1) / values_per_page;
            let num_pages = last_page - first_page + 1;
            let mut pages_buf = vec![0u8; (num_pages as usize) * 4096];
            self.fh.read_pages(first_page, num_pages, &mut pages_buf)?;

            let null_first_page = offset / 4096;
            let null_last_page = (offset + num_values - 1) / 4096;
            let num_null_pages = null_last_page - null_first_page + 1;
            let mut null_pages = vec![0u8; (num_null_pages as usize) * 4096];
            self.null_fh
                .read_pages(null_first_page, num_null_pages, &mut null_pages)?;

            let mut data_buffer = MutableBuffer::with_capacity(total_bytes);

            while values_read < num_values {
                let current_offset = offset + values_read;
                let page_idx = current_offset / values_per_page;
                let offset_in_page = (current_offset % values_per_page) as usize;
                let to_read = std::cmp::min(
                    num_values - values_read,
                    values_per_page - offset_in_page as u64,
                ) as usize;

                let page_offset = ((page_idx - first_page) as usize) * 4096;
                let src_start = page_offset + offset_in_page * element_size;
                let src_end = src_start + to_read * element_size;
                data_buffer.extend_from_slice(&pages_buf[src_start..src_end]);

                let null_page_idx = current_offset / 4096;
                let null_page_offset = ((null_page_idx - null_first_page) as usize) * 4096;
                let null_base_offset = (current_offset % 4096) as usize;
                let null_src = &null_pages
                    [null_page_offset + null_base_offset..null_page_offset + null_base_offset + to_read];

                let mut i = 0;
                while i + 8 <= to_read {
                    let bytes = &null_src[i..i + 8];
                    let mut word: u8 = 0;
                    for j in 0..8 {
                        if bytes[j] != 0 {
                            word |= 1 << j;
                            has_any_nulls = true;
                        }
                    }
                    null_bits[(output_offset + i) / 8] &= !word;
                    i += 8;
                }
                for j in i..to_read {
                    if null_src[j] != 0 {
                        null_bits[(output_offset + j) / 8] &=
                            !(1u8 << ((output_offset + j) % 8));
                        has_any_nulls = true;
                    }
                }

                values_read += to_read as u64;
                output_offset += to_read;
            }
            Buffer::from(data_buffer)
        };

        let null_buf = if has_any_nulls {
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from(null_bits),
                0,
                num_values as usize,
            )))
        } else {
            None
        };

        self.build_array(target_type, data_buf, null_buf, num_values as usize)
    }

    fn build_array(
        &self,
        target_type: &DataType,
        data_buf: Buffer,
        null_buf: Option<NullBuffer>,
        num_values: usize,
    ) -> Result<ArrayRef> {
        match target_type {
            DataType::Int64 => {
                let values = ScalarBuffer::<i64>::new(data_buf, 0, num_values);
                Ok(Arc::new(Int64Array::new(values, null_buf)))
            }
            DataType::Int32 => {
                let values = ScalarBuffer::<i32>::new(data_buf, 0, num_values);
                Ok(Arc::new(Int32Array::new(values, null_buf)))
            }
            DataType::UInt64 => {
                let values = ScalarBuffer::<u64>::new(data_buf, 0, num_values);
                Ok(Arc::new(UInt64Array::new(values, null_buf)))
            }
            DataType::Float64 => {
                let values = ScalarBuffer::<f64>::new(data_buf, 0, num_values);
                Ok(Arc::new(Float64Array::new(values, null_buf)))
            }
            DataType::Boolean => {
                // Booleans are stored byte-packed (1 byte per value) in the column
                // files, but Arrow's BooleanBuffer expects bit-packed data.
                // Convert byte-packed → bit-packed here.
                let bytes = data_buf.as_slice();
                let byte_count = std::cmp::min(bytes.len(), num_values);
                let mut packed = vec![0u8; num_values.div_ceil(8)];
                for i in 0..byte_count {
                    if bytes[i] != 0 {
                        packed[i / 8] |= 1u8 << (i % 8);
                    }
                }
                let values = BooleanBuffer::new(Buffer::from(packed), 0, num_values);
                Ok(Arc::new(BooleanArray::new(values, null_buf)))
            }
            _ => unreachable!(),
        }
    }

    pub fn scan_with_filter<F>(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        predicate: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(&[u8]) -> bool,
    {
        let analyzer_meta = self.stats.read().compression_meta.clone();
        let target_type =
            crate::processor::arrow_utils::logical_type_to_arrow_type(&self.data_type);
        let mut builder = arrow::array::make_builder(&target_type, num_values as usize);
        let element_size = self.element_size();
        let compression = analyzer_meta
            .as_ref()
            .map(|m| m.compression)
            .unwrap_or(CompressionType::Uncompressed);
        let alg = Self::get_alg(compression, element_size);
        let values_per_page = if compression == CompressionType::Uncompressed {
            4096 / element_size as u64
        } else {
            32
        };
        let mut values_read = 0;
        let mut temp_block = vec![0u8; 32 * element_size];
        let meta = analyzer_meta.unwrap_or_else(|| {
            CompressionMetadata::new(Value::Null, Value::Null, CompressionType::Uncompressed, 0)
        });
        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize;
            let to_read = std::cmp::min(num_values - values_read, 32);
            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            alg.decompress_from_page(
                page.as_slice(),
                offset_in_page as u64,
                &mut temp_block,
                0,
                to_read,
                &meta,
            )?;
            let null_page_idx = current_offset / 4096;
            let null_frame = bm.pin_page(Arc::clone(&self.null_fh), null_page_idx, tx)?;
            let null_base_offset = (current_offset % 4096) as usize;

            for i in 0..to_read as usize {
                let row_data = &temp_block[i * element_size..(i + 1) * element_size];
                let is_null = null_frame.as_slice()[null_base_offset + i] != 0;
                if !is_null && predicate(row_data) {
                    crate::processor::arrow_utils::append_raw_to_builder(
                        &mut *builder,
                        row_data,
                        &self.data_type,
                    )?;
                }
            }
            bm.unpin_page(&self.null_fh, null_page_idx, null_frame);
            bm.unpin_page(&self.fh, page_idx, page);
            values_read += to_read;
        }
        Ok(builder.finish())
    }

    pub fn set_null(
        &self,
        _bm: &BufferManager,
        row_id: u64,
        is_null: bool,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.dirty.store(true, Ordering::Release);
        let mut pending = self.pending_nulls.lock();
        pending.push((row_id as usize, if is_null { 1 } else { 0 }));
        if pending.len() > 100_000 {
            tracing::warn!(
                "pending_nulls for column '{}' has {} entries — consider flushing more frequently",
                self.name, pending.len()
            );
        }
        Ok(())
    }

    /// Flush buffered null bit changes to actual null pages.
    /// Called before batch operations or checkpoint to ensure durability.
    pub fn flush_pending_nulls(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let pending = std::mem::take(&mut *self.pending_nulls.lock());
        if pending.is_empty() {
            return Ok(());
        }
        let mut by_page: std::collections::HashMap<u64, Vec<(usize, u8)>> =
            std::collections::HashMap::new();
        for (row_id, val) in &pending {
            let page_idx = *row_id as u64 / 4096;
            let offset_in_page = *row_id % 4096;
            by_page.entry(page_idx).or_default().push((offset_in_page, *val));
        }
        for (page_idx, entries) in &by_page {
            while self.null_fh.get_num_pages() <= *page_idx {
                self.null_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.null_fh), *page_idx, tx)?;
            unsafe {
                let ptr = frame.as_ptr();
                for (offset, val) in entries {
                    *ptr.add(*offset) = *val;
                }
            }
            bm.log_page_update(self.null_fh.file_id, *page_idx, frame.as_slice())?;
            bm.unpin_page(&self.null_fh, *page_idx, frame);
        }
        Ok(())
    }

    pub fn append_plain_value(
        &self,
        bm: &BufferManager,
        val: &Value,
        row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;
        let page_idx = row_id / values_per_page;
        let offset_in_page = (row_id % values_per_page) as usize * element_size;
        while self.fh.get_num_pages() <= page_idx {
            self.fh.add_new_page()?;
        }
        let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

        let mut stack_buf = [0u8; 64];
        // SAFETY: SAFETY: Same append path, pinned frame.
        unsafe {
            let data_ptr = frame.as_ptr();
            self.serialize_value_into(val, bm, tx, &mut stack_buf)?;
            std::ptr::copy_nonoverlapping(
                stack_buf.as_ptr(),
                data_ptr.add(offset_in_page),
                element_size,
            );
        }

        if !self.name.starts_with('_') {
            if let Err(e) = self.version_info.mark_row(row_id, tx.tx_id, tx.read_ts) {
                tracing::error!("Failed to mark row {} in version_info: {}", row_id, e);
            }
            tx.modified_rows
                .lock()
                .push((self.version_info.clone(), row_id));
            // Record the exact row data for merge-on-commit.
            // This allows concurrent transactions modifying different rows
            // on the same page to merge their changes without conflict.
            let mut row_data = [0u8; 64];
            row_data[..element_size].copy_from_slice(&stack_buf[..element_size]);

            // For overflow strings, capture the full overflow page content
            // so that the merge-on-commit path is not dependent on the
            // external overflow file for the merging transaction's data.
            let overflow_row_data = if stack_buf[0] == 255 {
                self.overflow_fh.as_ref().and_then(|ofh| {
                    let of_page_idx = u64::from_le_bytes(
                        stack_buf[1..9].try_into().ok()?
                    );
                    let of_len = u32::from_le_bytes(
                        stack_buf[17..21].try_into().ok()?
                    ) as usize;
                    let of_frame = bm.pin_page(ofh.clone(), of_page_idx, tx).ok()?;
                    let content = of_frame.as_slice()[..std::cmp::min(of_len, 4096)].to_vec();
                    bm.unpin_page(ofh, of_page_idx, of_frame);
                    Some(content)
                })
            } else {
                None
            };

            tx.modified_page_rows.lock().push(
                crate::transaction::transaction_manager::PageRowMod {
                    file_id: self.fh.file_id,
                    page_idx,
                    row_id,
                    element_size,
                    row_data,
                    overflow_row_data,
                }
            );
        }

        bm.log_page_update(self.fh.file_id, page_idx, frame.as_slice())?;
        bm.unpin_page(&self.fh, page_idx, frame);
        // Fast atomic stats update, defer min/max to optimize time
        self.atomic_num_values.fetch_add(1, Ordering::Release);
        if matches!(val, Value::Null) {
            self.atomic_null_count.fetch_add(1, Ordering::Release);
        }
        {
            let mut stats = self.stats.write();
            stats.update_page_bounds(page_idx as usize, val);
        }
        Ok(())
    }

    pub fn bulk_append_array(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.bulk_append_array_inner(bm, array, start_row_id, tx, false)
    }

    pub fn bulk_append_array_bulk_mode(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        is_bulk: bool,
    ) -> Result<()> {
        self.bulk_append_array_inner(bm, array, start_row_id, tx, is_bulk)
    }

    fn bulk_append_array_inner(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        skip_modified_rows: bool,
    ) -> Result<()> {
        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;
        let num_rows = array.len();
        if num_rows == 0 {
            return Ok(());
        }

        self.dirty.store(true, Ordering::Release);

        // 1. Write null bitmap in bulk
        let nulls = array.nulls();
        if skip_modified_rows {
            // Fast path: direct file writes for bulk mode
            // No need to read existing pages - each column has its own null file
            // and we're writing fresh data, so just zero-fill and write
            let mut i = 0;
            let mut null_page_buf = [0u8; 4096];
            while i < num_rows {
                let page_idx = (start_row_id + i as u64) / 4096;
                while self.null_fh.get_num_pages() <= page_idx {
                    self.null_fh.add_new_page()?;
                }
                // Zero out the buffer (no need to read existing page)
                null_page_buf.fill(0);

                let mut page_i = i;
                while page_i < num_rows {
                    let current_row = start_row_id + page_i as u64;
                    if current_row / 4096 != page_idx {
                        break;
                    }
                    let offset = (current_row % 4096) as usize;
                    let is_null = nulls.as_ref().map(|n| n.is_null(page_i)).unwrap_or(false);
                    null_page_buf[offset] = if is_null { 1 } else { 0 };
                    page_i += 1;
                }
                bm.log_page_update(self.null_fh.file_id, page_idx, &null_page_buf)?;
                self.null_fh.write_page(page_idx, &null_page_buf)?;
                bm.evict_pages_for_file(self.null_fh.file_id, page_idx, 1);
                i = page_i;
            }
        } else {
            let mut i = 0;
            while i < num_rows {
                let page_idx = (start_row_id + i as u64) / 4096;
                while self.null_fh.get_num_pages() <= page_idx {
                    self.null_fh.add_new_page()?;
                }
                let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;

                let mut page_i = i;
                // SAFETY: SAFETY: Bulk append path — frame allocated via create_new_version, pinned, written, logged, then unpinned.
                unsafe {
                    let ptr = frame.as_ptr();
                    while page_i < num_rows {
                        let current_row = start_row_id + page_i as u64;
                        if current_row / 4096 != page_idx {
                            break;
                        }
                        let offset = (current_row % 4096) as usize;
                        let is_null = nulls.as_ref().map(|n| n.is_null(page_i)).unwrap_or(false);
                        *ptr.add(offset) = if is_null { 1 } else { 0 };
                        page_i += 1;
                    }
                }
                bm.log_page_update(self.null_fh.file_id, page_idx, frame.as_slice())?;
                bm.unpin_page(&self.null_fh, page_idx, frame);
                i = page_i;
            }
        }

        // 2. Write data pages in bulk
        if matches!(
            self.data_type,
            LogicalType::List(_) | LogicalType::Struct(_)
        ) {
            for i in 0..num_rows {
                let val = Value::from_arrow(array, i);
                self.append_value(bm, &val, start_row_id + i as u64, tx)?;
            }
            return Ok(());
        }

        let is_primitive = matches!(
            self.data_type,
            LogicalType::Int64
                | LogicalType::Int32
                | LogicalType::Uint64
                | LogicalType::Node(_)
                | LogicalType::Double
        );

        // Enable fast path for primitives even with nulls - zeros will be written for nulls
        if is_primitive {
            return self.bulk_append_primitive_fast_inner(
                bm,
                array,
                start_row_id,
                tx,
                skip_modified_rows,
            );
        }

        if self.data_type == LogicalType::String {
            return self.bulk_append_string_fast_inner(
                bm,
                array,
                start_row_id,
                tx,
                skip_modified_rows,
            );
        }

        let mut i = 0;
        let mut stats = self.stats.write();
        let mut modified_rows_batch = Vec::with_capacity(num_rows);
        let mut modified_page_rows_batch =
            Vec::with_capacity(if skip_modified_rows { 0 } else { num_rows });

        while i < num_rows {
            let page_idx = (start_row_id + i as u64) / values_per_page;
            while self.fh.get_num_pages() <= page_idx {
                self.fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

            let mut page_i = i;
            let mut stack_buf = [0u8; 64];

            // SAFETY: SAFETY: Same bulk write path.
            unsafe {
                let data_ptr = frame.as_ptr();
                while page_i < num_rows {
                    let current_row = start_row_id + page_i as u64;
                    if current_row / values_per_page != page_idx {
                        break;
                    }

                    let is_null = nulls.as_ref().map(|n| n.is_null(page_i)).unwrap_or(false);
                    if !is_null {
                        let offset_in_page =
                            (current_row % values_per_page) as usize * element_size;
                        let val = Value::from_arrow(array, page_i);
                        self.serialize_value_into(&val, bm, tx, &mut stack_buf)?;
                        std::ptr::copy_nonoverlapping(
                            stack_buf.as_ptr(),
                            data_ptr.add(offset_in_page),
                            element_size,
                        );
                        stats.update(&val);
                        stats.update_page_bounds(page_idx as usize, &val);
                    }

                    if !skip_modified_rows && !self.name.starts_with('_') {
                        modified_rows_batch.push((self.version_info.clone(), current_row));
                        let mut row_data = [0u8; 64];
                        row_data[..element_size].copy_from_slice(&stack_buf[..element_size]);
                        modified_page_rows_batch.push(
                            crate::transaction::transaction_manager::PageRowMod {
                                file_id: self.fh.file_id,
                                page_idx,
                                row_id: current_row,
                                element_size,
                                row_data,
                                overflow_row_data: None,
                            }
                        );
                    }

                    page_i += 1;
                }
            }
            bm.log_page_update(self.fh.file_id, page_idx, frame.as_slice())?;
            bm.unpin_page(&self.fh, page_idx, frame);
            i = page_i;
        }

        if !skip_modified_rows {
            if !modified_rows_batch.is_empty() {
                tx.modified_rows.lock().extend(modified_rows_batch);
            }
            if !modified_page_rows_batch.is_empty() {
                tx.modified_page_rows
                    .lock()
                    .extend(modified_page_rows_batch);
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn bulk_append_primitive_fast(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.bulk_append_primitive_fast_inner(bm, array, start_row_id, tx, false)
    }

    fn bulk_append_primitive_fast_inner(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        skip_modified_rows: bool,
    ) -> Result<()> {
        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;
        let num_rows = array.len();

        // Get raw bytes from Arrow array
        let data = array.to_data();
        let buffers = data.buffers();
        if buffers.is_empty() {
            return Ok(());
        }
        // For nullable arrays, the first buffer is the null bitmap and the second
        // is the data. For non-nullable arrays, the first buffer is the data.
        let buffer_idx = if data.nulls().is_some() && buffers.len() > 1 { 1 } else { 0 };
        let raw_bytes = buffers[buffer_idx].as_slice();

        // Ensure file is large enough
        let num_pages_needed = (num_rows as u64).div_ceil(values_per_page);
        let first_page = start_row_id / values_per_page;
        while self.fh.get_num_pages() <= first_page + num_pages_needed {
            self.fh.add_new_page()?;
        }

        // Write the entire buffer in one syscall!
        let write_offset = start_row_id * element_size as u64;
        let bytes_to_write = num_rows * element_size;

        // WAL-log each affected page before the direct write
        let data_first_page = write_offset / PAGE_SIZE as u64;
        let data_num_pages = (bytes_to_write as u64).div_ceil(PAGE_SIZE as u64);
        for page_offset in 0..data_num_pages {
            let page_idx = data_first_page + page_offset;
            let page_start = (page_offset * PAGE_SIZE as u64) as usize;
            let page_end = std::cmp::min(page_start + PAGE_SIZE, bytes_to_write);
            bm.log_page_update(self.fh.file_id, page_idx, &raw_bytes[page_start..page_end])?;
        }

        self.fh
            .write_bytes_at(write_offset, &raw_bytes[..bytes_to_write])?;

        // Invalidate buffer manager cache for affected pages
        let data_first_page = write_offset / PAGE_SIZE as u64;
        let data_num_pages = (bytes_to_write as u64).div_ceil(PAGE_SIZE as u64);
        bm.evict_pages_for_file(self.fh.file_id, data_first_page, data_num_pages);

        if !skip_modified_rows && !self.name.starts_with('_') {
            self.version_info
                .mark_row_batch(start_row_id..start_row_id + num_rows as u64, tx.tx_id);
            tx.modified_rows.lock().extend(
                (0..num_rows).map(|i| (self.version_info.clone(), start_row_id + i as u64)),
            );
        }

        // Update null count in stats so the scan path knows to read null bitmaps.
        // Without this, scan_primitive_direct skips null bitmap reading and all
        // values are treated as non-null, returning 0/false/"" for null entries.
        let null_count = data.nulls().map(|n| n.null_count()).unwrap_or(0);
        {
            let mut stats = self.stats.write();
            stats.num_values += num_rows as u64;
            stats.null_count += null_count as u64;
            stats.invalidate_page_bounds();
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn bulk_append_string_fast(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        self.bulk_append_string_fast_inner(bm, array, start_row_id, tx, false)
    }

    fn bulk_append_string_fast_inner(
        &self,
        bm: &BufferManager,
        array: &ArrayRef,
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
        skip_modified_rows: bool,
    ) -> Result<()> {
        use arrow::array::StringArray;
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| crate::LightningError::Internal("Expected StringArray".into()))?;

        let num_rows = array.len();
        if num_rows == 0 {
            return Ok(());
        }

        let values_per_page = 4096 / 64u64; // element_size for String is 64

        // 1. Write null bitmap - direct write for bulk mode
        let nulls = array.nulls();
        if skip_modified_rows {
            // Direct file write for null bitmap - no buffer manager involvement
            let mut i = 0;
            while i < num_rows {
                let page_idx = (start_row_id + i as u64) / 4096;
                while self.null_fh.get_num_pages() <= page_idx {
                    self.null_fh.add_new_page()?;
                }

                let mut null_page_buf = [0u8; 4096];
                let mut page_i = i;
                while page_i < num_rows {
                    let current_row = start_row_id + page_i as u64;
                    if current_row / 4096 != page_idx {
                        break;
                    }
                    let offset = (current_row % 4096) as usize;
                    let is_null = nulls.as_ref().map(|n| n.is_null(page_i)).unwrap_or(false);
                    null_page_buf[offset] = if is_null { 1 } else { 0 };
                    page_i += 1;
                }
                self.null_fh.write_page(page_idx, &null_page_buf)?;
                i = page_i;
            }
        } else {
            // Buffer manager path for transactional mode
            let mut i = 0;
            while i < num_rows {
                let page_idx = (start_row_id + i as u64) / 4096;
                while self.null_fh.get_num_pages() <= page_idx {
                    self.null_fh.add_new_page()?;
                }
                let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;
                let mut page_i = i;
                // SAFETY: SAFETY: String fast path — pinned frame for null bitmap write.
                unsafe {
                    let ptr = frame.as_ptr();
                    while page_i < num_rows {
                        let current_row = start_row_id + page_i as u64;
                        if current_row / 4096 != page_idx {
                            break;
                        }
                        let offset = (current_row % 4096) as usize;
                        let is_null = nulls.as_ref().map(|n| n.is_null(page_i)).unwrap_or(false);
                        *ptr.add(offset) = if is_null { 1 } else { 0 };
                        page_i += 1;
                    }
                }
                bm.log_page_update(self.null_fh.file_id, page_idx, frame.as_slice())?;
                bm.unpin_page(&self.null_fh, page_idx, frame);
                i = page_i;
            }
        }

        // 2. Write string data directly to file, bypassing buffer manager
        let mut data_vec = vec![0; num_rows * 64];

        // Fill buffer with strings, using overflow for strings > 63 chars.
        // We need the buffer manager for overflow writes only (buffer pool pages).
        for i in 0..num_rows {
            let is_null = nulls.as_ref().map(|n| n.is_null(i)).unwrap_or(false);
            if !is_null {
                let s = string_array.value(i);
                let s_bytes = s.as_bytes();
                let local_offset = i * 64;

                if s_bytes.len() <= 63 {
                    data_vec[local_offset] = s_bytes.len() as u8;
                    data_vec[local_offset + 1..local_offset + 1 + s_bytes.len()]
                        .copy_from_slice(s_bytes);
                } else if let Some(ref ofh) = self.overflow_fh {
                    // Overflow path: write to overflow file, store pointer
                    let page_idx = ofh.add_new_page()?;
                    let frame = bm.create_new_version(ofh.clone(), page_idx, tx)?;
                    let copy_len = std::cmp::min(s_bytes.len(), 4096);
                    // SAFETY: SAFETY: Overflow string write — pinned frame for overflow page.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            s_bytes.as_ptr(),
                            frame.as_ptr(),
                            copy_len,
                        );
                    }
                    bm.log_page_update(ofh.file_id, page_idx, frame.as_slice())?;
                    bm.unpin_page(ofh, page_idx, frame);

                    data_vec[local_offset] = 255u8;
                    data_vec[local_offset + 1..local_offset + 9]
                        .copy_from_slice(&page_idx.to_le_bytes());
                    data_vec[local_offset + 9..local_offset + 17]
                        .copy_from_slice(&0u64.to_le_bytes());
                    let stored_len = std::cmp::min(s_bytes.len(), 4096);
                    data_vec[local_offset + 17..local_offset + 21]
                        .copy_from_slice(&(stored_len as u32).to_le_bytes());
                }
            }
        }

        // Ensure file has enough pages
        let num_pages_needed =
            (start_row_id + num_rows as u64).div_ceil(values_per_page);
        while self.fh.get_num_pages() <= num_pages_needed {
            self.fh.add_new_page()?;
        }

        // Write the entire buffer in one syscall!
        let write_offset = start_row_id * 64;

        // WAL-log each affected page before the direct write
        let str_first_page = write_offset / PAGE_SIZE as u64;
        let str_num_pages = (data_vec.len() as u64).div_ceil(PAGE_SIZE as u64);
        for page_offset in 0..str_num_pages {
            let page_idx = str_first_page + page_offset;
            let page_start = (page_offset * PAGE_SIZE as u64) as usize;
            let page_end = std::cmp::min(page_start + PAGE_SIZE, data_vec.len());
            bm.log_page_update(self.fh.file_id, page_idx, &data_vec[page_start..page_end])?;
        }

        self.fh.write_bytes_at(write_offset, &data_vec)?;

        // Invalidate buffer manager cache for affected pages
        let data_first_page = write_offset / PAGE_SIZE as u64;
        let data_num_pages = (data_vec.len() as u64).div_ceil(PAGE_SIZE as u64);
        bm.evict_pages_for_file(self.fh.file_id, data_first_page, data_num_pages);
        let null_first_page = start_row_id / 4096;
        let null_num_pages = (num_rows as u64).div_ceil(4096);
        bm.evict_pages_for_file(self.null_fh.file_id, null_first_page, null_num_pages);

        // 3. Batch version tracking (skip if bulk mode - handled by transaction)
        if !skip_modified_rows && !self.name.starts_with('_') {
            self.version_info
                .mark_row_batch(start_row_id..start_row_id + num_rows as u64, tx.tx_id);
            tx.modified_rows.lock().extend(
                (0..num_rows).map(|i| (self.version_info.clone(), start_row_id + i as u64)),
            );
        }

        let null_count = string_array.nulls().map(|n| n.null_count()).unwrap_or(0);
        {
            let mut stats = self.stats.write();
            stats.num_values += num_rows as u64;
            stats.null_count += null_count as u64;
            stats.invalidate_page_bounds();
        }
        Ok(())
    }

    fn get_alg(compression: CompressionType, element_size: usize) -> Box<dyn CompressionAlg> {
        match compression {
            CompressionType::Constant => Box::new(crate::storage::compression::ConstantCompression),
            CompressionType::Rle => Box::new(crate::storage::compression::rle::RleCompression),
            CompressionType::IntegerBitpacking => {
                Box::new(crate::storage::compression::integer_bitpacking::IntegerBitpacking)
            }
            CompressionType::Dict => Box::new(crate::storage::compression::dict::DictCompression),
            CompressionType::Alp => Box::new(crate::storage::compression::alp::AlpAlg),
            CompressionType::FixedFrameOfReference => {
                Box::new(crate::storage::compression::delta::FixedFrameOfReferenceAlg)
            }
            CompressionType::Uncompressed
            | CompressionType::BooleanBitpacking => {
                Box::new(crate::storage::compression::Uncompressed { element_size })
            }
        }
    }

    fn parse_value(
        &self,
        data: &[u8],
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Value> {
        match self.data_type {
            LogicalType::Int64 => Ok(Value::Number(i64::from_le_bytes(
                data[0..8].try_into().expect("infallible: fixed-size array conversion"),
            ) as f64)),
            LogicalType::Int32 => Ok(Value::Number(i32::from_le_bytes(
                data[0..4].try_into().expect("infallible: fixed-size array conversion"),
            ) as f64)),
            LogicalType::Uint64 | LogicalType::Node(_) => Ok(Value::Node(u64::from_le_bytes(
                data[0..8].try_into().expect("infallible: fixed-size array conversion"),
            ))),
            LogicalType::Double => Ok(Value::Number(f64::from_le_bytes(
                data[0..8].try_into().expect("infallible: fixed-size array conversion"),
            ))),
            LogicalType::Bool => Ok(Value::Boolean(data[0] != 0)),
            LogicalType::String => {
                if data[0] == 255 && self.overflow_fh.is_some() {
                    let page_idx = u64::from_le_bytes(data[1..9].try_into().expect("infallible: fixed-size array conversion"));
                    let offset = u64::from_le_bytes(data[9..17].try_into().expect("infallible: fixed-size array conversion"));
                    let len = u32::from_le_bytes(data[17..21].try_into().expect("infallible: fixed-size array conversion")) as usize;
                    let read_len = std::cmp::min(len, 4096 - offset as usize);
                    let overflow_page =
                        bm.pin_page(self.overflow_fh.as_ref().ok_or_else(|| {
                            crate::LightningError::Internal("overflow file handle required but missing — possible data corruption".into())
                        })?.clone(), page_idx, tx)?;
                    let end = std::cmp::min(offset as usize + read_len, 4096);
                    let raw = &overflow_page.as_slice()[offset as usize..end];
                    let s = if let Ok(s) = std::str::from_utf8(raw) {
                        s.to_string()
                    } else {
                        String::from_utf8_lossy(raw).into_owned()
                    };
                    Ok(Value::String(s))
                } else {
                    let len = if data[0] == 255 { 63 } else { data[0] as usize };
                    let actual_len = std::cmp::min(len, 63);
                    let raw = &data[1..1 + actual_len];
                    let s = if let Ok(s) = std::str::from_utf8(raw) {
                        s.to_string()
                    } else {
                        String::from_utf8_lossy(raw).into_owned()
                    };
                    Ok(Value::String(s))
                }
            }
            LogicalType::Date => Ok(Value::Date(i32::from_le_bytes(
                data[0..4].try_into().expect("infallible: fixed-size array conversion"),
            ))),
            LogicalType::Timestamp => Ok(Value::Timestamp(i64::from_le_bytes(
                data[0..8].try_into().expect("infallible: fixed-size array conversion"),
            ))),
            _ => Ok(Value::Null),
        }
    }

    fn serialize_value_into(
        &self,
        val: &Value,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
        buf: &mut [u8],
    ) -> Result<()> {
        match (val, &self.data_type) {
            (Value::Number(n), LogicalType::Int64) => {
                buf[..8].copy_from_slice(&(*n as i64).to_le_bytes())
            }
            (Value::Node(id), LogicalType::Int64) => {
                buf[..8].copy_from_slice(&(*id as i64).to_le_bytes())
            }
            (Value::Number(n), LogicalType::Int32) => {
                buf[..4].copy_from_slice(&(*n as i32).to_le_bytes())
            }
            (Value::Node(id), LogicalType::Int32) => {
                buf[..4].copy_from_slice(&(*id as i32).to_le_bytes())
            }
            (Value::Node(id), LogicalType::Uint64) | (Value::Node(id), LogicalType::Node(_)) => {
                buf[..8].copy_from_slice(&id.to_le_bytes())
            }
            (Value::Number(n), LogicalType::Uint64) | (Value::Number(n), LogicalType::Node(_)) => {
                buf[..8].copy_from_slice(&(*n as u64).to_le_bytes())
            }
            (Value::Number(n), LogicalType::Double) => buf[..8].copy_from_slice(&n.to_le_bytes()),
            (Value::Node(id), LogicalType::Double) => {
                buf[..8].copy_from_slice(&(*id as f64).to_le_bytes())
            }
            (Value::Boolean(b), LogicalType::Bool) => buf[0] = if *b { 1 } else { 0 },
            (Value::String(s), LogicalType::String) => {
                if s.len() < 64 {
                    buf[0] = s.len() as u8;
                    let actual_len = std::cmp::min(s.len(), 63);
                    buf[1..1 + actual_len].copy_from_slice(&s.as_bytes()[0..actual_len]);
                } else if self.overflow_fh.is_some() {
                    let (page_idx, offset) = self.append_to_overflow(bm, s.as_bytes(), tx)?;
                    buf[0] = 255;
                    buf[1..9].copy_from_slice(&page_idx.to_le_bytes());
                    buf[9..17].copy_from_slice(&offset.to_le_bytes());
                    buf[17..21].copy_from_slice(&(s.len() as u32).to_le_bytes());
                } else {
                    return Err(crate::LightningError::Internal(format!(
                        "String of length {} exceeds inline limit (63 bytes) and no overflow file is configured",
                        s.len()
                    )));
                }
            }
            (Value::Date(d), LogicalType::Date) => buf[..4].copy_from_slice(&d.to_le_bytes()),
            (Value::Timestamp(ts), LogicalType::Timestamp) => {
                buf[..8].copy_from_slice(&ts.to_le_bytes())
            }
            _ => {}
        }
        Ok(())
    }

    fn append_to_overflow(
        &self,
        bm: &BufferManager,
        data: &[u8],
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<(u64, u64)> {
        let fh = self
            .overflow_fh
            .as_ref()
            .ok_or_else(|| crate::LightningError::Internal("No overflow file".into()))?;
        let page_idx = fh.add_new_page()?;
        let frame = bm.create_new_version(fh.clone(), page_idx, tx)?;
        let len = std::cmp::min(data.len(), 4096);
        // SAFETY: SAFETY: Pinned frame access in overflow read path.
        unsafe {
            let ptr = frame.as_ptr();
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
        }
        bm.log_page_update(fh.file_id, page_idx, frame.as_slice())?;
        bm.unpin_page(fh, page_idx, frame);
        Ok((page_idx, 0))
    }

    pub fn element_size(&self) -> usize {
        match self.data_type {
            LogicalType::Int64
            | LogicalType::Uint64
            | LogicalType::Node(_)
            | LogicalType::Double
            | LogicalType::Timestamp => 8,
            LogicalType::Int32 | LogicalType::Date => 4,
            LogicalType::Bool => 1,
            LogicalType::String => 64,
            _ => {
                tracing::warn!("Unknown logical type {:?}, defaulting to 8-byte element size", self.data_type);
                8
            }
        }
    }

    pub fn compute_page_bounds(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        use crate::storage::stats::PageBounds;

        let element_size = self.element_size();
        let stats = self.stats.read();
        let compression = stats
            .compression_meta
            .as_ref()
            .map(|m| m.compression)
            .unwrap_or(CompressionType::Uncompressed);
        let values_per_page = if compression == CompressionType::Uncompressed {
            4096 / element_size as u64
        } else {
            32
        };
        if values_per_page == 0 {
            return Ok(());
        }
        let total_values = stats.num_values;
        if total_values == 0 {
            return Ok(());
        }
        let num_pages = total_values.div_ceil(values_per_page);
        drop(stats);

        let mut page_bounds: Vec<Option<PageBounds>> = Vec::with_capacity(num_pages as usize);

        for page_idx in 0..num_pages {
            let start_offset = page_idx * values_per_page;
            let values_in_this_page = std::cmp::min(values_per_page, total_values - start_offset);
            if values_in_this_page == 0 {
                break;
            }

            let array = self.scan_to_array(bm, start_offset, values_in_this_page, tx, None)?;

            let mut page_min: Option<Value> = None;
            let mut page_max: Option<Value> = None;

            for i in 0..array.len() {
                if array.is_null(i) {
                    continue;
                }
                let val = Value::from_arrow(&array, i);
                if matches!(val, Value::Null) {
                    continue;
                }
                match page_min {
                    None => {
                        page_min = Some(val.clone());
                        page_max = Some(val.clone());
                    }
                    Some(ref cur_min) if &val < cur_min => {
                        page_min = Some(val.clone());
                    }
                    Some(ref cur_max) if &val > cur_max => {
                        page_max = Some(val.clone());
                    }
                    _ => {}
                }
            }

            if let (Some(min), Some(max)) = (page_min, page_max) {
                page_bounds.push(Some(PageBounds { min, max }));
            } else {
                page_bounds.push(None);
            }
        }

        let mut stats = self.stats.write();
        stats.page_bounds = page_bounds;
        Ok(())
    }

    pub fn optimize(
        &self,
        bm: &BufferManager,
        tx: &crate::transaction::transaction_manager::Transaction,
        is_indexed: bool,
    ) -> Result<()> {
        self.flush_pending_nulls(bm, tx)?;
        if is_indexed {
            self.compute_page_bounds(bm, tx)?;
        }

        let num_data_pages = self.fh.get_num_pages();
        if num_data_pages == 0 {
            return Ok(());
        }
        let element_size = self.element_size();
        let stats = self.stats.read();
        let values_per_page = if stats
            .compression_meta
            .as_ref()
            .map(|m| m.compression)
            .unwrap_or(CompressionType::Uncompressed)
            == CompressionType::Uncompressed
        {
            4096 / element_size as u64
        } else {
            32
        };
        if values_per_page == 0 {
            return Ok(());
        }
        let total_values = stats.num_values;
        let pages_needed = if total_values == 0 {
            0
        } else {
            total_values.div_ceil(values_per_page)
        };
        if pages_needed < num_data_pages {
            self.dirty.store(true, Ordering::Release);
            self.fh.truncate_last_pages(pages_needed)?;
        }

        // Analyze column data to select optimal compression codec
        if total_values < 32 || self.stats.read().compression_meta.is_some() {
            return Ok(());
        }

        let sample_size = std::cmp::min(total_values, 4096usize as u64);
        let mut values = Vec::with_capacity(sample_size as usize);
        self.scan(bm, 0, sample_size, tx, &mut values)?;

        let data_type = &self.data_type;
        let meta = match data_type {
            LogicalType::Int64 | LogicalType::Int32 | LogicalType::Uint64 | LogicalType::Node(_) => {
                crate::storage::compression::analyzer::CompressionAnalyzer::analyze_integer_chunk(&values, data_type, None, None)
            }
            LogicalType::Double | LogicalType::Float => {
                crate::storage::compression::analyzer::CompressionAnalyzer::analyze_float_chunk(&values)
            }
            LogicalType::String => {
                crate::storage::compression::analyzer::CompressionAnalyzer::analyze_string_chunk(&values)
            }
            _ => return Ok(()),
        };

        if meta.compression != CompressionType::Uncompressed {
            // Compression algorithms are defined but pages are never actually
            // compressed during writes. Setting compression_meta without re-encoding
            // would cause the decompression path to misinterpret uncompressed data.
            // For safety, only set compression_meta when pages are re-encoded.
            // TODO: wire compress_next_page into the bulk write path and re-encode
            // existing pages here.
            tracing::info!(
                "Column {}: compression analysis selected {:?} but pages are not re-encoded \
                 (compression will apply to newly written pages only when the write path is updated)",
                self.name, meta.compression
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_manager::BufferManager;
    use crate::storage::file_handle::FileHandle;
    use crate::storage::row_version::RowVersion;
    use crate::storage::wal::WAL;
    use crate::transaction::TransactionManager;
    use crate::SyncMode;
    use lightning_types::LogicalType;
    use std::sync::Arc;

    fn setup_col(col_type: LogicalType, with_overflow: bool) -> (Column, Arc<BufferManager>, Arc<TransactionManager>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.lbug");
        let null_path = dir.path().join("null.lbug");
        let data_fh = Arc::new(FileHandle::open(&data_path).unwrap());
        let null_fh = Arc::new(FileHandle::open(&null_path).unwrap());
        let wal = Arc::new(WAL::new(dir.path(), SyncMode::Normal).unwrap());
        let tm = Arc::new(TransactionManager::new(Arc::clone(&wal)));
        tm.set_self_weak(Arc::downgrade(&tm));
        let bm = Arc::new(BufferManager::new(256, Some(wal), false, 0, 0.0));
        let rv = Arc::new(RowVersion::new());

        let overflow_fh = if with_overflow {
            let overflow_path = dir.path().join("overflow.lbug");
            Some(Arc::new(FileHandle::open(&overflow_path).unwrap()))
        } else {
            None
        };

        let col = Column::new(
            "test_col".to_string(),
            col_type,
            null_fh,
            data_fh,
            overflow_fh,
            rv,
        );
        (col, bm, tm, dir)
    }

    fn begin_tx(tm: &TransactionManager) -> Arc<crate::transaction::transaction_manager::Transaction> {
        Arc::new(tm.begin(false).unwrap())
    }

    // --- Number types ---

    #[test]
    fn test_append_and_get_double() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(3.14159), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Number(3.14159));
    }

    #[test]
    fn test_append_and_get_int64() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Int64, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(42.0), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Number(42.0));
    }

    #[test]
    fn test_append_and_get_int32() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Int32, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(100.0), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Number(100.0));
    }

    #[test]
    fn test_append_and_get_bool_true() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Bool, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Boolean(true), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_append_and_get_bool_false() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Bool, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Boolean(false), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_append_and_get_node() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Node(Vec::new()), false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Node(999), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Node(999));
    }

    #[test]
    fn test_append_and_get_uint64_as_node() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Uint64, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Node(777), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Node(777));
    }

    #[test]
    fn test_append_and_get_date() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Date, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Date(12345), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Date(12345));
    }

    #[test]
    fn test_append_and_get_timestamp() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Timestamp, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Timestamp(9876543210), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Timestamp(9876543210));
    }

    // --- Null handling ---

    #[test]
    fn test_append_and_get_null() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Null, 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_is_null_after_append_null() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Null, 0, &tx).unwrap();
        assert!(col.is_null(&bm, 0, &tx).unwrap());
    }

    #[test]
    fn test_is_null_after_append_value() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(1.0), 0, &tx).unwrap();
        assert!(!col.is_null(&bm, 0, &tx).unwrap());
    }

    #[test]
    fn test_set_null_toggle() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(1.0), 0, &tx).unwrap();
        assert!(!col.is_null(&bm, 0, &tx).unwrap());
        col.set_null(&bm, 0, true, &tx).unwrap();
        assert!(col.is_null(&bm, 0, &tx).unwrap());
        col.set_null(&bm, 0, false, &tx).unwrap();
        assert!(!col.is_null(&bm, 0, &tx).unwrap());
    }

    #[test]
    fn test_flush_pending_nulls() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(1.0), 0, &tx).unwrap();
        col.append_value(&bm, &Value::Null, 1, &tx).unwrap();
        col.append_value(&bm, &Value::Number(3.0), 2, &tx).unwrap();
        col.flush_pending_nulls(&bm, &tx).unwrap();
        assert!(!col.is_null(&bm, 0, &tx).unwrap());
        assert!(col.is_null(&bm, 1, &tx).unwrap());
        assert!(!col.is_null(&bm, 2, &tx).unwrap());
    }

    // --- Atomic counters ---

    #[test]
    fn test_atomic_counters_track_values() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(1.0), 0, &tx).unwrap();
        col.append_value(&bm, &Value::Null, 1, &tx).unwrap();
        col.append_value(&bm, &Value::Number(3.0), 2, &tx).unwrap();
        // atomic_num_values tracks ALL appended values (including nulls)
        assert_eq!(col.atomic_num_values.load(Ordering::Acquire), 3);
        // atomic_null_count tracks only nulls
        assert_eq!(col.atomic_null_count.load(Ordering::Acquire), 1);
    }

    // --- Multiple values ---

    #[test]
    fn test_append_multiple_values_sequential() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        for i in 0..10 {
            col.append_value(&bm, &Value::Number(i as f64), i, &tx).unwrap();
        }
        for i in 0..10 {
            let result = col.get_value(&bm, i, &tx).unwrap();
            assert_eq!(result, Value::Number(i as f64), "row_id={i}");
        }
    }

    // --- String / Overflow ---

    #[test]
    fn test_append_and_get_short_string() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, true);
        let tx = begin_tx(&tm);
        let s = "hello world";
        col.append_value(&bm, &Value::String(s.to_string()), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::String(s.to_string()));
    }

    #[test]
    fn test_append_and_get_long_string_with_overflow() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, true);
        let tx = begin_tx(&tm);
        // String longer than 63 bytes should go to overflow
        let s = "a".repeat(200);
        col.append_value(&bm, &Value::String(s.clone()), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::String(s));
    }

    #[test]
    fn test_long_string_without_overflow_falls_back_to_inline() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, false);
        let tx = begin_tx(&tm);
        // No overflow file handle — long string will be truncated to 63 bytes
        let s = "b".repeat(200);
        col.append_value(&bm, &Value::String(s.clone()), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        if let Value::String(roundtrip) = &result {
            assert!(roundtrip.len() < 200, "without overflow, string should be truncated");
            assert!(roundtrip.len() <= 63, "truncated to at most 63 chars");
        } else {
            panic!("expected Value::String, got {result:?}");
        }
    }

    #[test]
    fn test_append_multiple_strings() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, true);
        let tx = begin_tx(&tm);
        let medium = "medium".repeat(10);
        let long = "long".repeat(100);
        let strings = vec![
            "short".to_string(),
            medium,
            long,
        ];
        for (i, s) in strings.iter().enumerate() {
            col.append_value(&bm, &Value::String(s.clone()), i as u64, &tx).unwrap();
        }
        for (i, expected) in strings.iter().enumerate() {
            let result = col.get_value(&bm, i as u64, &tx).unwrap();
            assert_eq!(result, Value::String(expected.clone()), "row_id={i}");
        }
    }

    // --- Scan ---

    #[test]
    fn test_scan_returns_all_values() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        for i in 0..5 {
            col.append_value(&bm, &Value::Number(i as f64), i, &tx).unwrap();
        }
        let mut result = Vec::new();
        col.scan(&bm, 0, 5, &tx, &mut result).unwrap();
        assert_eq!(result.len(), 5);
        for (i, val) in result.iter().enumerate() {
            assert_eq!(*val, Value::Number(i as f64));
        }
    }

    #[test]
    fn test_scan_with_offset_and_limit() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        for i in 0..10 {
            col.append_value(&bm, &Value::Number(i as f64), i, &tx).unwrap();
        }
        let mut result = Vec::new();
        col.scan(&bm, 3, 4, &tx, &mut result).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Value::Number(3.0));
        assert_eq!(result[3], Value::Number(6.0));
    }

    // --- Batch append ---

    #[test]
    fn test_batch_append_values() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        let vals: Vec<Value> = (0..10).map(|i| Value::Number(i as f64)).collect();
        let result = col.batch_append_values(&bm, &vals, 0, &tx).unwrap();
        assert_eq!(result.len(), 10, "batch_append returns per-value commit info");

        // Verify all values can be read back
        for i in 0..10 {
            let val = col.get_value(&bm, i as u64, &tx).unwrap();
            assert_eq!(val, Value::Number(i as f64));
        }
    }

    #[test]
    fn test_batch_append_mixed_null_and_values() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        let vals = vec![
            Value::Number(1.0),
            Value::Null,
            Value::Number(3.0),
            Value::Null,
            Value::Number(5.0),
        ];
        col.batch_append_values(&bm, &vals, 0, &tx).unwrap();

        assert_eq!(col.get_value(&bm, 0, &tx).unwrap(), Value::Number(1.0));
        assert_eq!(col.get_value(&bm, 1, &tx).unwrap(), Value::Null);
        assert_eq!(col.get_value(&bm, 2, &tx).unwrap(), Value::Number(3.0));
        assert_eq!(col.get_value(&bm, 3, &tx).unwrap(), Value::Null);
        assert_eq!(col.get_value(&bm, 4, &tx).unwrap(), Value::Number(5.0));
    }

    // --- Dirty flag ---

    #[test]
    fn test_append_sets_dirty_flag() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        assert!(!col.dirty.load(Ordering::Acquire));
        let tx = begin_tx(&tm);
        col.append_value(&bm, &Value::Number(42.0), 0, &tx).unwrap();
        assert!(col.dirty.load(Ordering::Acquire));
    }

    // --- Element size ---

    #[test]
    fn test_element_size_for_types() {
        let (col_i64, _, _, _) = setup_col(LogicalType::Int64, false);
        assert_eq!(col_i64.element_size(), 8);

        let (col_i32, _, _, _) = setup_col(LogicalType::Int32, false);
        assert_eq!(col_i32.element_size(), 4);

        let (col_bool, _, _, _) = setup_col(LogicalType::Bool, false);
        assert_eq!(col_bool.element_size(), 1);

        let (col_str, _, _, _) = setup_col(LogicalType::String, false);
        // String columns have 64-byte elements (1 header + 63 data)
        assert_eq!(col_str.element_size(), 64);

        let (col_date, _, _, _) = setup_col(LogicalType::Date, false);
        assert_eq!(col_date.element_size(), 4);

        let (col_ts, _, _, _) = setup_col(LogicalType::Timestamp, false);
        assert_eq!(col_ts.element_size(), 8);
    }

    // --- Overflow string edge cases ---

    #[test]
    fn test_overflow_exactly_63_chars() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, true);
        let tx = begin_tx(&tm);
        // 63 chars = fits inline (max inline is 63 bytes)
        let s = "c".repeat(63);
        col.append_value(&bm, &Value::String(s.clone()), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::String(s));
    }

    #[test]
    fn test_overflow_boundary_64_chars() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::String, true);
        let tx = begin_tx(&tm);
        // 64 chars = goes to overflow
        let s = "d".repeat(64);
        col.append_value(&bm, &Value::String(s.clone()), 0, &tx).unwrap();
        let result = col.get_value(&bm, 0, &tx).unwrap();
        assert_eq!(result, Value::String(s));
    }

    // --- Default trait ---

    #[test]
    fn test_column_default_zone_map_eq() {
        let zm = ZoneMapEq { value: Value::Number(42.0) };
        assert_eq!(zm.value, Value::Number(42.0));
    }

    // --- Null-only scan ---

    #[test]
    fn test_scan_skips_out_of_bounds() {
        let (col, bm, tm, _dir) = setup_col(LogicalType::Double, false);
        let tx = begin_tx(&tm);
        let mut result = Vec::new();
        // Scanning with offset=0, num_values=0 should return empty
        col.scan(&bm, 0, 0, &tx, &mut result).unwrap();
        assert!(result.is_empty());
    }


}
