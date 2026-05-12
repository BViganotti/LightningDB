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
use std::sync::Arc;

use crate::processor::Value;

pub struct Column {
    pub name: String,
    pub data_type: LogicalType,
    pub fh: Arc<FileHandle>,
    pub null_fh: Arc<FileHandle>,
    pub overflow_fh: Option<Arc<FileHandle>>,
    pub stats: Arc<RwLock<ColumnStats>>,
    pub version_info: Arc<RowVersion>,
    pub child_columns: Vec<Column>,
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
        let is_val_null = matches!(val, Value::Null);
        self.set_null(bm, row_id, is_val_null, tx)?;
        if is_val_null {
            return Ok(());
        }
        match &self.data_type {
            LogicalType::List(_) => {
                if let Some(elements) = val.as_list() {
                    let child = &self.child_columns[0];
                    for el in elements {
                        child.append_value(bm, el, child.stats.read().num_values, tx)?;
                    }
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
            &frame.data[offset_in_page..offset_in_page + element_size],
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
        let page_idx = row_id / 4096;
        let offset = (row_id % 4096) as usize;
        let frame = bm.pin_page(Arc::clone(&self.null_fh), page_idx, tx)?;
        let is_null = frame.data[offset] != 0;
        bm.unpin_page(&self.null_fh, page_idx, frame);
        Ok(is_null)
    }

    pub fn batch_append_values(
        &self,
        bm: &BufferManager,
        vals: &[Value],
        start_row_id: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let num_rows = vals.len();
        if num_rows == 0 {
            return Ok(());
        }

        // 1. Write null bitmap in batches
        let mut i = 0;
        while i < num_rows {
            let page_idx = (start_row_id + i as u64) / 4096;
            while (self.null_fh.get_num_pages() as u64) <= page_idx {
                self.null_fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;

            let mut page_i = i;
            unsafe {
                let ptr = frame.data.as_ptr() as *mut u8;
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
            bm.log_page_update(self.null_fh.file_id, page_idx, &frame.data)?;
            bm.unpin_page(&self.null_fh, page_idx, frame);
            i = page_i;
        }

        let element_size = self.element_size();
        let values_per_page = 4096 / element_size as u64;

        let mut i = 0;
        let mut stats = self.stats.write();
        let mut modified_rows_batch = Vec::with_capacity(std::cmp::min(num_rows, 1024));

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
            while (self.fh.get_num_pages() as u64) <= page_idx {
                self.fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

            let mut page_i = i;
            let mut stack_buf = [0u8; 64];

            unsafe {
                let data_ptr = frame.data.as_ptr() as *mut u8;
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
                    }

                    if !self.name.starts_with('_') {
                        let _ = self
                            .version_info
                            .mark_row(current_row, tx.tx_id, tx.read_ts);
                        modified_rows_batch.push((self.version_info.clone(), current_row));
                        if modified_rows_batch.len() >= 1024 {
                            tx.modified_rows
                                .lock()
                                .extend(modified_rows_batch.drain(..));
                        }
                    }

                    page_i += 1;
                }
            }
            bm.log_page_update(self.fh.file_id, page_idx, &frame.data)?;
            bm.unpin_page(&self.fh, page_idx, frame);
            i = page_i;
        }

        if !modified_rows_batch.is_empty() {
            tx.modified_rows.lock().extend(modified_rows_batch);
        }

        Ok(())
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
                result.push(self.parse_value(&frame.data[start..start + element_size], bm, tx)?);
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
    ) -> Result<ArrayRef> {
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
                return self.scan_string_vectorized(bm, offset, num_values, tx);
            }
            return self.scan_to_array_vectorized(
                bm,
                offset,
                num_values,
                tx,
                element_size,
                &target_type,
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

        while values_read < num_values {
            let current_offset = offset + values_read;
            let page_idx = current_offset / values_per_page;
            let offset_in_page = (current_offset % values_per_page) as usize;
            let to_read = std::cmp::min(num_values - values_read, 32);
            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            alg.decompress_from_page(
                &page.data,
                offset_in_page as u64,
                &mut temp_block,
                0,
                to_read,
                &meta,
            )?;

            let null_page_idx = current_offset / 4096;
            let null_frame = bm.pin_page(null_fh.clone(), null_page_idx, tx)?;
            let null_base_offset = (current_offset % 4096) as usize;

            for i in 0..to_read as usize {
                let is_null = null_frame.data[null_base_offset + i] != 0;
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
                        &self.data_type,
                    )?;
                }
            }
            bm.unpin_page(&null_fh, null_page_idx, null_frame);
            bm.unpin_page(&self.fh, page_idx, page);
            values_read += to_read;
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

    fn scan_string_vectorized(
        &self,
        bm: &BufferManager,
        offset: u64,
        num_values: u64,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<ArrayRef> {
        use arrow::array::StringBuilder;
        let values_per_page = 4096 / 64u64;

        // Check if we can use direct file reads (no uncommitted modifications in range)
        let can_direct_read = !self.version_info.has_modifications();

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

            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            let base_offset = offset_in_page * 64;

            let null_page_idx = current_offset / 4096;
            let null_frame = &null_frames
                .iter()
                .find(|(idx, _)| *idx == null_page_idx)
                .unwrap()
                .1;
            let null_base_offset = (current_offset % 4096) as usize;

            for i in 0..to_read {
                let is_null = null_frame.data[null_base_offset + i] != 0;
                if is_null {
                    builder.append_null();
                } else {
                    let slot_offset = base_offset + i * 64;
                    let marker = page.data[slot_offset];
                    let s = if marker == 255 {
                        // Overflow string: read from overflow file via parse_value
                        // (which handles buffer manager pinning internally)
                        match self.parse_value(
                            &page.data[slot_offset..slot_offset + 64],
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
                            &page.data[slot_offset + 1..slot_offset + 1 + actual_len],
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

        let mut data_buf = Vec::with_capacity((num_pages as usize) * 4096);
        unsafe {
            data_buf.set_len((num_pages as usize) * 4096);
        }
        self.fh.read_pages(first_page, num_pages, &mut data_buf)?;

        // Batch read null pages
        let null_first_page = offset / 4096;
        let null_last_page = (offset + num_values - 1) / 4096;
        let num_null_pages = null_last_page - null_first_page + 1;
        let mut null_data = Vec::with_capacity((num_null_pages as usize) * 4096);
        unsafe {
            null_data.set_len((num_null_pages as usize) * 4096);
        }
        self.null_fh
            .read_pages(null_first_page, num_null_pages, &mut null_data)?;

        // Build Arrow buffers directly
        let mut offsets = Vec::with_capacity(num_values as usize + 1);
        let mut values = Vec::with_capacity(num_values as usize * 16);
        let mut current_offset = 0i32;
        offsets.push(current_offset);

        let mut null_bits = vec![0xFFu8; (num_values as usize + 7) / 8];
        let mut has_any_nulls = false;

        // Pre-read overflow file if needed — we need it for overflow strings
        let overflow_data: Vec<u8> = if self.overflow_fh.is_some() {
            let ofh = self.overflow_fh.as_ref().unwrap();
            let num_of_pages = ofh.get_num_pages() as usize;
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
            let is_null = null_data[null_idx]; // 0 or 1

            let page_offset =
                ((row_offset / values_per_page as usize) - first_page as usize) * 4096;
            let offset_in_page = (row_offset % values_per_page as usize) * 64;
            let slot_offset = page_offset + offset_in_page;

            if is_null != 0 {
                // Null handling stays the same
                null_bits[i / 8] &= !(is_null << (i % 8));
                has_any_nulls |= is_null != 0;
                continue;
            }

            let marker = data_buf[slot_offset];
            let s_bytes = if marker == 255 && !overflow_data.is_empty() {
                let of_page = u64::from_le_bytes(
                    data_buf[slot_offset + 1..slot_offset + 9].try_into().unwrap(),
                ) as usize;
                let of_offset = u64::from_le_bytes(
                    data_buf[slot_offset + 9..slot_offset + 17].try_into().unwrap(),
                ) as usize;
                let of_len = std::cmp::min(
                    u32::from_le_bytes(
                        data_buf[slot_offset + 17..slot_offset + 21].try_into().unwrap(),
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
    ) -> Result<ArrayRef> {
        let values_per_page = 4096 / element_size as u64;

        // Direct file read for unmodified columns
        if !self.version_info.has_modifications() {
            return self.scan_primitive_direct(offset, num_values, element_size, target_type);
        }

        // FIX #4: Fast path for full page reads - zero copy (shallow copy for now)
        if num_values == values_per_page && offset % values_per_page == 0 {
            let page_idx = offset / values_per_page;
            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;

            let data_buf = Buffer::from(&page.data);

            let null_page_idx = offset / 4096;
            let null_frame = bm.pin_page(Arc::clone(&self.null_fh), null_page_idx, tx)?;
            let null_base_offset = (offset % 4096) as usize;

            let has_nulls = null_frame.data
                [null_base_offset..null_base_offset + num_values as usize]
                .iter()
                .any(|&v| v != 0);

            let null_buf = if has_nulls {
                let mut bits = vec![0xFFu8; (num_values as usize + 7) / 8];
                for i in 0..num_values as usize {
                    if null_frame.data[null_base_offset + i] != 0 {
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
        let mut null_bits = vec![0xFFu8; (num_values as usize + 7) / 8];
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

            let page = bm.pin_page(Arc::clone(&self.fh), page_idx, tx)?;
            let src_start = offset_in_page * element_size;
            let src_end = src_start + to_read * element_size;
            data_buffer.extend_from_slice(&page.data[src_start..src_end]);

            // Optimization: Skip null scan if column has no nulls
            let has_any_nulls_in_stats = self.stats.read().null_count > 0;

            if has_any_nulls_in_stats {
                let null_page_idx = current_offset / 4096;
                let null_frame = bm.pin_page(null_fh.clone(), null_page_idx, tx)?;
                let null_base_offset = (current_offset % 4096) as usize;

                // SIMD-optimized null bit processing with u64
                let null_src = &null_frame.data[null_base_offset..null_base_offset + to_read];
                let mut j = 0;
                while j + 8 <= to_read {
                    let val = u64::from_le_bytes(null_src[j..j + 8].try_into().unwrap());
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
    ) -> Result<ArrayRef> {
        let values_per_page = 4096 / element_size as u64;
        let total_bytes = num_values as usize * element_size;
        let mut has_any_nulls = false;
        let mut output_offset = 0usize;
        let mut values_read = 0u64;

        // Optimization: Skip null scan if column has no nulls
        let has_any_nulls_in_stats = self.stats.read().null_count > 0;
        let mut null_bits = if has_any_nulls_in_stats {
            vec![0xFFu8; (num_values as usize + 7) / 8]
        } else {
            Vec::new() // Avoid allocation if not needed
        };

        // Optimization: When offset is page-aligned, we can use fast bulk-reads
        let is_page_aligned = offset % values_per_page == 0;

        let data_buf = if is_page_aligned {
            // Read all data pages in one syscall
            let first_page = offset / values_per_page;
            let last_page = (offset + num_values - 1) / values_per_page;
            let num_pages = last_page - first_page + 1;
            let expected_bytes = (num_pages as usize) * 4096;

            // Read directly into Vec, bypassing zeroing initialization
            let mut data_vec = Vec::with_capacity(expected_bytes);
            unsafe {
                data_vec.set_len(expected_bytes);
            }
            self.fh.read_pages(first_page, num_pages, &mut data_vec)?;
            // Truncate down to the exact requested data size
            data_vec.truncate(total_bytes);

            if has_any_nulls_in_stats {
                // Read null pages in one syscall
                let null_first_page = offset / 4096;
                let null_last_page = (offset + num_values - 1) / 4096;
                let num_null_pages = null_last_page - null_first_page + 1;
                let mut null_data = Vec::with_capacity((num_null_pages as usize) * 4096);
                unsafe {
                    null_data.set_len((num_null_pages as usize) * 4096);
                }
                self.null_fh
                    .read_pages(null_first_page, num_null_pages, &mut null_data)?;

                // Efficient branchless null bit extraction (8 bytes at a time)
                let null_src = &null_data[..(num_values as usize)];
                let mut chunk_iter = null_src.chunks_exact(8);
                let mut out_idx = 0;
                let mut any_nulls_int = 0u8;

                for chunk in chunk_iter.by_ref() {
                    let mut bitmask = 0u8;
                    bitmask |= chunk[0] << 0;
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
            let mut data_buffer = MutableBuffer::with_capacity(total_bytes);
            let mut page_buf = [0u8; 4096];
            let mut null_buf = [0u8; 4096];

            if !has_any_nulls_in_stats {
                while values_read < num_values {
                    let current_offset = offset + values_read;
                    let page_idx = current_offset / values_per_page;
                    let offset_in_page = (current_offset % values_per_page) as usize;
                    let to_read = std::cmp::min(
                        num_values - values_read,
                        values_per_page - offset_in_page as u64,
                    ) as usize;

                    self.fh.read_page(page_idx, &mut page_buf)?;
                    let src_start = offset_in_page * element_size;
                    let src_end = src_start + to_read * element_size;
                    data_buffer.extend_from_slice(&page_buf[src_start..src_end]);

                    values_read += to_read as u64;
                    output_offset += to_read;
                }
            } else {
                while values_read < num_values {
                    let current_offset = offset + values_read;
                    let page_idx = current_offset / values_per_page;
                    let offset_in_page = (current_offset % values_per_page) as usize;
                    let to_read = std::cmp::min(
                        num_values - values_read,
                        values_per_page - offset_in_page as u64,
                    ) as usize;

                    // Direct file reads
                    self.fh.read_page(page_idx, &mut page_buf)?;
                    let null_page_idx = current_offset / 4096;
                    self.null_fh.read_page(null_page_idx, &mut null_buf)?;

                    let src_start = offset_in_page * element_size;
                    let src_end = src_start + to_read * element_size;
                    data_buffer.extend_from_slice(&page_buf[src_start..src_end]);

                    let null_base_offset = (current_offset % 4096) as usize;
                    let null_src = &null_buf[null_base_offset..null_base_offset + to_read];

                    // Process in 8-byte chunks
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
                let mut packed = vec![0u8; (num_values + 7) / 8];
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
                &page.data,
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
                let is_null = null_frame.data[null_base_offset + i] != 0;
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
        bm: &BufferManager,
        row_id: u64,
        is_null: bool,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        let page_idx = row_id / 4096;
        let offset = (row_id % 4096) as usize;
        while (self.null_fh.get_num_pages() as u64) <= page_idx {
            self.null_fh.add_new_page()?;
        }
        let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;
        unsafe {
            let ptr = frame.data.as_ptr() as *mut u8;
            *ptr.add(offset) = if is_null { 1 } else { 0 };
        }
        bm.log_page_update(self.null_fh.file_id, page_idx, &frame.data)?;
        bm.unpin_page(&self.null_fh, page_idx, frame);
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
        while (self.fh.get_num_pages() as u64) <= page_idx {
            self.fh.add_new_page()?;
        }
        let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

        unsafe {
            let data_ptr = frame.data.as_ptr() as *mut u8;
            let mut stack_buf = [0u8; 64];
            self.serialize_value_into(val, bm, tx, &mut stack_buf)?;
            std::ptr::copy_nonoverlapping(
                stack_buf.as_ptr(),
                data_ptr.add(offset_in_page),
                element_size,
            );
        }

        if !self.name.starts_with('_') {
            let _ = self.version_info.mark_row(row_id, tx.tx_id, tx.read_ts);
            tx.modified_rows
                .lock()
                .push((self.version_info.clone(), row_id));
        }

        bm.log_page_update(self.fh.file_id, page_idx, &frame.data)?;
        bm.unpin_page(&self.fh, page_idx, frame);
        self.stats.write().update(val);
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
                while (self.null_fh.get_num_pages() as u64) <= page_idx {
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
                self.null_fh.write_page(page_idx, &null_page_buf)?;
                i = page_i;
            }
        } else {
            let mut i = 0;
            while i < num_rows {
                let page_idx = (start_row_id + i as u64) / 4096;
                while (self.null_fh.get_num_pages() as u64) <= page_idx {
                    self.null_fh.add_new_page()?;
                }
                let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;

                let mut page_i = i;
                unsafe {
                    let ptr = frame.data.as_ptr() as *mut u8;
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
                bm.log_page_update(self.null_fh.file_id, page_idx, &frame.data)?;
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
                | LogicalType::Bool
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
        let mut modified_rows_batch = Vec::with_capacity(1024);

        while i < num_rows {
            let page_idx = (start_row_id + i as u64) / values_per_page;
            while (self.fh.get_num_pages() as u64) <= page_idx {
                self.fh.add_new_page()?;
            }
            let frame = bm.create_new_version(Arc::clone(&self.fh), page_idx, tx)?;

            let mut page_i = i;
            let mut stack_buf = [0u8; 64];

            unsafe {
                let data_ptr = frame.data.as_ptr() as *mut u8;
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
                    }

                    if !skip_modified_rows && !self.name.starts_with('_') {
                        modified_rows_batch.push((self.version_info.clone(), current_row));
                        if modified_rows_batch.len() >= 1024 {
                            tx.modified_rows
                                .lock()
                                .extend(modified_rows_batch.drain(..));
                        }
                    }

                    page_i += 1;
                }
            }
            bm.log_page_update(self.fh.file_id, page_idx, &frame.data)?;
            bm.unpin_page(&self.fh, page_idx, frame);
            i = page_i;
        }

        if !skip_modified_rows && !modified_rows_batch.is_empty() {
            tx.modified_rows.lock().extend(modified_rows_batch);
        }

        Ok(())
    }

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
        let raw_bytes = buffers[0].as_slice();

        // Ensure file is large enough
        let num_pages_needed = (num_rows as u64 + values_per_page - 1) / values_per_page;
        let first_page = start_row_id / values_per_page;
        while (self.fh.get_num_pages() as u64) <= first_page + num_pages_needed {
            self.fh.add_new_page()?;
        }

        // Write the entire buffer in one syscall!
        let write_offset = start_row_id * element_size as u64;
        let bytes_to_write = num_rows * element_size;
        self.fh
            .write_bytes_at(write_offset, &raw_bytes[..bytes_to_write])?;

        if skip_modified_rows {
            let data_first_page = write_offset / PAGE_SIZE as u64;
            let data_num_pages = (bytes_to_write as u64 + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64;
            bm.evict_pages_for_file(self.fh.file_id, data_first_page, data_num_pages);
        }

        if !skip_modified_rows && !self.name.starts_with('_') {
            self.version_info
                .mark_row_batch(start_row_id..start_row_id + num_rows as u64, tx.tx_id);
            tx.modified_rows.lock().extend(
                (0..num_rows).map(|i| (self.version_info.clone(), start_row_id + i as u64)),
            );
        }

        self.stats.write().num_values += num_rows as u64;
        Ok(())
    }

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
                while (self.null_fh.get_num_pages() as u64) <= page_idx {
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
                while (self.null_fh.get_num_pages() as u64) <= page_idx {
                    self.null_fh.add_new_page()?;
                }
                let frame = bm.create_new_version(Arc::clone(&self.null_fh), page_idx, tx)?;
                let mut page_i = i;
                unsafe {
                    let ptr = frame.data.as_ptr() as *mut u8;
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
                bm.log_page_update(self.null_fh.file_id, page_idx, &frame.data)?;
                bm.unpin_page(&self.null_fh, page_idx, frame);
                i = page_i;
            }
        }

        // 2. Write string data directly to file, bypassing buffer manager
        let mut data_vec = Vec::with_capacity(num_rows * 64);
        data_vec.resize(num_rows * 64, 0u8);

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
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            s_bytes.as_ptr(),
                            frame.data.as_ptr() as *mut u8,
                            copy_len,
                        );
                    }
                    bm.log_page_update(ofh.file_id, page_idx, &frame.data)?;
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
            (start_row_id + num_rows as u64 + values_per_page - 1) / values_per_page;
        while (self.fh.get_num_pages() as u64) <= num_pages_needed {
            self.fh.add_new_page()?;
        }

        // Write the entire buffer in one syscall!
        let write_offset = start_row_id * 64;
        self.fh.write_bytes_at(write_offset, &data_vec)?;

        // Invalidate buffer manager cache for affected pages
        if skip_modified_rows {
            let data_first_page = write_offset / PAGE_SIZE as u64;
            let data_num_pages = (data_vec.len() as u64 + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64;
            bm.evict_pages_for_file(self.fh.file_id, data_first_page, data_num_pages);
            let null_first_page = start_row_id / 4096;
            let null_num_pages = (num_rows as u64 + 4095) / 4096;
            bm.evict_pages_for_file(self.null_fh.file_id, null_first_page, null_num_pages);
        }

        // 3. Batch version tracking (skip if bulk mode - handled by transaction)
        if !skip_modified_rows && !self.name.starts_with('_') {
            self.version_info
                .mark_row_batch(start_row_id..start_row_id + num_rows as u64, tx.tx_id);
            tx.modified_rows.lock().extend(
                (0..num_rows).map(|i| (self.version_info.clone(), start_row_id + i as u64)),
            );
        }

        self.stats.write().num_values += num_rows as u64;
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
            _ => Box::new(crate::storage::compression::Uncompressed { element_size }),
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
                data[0..8].try_into().unwrap(),
            ) as f64)),
            LogicalType::Int32 => Ok(Value::Number(i32::from_le_bytes(
                data[0..4].try_into().unwrap(),
            ) as f64)),
            LogicalType::Uint64 | LogicalType::Node(_) => Ok(Value::Node(u64::from_le_bytes(
                data[0..8].try_into().unwrap(),
            ))),
            LogicalType::Double => Ok(Value::Number(f64::from_le_bytes(
                data[0..8].try_into().unwrap(),
            ))),
            LogicalType::Bool => Ok(Value::Boolean(data[0] != 0)),
            LogicalType::String => {
                if data[0] == 255 && self.overflow_fh.is_some() {
                    let page_idx = u64::from_le_bytes(data[1..9].try_into().unwrap());
                    let offset = u64::from_le_bytes(data[9..17].try_into().unwrap());
                    let len = u32::from_le_bytes(data[17..21].try_into().unwrap()) as usize;
                    let read_len = std::cmp::min(len, 4096 - offset as usize);
                    let overflow_page =
                        bm.pin_page(self.overflow_fh.as_ref().unwrap().clone(), page_idx, tx)?;
                    let end = std::cmp::min(offset as usize + read_len, 4096);
                    Ok(Value::String(
                        String::from_utf8_lossy(
                            &overflow_page.data[offset as usize..end],
                        )
                        .to_string(),
                    ))
                } else {
                    let len = if data[0] == 255 { 63 } else { data[0] as usize };
                    let actual_len = std::cmp::min(len, 63);
                    Ok(Value::String(
                        String::from_utf8_lossy(&data[1..1 + actual_len]).to_string(),
                    ))
                }
            }
            LogicalType::Date => Ok(Value::Date(i32::from_le_bytes(
                data[0..4].try_into().unwrap(),
            ))),
            LogicalType::Timestamp => Ok(Value::Timestamp(i64::from_le_bytes(
                data[0..8].try_into().unwrap(),
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
        match (val, self.data_type.clone()) {
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
                } else if let Some(_) = &self.overflow_fh {
                    let (page_idx, offset) = self.append_to_overflow(bm, s.as_bytes(), tx)?;
                    buf[0] = 255;
                    buf[1..9].copy_from_slice(&page_idx.to_le_bytes());
                    buf[9..17].copy_from_slice(&offset.to_le_bytes());
                    buf[17..21].copy_from_slice(&(s.len() as u32).to_le_bytes());
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
        unsafe {
            let ptr = frame.data.as_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
        }
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
            _ => 8,
        }
    }

    pub fn optimize(
        &self,
        _bm: &BufferManager,
        _tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<()> {
        Ok(())
    }
}
