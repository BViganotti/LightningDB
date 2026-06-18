use crate::storage::compression::bitpacking::BitPacker;
use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::LightningError;
use crate::Result;
use std::collections::HashMap;

pub struct DictCompression;

impl CompressionAlg for DictCompression {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::Dict
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        _metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        let element_size = 8;
        let num_values = std::cmp::min(num_values_remaining, 32);
        if num_values == 0 {
            return Ok((0, 0));
        }

        let mut dict = Vec::new();
        let mut dict_map = HashMap::new();
        let mut indices = vec![0u64; num_values as usize];

        for (i, idx) in indices.iter_mut().enumerate() {
            let start = i * element_size;
            let val = &src[start..start + element_size];
            if !dict_map.contains_key(val) {
                dict_map.insert(val, dict.len() as u64);
                dict.push(val);
            }
            *idx = *dict_map.get(val).expect("internal invariant violated");
        }

        // Write dict count (4 bytes)
        let dict_count = dict.len() as u32;
        if dst.len() < 4 + dict.len() * element_size + 32 {
            return Err(crate::LightningError::Internal(format!(
                "Dict compress: dst buffer too small ({} bytes needed, {} available)",
                4 + dict.len() * element_size + 32, dst.len()
            )));
        }

        dst[0..4].copy_from_slice(&dict_count.to_le_bytes());
        let mut offset = 4;

        // Write dict values
        for val in dict {
            dst[offset..offset + element_size].copy_from_slice(val);
            offset += element_size;
        }

        // Bit-pack the indices
        let bit_width = 64 - (dict_count as u64).leading_zeros();
        let bit_width = std::cmp::max(bit_width, 1) as u8;

        let mut packed_indices = [0u64; 32];
        packed_indices[0..num_values as usize].copy_from_slice(&indices);
        BitPacker::pack_32(&packed_indices, bit_width, &mut dst[offset..]);

        offset += (32 * bit_width as usize).div_ceil(8);

        Ok((offset as u64, num_values))
    }

    fn decompress_from_page(
        &self,
        src: &[u8],
        src_offset: u64,
        dst: &mut [u8],
        dst_offset: u64,
        num_values: u64,
        _metadata: &CompressionMetadata,
    ) -> Result<()> {
        let element_size = 8;

        if num_values == 0 {
            return Ok(());
        }

        let dict_count = {
            if src.len() < 4 {
                return Err(LightningError::Internal("Dict decompress: src too short for header".into()));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&src[..4]);
            u32::from_le_bytes(bytes) as usize
        };

        if dict_count == 0 {
            return Ok(());
        }

        let dict_start = 4;
        let dict_end = dict_start + dict_count * element_size;
        let packed_start = dict_end;

        if dict_end > src.len() {
            return Err(LightningError::Internal(format!(
                "Dict decompress: dict entries exceed src length (dict_end={}, src_len={})",
                dict_end, src.len()
            )));
        }

        let bit_width = std::cmp::max(64 - (dict_count as u64).leading_zeros(), 1) as u8;

        let mut indices = [0u64; 32];
        BitPacker::unpack_32(&src[packed_start..], bit_width, &mut indices);

        let dict_bytes = &src[dict_start..dict_end];

        for i in 0..num_values as usize {
            let val_idx = ((src_offset as usize) + i) % 32;
            let dict_idx = indices[val_idx] as usize;

            if dict_idx >= dict_count {
                return Err(LightningError::Internal(format!(
                    "Dict decompression: index {dict_idx} out of bounds (dict_count={dict_count})"
                )));
            }

            let entry_start = dict_idx * element_size;
            let dst_start = ((dst_offset as usize) + i) * element_size;
            dst[dst_start..dst_start + element_size]
                .copy_from_slice(&dict_bytes[entry_start..entry_start + element_size]);
        }

        Ok(())
    }
}
