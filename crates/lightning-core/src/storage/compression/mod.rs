use crate::processor::Value;
use crate::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    Uncompressed = 0,
    IntegerBitpacking = 1,
    BooleanBitpacking = 2,
    Constant = 3,
    Alp = 4,
    FixedFrameOfReference = 5,
    Rle = 6,
    Dict = 7,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionMetadata {
    pub min: Value,
    pub max: Value,
    pub compression: CompressionType,
    pub bit_width: u8, // Used for bit-packing
}

impl CompressionMetadata {
    pub fn new(min: Value, max: Value, compression: CompressionType, bit_width: u8) -> Self {
        Self {
            min,
            max,
            compression,
            bit_width,
        }
    }
}

pub trait CompressionAlg: Send + Sync {
    fn get_compression_type(&self) -> CompressionType;

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)>; // Returns (compressed_size, num_values_processed)

    fn decompress_from_page(
        &self,
        src: &[u8],
        src_offset: u64,
        dst: &mut [u8],
        dst_offset: u64,
        num_values: u64,
        metadata: &CompressionMetadata,
    ) -> Result<()>;
}

pub struct Uncompressed {
    pub element_size: usize,
}

impl CompressionAlg for Uncompressed {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::Uncompressed
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        _metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        let values_to_copy =
            std::cmp::min(num_values_remaining as usize, dst.len() / self.element_size);
        let size_to_copy = values_to_copy * self.element_size;
        if size_to_copy > src.len() {
            return Err(crate::LightningError::Internal(format!(
                "Uncompressed copy: src too short (need {} bytes, have {})",
                size_to_copy, src.len()
            )));
        }
        dst[0..size_to_copy].copy_from_slice(&src[0..size_to_copy]);
        Ok((size_to_copy as u64, values_to_copy as u64))
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
        let src_start = (src_offset as usize) * self.element_size;
        let dst_start = (dst_offset as usize) * self.element_size;
        let size = (num_values as usize) * self.element_size;
        if src_start + size > src.len() {
            return Err(crate::LightningError::Internal(format!(
                "Uncompressed decompress: src too short (need {}..{}, have {})",
                src_start, src_start + size, src.len()
            )));
        }
        if dst_start + size > dst.len() {
            return Err(crate::LightningError::Internal(format!(
                "Uncompressed decompress: dst too short (need {}..{}, have {})",
                dst_start, dst_start + size, dst.len()
            )));
        }
        dst[dst_start..dst_start + size].copy_from_slice(&src[src_start..src_start + size]);
        Ok(())
    }
}

pub struct ConstantCompression;

impl CompressionAlg for ConstantCompression {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::Constant
    }

    fn compress_next_page(
        &self,
        _src: &[u8],
        num_values_remaining: u64,
        _dst: &mut [u8],
        _metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        Ok((0, num_values_remaining)) // Constant stores no data on page, so it "processes" all remaining.
    }

    fn decompress_from_page(
        &self,
        _src: &[u8],
        _src_offset: u64,
        dst: &mut [u8],
        dst_offset: u64,
        num_values: u64,
        metadata: &CompressionMetadata,
    ) -> Result<()> {
        // We need to know the element size to fill the output buffer
        // For now, assume f64/i64 (8 bytes) as that's what we mostly use
        // In a real implementation, we'd use the physical type.
        let val_bytes = metadata.min.to_le_bytes();
        let element_size = val_bytes.len();

        for i in 0..num_values as usize {
            let start = (dst_offset as usize + i) * element_size;
            dst[start..start + element_size].copy_from_slice(&val_bytes);
        }
        Ok(())
    }
}

pub mod alp;
pub mod analyzer;
pub mod bitpacking;
pub mod delta;
pub mod dict;
pub mod integer_bitpacking;
pub mod rle;

#[cfg(test)]
mod alp_test;
#[cfg(test)]
mod analyzer_test;
#[cfg(test)]
mod bitpacking_test;
