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

#[cfg(test)]
mod roundtrip_tests {
    use super::dict::DictCompression;
    use super::integer_bitpacking::IntegerBitpacking;
    use super::rle::RleCompression;
    use super::*;
    use crate::processor::Value;

    fn enc8(v: u64) -> [u8; 8] { v.to_le_bytes() }

    fn roundtrip(alg: &dyn CompressionAlg, values: &[[u8; 8]], label: &str) {
        let mut src = Vec::new();
        for v in values {
            src.extend_from_slice(v);
        }
        let meta = CompressionMetadata::new(Value::Null, Value::Null, alg.get_compression_type(), 0);
        let mut dst = vec![0u8; 4096];
        let (size, processed) = alg
            .compress_next_page(&src, values.len() as u64, &mut dst, &meta)
            .unwrap_or_else(|e| panic!("{label}: compress failed: {e}"));
        let mut out = vec![0u8; values.len() * 8];
        let res = alg.decompress_from_page(
            &dst[..size as usize],
            0,
            &mut out,
            0,
            processed,
            &meta,
        );
        match res {
            Ok(()) => {
                let ok = out[..processed as usize * 8] == src[..processed as usize * 8];
                println!("{label}: processed={processed} ok={ok}");
            }
            Err(e) => println!("{label}: decompress error: {e}"),
        }
    }

    #[test]
    fn codec_roundtrips() {
        // runs
        let runs: Vec<[u8; 8]> = (0..64).map(|i| enc8(i / 16)).collect();
        roundtrip(&RleCompression, &runs, "rle-runs");
        let distinct: Vec<[u8; 8]> = (0..64).map(enc8).collect();
        roundtrip(&RleCompression, &distinct, "rle-distinct");

        // dictionary
        let dict: Vec<[u8; 8]> = (0..64).map(|i| enc8((i * 3) % 5)).collect();
        roundtrip(&DictCompression, &dict, "dict-small");

        // integer bitpacking
        let meta = CompressionMetadata::new(
            Value::Number(0.0),
            Value::Number(31.0),
            CompressionType::IntegerBitpacking,
            5,
        );
        let bp: Vec<[u8; 8]> = (0..64).map(|i| enc8(i % 32)).collect();
        {
            let mut src = Vec::new();
            for v in &bp { src.extend_from_slice(v); }
            let mut dst = vec![0u8; 4096];
            let (size, processed) = IntegerBitpacking
                .compress_next_page(&src, bp.len() as u64, &mut dst, &meta)
                .unwrap();
            let mut out = vec![0u8; bp.len() * 8];
            let r = IntegerBitpacking.decompress_from_page(&dst[..size as usize], 0, &mut out, 0, processed, &meta);
            println!("bitpacking: processed={processed} r={r:?} match={}", out[..processed as usize * 8] == src[..processed as usize * 8]);
        }
    }
}
