use crate::processor::Value;
use crate::storage::compression::bitpacking::BitPacker;
use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::Result;

pub struct FixedFrameOfReferenceAlg;

impl CompressionAlg for FixedFrameOfReferenceAlg {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::FixedFrameOfReference
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        let bit_width = metadata.bit_width;
        let num_values = std::cmp::min(num_values_remaining, 32);

        let min = match &metadata.min {
            Value::Node(v) => *v as i64,
            Value::Number(n) => *n as i64,
            _ => 0,
        };

        let mut deltas = [0u64; 32];
        for i in 0..num_values as usize {
            let start = i * 8;
            let mut val_bytes = [0u8; 8];
            val_bytes.copy_from_slice(&src[start..start + 8]);
            let val = i64::from_le_bytes(val_bytes);
            deltas[i] = (val as i128 - min as i128) as u64;
        }

        BitPacker::pack_32(&deltas, bit_width, dst);
        let compressed_size = (32 * bit_width as usize).div_ceil(8);
        Ok((compressed_size as u64, num_values))
    }

    fn decompress_from_page(
        &self,
        src: &[u8],
        src_offset: u64,
        dst: &mut [u8],
        dst_offset: u64,
        num_values: u64,
        metadata: &CompressionMetadata,
    ) -> Result<()> {
        let bit_width = metadata.bit_width;
        let min = match &metadata.min {
            Value::Node(v) => *v as i64,
            Value::Number(n) => *n as i64,
            _ => 0,
        };

        let mut deltas = [0u64; 32];
        // BitPacker always unpacks from the beginning of the block (32 values)
        BitPacker::unpack_32(src, bit_width, &mut deltas);

        for i in 0..num_values as usize {
            let val_idx = (src_offset as usize + i) % 32;
            let val = min + (deltas[val_idx] as i64);
            let dst_start = (dst_offset as usize + i) * 8;
            dst[dst_start..dst_start + 8].copy_from_slice(&val.to_le_bytes());
        }
        Ok(())
    }
}
