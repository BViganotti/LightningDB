use crate::storage::compression::bitpacking::BitPacker;
use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::Result;

pub struct IntegerBitpacking;

impl CompressionAlg for IntegerBitpacking {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::IntegerBitpacking
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        let bit_width = metadata.bit_width;
        if bit_width == 0 {
            return Ok((0, num_values_remaining));
        }

        // We process in chunks of 32 values
        let num_values_to_compress = std::cmp::min(num_values_remaining, 32);

        let mut values = [0u64; 32];
        let element_size = 8;
        for i in 0..num_values_to_compress as usize {
            let start = i * element_size;
            let mut val_bytes = [0u8; 8];
            val_bytes.copy_from_slice(&src[start..start + 8]);
            values[i] = u64::from_le_bytes(val_bytes);
        }

        BitPacker::pack_32(&values, bit_width, dst);

        let compressed_size = (32 * bit_width as usize + 7) / 8;
        Ok((compressed_size as u64, num_values_to_compress))
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
        if bit_width == 0 {
            // Constant 0
            let element_size = 8;
            for i in 0..num_values as usize {
                let start = (dst_offset as usize + i) * element_size;
                dst[start..start + 8].copy_from_slice(&[0u8; 8]);
            }
            return Ok(());
        }

        let mut values = [0u64; 32];
        // BitPacker always unpacks from the beginning of the block (32 values)
        // A page currently stores exactly one 32-value block in this implementation.
        BitPacker::unpack_32(src, bit_width, &mut values);

        let element_size = 8;
        for i in 0..num_values as usize {
            let val_idx = (src_offset as usize + i) % 32;
            let start = (dst_offset as usize + i) * element_size;
            let val_bytes = values[val_idx].to_le_bytes();
            dst[start..start + 8].copy_from_slice(&val_bytes);
        }
        Ok(())
    }
}
