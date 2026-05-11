use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::Result;

pub struct RleCompression;

impl CompressionAlg for RleCompression {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::Rle
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        _metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        let element_size = 8;
        let mut values_processed = 0;
        let mut dst_offset = 0;

        while values_processed < num_values_remaining && dst_offset + element_size + 4 <= dst.len()
        {
            let start = values_processed as usize * element_size;
            let val = &src[start..start + element_size];

            let mut count = 1u32;
            let mut j = values_processed + 1;
            while j < num_values_remaining
                && &src[j as usize * element_size..(j as usize + 1) * element_size] == val
            {
                count += 1;
                j += 1;
            }

            // Check if we can fit this run
            if dst_offset + element_size + 4 > dst.len() {
                break;
            }

            dst[dst_offset..dst_offset + element_size].copy_from_slice(val);
            dst[dst_offset + element_size..dst_offset + element_size + 4]
                .copy_from_slice(&count.to_le_bytes());

            dst_offset += element_size + 4;
            values_processed = j;
        }

        Ok((dst_offset as u64, values_processed))
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
        let mut src_ptr = src_offset as usize;
        let mut dst_ptr = dst_offset as usize;
        let mut values_emitted = 0;

        while values_emitted < num_values && src_ptr + element_size + 4 <= src.len() {
            let val = &src[src_ptr..src_ptr + element_size];
            let mut count_bytes = [0u8; 4];
            count_bytes.copy_from_slice(&src[src_ptr + element_size..src_ptr + element_size + 4]);
            let count = u32::from_le_bytes(count_bytes);

            let to_emit = std::cmp::min(count as u64, num_values - values_emitted);
            for _ in 0..to_emit {
                let start = (dst_ptr * element_size) as usize;
                dst[start..start + element_size].copy_from_slice(val);
                dst_ptr += 1;
            }

            values_emitted += to_emit;
            src_ptr += element_size + 4;

            // If the run didn't finish emitting everything (shouldn't happen in single page case),
            // or we reached the end of src, we stop.
        }
        Ok(())
    }
}
