pub struct Alp;

impl Alp {
    // Standard factors from ALP
    const FRAC_ARR: [f64; 11] = [
        1.0,
        0.1,
        0.01,
        0.001,
        0.0001,
        0.00001,
        0.000001,
        0.0000001,
        0.00000001,
        0.000000001,
        0.0000000001,
    ];

    const EXP_ARR: [f64; 11] = [
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0,
    ];

    const FACTOR_ARR: [f64; 19] = [
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0,
        100000000000.0,
        1000000000000.0,
        10000000000000.0,
        100000000000000.0,
        1000000000000000.0,
        10000000000000000.0,
        100000000000000000.0,
        1000000000000000000.0,
    ];

    pub fn encode_value(val: f64, fac_idx: u8, exp_idx: u8) -> i64 {
        let tmp = val * Self::EXP_ARR[exp_idx as usize] / Self::FACTOR_ARR[fac_idx as usize];
        tmp.round() as i64
    }

    pub fn decode_value(encoded: i64, fac_idx: u8, exp_idx: u8) -> f64 {
        (encoded as f64) * Self::FACTOR_ARR[fac_idx as usize] * Self::FRAC_ARR[exp_idx as usize]
    }
}

use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};
use crate::Result;

pub struct AlpAlg;

impl CompressionAlg for AlpAlg {
    fn get_compression_type(&self) -> CompressionType {
        CompressionType::Alp
    }

    fn compress_next_page(
        &self,
        src: &[u8],
        num_values_remaining: u64,
        dst: &mut [u8],
        _metadata: &CompressionMetadata,
    ) -> Result<(u64, u64)> {
        // Simple ALP mock: just copy 32 values
        let to_copy = std::cmp::min(num_values_remaining, 32);
        let size = to_copy * 8;
        dst[0..size as usize].copy_from_slice(&src[0..size as usize]);
        Ok((size, to_copy))
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
        // Simple ALP decompression from bit-packed values
        // For now, assume fac=0, exp=0 (uncompressed i64)
        for i in 0..num_values as usize {
            let start = (src_offset as usize + i) * 8;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&src[start..start + 8]);
            let encoded = i64::from_le_bytes(bytes);
            let decoded = Alp::decode_value(encoded, 0, 0);
            let dst_start = (dst_offset as usize + i) * 8;
            dst[dst_start..dst_start + 8].copy_from_slice(&decoded.to_le_bytes());
        }
        Ok(())
    }
}
