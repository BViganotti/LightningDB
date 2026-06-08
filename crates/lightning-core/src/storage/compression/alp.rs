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

    /// Sentinel for NaN encoding: uses i64::MIN (most negative value)
    const NAN_SENTINEL: i64 = i64::MIN;
    /// Sentinel for +Infinity encoding
    const POS_INF_SENTINEL: i64 = i64::MIN + 1;
    /// Sentinel for -Infinity encoding
    const NEG_INF_SENTINEL: i64 = i64::MIN + 2;

    pub fn encode_value(val: f64, fac_idx: u8, exp_idx: u8) -> i64 {
        if val.is_nan() {
            return Self::NAN_SENTINEL;
        }
        if val.is_infinite() {
            return if val.is_sign_positive() {
                Self::POS_INF_SENTINEL
            } else {
                Self::NEG_INF_SENTINEL
            };
        }
        let tmp = val * Self::EXP_ARR[exp_idx as usize] / Self::FACTOR_ARR[fac_idx as usize];
        tmp.round() as i64
    }

    pub fn decode_value(encoded: i64, fac_idx: u8, exp_idx: u8) -> f64 {
        match encoded {
            Self::NAN_SENTINEL => f64::NAN,
            Self::POS_INF_SENTINEL => f64::INFINITY,
            Self::NEG_INF_SENTINEL => f64::NEG_INFINITY,
            _ => (encoded as f64) * Self::FACTOR_ARR[fac_idx as usize] * Self::FRAC_ARR[exp_idx as usize],
        }
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
        let num_values = std::cmp::min(num_values_remaining, 32);
        if num_values == 0 {
            return Ok((0, 0));
        }

        let mut best_shared_exp = 0u8;
        let mut best_cost = u64::MAX;

        for exp_idx in 0..=10u8 {
            let mut cost: u64 = 0;
            for i in 0..num_values as usize {
                let val = f64::from_le_bytes(src[i * 8..i * 8 + 8].try_into().expect("infallible: fixed-size array conversion"));
                let encoded = Alp::encode_value(val, 0, exp_idx);
                cost = cost.saturating_add(encoded.unsigned_abs());
            }
            if cost < best_cost {
                best_cost = cost;
                best_shared_exp = exp_idx;
            }
        }

        dst[0] = best_shared_exp;
        for i in 0..num_values as usize {
            let val = f64::from_le_bytes(src[i * 8..i * 8 + 8].try_into().expect("infallible: fixed-size array conversion"));
            let encoded = Alp::encode_value(val, 0, best_shared_exp);
            let dst_start = 1 + i * 8;
            dst[dst_start..dst_start + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        Ok(((1 + num_values * 8) as u64, num_values))
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
        let exp_idx = src[0];
        for i in 0..num_values as usize {
            let start = 1 + (src_offset as usize + i) * 8;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&src[start..start + 8]);
            let encoded = i64::from_le_bytes(bytes);
            let decoded = Alp::decode_value(encoded, 0, exp_idx);
            let dst_start = (dst_offset as usize + i) * 8;
            dst[dst_start..dst_start + 8].copy_from_slice(&decoded.to_le_bytes());
        }
        Ok(())
    }
}
