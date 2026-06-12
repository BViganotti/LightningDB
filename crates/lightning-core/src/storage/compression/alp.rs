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
    pub const NAN_SENTINEL: i64 = i64::MIN;
    /// Sentinel for +Infinity encoding
    pub const POS_INF_SENTINEL: i64 = i64::MIN + 1;
    /// Sentinel for -Infinity encoding
    pub const NEG_INF_SENTINEL: i64 = i64::MIN + 2;

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
        if !tmp.is_finite() {
            return if tmp.is_sign_positive() || tmp.is_nan() {
                Self::POS_INF_SENTINEL
            } else {
                Self::NEG_INF_SENTINEL
            };
        }
        // Saturating cast to avoid overflow on extreme values
        let rounded = tmp.round();
        if rounded >= (i64::MAX - 1) as f64 {
            return i64::MAX;
        }
        if rounded <= (i64::MIN + 2) as f64 {
            return Self::NEG_INF_SENTINEL;
        }
        rounded as i64
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
use crate::LightningError;
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
        let required_src = num_values as usize * 8;
        if src.len() < required_src {
            return Err(LightningError::Internal(format!(
                "ALP compress: src too short, need {required_src} bytes"
            )));
        }

        let mut best_shared_exp = 0u8;
        let mut best_shared_fac = 0u8;
        let mut best_cost = u64::MAX;

        for exp_idx in 0..=10u8 {
            for fac_idx in 0..19u8 {
                let mut cost: u64 = 0;
                for i in 0..num_values as usize {
                    let val = f64::from_le_bytes(src[i * 8..i * 8 + 8].try_into().expect("infallible: fixed-size array conversion"));
                    let encoded = Alp::encode_value(val, fac_idx, exp_idx);
                    cost = cost.saturating_add(encoded.unsigned_abs());
                }
                if cost < best_cost {
                    best_cost = cost;
                    best_shared_exp = exp_idx;
                    best_shared_fac = fac_idx;
                }
            }
        }

        let required_dst = 2 + num_values as usize * 8;
        if dst.len() < required_dst {
            return Err(LightningError::Internal(format!(
                "ALP compress: dst too short, need {required_dst} bytes"
            )));
        }
        dst[0] = best_shared_exp;
        dst[1] = best_shared_fac;
        for i in 0..num_values as usize {
            let val = f64::from_le_bytes(src[i * 8..i * 8 + 8].try_into().expect("infallible: fixed-size array conversion"));
            let encoded = Alp::encode_value(val, best_shared_fac, best_shared_exp);
            let dst_start = 2 + i * 8;
            dst[dst_start..dst_start + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        Ok(((2 + num_values * 8) as u64, num_values))
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
        if src.len() < 2 {
            return Err(LightningError::Internal("ALP decompress: src too short for header".into()));
        }
        let exp_idx = src[0];
        let fac_idx = src[1];
        for i in 0..num_values as usize {
            let start = 2 + (src_offset as usize + i) * 8;
            if start + 8 > src.len() {
                return Err(LightningError::Internal(format!(
                    "ALP decompress: src too short at offset {start}"
                )));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&src[start..start + 8]);
            let encoded = i64::from_le_bytes(bytes);
            let decoded = Alp::decode_value(encoded, fac_idx, exp_idx);
            let dst_start = (dst_offset as usize + i) * 8;
            if dst_start + 8 > dst.len() {
                return Err(LightningError::Internal(format!(
                    "ALP decompress: dst too short at offset {dst_start}"
                )));
            }
            dst[dst_start..dst_start + 8].copy_from_slice(&decoded.to_le_bytes());
        }
        Ok(())
    }
}
