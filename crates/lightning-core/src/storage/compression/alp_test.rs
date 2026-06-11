use crate::storage::compression::alp::Alp;

#[test]
fn test_alp_encode_decode() {
    let val = 123.456;
    let encoded = Alp::encode_value(val, 0, 3);
    assert_eq!(encoded, 123456);

    let decoded = Alp::decode_value(encoded, 0, 3);
    assert!((decoded - val).abs() < 1e-10);
}

#[test]
fn test_alp_with_factor() {
    let val = 123456000.0;
    let encoded = Alp::encode_value(val, 3, 0);
    assert_eq!(encoded, 123456);

    let decoded = Alp::decode_value(encoded, 3, 0);
    assert_eq!(decoded, val);
}

#[test]
fn test_alp_nan_encoding() {
    let encoded = Alp::encode_value(f64::NAN, 0, 0);
    assert_eq!(encoded, Alp::NAN_SENTINEL);

    let decoded = Alp::decode_value(Alp::NAN_SENTINEL, 0, 0);
    assert!(decoded.is_nan());
}

#[test]
fn test_alp_infinity_encoding() {
    let encoded_pos = Alp::encode_value(f64::INFINITY, 0, 0);
    assert_eq!(encoded_pos, Alp::POS_INF_SENTINEL);
    let decoded_pos = Alp::decode_value(Alp::POS_INF_SENTINEL, 0, 0);
    assert!(decoded_pos.is_infinite());
    assert!(decoded_pos.is_sign_positive());

    let encoded_neg = Alp::encode_value(f64::NEG_INFINITY, 0, 0);
    assert_eq!(encoded_neg, Alp::NEG_INF_SENTINEL);
    let decoded_neg = Alp::decode_value(Alp::NEG_INF_SENTINEL, 0, 0);
    assert!(decoded_neg.is_infinite());
    assert!(decoded_neg.is_sign_negative());
}

#[test]
fn test_alp_overflow_encoding() {
    // Value that becomes infinity after scaling -> overflows to POS_INF_SENTINEL
    let val = f64::MAX;
    let encoded = Alp::encode_value(val, 0, 1);
    assert_eq!(encoded, Alp::POS_INF_SENTINEL);

    // Value that becomes -infinity after scaling -> overflows to NEG_INF_SENTINEL
    let val = -f64::MAX;
    let encoded = Alp::encode_value(val, 0, 1);
    assert_eq!(encoded, Alp::NEG_INF_SENTINEL);

    // Large finite value clamped to i64::MAX (not sentinel)
    let val = i64::MAX as f64 * 1.5;
    let encoded = Alp::encode_value(val, 0, 0);
    assert_eq!(encoded, i64::MAX);

    // Large negative finite value -> NEG_INF_SENTINEL (underflow, can't represent i64::MIN)
    let val = i64::MIN as f64 * 1.5;
    let encoded = Alp::encode_value(val, 0, 0);
    assert_eq!(encoded, Alp::NEG_INF_SENTINEL);
}

#[test]
fn test_alp_subnormal() {
    let val = f64::from_bits(1);
    let encoded = Alp::encode_value(val, 10, 10);
    assert!(!encoded.is_negative() || encoded == 0, "subnormal encoded to {}", encoded);

    let decoded = Alp::decode_value(encoded, 10, 10);
    assert!(!decoded.is_nan(), "subnormal decoded to NaN");
}

#[test]
fn test_alp_i64_boundary() {
    let val = i64::MAX as f64 - 1024.0;
    let encoded = Alp::encode_value(val, 0, 0);
    assert_ne!(encoded, Alp::POS_INF_SENTINEL);
    let decoded = Alp::decode_value(encoded, 0, 0);
    assert!(!decoded.is_nan());

    let val = i64::MIN as f64 + 1024.0;
    let encoded = Alp::encode_value(val, 0, 0);
    assert!(encoded != Alp::NEG_INF_SENTINEL || encoded == i64::MIN);
    let decoded = Alp::decode_value(encoded, 0, 0);
    assert!(!decoded.is_nan() || encoded == Alp::NAN_SENTINEL);
}

#[test]
fn test_alp_roundtrip() {
    for raw in &[0.0, -0.0, 1.0, -1.0, 3.14159, -2.71828, 1e10, -1e10] {
        let encoded = Alp::encode_value(*raw, 0, 0);
        if encoded == Alp::POS_INF_SENTINEL || encoded == Alp::NEG_INF_SENTINEL || encoded == Alp::NAN_SENTINEL {
            continue;
        }
        let decoded = Alp::decode_value(encoded, 0, 0);
        let diff = (decoded - raw).abs();
        assert!(diff < 1.0, "mismatch for {}: encoded={} decoded={} diff={}", raw, encoded, decoded, diff);
    }
}
