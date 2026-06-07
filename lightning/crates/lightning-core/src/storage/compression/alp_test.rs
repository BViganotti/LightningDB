use crate::storage::compression::alp::Alp;

#[test]
fn test_alp_encode_decode() {
    let val = 123.456;
    // Choose fac=0, exp=3 -> 123.456 * 1000 / 1 = 123456
    let encoded = Alp::encode_value(val, 0, 3);
    assert_eq!(encoded, 123456);

    let decoded = Alp::decode_value(encoded, 0, 3);
    assert!((decoded - val).abs() < 1e-10);
}

#[test]
fn test_alp_with_factor() {
    let val = 123456000.0;
    // Choose fac=3 (1000), exp=0 -> 123456000 / 1000 = 123456
    let encoded = Alp::encode_value(val, 3, 0);
    assert_eq!(encoded, 123456);

    let decoded = Alp::decode_value(encoded, 3, 0);
    assert_eq!(decoded, val);
}
