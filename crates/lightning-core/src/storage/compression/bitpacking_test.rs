use crate::storage::compression::bitpacking::BitPacker;

#[test]
fn test_bit_packing_32() {
    let mut values = [0u64; 32];
    for i in 0..32 {
        values[i] = i as u64;
    }

    let bit_width = 5;
    let mut output = [0u8; 20];
    BitPacker::pack_32(&values, bit_width, &mut output);

    let mut unpacked = [0u64; 32];
    BitPacker::unpack_32(&output, bit_width, &mut unpacked);

    assert_eq!(values, unpacked);
}

#[test]
fn test_bit_packing_large() {
    let mut values = [0u64; 32];
    for i in 0..32 {
        values[i] = (1u64 << 10) + i as u64;
    }

    let bit_width = 11;
    let mut output = [0u8; 44];
    BitPacker::pack_32(&values, bit_width, &mut output);

    let mut unpacked = [0u64; 32];
    BitPacker::unpack_32(&output, bit_width, &mut unpacked);

    assert_eq!(values, unpacked);
}

#[test]
fn test_bitpacking_offset() {
    use crate::processor::Value;
    use crate::storage::compression::integer_bitpacking::IntegerBitpacking;
    use crate::storage::compression::{CompressionAlg, CompressionMetadata, CompressionType};

    let alg = IntegerBitpacking;
    let meta = CompressionMetadata::new(
        Value::Null,
        Value::Null,
        CompressionType::IntegerBitpacking,
        2,
    );

    let mut values = [0u8; 32 * 8];
    for i in 0..32 {
        let val = (i % 4) as u64;
        values[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
    }

    let mut compressed = vec![0u8; 4096];
    let (processed, size) = alg
        .compress_next_page(&values, 32, &mut compressed, &meta)
        .unwrap();
    assert_eq!(size, 32);
    assert_eq!(processed, 8);

    let mut decompressed = vec![0u8; 32 * 8];
    alg.decompress_from_page(&compressed, 1, &mut decompressed, 0, 31, &meta)
        .unwrap();

    let val1 = u64::from_le_bytes(decompressed[0..8].try_into().unwrap());
    assert_eq!(val1, 1);
}

#[test]
fn test_bitpacking_width_0() {
    let mut values = [0u64; 32];
    let bit_width = 0;
    let mut output = [0xFFu8; 1];
    BitPacker::pack_32(&values, bit_width, &mut output);

    let mut unpacked = [0u64; 32];
    BitPacker::unpack_32(&output, bit_width, &mut unpacked);

    assert_eq!(unpacked, [0u64; 32]);
    // Non-zero values should panic (all values must be 0 for bit_width=0)
    let mut bad_values = [0u64; 32];
    bad_values[0] = 1;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        BitPacker::pack_32(&bad_values, 0, &mut [0u8; 1]);
    }));
    assert!(result.is_err(), "bit_width=0 with non-zero values should panic");
}

#[test]
fn test_bitpacking_width_64() {
    let mut values = [0u64; 32];
    for i in 0..32 {
        values[i] = u64::MAX;
    }

    let bit_width = 64;
    let mut output = [0u8; 256];
    BitPacker::pack_32(&values, bit_width, &mut output);

    let mut unpacked = [0u64; 32];
    BitPacker::unpack_32(&output, bit_width, &mut unpacked);

    assert_eq!(values, unpacked);
}

#[test]
fn test_bitpacking_roundtrip_all_widths() {
    for bw in 0u8..=64u8 {
        let max_val = if bw == 64 { u64::MAX } else if bw == 0 { 0 } else { (1u64 << (bw - 1)) - 1 };
        let mut values = [0u64; 32];
        for i in 0..32 {
            values[i] = (i as u64).min(max_val);
        }

        let output_size = ((32 * bw as usize) + 7) / 8;
        let mut output = vec![0u8; output_size.max(1)];
        BitPacker::pack_32(&values, bw, &mut output);

        let mut unpacked = [0u64; 32];
        BitPacker::unpack_32(&output, bw, &mut unpacked);

        assert_eq!(values, unpacked, "roundtrip mismatch for bit_width={}", bw);
    }
}
