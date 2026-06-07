use crate::storage::compression::bitpacking::BitPacker;

#[test]
fn test_bit_packing_32() {
    let mut values = [0u64; 32];
    for i in 0..32 {
        values[i] = i as u64;
    }

    let bit_width = 5; // Can represent up to 31
    let mut output = [0u8; 20]; // 32 * 5 / 8 = 20 bytes
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
    let mut output = [0u8; 44]; // 32 * 11 / 8 = 44 bytes
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
    // Wait! processed and size are swapped in returning (size, processed)
    // Actually returned Ok((compressed_size as u64, num_values_to_compress))
    // So 0 is compressed_size, 1 is num_processed
    assert_eq!(size, 32);
    assert_eq!(processed, 8); // 32 * 2 bits = 8 bytes

    // Now decompress starting at offset 1
    let mut decompressed = vec![0u8; 32 * 8];
    alg.decompress_from_page(&compressed, 1, &mut decompressed, 0, 31, &meta)
        .unwrap();

    let val1 = u64::from_le_bytes(decompressed[0..8].try_into().unwrap());
    assert_eq!(val1, 1); // Value 1 should be 1%4 = 1
}
