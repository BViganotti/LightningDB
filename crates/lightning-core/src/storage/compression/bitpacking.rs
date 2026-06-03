pub struct BitPacker;

impl BitPacker {
    /// Pack 32 values into a byte buffer using bit_width bits per value.
    pub fn pack_32(values: &[u64], bit_width: u8, output: &mut [u8]) {
        assert!(values.len() >= 32);
        if bit_width == 0 {
            return;
        }

        let mut bit_offset = 0;
        for &val in &values[0..32] {
            Self::write_bits(val, bit_width, bit_offset, output);
            bit_offset += bit_width as usize;
        }
    }

    /// Unpack 32 values from a byte buffer.
    pub fn unpack_32(data: &[u8], bit_width: u8, output: &mut [u64]) {
        assert!(output.len() >= 32);
        if bit_width == 0 {
            for v in output.iter_mut().take(32) {
                *v = 0;
            }
            return;
        }

        let mut bit_offset = 0;
        for v in output.iter_mut().take(32) {
            *v = Self::read_bits(bit_width, bit_offset, data);
            bit_offset += bit_width as usize;
        }
    }

    fn write_bits(val: u64, bit_width: u8, bit_offset: usize, data: &mut [u8]) {
        if bit_width == 0 {
            return;
        }
        let word_idx = bit_offset / 64;
        let bit_in_word = bit_offset % 64;

        if bit_in_word + bit_width as usize <= 64 {
            let start = word_idx * 8;
            let end = std::cmp::min(start + 8, data.len());
            let len = end - start;
            let mut bytes = [0u8; 8];
            bytes[..len].copy_from_slice(&data[start..end]);
            let mut word = u64::from_le_bytes(bytes);
            let mask = ((1u64 << bit_width) - 1) << bit_in_word;
            word = (word & !mask) | ((val << bit_in_word) & mask);
            let bytes = word.to_le_bytes();
            data[start..end].copy_from_slice(&bytes[..len]);
            return;
        }

        let mut bits_written = 0;
        while bits_written < bit_width {
            let byte_idx = (bit_offset + bits_written as usize) / 8;
            let bit_in_byte = (bit_offset + bits_written as usize) % 8;
            let bits_to_write_in_byte =
                std::cmp::min(bit_width - bits_written, 8 - bit_in_byte as u8);

            let mask = ((1u32 << bits_to_write_in_byte) - 1) as u8;
            let bits = ((val >> bits_written) & mask as u64) as u8;

            data[byte_idx] |= bits << bit_in_byte;

            bits_written += bits_to_write_in_byte;
        }
    }

    fn read_bits(bit_width: u8, bit_offset: usize, data: &[u8]) -> u64 {
        if bit_width == 0 {
            return 0;
        }
        let word_idx = bit_offset / 64;
        let bit_in_word = bit_offset % 64;

        if bit_in_word + bit_width as usize <= 64 {
            let start = word_idx * 8;
            let end = std::cmp::min(start + 8, data.len());
            let len = end - start;
            let mut bytes = [0u8; 8];
            bytes[..len].copy_from_slice(&data[start..end]);
            let word = u64::from_le_bytes(bytes);
            let mask = (1u64 << bit_width) - 1;
            return (word >> bit_in_word) & mask;
        }

        let mut val = 0u64;
        let mut bits_read = 0;
        while bits_read < bit_width {
            let byte_idx = (bit_offset + bits_read as usize) / 8;
            let bit_in_byte = (bit_offset + bits_read as usize) % 8;
            let bits_to_read_in_byte = std::cmp::min(bit_width - bits_read, 8 - bit_in_byte as u8);

            let mask = ((1u32 << bits_to_read_in_byte) - 1) as u8;
            let bits = (data[byte_idx] >> bit_in_byte) & mask;

            val |= (bits as u64) << bits_read;

            bits_read += bits_to_read_in_byte;
        }
        val
    }
}
