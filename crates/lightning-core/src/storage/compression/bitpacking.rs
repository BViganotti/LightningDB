pub struct BitPacker;

impl BitPacker {
    /// Pack 32 values into a byte buffer using bit_width bits per value.
    pub fn pack_32(values: &[u64], bit_width: u8, output: &mut [u8]) {
        if bit_width == 0 {
            return;
        }
        // Special case: bit_width == 64 means store raw u64 values (no packing)
        if bit_width == 64 {
            for (i, &val) in values.iter().enumerate() {
                let offset = i * 8;
                if offset + 8 <= output.len() {
                    output[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
                }
            }
            return;
        }
        let mut bit_offset = 0usize;
        for &val in values.iter() {
            Self::write_bits(val, bit_width, bit_offset, output);
            bit_offset += bit_width as usize;
        }
    }

    pub fn unpack_32(data: &[u8], bit_width: u8, output: &mut [u64]) {
        if bit_width == 0 {
            return;
        }
        // Special case: bit_width == 64 means read raw u64 values
        if bit_width == 64 {
            for (i, v) in output.iter_mut().enumerate() {
                let offset = i * 8;
                if offset + 8 <= data.len() {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[offset..offset + 8]);
                    *v = u64::from_le_bytes(bytes);
                }
            }
            return;
        }
        let mut bit_offset = 0usize;
        for v in output.iter_mut() {
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

        if bit_in_word + bit_width as usize <= 64 && word_idx * 8 + 8 <= data.len() {
            let mut word = u64::from_le_bytes(
                data[word_idx * 8..word_idx * 8 + 8]
                    .try_into()
                    .expect("internal invariant violated"),
            );
            let mask = ((1u64 << bit_width) - 1) << bit_in_word;
            word = (word & !mask) | ((val << bit_in_word) & mask);
            data[word_idx * 8..word_idx * 8 + 8].copy_from_slice(&word.to_le_bytes());
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

        if bit_in_word + bit_width as usize <= 64 && word_idx * 8 + 8 <= data.len() {
            let word = u64::from_le_bytes(
                data[word_idx * 8..word_idx * 8 + 8]
                    .try_into()
                    .expect("internal invariant violated"),
            );
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
