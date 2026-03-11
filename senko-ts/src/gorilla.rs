use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, Default)]
pub struct BitWriter {
    pub(crate) buf: BytesMut,
    pub(crate) current: u8,
    pub(crate) used_bits: u8,
    pub(crate) total_bits: usize,
}

impl BitWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            current: 0,
            used_bits: 0,
            total_bits: 0,
        }
    }

    pub fn write_bit(&mut self, bit: bool) {
        self.current <<= 1;
        if bit {
            self.current |= 1;
        }
        self.used_bits += 1;
        self.total_bits += 1;
        if self.used_bits == 8 {
            self.buf.extend_from_slice(&[self.current]);
            self.current = 0;
            self.used_bits = 0;
        }
    }

    pub fn write_bits(&mut self, value: u64, bits: u8) {
        if bits == 0 {
            return;
        }
        for shift in (0..bits).rev() {
            self.write_bit(((value >> shift) & 1) != 0);
        }
    }

    pub fn write_i64_bits(&mut self, value: i64, bits: u8) {
        if bits == 64 {
            self.write_bits(value as u64, 64);
            return;
        }
        let mask = (1_u64 << bits) - 1;
        self.write_bits((value as u64) & mask, bits);
    }

    pub fn bit_len(&self) -> usize {
        self.total_bits
    }

    pub fn byte_len(&self) -> usize {
        self.buf.len() + usize::from(self.used_bits > 0)
    }

    pub fn finish(mut self) -> Bytes {
        if self.used_bits > 0 {
            self.current <<= 8 - self.used_bits;
            self.buf.extend_from_slice(&[self.current]);
            self.current = 0;
            self.used_bits = 0;
        }
        self.buf.freeze()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BitReader<'a> {
    buf: &'a [u8],
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, bit_pos: 0 }
    }

    pub fn read_bit(&mut self) -> Option<bool> {
        if self.bit_pos >= self.buf.len() * 8 {
            return None;
        }
        let byte = self.buf[self.bit_pos / 8];
        let shift = 7 - (self.bit_pos % 8);
        self.bit_pos += 1;
        Some(((byte >> shift) & 1) != 0)
    }

    pub fn read_bits(&mut self, bits: u8) -> Option<u64> {
        if bits == 0 {
            return Some(0);
        }
        let mut value = 0_u64;
        for _ in 0..bits {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Some(value)
    }

    pub fn read_i64_bits(&mut self, bits: u8) -> Option<i64> {
        if bits == 64 {
            return self.read_bits(64).map(|value| value as i64);
        }
        let raw = self.read_bits(bits)?;
        Some(sign_extend(raw, bits))
    }
}

#[derive(Debug, Clone)]
pub struct CompressedChunk {
    writer: BitWriter,
    prev_ts: i64,
    prev_delta: i64,
    prev_val_bits: u64,
    prev_leading: u8,
    prev_trailing: u8,
    sample_count: u16,
}

impl Default for CompressedChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressedChunk {
    pub fn new() -> Self {
        Self {
            writer: BitWriter::new(),
            prev_ts: 0,
            prev_delta: 0,
            prev_val_bits: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            sample_count: 0,
        }
    }

    pub fn from_samples(samples: &[(i64, f64)]) -> Self {
        let mut chunk = Self::new();
        for &(ts, val) in samples {
            chunk.compress_sample(ts, val);
        }
        chunk
    }

    pub fn compress_sample(&mut self, ts: i64, val: f64) {
        let val_bits = val.to_bits();
        match self.sample_count {
            0 => {
                self.writer.write_i64_bits(ts, 64);
                self.writer.write_bits(val_bits, 64);
                self.prev_ts = ts;
                self.prev_val_bits = val_bits;
            }
            1 => {
                let delta = ts - self.prev_ts;
                self.writer.write_i64_bits(delta, 14);
                encode_value_xor(
                    &mut self.writer,
                    val_bits,
                    self.prev_val_bits,
                    &mut self.prev_leading,
                    &mut self.prev_trailing,
                );
                self.prev_delta = delta;
                self.prev_ts = ts;
                self.prev_val_bits = val_bits;
            }
            _ => {
                let delta = ts - self.prev_ts;
                let dod = delta - self.prev_delta;
                encode_delta_of_delta(&mut self.writer, dod);
                encode_value_xor(
                    &mut self.writer,
                    val_bits,
                    self.prev_val_bits,
                    &mut self.prev_leading,
                    &mut self.prev_trailing,
                );
                self.prev_delta = delta;
                self.prev_ts = ts;
                self.prev_val_bits = val_bits;
            }
        }
        self.sample_count = self.sample_count.saturating_add(1);
    }

    pub fn decompress_all(&self) -> Vec<(i64, f64)> {
        if self.sample_count == 0 {
            return Vec::new();
        }
        let encoded = self.writer.clone().finish();
        let mut reader = BitReader::new(encoded.as_ref());
        let mut out = Vec::with_capacity(self.sample_count as usize);

        let first_ts = reader
            .read_i64_bits(64)
            .expect("invalid compressed timestamp");
        let first_val = f64::from_bits(reader.read_bits(64).expect("invalid compressed value"));
        out.push((first_ts, first_val));
        if self.sample_count == 1 {
            return out;
        }

        let mut prev_delta = reader.read_i64_bits(14).expect("invalid compressed delta");
        let second_ts = first_ts + prev_delta;
        let mut prev_val_bits = first_val.to_bits();
        let mut prev_leading = u8::MAX;
        let mut prev_trailing = 0_u8;
        let second_val_bits = decode_value_xor(
            &mut reader,
            prev_val_bits,
            &mut prev_leading,
            &mut prev_trailing,
        )
        .expect("invalid compressed value xor");
        out.push((second_ts, f64::from_bits(second_val_bits)));
        let mut prev_ts = second_ts;
        prev_val_bits = second_val_bits;

        while out.len() < self.sample_count as usize {
            let dod = decode_delta_of_delta(&mut reader).expect("invalid compressed dod");
            let delta = prev_delta + dod;
            let ts = prev_ts + delta;
            let val_bits = decode_value_xor(
                &mut reader,
                prev_val_bits,
                &mut prev_leading,
                &mut prev_trailing,
            )
            .expect("invalid compressed value xor");
            out.push((ts, f64::from_bits(val_bits)));
            prev_ts = ts;
            prev_delta = delta;
            prev_val_bits = val_bits;
        }

        out
    }

    pub fn byte_len(&self) -> usize {
        self.writer.byte_len()
    }
}

fn encode_delta_of_delta(writer: &mut BitWriter, dod: i64) {
    if dod == 0 {
        writer.write_bit(false);
    } else if (-63..=64).contains(&dod) {
        writer.write_bits(0b10, 2);
        writer.write_i64_bits(dod, 7);
    } else if (-255..=256).contains(&dod) {
        writer.write_bits(0b110, 3);
        writer.write_i64_bits(dod, 9);
    } else if (-2047..=2048).contains(&dod) {
        writer.write_bits(0b1110, 4);
        writer.write_i64_bits(dod, 12);
    } else {
        writer.write_bits(0b1111, 4);
        writer.write_i64_bits(dod, 64);
    }
}

fn decode_delta_of_delta(reader: &mut BitReader<'_>) -> Option<i64> {
    if !reader.read_bit()? {
        return Some(0);
    }
    if !reader.read_bit()? {
        return reader.read_i64_bits(7);
    }
    if !reader.read_bit()? {
        return reader.read_i64_bits(9);
    }
    if !reader.read_bit()? {
        return reader.read_i64_bits(12);
    }
    reader.read_i64_bits(64)
}

fn encode_value_xor(
    writer: &mut BitWriter,
    value_bits: u64,
    prev_bits: u64,
    prev_leading: &mut u8,
    prev_trailing: &mut u8,
) {
    let xor = value_bits ^ prev_bits;
    if xor == 0 {
        writer.write_bit(false);
        return;
    }

    let leading = xor.leading_zeros() as u8;
    let trailing = xor.trailing_zeros() as u8;
    let meaningful_bits = 64_u8.saturating_sub(leading + trailing);

    writer.write_bit(true);
    if *prev_leading != u8::MAX
        && leading >= *prev_leading
        && trailing >= *prev_trailing
        && 64_u8.saturating_sub(*prev_leading + *prev_trailing) >= meaningful_bits
    {
        writer.write_bit(false);
        let reused_bits = 64_u8.saturating_sub(*prev_leading + *prev_trailing);
        writer.write_bits(xor >> *prev_trailing, reused_bits);
    } else {
        writer.write_bit(true);
        writer.write_bits(u64::from(leading), 5);
        writer.write_bits(u64::from(encode_sig_len(meaningful_bits)), 6);
        writer.write_bits(xor >> trailing, meaningful_bits);
        *prev_leading = leading;
        *prev_trailing = trailing;
    }
}

fn decode_value_xor(
    reader: &mut BitReader<'_>,
    prev_bits: u64,
    prev_leading: &mut u8,
    prev_trailing: &mut u8,
) -> Option<u64> {
    if !reader.read_bit()? {
        return Some(prev_bits);
    }

    let xor = if !reader.read_bit()? {
        let meaningful_bits = 64_u8.saturating_sub(*prev_leading + *prev_trailing);
        let meaningful = reader.read_bits(meaningful_bits)?;
        meaningful << *prev_trailing
    } else {
        let leading = reader.read_bits(5)? as u8;
        let meaningful_bits = decode_sig_len(reader.read_bits(6)? as u8);
        let meaningful = reader.read_bits(meaningful_bits)?;
        let trailing = 64_u8.saturating_sub(leading + meaningful_bits);
        *prev_leading = leading;
        *prev_trailing = trailing;
        meaningful << trailing
    };
    Some(prev_bits ^ xor)
}

#[inline]
fn encode_sig_len(len: u8) -> u8 {
    if len == 64 { 0 } else { len }
}

#[inline]
fn decode_sig_len(len: u8) -> u8 {
    if len == 0 { 64 } else { len }
}

fn sign_extend(value: u64, bits: u8) -> i64 {
    let shift = 64 - bits;
    ((value << shift) as i64) >> shift
}

#[cfg(test)]
mod tests {
    use super::CompressedChunk;

    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }

    #[test]
    fn gorilla_round_trip_preserves_samples() {
        let mut seed = 42_u64;
        let mut ts = 1_700_000_000_000_i64;
        let mut value = 17.25_f64;
        let mut samples = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            ts += 950 + (lcg(&mut seed) % 100) as i64;
            let delta = ((lcg(&mut seed) >> 11) as f64 / ((1_u64 << 20) as f64)) - 0.5;
            value += delta * 0.01;
            samples.push((ts, value));
        }

        let chunk = CompressedChunk::from_samples(&samples);
        assert_eq!(chunk.decompress_all(), samples);
    }

    #[test]
    fn compression_ratio_stays_below_two_bytes_on_stable_series() {
        let mut ts = 1_700_000_000_000_i64;
        let mut samples = Vec::with_capacity(10_000);
        for idx in 0..10_000 {
            ts += 1_000;
            let val = 42.0 + ((idx / 256) as f64) * 0.000_001;
            samples.push((ts, val));
        }

        let chunk = CompressedChunk::from_samples(&samples);
        let bytes_per_sample = chunk.byte_len() as f64 / samples.len() as f64;
        assert!(bytes_per_sample < 2.0, "bytes/sample = {bytes_per_sample}");
    }
}
