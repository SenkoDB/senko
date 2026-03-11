use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

const HLL_P: usize = 14;
const HLL_REGISTERS: usize = 1 << HLL_P;
const HLL_BITS: usize = 6;
const HLL_DENSE_BYTES: usize = (HLL_REGISTERS * HLL_BITS) / 8;
const HLL_HDR_SIZE: usize = 16;
const HLL_MAGIC: &[u8; 4] = b"HYLL";
const HLL_SPARSE: u8 = 1;
const HLL_DENSE: u8 = 0;
const HLL_CACHE_INVALID: u64 = 1 << 63;
const MURMUR_SEED: u64 = 0xadc83b19;
const DEFAULT_SPARSE_MAX_BYTES: u64 = 3000;
const DEFAULT_DEBUG_ENABLED: bool = false;
const HLL_ALPHA_INF: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64);

static HLL_SPARSE_MAX_BYTES: AtomicU64 = AtomicU64::new(DEFAULT_SPARSE_MAX_BYTES);
static HLL_DEBUG_COMMANDS: AtomicBool = AtomicBool::new(DEFAULT_DEBUG_ENABLED);
#[cfg(test)]
static ESTIMATE_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PfDebugSubcommand {
    GetReg,
    Decode,
    Encode,
    ToDense,
    Encoding,
    SimdOn,
    SimdOff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HllError {
    WrongType,
    InvalidObject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    repr: Repr,
    cached_cardinality: u64,
    cache_valid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Repr {
    Sparse(Vec<u8>),
    Dense(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HllDescription {
    pub encoding: &'static str,
    pub cached_cardinality: Option<u64>,
    pub payload_len: usize,
    pub zero_registers: usize,
    pub max_register: u8,
}

pub fn set_sparse_max_bytes(value: u64) {
    HLL_SPARSE_MAX_BYTES.store(value.max(1), Ordering::Relaxed);
}

pub fn sparse_max_bytes() -> u64 {
    HLL_SPARSE_MAX_BYTES.load(Ordering::Relaxed)
}

pub fn set_debug_commands_enabled(enabled: bool) {
    HLL_DEBUG_COMMANDS.store(enabled, Ordering::Relaxed);
}

pub fn debug_commands_enabled() -> bool {
    HLL_DEBUG_COMMANDS.load(Ordering::Relaxed)
}

#[cfg(test)]
pub fn reset_estimate_counter() {
    ESTIMATE_COUNT.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub fn estimate_counter() -> usize {
    ESTIMATE_COUNT.load(Ordering::Relaxed)
}

impl Hll {
    pub fn new() -> Self {
        Self {
            repr: Repr::Sparse(encode_sparse_registers(&vec![0u8; HLL_REGISTERS])),
            cached_cardinality: 0,
            cache_valid: false,
        }
    }

    pub fn parse(raw: &[u8]) -> Result<Self, HllError> {
        if raw.len() < HLL_HDR_SIZE || &raw[..4] != HLL_MAGIC {
            return Err(HllError::WrongType);
        }
        let encoding = raw[4];
        let card = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        let cache_valid = (card & HLL_CACHE_INVALID) == 0;
        let cached_cardinality = card & !HLL_CACHE_INVALID;
        let payload = &raw[HLL_HDR_SIZE..];
        match encoding {
            HLL_DENSE => {
                if payload.len() != HLL_DENSE_BYTES {
                    return Err(HllError::WrongType);
                }
                Ok(Self {
                    repr: Repr::Dense(payload.to_vec()),
                    cached_cardinality,
                    cache_valid,
                })
            }
            HLL_SPARSE => {
                validate_sparse(payload)?;
                Ok(Self {
                    repr: Repr::Sparse(payload.to_vec()),
                    cached_cardinality,
                    cache_valid,
                })
            }
            _ => Err(HllError::WrongType),
        }
    }

    pub fn from_bytes(value: Option<&Bytes>) -> Result<Self, HllError> {
        match value {
            None => Ok(Self::new()),
            Some(raw) => Self::parse(raw.as_ref()),
        }
    }

    pub fn is_sparse(&self) -> bool {
        matches!(self.repr, Repr::Sparse(_))
    }

    pub fn encoding_name(&self) -> &'static str {
        if self.is_sparse() { "sparse" } else { "dense" }
    }

    pub fn zero_registers(&self) -> usize {
        self.registers().iter().filter(|&&v| v == 0).count()
    }

    pub fn max_register(&self) -> u8 {
        self.registers().into_iter().max().unwrap_or(0)
    }

    pub fn count(&mut self) -> u64 {
        if self.cache_valid {
            return self.cached_cardinality;
        }
        #[cfg(test)]
        ESTIMATE_COUNT.fetch_add(1, Ordering::Relaxed);
        let registers = self.registers();
        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        for &reg in &registers {
            sum += 2f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }
        let m = HLL_REGISTERS as f64;
        let mut estimate = HLL_ALPHA_INF * m * m / sum;
        if estimate <= 2.5 * m && zeros != 0 {
            estimate = m * (m / zeros as f64).ln();
        } else if estimate > (u32::MAX as f64) / 30.0 {
            let ratio = estimate / (u32::MAX as f64);
            estimate = -((u32::MAX as f64) * (1.0 - ratio).ln());
        }
        let rounded = estimate.round().max(0.0) as u64;
        self.cached_cardinality = rounded;
        self.cache_valid = true;
        rounded
    }

    pub fn add(&mut self, element: &[u8]) -> bool {
        let hash = murmurhash64a(element, MURMUR_SEED);
        let index = (hash & ((1u64 << HLL_P) - 1)) as usize;
        let mut value = hash >> HLL_P;
        value |= 1u64 << (63 - HLL_P);
        let rank = value.trailing_zeros() as u8 + 1;
        self.set_register(index, rank.min(63))
    }

    pub fn merge_from(&mut self, other: &Hll) {
        let mut left = self.registers();
        let right = other.registers();
        for (l, r) in left.iter_mut().zip(right.iter()) {
            *l = (*l).max(*r);
        }
        self.repr = Repr::Dense(registers_to_dense(&left));
        self.invalidate_cache();
    }

    pub fn to_dense(&mut self) {
        if self.is_sparse() {
            let regs = self.registers();
            self.repr = Repr::Dense(registers_to_dense(&regs));
        }
    }

    pub fn reencode(&mut self) {
        let regs = self.registers();
        if regs.iter().all(|&reg| reg <= 32) {
            let sparse = encode_sparse_registers(&regs);
            if sparse.len() <= sparse_max_bytes() as usize {
                self.repr = Repr::Sparse(sparse);
                return;
            }
        }
        self.repr = Repr::Dense(registers_to_dense(&regs));
    }

    pub fn get_registers(&self) -> Vec<u8> {
        self.registers()
    }

    pub fn describe(&self) -> HllDescription {
        let regs = self.registers();
        HllDescription {
            encoding: self.encoding_name(),
            cached_cardinality: self.cache_valid.then_some(self.cached_cardinality),
            payload_len: match &self.repr {
                Repr::Sparse(buf) | Repr::Dense(buf) => buf.len(),
            },
            zero_registers: regs.iter().filter(|&&v| v == 0).count(),
            max_register: regs.into_iter().max().unwrap_or(0),
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let payload = match &self.repr {
            Repr::Sparse(buf) | Repr::Dense(buf) => buf.as_slice(),
        };
        let mut out = BytesMut::with_capacity(HLL_HDR_SIZE + payload.len());
        out.extend_from_slice(HLL_MAGIC);
        out.extend_from_slice(&[
            match self.repr {
                Repr::Dense(_) => HLL_DENSE,
                Repr::Sparse(_) => HLL_SPARSE,
            },
            0,
            0,
            0,
        ]);
        let card = if self.cache_valid {
            self.cached_cardinality
        } else {
            self.cached_cardinality | HLL_CACHE_INVALID
        };
        out.extend_from_slice(&card.to_le_bytes());
        out.extend_from_slice(payload);
        out.freeze()
    }

    fn registers(&self) -> Vec<u8> {
        match &self.repr {
            Repr::Dense(buf) => {
                let mut regs = vec![0u8; HLL_REGISTERS];
                for (idx, reg) in regs.iter_mut().enumerate() {
                    *reg = dense_get(buf, idx);
                }
                regs
            }
            Repr::Sparse(buf) => decode_sparse_registers(buf).expect("validated sparse payload"),
        }
    }

    fn set_register(&mut self, idx: usize, value: u8) -> bool {
        let mut regs = self.registers();
        if regs[idx] >= value {
            return false;
        }
        regs[idx] = value;
        let must_dense = regs[idx] > 32;
        if !must_dense {
            let sparse = encode_sparse_registers(&regs);
            if sparse.len() <= sparse_max_bytes() as usize {
                self.repr = Repr::Sparse(sparse);
            } else {
                self.repr = Repr::Dense(registers_to_dense(&regs));
            }
        } else {
            self.repr = Repr::Dense(registers_to_dense(&regs));
        }
        self.invalidate_cache();
        true
    }

    fn invalidate_cache(&mut self) {
        self.cache_valid = false;
    }
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

pub fn dense_get(regs: &[u8], idx: usize) -> u8 {
    let bit = idx * HLL_BITS;
    let byte = bit / 8;
    let shift = bit & 7;
    let first = regs.get(byte).copied().unwrap_or(0) as u16;
    let second = regs.get(byte + 1).copied().unwrap_or(0) as u16;
    (((first >> shift) | (second << (8 - shift))) & 0x3f) as u8
}

pub fn dense_set(regs: &mut [u8], idx: usize, val: u8) {
    let bit = idx * HLL_BITS;
    let byte = bit / 8;
    let shift = bit & 7;
    let mask = 0x3fu16 << shift;
    let pair = (regs[byte] as u16) | ((regs.get(byte + 1).copied().unwrap_or(0) as u16) << 8);
    let next = (pair & !mask) | (((val as u16) & 0x3f) << shift);
    regs[byte] = (next & 0xff) as u8;
    if let Some(dst) = regs.get_mut(byte + 1) {
        *dst = (next >> 8) as u8;
    }
}

pub fn murmurhash64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let len = data.len() as u64;
    let mut h = seed ^ len.wrapping_mul(M);

    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let mut k = u64::from_le_bytes(chunk.try_into().unwrap());
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }

    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut tail = 0u64;
        for (shift, byte) in rem.iter().enumerate() {
            tail ^= (*byte as u64) << (shift * 8);
        }
        h ^= tail;
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^ (h >> R)
}

fn registers_to_dense(regs: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; HLL_DENSE_BYTES];
    for (idx, &val) in regs.iter().enumerate() {
        dense_set(&mut out, idx, val);
    }
    out
}

fn decode_sparse_registers(buf: &[u8]) -> Result<Vec<u8>, HllError> {
    let mut regs = vec![0u8; HLL_REGISTERS];
    let mut reg = 0usize;
    let mut idx = 0usize;
    while idx < buf.len() {
        let op = buf[idx];
        if op & 0b1100_0000 == 0b0000_0000 {
            let len = (op & 0x3f) as usize + 1;
            reg = reg.checked_add(len).ok_or(HllError::InvalidObject)?;
            idx += 1;
        } else if op & 0b1100_0000 == 0b0100_0000 {
            if idx + 1 >= buf.len() {
                return Err(HllError::InvalidObject);
            }
            let len = ((((op & 0x3f) as usize) << 8) | buf[idx + 1] as usize) + 1;
            reg = reg.checked_add(len).ok_or(HllError::InvalidObject)?;
            idx += 2;
        } else {
            let val = ((op >> 2) & 0x1f) + 1;
            let len = (op & 0x03) as usize + 1;
            if val > 32 {
                return Err(HllError::InvalidObject);
            }
            let end = reg.checked_add(len).ok_or(HllError::InvalidObject)?;
            if end > HLL_REGISTERS {
                return Err(HllError::InvalidObject);
            }
            for slot in &mut regs[reg..end] {
                *slot = val;
            }
            reg = end;
            idx += 1;
        }
        if reg > HLL_REGISTERS {
            return Err(HllError::InvalidObject);
        }
    }
    if reg != HLL_REGISTERS {
        return Err(HllError::InvalidObject);
    }
    Ok(regs)
}

fn validate_sparse(buf: &[u8]) -> Result<(), HllError> {
    decode_sparse_registers(buf).map(|_| ())
}

fn encode_sparse_registers(regs: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx < regs.len() {
        let value = regs[idx];
        if value == 0 {
            let mut len = 1usize;
            while idx + len < regs.len() && regs[idx + len] == 0 {
                len += 1;
            }
            emit_zero_run(&mut out, len);
            idx += len;
            continue;
        }

        let mut len = 1usize;
        while idx + len < regs.len() && regs[idx + len] == value && len < 4 {
            len += 1;
        }
        out.push(0x80 | ((value - 1) << 2) | ((len - 1) as u8));
        idx += len;
    }
    out
}

fn emit_zero_run(out: &mut Vec<u8>, mut len: usize) {
    while len > 0 {
        if len >= 65 {
            let chunk = len.min(16_384);
            let encoded = chunk - 1;
            out.push(0x40 | ((encoded >> 8) as u8 & 0x3f));
            out.push((encoded & 0xff) as u8);
            len -= chunk;
        } else {
            let chunk = len.min(64);
            out.push((chunk - 1) as u8);
            len -= chunk;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HLL_CACHE_INVALID, HLL_HDR_SIZE, HLL_MAGIC, HLL_REGISTERS, Hll, decode_sparse_registers,
        dense_get, dense_set, encode_sparse_registers, estimate_counter, murmurhash64a,
        reset_estimate_counter, sparse_max_bytes,
    };

    #[test]
    fn dense_round_trip() {
        let mut regs = vec![0u8; (HLL_REGISTERS * 6) / 8];
        dense_set(&mut regs, 0, 1);
        dense_set(&mut regs, 1, 17);
        dense_set(&mut regs, 1234, 63);
        assert_eq!(dense_get(&regs, 0), 1);
        assert_eq!(dense_get(&regs, 1), 17);
        assert_eq!(dense_get(&regs, 1234), 63);
    }

    #[test]
    fn sparse_encode_decode_round_trip() {
        let mut regs = vec![0u8; HLL_REGISTERS];
        regs[1] = 5;
        regs[2] = 5;
        regs[100] = 7;
        let encoded = encode_sparse_registers(&regs);
        assert_eq!(decode_sparse_registers(&encoded).unwrap(), regs);
    }

    #[test]
    fn murmur_hash_is_stable() {
        assert_eq!(
            murmurhash64a(b"redis", 0xadc83b19),
            murmurhash64a(b"redis", 0xadc83b19)
        );
    }

    #[test]
    fn cache_header_bit_flips_on_count_and_add() {
        let mut hll = Hll::new();
        let bytes = hll.to_bytes();
        assert_eq!(&bytes[..4], HLL_MAGIC);
        assert_ne!(
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()) & HLL_CACHE_INVALID,
            0
        );
        let _ = hll.count();
        let counted = hll.to_bytes();
        assert_eq!(counted[15] & 0x80, 0);
        let _ = hll.add(b"foo");
        let dirty = hll.to_bytes();
        assert_eq!(dirty[15] & 0x80, 0x80);
        assert_eq!(dirty.len() >= HLL_HDR_SIZE, true);
    }

    #[test]
    fn estimate_cache_skips_second_pass() {
        let mut hll = Hll::new();
        for i in 0..1000 {
            let _ = hll.add(format!("v{i}").as_bytes());
        }
        reset_estimate_counter();
        let _ = hll.count();
        let first = estimate_counter();
        let _ = hll.count();
        let second = estimate_counter();
        assert_eq!(first, 1);
        assert_eq!(second, 1);
    }

    #[test]
    fn promotion_respects_sparse_threshold() {
        let mut hll = Hll::new();
        let threshold = sparse_max_bytes() as usize;
        let mut i = 0usize;
        while hll.to_bytes().len() <= HLL_HDR_SIZE + threshold {
            let _ = hll.add(format!("x{i}").as_bytes());
            i += 1;
            if i > 20_000 {
                break;
            }
        }
        assert!(!hll.is_sparse() || hll.to_bytes().len() <= HLL_HDR_SIZE + threshold);
    }
}
