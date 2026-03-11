use std::convert::TryInto;

use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    store::{SetCondition, SetExpiry, SetOptions, Store},
};

const MAX_BITMAP_OFFSET: u64 = u32::MAX as u64;
const MAX_STRING_SIZE: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeUnit {
    Byte,
    Bit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
    Diff,
    Diff1,
    AndOr,
    One,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldEncoding {
    Unsigned(u8),
    Signed(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldOffset {
    Absolute(u64),
    Multiplied(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OverflowMode {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitfieldSubcmd {
    Get {
        enc: BitfieldEncoding,
        offset: BitfieldOffset,
    },
    Set {
        enc: BitfieldEncoding,
        offset: BitfieldOffset,
        value: i64,
    },
    IncrBy {
        enc: BitfieldEncoding,
        offset: BitfieldOffset,
        increment: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParsedBitfieldOp {
    Overflow(OverflowMode),
    Action(BitfieldSubcmd),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedBitfield {
    ops: SmallVec<[ParsedBitfieldOp; 8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitmapBuf(BytesMut);

impl BitmapBuf {
    fn new(inner: BytesMut) -> Self {
        Self(inner)
    }

    fn freeze(self) -> Bytes {
        self.0.freeze()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get_bit(&self, offset: u64) -> u8 {
        let byte_index = (offset / 8) as usize;
        if byte_index >= self.0.len() {
            return 0;
        }
        let shift = 7 - (offset % 8) as u8;
        (self.0[byte_index] >> shift) & 1
    }

    fn set_bit(&mut self, offset: u64, value: u8) -> u8 {
        let byte_index = (offset / 8) as usize;
        let shift = 7 - (offset % 8) as u8;
        let mask = 1u8 << shift;
        let old = if byte_index < self.0.len() {
            ((self.0[byte_index] & mask) != 0) as u8
        } else {
            0
        };
        if self.0.len() <= byte_index {
            self.0.resize(byte_index + 1, 0);
        }
        if value == 0 {
            self.0[byte_index] &= !mask;
        } else {
            self.0[byte_index] |= mask;
        }
        old
    }

    fn count_ones_range(&self, start: i64, end: i64, unit: RangeUnit) -> u64 {
        let Some((range_start, range_end)) = normalise_range(start, end, self.0.len(), unit) else {
            return 0;
        };

        match unit {
            RangeUnit::Byte => popcount_bytes(&self.0[range_start..=range_end]),
            RangeUnit::Bit => popcount_bit_range(self.0.as_ref(), range_start, range_end),
        }
    }

    fn find_first_bit(&self, target: u8, start: i64, end: Option<i64>, unit: RangeUnit) -> i64 {
        if self.0.is_empty() {
            return if target == 0 { 0 } else { -1 };
        }

        let total_bits = self.0.len() * 8;
        let search_end = match end {
            Some(value) => value,
            None => match unit {
                RangeUnit::Byte => self.0.len().saturating_sub(1) as i64,
                RangeUnit::Bit => total_bits.saturating_sub(1) as i64,
            },
        };

        let Some((range_start, range_end)) = normalise_range(start, search_end, self.0.len(), unit)
        else {
            return -1;
        };

        let bit_start = match unit {
            RangeUnit::Byte => range_start * 8,
            RangeUnit::Bit => range_start,
        };
        let bit_end = match unit {
            RangeUnit::Byte => range_end * 8 + 7,
            RangeUnit::Bit => range_end,
        };

        let found = find_first_in_bit_range(self.0.as_ref(), target, bit_start, bit_end);
        if found >= 0 {
            return found;
        }
        if target == 0 && end.is_none() {
            return total_bits as i64;
        }
        -1
    }

    fn read_bits(&self, offset: u64, width: u8) -> u64 {
        let mut out = 0u64;
        let mut index = 0u8;
        while index < width {
            out = (out << 1) | self.get_bit(offset + index as u64) as u64;
            index += 1;
        }
        out
    }

    fn write_bits(&mut self, offset: u64, width: u8, value: u64) {
        let mut index = 0u8;
        while index < width {
            let shift = width - 1 - index;
            let bit = ((value >> shift) & 1) as u8;
            let _ = self.set_bit(offset + index as u64, bit);
            index += 1;
        }
    }
}

#[inline]
pub fn getbit(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'getbit' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let offset = parse_bit_offset(arg_bytes(&args[1])?)?;
    let current = store.get_cloned(key);
    let bitmap = BitmapBuf::new(materialize_value_bytes(current.as_ref())?);
    Ok(Response::Integer(bitmap.get_bit(offset) as i64))
}

#[inline]
pub fn setbit(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'setbit' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let offset = parse_bit_offset(arg_bytes(&args[1])?)?;
    let value = parse_bit_value(arg_bytes(&args[2])?)?;
    let needed_len = offset as usize / 8 + 1;
    ensure_bitmap_len(needed_len)?;

    let current = store.get_cloned(key_bytes);
    let mut bitmap = BitmapBuf::new(materialize_value_bytes(current.as_ref())?);
    if bitmap.len() < needed_len {
        bitmap.0.resize(needed_len, 0);
    }
    let old = bitmap.set_bit(offset, value);
    store_bitmap(store, key, current.is_some(), bitmap)?;
    Ok(Response::Integer(old as i64))
}

#[inline]
pub fn bitcount(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'bitcount' command",
        ));
    }
    if args.len() == 2 || args.len() > 4 {
        return Err(SenkoError::Protocol("syntax error"));
    }

    let key = arg_bytes(&args[0])?;
    let parsed_range = match args.len() {
        1 => None,
        3 => Some((
            parse_i64(arg_bytes(&args[1])?)?,
            parse_i64(arg_bytes(&args[2])?)?,
            RangeUnit::Byte,
        )),
        4 => Some((
            parse_i64(arg_bytes(&args[1])?)?,
            parse_i64(arg_bytes(&args[2])?)?,
            parse_range_unit(arg_bytes(&args[3])?)?,
        )),
        _ => unreachable!(),
    };
    let Some(current) = store.get_cloned(key) else {
        return Ok(Response::Integer(0));
    };

    let bitmap = BitmapBuf::new(materialize_value_bytes(Some(&current))?);
    let count = match parsed_range {
        Some((start, end, unit)) => bitmap.count_ones_range(start, end, unit),
        None => popcount_bytes(bitmap.0.as_ref()),
    };
    Ok(Response::Integer(count as i64))
}

#[inline]
pub fn bitpos(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 || args.len() > 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'bitpos' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let bit = parse_bit_value(arg_bytes(&args[1])?)?;
    let start = if args.len() >= 3 {
        Some(parse_i64(arg_bytes(&args[2])?)?)
    } else {
        None
    };
    let end = if args.len() >= 4 {
        Some(parse_i64(arg_bytes(&args[3])?)?)
    } else {
        None
    };
    let unit = if args.len() == 5 {
        parse_range_unit(arg_bytes(&args[4])?)?
    } else {
        RangeUnit::Byte
    };

    let Some(current) = store.get_cloned(key) else {
        return Ok(Response::Integer(if bit == 0 { 0 } else { -1 }));
    };
    let bitmap = BitmapBuf::new(materialize_value_bytes(Some(&current))?);
    let pos = bitmap.find_first_bit(bit, start.unwrap_or(0), end, unit);
    Ok(Response::Integer(pos))
}

#[inline]
pub fn bitop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'bitop' command",
        ));
    }

    let op = parse_bitop(arg_bytes(&args[0])?)?;
    let dest_key = parse_key(arg_bytes(&args[1])?)?;
    let source_frames = &args[2..];
    if matches!(op, BitOp::Not) && source_frames.len() != 1 {
        return Err(SenkoError::Protocol(
            "ERR BITOP NOT must be called with a single source key",
        ));
    }

    let mut sources = SmallVec::<[Bytes; 8]>::with_capacity(source_frames.len());
    let mut longest = 0usize;
    for frame in source_frames {
        let key = arg_bytes(frame)?;
        let Some(value) = store.get_cloned(key) else {
            sources.push(Bytes::new());
            continue;
        };
        let bytes = materialize_value_bytes(Some(&value))?.freeze();
        longest = longest.max(bytes.len());
        sources.push(bytes);
    }

    if longest == 0 {
        let _ = store.delete(dest_key.as_bytes());
        return Ok(Response::Integer(0));
    }

    let mut out = BytesMut::with_capacity(longest);
    out.resize(longest, 0);
    apply_bitop(op, &sources, out.as_mut());
    let _ = store.set(
        dest_key,
        SenkoValue::Raw(out.freeze()),
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::None,
            get_old: false,
        },
    );
    Ok(Response::Integer(longest as i64))
}

#[inline]
pub fn bitfield(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    bitfield_impl(store, args, false)
}

#[inline]
pub fn bitfield_ro(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    bitfield_impl(store, args, true)
}

fn bitfield_impl(store: &mut Store, args: &[Frame<'_>], read_only: bool) -> SenkoResult<Response> {
    if args.is_empty() {
        let name = if read_only { "bitfield_ro" } else { "bitfield" };
        return Err(SenkoError::ProtocolMessage(CompactString::from(format!(
            "wrong number of arguments for '{name}' command"
        ))));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let parsed = parse_bitfield_subcommands(&args[1..], read_only)?;
    if parsed.ops.is_empty() {
        return Ok(Response::Array(Box::default()));
    }

    let current = store.get_cloned(key_bytes);
    let mut bitmap = BitmapBuf::new(materialize_value_bytes(current.as_ref())?);
    let mut touched = false;
    let mut results = SmallVec::<[Response; 16]>::new();
    let mut overflow = OverflowMode::Wrap;

    for op in parsed.ops {
        match op {
            ParsedBitfieldOp::Overflow(mode) => {
                overflow = mode;
            }
            ParsedBitfieldOp::Action(BitfieldSubcmd::Get { enc, offset }) => {
                let offset = resolve_bitfield_offset(enc, offset);
                results.push(Response::Integer(read_bitfield_value(
                    &bitmap, enc, offset,
                )?));
            }
            ParsedBitfieldOp::Action(BitfieldSubcmd::Set { enc, offset, value }) => {
                let offset = resolve_bitfield_offset(enc, offset);
                let old = read_bitfield_value(&bitmap, enc, offset)?;
                match prepare_bitfield_write(enc, value as i128, overflow)? {
                    Some(bits) => {
                        ensure_bitmap_len(bitfield_required_len(offset, enc.width())?)?;
                        bitmap.write_bits(offset, enc.width(), bits);
                        touched = true;
                        results.push(Response::Integer(old));
                    }
                    None => results.push(Response::Value(None)),
                }
            }
            ParsedBitfieldOp::Action(BitfieldSubcmd::IncrBy {
                enc,
                offset,
                increment,
            }) => {
                let offset = resolve_bitfield_offset(enc, offset);
                let old = read_bitfield_value(&bitmap, enc, offset)? as i128;
                let next = old + increment as i128;
                match prepare_bitfield_write(enc, next, overflow)? {
                    Some(bits) => {
                        let value = decode_bitfield(enc, bits);
                        ensure_bitmap_len(bitfield_required_len(offset, enc.width())?)?;
                        bitmap.write_bits(offset, enc.width(), bits);
                        touched = true;
                        results.push(Response::Integer(value));
                    }
                    None => results.push(Response::Value(None)),
                }
            }
        }
    }

    if touched && !read_only {
        store_bitmap(store, key, current.is_some(), bitmap)?;
    }
    Ok(Response::Array(Box::new(results)))
}

fn parse_bitfield_subcommands(args: &[Frame<'_>], read_only: bool) -> SenkoResult<ParsedBitfield> {
    let mut index = 0usize;
    let mut out = SmallVec::<[ParsedBitfieldOp; 8]>::new();
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if token.eq_ignore_ascii_case(b"OVERFLOW") {
            if read_only {
                return Err(SenkoError::Protocol(
                    "ERR BITFIELD_RO only supports the GET subcommand",
                ));
            }
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let mode = parse_overflow_mode(arg_bytes(&args[index])?)?;
            out.push(ParsedBitfieldOp::Overflow(mode));
            index += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"GET") {
            if index + 2 >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let enc = parse_bitfield_encoding(arg_bytes(&args[index + 1])?)?;
            let offset = parse_bitfield_offset(arg_bytes(&args[index + 2])?)?;
            out.push(ParsedBitfieldOp::Action(BitfieldSubcmd::Get {
                enc,
                offset,
            }));
            index += 3;
            continue;
        }

        if token.eq_ignore_ascii_case(b"SET") {
            if read_only {
                return Err(SenkoError::Protocol(
                    "ERR BITFIELD_RO only supports the GET subcommand",
                ));
            }
            if index + 3 >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let enc = parse_bitfield_encoding(arg_bytes(&args[index + 1])?)?;
            let offset = parse_bitfield_offset(arg_bytes(&args[index + 2])?)?;
            let value = parse_i64(arg_bytes(&args[index + 3])?)?;
            out.push(ParsedBitfieldOp::Action(BitfieldSubcmd::Set {
                enc,
                offset,
                value,
            }));
            index += 4;
            continue;
        }

        if token.eq_ignore_ascii_case(b"INCRBY") {
            if read_only {
                return Err(SenkoError::Protocol(
                    "ERR BITFIELD_RO only supports the GET subcommand",
                ));
            }
            if index + 3 >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let enc = parse_bitfield_encoding(arg_bytes(&args[index + 1])?)?;
            let offset = parse_bitfield_offset(arg_bytes(&args[index + 2])?)?;
            let increment = parse_i64(arg_bytes(&args[index + 3])?)?;
            out.push(ParsedBitfieldOp::Action(BitfieldSubcmd::IncrBy {
                enc,
                offset,
                increment,
            }));
            index += 4;
            continue;
        }

        return Err(SenkoError::Protocol("syntax error"));
    }
    Ok(ParsedBitfield { ops: out })
}

fn parse_bitop(raw: &[u8]) -> SenkoResult<BitOp> {
    if raw.eq_ignore_ascii_case(b"AND") {
        Ok(BitOp::And)
    } else if raw.eq_ignore_ascii_case(b"OR") {
        Ok(BitOp::Or)
    } else if raw.eq_ignore_ascii_case(b"XOR") {
        Ok(BitOp::Xor)
    } else if raw.eq_ignore_ascii_case(b"NOT") {
        Ok(BitOp::Not)
    } else if raw.eq_ignore_ascii_case(b"DIFF") {
        Ok(BitOp::Diff)
    } else if raw.eq_ignore_ascii_case(b"DIFF1") {
        Ok(BitOp::Diff1)
    } else if raw.eq_ignore_ascii_case(b"ANDOR") {
        Ok(BitOp::AndOr)
    } else if raw.eq_ignore_ascii_case(b"ONE") {
        Ok(BitOp::One)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

fn apply_bitop(op: BitOp, sources: &[Bytes], dest: &mut [u8]) {
    let mut offset = 0usize;
    while offset + 8 <= dest.len() {
        let word = apply_bitop_word(op, sources, offset);
        dest[offset..offset + 8].copy_from_slice(&word.to_ne_bytes());
        offset += 8;
    }

    if offset < dest.len() {
        let word = apply_bitop_word(op, sources, offset);
        let tail = word.to_ne_bytes();
        let tail_len = dest.len() - offset;
        dest[offset..].copy_from_slice(&tail[..tail_len]);
    }
}

fn apply_bitop_word(op: BitOp, sources: &[Bytes], offset: usize) -> u64 {
    let first = padded_word(sources.first().map(Bytes::as_ref).unwrap_or(&[]), offset);
    match op {
        BitOp::And => sources
            .iter()
            .skip(1)
            .fold(first, |acc, src| acc & padded_word(src.as_ref(), offset)),
        BitOp::Or => sources
            .iter()
            .skip(1)
            .fold(first, |acc, src| acc | padded_word(src.as_ref(), offset)),
        BitOp::Xor => sources
            .iter()
            .skip(1)
            .fold(first, |acc, src| acc ^ padded_word(src.as_ref(), offset)),
        BitOp::Not => !first,
        BitOp::Diff => {
            let rest = sources
                .iter()
                .skip(1)
                .fold(0u64, |acc, src| acc | padded_word(src.as_ref(), offset));
            first & !rest
        }
        BitOp::Diff1 => {
            let rest = sources
                .iter()
                .skip(1)
                .fold(0u64, |acc, src| acc | padded_word(src.as_ref(), offset));
            !first & rest
        }
        BitOp::AndOr => {
            let rest = sources
                .iter()
                .skip(1)
                .fold(0u64, |acc, src| acc | padded_word(src.as_ref(), offset));
            first & rest
        }
        BitOp::One => {
            let mut one = 0u64;
            let mut multiple = 0u64;
            for src in sources {
                let word = padded_word(src.as_ref(), offset);
                multiple |= one & word;
                one ^= word;
                one &= !multiple;
            }
            one
        }
    }
}

fn padded_word(data: &[u8], offset: usize) -> u64 {
    if offset >= data.len() {
        return 0;
    }
    let end = (offset + 8).min(data.len());
    let mut buf = [0u8; 8];
    buf[..end - offset].copy_from_slice(&data[offset..end]);
    u64::from_ne_bytes(buf)
}

fn popcount_bytes(data: &[u8]) -> u64 {
    let mut total = 0u64;
    let mut chunks64 = data.chunks_exact(64);
    for chunk in &mut chunks64 {
        let mut lane = 0usize;
        while lane < 64 {
            let word = u64::from_ne_bytes(chunk[lane..lane + 8].try_into().unwrap());
            total += word.count_ones() as u64;
            lane += 8;
        }
    }
    let rem64 = chunks64.remainder();
    let mut chunks8 = rem64.chunks_exact(8);
    for chunk in &mut chunks8 {
        total += u64::from_ne_bytes(chunk.try_into().unwrap()).count_ones() as u64;
    }
    for &byte in chunks8.remainder() {
        total += byte.count_ones() as u64;
    }
    total
}

fn popcount_bit_range(data: &[u8], bit_start: usize, bit_end: usize) -> u64 {
    let start_byte = bit_start / 8;
    let end_byte = bit_end / 8;
    if start_byte == end_byte {
        return masked_byte(data[start_byte], bit_start % 8, bit_end % 8).count_ones() as u64;
    }

    let mut total = masked_byte(data[start_byte], bit_start % 8, 7).count_ones() as u64;
    if end_byte > start_byte + 1 {
        total += popcount_bytes(&data[start_byte + 1..end_byte]);
    }
    total + masked_byte(data[end_byte], 0, bit_end % 8).count_ones() as u64
}

fn masked_byte(byte: u8, start_bit_in_byte: usize, end_bit_in_byte: usize) -> u8 {
    let mut mask = 0u8;
    let mut bit = start_bit_in_byte;
    while bit <= end_bit_in_byte {
        mask |= 1u8 << (7 - bit);
        bit += 1;
    }
    byte & mask
}

fn find_first_in_bit_range(data: &[u8], target: u8, bit_start: usize, bit_end: usize) -> i64 {
    let start_byte = bit_start / 8;
    let end_byte = bit_end / 8;
    if start_byte == end_byte {
        return scan_byte_range(
            data[start_byte],
            target,
            bit_start % 8,
            bit_end % 8,
            bit_start,
        );
    }

    let prefix = masked_byte(data[start_byte], bit_start % 8, 7);
    let prefix_found = scan_byte_range(prefix, target, bit_start % 8, 7, start_byte * 8);
    if prefix_found >= 0 {
        return prefix_found;
    }

    if end_byte > start_byte + 1 {
        let middle = &data[start_byte + 1..end_byte];
        let found = scan_full_bytes(middle, target, (start_byte + 1) * 8);
        if found >= 0 {
            return found;
        }
    }

    let suffix = masked_byte(data[end_byte], 0, bit_end % 8);
    scan_byte_range(suffix, target, 0, bit_end % 8, end_byte * 8)
}

fn scan_full_bytes(data: &[u8], target: u8, base_bit: usize) -> i64 {
    let mut offset = 0usize;
    while offset + 8 <= data.len() {
        let word = u64::from_ne_bytes(data[offset..offset + 8].try_into().unwrap());
        let candidate = if target == 1 { word } else { !word };
        if candidate != 0 {
            let byte_index = (candidate.to_ne_bytes())
                .iter()
                .position(|&byte| byte != 0)
                .unwrap();
            let byte = if target == 1 {
                data[offset + byte_index]
            } else {
                !data[offset + byte_index]
            };
            let bit = byte.leading_zeros() as usize;
            return (base_bit + (offset + byte_index) * 8 + bit) as i64;
        }
        offset += 8;
    }

    while offset < data.len() {
        let found = scan_byte_range(data[offset], target, 0, 7, base_bit + offset * 8);
        if found >= 0 {
            return found;
        }
        offset += 1;
    }
    -1
}

fn scan_byte_range(
    byte: u8,
    target: u8,
    start_bit_in_byte: usize,
    end_bit_in_byte: usize,
    base_bit: usize,
) -> i64 {
    let relevant = masked_byte(byte, start_bit_in_byte, end_bit_in_byte);
    let candidate = if target == 1 { relevant } else { !relevant };
    let mask = masked_byte(0xFF, start_bit_in_byte, end_bit_in_byte);
    let candidate = candidate & mask;
    if candidate == 0 {
        return -1;
    }
    let bit = candidate.leading_zeros() as usize;
    (base_bit + bit) as i64
}

pub fn normalise_range(
    start: i64,
    end: i64,
    len: usize,
    unit: RangeUnit,
) -> Option<(usize, usize)> {
    let len = match unit {
        RangeUnit::Byte => len as i64,
        RangeUnit::Bit => (len.checked_mul(8)?) as i64,
    };
    if len == 0 {
        return None;
    }

    let mut start = if start < 0 { len + start } else { start };
    let mut end = if end < 0 { len + end } else { end };

    if start < 0 {
        start = 0;
    }
    if end < 0 {
        end = 0;
    }
    if start >= len {
        return None;
    }
    if end >= len {
        end = len - 1;
    }
    if start > end {
        return None;
    }
    Some((start as usize, end as usize))
}

fn parse_bitfield_encoding(raw: &[u8]) -> SenkoResult<BitfieldEncoding> {
    if raw.len() < 2 {
        return Err(SenkoError::Protocol("ERR invalid bitfield type"));
    }
    let width =
        parse_u8(&raw[1..]).map_err(|_| SenkoError::Protocol("ERR invalid bitfield type"))?;
    match raw[0] {
        b'u' | b'U' if (1..=63).contains(&width) => Ok(BitfieldEncoding::Unsigned(width)),
        b'i' | b'I' if (1..=64).contains(&width) => Ok(BitfieldEncoding::Signed(width)),
        _ => Err(SenkoError::Protocol("ERR invalid bitfield type")),
    }
}

fn parse_bitfield_offset(raw: &[u8]) -> SenkoResult<BitfieldOffset> {
    if let Some(rest) = raw.strip_prefix(b"#") {
        Ok(BitfieldOffset::Multiplied(parse_u64(rest)?))
    } else {
        Ok(BitfieldOffset::Absolute(parse_u64(raw)?))
    }
}

fn resolve_bitfield_offset(enc: BitfieldEncoding, offset: BitfieldOffset) -> u64 {
    match offset {
        BitfieldOffset::Absolute(value) => value,
        BitfieldOffset::Multiplied(value) => value.saturating_mul(enc.width() as u64),
    }
}

fn parse_overflow_mode(raw: &[u8]) -> SenkoResult<OverflowMode> {
    if raw.eq_ignore_ascii_case(b"WRAP") {
        Ok(OverflowMode::Wrap)
    } else if raw.eq_ignore_ascii_case(b"SAT") {
        Ok(OverflowMode::Sat)
    } else if raw.eq_ignore_ascii_case(b"FAIL") {
        Ok(OverflowMode::Fail)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

fn read_bitfield_value(bitmap: &BitmapBuf, enc: BitfieldEncoding, offset: u64) -> SenkoResult<i64> {
    let end = offset
        .checked_add(enc.width() as u64)
        .ok_or_else(bit_offset_error)?
        .saturating_sub(1);
    if end > MAX_BITMAP_OFFSET {
        return Err(bit_offset_error());
    }
    Ok(decode_bitfield(enc, bitmap.read_bits(offset, enc.width())))
}

fn prepare_bitfield_write(
    enc: BitfieldEncoding,
    value: i128,
    overflow: OverflowMode,
) -> SenkoResult<Option<u64>> {
    let (min, max) = bitfield_range(enc);
    let adjusted = if value < min || value > max {
        match overflow {
            OverflowMode::Wrap => wrap_bitfield(enc, value),
            OverflowMode::Sat => value.clamp(min, max),
            OverflowMode::Fail => return Ok(None),
        }
    } else {
        value
    };
    Ok(Some(encode_bitfield(enc, adjusted)))
}

fn wrap_bitfield(enc: BitfieldEncoding, value: i128) -> i128 {
    let width = enc.width();
    if width == 64 {
        match enc {
            BitfieldEncoding::Signed(_) => (value as i64) as i128,
            BitfieldEncoding::Unsigned(_) => unreachable!(),
        }
    } else {
        let modulo = 1i128 << width;
        let mut wrapped = value % modulo;
        if wrapped < 0 {
            wrapped += modulo;
        }
        match enc {
            BitfieldEncoding::Unsigned(_) => wrapped,
            BitfieldEncoding::Signed(_) => {
                let sign_bit = 1i128 << (width - 1);
                if wrapped >= sign_bit {
                    wrapped - modulo
                } else {
                    wrapped
                }
            }
        }
    }
}

fn encode_bitfield(enc: BitfieldEncoding, value: i128) -> u64 {
    match enc {
        BitfieldEncoding::Unsigned(_) => value as u64,
        BitfieldEncoding::Signed(64) => value as i64 as u64,
        BitfieldEncoding::Signed(width) => {
            let modulo = 1i128 << width;
            let mut wrapped = value % modulo;
            if wrapped < 0 {
                wrapped += modulo;
            }
            wrapped as u64
        }
    }
}

fn decode_bitfield(enc: BitfieldEncoding, raw: u64) -> i64 {
    match enc {
        BitfieldEncoding::Unsigned(_) => raw as i64,
        BitfieldEncoding::Signed(64) => raw as i64,
        BitfieldEncoding::Signed(width) => {
            let shift = 64 - width;
            ((raw << shift) as i64) >> shift
        }
    }
}

fn bitfield_range(enc: BitfieldEncoding) -> (i128, i128) {
    match enc {
        BitfieldEncoding::Unsigned(width) => (0, (1i128 << width) - 1),
        BitfieldEncoding::Signed(64) => (i64::MIN as i128, i64::MAX as i128),
        BitfieldEncoding::Signed(width) => {
            let bound = 1i128 << (width - 1);
            (-bound, bound - 1)
        }
    }
}

fn bitfield_required_len(offset: u64, width: u8) -> SenkoResult<usize> {
    let last_bit = offset
        .checked_add(width as u64)
        .ok_or_else(bit_offset_error)?
        .saturating_sub(1);
    let last_byte = last_bit / 8;
    usize::try_from(last_byte + 1).map_err(|_| size_error())
}

impl BitfieldEncoding {
    fn width(self) -> u8 {
        match self {
            Self::Unsigned(width) | Self::Signed(width) => width,
        }
    }
}

fn store_bitmap(
    store: &mut Store,
    key: CompactString,
    existed: bool,
    bitmap: BitmapBuf,
) -> SenkoResult<()> {
    let _ = store.set(
        key,
        SenkoValue::Raw(bitmap.freeze()),
        SetOptions {
            condition: SetCondition::Always,
            expiry: if existed {
                SetExpiry::KeepTtl
            } else {
                SetExpiry::None
            },
            get_old: false,
        },
    );
    Ok(())
}

fn arg_bytes<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes),
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::WrongType {
            expected: "string",
            actual: frame_type_name(frame),
        }),
    }
}

fn parse_key(raw: &[u8]) -> SenkoResult<CompactString> {
    let key = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("invalid UTF-8 key"))?;
    Ok(CompactString::new(key))
}

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))?;
    text.parse::<i64>()
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))
}

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))?;
    text.parse::<u64>()
        .map_err(|_| SenkoError::Protocol("value is not an integer or out of range"))
}

fn parse_u8(raw: &[u8]) -> Result<u8, ()> {
    let text = std::str::from_utf8(raw).map_err(|_| ())?;
    text.parse::<u8>().map_err(|_| ())
}

fn parse_bit_offset(raw: &[u8]) -> SenkoResult<u64> {
    let offset = parse_u64(raw).map_err(|_| bit_offset_error())?;
    if offset > MAX_BITMAP_OFFSET {
        return Err(bit_offset_error());
    }
    Ok(offset)
}

fn parse_bit_value(raw: &[u8]) -> SenkoResult<u8> {
    match raw {
        b"0" => Ok(0),
        b"1" => Ok(1),
        _ => Err(SenkoError::Protocol(
            "ERR bit is not an integer or out of range",
        )),
    }
}

fn parse_range_unit(raw: &[u8]) -> SenkoResult<RangeUnit> {
    if raw.eq_ignore_ascii_case(b"BYTE") {
        Ok(RangeUnit::Byte)
    } else if raw.eq_ignore_ascii_case(b"BIT") {
        Ok(RangeUnit::Bit)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

fn materialize_value_bytes(value: Option<&SenkoValue>) -> SenkoResult<BytesMut> {
    match value {
        None => Ok(BytesMut::new()),
        Some(SenkoValue::Raw(raw)) => Ok(match raw.clone().try_into_mut() {
            Ok(mut_ref) => mut_ref,
            Err(shared) => {
                let mut out = BytesMut::with_capacity(shared.len().max(1));
                out.extend_from_slice(shared.as_ref());
                out
            }
        }),
        Some(SenkoValue::Int(int)) => {
            let rendered = int.to_string();
            let mut out = BytesMut::with_capacity(rendered.len());
            out.extend_from_slice(rendered.as_bytes());
            Ok(out)
        }
        Some(SenkoValue::Float(float)) => {
            let rendered = float.to_string();
            let mut out = BytesMut::with_capacity(rendered.len());
            out.extend_from_slice(rendered.as_bytes());
            Ok(out)
        }
        Some(SenkoValue::Hash(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "hash",
        }),
        Some(SenkoValue::List(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "list",
        }),
        Some(SenkoValue::Set(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "set",
        }),
        Some(SenkoValue::Stream(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "stream",
        }),
        Some(SenkoValue::ZSet(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "zset",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::BloomFilter(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "MBbloom--",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CuckooFilter(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "cuckooFilter",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CountMinSketch(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "CMSk--",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TopK(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "topk",
        }),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TDigest(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "TDIS-TYPE",
        }),
        #[cfg(feature = "json")]
        Some(SenkoValue::Json(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "json",
        }),
        #[cfg(feature = "vector")]
        Some(SenkoValue::VectorSet(_)) => Err(SenkoError::WrongType {
            expected: "string",
            actual: "vectorset",
        }),
    }
}

fn ensure_bitmap_len(needed_len: usize) -> SenkoResult<()> {
    if needed_len > MAX_STRING_SIZE {
        return Err(size_error());
    }
    Ok(())
}

fn bit_offset_error() -> SenkoError {
    SenkoError::Protocol("ERR bit offset is not an integer or out of range")
}

fn size_error() -> SenkoError {
    SenkoError::Protocol("ERR string exceeds maximum allowed size (512MB)")
}

fn frame_type_name(frame: &Frame<'_>) -> &'static str {
    match frame {
        Frame::SimpleString(_) => "simple-string",
        Frame::SimpleError(_) => "simple-error",
        Frame::Integer(_) => "integer",
        Frame::BulkString(_) => "bulk-string",
        Frame::Array(_) => "array",
        Frame::Null => "null",
        Frame::Boolean(_) => "boolean",
        Frame::Double(_) => "double",
        Frame::BigNumber(_) => "big-number",
        Frame::BlobError(_) => "blob-error",
        Frame::VerbatimString { .. } => "verbatim-string",
        Frame::Map(_) => "map",
        Frame::Set(_) => "set",
        Frame::Push(_) => "push",
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{SenkoError, SenkoValue};
    use senko_proto::Frame;

    use crate::{
        commands::{Response, bitmap},
        store::{SetOptions, Store},
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    fn set_raw(store: &mut Store, key: &str, value: &[u8]) {
        let _ = store.set(
            CompactString::from(key),
            SenkoValue::Raw(Bytes::copy_from_slice(value)),
            SetOptions::default(),
        );
    }

    fn int_of(response: Response) -> i64 {
        match response {
            Response::Integer(value) => value,
            other => panic!("expected integer, got {other:?}"),
        }
    }

    fn bytes_of(response: Response) -> Option<Vec<u8>> {
        match response {
            Response::Value(Some(value)) => Some(value.as_bytes().into_owned()),
            Response::Value(None) => None,
            other => panic!("expected bulk response, got {other:?}"),
        }
    }

    #[test]
    fn getbit_and_setbit_cover_empty_and_growth() {
        let mut store = Store::default();
        assert_eq!(
            int_of(bitmap::getbit(&mut store, &[bs(b"k"), bs(b"7")]).unwrap()),
            0
        );
        assert_eq!(
            int_of(bitmap::setbit(&mut store, &[bs(b"k"), bs(b"9"), bs(b"1")]).unwrap()),
            0
        );
        assert_eq!(
            bytes_of(crate::commands::basic::get(&mut store, &[bs(b"k")]).unwrap()),
            Some(vec![0, 0b0100_0000])
        );
        assert_eq!(
            int_of(bitmap::getbit(&mut store, &[bs(b"k"), bs(b"9")]).unwrap()),
            1
        );
    }

    #[test]
    fn bitcount_handles_byte_and_bit_ranges() {
        let mut store = Store::default();
        set_raw(&mut store, "bits", b"foobar");
        assert_eq!(
            int_of(bitmap::bitcount(&mut store, &[bs(b"bits")]).unwrap()),
            26
        );
        assert_eq!(
            int_of(bitmap::bitcount(&mut store, &[bs(b"bits"), bs(b"1"), bs(b"-2")]).unwrap()),
            18
        );
        assert_eq!(
            int_of(
                bitmap::bitcount(&mut store, &[bs(b"bits"), bs(b"3"), bs(b"14"), bs(b"bit")])
                    .unwrap()
            ),
            7
        );
    }

    #[test]
    fn bitpos_handles_empty_ranges_and_all_ones_semantics() {
        let mut store = Store::default();
        assert_eq!(
            int_of(bitmap::bitpos(&mut store, &[bs(b"missing"), bs(b"0")]).unwrap()),
            0
        );
        assert_eq!(
            int_of(bitmap::bitpos(&mut store, &[bs(b"missing"), bs(b"1")]).unwrap()),
            -1
        );

        set_raw(&mut store, "ones", b"\xff\xff\xff");
        assert_eq!(
            int_of(bitmap::bitpos(&mut store, &[bs(b"ones"), bs(b"0")]).unwrap()),
            24
        );
        assert_eq!(
            int_of(
                bitmap::bitpos(&mut store, &[bs(b"ones"), bs(b"0"), bs(b"0"), bs(b"-1")]).unwrap()
            ),
            -1
        );
    }

    #[test]
    fn bitop_extended_operators_match_expected() {
        let mut store = Store::default();
        set_raw(&mut store, "a", &[0b1111_0000]);
        set_raw(&mut store, "b", &[0b1010_1010]);
        set_raw(&mut store, "c", &[0b0101_0101]);

        assert_eq!(
            int_of(
                bitmap::bitop(&mut store, &[bs(b"AND"), bs(b"d1"), bs(b"a"), bs(b"b")]).unwrap()
            ),
            1
        );
        assert_eq!(
            store.get(b"d1").unwrap().as_bytes().as_ref(),
            &[0b1010_0000]
        );

        assert_eq!(
            int_of(
                bitmap::bitop(
                    &mut store,
                    &[bs(b"DIFF"), bs(b"d2"), bs(b"a"), bs(b"b"), bs(b"c")]
                )
                .unwrap()
            ),
            1
        );
        assert_eq!(store.get(b"d2").unwrap().as_bytes().as_ref(), &[0]);

        assert_eq!(
            int_of(
                bitmap::bitop(
                    &mut store,
                    &[bs(b"ONE"), bs(b"d3"), bs(b"a"), bs(b"b"), bs(b"c")]
                )
                .unwrap()
            ),
            1
        );
        assert_eq!(
            store.get(b"d3").unwrap().as_bytes().as_ref(),
            &[0b0000_1111]
        );
    }

    #[test]
    fn bitfield_supports_get_set_incrby_and_ro_restrictions() {
        let mut store = Store::default();
        let Response::Array(values) = bitmap::bitfield(
            &mut store,
            &[
                bs(b"bf"),
                bs(b"SET"),
                bs(b"u8"),
                bs(b"#0"),
                bs(b"65"),
                bs(b"INCRBY"),
                bs(b"u8"),
                bs(b"#0"),
                bs(b"1"),
                bs(b"GET"),
                bs(b"u8"),
                bs(b"#0"),
            ],
        )
        .unwrap() else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 3);
        assert_eq!(int_of(values[0].clone()), 0);
        assert_eq!(int_of(values[1].clone()), 66);
        assert_eq!(int_of(values[2].clone()), 66);
        assert_eq!(store.get(b"bf").unwrap().as_bytes().as_ref(), b"B");

        let err = bitmap::bitfield_ro(
            &mut store,
            &[bs(b"bf"), bs(b"SET"), bs(b"u8"), bs(b"0"), bs(b"1")],
        )
        .unwrap_err();
        assert!(
            matches!(err, SenkoError::Protocol(message) if message.contains("BITFIELD_RO only supports the GET subcommand"))
        );
    }

    #[test]
    fn normalise_range_covers_negative_indexes() {
        assert_eq!(
            bitmap::normalise_range(1, -2, 6, bitmap::RangeUnit::Byte),
            Some((1, 4))
        );
        assert_eq!(
            bitmap::normalise_range(3, -19, 6, bitmap::RangeUnit::Bit),
            Some((3, 29))
        );
        assert_eq!(
            bitmap::normalise_range(-2, 1, 6, bitmap::RangeUnit::Bit),
            None
        );
    }
}
