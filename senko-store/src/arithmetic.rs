use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};

const DIGIT_ZERO_MASK: u64 = 0x3030_3030_3030_3030;
const DIGIT_NINE_MASK: u64 = 0x3939_3939_3939_3939;

pub fn value_as_i64(value: &SenkoValue) -> SenkoResult<i64> {
    match value {
        SenkoValue::Int(value) => Ok(*value),
        SenkoValue::Raw(raw) => parse_i64_fast(raw.as_ref()).ok_or_else(integer_range_error),
        SenkoValue::Float(_)
        | SenkoValue::Hash(_)
        | SenkoValue::List(_)
        | SenkoValue::Set(_)
        | SenkoValue::Stream(_)
        | SenkoValue::ZSet(_) => Err(integer_range_error()),
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_)
        | SenkoValue::CuckooFilter(_)
        | SenkoValue::CountMinSketch(_)
        | SenkoValue::TopK(_)
        | SenkoValue::TDigest(_) => Err(integer_range_error()),
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => Err(integer_range_error()),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => Err(integer_range_error()),
    }
}

pub fn value_as_f64(value: &SenkoValue) -> SenkoResult<f64> {
    match value {
        SenkoValue::Int(value) => Ok(*value as f64),
        SenkoValue::Float(value) => Ok(*value),
        SenkoValue::Raw(raw) => parse_f64(raw.as_ref()).ok_or_else(float_value_error),
        SenkoValue::Hash(_)
        | SenkoValue::List(_)
        | SenkoValue::Set(_)
        | SenkoValue::Stream(_)
        | SenkoValue::ZSet(_) => Err(float_value_error()),
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_)
        | SenkoValue::CuckooFilter(_)
        | SenkoValue::CountMinSketch(_)
        | SenkoValue::TopK(_)
        | SenkoValue::TDigest(_) => Err(float_value_error()),
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => Err(float_value_error()),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => Err(float_value_error()),
    }
}

pub fn checked_add_i64(base: i64, delta: i64) -> SenkoResult<i64> {
    base.checked_add(delta).ok_or_else(overflow_error)
}

pub fn checked_sub_i64(base: i64, delta: i64) -> SenkoResult<i64> {
    base.checked_sub(delta).ok_or_else(overflow_error)
}

pub fn parse_i64_fast(input: &[u8]) -> Option<i64> {
    parse_i64_swar(input).or_else(|| parse_i64_fallback(input))
}

pub fn parse_f64(input: &[u8]) -> Option<f64> {
    fast_float::parse::<f64, _>(input).ok()
}

pub fn format_f64_no_scientific(value: f64) -> Vec<u8> {
    let mut buffer = ryu::Buffer::new();
    let repr = buffer.format_finite(value);
    let bytes = repr.as_bytes();
    if !bytes.contains(&b'e') && !bytes.contains(&b'E') {
        return bytes.to_vec();
    }

    let exp_pos = bytes
        .iter()
        .position(|byte| *byte == b'e' || *byte == b'E')
        .unwrap_or(bytes.len());
    let mut mantissa = &bytes[..exp_pos];
    let exponent = std::str::from_utf8(&bytes[exp_pos + 1..])
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);

    let negative = mantissa.first().copied() == Some(b'-');
    if negative || mantissa.first().copied() == Some(b'+') {
        mantissa = &mantissa[1..];
    }

    let mut digits = Vec::with_capacity(mantissa.len());
    let mut frac_digits = 0usize;
    let mut seen_dot = false;
    for &byte in mantissa {
        if byte == b'.' {
            seen_dot = true;
            continue;
        }
        digits.push(byte);
        if seen_dot {
            frac_digits += 1;
        }
    }

    if digits.iter().all(|byte| *byte == b'0') {
        return b"0".to_vec();
    }

    let mut out = Vec::with_capacity(digits.len() + 24);
    let decimal_pos = digits.len() as i32 - frac_digits as i32 + exponent;
    if decimal_pos <= 0 {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', (-decimal_pos) as usize));
        out.extend_from_slice(&digits);
    } else if decimal_pos as usize >= digits.len() {
        out.extend_from_slice(&digits);
        out.extend(std::iter::repeat_n(
            b'0',
            decimal_pos as usize - digits.len(),
        ));
    } else {
        let split = decimal_pos as usize;
        out.extend_from_slice(&digits[..split]);
        out.push(b'.');
        out.extend_from_slice(&digits[split..]);
    }

    if let Some(dot) = out.iter().position(|byte| *byte == b'.') {
        while out.last().copied() == Some(b'0') {
            out.pop();
        }
        if out.last().copied() == Some(b'.') && dot + 1 == out.len() {
            out.pop();
        }
    }
    if negative {
        out.insert(0, b'-');
    }
    out
}

pub fn integer_range_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(
        "ERR value is not an integer or out of range",
    ))
}

pub fn overflow_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(
        "ERR increment or decrement would overflow",
    ))
}

pub fn float_value_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new("ERR value is not a valid float"))
}

pub fn float_nan_inf_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(
        "ERR increment would produce NaN or Infinity",
    ))
}

fn parse_i64_swar(input: &[u8]) -> Option<i64> {
    if input.is_empty() {
        return None;
    }
    let (negative, digits) = if input[0] == b'-' {
        (true, &input[1..])
    } else if input[0] == b'+' {
        (false, &input[1..])
    } else {
        (false, input)
    };
    if digits.is_empty() || digits.len() > 19 || !all_digits_swar(digits) {
        return None;
    }
    let mut acc: i128 = 0;
    for &byte in digits {
        let digit = (byte - b'0') as i128;
        acc = acc.checked_mul(10)?.checked_add(digit)?;
    }
    if negative {
        if acc == (i64::MAX as i128) + 1 {
            return Some(i64::MIN);
        }
        Some(-(acc as i64))
    } else {
        (acc <= i64::MAX as i128).then_some(acc as i64)
    }
}

fn all_digits_swar(digits: &[u8]) -> bool {
    let mut idx = 0usize;
    while idx + 8 <= digits.len() {
        let mut block = [0u8; 8];
        block.copy_from_slice(&digits[idx..idx + 8]);
        let word = u64::from_le_bytes(block);
        let below_zero = word.wrapping_sub(DIGIT_ZERO_MASK);
        let above_nine = DIGIT_NINE_MASK.wrapping_sub(word);
        if ((below_zero | above_nine) & 0x8080_8080_8080_8080) != 0 {
            return false;
        }
        idx += 8;
    }
    while idx < digits.len() {
        if !digits[idx].is_ascii_digit() {
            return false;
        }
        idx += 1;
    }
    true
}

fn parse_i64_fallback(input: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(input).ok()?;
    if let Some(rest) = text.strip_prefix('-') {
        let mag = rest.parse::<i64>().ok()?;
        mag.checked_neg()
    } else if let Some(rest) = text.strip_prefix('+') {
        rest.parse::<i64>().ok()
    } else {
        text.parse::<i64>().ok()
    }
}
