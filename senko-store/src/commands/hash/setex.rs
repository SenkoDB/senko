use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;

use crate::{
    commands::Response,
    store::{Store, current_unix_ms},
};

#[derive(Clone, Copy)]
enum FieldSetCondition {
    Always,
    Fnx,
    Fxx,
}

#[derive(Clone, Copy)]
enum ExpirySpec {
    None,
    KeepTtl,
    AtMs(u64),
}

#[inline]
pub fn hsetex(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hsetex' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = current_unix_ms();
    let (cond, expiry, fields, values) = parse_hsetex_args(frames, now_ms)?;

    let hash = store.get_or_create_hash(parse_compact(key));
    let mut written = 0i64;
    let mut scheduled = Vec::<CompactString>::new();
    let mut any_ttl = false;

    for (field_raw, value_raw) in fields.into_iter().zip(values.into_iter()) {
        let field_exists = hash.get(field_raw, now_ms).is_some();
        let allowed = match cond {
            FieldSetCondition::Always => true,
            FieldSetCondition::Fnx => !field_exists,
            FieldSetCondition::Fxx => field_exists,
        };
        if !allowed {
            continue;
        }

        let expires_at = match expiry {
            ExpirySpec::None => None,
            ExpirySpec::AtMs(deadline) => {
                any_ttl = true;
                Some(deadline)
            }
            ExpirySpec::KeepTtl => hash.get(field_raw, now_ms).and_then(|f| f.expires_at),
        };
        if expires_at.is_some() {
            any_ttl = true;
        }
        let field = parse_compact(field_raw);
        let value = SenkoValue::encode_attempt(value_raw);
        let _ = hash.set(field.clone(), value, expires_at);
        if let Some(_) = expires_at {
            if matches!(expiry, ExpirySpec::AtMs(_)) {
                scheduled.push(field);
            }
        }
        written += 1;
    }

    if any_ttl {
        hash.has_field_expiry = true;
    }
    let empty = hash.is_empty(now_ms);
    let key_owned = parse_compact(key);
    let deadline = match expiry {
        ExpirySpec::AtMs(deadline) => Some(deadline),
        _ => None,
    };
    if let Some(deadline) = deadline {
        for field in scheduled {
            store.schedule_hash_field_expiry(key_owned.clone(), field, deadline);
        }
    }
    if written == 0 && empty {
        let _ = store.delete(key);
    }

    Ok(Response::Integer(written))
}

fn parse_hsetex_args<'a>(
    frames: &'a [Frame<'_>],
    now_ms: u64,
) -> SenkoResult<(FieldSetCondition, ExpirySpec, Vec<&'a [u8]>, Vec<&'a [u8]>)> {
    let mut idx = 1usize;
    let mut cond = FieldSetCondition::Always;
    let mut expiry = ExpirySpec::None;
    let mut fields = None;
    let mut values = None;

    while idx < frames.len() {
        let token = arg_bytes(&frames[idx])?;
        if token.eq_ignore_ascii_case(b"FIELDS") {
            let (parsed_fields, parsed_values, next_idx) = parse_fields_value_pairs(frames, idx)?;
            fields = Some(parsed_fields);
            values = Some(parsed_values);
            idx = next_idx;
            continue;
        }
        if token.eq_ignore_ascii_case(b"FNX") || token.eq_ignore_ascii_case(b"FXX") {
            if !matches!(cond, FieldSetCondition::Always) {
                return Err(SenkoError::ProtocolMessage(CompactString::new(
                    "ERR Only one of FXX or FNX arguments can be specified",
                )));
            }
            cond = if token.eq_ignore_ascii_case(b"FNX") {
                FieldSetCondition::Fnx
            } else {
                FieldSetCondition::Fxx
            };
            idx += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"KEEPTTL") {
            if !matches!(expiry, ExpirySpec::None) {
                return Err(expiry_conflict_error());
            }
            expiry = ExpirySpec::KeepTtl;
            idx += 1;
            continue;
        }

        if token.eq_ignore_ascii_case(b"EX")
            || token.eq_ignore_ascii_case(b"PX")
            || token.eq_ignore_ascii_case(b"EXAT")
            || token.eq_ignore_ascii_case(b"PXAT")
        {
            if !matches!(expiry, ExpirySpec::None) {
                return Err(expiry_conflict_error());
            }
            idx += 1;
            if idx >= frames.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let raw = parse_positive_i64(arg_bytes(&frames[idx])?)? as u64;
            let deadline = if token.eq_ignore_ascii_case(b"EX") {
                now_ms.saturating_add(raw.saturating_mul(1_000))
            } else if token.eq_ignore_ascii_case(b"PX") {
                now_ms.saturating_add(raw)
            } else if token.eq_ignore_ascii_case(b"EXAT") {
                raw.saturating_mul(1_000)
            } else {
                raw
            };
            expiry = ExpirySpec::AtMs(deadline);
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    let Some(fields) = fields else {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hsetex' command",
        ));
    };
    let Some(values) = values else {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hsetex' command",
        ));
    };
    Ok((cond, expiry, fields, values))
}

fn parse_fields_value_pairs<'a>(
    frames: &'a [Frame<'_>],
    idx: usize,
) -> SenkoResult<(Vec<&'a [u8]>, Vec<&'a [u8]>, usize)> {
    if idx + 2 > frames.len() {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let numfields = parse_non_zero_usize(arg_bytes(&frames[idx + 1])?)?;
    let payload_start = idx + 2;
    let payload_len = numfields.saturating_mul(2);
    let payload_end = payload_start.saturating_add(payload_len);
    if payload_end > frames.len() {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numfields does not match the number of arguments",
        )));
    }
    let payload = &frames[payload_start..payload_end];
    let mut fields = Vec::with_capacity(numfields);
    let mut values = Vec::with_capacity(numfields);
    for pair in payload.chunks_exact(2) {
        fields.push(arg_bytes(&pair[0])?);
        values.push(arg_bytes(&pair[1])?);
    }
    Ok((fields, values, payload_end))
}

fn parse_non_zero_usize(raw: &[u8]) -> SenkoResult<usize> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|t| t.parse::<usize>().ok())
        .ok_or_else(|| SenkoError::Protocol("syntax error"))?;
    if value == 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR invalid number of fields",
        )));
    }
    Ok(value)
}

fn parse_positive_i64(raw: &[u8]) -> SenkoResult<i64> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|t| t.parse::<i64>().ok())
        .ok_or_else(|| SenkoError::Protocol("syntax error"))?;
    if value <= 0 {
        return Err(SenkoError::Protocol("syntax error"));
    }
    Ok(value)
}

fn expiry_conflict_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(
        "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified",
    ))
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

fn parse_compact(raw: &[u8]) -> CompactString {
    CompactString::from(String::from_utf8_lossy(raw).as_ref())
}

fn ensure_hash_type_or_missing(store: &mut Store, key: &[u8]) -> SenkoResult<()> {
    if let Some(entry) = store.get_mut(key)
        && !matches!(entry.value, SenkoValue::Hash(_))
    {
        return Err(SenkoError::WrongType {
            expected: "hash",
            actual: "string",
        });
    }
    Ok(())
}
