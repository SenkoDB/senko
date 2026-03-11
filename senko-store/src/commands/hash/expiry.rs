use compact_str::CompactString;
use senko_core::{HashField, SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    arithmetic::integer_range_error,
    commands::Response,
    store::{Store, current_unix_ms},
};

#[derive(Clone, Copy)]
pub enum ExpiryCondition {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

pub fn apply_field_expiry(
    field: &mut HashField,
    new_expires_at: u64,
    condition: ExpiryCondition,
    _now_ms: u64,
) -> i64 {
    let allowed = match condition {
        ExpiryCondition::None => true,
        ExpiryCondition::Nx => field.expires_at.is_none(),
        ExpiryCondition::Xx => field.expires_at.is_some(),
        ExpiryCondition::Gt => field
            .expires_at
            .is_some_and(|current| new_expires_at > current),
        ExpiryCondition::Lt => field
            .expires_at
            .is_some_and(|current| new_expires_at < current),
    };
    if !allowed {
        return 0;
    }
    field.expires_at = Some(new_expires_at);
    1
}

#[inline]
pub fn hexpire(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    let now_ms = current_unix_ms();
    let (key, condition, fields, ttl_raw) = parse_expire_args(frames, "hexpire")?;
    let ttl = parse_positive_i64(ttl_raw)? as u64;
    let new_expires_at = now_ms
        .checked_add(ttl.saturating_mul(1_000))
        .ok_or_else(invalid_expire_error)?;
    apply_expiry_to_fields(store, key, fields, condition, new_expires_at, now_ms)
}

#[inline]
pub fn hpexpire(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    let now_ms = current_unix_ms();
    let (key, condition, fields, ttl_raw) = parse_expire_args(frames, "hpexpire")?;
    let ttl = parse_positive_i64(ttl_raw)? as u64;
    let new_expires_at = now_ms.checked_add(ttl).ok_or_else(invalid_expire_error)?;
    apply_expiry_to_fields(store, key, fields, condition, new_expires_at, now_ms)
}

#[inline]
pub fn hexpireat(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    let now_ms = current_unix_ms();
    let (key, condition, fields, at_raw) = parse_expire_args(frames, "hexpireat")?;
    let at_secs = parse_positive_i64(at_raw)? as u64;
    let new_expires_at = at_secs.saturating_mul(1_000);
    if new_expires_at <= now_ms {
        return Err(invalid_expire_error());
    }
    apply_expiry_to_fields(store, key, fields, condition, new_expires_at, now_ms)
}

#[inline]
pub fn hpexpireat(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    let now_ms = current_unix_ms();
    let (key, condition, fields, at_raw) = parse_expire_args(frames, "hpexpireat")?;
    let new_expires_at = parse_positive_i64(at_raw)? as u64;
    if new_expires_at <= now_ms {
        return Err(invalid_expire_error());
    }
    apply_expiry_to_fields(store, key, fields, condition, new_expires_at, now_ms)
}

#[inline]
pub fn httl(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    ttl_like(store, frames, true)
}

#[inline]
pub fn hpttl(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    ttl_like(store, frames, false)
}

#[inline]
pub fn hexpiretime(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    expiretime_like(store, frames, true)
}

#[inline]
pub fn hpexpiretime(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    expiretime_like(store, frames, false)
}

#[inline]
pub fn hpersist(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    let (key, fields) = parse_fields_args(frames, "hpersist")?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = current_unix_ms();
    let mut out = SmallVec::<[Response; 16]>::new();
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        for field in fields {
            match hash.get_mut(field, now_ms) {
                None => out.push(Response::Integer(2)),
                Some(hash_field) => {
                    if hash_field.expires_at.is_some() {
                        hash_field.expires_at = None;
                        out.push(Response::Integer(1));
                    } else {
                        out.push(Response::Integer(-1));
                    }
                }
            }
        }
        hash.has_field_expiry = hash
            .fields
            .iter()
            .any(|(_, field)| field.expires_at.is_some());
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(count_fields(frames, 1)?, Response::Integer(2));
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Array(Box::new(out)))
}

fn ttl_like(store: &mut Store, frames: &[Frame<'_>], seconds: bool) -> SenkoResult<Response> {
    let (key, fields) = parse_fields_args(frames, if seconds { "httl" } else { "hpttl" })?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = current_unix_ms();
    let mut out = SmallVec::<[Response; 16]>::new();
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        for field in fields {
            match hash.get_mut(field, now_ms) {
                None => out.push(Response::Integer(2)),
                Some(hash_field) => match hash_field.expires_at {
                    None => out.push(Response::Integer(-1)),
                    Some(deadline) => {
                        let remaining = deadline.saturating_sub(now_ms);
                        out.push(Response::Integer(if seconds {
                            (remaining / 1_000) as i64
                        } else {
                            remaining as i64
                        }));
                    }
                },
            }
        }
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(count_fields(frames, 1)?, Response::Integer(2));
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Array(Box::new(out)))
}

fn expiretime_like(
    store: &mut Store,
    frames: &[Frame<'_>],
    seconds: bool,
) -> SenkoResult<Response> {
    let (key, fields) = parse_fields_args(
        frames,
        if seconds {
            "hexpiretime"
        } else {
            "hpexpiretime"
        },
    )?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = current_unix_ms();
    let mut out = SmallVec::<[Response; 16]>::new();
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        for field in fields {
            match hash.get_mut(field, now_ms) {
                None => out.push(Response::Integer(2)),
                Some(hash_field) => match hash_field.expires_at {
                    None => out.push(Response::Integer(-2)),
                    Some(deadline) => out.push(Response::Integer(if seconds {
                        (deadline / 1_000) as i64
                    } else {
                        deadline as i64
                    })),
                },
            }
        }
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(count_fields(frames, 1)?, Response::Integer(2));
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Array(Box::new(out)))
}

fn apply_expiry_to_fields(
    store: &mut Store,
    key: &[u8],
    fields: Vec<&[u8]>,
    condition: ExpiryCondition,
    new_expires_at: u64,
    now_ms: u64,
) -> SenkoResult<Response> {
    ensure_hash_type_or_missing(store, key)?;
    let mut out = SmallVec::<[Response; 16]>::new();
    let mut schedule = Vec::<CompactString>::new();
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        let mut any_set = false;
        for field in fields {
            match hash.get_mut(field, now_ms) {
                None => out.push(Response::Integer(2)),
                Some(hash_field) => {
                    let code = apply_field_expiry(hash_field, new_expires_at, condition, now_ms);
                    if code == 1 {
                        any_set = true;
                        schedule.push(parse_compact(field));
                    }
                    out.push(Response::Integer(code));
                }
            }
        }
        if any_set {
            hash.has_field_expiry = true;
        }
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(fields.len(), Response::Integer(2));
    }
    let key_owned = parse_compact(key);
    for field in schedule {
        store.schedule_hash_field_expiry(key_owned.clone(), field, new_expires_at);
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Array(Box::new(out)))
}

#[allow(clippy::type_complexity)]
fn parse_expire_args<'a>(
    frames: &'a [Frame<'_>],
    command: &'static str,
) -> SenkoResult<(&'a [u8], ExpiryCondition, Vec<&'a [u8]>, &'a [u8])> {
    if frames.len() < 4 {
        return Err(wrong_arity(command));
    }
    let key = arg_bytes(&frames[0])?;
    let ttl = arg_bytes(&frames[1])?;
    let _ = std::str::from_utf8(ttl)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(integer_range_error)?;
    let mut idx = 2usize;
    let mut cond = ExpiryCondition::None;
    while idx < frames.len() {
        let token = arg_bytes(&frames[idx])?;
        if token.eq_ignore_ascii_case(b"NX")
            || token.eq_ignore_ascii_case(b"XX")
            || token.eq_ignore_ascii_case(b"GT")
            || token.eq_ignore_ascii_case(b"LT")
        {
            if !matches!(cond, ExpiryCondition::None) {
                return Err(SenkoError::ProtocolMessage(CompactString::new(
                    "ERR Multiple condition flags are not allowed",
                )));
            }
            cond = if token.eq_ignore_ascii_case(b"NX") {
                ExpiryCondition::Nx
            } else if token.eq_ignore_ascii_case(b"XX") {
                ExpiryCondition::Xx
            } else if token.eq_ignore_ascii_case(b"GT") {
                ExpiryCondition::Gt
            } else {
                ExpiryCondition::Lt
            };
            idx += 1;
            continue;
        }
        break;
    }
    let fields = parse_fields_clause(frames, idx)?;
    Ok((key, cond, fields, ttl))
}

fn parse_fields_args<'a>(
    frames: &'a [Frame<'_>],
    command: &'static str,
) -> SenkoResult<(&'a [u8], Vec<&'a [u8]>)> {
    if frames.len() < 4 {
        return Err(wrong_arity(command));
    }
    let key = arg_bytes(&frames[0])?;
    let fields = parse_fields_clause(frames, 1)?;
    Ok((key, fields))
}

fn parse_fields_clause<'a>(frames: &'a [Frame<'_>], idx: usize) -> SenkoResult<Vec<&'a [u8]>> {
    if idx + 2 > frames.len() {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let token = arg_bytes(&frames[idx])?;
    if !token.eq_ignore_ascii_case(b"FIELDS") {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let expected = count_fields(frames, idx)?;
    let actual = frames.len().saturating_sub(idx + 2);
    if expected != actual {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let mut out = Vec::with_capacity(expected);
    for frame in &frames[idx + 2..] {
        out.push(arg_bytes(frame)?);
    }
    Ok(out)
}

fn count_fields(frames: &[Frame<'_>], idx: usize) -> SenkoResult<usize> {
    let num = std::str::from_utf8(arg_bytes(&frames[idx + 1])?)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(integer_range_error)?;
    if num <= 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR invalid number of fields",
        )));
    }
    Ok(num as usize)
}

fn parse_positive_i64(raw: &[u8]) -> SenkoResult<i64> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(integer_range_error)?;
    if value <= 0 {
        return Err(invalid_expire_error());
    }
    Ok(value)
}

fn invalid_expire_error() -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(
        "ERR invalid expire time in 'hexpire' command",
    ))
}

fn wrong_arity(command: &'static str) -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(format!(
        "wrong number of arguments for '{command}' command"
    )))
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
