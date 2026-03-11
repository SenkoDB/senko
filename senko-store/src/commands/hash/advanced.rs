use std::collections::HashSet;

use bytes::Bytes;
use compact_str::CompactString;
use rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom};
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    arithmetic::{
        checked_add_i64, float_nan_inf_error, float_value_error, format_f64_no_scientific,
        integer_range_error, parse_f64, parse_i64_fast, value_as_f64, value_as_i64,
    },
    commands::Response,
    store::{Store, current_unix_ms},
};

#[inline]
pub fn hincrby(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hincrby' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let field_raw = arg_bytes(&frames[1])?;
    let delta = parse_i64_fast(arg_bytes(&frames[2])?).ok_or_else(integer_range_error)?;
    let now_ms = current_unix_ms();

    let hash = store.get_or_create_hash(parse_compact(key));
    let (base, expires_at) = if let Some(current) = hash.get_mut(field_raw, now_ms) {
        (value_as_i64(&current.value)?, current.expires_at)
    } else {
        (0, None)
    };
    let result = checked_add_i64(base, delta)?;
    let _ = hash.set(
        parse_compact(field_raw),
        SenkoValue::Int(result),
        expires_at,
    );
    Ok(Response::Integer(result))
}

#[inline]
pub fn hincrbyfloat(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hincrbyfloat' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let field_raw = arg_bytes(&frames[1])?;
    let increment = parse_f64(arg_bytes(&frames[2])?).ok_or_else(float_value_error)?;
    if !increment.is_finite() {
        return Err(float_nan_inf_error());
    }

    let now_ms = current_unix_ms();
    let hash = store.get_or_create_hash(parse_compact(key));
    let (base, expires_at) = if let Some(current) = hash.get_mut(field_raw, now_ms) {
        (value_as_f64(&current.value)?, current.expires_at)
    } else {
        (0.0, None)
    };
    let result = base + increment;
    if !result.is_finite() {
        return Err(float_nan_inf_error());
    }
    let _ = hash.set(
        parse_compact(field_raw),
        SenkoValue::Float(result),
        expires_at,
    );
    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        format_f64_no_scientific(result),
    )))))
}

#[inline]
pub fn hrandfield(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.is_empty() || frames.len() > 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hrandfield' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = current_unix_ms();

    let (count, with_values) = match frames.len() {
        1 => (None, false),
        2 => (
            Some(parse_i64_fast(arg_bytes(&frames[1])?).ok_or_else(integer_range_error)?),
            false,
        ),
        3 => {
            let count = parse_i64_fast(arg_bytes(&frames[1])?).ok_or_else(integer_range_error)?;
            if !arg_bytes(&frames[2])?.eq_ignore_ascii_case(b"WITHVALUES") {
                return Err(SenkoError::Protocol("syntax error"));
            }
            (Some(count), true)
        }
        _ => unreachable!(),
    };
    if count.is_none() && with_values {
        return Err(SenkoError::Protocol("syntax error"));
    }

    let entries = if let Some(hash) = store.get_hash(key) {
        collect_live_entries(hash, now_ms)
    } else {
        Vec::new()
    };
    if entries.is_empty() {
        return Ok(match count {
            None => Response::Value(None),
            Some(_) => Response::Array(Box::new(SmallVec::new())),
        });
    }

    let seed = store.next_random_seed();
    let mut rng = SmallRng::seed_from_u64(seed);

    if let Some(count) = count {
        if count == 0 {
            return Ok(Response::Array(Box::new(SmallVec::new())));
        }
        let mut out = SmallVec::<[Response; 16]>::new();
        if count > 0 {
            let requested = count as usize;
            let indices = sample_distinct_indices(entries.len(), requested, &mut rng);
            for idx in indices {
                let (field, value) = &entries[idx];
                out.push(bytes_response(field));
                if with_values {
                    out.push(bytes_response(value));
                }
            }
        } else {
            let repeats = count.unsigned_abs() as usize;
            for _ in 0..repeats {
                let idx = rng.gen_range(0..entries.len());
                let (field, value) = &entries[idx];
                out.push(bytes_response(field));
                if with_values {
                    out.push(bytes_response(value));
                }
            }
        }
        return Ok(Response::Array(Box::new(out)));
    }

    let idx = rng.gen_range(0..entries.len());
    Ok(Response::Value(Some(SenkoValue::Raw(
        Bytes::copy_from_slice(&entries[idx].0),
    ))))
}

#[inline]
pub fn hgetdel(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hgetdel' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let fields = parse_fields_clause(frames, 1, "hgetdel")?;
    let now_ms = current_unix_ms();

    let mut out = SmallVec::<[Response; 16]>::new();
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        let mut delete_fields = Vec::new();
        for field in &fields {
            if let Some(value) = hash.get_mut(field, now_ms) {
                out.push(Response::Value(Some(to_bulk_value(&value.value))));
                delete_fields.push(parse_compact(field));
            } else {
                out.push(Response::Value(None));
            }
        }
        for field in delete_fields {
            let _ = hash.delete(field.as_bytes());
        }
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(fields.len(), Response::Value(None));
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Array(Box::new(out)))
}

#[derive(Clone, Copy)]
enum FieldExpiryOp {
    Keep,
    Set(u64),
    Persist,
}

#[inline]
pub fn hgetex(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hgetex' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;

    let now_ms = current_unix_ms();
    let (op, fields) = parse_hgetex_args(frames, now_ms)?;
    let field_count = fields.len();
    let mut out = SmallVec::<[Response; 16]>::new();
    let mut remove_key = false;
    let mut scheduled = Vec::<(CompactString, u64)>::new();
    if let Some(hash) = store.get_hash_mut(key) {
        for field in fields {
            if let Some(value) = hash.get_mut(field, now_ms) {
                out.push(Response::Value(Some(to_bulk_value(&value.value))));
                match op {
                    FieldExpiryOp::Keep => {}
                    FieldExpiryOp::Persist => value.expires_at = None,
                    FieldExpiryOp::Set(deadline) => {
                        value.expires_at = Some(deadline);
                        scheduled.push((parse_compact(field), deadline));
                    }
                }
            } else {
                out.push(Response::Value(None));
            }
        }
        hash.has_field_expiry = hash
            .fields
            .iter()
            .any(|(_, field)| field.expires_at.is_some());
        remove_key = hash.is_empty(now_ms);
    } else {
        out.resize(field_count, Response::Value(None));
    }
    if remove_key {
        let _ = store.delete(key);
    }
    let key_owned = parse_compact(key);
    for (field, deadline) in scheduled {
        store.schedule_hash_field_expiry(key_owned.clone(), field, deadline);
    }
    Ok(Response::Array(Box::new(out)))
}

fn sample_distinct_indices(len: usize, requested: usize, rng: &mut SmallRng) -> Vec<usize> {
    if requested >= len {
        let mut idx: Vec<usize> = (0..len).collect();
        idx.shuffle(rng);
        return idx;
    }
    if requested < (len / 4).max(1) {
        let mut set = HashSet::with_capacity(requested * 2);
        while set.len() < requested {
            set.insert(rng.gen_range(0..len));
        }
        return set.into_iter().collect();
    }
    let mut idx: Vec<usize> = (0..len).collect();
    idx.shuffle(rng);
    idx.truncate(requested);
    idx
}

fn collect_live_entries(hash: &senko_core::HashObject, now_ms: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    if hash.is_listpack() {
        hash.iter_live(now_ms)
            .map(|(field, value)| {
                (
                    field.as_bytes().to_vec(),
                    value.value.as_bytes().as_ref().to_vec(),
                )
            })
            .collect()
    } else {
        hash.iter_live(now_ms)
            .map(|(field, value)| {
                (
                    field.as_bytes().to_vec(),
                    value.value.as_bytes().as_ref().to_vec(),
                )
            })
            .collect()
    }
}

fn bytes_response(bytes: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(bytes))))
}

fn parse_fields_clause<'a>(
    frames: &'a [Frame<'_>],
    start_idx: usize,
    command: &'static str,
) -> SenkoResult<Vec<&'a [u8]>> {
    if start_idx + 2 > frames.len() {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }
    let token = arg_bytes(&frames[start_idx])?;
    if !token.eq_ignore_ascii_case(b"FIELDS") {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let expected = parse_field_count(arg_bytes(&frames[start_idx + 1])?)?;
    let actual = frames.len().saturating_sub(start_idx + 2);
    if expected != actual {
        return Err(numfields_mismatch_error(command));
    }
    let mut out = Vec::with_capacity(expected);
    for frame in &frames[start_idx + 2..] {
        out.push(arg_bytes(frame)?);
    }
    Ok(out)
}

fn parse_hgetex_args<'a>(
    frames: &'a [Frame<'_>],
    now_ms: u64,
) -> SenkoResult<(FieldExpiryOp, Vec<&'a [u8]>)> {
    let mut idx = 1usize;
    let mut op = FieldExpiryOp::Keep;
    let mut expiry_seen = false;
    let mut fields = None;

    while idx < frames.len() {
        let token = arg_bytes(&frames[idx])?;
        if token.eq_ignore_ascii_case(b"FIELDS") {
            fields = Some(parse_fields_segment(frames, idx, "hgetex")?);
            idx = fields.as_ref().map_or(idx + 1, |(_, end)| *end);
            continue;
        }
        if token.eq_ignore_ascii_case(b"PERSIST") {
            if expiry_seen {
                return Err(SenkoError::Protocol("syntax error"));
            }
            expiry_seen = true;
            op = FieldExpiryOp::Persist;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"EX")
            || token.eq_ignore_ascii_case(b"PX")
            || token.eq_ignore_ascii_case(b"EXAT")
            || token.eq_ignore_ascii_case(b"PXAT")
        {
            if expiry_seen {
                return Err(SenkoError::Protocol("syntax error"));
            }
            idx += 1;
            if idx >= frames.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let ttl = parse_positive_expiry_i64(arg_bytes(&frames[idx])?)? as u64;
            op = if token.eq_ignore_ascii_case(b"EX") {
                FieldExpiryOp::Set(now_ms.saturating_add(ttl.saturating_mul(1_000)))
            } else if token.eq_ignore_ascii_case(b"PX") {
                FieldExpiryOp::Set(now_ms.saturating_add(ttl))
            } else if token.eq_ignore_ascii_case(b"EXAT") {
                FieldExpiryOp::Set(ttl.saturating_mul(1_000))
            } else {
                FieldExpiryOp::Set(ttl)
            };
            expiry_seen = true;
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    let Some((fields, _)) = fields else {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hgetex' command",
        ));
    };
    Ok((op, fields))
}

fn parse_fields_segment<'a>(
    frames: &'a [Frame<'_>],
    start_idx: usize,
    command: &'static str,
) -> SenkoResult<(Vec<&'a [u8]>, usize)> {
    if start_idx + 2 > frames.len() {
        return Err(SenkoError::Protocol("syntax error"));
    }
    let expected = parse_field_count(arg_bytes(&frames[start_idx + 1])?)?;
    let payload_start = start_idx + 2;
    let payload_end = payload_start.saturating_add(expected);
    if payload_end > frames.len() {
        return Err(numfields_mismatch_error(command));
    }
    let mut out = Vec::with_capacity(expected);
    for frame in &frames[payload_start..payload_end] {
        out.push(arg_bytes(frame)?);
    }
    Ok((out, payload_end))
}

fn parse_field_count(raw: &[u8]) -> SenkoResult<usize> {
    let count = parse_i64_fast(raw).ok_or_else(integer_range_error)?;
    if count <= 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR invalid number of fields",
        )));
    }
    Ok(count as usize)
}

fn parse_positive_expiry_i64(raw: &[u8]) -> SenkoResult<i64> {
    let ttl = parse_i64_fast(raw).ok_or_else(integer_range_error)?;
    if ttl <= 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR invalid expire time in 'hgetex' command",
        )));
    }
    Ok(ttl)
}

fn numfields_mismatch_error(command: &'static str) -> SenkoError {
    SenkoError::ProtocolMessage(CompactString::new(format!(
        "ERR numfields does not match the number of arguments for '{command}' command"
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

fn to_bulk_value(value: &SenkoValue) -> SenkoValue {
    SenkoValue::Raw(Bytes::copy_from_slice(value.as_bytes().as_ref()))
}
