use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

const OK: &[u8] = b"OK";

#[inline]
pub fn hset(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 3 || frames.len().is_multiple_of(2) {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hset' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let hash = store.get_or_create_hash(parse_compact(key));

    let mut added = 0i64;
    let mut index = 1usize;
    while index < frames.len() {
        let field = parse_compact(arg_bytes(&frames[index])?);
        let value = SenkoValue::encode_attempt(arg_bytes(&frames[index + 1])?);
        if hash.set(field, value, None) {
            added += 1;
        }
        index += 2;
    }
    Ok(Response::Integer(added))
}

#[inline]
pub fn hget(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hget' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    let field = arg_bytes(&frames[1])?;
    ensure_hash_type_or_missing(store, key)?;

    let now_ms = crate::store::current_unix_ms();
    let mut out = None;
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        if let Some(value) = hash.get_mut(field, now_ms) {
            out = Some(to_bulk_value(&value.value));
        }
        remove_key = hash.is_empty(now_ms);
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Value(out))
}

#[inline]
pub fn hdel(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hdel' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;

    let now_ms = crate::store::current_unix_ms();
    let mut removed = 0i64;
    let mut remove_key = false;
    if let Some(hash) = store.get_hash_mut(key) {
        for frame in &frames[1..] {
            let field = arg_bytes(frame)?;
            if hash.get(field, now_ms).is_some() {
                if hash.delete(field) {
                    removed += 1;
                }
            } else {
                let _ = hash.get_mut(field, now_ms);
            }
        }
        remove_key = hash.is_empty(now_ms);
    }
    if remove_key {
        let _ = store.delete(key);
    }
    Ok(Response::Integer(removed))
}

#[inline]
pub fn hexists(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hexists' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    let field = arg_bytes(&frames[1])?;
    ensure_hash_type_or_missing(store, key)?;

    let now_ms = crate::store::current_unix_ms();
    let exists = store
        .get_hash(key)
        .is_some_and(|hash| hash.exists(field, now_ms));
    Ok(Response::Integer(exists as i64))
}

#[inline]
pub fn hlen(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hlen' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();
    let len = store.get_hash(key).map_or(0, |hash| hash.len(now_ms));
    Ok(Response::Integer(len as i64))
}

#[inline]
pub fn hkeys(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hkeys' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();

    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(hash) = store.get_hash(key) {
        out.extend(hash.iter_live(now_ms).map(|(field, _)| {
            Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(
                field.as_bytes(),
            ))))
        }));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn hvals(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hvals' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();

    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(hash) = store.get_hash(key) {
        out.extend(
            hash.iter_live(now_ms)
                .map(|(_, value)| Response::Value(Some(to_bulk_value(&value.value)))),
        );
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn hgetall(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hgetall' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();

    let mut map = SmallVec::<[Response; 32]>::new();
    if let Some(hash) = store.get_hash(key) {
        for (field, value) in hash.iter_live(now_ms) {
            map.push(Response::Value(Some(SenkoValue::Raw(
                Bytes::copy_from_slice(field.as_bytes()),
            ))));
            map.push(Response::Value(Some(to_bulk_value(&value.value))));
        }
    }
    Ok(Response::Map(Box::new(map)))
}

#[inline]
pub fn hmget(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hmget' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();

    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(hash) = store.get_hash(key) {
        for frame in &frames[1..] {
            let field = arg_bytes(frame)?;
            let value = hash
                .get(field, now_ms)
                .map(|field| to_bulk_value(&field.value));
            out.push(Response::Value(value));
        }
    } else {
        out.resize(frames.len() - 1, Response::Value(None));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn hmset(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 3 || frames.len().is_multiple_of(2) {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hmset' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let hash = store.get_or_create_hash(parse_compact(key));

    let mut index = 1usize;
    while index < frames.len() {
        let field = parse_compact(arg_bytes(&frames[index])?);
        let value = SenkoValue::encode_attempt(arg_bytes(&frames[index + 1])?);
        let _ = hash.set(field, value, None);
        index += 2;
    }
    Ok(Response::Simple(OK))
}

#[inline]
pub fn hsetnx(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hsetnx' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let field = arg_bytes(&frames[1])?;
    let value = SenkoValue::encode_attempt(arg_bytes(&frames[2])?);
    let now_ms = crate::store::current_unix_ms();

    let hash = store.get_or_create_hash(parse_compact(key));
    if hash.get_mut(field, now_ms).is_some() {
        return Ok(Response::Integer(0));
    }
    let inserted = hash.set(parse_compact(field), value, None);
    Ok(Response::Integer(inserted as i64))
}

#[inline]
pub fn hstrlen(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hstrlen' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    let field = arg_bytes(&frames[1])?;
    ensure_hash_type_or_missing(store, key)?;
    let now_ms = crate::store::current_unix_ms();
    let len = store
        .get_hash(key)
        .and_then(|hash| hash.get(field, now_ms))
        .map_or(0usize, |field| encoded_len(&field.value));
    Ok(Response::Integer(len as i64))
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

fn encoded_len(value: &SenkoValue) -> usize {
    match value {
        SenkoValue::Raw(raw) => raw.len(),
        SenkoValue::Int(v) => int_len(*v),
        SenkoValue::Float(v) => v.to_string().len(),
        SenkoValue::Hash(_) => 0,
        SenkoValue::List(_) => 0,
        SenkoValue::Set(_) => 0,
        SenkoValue::Stream(_) => 0,
        SenkoValue::ZSet(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => 0,
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => 0,
        #[cfg(feature = "json")]
        SenkoValue::Json(value) => SenkoValue::Json(value.clone()).as_bytes().len(),
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => 0,
    }
}

fn int_len(value: i64) -> usize {
    if value == 0 {
        return 1;
    }
    let negative = value < 0;
    let mut n = value.unsigned_abs();
    let mut digits = 0usize;
    while n > 0 {
        n /= 10;
        digits += 1;
    }
    digits + negative as usize
}
