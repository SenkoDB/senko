use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{FeroxValue, SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    pattern::glob_match,
    store::{Store, current_unix_ms},
};

#[derive(Debug, Clone)]
pub struct ScanEntry {
    pub field: CompactString,
    pub value: Option<FeroxValue>,
}

#[inline]
pub fn hscan(store: &mut Store, frames: &[Frame<'_>]) -> SenkoResult<Response> {
    if frames.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'hscan' command",
        ));
    }
    let key = arg_bytes(&frames[0])?;
    ensure_hash_type_or_missing(store, key)?;
    let cursor = parse_u64(arg_bytes(&frames[1])?)?;
    let mut idx = 2usize;
    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut novalues = false;

    while idx < frames.len() {
        let token = arg_bytes(&frames[idx])?;
        if token.eq_ignore_ascii_case(b"MATCH") {
            idx += 1;
            if idx >= frames.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            pattern = Some(arg_bytes(&frames[idx])?);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= frames.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            count = parse_usize(arg_bytes(&frames[idx])?)?.max(1);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"NOVALUES") {
            novalues = true;
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    let now_ms = current_unix_ms();
    let (next, entries) = if let Some(hash) = store.get_hash(key) {
        hscan_step(hash, cursor, count, pattern, novalues, now_ms)
    } else {
        (0, Vec::new())
    };

    let mut top = SmallVec::<[Response; 16]>::new();
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        next.to_string().into_bytes(),
    )))));

    let mut values = SmallVec::<[Response; 16]>::new();
    for entry in entries {
        values.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(entry.field.as_bytes()),
        ))));
        if let Some(value) = entry.value {
            values.push(Response::Value(Some(SenkoValue::Raw(
                Bytes::copy_from_slice(value.as_bytes().as_ref()),
            ))));
        }
    }
    top.push(Response::Array(Box::new(values)));
    Ok(Response::Array(Box::new(top)))
}

pub fn hscan_step(
    hash: &senko_core::HashObject,
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
    novalues: bool,
    now_ms: u64,
) -> (u64, Vec<ScanEntry>) {
    if hash.is_empty(now_ms) {
        return (0, Vec::new());
    }

    if hash.is_listpack() {
        if cursor != 0 {
            return (0, Vec::new());
        }
        let entries = hash
            .iter_live(now_ms)
            .filter(|(field, _)| pattern.is_none_or(|p| glob_match(p, field.as_bytes())))
            .map(|(field, value)| ScanEntry {
                field: field.clone(),
                value: (!novalues).then_some(value.value.clone()),
            })
            .collect();
        return (0, entries);
    }

    let buckets = hash.fields.num_buckets().max(1);
    let mut cur = cursor % buckets as u64;
    let mut scanned = 0usize;
    let mut wrapped = false;
    let mut out = Vec::new();

    while scanned < count {
        if let Some((field, value)) = hash.fields.get_bucket(cur as usize)
            && !is_expired(value.expires_at, now_ms)
            && pattern.is_none_or(|p| glob_match(p, field.as_bytes()))
        {
            out.push(ScanEntry {
                field: field.clone(),
                value: (!novalues).then_some(value.value.clone()),
            });
        }
        scanned += 1;
        cur = reverse_binary_next(cur, buckets as u64);
        if cur == 0 {
            wrapped = true;
            break;
        }
    }

    (if wrapped { 0 } else { cur }, out)
}

fn reverse_binary_next(cursor: u64, modulo: u64) -> u64 {
    if modulo <= 1 {
        return 0;
    }
    let bits = modulo.trailing_zeros().max(1);
    let mask = (1u64 << bits) - 1;
    let low = cursor & mask;
    let rev = reverse_low_bits(low, bits);
    let next = rev.wrapping_add(1) & mask;
    reverse_low_bits(next, bits) & mask
}

fn reverse_low_bits(mut value: u64, bits: u32) -> u64 {
    let mut out = 0u64;
    let mut i = 0;
    while i < bits {
        out = (out << 1) | (value & 1);
        value >>= 1;
        i += 1;
    }
    out
}

fn is_expired(expires_at: Option<u64>, now_ms: u64) -> bool {
    expires_at.is_some_and(|deadline| deadline <= now_ms)
}

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("invalid cursor"))?
        .parse::<u64>()
        .map_err(|_| SenkoError::Protocol("invalid cursor"))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("syntax error"))?
        .parse::<usize>()
        .map_err(|_| SenkoError::Protocol("syntax error"))
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
