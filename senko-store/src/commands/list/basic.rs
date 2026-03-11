use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

#[inline]
pub fn lpush(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    push(store, args, Side::Left, false, "lpush")
}

#[inline]
pub fn rpush(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    push(store, args, Side::Right, false, "rpush")
}

#[inline]
pub fn lpushx(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    push(store, args, Side::Left, true, "lpushx")
}

#[inline]
pub fn rpushx(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    push(store, args, Side::Right, true, "rpushx")
}

#[inline]
pub fn lpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    pop(store, args, Side::Left, "lpop")
}

#[inline]
pub fn rpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    pop(store, args, Side::Right, "rpop")
}

#[inline]
pub fn llen(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'llen' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let value = store.get(key).cloned();
    match value {
        None => Ok(Response::Integer(0)),
        Some(SenkoValue::List(list)) => Ok(Response::Integer(list.len() as i64)),
        Some(other) => Err(wrong_type(&other)),
    }
}

enum Side {
    Left,
    Right,
}

fn push(
    store: &mut Store,
    args: &[Frame<'_>],
    side: Side,
    existing_only: bool,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }

    let key_bytes = arg_bytes(&args[0])?;
    let key = parse_key(key_bytes)?;
    let value = store.get(key_bytes).cloned();

    match value {
        None if existing_only => return Ok(Response::Integer(0)),
        Some(other) if !matches!(other, SenkoValue::List(_)) => return Err(wrong_type(&other)),
        _ => {}
    }

    let list = if value.is_none() {
        store.get_or_create_list(key)
    } else {
        store
            .get_list_mut(key_bytes)
            .expect("list must exist after type check")
    };

    for frame in &args[1..] {
        let encoded = SenkoValue::encode_attempt(arg_bytes(frame)?);
        let payload = encoded.as_bytes();
        match side {
            Side::Left => list.push_front(payload.as_ref()),
            Side::Right => list.push_back(payload.as_ref()),
        }
    }

    Ok(Response::Integer(list.len() as i64))
}

fn pop(
    store: &mut Store,
    args: &[Frame<'_>],
    side: Side,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.is_empty() || args.len() > 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }

    let key = arg_bytes(&args[0])?;
    let value = store.get(key).cloned();
    match value {
        None => {
            return Ok(if args.len() == 1 {
                Response::Value(None)
            } else {
                Response::Array(Box::default())
            });
        }
        Some(other) if !matches!(other, SenkoValue::List(_)) => return Err(wrong_type(&other)),
        _ => {}
    }

    if args.len() == 1 {
        let popped = {
            let list = store
                .get_list_mut(key)
                .expect("list must exist after type check");
            match side {
                Side::Left => list.pop_front(),
                Side::Right => list.pop_back(),
            }
        };
        store.remove_list_if_empty(key);
        if popped.is_some() {
            notify_waiters(key);
        }
        return Ok(Response::Value(
            popped.map(|value| SenkoValue::Raw(Bytes::from(value))),
        ));
    }

    let count = parse_count(arg_bytes(&args[1])?, command)?;
    if count == 0 {
        return Ok(Response::Array(Box::default()));
    }

    let mut out = SmallVec::<[Response; 16]>::new();
    {
        let list = store
            .get_list_mut(key)
            .expect("list must exist after type check");
        for _ in 0..count {
            let popped = match side {
                Side::Left => list.pop_front(),
                Side::Right => list.pop_back(),
            };
            let Some(value) = popped else {
                break;
            };
            out.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))));
        }
    }
    store.remove_list_if_empty(key);
    if !out.is_empty() {
        notify_waiters(key);
    }
    Ok(Response::Array(Box::new(out)))
}

fn notify_waiters(_key: &[u8]) {}

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

fn parse_count(raw: &[u8], command: &'static str) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| {
            SenkoError::ProtocolMessage(CompactString::new(format!(
                "ERR value is out of range for '{command}' command"
            )))
        })
}

fn wrong_type(value: &SenkoValue) -> SenkoError {
    let actual = match value {
        SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_) => "string",
        SenkoValue::Hash(_) => "hash",
        SenkoValue::List(_) => "list",
        SenkoValue::Set(_) => "set",
        SenkoValue::Stream(_) => "stream",
        SenkoValue::ZSet(_) => "zset",
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => "MBbloom--",
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => "cuckooFilter",
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => "CMSk--",
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => "topk",
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => "TDIS-TYPE",
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => "json",
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => "vectorset",
    };
    SenkoError::WrongType {
        expected: "list",
        actual,
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
