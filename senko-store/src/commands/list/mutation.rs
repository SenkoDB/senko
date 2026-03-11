use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

#[inline]
pub fn lrem(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lrem' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => return Ok(Response::Integer(0)),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let count = parse_i64(arg_bytes(&args[1])?)?;
    let encoded = SenkoValue::encode_attempt(arg_bytes(&args[2])?);
    let payload = encoded.as_bytes();
    let removed = store
        .get_list_mut(key)
        .expect("list must exist after type check")
        .remove(count, payload.as_ref());
    store.remove_list_if_empty(key);
    if removed > 0 {
        notify_waiters(key);
    }
    Ok(Response::Integer(removed as i64))
}

#[inline]
pub fn ltrim(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'ltrim' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => return Ok(Response::Simple(b"OK")),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let start = parse_i64(arg_bytes(&args[1])?)?;
    let stop = parse_i64(arg_bytes(&args[2])?)?;
    store
        .get_list_mut(key)
        .expect("list must exist after type check")
        .trim(start, stop);
    store.remove_list_if_empty(key);
    Ok(Response::Simple(b"OK"))
}

#[inline]
pub fn lmove(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lmove' command",
        ));
    }

    let source = arg_bytes(&args[0])?;
    let destination = arg_bytes(&args[1])?;
    let from = parse_side(arg_bytes(&args[2])?)?;
    let to = parse_side(arg_bytes(&args[3])?)?;
    move_one(store, source, destination, from, to)
}

#[inline]
pub fn rpoplpush(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'rpoplpush' command",
        ));
    }

    let source = arg_bytes(&args[0])?;
    let destination = arg_bytes(&args[1])?;
    move_one(store, source, destination, Side::Right, Side::Left)
}

#[inline]
pub fn lmpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lmpop' command",
        ));
    }

    let numkeys = parse_usize(arg_bytes(&args[0])?)?;
    if numkeys == 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys should be greater than 0",
        )));
    }
    if args.len() < numkeys + 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys does not match number of keys",
        )));
    }

    let side_index = 1 + numkeys;
    let side = parse_side(arg_bytes(&args[side_index])?)?;
    let mut count = 1usize;
    let mut option_index = side_index + 1;
    if option_index < args.len() {
        let token = arg_bytes(&args[option_index])?;
        option_index += 1;
        if !token.eq_ignore_ascii_case(b"COUNT") || option_index >= args.len() {
            return Err(SenkoError::Protocol("syntax error"));
        }
        count = parse_usize(arg_bytes(&args[option_index])?)?;
        option_index += 1;
    }
    if option_index != args.len() {
        return Err(SenkoError::Protocol("syntax error"));
    }

    for key_frame in &args[1..=numkeys] {
        let key = arg_bytes(key_frame)?;
        match store.get(key) {
            None => continue,
            Some(SenkoValue::List(_)) => {}
            Some(other) => return Err(wrong_type(other)),
        }

        let list = store
            .get_list_mut(key)
            .expect("list must exist after type check");
        let mut values = SmallVec::<[Response; 16]>::new();
        if count > 0 {
            for _ in 0..count {
                let popped = match side {
                    Side::Left => list.pop_front(),
                    Side::Right => list.pop_back(),
                };
                let Some(value) = popped else {
                    break;
                };
                values.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))));
            }
        }

        store.remove_list_if_empty(key);
        if count == 0 || !values.is_empty() {
            notify_waiters(key);
            return Ok(Response::Array(Box::new(smallvec::smallvec![
                Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(key)))),
                Response::Array(Box::new(values)),
            ])));
        }
    }

    Ok(Response::Value(None))
}

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

fn move_one(
    store: &mut Store,
    source: &[u8],
    destination: &[u8],
    from: Side,
    to: Side,
) -> SenkoResult<Response> {
    match store.get(source) {
        None => return Ok(Response::Value(None)),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }
    if source != destination {
        if let Some(value) = store.get(destination)
            && !matches!(value, SenkoValue::List(_))
        {
            return Err(wrong_type(value));
        }
    }

    let moved = if source == destination {
        let list = store
            .get_list_mut(source)
            .expect("list must exist after type check");
        let value = match from {
            Side::Left => list.pop_front(),
            Side::Right => list.pop_back(),
        };
        if let Some(ref value) = value {
            match to {
                Side::Left => list.push_front(value),
                Side::Right => list.push_back(value),
            }
        }
        value
    } else {
        let value = {
            let list = store
                .get_list_mut(source)
                .expect("source list must exist after type check");
            match from {
                Side::Left => list.pop_front(),
                Side::Right => list.pop_back(),
            }
        };
        let Some(value) = value else {
            store.remove_list_if_empty(source);
            return Ok(Response::Value(None));
        };
        let destination_key = parse_key(destination)?;
        let list = store.get_or_create_list(destination_key);
        match to {
            Side::Left => list.push_front(&value),
            Side::Right => list.push_back(&value),
        }
        store.remove_list_if_empty(source);
        Some(value)
    };

    let Some(value) = moved else {
        store.remove_list_if_empty(source);
        return Ok(Response::Value(None));
    };
    if source != destination {
        store.remove_list_if_empty(source);
    }
    notify_waiters(destination);
    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))))
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

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
}

fn parse_side(raw: &[u8]) -> SenkoResult<Side> {
    if raw.eq_ignore_ascii_case(b"LEFT") {
        Ok(Side::Left)
    } else if raw.eq_ignore_ascii_case(b"RIGHT") {
        Ok(Side::Right)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
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
