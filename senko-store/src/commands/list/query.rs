use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{InsertResult, SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

#[inline]
pub fn lrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lrange' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => return Ok(Response::Array(Box::default())),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let list = store
        .get_list(key)
        .expect("list must exist after type check");
    let start = parse_i64(arg_bytes(&args[1])?)?;
    let stop = parse_i64(arg_bytes(&args[2])?)?;
    let values = list
        .range(start, stop)
        .map(|value| Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value)))))
        .collect::<SmallVec<[Response; 16]>>();
    Ok(Response::Array(Box::new(values)))
}

#[inline]
pub fn lindex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lindex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => return Ok(Response::Value(None)),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let index = parse_i64(arg_bytes(&args[1])?)?;
    let value = store
        .get_list(key)
        .and_then(|list| list.index(index))
        .map(|value| SenkoValue::Raw(Bytes::copy_from_slice(value)));
    Ok(Response::Value(value))
}

#[inline]
pub fn lset(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lset' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => {
            return Err(SenkoError::ProtocolMessage(CompactString::new(
                "ERR no such key",
            )));
        }
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let index = parse_i64(arg_bytes(&args[1])?)?;
    let encoded = SenkoValue::encode_attempt(arg_bytes(&args[2])?);
    let payload = encoded.as_bytes();
    let updated = store
        .get_list_mut(key)
        .expect("list must exist after type check")
        .set_index(index, payload.as_ref());
    if !updated {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR index out of range",
        )));
    }
    Ok(Response::Simple(b"OK"))
}

#[inline]
pub fn linsert(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'linsert' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let before = if arg_bytes(&args[1])?.eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if arg_bytes(&args[1])?.eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Err(SenkoError::Protocol("syntax error"));
    };

    match store.get(key) {
        None => return Ok(Response::Integer(0)),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let pivot = arg_bytes(&args[2])?;
    let encoded = SenkoValue::encode_attempt(arg_bytes(&args[3])?);
    let payload = encoded.as_bytes();
    let list = store
        .get_list_mut(key)
        .expect("list must exist after type check");
    let result = if before {
        list.insert_before(pivot, payload.as_ref())
    } else {
        list.insert_after(pivot, payload.as_ref())
    };
    let response = match result {
        InsertResult::Found => Response::Integer(list.len() as i64),
        InsertResult::NotFound => Response::Integer(-1),
        InsertResult::KeyMissing => Response::Integer(0),
    };
    Ok(response)
}

#[inline]
pub fn lpos(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lpos' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    match store.get(key) {
        None => return Ok(no_match_response(false, 1)),
        Some(SenkoValue::List(_)) => {}
        Some(other) => return Err(wrong_type(other)),
    }

    let needle = arg_bytes(&args[1])?;
    let mut rank = 1i64;
    let mut count = 1usize;
    let mut count_specified = false;
    let mut maxlen = 0usize;

    let mut index = 2usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        index += 1;
        if index >= args.len() {
            return Err(SenkoError::Protocol("syntax error"));
        }
        if token.eq_ignore_ascii_case(b"RANK") {
            rank = parse_i64(arg_bytes(&args[index])?)?;
        } else if token.eq_ignore_ascii_case(b"COUNT") {
            count = parse_usize(arg_bytes(&args[index])?)?;
            count_specified = true;
        } else if token.eq_ignore_ascii_case(b"MAXLEN") {
            maxlen = parse_usize(arg_bytes(&args[index])?)?;
        } else {
            return Err(SenkoError::Protocol("syntax error"));
        }
        index += 1;
    }

    let list = store
        .get_list(key)
        .expect("list must exist after type check");
    let matches = list.pos(needle, rank, count, maxlen);

    if matches.is_empty() {
        return Ok(no_match_response(count_specified, count));
    }

    if !count_specified || count == 1 {
        return Ok(Response::Integer(matches[0]));
    }

    let values = matches
        .into_iter()
        .map(Response::Integer)
        .collect::<SmallVec<[Response; 16]>>();
    Ok(Response::Array(Box::new(values)))
}

fn no_match_response(count_specified: bool, count: usize) -> Response {
    if count_specified {
        let _ = count;
        Response::Array(Box::default())
    } else {
        Response::Value(None)
    }
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
