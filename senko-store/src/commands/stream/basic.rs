use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue, StreamId, StreamRefMode};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

const OK: &[u8] = b"OK";
const XADD_ID_ORDER_ERROR: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";

#[inline]
pub fn xadd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xadd' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key_bytes)?;

    let mut index = 1usize;
    let mut no_mkstream = false;
    let mut ref_mode = StreamRefMode::KeepRef;
    let mut trim = None;

    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"NOMKSTREAM") {
            no_mkstream = true;
            index += 1;
            continue;
        }
        if let Some(parsed_mode) = parse_ref_mode(token) {
            ref_mode = parsed_mode;
            index += 1;
            continue;
        }
        if is_opt(token, b"MAXLEN") || is_opt(token, b"MINID") {
            let parsed = parse_trim(args, &mut index)?;
            trim = Some(parsed);
            continue;
        }
        break;
    }

    if args.len().saturating_sub(index) < 3 || (args.len() - index).is_multiple_of(2) {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xadd' command",
        ));
    }

    if no_mkstream && store.get_stream(key_bytes).is_none() {
        return Ok(Response::Value(None));
    }

    let key = parse_key(key_bytes)?;
    let id = resolve_xadd_id(
        store.get_stream(key_bytes).map(|stream| &stream.tree),
        arg_bytes(&args[index])?,
    )?;
    index += 1;

    let mut owned = Vec::new();
    while index < args.len() {
        owned.push((
            arg_bytes(&args[index])?.to_vec(),
            arg_bytes(&args[index + 1])?.to_vec(),
        ));
        index += 2;
    }
    let fields = owned
        .iter()
        .map(|(field, value)| (field.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();

    let stream = store.get_or_create_stream(key);
    stream.tree.insert_with_mode(id, &fields, ref_mode)?;

    if let Some(trim) = trim {
        apply_trim(&mut stream.tree, trim);
    }

    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        id.to_string().to_string(),
    )))))
}

#[inline]
pub fn xlen(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xlen' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key)?;
    Ok(Response::Integer(
        store
            .get_stream(key)
            .map(|stream| stream.tree.len)
            .unwrap_or(0) as i64,
    ))
}

#[inline]
pub fn xdel(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xdel' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key)?;
    let Some(stream) = store.get_stream_mut(key) else {
        return Ok(Response::Integer(0));
    };

    let mut seen = HashSet::new();
    let mut deleted = 0i64;
    for frame in &args[1..] {
        let id = StreamId::parse(arg_bytes(frame)?)?;
        if seen.insert(id) && stream.tree.delete(id) {
            deleted += 1;
        }
    }
    Ok(Response::Integer(deleted))
}

#[inline]
pub fn xdelex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xdelex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key)?;

    let mut index = 1usize;
    let mut ref_mode = StreamRefMode::KeepRef;
    if let Some(mode) = parse_ref_mode(arg_bytes(&args[index])?) {
        ref_mode = mode;
        index += 1;
    }
    if index >= args.len() || !is_opt(arg_bytes(&args[index])?, b"IDS") {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    index += 1;
    if index >= args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    let numids = parse_usize(arg_bytes(&args[index])?)?;
    index += 1;
    if args.len() - index != numids {
        return Err(SenkoError::Protocol(
            "ERR numids does not match actual number of IDs",
        ));
    }

    let Some(stream) = store.get_stream_mut(key) else {
        return Ok(Response::Integer(0));
    };
    let mut seen = HashSet::new();
    let mut deleted = 0i64;
    for frame in &args[index..] {
        let id = StreamId::parse(arg_bytes(frame)?)?;
        if seen.insert(id) && stream.tree.delete_with_mode(id, ref_mode) {
            deleted += 1;
        }
    }
    Ok(Response::Integer(deleted))
}

#[inline]
pub fn xrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    range_common(store, args, false)
}

#[inline]
pub fn xrevrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    range_common(store, args, true)
}

#[inline]
pub fn xsetid(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xsetid' command",
        ));
    }
    let key_bytes = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key_bytes)?;
    let key = parse_key(key_bytes)?;
    let last_id = StreamId::parse(arg_bytes(&args[1])?)?;
    let stream = store.get_or_create_stream(key);

    if last_id < stream.tree.last_id {
        return Err(SenkoError::Protocol(XADD_ID_ORDER_ERROR));
    }

    let mut index = 2usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        index += 1;
        if is_opt(token, b"ENTRIESADDED") {
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let entries_added = parse_u64(arg_bytes(&args[index])?)?;
            if entries_added < stream.tree.entries_added {
                return Err(SenkoError::Protocol("ERR entries_added must not decrease"));
            }
            stream.tree.entries_added = entries_added;
            index += 1;
            continue;
        }
        if is_opt(token, b"MAXDELETEDID") {
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let max_deleted_id = StreamId::parse(arg_bytes(&args[index])?)?;
            if max_deleted_id < stream.tree.max_deleted_entry_id {
                return Err(SenkoError::Protocol("ERR max_deleted_id must not decrease"));
            }
            stream.tree.max_deleted_entry_id = max_deleted_id;
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    stream.tree.last_id = last_id;
    if stream.tree.first_entry_id == StreamId::ZERO && stream.tree.total_len == 0 {
        stream.tree.first_entry_id = last_id;
    }
    Ok(Response::Simple(OK))
}

#[inline]
pub fn xtrim(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xtrim' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key)?;

    let mut index = 1usize;
    let trim = parse_trim(args, &mut index)?;
    if index < args.len() {
        let _ = parse_ref_mode(arg_bytes(&args[index])?)
            .ok_or(SenkoError::Protocol("ERR syntax error"))?;
        index += 1;
    }
    if index != args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let Some(stream) = store.get_stream_mut(key) else {
        return Ok(Response::Integer(0));
    };
    let before = stream.tree.total_len;
    apply_trim(&mut stream.tree, trim);
    Ok(Response::Integer((before - stream.tree.total_len) as i64))
}

fn range_common(store: &mut Store, args: &[Frame<'_>], reverse: bool) -> SenkoResult<Response> {
    if args.len() < 3 || args.len() > 5 {
        return Err(SenkoError::Protocol(if reverse {
            "wrong number of arguments for 'xrevrange' command"
        } else {
            "wrong number of arguments for 'xrange' command"
        }));
    }
    let key = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key)?;

    let count = if args.len() == 5 {
        if !is_opt(arg_bytes(&args[3])?, b"COUNT") {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        let parsed = parse_usize(arg_bytes(&args[4])?)?;
        if parsed == 0 { None } else { Some(parsed) }
    } else {
        None
    };

    let Some(stream) = store.get_stream(key) else {
        return Ok(Response::Array(Box::default()));
    };

    let entries = if reverse {
        let end = StreamId::parse_range_end(arg_bytes(&args[1])?)?;
        let start = StreamId::parse_range_start(arg_bytes(&args[2])?)?;
        stream.tree.range_rev(end, start, count)
    } else {
        let start = StreamId::parse_range_start(arg_bytes(&args[1])?)?;
        let end = StreamId::parse_range_end(arg_bytes(&args[2])?)?;
        stream.tree.range(start, end, count)
    };

    let mut out = SmallVec::<[Response; 16]>::new();
    for (id, fields) in entries {
        let mut flat = SmallVec::<[Response; 16]>::new();
        for (field, value) in fields {
            flat.push(raw_response(&field));
            flat.push(raw_response(&value));
        }
        out.push(Response::Array(Box::new(SmallVec::from_iter([
            raw_response(id.to_string().as_bytes()),
            Response::Array(Box::new(flat)),
        ]))));
    }
    Ok(Response::Array(Box::new(out)))
}

fn resolve_xadd_id(
    stream: Option<&senko_core::StreamRadixTree>,
    raw: &[u8],
) -> SenkoResult<StreamId> {
    let last_id = stream
        .map(|stream| stream.last_id)
        .unwrap_or(StreamId::ZERO);
    let parsed = StreamId::parse(raw)?;
    let now_ms = current_unix_ms();
    let resolved = if parsed == StreamId::AUTO {
        StreamId::auto_generate(last_id, now_ms)
    } else if parsed.seq == StreamId::PARTIAL_AUTO_SEQ {
        let seq = if parsed.ms > last_id.ms {
            0
        } else if parsed.ms == last_id.ms {
            last_id.seq.saturating_add(1)
        } else {
            return Err(SenkoError::Protocol(XADD_ID_ORDER_ERROR));
        };
        StreamId { ms: parsed.ms, seq }
    } else {
        parsed
    };

    if resolved <= last_id {
        return Err(SenkoError::Protocol(XADD_ID_ORDER_ERROR));
    }
    Ok(resolved)
}

#[derive(Clone, Copy)]
enum TrimSpec {
    MaxLen {
        threshold: u64,
        approx: bool,
        limit: usize,
    },
    MinId {
        threshold: StreamId,
        approx: bool,
        limit: usize,
    },
}

fn parse_trim(args: &[Frame<'_>], index: &mut usize) -> SenkoResult<TrimSpec> {
    let kind = arg_bytes(&args[*index])?;
    *index += 1;
    if *index >= args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let mut approx = false;
    let symbol = arg_bytes(&args[*index])?;
    if symbol == b"=" || symbol == b"~" {
        approx = symbol == b"~";
        *index += 1;
    }
    if *index >= args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let threshold_frame = arg_bytes(&args[*index])?;
    *index += 1;

    let mut limit = 0usize;
    if *index + 1 < args.len() && is_opt(arg_bytes(&args[*index])?, b"LIMIT") {
        if !approx {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        *index += 1;
        limit = parse_usize(arg_bytes(&args[*index])?)?;
        *index += 1;
    }

    if is_opt(kind, b"MAXLEN") {
        return Ok(TrimSpec::MaxLen {
            threshold: parse_u64(threshold_frame)?,
            approx,
            limit,
        });
    }
    if is_opt(kind, b"MINID") {
        return Ok(TrimSpec::MinId {
            threshold: StreamId::parse(threshold_frame)?,
            approx,
            limit,
        });
    }
    Err(SenkoError::Protocol("ERR syntax error"))
}

fn apply_trim(tree: &mut senko_core::StreamRadixTree, trim: TrimSpec) {
    match trim {
        TrimSpec::MaxLen {
            threshold,
            approx,
            limit,
        } => tree.trim_by_maxlen(threshold, approx, limit),
        TrimSpec::MinId {
            threshold,
            approx,
            limit,
        } => tree.trim_by_minid(threshold, approx, limit),
    }
}

fn ensure_stream_type_or_missing(store: &mut Store, key: &[u8]) -> SenkoResult<()> {
    if let Some(value) = store.get(key).cloned()
        && !matches!(value, SenkoValue::Stream(_))
    {
        return Err(wrong_type(&value));
    }
    Ok(())
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
        expected: "stream",
        actual,
    }
}

fn raw_response(bytes: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(bytes))))
}

fn parse_ref_mode(raw: &[u8]) -> Option<StreamRefMode> {
    if raw.eq_ignore_ascii_case(b"KEEPREF") {
        Some(StreamRefMode::KeepRef)
    } else if raw.eq_ignore_ascii_case(b"DELREF") {
        Some(StreamRefMode::DelRef)
    } else if raw.eq_ignore_ascii_case(b"ACKED") {
        Some(StreamRefMode::Acked)
    } else {
        None
    }
}

fn is_opt(raw: &[u8], expected: &[u8]) -> bool {
    raw.eq_ignore_ascii_case(expected)
}

fn parse_key(raw: &[u8]) -> SenkoResult<CompactString> {
    std::str::from_utf8(raw)
        .map(CompactString::new)
        .map_err(|_| SenkoError::Protocol("invalid UTF-8 key"))
}

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(SenkoError::Protocol("value is out of range"))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SenkoError::Protocol("value is out of range"))
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

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn raw_bytes(response: &Response) -> Option<&[u8]> {
        match response {
            Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.as_ref()),
            _ => None,
        }
    }

    fn flatten_ids(response: Response) -> Vec<Vec<u8>> {
        let Response::Array(entries) = response else {
            panic!("expected array");
        };
        entries
            .into_iter()
            .map(|entry| {
                let Response::Array(parts) = entry else {
                    panic!("expected entry array");
                };
                match &parts[0] {
                    Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                    other => panic!("unexpected id response: {other:?}"),
                }
            })
            .collect()
    }

    #[test]
    fn xadd_auto_ids_are_monotonic() {
        let mut store = Store::default();
        let first = xadd(&mut store, &[bs(b"s"), bs(b"*"), bs(b"f"), bs(b"1")]).unwrap();
        let second = xadd(&mut store, &[bs(b"s"), bs(b"*"), bs(b"f"), bs(b"2")]).unwrap();
        assert!(raw_bytes(&first).unwrap() < raw_bytes(&second).unwrap());
    }

    #[test]
    fn xadd_rejects_explicit_id_below_last() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"10-1"), bs(b"f"), bs(b"1")]).unwrap();
        assert!(matches!(
            xadd(&mut store, &[bs(b"s"), bs(b"10-1"), bs(b"f"), bs(b"2")]),
            Err(SenkoError::Protocol(XADD_ID_ORDER_ERROR))
        ));
    }

    #[test]
    fn xadd_ms_star_auto_increments_sequence() {
        let mut store = Store::default();
        let first = xadd(&mut store, &[bs(b"s"), bs(b"10-*"), bs(b"f"), bs(b"1")]).unwrap();
        let second = xadd(&mut store, &[bs(b"s"), bs(b"10-*"), bs(b"f"), bs(b"2")]).unwrap();
        assert_eq!(raw_bytes(&first), Some(b"10-0".as_slice()));
        assert_eq!(raw_bytes(&second), Some(b"10-1".as_slice()));
    }

    #[test]
    fn xadd_nomkstream_on_missing_key_returns_null() {
        let mut store = Store::default();
        assert_eq!(
            xadd(
                &mut store,
                &[bs(b"s"), bs(b"NOMKSTREAM"), bs(b"*"), bs(b"f"), bs(b"1")]
            )
            .unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn xadd_maxlen_exact_trims_to_threshold() {
        let mut store = Store::default();
        for i in 0..100 {
            let value = i.to_string().into_bytes();
            let frames = [
                Frame::BulkString(b"s"),
                Frame::BulkString(b"MAXLEN"),
                Frame::BulkString(b"50"),
                Frame::BulkString(b"*"),
                Frame::BulkString(b"f"),
                Frame::BulkString(Box::leak(value.into_boxed_slice())),
            ];
            let _ = xadd(&mut store, &frames).unwrap();
        }
        assert_eq!(
            xlen(&mut store, &[bs(b"s")]).unwrap(),
            Response::Integer(50)
        );
    }

    #[test]
    fn xadd_maxlen_approx_stays_within_macro_node_slack() {
        let mut store = Store::default();
        for i in 0..100 {
            let value = i.to_string().into_bytes();
            let frames = [
                Frame::BulkString(b"s"),
                Frame::BulkString(b"MAXLEN"),
                Frame::BulkString(b"~"),
                Frame::BulkString(b"50"),
                Frame::BulkString(b"*"),
                Frame::BulkString(b"f"),
                Frame::BulkString(Box::leak(value.into_boxed_slice())),
            ];
            let _ = xadd(&mut store, &frames).unwrap();
        }
        let len = match xlen(&mut store, &[bs(b"s")]).unwrap() {
            Response::Integer(value) => value as usize,
            other => panic!("unexpected response: {other:?}"),
        };
        assert!(len <= 150);
    }

    #[test]
    fn xadd_limit_without_approx_is_error() {
        let mut store = Store::default();
        assert!(matches!(
            xadd(
                &mut store,
                &[
                    bs(b"s"),
                    bs(b"MAXLEN"),
                    bs(b"50"),
                    bs(b"LIMIT"),
                    bs(b"1"),
                    bs(b"*"),
                    bs(b"f"),
                    bs(b"1"),
                ]
            ),
            Err(SenkoError::Protocol("ERR syntax error"))
        ));
    }

    #[test]
    fn xdel_missing_ids_return_zero() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        assert_eq!(
            xdel(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"2-0")]).unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn xdelex_numids_mismatch_is_error() {
        let mut store = Store::default();
        assert!(matches!(
            xdelex(&mut store, &[bs(b"s"), bs(b"IDS"), bs(b"2"), bs(b"1-0")]),
            Err(SenkoError::Protocol(
                "ERR numids does not match actual number of IDs"
            ))
        ));
    }

    #[test]
    fn xrange_and_xrevrange_order_and_count() {
        let mut store = Store::default();
        for i in 1..=3 {
            let id = format!("{i}-0");
            let frames = [
                Frame::BulkString(b"s"),
                Frame::BulkString(Box::leak(id.into_bytes().into_boxed_slice())),
                Frame::BulkString(b"f"),
                Frame::BulkString(b"v"),
            ];
            let _ = xadd(&mut store, &frames).unwrap();
        }

        assert_eq!(
            flatten_ids(xrange(&mut store, &[bs(b"s"), bs(b"-"), bs(b"+")]).unwrap()),
            vec![b"1-0".to_vec(), b"2-0".to_vec(), b"3-0".to_vec()]
        );
        assert_eq!(
            flatten_ids(
                xrange(
                    &mut store,
                    &[bs(b"s"), bs(b"-"), bs(b"+"), bs(b"COUNT"), bs(b"2")]
                )
                .unwrap()
            ),
            vec![b"1-0".to_vec(), b"2-0".to_vec()]
        );
        assert_eq!(
            flatten_ids(xrevrange(&mut store, &[bs(b"s"), bs(b"+"), bs(b"-")]).unwrap()),
            vec![b"3-0".to_vec(), b"2-0".to_vec(), b"1-0".to_vec()]
        );
    }

    #[test]
    fn xrange_missing_key_returns_empty_array() {
        let mut store = Store::default();
        assert_eq!(
            xrange(&mut store, &[bs(b"missing"), bs(b"-"), bs(b"+")]).unwrap(),
            Response::Array(Box::default())
        );
    }

    #[test]
    fn xsetid_sets_last_id_and_rejects_lower_values() {
        let mut store = Store::default();
        assert_eq!(
            xsetid(&mut store, &[bs(b"s"), bs(b"10-0")]).unwrap(),
            Response::Simple(OK)
        );
        assert!(matches!(
            xsetid(&mut store, &[bs(b"s"), bs(b"9-0")]),
            Err(SenkoError::Protocol(XADD_ID_ORDER_ERROR))
        ));
    }

    #[test]
    fn xtrim_minid_removes_correct_entries() {
        let mut store = Store::default();
        for i in 1..=5 {
            let id = format!("{i}-0");
            let frames = [
                Frame::BulkString(b"s"),
                Frame::BulkString(Box::leak(id.into_bytes().into_boxed_slice())),
                Frame::BulkString(b"f"),
                Frame::BulkString(b"v"),
            ];
            let _ = xadd(&mut store, &frames).unwrap();
        }
        assert_eq!(
            xtrim(&mut store, &[bs(b"s"), bs(b"MINID"), bs(b"3-0")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            flatten_ids(xrange(&mut store, &[bs(b"s"), bs(b"-"), bs(b"+")]).unwrap()),
            vec![b"3-0".to_vec(), b"4-0".to_vec(), b"5-0".to_vec()]
        );
    }

    #[test]
    fn ref_modes_are_stored_and_retrievable() {
        let mut store = Store::default();
        let _ = xadd(
            &mut store,
            &[bs(b"s"), bs(b"DELREF"), bs(b"1-0"), bs(b"f"), bs(b"1")],
        )
        .unwrap();
        let _ = xadd(
            &mut store,
            &[bs(b"s"), bs(b"ACKED"), bs(b"2-0"), bs(b"f"), bs(b"2")],
        )
        .unwrap();
        let _ = xdelex(
            &mut store,
            &[bs(b"s"), bs(b"ACKED"), bs(b"IDS"), bs(b"1"), bs(b"1-0")],
        )
        .unwrap();

        let stream = store.get_stream(b"s").unwrap();
        assert_eq!(
            stream.entry_ref_mode(StreamId { ms: 1, seq: 0 }),
            Some(StreamRefMode::Acked)
        );
        assert_eq!(
            stream.entry_ref_mode(StreamId { ms: 2, seq: 0 }),
            Some(StreamRefMode::Acked)
        );
    }
}
