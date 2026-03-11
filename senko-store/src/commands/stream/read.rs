use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{ConsumerGroup, SenkoError, SenkoResult, SenkoValue, StreamId, StreamObject};
use senko_proto::Frame;
use smallvec::{SmallVec, smallvec};

use crate::{
    commands::Response,
    commands::list::blocking::BlockingResponseKind,
    store::Store,
    stream::{add_pending_entry, now_ms},
};

const UNBALANCED_XREAD: &str =
    "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified";
const ERR_NO_GROUP: &str = "NOGROUP No such consumer group";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XReadBlockSpec {
    pub keys: SmallVec<[CompactString; 4]>,
    pub streams: SmallVec<[(CompactString, StreamId); 4]>,
    pub timeout: Option<Duration>,
    pub count: Option<usize>,
    pub timeout_response: BlockingResponseKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XReadGroupBlockSpec {
    pub keys: SmallVec<[CompactString; 4]>,
    pub streams: SmallVec<[(CompactString, StreamId); 4]>,
    pub group: CompactString,
    pub consumer: CompactString,
    pub timeout: Option<Duration>,
    pub count: Option<usize>,
    pub noack: bool,
    pub timeout_response: BlockingResponseKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlockingCommandResult {
    Immediate(Response),
    Block(XReadBlockSpec),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupBlockingCommandResult {
    Immediate(Response),
    Block(XReadGroupBlockSpec),
}

#[inline]
pub fn xread(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<BlockingCommandResult> {
    let parsed = parse_xread_args(store, args, false)?;
    let response = xread_now(store, &parsed.streams, parsed.count)?;
    if !parsed.block {
        return Ok(BlockingCommandResult::Immediate(response));
    }
    if !matches!(response, Response::Value(None)) {
        return Ok(BlockingCommandResult::Immediate(response));
    }
    let registered = parse_xread_args(store, args, true)?;

    Ok(BlockingCommandResult::Block(XReadBlockSpec {
        keys: registered.keys,
        streams: registered.streams,
        timeout: registered.timeout,
        count: registered.count,
        timeout_response: BlockingResponseKind::NullBulk,
    }))
}

#[inline]
pub fn xreadgroup(
    store: &mut Store,
    args: &[Frame<'_>],
) -> SenkoResult<GroupBlockingCommandResult> {
    let parsed = parse_xreadgroup_args(store, args)?;
    let response = xreadgroup_now(
        store,
        &parsed.group,
        &parsed.consumer,
        &parsed.streams,
        parsed.count,
        parsed.noack,
        parsed.claim_idle_ms,
    )?;
    if !parsed.block {
        return Ok(GroupBlockingCommandResult::Immediate(response));
    }
    if !matches!(response, Response::Value(None)) {
        return Ok(GroupBlockingCommandResult::Immediate(response));
    }

    Ok(GroupBlockingCommandResult::Block(XReadGroupBlockSpec {
        keys: parsed.keys.clone(),
        streams: parsed.streams,
        group: parsed.group,
        consumer: parsed.consumer,
        timeout: parsed.timeout,
        count: parsed.count,
        noack: parsed.noack,
        timeout_response: BlockingResponseKind::NullBulk,
    }))
}

pub fn xread_now(
    store: &mut Store,
    streams: &[(CompactString, StreamId)],
    count: Option<usize>,
) -> SenkoResult<Response> {
    let mut top = SmallVec::<[Response; 16]>::new();
    for (key, after_id) in streams {
        ensure_stream_type_or_missing(store, key.as_bytes())?;
        let Some(stream) = store.get_stream(key.as_bytes()) else {
            continue;
        };
        let entries = collect_entries(stream, *after_id, count);
        if entries.is_empty() {
            continue;
        }
        top.push(stream_response(key, entries));
    }
    if top.is_empty() {
        Ok(Response::Value(None))
    } else {
        Ok(Response::Array(Box::new(top)))
    }
}

pub fn xreadgroup_now(
    store: &mut Store,
    group_name: &CompactString,
    consumer: &CompactString,
    streams: &[(CompactString, StreamId)],
    count: Option<usize>,
    noack: bool,
    claim_idle_ms: Option<u64>,
) -> SenkoResult<Response> {
    let mut top = SmallVec::<[Response; 16]>::new();
    for (key, requested_id) in streams {
        ensure_stream_type_or_missing(store, key.as_bytes())?;
        let Some(stream) = store.get_stream_mut(key.as_bytes()) else {
            return Err(SenkoError::Protocol("ERR no such key"));
        };
        let Some(mut group) = stream.groups.remove(group_name.as_str()) else {
            return Err(SenkoError::Protocol(ERR_NO_GROUP));
        };

        let entries = if *requested_id == StreamId::MAX {
            deliver_new_entries(stream, &mut group, consumer, count, noack, claim_idle_ms)
        } else {
            redeliver_pending_entries(stream, &mut group, consumer, *requested_id, count)
        };

        let entries = entries?;
        let group_name = group.name.clone();
        stream.groups.insert(group_name, group);
        if entries.is_empty() {
            continue;
        }
        top.push(stream_response(key, entries));
    }
    if top.is_empty() {
        Ok(Response::Value(None))
    } else {
        Ok(Response::Array(Box::new(top)))
    }
}

fn deliver_new_entries(
    stream: &mut StreamObject,
    group: &mut ConsumerGroup,
    consumer: &CompactString,
    count: Option<usize>,
    noack: bool,
    claim_idle_ms: Option<u64>,
) -> SenkoResult<Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>> {
    let now = now_ms();
    let mut out = Vec::new();
    let limit = count.unwrap_or(usize::MAX);

    if let Some(min_idle_ms) = claim_idle_ms {
        let claimable = group
            .global_pel
            .iter()
            .filter_map(|(id, owner)| {
                if owner.as_str() == consumer.as_str() {
                    return None;
                }
                let state = group.consumers.get(owner.as_str())?;
                let entry = state.pel.get(id)?;
                (now.saturating_sub(entry.delivery_time) > min_idle_ms)
                    .then_some((*id, owner.clone()))
            })
            .collect::<Vec<_>>();

        for (id, old_owner) in claimable {
            if out.len() >= limit {
                break;
            }
            let Some(fields) = stream.tree.get(id) else {
                continue;
            };
            let Some(old_state) = group.consumers.get_mut(old_owner.as_str()) else {
                continue;
            };
            let Some(mut pel_entry) = old_state.pel.remove(&id) else {
                continue;
            };
            pel_entry.consumer = consumer.clone();
            pel_entry.delivery_time = now;
            pel_entry.delivery_count = pel_entry.delivery_count.saturating_add(1);
            let entry_count = pel_entry.delivery_count;
            let consumer_name = consumer.clone();
            if !group.consumers.contains_key(consumer.as_str()) {
                group.consumers.insert(
                    consumer.clone(),
                    senko_core::ConsumerState {
                        name: consumer.clone(),
                        seen_time: now,
                        active_time: now,
                        pel: Default::default(),
                    },
                );
            }
            if let Some(new_state) = group.consumers.get_mut(consumer.as_str()) {
                new_state.seen_time = now;
                new_state.active_time = now;
                new_state.pel.insert(id, pel_entry);
            }
            group.global_pel.insert(id, consumer_name);
            out.push((id, materialize_fields(fields)));
            if !noack {
                let _ = entry_count;
            }
        }
    }

    if out.len() >= limit {
        return Ok(out);
    }

    let entries = stream
        .tree
        .range(group.last_delivered_id, StreamId::MAX, None)
        .filter(|(id, _)| *id > group.last_delivered_id)
        .take(limit - out.len())
        .map(|(id, fields)| (id, fields))
        .collect::<Vec<_>>();

    if entries.is_empty() {
        return Ok(out);
    }

    let mut highest = group.last_delivered_id;
    for (id, fields) in entries {
        highest = id;
        let materialized = fields
            .iter()
            .map(|(field, value)| (field.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        if !noack {
            add_pending_entry(group, consumer.clone(), id, now, 1);
        }
        out.push((id, materialized));
    }
    group.last_delivered_id = highest;
    group.entries_read = group.entries_read.saturating_add(out.len() as u64);
    Ok(out)
}

fn redeliver_pending_entries(
    stream: &mut StreamObject,
    group: &mut ConsumerGroup,
    consumer: &CompactString,
    requested_id: StreamId,
    count: Option<usize>,
) -> SenkoResult<Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>> {
    let Some(state) = group.consumers.get_mut(consumer.as_str()) else {
        return Ok(Vec::new());
    };
    let now = now_ms();
    let limit = count.unwrap_or(usize::MAX);
    let ids = state
        .pel
        .range(requested_id..=StreamId::MAX)
        .map(|(id, _)| *id)
        .take(limit)
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for id in ids {
        let Some(entry) = state.pel.get_mut(&id) else {
            continue;
        };
        entry.delivery_time = now;
        entry.delivery_count = entry.delivery_count.saturating_add(1);
        state.seen_time = now;
        state.active_time = now;
        if let Some(fields) = stream.tree.get(id) {
            out.push((id, materialize_fields(fields)));
        }
    }
    Ok(out)
}

fn collect_entries(
    stream: &StreamObject,
    after_id: StreamId,
    count: Option<usize>,
) -> Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)> {
    let limit = count.unwrap_or(usize::MAX);
    stream
        .tree
        .range(after_id, StreamId::MAX, None)
        .filter(|(id, _)| *id > after_id)
        .take(limit)
        .map(|(id, fields)| (id, fields))
        .collect()
}

fn materialize_fields(fields: Vec<(&[u8], &[u8])>) -> Vec<(Vec<u8>, Vec<u8>)> {
    fields
        .into_iter()
        .map(|(field, value)| (field.to_vec(), value.to_vec()))
        .collect()
}

fn stream_response(
    key: &CompactString,
    entries: Vec<(StreamId, Vec<(Vec<u8>, Vec<u8>)>)>,
) -> Response {
    let mut values = SmallVec::<[Response; 16]>::new();
    for (id, fields) in entries {
        let mut flat = SmallVec::<[Response; 16]>::new();
        for (field, value) in fields {
            flat.push(raw_response(&field));
            flat.push(raw_response(&value));
        }
        values.push(Response::Array(Box::new(smallvec![
            raw_response(id.to_string().as_bytes()),
            Response::Array(Box::new(flat)),
        ])));
    }
    Response::Array(Box::new(smallvec![
        raw_response(key.as_bytes()),
        Response::Array(Box::new(values)),
    ]))
}

struct ParsedXRead {
    keys: SmallVec<[CompactString; 4]>,
    streams: SmallVec<[(CompactString, StreamId); 4]>,
    count: Option<usize>,
    timeout: Option<Duration>,
    block: bool,
}

fn parse_xread_args(
    store: &mut Store,
    args: &[Frame<'_>],
    blocking_registration: bool,
) -> SenkoResult<ParsedXRead> {
    let mut index = 0usize;
    let mut count = None;
    let mut timeout = None;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"COUNT") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let parsed = parse_usize(arg_bytes(&args[index])?)?;
            count = (parsed != 0).then_some(parsed);
            index += 1;
            continue;
        }
        if is_opt(token, b"BLOCK") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            timeout = parse_block_milliseconds(arg_bytes(&args[index])?)?;
            index += 1;
            continue;
        }
        break;
    }
    if index >= args.len() || !is_opt(arg_bytes(&args[index])?, b"STREAMS") {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    index += 1;
    if index >= args.len() {
        return Err(SenkoError::Protocol(UNBALANCED_XREAD));
    }

    let remaining = &args[index..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return Err(SenkoError::Protocol(UNBALANCED_XREAD));
    }
    let half = remaining.len() / 2;
    let key_frames = &remaining[..half];
    let id_frames = &remaining[half..];

    let mut keys = SmallVec::<[CompactString; 4]>::new();
    let mut streams = SmallVec::<[(CompactString, StreamId); 4]>::new();
    for (key_frame, id_frame) in key_frames.iter().zip(id_frames.iter()) {
        let key = parse_key(arg_bytes(key_frame)?)?;
        let raw_id = arg_bytes(id_frame)?;
        let resolved = if raw_id == b"$" {
            if blocking_registration {
                store
                    .get_stream(key.as_bytes())
                    .map(|stream| stream.tree.last_id)
                    .unwrap_or(StreamId::ZERO)
            } else {
                StreamId::ZERO
            }
        } else {
            StreamId::parse(raw_id)?
        };
        keys.push(key.clone());
        streams.push((key, resolved));
    }

    Ok(ParsedXRead {
        keys,
        streams,
        count,
        timeout,
        block: timeout.is_some() || args.iter().any(|frame| matches!(frame, Frame::BulkString(bytes) | Frame::SimpleString(bytes) if bytes.eq_ignore_ascii_case(b"BLOCK"))),
    })
}

struct ParsedXReadGroup {
    keys: SmallVec<[CompactString; 4]>,
    streams: SmallVec<[(CompactString, StreamId); 4]>,
    group: CompactString,
    consumer: CompactString,
    count: Option<usize>,
    timeout: Option<Duration>,
    block: bool,
    noack: bool,
    claim_idle_ms: Option<u64>,
}

fn parse_xreadgroup_args(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<ParsedXReadGroup> {
    if args.len() < 5 || !is_opt(arg_bytes(&args[0])?, b"GROUP") {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    let group = parse_key(arg_bytes(&args[1])?)?;
    let consumer = parse_key(arg_bytes(&args[2])?)?;
    let mut index = 3usize;
    let mut count = None;
    let mut timeout = None;
    let mut noack = false;
    let mut claim_idle_ms = None;

    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"COUNT") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let parsed = parse_usize(arg_bytes(&args[index])?)?;
            count = (parsed != 0).then_some(parsed);
            index += 1;
            continue;
        }
        if is_opt(token, b"BLOCK") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            timeout = parse_block_milliseconds(arg_bytes(&args[index])?)?;
            index += 1;
            continue;
        }
        if is_opt(token, b"CLAIM") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            claim_idle_ms = Some(parse_u64(arg_bytes(&args[index])?)?);
            index += 1;
            continue;
        }
        if is_opt(token, b"NOACK") {
            noack = true;
            index += 1;
            continue;
        }
        break;
    }

    if index >= args.len() || !is_opt(arg_bytes(&args[index])?, b"STREAMS") {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    index += 1;
    let remaining = &args[index..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return Err(SenkoError::Protocol(UNBALANCED_XREAD));
    }
    let half = remaining.len() / 2;
    let mut keys = SmallVec::<[CompactString; 4]>::new();
    let mut streams = SmallVec::<[(CompactString, StreamId); 4]>::new();
    for (key_frame, id_frame) in remaining[..half].iter().zip(remaining[half..].iter()) {
        let key = parse_key(arg_bytes(key_frame)?)?;
        let raw_id = arg_bytes(id_frame)?;
        let id = if raw_id == b">" {
            StreamId::MAX
        } else if raw_id == b"$" {
            store
                .get_stream(key.as_bytes())
                .map(|stream| stream.tree.last_id)
                .unwrap_or(StreamId::ZERO)
        } else {
            StreamId::parse(raw_id)?
        };
        keys.push(key.clone());
        streams.push((key, id));
    }

    Ok(ParsedXReadGroup {
        keys,
        streams,
        group,
        consumer,
        count,
        timeout,
        block: timeout.is_some() || args.iter().any(|frame| matches!(frame, Frame::BulkString(bytes) | Frame::SimpleString(bytes) if bytes.eq_ignore_ascii_case(b"BLOCK"))),
        noack,
        claim_idle_ms,
    })
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

fn parse_block_milliseconds(raw: &[u8]) -> SenkoResult<Option<Duration>> {
    let value = parse_u64(raw)?;
    if value == 0 {
        Ok(None)
    } else {
        Ok(Some(Duration::from_millis(value)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::stream::{
        basic::{xadd, xsetid},
        group::xgroup,
    };

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn count_streams(response: &Response) -> usize {
        match response {
            Response::Array(values) => values.len(),
            _ => 0,
        }
    }

    fn response_ids(response: &Response) -> Vec<Vec<Vec<u8>>> {
        let Response::Array(streams) = response else {
            return Vec::new();
        };
        streams
            .iter()
            .filter_map(|stream| {
                let Response::Array(parts) = stream else {
                    return None;
                };
                let Some(Response::Array(entries)) = parts.get(1) else {
                    return None;
                };
                Some(
                    entries
                        .iter()
                        .filter_map(|entry| {
                            let Response::Array(pair) = entry else {
                                return None;
                            };
                            let Some(Response::Value(Some(SenkoValue::Raw(id)))) = pair.first()
                            else {
                                return None;
                            };
                            Some(id.to_vec())
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect()
    }

    #[test]
    fn xread_multiple_streams_filters_ids() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s1"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xadd(&mut store, &[bs(b"s2"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let response = match xread(
            &mut store,
            &[bs(b"STREAMS"), bs(b"s1"), bs(b"s2"), bs(b"0-0"), bs(b"1-0")],
        )
        .unwrap()
        {
            BlockingCommandResult::Immediate(response) => response,
            _ => panic!("expected immediate"),
        };
        assert_eq!(count_streams(&response), 2);
    }

    #[test]
    fn xread_dollar_non_blocking_returns_existing_entries() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let response = match xread(&mut store, &[bs(b"STREAMS"), bs(b"s"), bs(b"$")]).unwrap() {
            BlockingCommandResult::Immediate(response) => response,
            _ => panic!("expected immediate"),
        };
        assert!(matches!(response, Response::Array(_)));
    }

    #[test]
    fn xreadgroup_new_entries_and_noack() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        let response = match xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap()
        {
            GroupBlockingCommandResult::Immediate(response) => response,
            _ => panic!("expected immediate"),
        };
        assert!(matches!(response, Response::Array(_)));
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert_eq!(group.pel_count, 1);

        let _ = xadd(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let _ = xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"NOACK"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap();
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert_eq!(group.pel_count, 1);
    }

    #[test]
    fn xreadgroup_pending_redelivery_increments_delivery_count() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        let _ = xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap();
        let _ = xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"COUNT"),
                bs(b"1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b"1-0"),
            ],
        )
        .unwrap();
        let entry = store
            .get_stream(b"s")
            .unwrap()
            .groups
            .get("g")
            .unwrap()
            .consumers
            .get("c1")
            .unwrap()
            .pel
            .get(&StreamId { ms: 1, seq: 0 })
            .unwrap();
        assert_eq!(entry.delivery_count, 2);
    }

    #[test]
    fn xreadgroup_claim_moves_idle_entries() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        let _ = xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap();
        {
            let stream = store.get_stream_mut(b"s").unwrap();
            let entry = stream
                .groups
                .get_mut("g")
                .unwrap()
                .consumers
                .get_mut("c1")
                .unwrap()
                .pel
                .get_mut(&StreamId { ms: 1, seq: 0 })
                .unwrap();
            entry.delivery_time = 0;
        }
        let _ = xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"CLAIM"),
                bs(b"1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap();
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert!(
            group
                .consumers
                .get("c2")
                .unwrap()
                .pel
                .contains_key(&StreamId { ms: 1, seq: 0 })
        );
    }

    #[test]
    fn xread_block_dollar_resolves_at_registration_time() {
        let mut store = Store::default();
        let _ = xsetid(&mut store, &[bs(b"s"), bs(b"1-0")]).unwrap();
        let block = match xread(
            &mut store,
            &[bs(b"BLOCK"), bs(b"1"), bs(b"STREAMS"), bs(b"s"), bs(b"$")],
        )
        .unwrap()
        {
            BlockingCommandResult::Block(spec) => spec,
            _ => panic!("expected block"),
        };
        assert_eq!(block.streams[0].1, StreamId { ms: 1, seq: 0 });
    }

    #[test]
    fn xreadgroup_block_dollar_resolves_at_registration_time() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        let block = match xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"BLOCK"),
                bs(b"1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b"$"),
            ],
        )
        .unwrap()
        {
            GroupBlockingCommandResult::Block(spec) => spec,
            _ => panic!("expected block"),
        };
        assert_eq!(block.streams[0].1, StreamId { ms: 1, seq: 0 });
    }

    #[test]
    fn xreadgroup_multiple_consumers_receive_distinct_entries() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();

        let first = match xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c1"),
                bs(b"COUNT"),
                bs(b"1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap()
        {
            GroupBlockingCommandResult::Immediate(response) => response,
            _ => panic!("expected immediate"),
        };
        let second = match xreadgroup(
            &mut store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"COUNT"),
                bs(b"1"),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap()
        {
            GroupBlockingCommandResult::Immediate(response) => response,
            _ => panic!("expected immediate"),
        };

        assert_eq!(response_ids(&first), vec![vec![b"1-0".to_vec()]]);
        assert_eq!(response_ids(&second), vec![vec![b"2-0".to_vec()]]);
    }

    #[test]
    fn unbalanced_streams_error_is_exact() {
        let mut store = Store::default();
        assert!(matches!(
            xread(
                &mut store,
                &[bs(b"STREAMS"), bs(b"s1"), bs(b"s2"), bs(b"0-0")]
            ),
            Err(SenkoError::Protocol(UNBALANCED_XREAD))
        ));
    }
}
