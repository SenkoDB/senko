use std::collections::HashSet;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{PelEntry, SenkoError, SenkoResult, SenkoValue, StreamId, StreamObject};
use senko_proto::Frame;
use smallvec::{SmallVec, smallvec};

use crate::{
    commands::Response,
    store::Store,
    stream::{insert_pending, now_ms, remove_pending_entry},
};

const ERR_NO_KEY: &str = "ERR no such key";
const ERR_NO_GROUP: &str = "NOGROUP No such consumer group";

#[inline]
pub fn xclaim(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xclaim' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let group_name = parse_key(arg_bytes(&args[1])?)?;
    let consumer = parse_key(arg_bytes(&args[2])?)?;
    let min_idle_time = parse_u64(arg_bytes(&args[3])?)?;

    let mut index = 4usize;
    let mut ids = Vec::new();
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_claim_option(token) {
            break;
        }
        ids.push(StreamId::parse(token)?);
        index += 1;
    }
    if ids.is_empty() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let mut idle = None;
    let mut time = None;
    let mut retrycount = None;
    let mut force = false;
    let mut justid = false;
    let mut lastid = None;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"IDLE") {
            index += 1;
            if index >= args.len() || time.is_some() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            idle = Some(parse_u64(arg_bytes(&args[index])?)?);
            index += 1;
            continue;
        }
        if is_opt(token, b"TIME") {
            index += 1;
            if index >= args.len() || idle.is_some() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            time = Some(parse_u64(arg_bytes(&args[index])?)?);
            index += 1;
            continue;
        }
        if is_opt(token, b"RETRYCOUNT") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            retrycount = Some(parse_u64(arg_bytes(&args[index])?)?);
            index += 1;
            continue;
        }
        if is_opt(token, b"FORCE") {
            force = true;
            index += 1;
            continue;
        }
        if is_opt(token, b"JUSTID") {
            justid = true;
            index += 1;
            continue;
        }
        if is_opt(token, b"LASTID") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            lastid = Some(StreamId::parse(arg_bytes(&args[index])?)?);
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let now = now_ms();
    let delivery_time = time.unwrap_or_else(|| idle.map_or(now, |ms| now.saturating_sub(ms)));

    let stream = get_stream_mut_or_err(store, key)?;
    let mut group = stream
        .groups
        .remove(group_name.as_str())
        .ok_or(SenkoError::Protocol(ERR_NO_GROUP))?;
    if let Some(lastid) = lastid
        && lastid > group.last_delivered_id
    {
        group.last_delivered_id = lastid;
    }

    let mut seen = HashSet::new();
    let mut claimed = SmallVec::<[Response; 16]>::new();
    for id in ids {
        if !seen.insert(id) {
            continue;
        }
        let Some(response) = claim_one(
            stream,
            &mut group,
            consumer.clone(),
            id,
            min_idle_time,
            delivery_time,
            retrycount,
            force,
            justid,
            now,
        )?
        else {
            continue;
        };
        claimed.push(response);
    }

    stream.groups.insert(group.name.clone(), group);
    Ok(Response::Array(Box::new(claimed)))
}

#[inline]
pub fn xautoclaim(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xautoclaim' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let group_name = parse_key(arg_bytes(&args[1])?)?;
    let consumer = parse_key(arg_bytes(&args[2])?)?;
    let min_idle_time = parse_u64(arg_bytes(&args[3])?)?;
    let start = StreamId::parse(arg_bytes(&args[4])?)?;

    let mut index = 5usize;
    let mut count = usize::MAX;
    let mut justid = false;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"COUNT") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            count = parse_usize(arg_bytes(&args[index])?)?;
            index += 1;
            continue;
        }
        if is_opt(token, b"JUSTID") {
            justid = true;
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let now = now_ms();
    let stream = get_stream_mut_or_err(store, key)?;
    let mut group = stream
        .groups
        .remove(group_name.as_str())
        .ok_or(SenkoError::Protocol(ERR_NO_GROUP))?;

    let mut claimed = SmallVec::<[Response; 16]>::new();
    let mut deleted_ids = SmallVec::<[Response; 16]>::new();
    let mut next_cursor = StreamId::ZERO;

    let ids = group
        .global_pel
        .range(start..=StreamId::MAX)
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();

    for (idx, id) in ids.iter().enumerate() {
        if claimed.len() >= count {
            next_cursor = *id;
            break;
        }
        let Some(owner) = group.global_pel.get(id).cloned() else {
            continue;
        };
        let Some(state) = group.consumers.get(owner.as_str()) else {
            let _ = remove_pending_entry(&mut group, *id);
            continue;
        };
        let Some(entry) = state.pel.get(id) else {
            let _ = remove_pending_entry(&mut group, *id);
            continue;
        };

        let Some(fields) = stream.tree.get(*id) else {
            let _ = remove_pending_entry(&mut group, *id);
            deleted_ids.push(raw_response(id.to_string().as_bytes()));
            continue;
        };

        if now.saturating_sub(entry.delivery_time) < min_idle_time {
            if idx + 1 < ids.len() && claimed.len() >= count {
                next_cursor = ids[idx + 1];
            }
            continue;
        }

        let mut pel_entry = remove_pending_entry(&mut group, *id)
            .ok_or(SenkoError::Protocol("ERR consumer group state corrupt"))?;
        pel_entry.consumer = consumer.clone();
        pel_entry.delivery_time = now;
        pel_entry.delivery_count = pel_entry.delivery_count.saturating_add(1);
        insert_pending(&mut group, consumer.clone(), pel_entry);

        claimed.push(if justid {
            raw_response(id.to_string().as_bytes())
        } else {
            entry_response(*id, fields)
        });
    }

    stream.groups.insert(group.name.clone(), group);
    Ok(Response::Array(Box::new(smallvec![
        raw_response(next_cursor.to_string().as_bytes()),
        Response::Array(Box::new(claimed)),
        Response::Array(Box::new(deleted_ids)),
    ])))
}

#[allow(clippy::too_many_arguments)]
fn claim_one(
    stream: &StreamObject,
    group: &mut senko_core::ConsumerGroup,
    consumer: CompactString,
    id: StreamId,
    min_idle_time: u64,
    delivery_time: u64,
    retrycount: Option<u64>,
    force: bool,
    justid: bool,
    now: u64,
) -> SenkoResult<Option<Response>> {
    let Some(fields) = stream.tree.get(id) else {
        return Ok(None);
    };

    let entry = if group.global_pel.contains_key(&id) {
        let owner = group
            .global_pel
            .get(&id)
            .cloned()
            .ok_or(SenkoError::Protocol("ERR consumer group state corrupt"))?;
        let state = group
            .consumers
            .get(owner.as_str())
            .ok_or(SenkoError::Protocol("ERR consumer group state corrupt"))?;
        let current = state
            .pel
            .get(&id)
            .ok_or(SenkoError::Protocol("ERR consumer group state corrupt"))?;
        if now.saturating_sub(current.delivery_time) < min_idle_time {
            return Ok(None);
        }
        let mut claimed = remove_pending_entry(group, id)
            .ok_or(SenkoError::Protocol("ERR consumer group state corrupt"))?;
        claimed.consumer = consumer.clone();
        claimed.delivery_time = delivery_time;
        claimed.delivery_count =
            retrycount.unwrap_or_else(|| claimed.delivery_count.saturating_add(1));
        claimed
    } else if force {
        PelEntry {
            id,
            consumer: consumer.clone(),
            delivery_time,
            delivery_count: retrycount.unwrap_or(1),
        }
    } else {
        return Ok(None);
    };

    insert_pending(group, consumer, entry);
    Ok(Some(if justid {
        raw_response(id.to_string().as_bytes())
    } else {
        entry_response(id, fields)
    }))
}

fn entry_response(id: StreamId, fields: Vec<(&[u8], &[u8])>) -> Response {
    let mut flat = SmallVec::<[Response; 16]>::new();
    for (field, value) in fields {
        flat.push(raw_response(field));
        flat.push(raw_response(value));
    }
    Response::Array(Box::new(smallvec![
        raw_response(id.to_string().as_bytes()),
        Response::Array(Box::new(flat)),
    ]))
}

fn get_stream_mut_or_err<'a>(
    store: &'a mut Store,
    key: &[u8],
) -> SenkoResult<&'a mut StreamObject> {
    ensure_stream_type_or_missing(store, key)?;
    store
        .get_stream_mut(key)
        .ok_or(SenkoError::Protocol(ERR_NO_KEY))
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

fn is_claim_option(raw: &[u8]) -> bool {
    is_opt(raw, b"IDLE")
        || is_opt(raw, b"TIME")
        || is_opt(raw, b"RETRYCOUNT")
        || is_opt(raw, b"FORCE")
        || is_opt(raw, b"JUSTID")
        || is_opt(raw, b"LASTID")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::stream::{
        basic::{xadd, xdel},
        group::{xgroup, xpending},
        read::xreadgroup,
    };

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn deliver_to(store: &mut Store, consumer: &'static [u8]) {
        let _ = xreadgroup(
            store,
            &[
                bs(b"GROUP"),
                bs(b"g"),
                Frame::BulkString(consumer),
                bs(b"STREAMS"),
                bs(b"s"),
                bs(b">"),
            ],
        )
        .unwrap();
    }

    fn assert_group_sync(store: &mut Store) {
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        let global = group.global_pel.len();
        let per_consumer = group
            .consumers
            .values()
            .map(|consumer| consumer.pel.len())
            .sum::<usize>();
        assert_eq!(global, per_consumer);
        assert_eq!(group.pel_count as usize, global);
    }

    #[test]
    fn xclaim_transfers_ownership_and_increments_delivery_count() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let entry = store
                .get_stream_mut(b"s")
                .unwrap()
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

        let _ = xclaim(
            &mut store,
            &[bs(b"s"), bs(b"g"), bs(b"c2"), bs(b"1"), bs(b"1-0")],
        )
        .unwrap();

        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert!(
            !group
                .consumers
                .get("c1")
                .unwrap()
                .pel
                .contains_key(&StreamId { ms: 1, seq: 0 })
        );
        let entry = group
            .consumers
            .get("c2")
            .unwrap()
            .pel
            .get(&StreamId { ms: 1, seq: 0 })
            .unwrap();
        assert_eq!(entry.delivery_count, 2);
        assert_eq!(group.pel_count, 1);
    }

    #[test]
    fn xclaim_min_idle_time_not_met_skips_entry() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");

        let response = xclaim(
            &mut store,
            &[bs(b"s"), bs(b"g"), bs(b"c2"), bs(b"999999999"), bs(b"1-0")],
        )
        .unwrap();
        assert!(matches!(response, Response::Array(values) if values.is_empty()));
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert!(
            group
                .consumers
                .get("c1")
                .unwrap()
                .pel
                .contains_key(&StreamId { ms: 1, seq: 0 })
        );
    }

    #[test]
    fn xclaim_force_creates_new_pel_entry() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();

        let _ = xclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"0"),
                bs(b"1-0"),
                bs(b"FORCE"),
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
        assert_eq!(group.pel_count, 1);
    }

    #[test]
    fn xclaim_idle_and_retrycount_and_lastid_apply() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let entry = store
                .get_stream_mut(b"s")
                .unwrap()
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
        let before = now_ms();
        let _ = xclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"1"),
                bs(b"1-0"),
                bs(b"IDLE"),
                bs(b"5000"),
                bs(b"RETRYCOUNT"),
                bs(b"7"),
                bs(b"LASTID"),
                bs(b"9-0"),
            ],
        )
        .unwrap();
        let after = now_ms();

        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        let entry = group
            .consumers
            .get("c2")
            .unwrap()
            .pel
            .get(&StreamId { ms: 1, seq: 0 })
            .unwrap();
        assert!(entry.delivery_time <= after.saturating_sub(5000));
        assert!(entry.delivery_time >= before.saturating_sub(5000));
        assert_eq!(entry.delivery_count, 7);
        assert_eq!(group.last_delivered_id, StreamId { ms: 9, seq: 0 });
    }

    #[test]
    fn xclaim_justid_returns_only_ids() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let entry = store
                .get_stream_mut(b"s")
                .unwrap()
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
        let response = xclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"1"),
                bs(b"1-0"),
                bs(b"JUSTID"),
            ],
        )
        .unwrap();
        match response {
            Response::Array(values) => {
                assert_eq!(values.len(), 1);
                assert!(
                    matches!(&values[0], Response::Value(Some(SenkoValue::Raw(id))) if id.as_ref() == b"1-0")
                );
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn xautoclaim_claims_idle_entries_and_returns_cursor() {
        let mut store = Store::default();
        for id in [b"1-0".as_slice(), b"2-0".as_slice()] {
            let _ = xadd(
                &mut store,
                &[bs(b"s"), Frame::BulkString(id), bs(b"f"), bs(b"1")],
            )
            .unwrap();
        }
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let group = store
                .get_stream_mut(b"s")
                .unwrap()
                .groups
                .get_mut("g")
                .unwrap();
            for entry in group.consumers.get_mut("c1").unwrap().pel.values_mut() {
                entry.delivery_time = 0;
            }
        }
        let response = xautoclaim(
            &mut store,
            &[bs(b"s"), bs(b"g"), bs(b"c2"), bs(b"1"), bs(b"0-0")],
        )
        .unwrap();
        match response {
            Response::Array(values) => {
                assert!(
                    matches!(&values[0], Response::Value(Some(SenkoValue::Raw(cursor))) if cursor.as_ref() == b"0-0")
                );
                assert!(matches!(&values[1], Response::Array(entries) if entries.len() == 2));
                assert!(matches!(&values[2], Response::Array(entries) if entries.is_empty()));
            }
            _ => panic!("expected array"),
        }
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert!(group.consumers.get("c1").unwrap().pel.is_empty());
        assert_eq!(group.consumers.get("c2").unwrap().pel.len(), 2);
    }

    #[test]
    fn xautoclaim_count_and_cursor_paginate() {
        let mut store = Store::default();
        for id in [b"1-0".as_slice(), b"2-0".as_slice(), b"3-0".as_slice()] {
            let _ = xadd(
                &mut store,
                &[bs(b"s"), Frame::BulkString(id), bs(b"f"), bs(b"1")],
            )
            .unwrap();
        }
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let group = store
                .get_stream_mut(b"s")
                .unwrap()
                .groups
                .get_mut("g")
                .unwrap();
            for entry in group.consumers.get_mut("c1").unwrap().pel.values_mut() {
                entry.delivery_time = 0;
            }
        }
        let first = xautoclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"1"),
                bs(b"0-0"),
                bs(b"COUNT"),
                bs(b"2"),
            ],
        )
        .unwrap();
        let next = match first {
            Response::Array(values) => match &values[0] {
                Response::Value(Some(SenkoValue::Raw(cursor))) => cursor.clone(),
                _ => panic!("expected cursor"),
            },
            _ => panic!("expected array"),
        };
        assert_eq!(next.as_ref(), b"3-0");

        let second = xautoclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"1"),
                Frame::BulkString(next.as_ref()),
                bs(b"COUNT"),
                bs(b"2"),
            ],
        )
        .unwrap();
        match second {
            Response::Array(values) => {
                assert!(
                    matches!(&values[0], Response::Value(Some(SenkoValue::Raw(cursor))) if cursor.as_ref() == b"0-0")
                );
                assert!(matches!(&values[1], Response::Array(entries) if entries.len() == 1));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn xautoclaim_deleted_entries_are_cleaned_up() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        let _ = xdel(&mut store, &[bs(b"s"), bs(b"1-0")]).unwrap();
        {
            let group = store
                .get_stream_mut(b"s")
                .unwrap()
                .groups
                .get_mut("g")
                .unwrap();
            for entry in group.consumers.get_mut("c1").unwrap().pel.values_mut() {
                entry.delivery_time = 0;
            }
        }

        let response = xautoclaim(
            &mut store,
            &[bs(b"s"), bs(b"g"), bs(b"c2"), bs(b"1"), bs(b"0-0")],
        )
        .unwrap();
        match response {
            Response::Array(values) => {
                assert!(matches!(&values[1], Response::Array(entries) if entries.is_empty()));
                assert!(matches!(&values[2], Response::Array(entries) if entries.len() == 1));
            }
            _ => panic!("expected array"),
        }
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert_eq!(group.pel_count, 0);
        assert!(group.global_pel.is_empty());
    }

    #[test]
    fn xclaim_and_xautoclaim_keep_pel_structures_in_sync() {
        let mut store = Store::default();
        for id in [b"1-0".as_slice(), b"2-0".as_slice(), b"3-0".as_slice()] {
            let _ = xadd(
                &mut store,
                &[bs(b"s"), Frame::BulkString(id), bs(b"f"), bs(b"1")],
            )
            .unwrap();
        }
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        deliver_to(&mut store, b"c1");
        {
            let group = store
                .get_stream_mut(b"s")
                .unwrap()
                .groups
                .get_mut("g")
                .unwrap();
            for entry in group.consumers.get_mut("c1").unwrap().pel.values_mut() {
                entry.delivery_time = 0;
            }
        }
        let _ = xclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c2"),
                bs(b"1"),
                bs(b"1-0"),
                bs(b"2-0"),
            ],
        )
        .unwrap();
        assert_group_sync(&mut store);
        let _ = xautoclaim(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"c3"),
                bs(b"1"),
                bs(b"0-0"),
                bs(b"COUNT"),
                bs(b"10"),
                bs(b"JUSTID"),
            ],
        )
        .unwrap();
        assert_group_sync(&mut store);
        let _ = xpending(&mut store, &[bs(b"s"), bs(b"g")]).unwrap();
    }
}
