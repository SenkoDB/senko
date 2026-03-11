use std::collections::HashSet;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{
    ConsumerGroup, SenkoError, SenkoResult, SenkoValue, StreamId, StreamObject, StreamRefMode,
};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    store::Store,
    stream::{
        ConsumerInfo, GroupInfo, PendingDetail, ack_id, consumer_info, create_consumer,
        create_group, delete_consumer, destroy_group, group_info, now_ms, pending_detail,
        pending_summary, set_group_id, xackdel_apply,
    },
};

const OK: &[u8] = b"OK";
const ERR_NO_KEY: &str = "ERR no such key";
const ERR_NO_GROUP: &str = "NOGROUP No such consumer group";

#[inline]
pub fn xgroup(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup' command",
        ));
    }

    let sub = arg_bytes(&args[0])?;
    if is_opt(sub, b"CREATE") {
        return xgroup_create(store, &args[1..]);
    }
    if is_opt(sub, b"CREATECONSUMER") {
        return xgroup_createconsumer(store, &args[1..]);
    }
    if is_opt(sub, b"DELCONSUMER") {
        return xgroup_delconsumer(store, &args[1..]);
    }
    if is_opt(sub, b"DESTROY") {
        return xgroup_destroy(store, &args[1..]);
    }
    if is_opt(sub, b"SETID") {
        return xgroup_setid(store, &args[1..]);
    }
    Err(SenkoError::Protocol("ERR syntax error"))
}

#[inline]
pub fn xinfo(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xinfo' command",
        ));
    }
    let sub = arg_bytes(&args[0])?;
    if is_opt(sub, b"STREAM") {
        return xinfo_stream(store, &args[1..]);
    }
    if is_opt(sub, b"GROUPS") {
        return xinfo_groups(store, &args[1..]);
    }
    if is_opt(sub, b"CONSUMERS") {
        return xinfo_consumers(store, &args[1..]);
    }
    Err(SenkoError::Protocol("ERR syntax error"))
}

#[inline]
pub fn xpending(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xpending' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let stream = get_stream_or_err(store, key)?;
    let group = get_group_or_err(stream, group_name)?;

    if args.len() == 2 {
        let summary = pending_summary(group);
        let mut consumers = SmallVec::<[Response; 16]>::new();
        for (name, count) in summary.per_consumer {
            consumers.push(Response::Array(Box::new(SmallVec::from_iter([
                raw_response(name.as_bytes()),
                Response::Integer(count as i64),
            ]))));
        }
        return Ok(Response::Array(Box::new(SmallVec::from_iter([
            Response::Integer(summary.pel_count as i64),
            id_or_null(summary.min_id),
            id_or_null(summary.max_id),
            Response::Array(Box::new(consumers)),
        ]))));
    }

    let mut index = 2usize;
    let mut min_idle_ms = None;
    if index + 1 < args.len() && is_opt(arg_bytes(&args[index])?, b"IDLE") {
        index += 1;
        min_idle_ms = Some(parse_u64(arg_bytes(&args[index])?)?);
        index += 1;
    }
    if args.len() < index + 3 {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let start = StreamId::parse_range_start(arg_bytes(&args[index])?)?;
    let end = StreamId::parse_range_end(arg_bytes(&args[index + 1])?)?;
    let count = parse_usize(arg_bytes(&args[index + 2])?)?;
    let consumer = if index + 3 < args.len() {
        Some(arg_bytes(&args[index + 3])?)
    } else {
        None
    };
    if index + 4 < args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let details = pending_detail(group, start, end, count, min_idle_ms, consumer, now_ms());
    Ok(pending_detail_response(details))
}

#[inline]
pub fn xack(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xack' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let stream = get_stream_mut_or_err(store, key)?;
    let group = get_group_mut_or_err(stream, group_name)?;

    let mut seen = HashSet::new();
    let mut acked = 0i64;
    for frame in &args[2..] {
        let id = StreamId::parse(arg_bytes(frame)?)?;
        if seen.insert(id) && ack_id(group, id) {
            acked += 1;
        }
    }
    Ok(Response::Integer(acked))
}

#[inline]
pub fn xackdel(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 5 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xackdel' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let mut index = 2usize;
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

    let stream = get_stream_mut_or_err(store, key)?;
    let group_key = group_key(group_name)?;
    let mut group = stream
        .groups
        .remove(group_key.as_str())
        .ok_or(SenkoError::Protocol(ERR_NO_GROUP))?;
    let mut seen = HashSet::new();
    let mut acked = 0i64;
    for frame in &args[index..] {
        let id = StreamId::parse(arg_bytes(frame)?)?;
        if seen.insert(id) && xackdel_apply(stream, &mut group, id, ref_mode) {
            acked += 1;
        }
    }
    stream.groups.insert(group.name.clone(), group);
    Ok(Response::Integer(acked))
}

fn xgroup_create(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup create' command",
        ));
    }
    let key_bytes = arg_bytes(&args[0])?;
    ensure_stream_type_or_missing(store, key_bytes)?;

    let group_name = parse_key(arg_bytes(&args[1])?)?;
    let start = parse_group_id(arg_bytes(&args[2])?, store.get_stream(key_bytes))?;

    let mut mkstream = false;
    let mut entries_read = 0u64;
    let mut index = 3usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"MKSTREAM") {
            mkstream = true;
            index += 1;
            continue;
        }
        if is_opt(token, b"ENTRIESREAD") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            entries_read = parse_u64(arg_bytes(&args[index])?)?;
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    if !mkstream && store.get_stream(key_bytes).is_none() {
        return Err(SenkoError::Protocol(ERR_NO_KEY));
    }

    let key = parse_key(key_bytes)?;
    let stream = store.get_or_create_stream(key);
    create_group(stream, group_name, start, entries_read)?;
    Ok(Response::Simple(OK))
}

fn xgroup_createconsumer(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup createconsumer' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let consumer = parse_key(arg_bytes(&args[2])?)?;
    let stream = get_stream_mut_or_err(store, key)?;
    let group = get_group_mut_or_err(stream, group_name)?;
    Ok(Response::Integer(
        create_consumer(group, consumer, now_ms()) as i64,
    ))
}

fn xgroup_delconsumer(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup delconsumer' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let consumer = arg_bytes(&args[2])?;
    let stream = get_stream_mut_or_err(store, key)?;
    let group = get_group_mut_or_err(stream, group_name)?;
    Ok(Response::Integer(delete_consumer(group, consumer) as i64))
}

fn xgroup_destroy(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup destroy' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let stream = get_stream_mut_or_err(store, key)?;
    Ok(Response::Integer(destroy_group(stream, group_name) as i64))
}

fn xgroup_setid(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xgroup setid' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let group_name = arg_bytes(&args[1])?;
    let stream = get_stream_mut_or_err(store, key)?;

    let requested = if arg_bytes(&args[2])? == b"$" {
        StreamId::MAX
    } else {
        StreamId::parse(arg_bytes(&args[2])?)?
    };
    let mut entries_read = None;
    let mut index = 3usize;
    if index < args.len() {
        if !is_opt(arg_bytes(&args[index])?, b"ENTRIESREAD") {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        index += 1;
        if index >= args.len() {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        entries_read = Some(parse_u64(arg_bytes(&args[index])?)?);
        index += 1;
    }
    if index != args.len() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let group_key = group_key(group_name)?;
    let Some(group) = stream.groups.get_mut(group_key.as_str()) else {
        return Err(SenkoError::Protocol(ERR_NO_GROUP));
    };
    let synthetic_stream = StreamObject {
        tree: stream.tree.clone(),
        groups: Default::default(),
    };
    set_group_id(&synthetic_stream, group, requested, entries_read);
    Ok(Response::Simple(OK))
}

fn xinfo_stream(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xinfo stream' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    let stream = get_stream_or_err(store, key)?;

    let mut full = false;
    let mut count = 10usize;
    let mut index = 1usize;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if is_opt(token, b"FULL") {
            full = true;
            index += 1;
            continue;
        }
        if is_opt(token, b"COUNT") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            count = parse_usize(arg_bytes(&args[index])?)?;
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let mut map = SmallVec::<[Response; 32]>::new();
    push_map_raw(&mut map, "length");
    map.push(Response::Integer(stream.tree.len as i64));
    push_map_raw(&mut map, "radix-tree-keys");
    map.push(Response::Integer(stream.tree.macro_node_count() as i64));
    push_map_raw(&mut map, "radix-tree-nodes");
    map.push(Response::Integer(stream.tree.macro_node_count() as i64));
    push_map_raw(&mut map, "last-generated-id");
    map.push(raw_response(stream.tree.last_id.to_string().as_bytes()));
    push_map_raw(&mut map, "max-deleted-entry-id");
    map.push(raw_response(
        stream.tree.max_deleted_entry_id.to_string().as_bytes(),
    ));
    push_map_raw(&mut map, "entries-added");
    map.push(Response::Integer(stream.tree.entries_added as i64));
    push_map_raw(&mut map, "first-entry");
    map.push(entry_or_null(stream.tree.first_entry()));
    push_map_raw(&mut map, "last-entry");
    map.push(entry_or_null(stream.tree.last_entry()));
    push_map_raw(&mut map, "groups");
    map.push(Response::Integer(stream.groups.len() as i64));

    if full {
        push_map_raw(&mut map, "groups-detail");
        let mut groups = SmallVec::<[Response; 16]>::new();
        for group in stream.groups.values() {
            let mut group_map = SmallVec::<[Response; 32]>::new();
            push_map_raw(&mut group_map, "name");
            group_map.push(raw_response(group.name.as_bytes()));
            push_map_raw(&mut group_map, "last-delivered-id");
            group_map.push(raw_response(group.last_delivered_id.to_string().as_bytes()));
            push_map_raw(&mut group_map, "entries-read");
            group_map.push(Response::Integer(group.entries_read as i64));
            push_map_raw(&mut group_map, "pending");
            group_map.push(Response::Integer(group.pel_count as i64));
            push_map_raw(&mut group_map, "consumers");

            let mut consumers = SmallVec::<[Response; 16]>::new();
            for consumer in group.consumers.values() {
                let mut consumer_map = SmallVec::<[Response; 32]>::new();
                push_map_raw(&mut consumer_map, "name");
                consumer_map.push(raw_response(consumer.name.as_bytes()));
                push_map_raw(&mut consumer_map, "pending");
                consumer_map.push(Response::Integer(consumer.pel.len() as i64));
                push_map_raw(&mut consumer_map, "pel");
                let details = pending_detail(
                    group,
                    StreamId::ZERO,
                    StreamId::MAX,
                    count,
                    None,
                    Some(consumer.name.as_bytes()),
                    now_ms(),
                );
                consumer_map.push(pending_detail_response(details));
                consumers.push(Response::Map(Box::new(consumer_map)));
            }
            group_map.push(Response::Array(Box::new(consumers)));
            groups.push(Response::Map(Box::new(group_map)));
        }
        map.push(Response::Array(Box::new(groups)));
    }

    Ok(Response::Map(Box::new(map)))
}

fn xinfo_groups(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xinfo groups' command",
        ));
    }
    let stream = get_stream_or_err(store, arg_bytes(&args[0])?)?;
    let infos = group_info(stream);
    let mut out = SmallVec::<[Response; 16]>::new();
    for info in infos {
        out.push(group_info_map(info));
    }
    Ok(Response::Array(Box::new(out)))
}

fn xinfo_consumers(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'xinfo consumers' command",
        ));
    }
    let stream = get_stream_or_err(store, arg_bytes(&args[0])?)?;
    let group = get_group_or_err(stream, arg_bytes(&args[1])?)?;
    let infos = consumer_info(group, now_ms());
    let mut out = SmallVec::<[Response; 16]>::new();
    for info in infos {
        out.push(consumer_info_map(info));
    }
    Ok(Response::Array(Box::new(out)))
}

fn group_info_map(info: GroupInfo) -> Response {
    let mut map = SmallVec::<[Response; 32]>::new();
    push_map_raw(&mut map, "name");
    map.push(raw_response(info.name.as_bytes()));
    push_map_raw(&mut map, "consumers");
    map.push(Response::Integer(info.consumers as i64));
    push_map_raw(&mut map, "pending");
    map.push(Response::Integer(info.pending as i64));
    push_map_raw(&mut map, "last-delivered-id");
    map.push(raw_response(info.last_delivered_id.to_string().as_bytes()));
    push_map_raw(&mut map, "entries-read");
    map.push(Response::Integer(info.entries_read as i64));
    push_map_raw(&mut map, "lag");
    map.push(Response::Integer(info.lag as i64));
    Response::Map(Box::new(map))
}

fn consumer_info_map(info: ConsumerInfo) -> Response {
    let mut map = SmallVec::<[Response; 32]>::new();
    push_map_raw(&mut map, "name");
    map.push(raw_response(info.name.as_bytes()));
    push_map_raw(&mut map, "pending");
    map.push(Response::Integer(info.pending as i64));
    push_map_raw(&mut map, "idle");
    map.push(Response::Integer(info.idle as i64));
    push_map_raw(&mut map, "inactive");
    map.push(Response::Integer(info.inactive as i64));
    Response::Map(Box::new(map))
}

fn pending_detail_response(details: Vec<PendingDetail>) -> Response {
    let mut out = SmallVec::<[Response; 16]>::new();
    for detail in details {
        out.push(Response::Array(Box::new(SmallVec::from_iter([
            raw_response(detail.id.to_string().as_bytes()),
            raw_response(detail.consumer.as_bytes()),
            Response::Integer(detail.idle_ms as i64),
            Response::Integer(detail.delivery_count as i64),
        ]))));
    }
    Response::Array(Box::new(out))
}

fn entry_or_null(entry: Option<(StreamId, Vec<(&[u8], &[u8])>)>) -> Response {
    let Some((id, fields)) = entry else {
        return Response::Value(None);
    };
    let mut flat = SmallVec::<[Response; 16]>::new();
    for (field, value) in fields {
        flat.push(raw_response(field));
        flat.push(raw_response(value));
    }
    Response::Array(Box::new(SmallVec::from_iter([
        raw_response(id.to_string().as_bytes()),
        Response::Array(Box::new(flat)),
    ])))
}

fn id_or_null(id: Option<StreamId>) -> Response {
    match id {
        Some(id) => raw_response(id.to_string().as_bytes()),
        None => Response::Value(None),
    }
}

fn push_map_raw(map: &mut SmallVec<[Response; 32]>, key: &str) {
    map.push(raw_response(key.as_bytes()));
}

fn get_stream_or_err<'a>(store: &'a mut Store, key: &[u8]) -> SenkoResult<&'a StreamObject> {
    ensure_stream_type_or_missing(store, key)?;
    store
        .get_stream(key)
        .ok_or(SenkoError::Protocol(ERR_NO_KEY))
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

fn get_group_or_err<'a>(stream: &'a StreamObject, group: &[u8]) -> SenkoResult<&'a ConsumerGroup> {
    let group = group_key(group)?;
    stream
        .groups
        .get(group.as_str())
        .ok_or(SenkoError::Protocol(ERR_NO_GROUP))
}

fn get_group_mut_or_err<'a>(
    stream: &'a mut StreamObject,
    group: &[u8],
) -> SenkoResult<&'a mut ConsumerGroup> {
    let group = group_key(group)?;
    stream
        .groups
        .get_mut(group.as_str())
        .ok_or(SenkoError::Protocol(ERR_NO_GROUP))
}

fn parse_group_id(raw: &[u8], stream: Option<&StreamObject>) -> SenkoResult<StreamId> {
    if raw == b"$" {
        return Ok(stream
            .map(|stream| stream.tree.last_id)
            .unwrap_or(StreamId::ZERO));
    }
    StreamId::parse(raw)
}

fn group_key(raw: &[u8]) -> SenkoResult<CompactString> {
    parse_key(raw)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::stream::basic::xadd;
    use crate::stream::add_pending_entry;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn response_map_contains_key(response: &Response, key: &[u8]) -> bool {
        let Response::Map(map) = response else {
            return false;
        };
        map.chunks(2).any(|chunk| {
            matches!(
                chunk.first(),
                Some(Response::Value(Some(SenkoValue::Raw(bytes)))) if bytes.as_ref() == key
            )
        })
    }

    fn seed_pending(store: &mut Store, key: &[u8], group_name: &str) {
        let stream = store.get_stream_mut(key).unwrap();
        let group = stream.groups.get_mut(group_name).unwrap();
        add_pending_entry(
            group,
            CompactString::new("c1"),
            StreamId { ms: 1, seq: 0 },
            10,
            1,
        );
        add_pending_entry(
            group,
            CompactString::new("c2"),
            StreamId { ms: 2, seq: 0 },
            20,
            2,
        );
        add_pending_entry(
            group,
            CompactString::new("c1"),
            StreamId { ms: 3, seq: 0 },
            30,
            3,
        );
    }

    #[test]
    fn xgroup_create_dollar_starts_after_existing_entries() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        assert_eq!(
            xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"$")]).unwrap(),
            Response::Simple(OK)
        );
        assert_eq!(
            store
                .get_stream(b"s")
                .unwrap()
                .groups
                .get("g")
                .unwrap()
                .last_delivered_id,
            StreamId { ms: 1, seq: 0 }
        );
    }

    #[test]
    fn xgroup_create_zero_and_duplicate() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        assert_eq!(
            store
                .get_stream(b"s")
                .unwrap()
                .groups
                .get("g")
                .unwrap()
                .last_delivered_id,
            StreamId::ZERO
        );
        assert!(matches!(
            xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]),
            Err(SenkoError::Protocol(
                "BUSYGROUP Consumer Group name already exists"
            ))
        ));
    }

    #[test]
    fn createconsumer_returns_one_then_zero() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        assert_eq!(
            xgroup(
                &mut store,
                &[bs(b"CREATECONSUMER"), bs(b"s"), bs(b"g"), bs(b"c1")]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            xgroup(
                &mut store,
                &[bs(b"CREATECONSUMER"), bs(b"s"), bs(b"g"), bs(b"c1")]
            )
            .unwrap(),
            Response::Integer(0)
        );
    }

    #[test]
    fn delconsumer_and_destroy_update_pel() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        seed_pending(&mut store, b"s", "g");
        assert_eq!(
            xgroup(
                &mut store,
                &[bs(b"DELCONSUMER"), bs(b"s"), bs(b"g"), bs(b"c1")]
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            store
                .get_stream(b"s")
                .unwrap()
                .groups
                .get("g")
                .unwrap()
                .pel_count,
            1
        );
        assert_eq!(
            xgroup(&mut store, &[bs(b"DESTROY"), bs(b"s"), bs(b"g")]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn setid_dollar_follows_latest_xadd() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"SETID"), bs(b"s"), bs(b"g"), bs(b"$")]).unwrap();
        assert_eq!(
            store
                .get_stream(b"s")
                .unwrap()
                .groups
                .get("g")
                .unwrap()
                .last_delivered_id,
            StreamId { ms: 2, seq: 0 }
        );
    }

    #[test]
    fn xinfo_stream_and_groups_present_fields() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(
            &mut store,
            &[
                bs(b"CREATE"),
                bs(b"s"),
                bs(b"g"),
                bs(b"0"),
                bs(b"ENTRIESREAD"),
                bs(b"0"),
            ],
        )
        .unwrap();
        let stream_info = xinfo(&mut store, &[bs(b"STREAM"), bs(b"s")]).unwrap();
        assert!(response_map_contains_key(&stream_info, b"length"));
        assert!(response_map_contains_key(&stream_info, b"entries-added"));
        let groups = xinfo(&mut store, &[bs(b"GROUPS"), bs(b"s")]).unwrap();
        let Response::Array(values) = groups else {
            panic!("expected groups array");
        };
        assert_eq!(values.len(), 1);
        let Response::Map(group) = &values[0] else {
            panic!("expected group map");
        };
        assert!(group.chunks(2).any(|chunk| {
            matches!(&chunk[0], Response::Value(Some(SenkoValue::Raw(bytes))) if bytes.as_ref() == b"lag")
                && matches!(&chunk[1], Response::Integer(1))
        }));
    }

    #[test]
    fn xpending_summary_and_detail_filters() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        seed_pending(&mut store, b"s", "g");

        let Response::Array(summary) = xpending(&mut store, &[bs(b"s"), bs(b"g")]).unwrap() else {
            panic!("expected summary");
        };
        assert_eq!(summary[0], Response::Integer(3));

        let Response::Array(detail) = xpending(
            &mut store,
            &[
                bs(b"s"),
                bs(b"g"),
                bs(b"IDLE"),
                bs(b"0"),
                bs(b"2-0"),
                bs(b"3-0"),
                bs(b"10"),
                bs(b"c1"),
            ],
        )
        .unwrap() else {
            panic!("expected detail");
        };
        assert_eq!(detail.len(), 1);
    }

    #[test]
    fn xack_and_xackdel_variants_work() {
        let mut store = Store::default();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0")]).unwrap();
        seed_pending(&mut store, b"s", "g");

        assert_eq!(
            xack(&mut store, &[bs(b"s"), bs(b"g"), bs(b"9-0")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            xack(&mut store, &[bs(b"s"), bs(b"g"), bs(b"1-0")]).unwrap(),
            Response::Integer(1)
        );
        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert!(!group.global_pel.contains_key(&StreamId { ms: 1, seq: 0 }));
        assert!(
            group
                .consumers
                .values()
                .all(|consumer| !consumer.pel.contains_key(&StreamId { ms: 1, seq: 0 }))
        );

        assert_eq!(
            xackdel(
                &mut store,
                &[
                    bs(b"s"),
                    bs(b"g"),
                    bs(b"DELREF"),
                    bs(b"IDS"),
                    bs(b"1"),
                    bs(b"2-0"),
                ],
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert!(
            store
                .get_stream(b"s")
                .unwrap()
                .tree
                .get(StreamId { ms: 2, seq: 0 })
                .is_none()
        );

        let _ = xadd(&mut store, &[bs(b"s"), bs(b"4-0"), bs(b"f"), bs(b"4")]).unwrap();
        {
            let stream = store.get_stream_mut(b"s").unwrap();
            let group = stream.groups.get_mut("g").unwrap();
            add_pending_entry(
                group,
                CompactString::new("c1"),
                StreamId { ms: 4, seq: 0 },
                40,
                1,
            );
        }
        assert_eq!(
            xackdel(
                &mut store,
                &[
                    bs(b"s"),
                    bs(b"g"),
                    bs(b"KEEPREF"),
                    bs(b"IDS"),
                    bs(b"1"),
                    bs(b"4-0"),
                ],
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert!(
            store
                .get_stream(b"s")
                .unwrap()
                .tree
                .get(StreamId { ms: 4, seq: 0 })
                .is_some()
        );
    }
}
