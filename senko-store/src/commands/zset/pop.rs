use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    commands::zset::basic::{
        arg_bytes, ensure_zset_type_or_missing, formatted_score_value, wrong_type,
    },
    store::Store,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZPopDir {
    Min,
    Max,
}

#[inline]
pub fn zpopmin(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zpop(store, args, ZPopDir::Min, "zpopmin")
}

#[inline]
pub fn zpopmax(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zpop(store, args, ZPopDir::Max, "zpopmax")
}

#[inline]
pub fn zmpop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zmpop' command",
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
    let direction = parse_direction(arg_bytes(&args[side_index])?)?;
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
            Some(SenkoValue::ZSet(_)) => {}
            Some(other) => return Err(wrong_type(other)),
        }

        let entries = pop_entries(store, key, direction, count)?;
        return Ok(zmpop_response(key, entries));
    }

    Ok(Response::Value(None))
}

pub(crate) fn zpop_block_response(key: &[u8], entries: Vec<(f64, CompactString)>) -> Response {
    let Some((score, member)) = entries.into_iter().next() else {
        return Response::NullArray;
    };

    Response::Array(Box::new(smallvec::smallvec![
        Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(key)))),
        Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(
            member.as_bytes()
        )))),
        Response::Value(Some(formatted_score_value(score))),
    ]))
}

pub(crate) fn zmpop_response(key: &[u8], entries: Vec<(f64, CompactString)>) -> Response {
    let mut values = SmallVec::<[Response; 16]>::new();
    for (score, member) in entries {
        values.push(Response::Array(Box::new(smallvec::smallvec![
            Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(
                member.as_bytes()
            )))),
            Response::Value(Some(formatted_score_value(score))),
        ])));
    }
    Response::Array(Box::new(smallvec::smallvec![
        Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(key)))),
        Response::Array(Box::new(values)),
    ]))
}

pub(crate) fn pop_entries(
    store: &mut Store,
    key: &[u8],
    direction: ZPopDir,
    count: usize,
) -> SenkoResult<Vec<(f64, CompactString)>> {
    ensure_zset_type_or_missing(store, key)?;
    if count == 0 {
        return Ok(Vec::new());
    }

    let entries = match store.get_zset_mut(key) {
        Some(zset) => match direction {
            ZPopDir::Min => zset.pop_min(count),
            ZPopDir::Max => zset.pop_max(count),
        },
        None => Vec::new(),
    };
    store.remove_zset_if_empty(key);
    Ok(entries)
}

fn zpop(
    store: &mut Store,
    args: &[Frame<'_>],
    direction: ZPopDir,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.is_empty() || args.len() > 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }

    let key = arg_bytes(&args[0])?;
    let count = if args.len() == 2 {
        parse_usize(arg_bytes(&args[1])?)?
    } else {
        1
    };
    let entries = pop_entries(store, key, direction, count)?;
    Ok(flat_pop_response(entries))
}

fn flat_pop_response(entries: Vec<(f64, CompactString)>) -> Response {
    let mut out = SmallVec::<[Response; 16]>::new();
    for (score, member) in entries {
        out.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(member.as_bytes()),
        ))));
        out.push(Response::Value(Some(formatted_score_value(score))));
    }
    Response::Array(Box::new(out))
}

pub(crate) fn parse_direction(raw: &[u8]) -> SenkoResult<ZPopDir> {
    if raw.eq_ignore_ascii_case(b"MIN") {
        Ok(ZPopDir::Min)
    } else if raw.eq_ignore_ascii_case(b"MAX") {
        Ok(ZPopDir::Max)
    } else {
        Err(SenkoError::Protocol("syntax error"))
    }
}

pub(crate) fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(SenkoError::Protocol("value is out of range"))
}

#[cfg(test)]
mod tests {
    use senko_proto::Frame;

    use super::*;
    use crate::commands::zset::basic::parse_compact;
    use crate::commands::zset::basic::zadd;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn seed() -> Store {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"zs"),
                bs(b"1"),
                bs(b"a"),
                bs(b"2"),
                bs(b"b"),
                bs(b"3"),
                bs(b"c"),
            ],
        )
        .unwrap();
        store
    }

    fn flat_bytes(response: &Response) -> Vec<Vec<u8>> {
        let Response::Array(items) = response else {
            panic!("expected array");
        };
        items
            .iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected response: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn zpopmin_single_returns_lowest_score_pair() {
        let mut store = seed();
        assert_eq!(
            flat_bytes(&zpopmin(&mut store, &[bs(b"zs")]).unwrap()),
            vec![b"a".to_vec(), b"1".to_vec()]
        );
    }

    #[test]
    fn zpopmin_count_zero_is_empty_and_non_mutating() {
        let mut store = seed();
        assert_eq!(
            flat_bytes(&zpopmin(&mut store, &[bs(b"zs"), bs(b"0")]).unwrap()),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(store.get_zset(b"zs").map(|z| z.len()), Some(3));
    }

    #[test]
    fn zpopmax_returns_highest_score_pair() {
        let mut store = seed();
        assert_eq!(
            flat_bytes(&zpopmax(&mut store, &[bs(b"zs")]).unwrap()),
            vec![b"c".to_vec(), b"3".to_vec()]
        );
    }

    #[test]
    fn zpopmin_count_greater_than_cardinality_returns_all() {
        let mut store = seed();
        assert_eq!(
            flat_bytes(&zpopmin(&mut store, &[bs(b"zs"), bs(b"10")]).unwrap()),
            vec![
                b"a".to_vec(),
                b"1".to_vec(),
                b"b".to_vec(),
                b"2".to_vec(),
                b"c".to_vec(),
                b"3".to_vec()
            ]
        );
    }

    #[test]
    fn zmpop_uses_first_non_empty_key() {
        let mut store = seed();
        assert_eq!(
            zmpop(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"empty"),
                    bs(b"zs"),
                    bs(b"MIN"),
                    bs(b"COUNT"),
                    bs(b"2")
                ]
            )
            .unwrap(),
            zmpop_response(
                b"zs",
                vec![(1.0, parse_compact(b"a")), (2.0, parse_compact(b"b"))]
            )
        );
    }

    #[test]
    fn zmpop_count_larger_than_set_returns_remaining_members() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[bs(b"zs"), bs(b"1"), bs(b"a"), bs(b"2"), bs(b"b")],
        )
        .unwrap();
        assert_eq!(
            zmpop(
                &mut store,
                &[bs(b"1"), bs(b"zs"), bs(b"MIN"), bs(b"COUNT"), bs(b"3")]
            )
            .unwrap(),
            zmpop_response(
                b"zs",
                vec![(1.0, parse_compact(b"a")), (2.0, parse_compact(b"b"))]
            )
        );
    }

    #[test]
    fn zpopmin_deletes_key_when_last_member_is_removed() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"zs"), bs(b"1"), bs(b"a")]).unwrap();
        let _ = zpopmin(&mut store, &[bs(b"zs")]).unwrap();
        assert!(store.get(b"zs").is_none());
    }
}
