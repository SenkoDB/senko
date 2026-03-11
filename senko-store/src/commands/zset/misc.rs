use bytes::Bytes;
use rand::{SeedableRng, rngs::SmallRng};
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    commands::zset::basic::{arg_bytes, ensure_zset_type_or_missing, formatted_score_value},
    store::Store,
    zset::{parse_lex_bound, parse_score_bound},
};

#[inline]
pub fn zrandmember(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() || args.len() > 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrandmember' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(if args.len() == 1 {
            Response::Value(None)
        } else {
            Response::Array(Box::default())
        });
    };

    let mut rng = SmallRng::from_entropy();
    if args.len() == 1 {
        return Ok(Response::Value(zset.random_member(&mut rng).map(
            |(member, _)| SenkoValue::Raw(Bytes::copy_from_slice(member.as_bytes())),
        )));
    }

    let count = parse_i64(arg_bytes(&args[1])?)?;
    let withscores = if args.len() == 3 {
        if !arg_bytes(&args[2])?.eq_ignore_ascii_case(b"withscores") {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        true
    } else {
        false
    };

    if count == 0 {
        return Ok(Response::Array(Box::default()));
    }

    let entries = if count > 0 {
        zset.random_members_distinct(count as usize, &mut rng)
    } else {
        zset.random_members_repeating(count.unsigned_abs() as usize, &mut rng)
    };
    Ok(entries_response(entries, withscores))
}

#[inline]
pub fn zremrangebyscore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zremrangebyscore' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let min = parse_score_bound(arg_bytes(&args[1])?)?;
    let max = parse_score_bound(arg_bytes(&args[2])?)?;
    let members = store
        .get_zset(key)
        .map(|zset| {
            zset.range_by_score(min, max, false, None)
                .map(|(_, member)| member)
                .collect()
        })
        .unwrap_or_default();
    remove_members(store, key, members)
}

#[inline]
pub fn zremrangebyrank(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zremrangebyrank' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let start = parse_i64(arg_bytes(&args[1])?)?;
    let stop = parse_i64(arg_bytes(&args[2])?)?;
    let members = store
        .get_zset(key)
        .map(|zset| {
            zset.range_by_rank(start, stop, false, None)
                .map(|(_, member)| member)
                .collect()
        })
        .unwrap_or_default();
    remove_members(store, key, members)
}

#[inline]
pub fn zremrangebylex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zremrangebylex' command",
        ));
    }
    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let min = parse_lex_bound(arg_bytes(&args[1])?)?;
    let max = parse_lex_bound(arg_bytes(&args[2])?)?;
    let members = store
        .get_zset(key)
        .map(|zset| {
            zset.range_by_lex(min, max, false, None)
                .map(|(_, member)| member)
                .collect()
        })
        .unwrap_or_default();
    remove_members(store, key, members)
}

pub(crate) fn entries_response(
    entries: Vec<(compact_str::CompactString, f64)>,
    withscores: bool,
) -> Response {
    let mut out = SmallVec::<[Response; 16]>::new();
    for (member, score) in entries {
        out.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(member.as_bytes()),
        ))));
        if withscores {
            out.push(Response::Value(Some(formatted_score_value(score))));
        }
    }
    Response::Array(Box::new(out))
}

fn remove_members(
    store: &mut Store,
    key: &[u8],
    members: Vec<compact_str::CompactString>,
) -> SenkoResult<Response> {
    if members.is_empty() {
        return Ok(Response::Integer(0));
    }
    let zset = store
        .get_zset_mut(key)
        .ok_or_else(|| SenkoError::Storage("missing zset during range removal"))?;
    let mut removed = 0_i64;
    for member in members {
        if zset.remove(member.as_bytes()).is_some() {
            removed += 1;
        }
    }
    store.remove_zset_if_empty(key);
    Ok(Response::Integer(removed))
}

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| SenkoError::Protocol("value is out of range"))
}

#[cfg(test)]
mod tests {
    use senko_proto::Frame;

    use super::*;
    use crate::commands::zset::basic::{zadd, zscore};
    use crate::commands::zset::range::zrange;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn seed(store: &mut Store) {
        let _ = zadd(
            store,
            &[
                bs(b"zs"),
                bs(b"1"),
                bs(b"a"),
                bs(b"2"),
                bs(b"b"),
                bs(b"3"),
                bs(b"c"),
                bs(b"3"),
                bs(b"d"),
            ],
        )
        .unwrap();
    }

    fn array_bytes(response: Response) -> Vec<Vec<u8>> {
        let Response::Array(items) = response else {
            panic!("expected array");
        };
        items
            .into_iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected response {other:?}"),
            })
            .collect()
    }

    #[test]
    fn zrandmember_distinct_and_repeating_work() {
        let mut store = Store::default();
        seed(&mut store);

        let distinct = zrandmember(&mut store, &[bs(b"zs"), bs(b"3")]).unwrap();
        let Response::Array(values) = distinct else {
            panic!("expected array");
        };
        assert!(values.len() <= 3);

        let repeating = zrandmember(&mut store, &[bs(b"zs"), bs(b"-5")]).unwrap();
        let Response::Array(values) = repeating else {
            panic!("expected array");
        };
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn zrandmember_withscores_interleaves_scores() {
        let mut store = Store::default();
        seed(&mut store);
        let Response::Array(values) =
            zrandmember(&mut store, &[bs(b"zs"), bs(b"2"), bs(b"WITHSCORES")]).unwrap()
        else {
            panic!("expected array");
        };
        assert_eq!(values.len() % 2, 0);
    }

    #[test]
    fn zremrangebyscore_removes_matching_members_and_syncs_index() {
        let mut store = Store::default();
        seed(&mut store);
        assert_eq!(
            zremrangebyscore(&mut store, &[bs(b"zs"), bs(b"(1"), bs(b"3")]).unwrap(),
            Response::Integer(3)
        );
        assert!(matches!(
            zscore(&mut store, &[bs(b"zs"), bs(b"b")]).unwrap(),
            Response::Value(None)
        ));
    }

    #[test]
    fn zremrangebyrank_removes_normalized_rank_slice() {
        let mut store = Store::default();
        seed(&mut store);
        assert_eq!(
            zremrangebyrank(&mut store, &[bs(b"zs"), bs(b"-2"), bs(b"-1")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            array_bytes(zrange(&mut store, &[bs(b"zs"), bs(b"0"), bs(b"-1")]).unwrap()),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn zremrangebylex_removes_equal_score_members() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"zs"),
                bs(b"1"),
                bs(b"a"),
                bs(b"1"),
                bs(b"b"),
                bs(b"1"),
                bs(b"c"),
            ],
        )
        .unwrap();
        assert_eq!(
            zremrangebylex(&mut store, &[bs(b"zs"), bs(b"[b"), bs(b"[c")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            array_bytes(zrange(&mut store, &[bs(b"zs"), bs(b"0"), bs(b"-1")]).unwrap()),
            vec![b"a".to_vec()]
        );
    }
}
