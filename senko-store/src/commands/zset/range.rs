use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue, ZSetObject};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    commands::zset::basic::{
        arg_bytes, ensure_zset_type_or_missing, formatted_score_value, parse_compact,
    },
    store::{SetOptions, Store},
    zset::{LexBound, ScoreBound, parse_lex_bound, parse_score_bound},
};

pub(crate) enum RangeSpec<'a> {
    ByRank {
        start: i64,
        stop: i64,
    },
    ByScore {
        min: ScoreBound,
        max: ScoreBound,
    },
    ByLex {
        min: LexBound<'a>,
        max: LexBound<'a>,
    },
}

pub(crate) struct RangeEntry {
    pub member: CompactString,
    pub score: Option<f64>,
}

pub(crate) fn execute_range(
    zset: &ZSetObject,
    spec: RangeSpec<'_>,
    reverse: bool,
    limit: Option<(i64, i64)>,
    withscores: bool,
) -> SenkoResult<Vec<RangeEntry>> {
    if matches!(spec, RangeSpec::ByRank { .. }) && limit.is_some() {
        return Err(SenkoError::Protocol(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if matches!(spec, RangeSpec::ByLex { .. }) && withscores {
        return Err(SenkoError::Protocol(
            "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }

    let entries: Vec<(f64, CompactString)> = match spec {
        RangeSpec::ByRank { start, stop } => {
            zset.range_by_rank(start, stop, reverse, None).collect()
        }
        RangeSpec::ByScore { min, max } => zset.range_by_score(min, max, reverse, None).collect(),
        RangeSpec::ByLex { min, max } => zset.range_by_lex(min, max, reverse, None).collect(),
    };

    let entries = apply_limit(entries, limit)?;
    Ok(entries
        .into_iter()
        .map(|(score, member)| RangeEntry {
            member,
            score: withscores.then_some(score),
        })
        .collect())
}

#[inline]
pub fn zrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrange' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;

    let spec = parse_zrange_spec(arg_bytes(&args[1])?, arg_bytes(&args[2])?, &args[3..])?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(zset, spec.spec, spec.reverse, spec.limit, spec.withscores)?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrangebyscore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrangebyscore' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let options = parse_legacy_range_tail(&args[3..], false)?;
    let min = parse_score_bound(arg_bytes(&args[1])?)?;
    let max = parse_score_bound(arg_bytes(&args[2])?)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(
        zset,
        RangeSpec::ByScore { min, max },
        false,
        options.limit,
        options.withscores,
    )?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrangebylex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrangebylex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let options = parse_legacy_range_tail(&args[3..], true)?;
    let min = parse_lex_bound(arg_bytes(&args[1])?)?;
    let max = parse_lex_bound(arg_bytes(&args[2])?)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(
        zset,
        RangeSpec::ByLex { min, max },
        false,
        options.limit,
        false,
    )?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrevrange(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 && args.len() != 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrevrange' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let withscores = if args.len() == 4 {
        if !arg_bytes(&args[3])?.eq_ignore_ascii_case(b"withscores") {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        true
    } else {
        false
    };
    let start = parse_i64(arg_bytes(&args[1])?)?;
    let stop = parse_i64(arg_bytes(&args[2])?)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(
        zset,
        RangeSpec::ByRank { start, stop },
        true,
        None,
        withscores,
    )?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrevrangebyscore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrevrangebyscore' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let options = parse_legacy_range_tail(&args[3..], false)?;
    let min = parse_score_bound(arg_bytes(&args[2])?)?;
    let max = parse_score_bound(arg_bytes(&args[1])?)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(
        zset,
        RangeSpec::ByScore { min, max },
        true,
        options.limit,
        options.withscores,
    )?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrevrangebylex(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrevrangebylex' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let options = parse_legacy_range_tail(&args[3..], true)?;
    let min = parse_lex_bound(arg_bytes(&args[2])?)?;
    let max = parse_lex_bound(arg_bytes(&args[1])?)?;
    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    };
    let entries = execute_range(
        zset,
        RangeSpec::ByLex { min, max },
        true,
        options.limit,
        false,
    )?;
    Ok(range_entries_response(entries))
}

#[inline]
pub fn zrangestore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 4 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrangestore' command",
        ));
    }

    let dst = arg_bytes(&args[0])?;
    let src = arg_bytes(&args[1])?;
    ensure_zset_type_or_missing(store, src)?;
    let spec = parse_zrange_spec(arg_bytes(&args[2])?, arg_bytes(&args[3])?, &args[4..])?;

    let entries = if let Some(zset) = store.get_zset(src) {
        execute_range(zset, spec.spec, spec.reverse, spec.limit, true)?
    } else {
        Vec::new()
    };

    if entries.is_empty() {
        let _ = store.delete(dst);
        return Ok(Response::Integer(0));
    }

    let mut out = ZSetObject::default();
    for entry in entries {
        let _ = out.add(
            entry.score.expect("zrangestore stores scores"),
            entry.member,
            Default::default(),
        );
    }

    let _ = store.set(
        parse_compact(dst),
        SenkoValue::ZSet(Box::new(out)),
        SetOptions::default(),
    );
    Ok(Response::Integer(
        store.get_zset(dst).map_or(0, |zset| zset.len() as i64),
    ))
}

struct ParsedZRange<'a> {
    spec: RangeSpec<'a>,
    reverse: bool,
    limit: Option<(i64, i64)>,
    withscores: bool,
}

struct LegacyTail {
    limit: Option<(i64, i64)>,
    withscores: bool,
}

fn parse_zrange_spec<'a>(
    start_raw: &'a [u8],
    stop_raw: &'a [u8],
    tail: &'a [Frame<'a>],
) -> SenkoResult<ParsedZRange<'a>> {
    let mut byscore = false;
    let mut bylex = false;
    let mut reverse = false;
    let mut limit = None;
    let mut withscores = false;
    let mut index = 0;

    while index < tail.len() {
        let token = arg_bytes(&tail[index])?;
        if token.eq_ignore_ascii_case(b"byscore") {
            byscore = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"bylex") {
            bylex = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"rev") {
            reverse = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"withscores") {
            withscores = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"limit") {
            if index + 2 >= tail.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            limit = Some((
                parse_i64(arg_bytes(&tail[index + 1])?)?,
                parse_i64(arg_bytes(&tail[index + 2])?)?,
            ));
            index += 3;
        } else {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
    }

    if byscore && bylex {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }

    let spec = if byscore {
        let (min, max) = if reverse {
            (parse_score_bound(stop_raw)?, parse_score_bound(start_raw)?)
        } else {
            (parse_score_bound(start_raw)?, parse_score_bound(stop_raw)?)
        };
        RangeSpec::ByScore { min, max }
    } else if bylex {
        let (min, max) = if reverse {
            (parse_lex_bound(stop_raw)?, parse_lex_bound(start_raw)?)
        } else {
            (parse_lex_bound(start_raw)?, parse_lex_bound(stop_raw)?)
        };
        RangeSpec::ByLex { min, max }
    } else {
        RangeSpec::ByRank {
            start: parse_i64(start_raw)?,
            stop: parse_i64(stop_raw)?,
        }
    };

    Ok(ParsedZRange {
        spec,
        reverse,
        limit,
        withscores,
    })
}

fn parse_legacy_range_tail(args: &[Frame<'_>], lex: bool) -> SenkoResult<LegacyTail> {
    let mut index = 0;
    let mut withscores = false;
    let mut limit = None;
    while index < args.len() {
        let token = arg_bytes(&args[index])?;
        if token.eq_ignore_ascii_case(b"withscores") && !lex {
            withscores = true;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"limit") {
            if index + 2 >= args.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            limit = Some((
                parse_i64(arg_bytes(&args[index + 1])?)?,
                parse_i64(arg_bytes(&args[index + 2])?)?,
            ));
            index += 3;
        } else {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
    }
    Ok(LegacyTail { limit, withscores })
}

fn apply_limit(
    entries: Vec<(f64, CompactString)>,
    limit: Option<(i64, i64)>,
) -> SenkoResult<Vec<(f64, CompactString)>> {
    let Some((offset, count)) = limit else {
        return Ok(entries);
    };
    if offset < 0 || count < -1 {
        return Err(SenkoError::Protocol("ERR value is out of range"));
    }
    let skip = offset as usize;
    let iter = entries.into_iter().skip(skip);
    if count == -1 {
        Ok(iter.collect())
    } else {
        Ok(iter.take(count as usize).collect())
    }
}

fn range_entries_response(entries: Vec<RangeEntry>) -> Response {
    let mut out = SmallVec::<[Response; 16]>::new();
    for entry in entries {
        out.push(Response::Value(Some(SenkoValue::Raw(
            entry.member.as_bytes().to_vec().into(),
        ))));
        if let Some(score) = entry.score {
            out.push(Response::Value(Some(formatted_score_value(score))));
        }
    }
    Response::Array(Box::new(out))
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
    use crate::commands::zset::basic::zadd;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn resp_bytes(response: &Response) -> Vec<Vec<u8>> {
        let Response::Array(values) = response else {
            panic!("expected array");
        };
        values
            .iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                Response::Integer(value) => value.to_string().into_bytes(),
                other => panic!("unexpected response: {other:?}"),
            })
            .collect()
    }

    fn seed_rank_store() -> Store {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"k"),
                bs(b"1"),
                bs(b"a"),
                bs(b"2"),
                bs(b"b"),
                bs(b"3"),
                bs(b"c"),
                bs(b"4"),
                bs(b"d"),
                bs(b"5"),
                bs(b"e"),
            ],
        )
        .unwrap();
        store
    }

    #[test]
    fn zrange_by_rank_supports_negative_indices_and_empty_ranges() {
        let mut store = seed_rank_store();
        assert_eq!(
            resp_bytes(&zrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec()
            ]
        );
        assert_eq!(
            resp_bytes(&zrange(&mut store, &[bs(b"k"), bs(b"-2"), bs(b"-1")]).unwrap()),
            vec![b"d".to_vec(), b"e".to_vec()]
        );
        assert_eq!(
            resp_bytes(&zrange(&mut store, &[bs(b"k"), bs(b"4"), bs(b"2")]).unwrap()),
            Vec::<Vec<u8>>::new()
        );
    }

    #[test]
    fn zrange_byscore_supports_bounds_and_infinities() {
        let mut store = seed_rank_store();
        assert_eq!(
            resp_bytes(
                &zrange(
                    &mut store,
                    &[bs(b"k"), bs(b"(1"), bs(b"+inf"), bs(b"BYSCORE")]
                )
                .unwrap()
            ),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
        );
    }

    #[test]
    fn zrange_bylex_works_on_equal_score_sets() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"k"),
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
            resp_bytes(
                &zrange(&mut store, &[bs(b"k"), bs(b"[a"), bs(b"[z"), bs(b"BYLEX")]).unwrap()
            ),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn zrange_rev_reverses_order() {
        let mut store = seed_rank_store();
        assert_eq!(
            resp_bytes(
                &zrange(
                    &mut store,
                    &[bs(b"k"), bs(b"5"), bs(b"2"), bs(b"BYSCORE"), bs(b"REV")]
                )
                .unwrap()
            ),
            vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn zrange_limit_applies_offset_and_count() {
        let mut store = Store::default();
        let mut args = vec![bs(b"k")];
        for i in 0..10 {
            args.push(Frame::BulkString(
                Box::leak(i.to_string().into_boxed_str()).as_bytes(),
            ));
            args.push(Frame::BulkString(
                Box::leak(format!("m{i}").into_boxed_str()).as_bytes(),
            ));
        }
        let _ = zadd(&mut store, &args).unwrap();
        assert_eq!(
            resp_bytes(
                &zrange(
                    &mut store,
                    &[
                        bs(b"k"),
                        bs(b"-inf"),
                        bs(b"+inf"),
                        bs(b"BYSCORE"),
                        bs(b"LIMIT"),
                        bs(b"2"),
                        bs(b"3")
                    ]
                )
                .unwrap()
            ),
            vec![b"m2".to_vec(), b"m3".to_vec(), b"m4".to_vec()]
        );
        assert_eq!(
            resp_bytes(
                &zrange(
                    &mut store,
                    &[
                        bs(b"k"),
                        bs(b"-inf"),
                        bs(b"+inf"),
                        bs(b"BYSCORE"),
                        bs(b"LIMIT"),
                        bs(b"8"),
                        bs(b"-1")
                    ]
                )
                .unwrap()
            ),
            vec![b"m8".to_vec(), b"m9".to_vec()]
        );
    }

    #[test]
    fn zrevrange_uses_reverse_rank_indices() {
        let mut store = seed_rank_store();
        assert_eq!(
            resp_bytes(&zrevrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"0")]).unwrap()),
            vec![b"e".to_vec()]
        );
    }

    #[test]
    fn zrevrangebyscore_uses_max_min_argument_order() {
        let mut store = seed_rank_store();
        assert_eq!(
            resp_bytes(&zrevrangebyscore(&mut store, &[bs(b"k"), bs(b"4"), bs(b"2")]).unwrap()),
            vec![b"d".to_vec(), b"c".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn zrangestore_supports_dst_equal_src() {
        let mut store = seed_rank_store();
        assert_eq!(
            zrangestore(&mut store, &[bs(b"k"), bs(b"k"), bs(b"1"), bs(b"3")]).unwrap(),
            Response::Integer(3)
        );
        assert_eq!(
            resp_bytes(&zrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[test]
    fn zrangestore_empty_result_deletes_destination() {
        let mut store = seed_rank_store();
        let _ = zadd(&mut store, &[bs(b"dst"), bs(b"1"), bs(b"x")]).unwrap();
        assert_eq!(
            zrangestore(&mut store, &[bs(b"dst"), bs(b"k"), bs(b"9"), bs(b"10")]).unwrap(),
            Response::Integer(0)
        );
        assert!(store.get_zset(b"dst").is_none());
    }

    #[test]
    fn legacy_commands_match_zrange_equivalents() {
        let mut store = seed_rank_store();
        let modern = zrange(
            &mut store,
            &[
                bs(b"k"),
                bs(b"2"),
                bs(b"4"),
                bs(b"BYSCORE"),
                bs(b"WITHSCORES"),
                bs(b"LIMIT"),
                bs(b"0"),
                bs(b"2"),
            ],
        )
        .unwrap();
        let legacy = zrangebyscore(
            &mut store,
            &[
                bs(b"k"),
                bs(b"2"),
                bs(b"4"),
                bs(b"WITHSCORES"),
                bs(b"LIMIT"),
                bs(b"0"),
                bs(b"2"),
            ],
        )
        .unwrap();
        assert_eq!(modern, legacy);
    }

    #[test]
    fn withscores_formats_special_values_and_decimals() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"k"),
                bs(b"+inf"),
                bs(b"a"),
                bs(b"-inf"),
                bs(b"b"),
                bs(b"1.5"),
                bs(b"c"),
            ],
        )
        .unwrap();
        assert_eq!(
            resp_bytes(
                &zrange(
                    &mut store,
                    &[bs(b"k"), bs(b"0"), bs(b"-1"), bs(b"WITHSCORES")]
                )
                .unwrap()
            ),
            vec![
                b"b".to_vec(),
                b"-inf".to_vec(),
                b"c".to_vec(),
                b"1.5".to_vec(),
                b"a".to_vec(),
                b"+inf".to_vec()
            ]
        );
    }

    #[test]
    fn limit_with_rank_range_returns_exact_redis_error() {
        let mut store = seed_rank_store();
        assert!(matches!(
            zrange(
                &mut store,
                &[
                    bs(b"k"),
                    bs(b"0"),
                    bs(b"-1"),
                    bs(b"LIMIT"),
                    bs(b"1"),
                    bs(b"2")
                ]
            ),
            Err(SenkoError::Protocol(
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
            ))
        ));
    }
}
