use std::cmp::Ordering;

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::{HashMap, HashSet};
use senko_core::{SenkoError, SenkoResult, SenkoValue, ZSetEncoding, ZSetObject};
use senko_proto::Frame;

use crate::{
    commands::Response,
    commands::zset::basic::{arg_bytes, formatted_score_value, parse_compact},
    store::{SetOptions, Store},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateMode {
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZAlgebraStrategy {
    SmallListpack,
    HashProbe,
    BPTreeMerge,
}

pub fn aggregate_scores(mode: AggregateMode, a: f64, b: f64) -> f64 {
    match mode {
        AggregateMode::Sum => a + b,
        AggregateMode::Min => match (a.is_nan(), b.is_nan()) {
            (true, true) => f64::NAN,
            (true, false) => b,
            (false, true) => a,
            (false, false) => {
                if a.total_cmp(&b).is_le() {
                    a
                } else {
                    b
                }
            }
        },
        AggregateMode::Max => match (a.is_nan(), b.is_nan()) {
            (true, true) => f64::NAN,
            (true, false) => b,
            (false, true) => a,
            (false, false) => {
                if a.total_cmp(&b).is_ge() {
                    a
                } else {
                    b
                }
            }
        },
    }
}

pub fn zset_algebra_strategy(sets: &[&ZSetObject]) -> ZAlgebraStrategy {
    if sets.is_empty() {
        return ZAlgebraStrategy::SmallListpack;
    }
    if sets
        .iter()
        .all(|set| matches!(set.inner, ZSetEncoding::Listpack(_)))
    {
        return ZAlgebraStrategy::SmallListpack;
    }
    let total_len: usize = sets.iter().map(|set| set.len()).sum();
    if sets.len() >= 4 && total_len >= 4096 {
        return ZAlgebraStrategy::BPTreeMerge;
    }
    ZAlgebraStrategy::HashProbe
}

#[inline]
pub fn zdiff(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zdiff' command",
        ));
    }
    let parsed = parse_numkey_sources(&args[0], &args[1..], false, "zdiff")?;
    let withscores = match parsed.tail {
        [] => false,
        [frame] if arg_bytes(frame)?.eq_ignore_ascii_case(b"withscores") => true,
        _ => return Err(SenkoError::Protocol("ERR syntax error")),
    };
    let sets = collect_zset_keys(store, &parsed.keys)?;
    Ok(zset_entries_response(compute_zdiff(&sets), withscores))
}

#[inline]
pub fn zdiffstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zdiffstore' command",
        ));
    }
    let destination = parse_compact(arg_bytes(&args[0])?);
    let parsed = parse_numkey_sources(&args[1], &args[2..], true, "zdiffstore")?;
    if !parsed.tail.is_empty() {
        return Err(SenkoError::Protocol("ERR syntax error"));
    }
    let sets = collect_zset_keys(store, &parsed.keys)?;
    let result = compute_zdiff(&sets);
    Ok(Response::Integer(
        store_zset_result(store, destination, result) as i64,
    ))
}

#[inline]
pub fn zinter(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zset_multi_read(store, args, "zinter", compute_zinter)
}

#[inline]
pub fn zinterstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zset_multi_store(store, args, "zinterstore", compute_zinter)
}

#[inline]
pub fn zunion(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zset_multi_read(store, args, "zunion", compute_zunion)
}

#[inline]
pub fn zunionstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    zset_multi_store(store, args, "zunionstore", compute_zunion)
}

#[inline]
pub fn zintercard(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zintercard' command",
        ));
    }
    let parsed = parse_numkey_sources(&args[0], &args[1..], false, "zintercard")?;
    let limit = match parsed.tail {
        [] => None,
        [token, value] if arg_bytes(token)?.eq_ignore_ascii_case(b"limit") => {
            Some(parse_non_negative_i64(arg_bytes(value)?)? as usize)
        }
        _ => return Err(SenkoError::Protocol("ERR syntax error")),
    };
    let sets = collect_zset_keys(store, &parsed.keys)?;
    let result = compute_zintercard(&sets, limit.unwrap_or(0));
    Ok(Response::Integer(result as i64))
}

fn zset_multi_read<F>(
    store: &mut Store,
    args: &[Frame<'_>],
    command: &'static str,
    compute: F,
) -> SenkoResult<Response>
where
    F: Fn(&[Option<ZSetObject>], &[f64], AggregateMode) -> Vec<(f64, CompactString)>,
{
    if args.len() < 2 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }
    let parsed = parse_weighted_args(args, command)?;
    let sets = collect_zset_keys(store, &parsed.keys)?;
    let entries = compute(&sets, &parsed.weights, parsed.aggregate);
    Ok(zset_entries_response(entries, parsed.withscores))
}

fn zset_multi_store<F>(
    store: &mut Store,
    args: &[Frame<'_>],
    command: &'static str,
    compute: F,
) -> SenkoResult<Response>
where
    F: Fn(&[Option<ZSetObject>], &[f64], AggregateMode) -> Vec<(f64, CompactString)>,
{
    if args.len() < 3 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(format!(
            "wrong number of arguments for '{command}' command"
        ))));
    }
    let destination = parse_compact(arg_bytes(&args[0])?);
    let parsed = parse_weighted_args(&args[1..], command)?;
    let sets = collect_zset_keys(store, &parsed.keys)?;
    let entries = compute(&sets, &parsed.weights, parsed.aggregate);
    Ok(Response::Integer(
        store_zset_result(store, destination, entries) as i64,
    ))
}

fn compute_zdiff(sets: &[Option<ZSetObject>]) -> Vec<(f64, CompactString)> {
    let Some(Some(first)) = sets.first() else {
        return Vec::new();
    };
    let mut excluded = HashSet::<CompactString, RandomState>::with_hasher(RandomState::default());
    for set in sets.iter().skip(1).flatten() {
        for (_, member) in zset_entries(set) {
            excluded.insert(member);
        }
    }

    zset_entries(first)
        .into_iter()
        .filter(|(_, member)| !excluded.contains(member))
        .collect()
}

fn compute_zinter(
    sets: &[Option<ZSetObject>],
    weights: &[f64],
    aggregate: AggregateMode,
) -> Vec<(f64, CompactString)> {
    if sets.is_empty() || sets.iter().any(Option::is_none) {
        return Vec::new();
    }

    let mut ordered: Vec<(usize, &ZSetObject)> = sets
        .iter()
        .enumerate()
        .filter_map(|(index, set)| set.as_ref().map(|set| (index, set)))
        .collect();
    ordered.sort_by_key(|(_, set)| set.len());
    if ordered.first().is_some_and(|(_, set)| set.is_empty()) {
        return Vec::new();
    }

    let (base_index, base) = ordered[0];
    let mut candidates: Vec<(CompactString, f64)> = zset_entries(base)
        .into_iter()
        .map(|(score, member)| (member, score * weights[base_index]))
        .collect();

    for (index, set) in ordered.iter().skip(1) {
        let weight = weights[*index];
        candidates.retain_mut(|(member, current)| {
            let Some(score) = set.score(member.as_bytes()) else {
                return false;
            };
            *current = aggregate_scores(aggregate, *current, score * weight);
            true
        });
        if candidates.is_empty() {
            break;
        }
    }

    let mut out: Vec<_> = candidates
        .into_iter()
        .map(|(member, score)| (score, member))
        .collect();
    sort_entries(&mut out);
    out
}

fn compute_zintercard(sets: &[Option<ZSetObject>], limit: usize) -> usize {
    if sets.is_empty() || sets.iter().any(Option::is_none) {
        return 0;
    }

    let mut ordered: Vec<&ZSetObject> = sets.iter().filter_map(Option::as_ref).collect();
    ordered.sort_by_key(|set| set.len());
    let Some(base) = ordered.first() else {
        return 0;
    };
    if base.is_empty() {
        return 0;
    }

    let mut count = 0usize;
    'outer: for (_, member) in zset_entries(base) {
        for set in ordered.iter().skip(1) {
            if set.score(member.as_bytes()).is_none() {
                continue 'outer;
            }
        }
        count += 1;
        if limit > 0 && count >= limit {
            break;
        }
    }
    count
}

fn compute_zunion(
    sets: &[Option<ZSetObject>],
    weights: &[f64],
    aggregate: AggregateMode,
) -> Vec<(f64, CompactString)> {
    let refs: Vec<&ZSetObject> = sets.iter().filter_map(Option::as_ref).collect();
    let capacity: usize = refs.iter().map(|set| set.len()).sum();
    let mut result = HashMap::<CompactString, f64, RandomState>::with_capacity_and_hasher(
        capacity,
        RandomState::default(),
    );

    match zset_algebra_strategy(&refs) {
        ZAlgebraStrategy::SmallListpack
        | ZAlgebraStrategy::HashProbe
        | ZAlgebraStrategy::BPTreeMerge => {
            for (index, set) in sets
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_ref().map(|s| (i, s)))
            {
                let weight = weights[index];
                for (score, member) in zset_entries(set) {
                    let weighted = score * weight;
                    result
                        .entry(member)
                        .and_modify(|existing| {
                            *existing = aggregate_scores(aggregate, *existing, weighted)
                        })
                        .or_insert(weighted);
                }
            }
        }
    }

    let mut out: Vec<_> = result
        .into_iter()
        .map(|(member, score)| (score, member))
        .collect();
    sort_entries(&mut out);
    out
}

fn zset_entries_response(entries: Vec<(f64, CompactString)>, withscores: bool) -> Response {
    Response::Array(Box::new(
        entries
            .into_iter()
            .flat_map(|(score, member)| {
                let first = Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(
                    member.as_bytes(),
                ))));
                if withscores {
                    vec![first, Response::Value(Some(formatted_score_value(score)))]
                } else {
                    vec![first]
                }
            })
            .collect(),
    ))
}

fn zset_entries(set: &ZSetObject) -> Vec<(f64, CompactString)> {
    set.range_by_rank(0, -1, false, None).collect()
}

fn sort_entries(entries: &mut [(f64, CompactString)]) {
    entries.sort_by(
        |(score_a, member_a), (score_b, member_b)| match score_a.total_cmp(score_b) {
            Ordering::Equal => member_a.as_str().cmp(member_b.as_str()),
            other => other,
        },
    );
}

fn store_zset_result(
    store: &mut Store,
    destination: CompactString,
    entries: Vec<(f64, CompactString)>,
) -> usize {
    if entries.is_empty() {
        let _ = store.delete(destination.as_bytes());
        return 0;
    }

    let mut result = ZSetObject::default();
    for (score, member) in entries {
        let _ = result.add(score, member, Default::default());
    }
    let len = result.len();
    let _ = store.set(
        destination,
        SenkoValue::ZSet(Box::new(result)),
        SetOptions::default(),
    );
    len
}

struct ParsedNumKeys<'a> {
    keys: &'a [Frame<'a>],
    tail: &'a [Frame<'a>],
}

struct ParsedWeighted<'a> {
    keys: &'a [Frame<'a>],
    weights: Vec<f64>,
    aggregate: AggregateMode,
    withscores: bool,
}

fn parse_numkey_sources<'a>(
    numkeys_frame: &'a Frame<'a>,
    rest: &'a [Frame<'a>],
    store_variant: bool,
    _command: &'static str,
) -> SenkoResult<ParsedNumKeys<'a>> {
    let numkeys = parse_non_negative_i64(arg_bytes(numkeys_frame)?)?;
    if numkeys <= 0 {
        return Err(SenkoError::ProtocolMessage(CompactString::new(
            "ERR numkeys should be greater than 0",
        )));
    }
    let numkeys = numkeys as usize;
    if rest.len() < numkeys {
        let msg = if store_variant {
            "ERR numkeys does not match number of keys"
        } else {
            "ERR numkeys does not match number of keys"
        };
        return Err(SenkoError::ProtocolMessage(CompactString::new(msg)));
    }
    Ok(ParsedNumKeys {
        keys: &rest[..numkeys],
        tail: &rest[numkeys..],
    })
}

fn parse_weighted_args<'a>(
    args: &'a [Frame<'a>],
    command: &'static str,
) -> SenkoResult<ParsedWeighted<'a>> {
    let ParsedNumKeys { keys, tail } = parse_numkey_sources(&args[0], &args[1..], false, command)?;
    let mut weights = vec![1.0; keys.len()];
    let mut aggregate = AggregateMode::Sum;
    let mut withscores = false;
    let mut index = 0usize;

    while index < tail.len() {
        let token = arg_bytes(&tail[index])?;
        if token.eq_ignore_ascii_case(b"weights") {
            index += 1;
            if tail.len() < index + keys.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            let mut parsed = Vec::with_capacity(keys.len());
            for frame in &tail[index..index + keys.len()] {
                parsed.push(parse_weight(arg_bytes(frame)?)?);
            }
            weights = parsed;
            index += keys.len();
        } else if token.eq_ignore_ascii_case(b"aggregate") {
            index += 1;
            if index >= tail.len() {
                return Err(SenkoError::Protocol("ERR syntax error"));
            }
            aggregate = parse_aggregate(arg_bytes(&tail[index])?)?;
            index += 1;
        } else if token.eq_ignore_ascii_case(b"withscores") {
            withscores = true;
            index += 1;
        } else {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
    }

    Ok(ParsedWeighted {
        keys,
        weights,
        aggregate,
        withscores,
    })
}

fn collect_zset_keys(
    store: &mut Store,
    args: &[Frame<'_>],
) -> SenkoResult<Vec<Option<ZSetObject>>> {
    let mut out = Vec::with_capacity(args.len());
    for frame in args {
        let key = arg_bytes(frame)?;
        match store.get(key).cloned() {
            None => out.push(None),
            Some(SenkoValue::ZSet(zset)) => out.push(Some((*zset).clone())),
            Some(value) => {
                return Err(SenkoError::WrongType {
                    expected: "zset",
                    actual: actual_type(&value),
                });
            }
        }
    }
    Ok(out)
}

fn actual_type(value: &SenkoValue) -> &'static str {
    match value {
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
    }
}

fn parse_weight(raw: &[u8]) -> SenkoResult<f64> {
    if raw.eq_ignore_ascii_case(b"+inf") {
        return Ok(f64::INFINITY);
    }
    if raw.eq_ignore_ascii_case(b"-inf") {
        return Ok(f64::NEG_INFINITY);
    }
    fast_float::parse::<f64, _>(raw).map_err(|_| SenkoError::Protocol("ERR syntax error"))
}

fn parse_aggregate(raw: &[u8]) -> SenkoResult<AggregateMode> {
    if raw.eq_ignore_ascii_case(b"sum") {
        Ok(AggregateMode::Sum)
    } else if raw.eq_ignore_ascii_case(b"min") {
        Ok(AggregateMode::Min)
    } else if raw.eq_ignore_ascii_case(b"max") {
        Ok(AggregateMode::Max)
    } else {
        Err(SenkoError::Protocol("ERR syntax error"))
    }
}

fn parse_non_negative_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .ok_or_else(|| SenkoError::Protocol("value is out of range"))
}

#[cfg(test)]
mod tests {
    use compact_str::CompactString;
    use senko_core::{SenkoValue, ZSetEncoding};
    use senko_proto::Frame;

    use super::*;
    use crate::commands::zset::basic::zadd;
    use crate::commands::zset::range::zrange;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn seed(store: &mut Store, key: &'static [u8], entries: &[(&'static [u8], &'static [u8])]) {
        let mut args = vec![bs(key)];
        for (score, member) in entries {
            args.push(bs(score));
            args.push(bs(member));
        }
        let _ = zadd(store, &args).unwrap();
    }

    fn resp_bytes(response: Response) -> Vec<Vec<u8>> {
        let Response::Array(values) = response else {
            panic!("expected array");
        };
        values
            .into_iter()
            .map(|value| match value {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected response {other:?}"),
            })
            .collect()
    }

    #[test]
    fn zdiff_basic() {
        let mut store = Store::default();
        seed(
            &mut store,
            b"a",
            &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")],
        );
        seed(&mut store, b"b", &[(b"4", b"b"), (b"5", b"d")]);
        assert_eq!(
            resp_bytes(zdiff(&mut store, &[bs(b"2"), bs(b"a"), bs(b"b")]).unwrap()),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn zdiff_withscores() {
        let mut store = Store::default();
        seed(
            &mut store,
            b"a",
            &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")],
        );
        seed(&mut store, b"b", &[(b"4", b"b")]);
        assert_eq!(
            resp_bytes(
                zdiff(
                    &mut store,
                    &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"WITHSCORES")]
                )
                .unwrap()
            ),
            vec![b"a".to_vec(), b"1".to_vec(), b"c".to_vec(), b"3".to_vec()]
        );
    }

    #[test]
    fn zdiff_first_missing_is_empty() {
        let mut store = Store::default();
        assert!(
            resp_bytes(zdiff(&mut store, &[bs(b"2"), bs(b"missing"), bs(b"b")]).unwrap())
                .is_empty()
        );
    }

    #[test]
    fn zinter_sum_min_and_weights_work() {
        let mut store = Store::default();
        seed(&mut store, b"a", &[(b"1", b"x"), (b"2", b"y")]);
        seed(&mut store, b"b", &[(b"3", b"x"), (b"5", b"z")]);

        assert_eq!(
            resp_bytes(
                zinter(
                    &mut store,
                    &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"WITHSCORES")]
                )
                .unwrap()
            ),
            vec![b"x".to_vec(), b"4".to_vec()]
        );
        assert_eq!(
            resp_bytes(
                zinter(
                    &mut store,
                    &[
                        bs(b"2"),
                        bs(b"a"),
                        bs(b"b"),
                        bs(b"WEIGHTS"),
                        bs(b"2"),
                        bs(b"1"),
                        bs(b"AGGREGATE"),
                        bs(b"MIN"),
                        bs(b"WITHSCORES")
                    ]
                )
                .unwrap()
            ),
            vec![b"x".to_vec(), b"2".to_vec()]
        );
    }

    #[test]
    fn zinter_early_exit_when_any_set_empty() {
        let mut store = Store::default();
        seed(&mut store, b"a", &[(b"1", b"x")]);
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::ZSet(Box::default()),
            Default::default(),
        );
        assert!(
            resp_bytes(zinter(&mut store, &[bs(b"2"), bs(b"a"), bs(b"b")]).unwrap()).is_empty()
        );
    }

    #[test]
    fn zintercard_limit_stops_early() {
        let mut store = Store::default();
        seed(
            &mut store,
            b"a",
            &[(b"1", b"a"), (b"1", b"b"), (b"1", b"c")],
        );
        seed(
            &mut store,
            b"b",
            &[(b"2", b"a"), (b"2", b"b"), (b"2", b"c")],
        );
        assert_eq!(
            zintercard(
                &mut store,
                &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"LIMIT"), bs(b"2")]
            )
            .unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn zunion_sum_min_max_work() {
        let mut store = Store::default();
        seed(&mut store, b"a", &[(b"1", b"a"), (b"2", b"b")]);
        seed(&mut store, b"b", &[(b"4", b"b"), (b"5", b"c")]);

        assert_eq!(
            resp_bytes(
                zunion(
                    &mut store,
                    &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"WITHSCORES")]
                )
                .unwrap()
            ),
            vec![
                b"a".to_vec(),
                b"1".to_vec(),
                b"c".to_vec(),
                b"5".to_vec(),
                b"b".to_vec(),
                b"6".to_vec()
            ]
        );

        assert_eq!(
            resp_bytes(
                zunion(
                    &mut store,
                    &[
                        bs(b"2"),
                        bs(b"a"),
                        bs(b"b"),
                        bs(b"AGGREGATE"),
                        bs(b"MAX"),
                        bs(b"WITHSCORES")
                    ]
                )
                .unwrap()
            ),
            vec![
                b"a".to_vec(),
                b"1".to_vec(),
                b"b".to_vec(),
                b"4".to_vec(),
                b"c".to_vec(),
                b"5".to_vec()
            ]
        );
    }

    #[test]
    fn zunion_single_set_matches_zrange() {
        let mut store = Store::default();
        seed(&mut store, b"a", &[(b"1", b"a"), (b"2", b"b")]);
        assert_eq!(
            zunion(&mut store, &[bs(b"1"), bs(b"a"), bs(b"WITHSCORES")]).unwrap(),
            zrange(
                &mut store,
                &[bs(b"a"), bs(b"0"), bs(b"-1"), bs(b"WITHSCORES")]
            )
            .unwrap()
        );
    }

    #[test]
    fn zunionstore_small_result_uses_listpack() {
        let mut store = Store::default();
        seed(&mut store, b"a", &[(b"1", b"a"), (b"2", b"b")]);
        assert_eq!(
            zunionstore(&mut store, &[bs(b"dst"), bs(b"1"), bs(b"a")]).unwrap(),
            Response::Integer(2)
        );
        assert!(matches!(
            &store.get_zset(b"dst").unwrap().inner,
            ZSetEncoding::Listpack(_)
        ));
    }

    #[test]
    fn zdiffstore_destination_equals_source() {
        let mut store = Store::default();
        seed(
            &mut store,
            b"a",
            &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")],
        );
        seed(&mut store, b"b", &[(b"4", b"b")]);
        assert_eq!(
            zdiffstore(&mut store, &[bs(b"a"), bs(b"2"), bs(b"a"), bs(b"b")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            resp_bytes(zrange(&mut store, &[bs(b"a"), bs(b"0"), bs(b"-1")]).unwrap()),
            vec![b"a".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn weights_count_mismatch_is_syntax_error() {
        let mut store = Store::default();
        let err = zunion(
            &mut store,
            &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"WEIGHTS"), bs(b"1")],
        )
        .unwrap_err();
        assert!(matches!(err, SenkoError::Protocol("ERR syntax error")));
    }

    #[test]
    fn numkeys_mismatch_uses_exact_error() {
        let mut store = Store::default();
        let err = zunion(&mut store, &[bs(b"2"), bs(b"a")]).unwrap_err();
        assert!(matches!(
            err,
            SenkoError::ProtocolMessage(message) if message.as_str() == "ERR numkeys does not match number of keys"
        ));
    }
}
