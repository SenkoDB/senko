use std::str;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoResult, SenkoValue, ZAddCond, ZAddOptions};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    store::Store,
    zset::{parse_lex_bound, parse_score_bound},
};

#[inline]
pub fn zadd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zadd' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;

    let parsed = parse_zadd_args(&args[1..])?;
    if parsed.incr {
        let score = parse_score_arg(arg_bytes(&parsed.pairs[0].0)?)?;
        let member = parse_compact(arg_bytes(&parsed.pairs[0].1)?);
        let existing = store
            .get_zset(key)
            .and_then(|zset| zset.score(member.as_bytes()));
        let should_skip = match parsed.condition {
            ZAddCond::NX => existing.is_some(),
            ZAddCond::XX => existing.is_none(),
            ZAddCond::Always => false,
        };
        if should_skip {
            return Ok(Response::Value(None));
        }
        let new_score = existing.unwrap_or(0.0) + score;
        if new_score.is_nan() {
            return Err(SenkoError::Protocol(
                "ERR resulting score is not a number (NaN)",
            ));
        }

        let zset = store.get_or_create_zset(parse_compact(key));
        let result = zset.add(
            score,
            member,
            ZAddOptions {
                condition: parsed.condition,
                gt: parsed.gt,
                lt: parsed.lt,
                ch: parsed.ch,
                incr: true,
            },
        );
        return Ok(Response::Value(result.new_score.map(formatted_score_value)));
    }

    if store.get_zset(key).is_none() && matches!(parsed.condition, ZAddCond::XX) {
        return Ok(Response::Integer(0));
    }

    let zset = store.get_or_create_zset(parse_compact(key));
    let mut total = 0_i64;
    for (score_frame, member_frame) in parsed.pairs {
        let score = parse_score_arg(arg_bytes(score_frame)?)?;
        let member = parse_compact(arg_bytes(member_frame)?);
        let result = zset.add(
            score,
            member,
            ZAddOptions {
                condition: parsed.condition,
                gt: parsed.gt,
                lt: parsed.lt,
                ch: parsed.ch,
                incr: false,
            },
        );
        total += if parsed.ch {
            result.changed as i64
        } else {
            result.added as i64
        };
    }

    Ok(Response::Integer(total))
}

#[inline]
pub fn zrem(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zrem' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;

    let mut removed = 0_i64;
    if let Some(zset) = store.get_zset_mut(key) {
        for member in &args[1..] {
            if zset.remove(arg_bytes(member)?).is_some() {
                removed += 1;
            }
        }
    }
    store.remove_zset_if_empty(key);
    Ok(Response::Integer(removed))
}

#[inline]
pub fn zscore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zscore' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let member = arg_bytes(&args[1])?;
    ensure_zset_type_or_missing(store, key)?;

    Ok(Response::Value(
        store
            .get_zset(key)
            .and_then(|zset| zset.score(member))
            .map(formatted_score_value),
    ))
}

#[inline]
pub fn zmscore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zmscore' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(zset) = store.get_zset(key) {
        for member in &args[1..] {
            out.push(Response::Value(
                zset.score(arg_bytes(member)?).map(formatted_score_value),
            ));
        }
    } else {
        out.resize(args.len() - 1, Response::Value(None));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn zincrby(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zincrby' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let increment = parse_score_arg(arg_bytes(&args[1])?)?;
    let member = parse_compact(arg_bytes(&args[2])?);
    ensure_zset_type_or_missing(store, key)?;

    let existing = store
        .get_zset(key)
        .and_then(|zset| zset.score(member.as_bytes()));
    let new_score = existing.unwrap_or(0.0) + increment;
    if new_score.is_nan() {
        return Err(SenkoError::Protocol(
            "ERR resulting score is not a number (NaN)",
        ));
    }

    let zset = store.get_or_create_zset(parse_compact(key));
    let result = zset.add(
        increment,
        member,
        ZAddOptions {
            incr: true,
            ..Default::default()
        },
    );
    Ok(Response::Value(result.new_score.map(formatted_score_value)))
}

#[inline]
pub fn zcard(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zcard' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    Ok(Response::Integer(
        store.get_zset(key).map_or(0, |zset| zset.len() as i64),
    ))
}

#[inline]
pub fn zrank(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    rank_impl(store, args, false, "zrank")
}

#[inline]
pub fn zrevrank(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    rank_impl(store, args, true, "zrevrank")
}

#[inline]
pub fn zcount(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zcount' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let min = parse_score_bound(arg_bytes(&args[1])?)?;
    let max = parse_score_bound(arg_bytes(&args[2])?)?;
    Ok(Response::Integer(
        store
            .get_zset(key)
            .map_or(0, |zset| zset.count_by_score(min, max) as i64),
    ))
}

#[inline]
pub fn zlexcount(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zlexcount' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_zset_type_or_missing(store, key)?;
    let min = parse_lex_bound(arg_bytes(&args[1])?)?;
    let max = parse_lex_bound(arg_bytes(&args[2])?)?;
    Ok(Response::Integer(
        store
            .get_zset(key)
            .map_or(0, |zset| zset.count_by_lex(min, max) as i64),
    ))
}

fn rank_impl(
    store: &mut Store,
    args: &[Frame<'_>],
    reverse: bool,
    command: &'static str,
) -> SenkoResult<Response> {
    if args.len() != 2 && args.len() != 3 {
        return Err(SenkoError::Protocol(match command {
            "zrank" => "wrong number of arguments for 'zrank' command",
            _ => "wrong number of arguments for 'zrevrank' command",
        }));
    }

    let key = arg_bytes(&args[0])?;
    let member = arg_bytes(&args[1])?;
    ensure_zset_type_or_missing(store, key)?;

    let withscore = if args.len() == 3 {
        if !arg_bytes(&args[2])?.eq_ignore_ascii_case(b"withscore") {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }
        true
    } else {
        false
    };

    let Some(zset) = store.get_zset(key) else {
        return Ok(Response::Value(None));
    };
    let Some(rank) = zset.rank(member, reverse) else {
        return Ok(Response::Value(None));
    };

    if !withscore {
        return Ok(Response::Integer(rank as i64));
    }

    let Some(score) = zset.score(member) else {
        return Ok(Response::Value(None));
    };
    let mut out = SmallVec::<[Response; 16]>::new();
    out.push(Response::Integer(rank as i64));
    out.push(Response::Value(Some(formatted_score_value(score))));
    Ok(Response::Array(Box::new(out)))
}

pub(crate) fn ensure_zset_type_or_missing(store: &mut Store, key: &[u8]) -> SenkoResult<()> {
    if let Some(value) = store.get(key).cloned()
        && !matches!(value, SenkoValue::ZSet(_))
    {
        return Err(wrong_type(&value));
    }
    Ok(())
}

pub(crate) fn wrong_type(value: &SenkoValue) -> SenkoError {
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
        expected: "zset",
        actual,
    }
}

struct ParsedZAddArgs<'a> {
    condition: ZAddCond,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
    pairs: Vec<(&'a Frame<'a>, &'a Frame<'a>)>,
}

fn parse_zadd_args<'a>(args: &'a [Frame<'a>]) -> SenkoResult<ParsedZAddArgs<'a>> {
    let mut index = 0;
    let mut condition = ZAddCond::Always;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    while index < args.len() {
        let raw = arg_bytes(&args[index])?;
        if raw.eq_ignore_ascii_case(b"nx") {
            if !matches!(condition, ZAddCond::Always) {
                return Err(SenkoError::Protocol(
                    "ERR XX and NX options at the same time are not compatible",
                ));
            }
            condition = ZAddCond::NX;
        } else if raw.eq_ignore_ascii_case(b"xx") {
            if !matches!(condition, ZAddCond::Always) {
                return Err(SenkoError::Protocol(
                    "ERR XX and NX options at the same time are not compatible",
                ));
            }
            condition = ZAddCond::XX;
        } else if raw.eq_ignore_ascii_case(b"gt") {
            gt = true;
        } else if raw.eq_ignore_ascii_case(b"lt") {
            lt = true;
        } else if raw.eq_ignore_ascii_case(b"ch") {
            ch = true;
        } else if raw.eq_ignore_ascii_case(b"incr") {
            incr = true;
        } else {
            break;
        }
        index += 1;
    }

    if gt && lt {
        return Err(SenkoError::Protocol(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if matches!(condition, ZAddCond::NX) && (gt || lt) {
        return Err(SenkoError::Protocol(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }

    let remaining = &args[index..];
    if remaining.len() < 2 || remaining.len() % 2 != 0 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'zadd' command",
        ));
    }
    if incr && remaining.len() != 2 {
        return Err(SenkoError::Protocol(
            "ERR INCR option supports a single increment-element pair",
        ));
    }

    let mut pairs = Vec::with_capacity(remaining.len() / 2);
    let mut it = remaining.iter();
    while let (Some(score), Some(member)) = (it.next(), it.next()) {
        pairs.push((score, member));
    }

    Ok(ParsedZAddArgs {
        condition,
        gt,
        lt,
        ch,
        incr,
        pairs,
    })
}

pub(crate) fn parse_score_arg(raw: &[u8]) -> SenkoResult<f64> {
    if raw.eq_ignore_ascii_case(b"+inf") {
        return Ok(f64::INFINITY);
    }
    if raw.eq_ignore_ascii_case(b"-inf") {
        return Ok(f64::NEG_INFINITY);
    }
    let value = fast_float::parse::<f64, _>(raw)
        .map_err(|_| SenkoError::Protocol("ERR not a float or out of range"))?;
    if value.is_nan() {
        return Err(SenkoError::Protocol("ERR not a float or out of range"));
    }
    Ok(value)
}

pub(crate) fn formatted_score_value(score: f64) -> SenkoValue {
    if score.is_infinite() {
        return if score.is_sign_positive() {
            SenkoValue::Raw(Bytes::from_static(b"+inf"))
        } else {
            SenkoValue::Raw(Bytes::from_static(b"-inf"))
        };
    }

    let mut buf = ryu::Buffer::new();
    let raw = buf.format_finite(score);
    if raw
        .as_bytes()
        .iter()
        .any(|byte| *byte == b'e' || *byte == b'E')
    {
        SenkoValue::Raw(Bytes::from(expand_scientific(raw)))
    } else {
        SenkoValue::Raw(Bytes::from(trim_decimal(raw.to_owned())))
    }
}

fn expand_scientific(raw: &str) -> String {
    let mut chars = raw.chars();
    let negative = matches!(chars.next(), Some('-'));
    let body = if negative { &raw[1..] } else { raw };
    let (mantissa, exponent_raw) = body.split_once(['e', 'E']).unwrap_or((body, "0"));
    let exponent: i32 = exponent_raw.parse().unwrap_or(0);
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int_part}{frac_part}");
    let point = int_part.len() as i32;
    let new_point = point + exponent;

    let mut out = String::new();
    if negative {
        out.push('-');
    }

    if new_point <= 0 {
        out.push_str("0.");
        for _ in 0..(-new_point) {
            out.push('0');
        }
        out.push_str(&digits);
    } else if new_point as usize >= digits.len() {
        out.push_str(&digits);
        for _ in 0..(new_point as usize - digits.len()) {
            out.push('0');
        }
    } else {
        let split = new_point as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }

    if out.contains('.') {
        while out.ends_with('0') {
            out.pop();
        }
        if out.ends_with('.') {
            out.pop();
        }
    }
    if out == "-0" {
        out.clear();
        out.push('0');
    }
    out
}

fn trim_decimal(mut raw: String) -> String {
    if raw.contains('.') {
        while raw.ends_with('0') {
            raw.pop();
        }
        if raw.ends_with('.') {
            raw.pop();
        }
    }
    if raw == "-0" {
        raw.clear();
        raw.push('0');
    }
    raw
}

pub(crate) fn parse_compact(raw: &[u8]) -> CompactString {
    CompactString::from(String::from_utf8_lossy(raw).as_ref())
}

pub(crate) fn arg_bytes<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes),
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::WrongType {
            expected: "string",
            actual: frame_type_name(frame),
        }),
    }
}

pub(crate) fn frame_type_name(frame: &Frame<'_>) -> &'static str {
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
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_proto::Frame;

    use super::*;
    use crate::store::SetOptions;

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn raw_bytes(resp: &Response) -> Option<&[u8]> {
        match resp {
            Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.as_ref()),
            _ => None,
        }
    }

    #[test]
    fn zadd_nx_updates_only_new_members() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]).unwrap();
        assert_eq!(
            zadd(
                &mut store,
                &[bs(b"k"), bs(b"NX"), bs(b"2"), bs(b"a"), bs(b"3"), bs(b"b")]
            )
            .unwrap(),
            Response::Integer(1)
        );
        assert_eq!(
            zscore(&mut store, &[bs(b"k"), bs(b"a")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"1"))))
        );
        assert_eq!(
            zscore(&mut store, &[bs(b"k"), bs(b"b")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"3"))))
        );
    }

    #[test]
    fn zadd_xx_skips_new_and_updates_existing() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]).unwrap();
        assert_eq!(
            zadd(
                &mut store,
                &[bs(b"k"), bs(b"XX"), bs(b"2"), bs(b"a"), bs(b"3"), bs(b"b")]
            )
            .unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            raw_bytes(&zscore(&mut store, &[bs(b"k"), bs(b"a")]).unwrap()),
            Some(b"2".as_slice())
        );
        assert_eq!(
            zscore(&mut store, &[bs(b"k"), bs(b"b")]).unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn zadd_gt_and_lt_apply_conditionally() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]).unwrap();
        assert_eq!(
            zadd(&mut store, &[bs(b"k"), bs(b"GT"), bs(b"0"), bs(b"a")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            zadd(&mut store, &[bs(b"k"), bs(b"GT"), bs(b"2"), bs(b"a")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            raw_bytes(&zscore(&mut store, &[bs(b"k"), bs(b"a")]).unwrap()),
            Some(b"2".as_slice())
        );
        assert_eq!(
            zadd(&mut store, &[bs(b"k"), bs(b"LT"), bs(b"3"), bs(b"a")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            zadd(&mut store, &[bs(b"k"), bs(b"LT"), bs(b"1"), bs(b"a")]).unwrap(),
            Response::Integer(0)
        );
        assert_eq!(
            raw_bytes(&zscore(&mut store, &[bs(b"k"), bs(b"a")]).unwrap()),
            Some(b"1".as_slice())
        );
    }

    #[test]
    fn zadd_ch_counts_added_plus_updated() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]).unwrap();
        assert_eq!(
            zadd(
                &mut store,
                &[bs(b"k"), bs(b"CH"), bs(b"2"), bs(b"a"), bs(b"3"), bs(b"b")]
            )
            .unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn zadd_incr_returns_null_when_nx_prevents_update() {
        let mut store = Store::default();
        let _ = zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]).unwrap();
        assert_eq!(
            zadd(
                &mut store,
                &[bs(b"k"), bs(b"NX"), bs(b"INCR"), bs(b"2"), bs(b"a")]
            )
            .unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn zadd_accepts_infinities_and_rejects_nan() {
        let mut store = Store::default();
        assert_eq!(
            zadd(
                &mut store,
                &[bs(b"k"), bs(b"+inf"), bs(b"a"), bs(b"-inf"), bs(b"b")]
            )
            .unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            raw_bytes(&zscore(&mut store, &[bs(b"k"), bs(b"a")]).unwrap()),
            Some(b"+inf".as_slice())
        );
        assert!(matches!(
            zadd(&mut store, &[bs(b"k"), bs(b"nan"), bs(b"c")]),
            Err(SenkoError::Protocol("ERR not a float or out of range"))
        ));
    }

    #[test]
    fn zincrby_creates_missing_member() {
        let mut store = Store::default();
        assert_eq!(
            zincrby(&mut store, &[bs(b"k"), bs(b"2.5"), bs(b"a")]).unwrap(),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"2.5"))))
        );
    }

    #[test]
    fn zincrby_infinity_nan_result_is_rejected() {
        let mut store = Store::default();
        let _ = zincrby(&mut store, &[bs(b"k"), bs(b"+inf"), bs(b"a")]).unwrap();
        assert!(matches!(
            zincrby(&mut store, &[bs(b"k"), bs(b"-inf"), bs(b"a")]),
            Err(SenkoError::Protocol(
                "ERR resulting score is not a number (NaN)"
            ))
        ));
    }

    #[test]
    fn zrank_withscore_returns_rank_and_score() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[bs(b"k"), bs(b"1"), bs(b"a"), bs(b"2"), bs(b"b")],
        )
        .unwrap();
        let Response::Array(values) =
            zrank(&mut store, &[bs(b"k"), bs(b"b"), bs(b"WITHSCORE")]).unwrap()
        else {
            panic!("expected array");
        };
        assert_eq!(values[0], Response::Integer(1));
        assert_eq!(
            values[1],
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"2"))))
        );
    }

    #[test]
    fn zcount_exclusive_bounds_exclude_endpoints() {
        let mut store = Store::default();
        let _ = zadd(
            &mut store,
            &[
                bs(b"k"),
                bs(b"1"),
                bs(b"a"),
                bs(b"1.0000001"),
                bs(b"b"),
                bs(b"3"),
                bs(b"c"),
            ],
        )
        .unwrap();
        assert_eq!(
            zcount(&mut store, &[bs(b"k"), bs(b"(1"), bs(b"(3")]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn zlexcount_works_for_equal_scores() {
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
            zlexcount(&mut store, &[bs(b"k"), bs(b"[a"), bs(b"[b")]).unwrap(),
            Response::Integer(2)
        );
    }

    #[test]
    fn wrongtype_is_reported_for_all_commands() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"value")),
            SetOptions::default(),
        );

        let commands = [
            zadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]),
            zrem(&mut store, &[bs(b"k"), bs(b"a")]),
            zscore(&mut store, &[bs(b"k"), bs(b"a")]),
            zmscore(&mut store, &[bs(b"k"), bs(b"a")]),
            zincrby(&mut store, &[bs(b"k"), bs(b"1"), bs(b"a")]),
            zcard(&mut store, &[bs(b"k")]),
            zrank(&mut store, &[bs(b"k"), bs(b"a")]),
            zrevrank(&mut store, &[bs(b"k"), bs(b"a")]),
            zcount(&mut store, &[bs(b"k"), bs(b"-inf"), bs(b"+inf")]),
            zlexcount(&mut store, &[bs(b"k"), bs(b"-"), bs(b"+")]),
        ];
        for result in commands {
            assert!(matches!(
                result,
                Err(SenkoError::WrongType {
                    expected: "zset",
                    actual: "string"
                })
            ));
        }
    }

    #[test]
    fn wrong_arity_uses_redis_strings() {
        let mut store = Store::default();
        assert!(matches!(
            zadd(&mut store, &[bs(b"k"), bs(b"1")]),
            Err(SenkoError::Protocol(
                "wrong number of arguments for 'zadd' command"
            ))
        ));
        assert!(matches!(
            zscore(&mut store, &[bs(b"k")]),
            Err(SenkoError::Protocol(
                "wrong number of arguments for 'zscore' command"
            ))
        ));
    }
}
