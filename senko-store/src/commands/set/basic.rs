use bytes::Bytes;
use compact_str::CompactString;
use rand::{SeedableRng, rngs::SmallRng};
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{commands::Response, store::Store};

#[inline]
pub fn sadd(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sadd' command",
        ));
    }

    let key_bytes = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key_bytes)?;
    let set = store.get_or_create_set(parse_compact(key_bytes));

    let mut added = 0i64;
    for frame in &args[1..] {
        if set.add(arg_bytes(frame)?) {
            added += 1;
        }
    }

    Ok(Response::Integer(added))
}

#[inline]
pub fn srem(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'srem' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;

    let mut removed = 0i64;
    if let Some(set) = store.get_set_mut(key) {
        for frame in &args[1..] {
            if set.remove(arg_bytes(frame)?) {
                removed += 1;
            }
        }
    }
    store.remove_set_if_empty(key);

    Ok(Response::Integer(removed))
}

#[inline]
pub fn scard(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'scard' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;
    Ok(Response::Integer(
        store.get_set(key).map_or(0, |set| set.len() as i64),
    ))
}

#[inline]
pub fn sismember(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sismember' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    let member = arg_bytes(&args[1])?;
    ensure_set_type_or_missing(store, key)?;

    let exists = store.get_set(key).is_some_and(|set| set.contains(member));
    Ok(Response::Integer(exists as i64))
}

#[inline]
pub fn smismember(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'smismember' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;

    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(set) = store.get_set(key) {
        for frame in &args[1..] {
            out.push(Response::Integer(set.contains(arg_bytes(frame)?) as i64));
        }
    } else {
        out.resize(args.len() - 1, Response::Integer(0));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn smembers(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'smembers' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;

    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(set) = store.get_set(key) {
        out.extend(set.iter().map(bytes_response));
    }
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn srandmember(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() || args.len() > 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'srandmember' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;

    let seed = store.next_random_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let Some(set) = store.get_set(key) else {
        return Ok(if args.len() == 1 {
            Response::Value(None)
        } else {
            Response::Array(Box::new(SmallVec::new()))
        });
    };

    if args.len() == 1 {
        return Ok(Response::Value(
            set.sample_random(&mut rng)
                .map(|member| SenkoValue::Raw(Bytes::from(member.into_owned()))),
        ));
    }

    let count = parse_i64(arg_bytes(&args[1])?)?;
    if count == 0 {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    }

    let values = if count > 0 {
        set.sample_n_distinct(count as usize, &mut rng)
    } else {
        set.sample_n_repeating(count.unsigned_abs() as usize, &mut rng)
    };

    Ok(Response::Array(Box::new(
        values
            .into_iter()
            .map(|value| raw_response(&value))
            .collect(),
    )))
}

#[inline]
pub fn spop(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() || args.len() > 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'spop' command",
        ));
    }

    let key = arg_bytes(&args[0])?;
    ensure_set_type_or_missing(store, key)?;

    if args.len() == 1 {
        let seed = store.next_random_seed();
        let mut rng = SmallRng::seed_from_u64(seed);
        let popped = store
            .get_set_mut(key)
            .and_then(|set| set.pop_random(&mut rng));
        store.remove_set_if_empty(key);
        return Ok(Response::Value(
            popped.map(|value| SenkoValue::Raw(Bytes::from(value))),
        ));
    }

    let count = parse_i64(arg_bytes(&args[1])?)?;
    if count < 0 {
        return Err(SenkoError::Protocol(
            "ERR value is out of range, must be positive",
        ));
    }
    if count == 0 {
        return Ok(Response::Array(Box::new(SmallVec::new())));
    }

    let seed = store.next_random_seed();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut out = SmallVec::<[Response; 16]>::new();
    if let Some(set) = store.get_set_mut(key) {
        for _ in 0..count as usize {
            let Some(value) = set.pop_random(&mut rng) else {
                break;
            };
            out.push(raw_response(&value));
        }
    }
    store.remove_set_if_empty(key);
    Ok(Response::Array(Box::new(out)))
}

#[inline]
pub fn smove(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 3 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'smove' command",
        ));
    }

    let source = arg_bytes(&args[0])?;
    let destination = arg_bytes(&args[1])?;
    let member = arg_bytes(&args[2])?;

    ensure_set_type_or_missing(store, source)?;
    ensure_set_type_or_missing(store, destination)?;

    if source == destination {
        let exists = store
            .get_set(source)
            .is_some_and(|set| set.contains(member));
        return Ok(Response::Integer(exists as i64));
    }

    let removed = store
        .get_set_mut(source)
        .is_some_and(|set| set.remove(member));
    if !removed {
        return Ok(Response::Integer(0));
    }
    store.remove_set_if_empty(source);

    let destination_set = store.get_or_create_set(parse_compact(destination));
    let _ = destination_set.add(member);
    Ok(Response::Integer(1))
}

fn ensure_set_type_or_missing(store: &mut Store, key: &[u8]) -> SenkoResult<()> {
    if let Some(value) = store.get(key).cloned()
        && !matches!(value, SenkoValue::Set(_))
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
        expected: "set",
        actual,
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

fn parse_compact(raw: &[u8]) -> CompactString {
    CompactString::from(String::from_utf8_lossy(raw).as_ref())
}

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| SenkoError::Protocol("value is out of range"))
}

fn raw_response(bytes: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(bytes))))
}

fn bytes_response(bytes: std::borrow::Cow<'_, [u8]>) -> Response {
    raw_response(bytes.as_ref())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use compact_str::CompactString;
    use senko_core::{SenkoValue, SetEncoding};
    use senko_proto::Frame;

    use super::{sadd, smembers, smismember, smove, spop, srandmember, srem};
    use crate::{commands::Response, store::Store};

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn resp_bytes(response: &Response) -> Option<&[u8]> {
        match response {
            Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.as_ref()),
            _ => None,
        }
    }

    #[test]
    fn sadd_counts_new_members_and_skips_duplicates() {
        let mut store = Store::default();
        assert_eq!(
            sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"a")]).unwrap(),
            Response::Integer(2)
        );
        assert_eq!(
            sadd(&mut store, &[bs(b"k"), bs(b"b"), bs(b"c")]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn sadd_uses_intset_for_integer_strings() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"1"), bs(b"2")]).unwrap();
        let set = store.get_set(b"k").unwrap();
        assert!(matches!(set.inner, SetEncoding::Intset(_)));
    }

    #[test]
    fn srem_deletes_key_when_last_member_removed() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a")]).unwrap();
        assert_eq!(
            srem(&mut store, &[bs(b"k"), bs(b"a")]).unwrap(),
            Response::Integer(1)
        );
        assert!(store.get_set(b"k").is_none());
    }

    #[test]
    fn smismember_preserves_order() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"c")]).unwrap();
        let response = smismember(&mut store, &[bs(b"k"), bs(b"c"), bs(b"b"), bs(b"a")]).unwrap();
        let Response::Array(values) = response else {
            panic!("expected array")
        };
        assert_eq!(
            values.as_slice(),
            &[
                Response::Integer(1),
                Response::Integer(0),
                Response::Integer(1)
            ]
        );
    }

    #[test]
    fn smembers_on_intset_returns_decimal_strings() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"42"), bs(b"7")]).unwrap();
        let Response::Array(values) = smembers(&mut store, &[bs(b"k")]).unwrap() else {
            panic!("expected array")
        };
        let members: HashSet<Vec<u8>> = values
            .iter()
            .filter_map(resp_bytes)
            .map(|v| v.to_vec())
            .collect();
        assert_eq!(members, HashSet::from([b"42".to_vec(), b"7".to_vec()]));
    }

    #[test]
    fn srandmember_positive_count_over_cardinality_returns_all_distinct() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap();
        let Response::Array(values) = srandmember(&mut store, &[bs(b"k"), bs(b"10")]).unwrap()
        else {
            panic!("expected array")
        };
        let members: HashSet<Vec<u8>> = values
            .iter()
            .filter_map(resp_bytes)
            .map(|v| v.to_vec())
            .collect();
        assert_eq!(values.len(), 3);
        assert_eq!(members.len(), 3);
    }

    #[test]
    fn srandmember_negative_count_keeps_requested_len() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b")]).unwrap();
        let Response::Array(values) = srandmember(&mut store, &[bs(b"k"), bs(b"-8")]).unwrap()
        else {
            panic!("expected array")
        };
        assert_eq!(values.len(), 8);
    }

    #[test]
    fn spop_count_over_cardinality_pops_all() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap();
        let Response::Array(values) = spop(&mut store, &[bs(b"k"), bs(b"10")]).unwrap() else {
            panic!("expected array")
        };
        assert_eq!(values.len(), 3);
        assert!(store.get_set(b"k").is_none());
    }

    #[test]
    fn spop_zero_returns_empty_array_without_mutation() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b")]).unwrap();
        let Response::Array(values) = spop(&mut store, &[bs(b"k"), bs(b"0")]).unwrap() else {
            panic!("expected array")
        };
        assert!(values.is_empty());
        assert_eq!(store.get_set(b"k").unwrap().len(), 2);
    }

    #[test]
    fn smove_same_source_destination_returns_one_for_existing_member() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"k"), bs(b"a")]).unwrap();
        assert_eq!(
            smove(&mut store, &[bs(b"k"), bs(b"k"), bs(b"a")]).unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn smove_missing_member_leaves_destination_unchanged() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"src"), bs(b"a")]).unwrap();
        let _ = sadd(&mut store, &[bs(b"dst"), bs(b"b")]).unwrap();
        assert_eq!(
            smove(&mut store, &[bs(b"src"), bs(b"dst"), bs(b"x")]).unwrap(),
            Response::Integer(0)
        );
        let members: HashSet<Vec<u8>> = store
            .get_set(b"dst")
            .unwrap()
            .iter()
            .map(|v| v.into_owned())
            .collect();
        assert_eq!(members, HashSet::from([b"b".to_vec()]));
    }

    #[test]
    fn smove_wrongtype_when_destination_is_list() {
        let mut store = Store::default();
        let _ = sadd(&mut store, &[bs(b"src"), bs(b"a")]).unwrap();
        store
            .get_or_create_list(CompactString::from("dst"))
            .push_back(b"x");
        let error = smove(&mut store, &[bs(b"src"), bs(b"dst"), bs(b"a")]).unwrap_err();
        assert!(matches!(
            error,
            senko_core::SenkoError::WrongType {
                expected: "set",
                actual: "list"
            }
        ));
    }
}
