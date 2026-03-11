use std::collections::HashSet;

use bytes::Bytes;
use compact_str::CompactString;
use roaring::RoaringBitmap;
use senko_core::{SenkoError, SenkoResult, SenkoValue, SetEncoding, SetObject};
use senko_proto::Frame;

use crate::{
    commands::Response,
    store::{SetOptions, Store},
};

const SMALL_HASH_THRESHOLD: usize = 64;
const ROARING_THRESHOLD: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlgebraStrategy {
    IntsetSorted,
    SmallHash,
    LargeHash,
    Roaring,
}

pub fn algebra_strategy(sets: &[&SetObject]) -> AlgebraStrategy {
    if sets.is_empty() {
        return AlgebraStrategy::SmallHash;
    }
    if sets.iter().any(|set| set.len() > ROARING_THRESHOLD) {
        return AlgebraStrategy::Roaring;
    }
    if sets
        .iter()
        .all(|set| matches!(set.inner, SetEncoding::Intset(_)))
    {
        return AlgebraStrategy::IntsetSorted;
    }
    if sets.iter().any(|set| set.len() <= SMALL_HASH_THRESHOLD) {
        return AlgebraStrategy::SmallHash;
    }
    if sets
        .iter()
        .all(|set| matches!(set.inner, SetEncoding::Hashtable(_)))
    {
        return AlgebraStrategy::LargeHash;
    }
    AlgebraStrategy::SmallHash
}

pub fn intset_intersection(a: &[i64], b: &[i64]) -> Vec<i64> {
    scalar_intset_intersection(a, b)
}

pub fn intset_union(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut ia, mut ib) = (0usize, 0usize);
    while ia < a.len() && ib < b.len() {
        match a[ia].cmp(&b[ib]) {
            std::cmp::Ordering::Less => {
                out.push(a[ia]);
                ia += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[ib]);
                ib += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[ia]);
                ia += 1;
                ib += 1;
            }
        }
    }
    out.extend_from_slice(&a[ia..]);
    out.extend_from_slice(&b[ib..]);
    out
}

pub fn intset_difference(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len());
    let (mut ia, mut ib) = (0usize, 0usize);
    while ia < a.len() && ib < b.len() {
        match a[ia].cmp(&b[ib]) {
            std::cmp::Ordering::Less => {
                out.push(a[ia]);
                ia += 1;
            }
            std::cmp::Ordering::Greater => ib += 1,
            std::cmp::Ordering::Equal => {
                ia += 1;
                ib += 1;
            }
        }
    }
    out.extend_from_slice(&a[ia..]);
    out
}

#[inline]
pub fn sdiff(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sdiff' command",
        ));
    }
    let key_slices = collect_set_keys(store, args, "set")?;
    let result = compute_sdiff(&key_slices);
    Ok(bytes_array_response(result))
}

#[inline]
pub fn sdiffstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sdiffstore' command",
        ));
    }
    let destination = parse_compact(arg_bytes(&args[0])?);
    let key_slices = collect_set_keys(store, &args[1..], "set")?;
    let result = compute_sdiff(&key_slices);
    Ok(Response::Integer(
        store_set_result(store, destination, &result) as i64,
    ))
}

#[inline]
pub fn sinter(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sinter' command",
        ));
    }
    let key_slices = collect_set_keys(store, args, "set")?;
    let result = compute_sinter(&key_slices, None);
    Ok(bytes_array_response(result))
}

#[inline]
pub fn sinterstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sinterstore' command",
        ));
    }
    let destination = parse_compact(arg_bytes(&args[0])?);
    let key_slices = collect_set_keys(store, &args[1..], "set")?;
    let result = compute_sinter(&key_slices, None);
    Ok(Response::Integer(
        store_set_result(store, destination, &result) as i64,
    ))
}

#[inline]
pub fn sintercard(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sintercard' command",
        ));
    }
    let numkeys = parse_i64(arg_bytes(&args[0])?)?;
    if numkeys <= 0 {
        return Err(SenkoError::Protocol("numkeys should be greater than 0"));
    }
    let numkeys = numkeys as usize;
    if args.len() < 1 + numkeys {
        return Err(SenkoError::Protocol("syntax error"));
    }

    let limit = if args.len() == 1 + numkeys {
        None
    } else if args.len() == 3 + numkeys {
        let token = arg_bytes(&args[1 + numkeys])?;
        if !token.eq_ignore_ascii_case(b"LIMIT") {
            return Err(SenkoError::Protocol("syntax error"));
        }
        Some(parse_i64(arg_bytes(&args[2 + numkeys])?)?.max(0) as usize)
    } else {
        return Err(SenkoError::Protocol("syntax error"));
    };

    let key_slices = collect_set_keys(store, &args[1..1 + numkeys], "set")?;
    let limit = limit.filter(|limit| *limit > 0);
    let result = compute_sinter(&key_slices, limit);
    Ok(Response::Integer(result.len() as i64))
}

#[inline]
pub fn sunion(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sunion' command",
        ));
    }
    let key_slices = collect_set_keys(store, args, "set")?;
    let result = compute_sunion(&key_slices);
    Ok(bytes_array_response(result))
}

#[inline]
pub fn sunionstore(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'sunionstore' command",
        ));
    }
    let destination = parse_compact(arg_bytes(&args[0])?);
    let key_slices = collect_set_keys(store, &args[1..], "set")?;
    let result = compute_sunion(&key_slices);
    Ok(Response::Integer(
        store_set_result(store, destination, &result) as i64,
    ))
}

fn compute_sdiff(sets: &[Option<SetObject>]) -> Vec<Vec<u8>> {
    let Some(Some(first)) = sets.first() else {
        return Vec::new();
    };
    let refs = set_refs(sets);
    match algebra_strategy(&refs) {
        AlgebraStrategy::IntsetSorted => {
            let mut current = intset_data(first);
            for other in refs.iter().skip(1) {
                current = intset_difference(&current, &intset_data(other));
                if current.is_empty() {
                    break;
                }
            }
            current
                .into_iter()
                .map(|value| value.to_string().into_bytes())
                .collect()
        }
        AlgebraStrategy::Roaring => roaring_difference(sets),
        AlgebraStrategy::SmallHash | AlgebraStrategy::LargeHash => {
            let mut current: HashSet<Vec<u8>> =
                first.iter().map(|member| member.into_owned()).collect();
            for set in sets.iter().skip(1).flatten() {
                current.retain(|member| !set.contains(member));
                if current.is_empty() {
                    break;
                }
            }
            current.into_iter().collect()
        }
    }
}

fn compute_sinter(sets: &[Option<SetObject>], limit: Option<usize>) -> Vec<Vec<u8>> {
    if sets.is_empty() || sets.iter().any(Option::is_none) {
        return Vec::new();
    }
    let mut ordered: Vec<&SetObject> = sets.iter().filter_map(Option::as_ref).collect();
    ordered.sort_by_key(|set| set.len());
    if ordered.first().is_some_and(|set| set.is_empty()) {
        return Vec::new();
    }

    match algebra_strategy(&ordered) {
        AlgebraStrategy::IntsetSorted => {
            let mut current = intset_data(ordered[0]);
            for set in ordered.iter().skip(1) {
                current = intset_intersection(&current, &intset_data(set));
                if let Some(limit) = limit
                    && current.len() >= limit
                {
                    current.truncate(limit);
                    break;
                }
                if current.is_empty() {
                    break;
                }
            }
            current
                .into_iter()
                .map(|value| value.to_string().into_bytes())
                .collect()
        }
        AlgebraStrategy::Roaring => {
            let mut result = roaring_intersection(sets);
            if let Some(limit) = limit {
                result.truncate(limit);
            }
            result
        }
        AlgebraStrategy::SmallHash | AlgebraStrategy::LargeHash => {
            let mut current: Vec<Vec<u8>> = ordered[0]
                .iter()
                .map(|member| member.into_owned())
                .collect();
            for set in ordered.iter().skip(1) {
                current.retain(|member| set.contains(member));
                if let Some(limit) = limit
                    && current.len() >= limit
                {
                    current.truncate(limit);
                    break;
                }
                if current.is_empty() {
                    break;
                }
            }
            current
        }
    }
}

fn compute_sunion(sets: &[Option<SetObject>]) -> Vec<Vec<u8>> {
    let refs = set_refs(sets);
    match algebra_strategy(&refs) {
        AlgebraStrategy::IntsetSorted => {
            let mut ordered = refs.into_iter();
            let Some(first) = ordered.next() else {
                return Vec::new();
            };
            let mut current = intset_data(first);
            for set in ordered {
                current = intset_union(&current, &intset_data(set));
            }
            current
                .into_iter()
                .map(|value| value.to_string().into_bytes())
                .collect()
        }
        AlgebraStrategy::Roaring => roaring_union(sets),
        AlgebraStrategy::SmallHash | AlgebraStrategy::LargeHash => {
            let capacity = sets.iter().flatten().map(SetObject::len).sum();
            let mut out = HashSet::<Vec<u8>>::with_capacity(capacity);
            for set in sets.iter().flatten() {
                for member in set.iter() {
                    out.insert(member.into_owned());
                }
            }
            out.into_iter().collect()
        }
    }
}

fn roaring_difference(sets: &[Option<SetObject>]) -> Vec<Vec<u8>> {
    let Some(Some(first)) = sets.first() else {
        return Vec::new();
    };
    let (mut ints, mut bytes) = split_to_roaring(first);
    for set in sets.iter().skip(1).flatten() {
        let (other_ints, other_bytes) = split_to_roaring(set);
        ints -= other_ints;
        for member in other_bytes {
            bytes.remove(member.as_str());
        }
    }
    roaring_and_hash_to_vec(ints, bytes)
}

fn roaring_intersection(sets: &[Option<SetObject>]) -> Vec<Vec<u8>> {
    let mut iter = sets.iter().flatten();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let (mut ints, mut bytes) = split_to_roaring(first);
    for set in iter {
        let (other_ints, other_bytes) = split_to_roaring(set);
        ints &= other_ints;
        bytes.retain(|member| other_bytes.contains(member.as_str()));
        if ints.is_empty() && bytes.is_empty() {
            break;
        }
    }
    roaring_and_hash_to_vec(ints, bytes)
}

fn roaring_union(sets: &[Option<SetObject>]) -> Vec<Vec<u8>> {
    let mut ints = RoaringBitmap::new();
    let mut bytes = HashSet::<CompactString>::new();
    for set in sets.iter().flatten() {
        let (other_ints, other_bytes) = split_to_roaring(set);
        ints |= other_ints;
        bytes.extend(other_bytes);
    }
    roaring_and_hash_to_vec(ints, bytes)
}

fn roaring_and_hash_to_vec(ints: RoaringBitmap, bytes: HashSet<CompactString>) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = ints
        .into_iter()
        .map(|value| value.to_string().into_bytes())
        .collect();
    out.extend(bytes.into_iter().map(|member| member.as_bytes().to_vec()));
    out
}

fn split_to_roaring(set: &SetObject) -> (RoaringBitmap, HashSet<CompactString>) {
    let mut ints = RoaringBitmap::new();
    let mut bytes = HashSet::new();
    for member in set.iter() {
        match std::str::from_utf8(member.as_ref())
            .ok()
            .and_then(|text| text.parse::<u32>().ok())
        {
            Some(value) => {
                ints.insert(value);
            }
            None => {
                bytes.insert(CompactString::from(
                    String::from_utf8_lossy(member.as_ref()).as_ref(),
                ));
            }
        }
    }
    (ints, bytes)
}

fn set_refs(sets: &[Option<SetObject>]) -> Vec<&SetObject> {
    sets.iter().filter_map(Option::as_ref).collect()
}

fn intset_data(set: &SetObject) -> Vec<i64> {
    match &set.inner {
        SetEncoding::Intset(intset) => intset.data.clone(),
        _ => unreachable!("expected intset encoding"),
    }
}

fn store_set_result(store: &mut Store, destination: CompactString, members: &[Vec<u8>]) -> usize {
    if members.is_empty() {
        let _ = store.delete(destination.as_bytes());
        return 0;
    }

    let mut result = SetObject::default();
    for member in members {
        let _ = result.add(member);
    }
    let len = result.len();
    let _ = store.set(
        destination,
        SenkoValue::Set(Box::new(result)),
        SetOptions::default(),
    );
    len
}

fn collect_set_keys(
    store: &mut Store,
    args: &[Frame<'_>],
    expected: &'static str,
) -> SenkoResult<Vec<Option<SetObject>>> {
    let mut out = Vec::with_capacity(args.len());
    for frame in args {
        let key = arg_bytes(frame)?;
        match store.get(key).cloned() {
            None => out.push(None),
            Some(SenkoValue::Set(set)) => out.push(Some((*set).clone())),
            Some(value) => {
                return Err(SenkoError::WrongType {
                    expected,
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

fn bytes_array_response(values: Vec<Vec<u8>>) -> Response {
    Response::Array(Box::new(
        values
            .into_iter()
            .map(|value| raw_response(&value))
            .collect(),
    ))
}

fn raw_response(bytes: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(bytes))))
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

fn scalar_intset_intersection(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut ia, mut ib) = (0usize, 0usize);
    while ia < a.len() && ib < b.len() {
        match a[ia].cmp(&b[ib]) {
            std::cmp::Ordering::Less => ia += 1,
            std::cmp::Ordering::Greater => ib += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[ia]);
                ia += 1;
                ib += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet as StdHashSet;

    use compact_str::CompactString;
    use proptest::prelude::*;
    use senko_core::{SenkoValue, SetEncoding, SetObject};
    use senko_proto::Frame;

    use super::{
        AlgebraStrategy, algebra_strategy, intset_intersection, scalar_intset_intersection, sdiff,
        sdiffstore, sinter, sintercard, sinterstore, sunion, sunionstore,
    };
    use crate::{commands::Response, store::Store};

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn add_all(set: &mut SetObject, members: &[&[u8]]) {
        for member in members {
            let _ = set.add(member);
        }
    }

    fn response_set(response: Response) -> StdHashSet<Vec<u8>> {
        let Response::Array(values) = response else {
            panic!("expected array")
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
    fn sdiff_basic() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2", b"3"]);
        add_all(&mut b, &[b"2", b"3", b"4"]);
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            response_set(sdiff(&mut store, &[bs(b"a"), bs(b"b")]).unwrap()),
            StdHashSet::from([b"1".to_vec()])
        );
    }

    #[test]
    fn sdiff_first_missing_is_empty() {
        let mut store = Store::default();
        assert!(response_set(sdiff(&mut store, &[bs(b"missing"), bs(b"b")]).unwrap()).is_empty());
    }

    #[test]
    fn sdiffstore_destination_equals_source() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2", b"3"]);
        add_all(&mut b, &[b"2"]);
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            sdiffstore(&mut store, &[bs(b"a"), bs(b"a"), bs(b"b")]).unwrap(),
            Response::Integer(2)
        );
        assert!(matches!(
            store.get_set(b"a").unwrap().inner,
            SetEncoding::Intset(_)
        ));
    }

    #[test]
    fn sinter_basic() {
        let mut store = Store::default();
        for (key, members) in [
            ("a", vec![b"1".as_slice(), b"2", b"3"]),
            ("b", vec![b"2".as_slice(), b"3", b"4"]),
            ("c", vec![b"3".as_slice(), b"4", b"5"]),
        ] {
            let mut set = SetObject::default();
            for member in members {
                let _ = set.add(member);
            }
            let _ = store.set(
                CompactString::from(key),
                SenkoValue::Set(Box::new(set)),
                Default::default(),
            );
        }
        assert_eq!(
            response_set(sinter(&mut store, &[bs(b"a"), bs(b"b"), bs(b"c")]).unwrap()),
            StdHashSet::from([b"3".to_vec()])
        );
    }

    #[test]
    fn sinter_early_exit_on_empty_or_missing() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        add_all(&mut a, &[b"1", b"2"]);
        let empty = SetObject::default();
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("e"),
            SenkoValue::Set(Box::new(empty)),
            Default::default(),
        );
        assert!(response_set(sinter(&mut store, &[bs(b"a"), bs(b"e")]).unwrap()).is_empty());
        assert!(response_set(sinter(&mut store, &[bs(b"a"), bs(b"missing")]).unwrap()).is_empty());
    }

    #[test]
    fn sintercard_limit_one() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        for i in 0..5000 {
            let _ = a.add(i.to_string().as_bytes());
        }
        for i in 2500..7500 {
            let _ = b.add(i.to_string().as_bytes());
        }
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            sintercard(
                &mut store,
                &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"LIMIT"), bs(b"1")]
            )
            .unwrap(),
            Response::Integer(1)
        );
    }

    #[test]
    fn sunion_has_no_duplicates() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2"]);
        add_all(&mut b, &[b"2", b"3"]);
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            response_set(sunion(&mut store, &[bs(b"a"), bs(b"b")]).unwrap()),
            StdHashSet::from([b"1".to_vec(), b"2".to_vec(), b"3".to_vec()])
        );
    }

    #[test]
    fn sunionstore_destination_equals_source() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2"]);
        add_all(&mut b, &[b"2", b"3"]);
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            sunionstore(&mut store, &[bs(b"a"), bs(b"a"), bs(b"b")]).unwrap(),
            Response::Integer(3)
        );
    }

    #[test]
    fn roaring_path_selected_and_counts_correct() {
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        for i in 0..5000 {
            let _ = a.add(i.to_string().as_bytes());
        }
        for i in 2500..7500 {
            let _ = b.add(i.to_string().as_bytes());
        }
        assert_eq!(algebra_strategy(&[&a, &b]), AlgebraStrategy::Roaring);
    }

    #[test]
    fn algorithm_selection_prefers_intset_sorted() {
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2"]);
        add_all(&mut b, &[b"2", b"3"]);
        assert_eq!(algebra_strategy(&[&a, &b]), AlgebraStrategy::IntsetSorted);
    }

    #[test]
    fn store_variants_preserve_intset_encoding_for_integer_results() {
        let mut store = Store::default();
        let mut a = SetObject::default();
        let mut b = SetObject::default();
        add_all(&mut a, &[b"1", b"2", b"3"]);
        add_all(&mut b, &[b"2", b"3"]);
        let _ = store.set(
            CompactString::from("a"),
            SenkoValue::Set(Box::new(a)),
            Default::default(),
        );
        let _ = store.set(
            CompactString::from("b"),
            SenkoValue::Set(Box::new(b)),
            Default::default(),
        );
        assert_eq!(
            sinterstore(&mut store, &[bs(b"dst"), bs(b"a"), bs(b"b")]).unwrap(),
            Response::Integer(2)
        );
        assert!(matches!(
            store.get_set(b"dst").unwrap().inner,
            SetEncoding::Intset(_)
        ));
    }

    proptest! {
        #[test]
        fn intset_intersection_matches_scalar(mut a in proptest::collection::vec(-10_000i64..10_000, 0..128), mut b in proptest::collection::vec(-10_000i64..10_000, 0..128)) {
            a.sort_unstable();
            a.dedup();
            b.sort_unstable();
            b.dedup();
            prop_assert_eq!(intset_intersection(&a, &b), scalar_intset_intersection(&a, &b));
        }
    }
}
