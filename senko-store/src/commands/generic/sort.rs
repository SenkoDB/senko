use std::borrow::Cow;

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{QuickList, SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{Response, store::Store};

#[derive(Clone)]
struct SortOptions<'a> {
    by: Option<&'a [u8]>,
    limit: Option<(i64, i64)>,
    gets: Vec<&'a [u8]>,
    desc: bool,
    alpha: bool,
    store: Option<CompactString>,
}

#[derive(Clone)]
struct SortItem {
    element: Vec<u8>,
    key: SortKey,
}

#[derive(Clone, PartialEq)]
enum SortKey {
    Numeric(f64),
    Alpha(Vec<u8>),
}

#[inline]
pub fn sort(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    sort_impl(store, args, false)
}

#[inline]
pub fn sort_ro(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    sort_impl(store, args, true)
}

fn sort_impl(store: &mut Store, args: &[Frame<'_>], read_only: bool) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(if read_only {
            "wrong number of arguments for 'sort_ro' command"
        } else {
            "wrong number of arguments for 'sort' command"
        }));
    }
    let key = arg_bytes(&args[0])?;
    let options = parse_options(&args[1..], read_only)?;
    let mut elements = collect_elements(store, key)?;
    if elements.is_empty() {
        return Ok(match options.store {
            Some(destination) => {
                let empty = QuickList::default();
                let _ = store.set(
                    destination,
                    SenkoValue::List(Box::new(empty)),
                    Default::default(),
                );
                Response::Integer(0)
            }
            None => Response::Array(Box::default()),
        });
    }

    let nosort = options
        .by
        .is_some_and(|pattern| pattern.eq_ignore_ascii_case(b"nosort"));
    if !nosort {
        let mut sortable = Vec::with_capacity(elements.len());
        for element in elements {
            let sort_bytes = lookup_sort_value(store, &element, options.by)?;
            let key = if options.alpha {
                SortKey::Alpha(sort_bytes.into_owned())
            } else {
                let text = std::str::from_utf8(sort_bytes.as_ref()).map_err(|_| {
                    SenkoError::Protocol("ERR One or more scores can't be converted into double")
                })?;
                let value = text.parse::<f64>().map_err(|_| {
                    SenkoError::Protocol("ERR One or more scores can't be converted into double")
                })?;
                SortKey::Numeric(value)
            };
            sortable.push(SortItem { element, key });
        }
        sortable.sort_by(|left, right| compare_sort_key(&left.key, &right.key));
        if options.desc {
            sortable.reverse();
        }
        elements = sortable.into_iter().map(|item| item.element).collect();
    }

    let elements = apply_limit(elements, options.limit);
    let result_items = materialize_results(store, &elements, &options.gets)?;

    if let Some(destination) = options.store {
        let mut list = QuickList::default();
        for item in &result_items {
            match item {
                Some(bytes) => list.push_back(bytes),
                None => list.push_back(b""),
            }
        }
        let _ = store.set(
            destination,
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        return Ok(Response::Integer(result_items.len() as i64));
    }

    let values = result_items
        .into_iter()
        .map(|item| Response::Value(item.map(Bytes::from).map(SenkoValue::Raw)))
        .collect::<SmallVec<[Response; 16]>>();
    Ok(Response::Array(Box::new(values)))
}

fn parse_options<'a>(args: &'a [Frame<'_>], read_only: bool) -> SenkoResult<SortOptions<'a>> {
    let mut by = None;
    let mut limit = None;
    let mut gets = Vec::new();
    let mut desc = false;
    let mut alpha = false;
    let mut store = None;
    let mut idx = 0usize;
    while idx < args.len() {
        let token = arg_bytes(&args[idx])?;
        if token.eq_ignore_ascii_case(b"BY") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            by = Some(arg_bytes(&args[idx])?);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"LIMIT") {
            if idx + 2 >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let offset = parse_i64(arg_bytes(&args[idx + 1])?)?;
            let count = parse_i64(arg_bytes(&args[idx + 2])?)?;
            limit = Some((offset, count));
            idx += 3;
            continue;
        }
        if token.eq_ignore_ascii_case(b"GET") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            gets.push(arg_bytes(&args[idx])?);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"ASC") {
            desc = false;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"DESC") {
            desc = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"ALPHA") {
            alpha = true;
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"STORE") {
            if read_only {
                return Err(SenkoError::Protocol(
                    "ERR STORE option not allowed in SORT_RO",
                ));
            }
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            store = Some(parse_key(arg_bytes(&args[idx])?)?);
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }
    Ok(SortOptions {
        by,
        limit,
        gets,
        desc,
        alpha,
        store,
    })
}

fn collect_elements(store: &mut Store, key: &[u8]) -> SenkoResult<Vec<Vec<u8>>> {
    match store.get(key).cloned() {
        None => Ok(Vec::new()),
        Some(SenkoValue::List(_)) => Ok(store
            .get_list(key)
            .unwrap()
            .iter()
            .map(|item| item.to_vec())
            .collect()),
        Some(SenkoValue::Set(_)) => Ok(store
            .get_set(key)
            .unwrap()
            .iter()
            .map(|item| item.into_owned())
            .collect()),
        Some(SenkoValue::ZSet(_)) => Ok(store
            .get_zset(key)
            .unwrap()
            .range_by_rank(0, -1, false, None)
            .map(|(_, member)| member.as_bytes().to_vec())
            .collect()),
        Some(other) => Err(SenkoError::WrongType {
            expected: "list/set/zset",
            actual: actual_type(&other),
        }),
    }
}

fn lookup_sort_value<'a>(
    store: &'a mut Store,
    element: &[u8],
    by: Option<&[u8]>,
) -> SenkoResult<Cow<'a, [u8]>> {
    let Some(pattern) = by else {
        return Ok(Cow::Owned(element.to_vec()));
    };
    if pattern.eq_ignore_ascii_case(b"nosort") {
        return Ok(Cow::Owned(element.to_vec()));
    }
    let key = substitute(pattern, element);
    match store.get(key.as_bytes()) {
        Some(value @ (SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_))) => {
            Ok(Cow::Owned(value.as_bytes().into_owned()))
        }
        Some(other) => Err(SenkoError::WrongType {
            expected: "string",
            actual: actual_type(other),
        }),
        None => Ok(Cow::Borrowed(b"0")),
    }
}

fn materialize_results(
    store: &mut Store,
    elements: &[Vec<u8>],
    gets: &[&[u8]],
) -> SenkoResult<Vec<Option<Vec<u8>>>> {
    let mut out = Vec::new();
    if gets.is_empty() {
        out.extend(elements.iter().map(|element| Some(element.clone())));
        return Ok(out);
    }
    for element in elements {
        for pattern in gets {
            if *pattern == b"#" {
                out.push(Some(element.clone()));
                continue;
            }
            let key = substitute(pattern, element);
            match store.get(key.as_bytes()) {
                Some(
                    value @ (SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_)),
                ) => out.push(Some(value.as_bytes().into_owned())),
                Some(other) => {
                    return Err(SenkoError::WrongType {
                        expected: "string",
                        actual: actual_type(other),
                    });
                }
                None => out.push(None),
            }
        }
    }
    Ok(out)
}

fn substitute(pattern: &[u8], element: &[u8]) -> CompactString {
    if pattern == b"#" {
        return CompactString::from_utf8_lossy(element);
    }
    let replaced = String::from_utf8_lossy(pattern).replace('*', &String::from_utf8_lossy(element));
    CompactString::new(replaced)
}

fn apply_limit(mut elements: Vec<Vec<u8>>, limit: Option<(i64, i64)>) -> Vec<Vec<u8>> {
    let Some((offset, count)) = limit else {
        return elements;
    };
    let offset = offset.max(0) as usize;
    if offset >= elements.len() {
        return Vec::new();
    }
    elements = elements.into_iter().skip(offset).collect();
    if count >= 0 {
        elements.truncate(count as usize);
    }
    elements
}

fn compare_sort_key(left: &SortKey, right: &SortKey) -> std::cmp::Ordering {
    match (left, right) {
        (SortKey::Numeric(a), SortKey::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (SortKey::Alpha(a), SortKey::Alpha(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
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

fn parse_key(raw: &[u8]) -> SenkoResult<CompactString> {
    let key = std::str::from_utf8(raw).map_err(|_| SenkoError::Protocol("invalid UTF-8 key"))?;
    Ok(CompactString::new(key))
}

fn parse_i64(raw: &[u8]) -> SenkoResult<i64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(SenkoError::Protocol(
            "value is not an integer or out of range",
        ))
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
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{QuickList, SenkoValue, SetObject, ZAddOptions, ZSetObject};
    use senko_proto::Frame;

    use super::{sort, sort_ro};
    use crate::{Response, Store};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn array_bytes(response: Response) -> Vec<Option<Vec<u8>>> {
        let Response::Array(items) = response else {
            panic!()
        };
        items
            .into_iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
                Response::Value(None) => None,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn sort_basic_modes() {
        let mut store = Store::default();
        let mut list = QuickList::default();
        list.push_back(b"3");
        list.push_back(b"1");
        list.push_back(b"2");
        let _ = store.set(
            CompactString::new("l"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"l")]).unwrap()),
            vec![
                Some(b"1".to_vec()),
                Some(b"2".to_vec()),
                Some(b"3".to_vec())
            ]
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"l"), bs(b"DESC")]).unwrap()),
            vec![
                Some(b"3".to_vec()),
                Some(b"2".to_vec()),
                Some(b"1".to_vec())
            ]
        );

        let mut alpha = QuickList::default();
        alpha.push_back(b"b");
        alpha.push_back(b"a");
        alpha.push_back(b"c");
        let _ = store.set(
            CompactString::new("alpha"),
            SenkoValue::List(Box::new(alpha)),
            Default::default(),
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"alpha"), bs(b"ALPHA")]).unwrap()),
            vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec())
            ]
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"l"), bs(b"LIMIT"), bs(b"1"), bs(b"2")]).unwrap()),
            vec![Some(b"2".to_vec()), Some(b"3".to_vec())]
        );
    }

    #[test]
    fn sort_set_zset_by_get_and_store() {
        let mut store = Store::default();
        let mut set = SetObject::default();
        let _ = set.add(b"3");
        let _ = set.add(b"1");
        let _ = set.add(b"2");
        let _ = store.set(
            CompactString::new("s"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        assert_eq!(array_bytes(sort(&mut store, &[bs(b"s")]).unwrap()).len(), 3);

        let mut zset = ZSetObject::default();
        let _ = zset.add(2.0, CompactString::new("b"), ZAddOptions::default());
        let _ = zset.add(1.0, CompactString::new("a"), ZAddOptions::default());
        let _ = store.set(
            CompactString::new("z"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"z"), bs(b"ALPHA")]).unwrap()),
            vec![Some(b"a".to_vec()), Some(b"b".to_vec())]
        );

        let _ = store.set(
            CompactString::new("weight_1"),
            SenkoValue::from(Bytes::from_static(b"30")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("weight_2"),
            SenkoValue::from(Bytes::from_static(b"10")),
            Default::default(),
        );
        let _ = store.set(
            CompactString::new("weight_3"),
            SenkoValue::from(Bytes::from_static(b"20")),
            Default::default(),
        );
        let res = array_bytes(
            sort(
                &mut store,
                &[bs(b"s"), bs(b"BY"), bs(b"weight_*"), bs(b"GET"), bs(b"#")],
            )
            .unwrap(),
        );
        assert_eq!(
            res,
            vec![
                Some(b"2".to_vec()),
                Some(b"3".to_vec()),
                Some(b"1".to_vec())
            ]
        );

        let _ = store.set(
            CompactString::new("data_1"),
            SenkoValue::from(Bytes::from_static(b"one")),
            Default::default(),
        );
        let res = array_bytes(
            sort(
                &mut store,
                &[
                    bs(b"s"),
                    bs(b"ALPHA"),
                    bs(b"GET"),
                    bs(b"data_*"),
                    bs(b"GET"),
                    bs(b"#"),
                ],
            )
            .unwrap(),
        );
        assert!(res.contains(&Some(b"one".to_vec())));
        assert!(res.contains(&None));

        assert_eq!(
            sort(&mut store, &[bs(b"s"), bs(b"STORE"), bs(b"out")]).unwrap(),
            Response::Integer(3)
        );
        assert!(store.get_list(b"out").is_some());
        assert!(sort_ro(&mut store, &[bs(b"s"), bs(b"STORE"), bs(b"out")]).is_err());
    }

    #[test]
    fn sort_nosort_and_numeric_error() {
        let mut store = Store::default();
        let mut list = QuickList::default();
        list.push_back(b"3");
        list.push_back(b"1");
        list.push_back(b"2");
        let _ = store.set(
            CompactString::new("l"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        assert_eq!(
            array_bytes(sort(&mut store, &[bs(b"l"), bs(b"BY"), bs(b"nosort")]).unwrap()),
            vec![
                Some(b"3".to_vec()),
                Some(b"1".to_vec()),
                Some(b"2".to_vec())
            ]
        );
        let mut bad = QuickList::default();
        bad.push_back(b"a");
        bad.push_back(b"1");
        let _ = store.set(
            CompactString::new("bad"),
            SenkoValue::List(Box::new(bad)),
            Default::default(),
        );
        assert!(
            sort(&mut store, &[bs(b"bad")])
                .unwrap_err()
                .to_string()
                .contains("One or more scores can't be converted into double")
        );
    }
}
