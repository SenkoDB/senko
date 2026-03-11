use bytes::Bytes;
use compact_str::CompactString;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::{
    commands::Response,
    pattern::glob_match,
    store::{Store, current_unix_ms},
};

const DEFAULT_COUNT: usize = 10;
const TYPE_STRING: &[u8] = b"string";
const TYPE_LIST: &[u8] = b"list";
const TYPE_SET: &[u8] = b"set";
const TYPE_ZSET: &[u8] = b"zset";
const TYPE_HASH: &[u8] = b"hash";
const TYPE_STREAM: &[u8] = b"stream";

#[inline]
pub fn keys(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() != 1 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'keys' command",
        ));
    }
    let pattern = arg_bytes(&args[0])?;
    let now_ms = current_unix_ms();
    // NOTE: KEYS is O(N) and blocks the shard. Use SCAN in production.
    let mut values = SmallVec::<[Response; 16]>::new();
    for (key, _, _, _) in store.live_entries_snapshot(now_ms) {
        if glob_match(pattern, key.as_bytes()) {
            values.push(raw_key_response(&key));
        }
    }
    Ok(Response::Array(Box::new(values)))
}

#[inline]
pub fn scan(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'scan' command",
        ));
    }
    let cursor = parse_u64(arg_bytes(&args[0])?)?;
    let mut idx = 1usize;
    let mut pattern: Option<&[u8]> = None;
    let mut count = DEFAULT_COUNT;
    let mut type_filter: Option<&[u8]> = None;

    while idx < args.len() {
        let token = arg_bytes(&args[idx])?;
        if token.eq_ignore_ascii_case(b"MATCH") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            pattern = Some(arg_bytes(&args[idx])?);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            count = parse_usize(arg_bytes(&args[idx])?)?.max(1);
            idx += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"TYPE") {
            idx += 1;
            if idx >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            type_filter = Some(arg_bytes(&args[idx])?);
            idx += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    let now_ms = current_unix_ms();
    let (next, keys) = scan_step(store, cursor, count, pattern, type_filter, now_ms);
    let mut top = SmallVec::<[Response; 16]>::new();
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        next.to_string().into_bytes(),
    )))));
    let mut out = SmallVec::<[Response; 16]>::new();
    for key in keys {
        out.push(raw_key_response(&key));
    }
    top.push(Response::Array(Box::new(out)));
    Ok(Response::Array(Box::new(top)))
}

#[inline]
pub fn randomkey(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if !args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'randomkey' command",
        ));
    }
    let counts = store.table_bucket_counts();
    if counts[0] == 0 || store.entry_count() == 0 {
        return Ok(Response::Value(None));
    }

    let now_ms = current_unix_ms();
    let seed = store.next_random_seed();
    let mut rng = SmallRng::seed_from_u64(seed);

    for _ in 0..10 {
        let table_index = if counts[1] > 0 && rng.gen_bool(0.5) {
            1
        } else {
            0
        };
        let bucket_count = counts[table_index];
        if bucket_count == 0 {
            continue;
        }
        let bucket = rng.gen_range(0..bucket_count);
        if let Some((key, _, expires_at)) = store.bucket_snapshot(table_index, bucket)
            && !is_expired(expires_at, now_ms)
        {
            let _ = store.touch(key.as_bytes());
            return Ok(Response::Value(Some(SenkoValue::Raw(
                Bytes::copy_from_slice(key.as_bytes()),
            ))));
        }
    }

    if let Some((key, _, _, _)) = store.live_entries_snapshot(now_ms).into_iter().next() {
        let _ = store.touch(key.as_bytes());
        return Ok(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(key.as_bytes()),
        ))));
    }

    Ok(Response::Value(None))
}

#[inline]
pub fn touch(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.is_empty() {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'touch' command",
        ));
    }
    let mut touched = 0i64;
    for arg in args {
        if store.touch(arg_bytes(arg)?) {
            touched += 1;
        }
    }
    Ok(Response::Integer(touched))
}

pub fn scan_step(
    store: &Store,
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
    type_filter: Option<&[u8]>,
    now_ms: u64,
) -> (u64, Vec<CompactString>) {
    if let Some(filter) = type_filter
        && normalize_type_filter(filter).is_none()
    {
        return (0, Vec::new());
    }

    let [primary_buckets, resize_buckets] = store.table_bucket_counts();
    let modulo = primary_buckets
        .max(resize_buckets)
        .max(1)
        .next_power_of_two() as u64;
    if store.entry_count() == 0 || modulo == 0 {
        return (0, Vec::new());
    }

    let mut cur = cursor % modulo;
    let mut scanned = 0usize;
    let mut wrapped = false;
    let mut out = Vec::new();

    while scanned < count {
        for table_index in 0..=1usize {
            let bucket_count = if table_index == 0 {
                primary_buckets
            } else {
                resize_buckets
            };
            if bucket_count == 0 || cur as usize >= bucket_count {
                continue;
            }
            if let Some((key, value_type, expires_at)) =
                store.bucket_snapshot(table_index, cur as usize)
            {
                if is_expired(expires_at, now_ms) {
                    continue;
                }
                if let Some(filter) = type_filter
                    && !type_matches(value_type, filter)
                {
                    continue;
                }
                if pattern.is_none_or(|matcher| glob_match(matcher, key.as_bytes())) {
                    out.push(key);
                }
            }
        }
        scanned += 1;
        cur = reverse_binary_next(cur, modulo);
        if cur == 0 {
            wrapped = true;
            break;
        }
    }

    (if wrapped { 0 } else { cur }, out)
}

fn reverse_binary_next(cursor: u64, modulo: u64) -> u64 {
    if modulo <= 1 {
        return 0;
    }
    let bits = modulo.trailing_zeros().max(1);
    let mask = (1u64 << bits) - 1;
    let low = cursor & mask;
    let rev = reverse_low_bits(low, bits);
    let next = rev.wrapping_add(1) & mask;
    reverse_low_bits(next, bits) & mask
}

fn reverse_low_bits(mut value: u64, bits: u32) -> u64 {
    let mut out = 0u64;
    let mut i = 0;
    while i < bits {
        out = (out << 1) | (value & 1);
        value >>= 1;
        i += 1;
    }
    out
}

fn type_matches(actual: &[u8], filter: &[u8]) -> bool {
    normalize_type_filter(filter).is_some_and(|expected| actual == expected)
}

fn normalize_type_filter(filter: &[u8]) -> Option<&'static [u8]> {
    if filter.eq_ignore_ascii_case(TYPE_STRING) {
        Some(TYPE_STRING)
    } else if filter.eq_ignore_ascii_case(TYPE_LIST) {
        Some(TYPE_LIST)
    } else if filter.eq_ignore_ascii_case(TYPE_SET) {
        Some(TYPE_SET)
    } else if filter.eq_ignore_ascii_case(TYPE_ZSET) {
        Some(TYPE_ZSET)
    } else if filter.eq_ignore_ascii_case(TYPE_HASH) {
        Some(TYPE_HASH)
    } else if filter.eq_ignore_ascii_case(TYPE_STREAM) {
        Some(TYPE_STREAM)
    } else {
        None
    }
}

fn is_expired(expires_at: Option<u64>, now_ms: u64) -> bool {
    expires_at.is_some_and(|deadline| deadline <= now_ms)
}

fn raw_key_response(key: &CompactString) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(
        key.as_bytes(),
    ))))
}

fn parse_u64(raw: &[u8]) -> SenkoResult<u64> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("invalid cursor"))?
        .parse::<u64>()
        .map_err(|_| SenkoError::Protocol("invalid cursor"))
}

fn parse_usize(raw: &[u8]) -> SenkoResult<usize> {
    std::str::from_utf8(raw)
        .map_err(|_| SenkoError::Protocol("syntax error"))?
        .parse::<usize>()
        .map_err(|_| SenkoError::Protocol("syntax error"))
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
    use std::{collections::HashSet, thread, time::Duration};

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::{QuickList, SenkoValue, SetObject, ZAddOptions, ZSetObject};
    use senko_proto::Frame;

    use super::{keys, randomkey, scan, touch};
    use crate::{Response, Store, store::current_unix_ms};

    fn bs<'a>(value: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(value)
    }

    fn set_string(store: &mut Store, key: &str) {
        let _ = store.set(
            CompactString::new(key),
            SenkoValue::from(Bytes::from_static(b"v")),
            Default::default(),
        );
    }

    fn key_array(response: Response) -> Vec<Vec<u8>> {
        let Response::Array(values) = response else {
            panic!("expected array")
        };
        values
            .iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    fn scan_page(response: Response) -> (u64, Vec<Vec<u8>>) {
        let Response::Array(top) = response else {
            panic!("expected array")
        };
        let Response::Value(Some(SenkoValue::Raw(cursor))) = &top[0] else {
            panic!("cursor")
        };
        let next = std::str::from_utf8(cursor).unwrap().parse::<u64>().unwrap();
        let Response::Array(values) = &top[1] else {
            panic!("values")
        };
        let out = values
            .iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        (next, out)
    }

    fn collect_scan_pages(store: &mut Store, extra: &[Frame<'_>]) -> Vec<Vec<u8>> {
        let mut cursor = 0u64;
        let mut out = Vec::new();
        loop {
            let cursor_buf = cursor.to_string().into_bytes();
            let mut args = Vec::with_capacity(extra.len() + 1);
            args.push(bs(&cursor_buf));
            args.extend_from_slice(extra);
            let (next, page) = scan_page(scan(store, &args).unwrap());
            out.extend(page);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        out
    }

    #[test]
    fn keys_returns_all_non_expired_keys() {
        let mut store = Store::default();
        set_string(&mut store, "a");
        set_string(&mut store, "b");
        set_string(&mut store, "gone");
        store.set_expiry(b"gone", current_unix_ms() + 20);
        thread::sleep(Duration::from_millis(30));

        let keys = key_array(keys(&mut store, &[bs(b"*")]).unwrap());
        let set: HashSet<_> = keys.into_iter().collect();
        assert_eq!(set.len(), 2);
        assert!(set.contains(b"a".as_slice()));
        assert!(set.contains(b"b".as_slice()));
    }

    #[test]
    fn keys_pattern_matching_and_empty_db() {
        let mut store = Store::default();
        set_string(&mut store, "hello");
        set_string(&mut store, "hallo");
        set_string(&mut store, "hllo");
        set_string(&mut store, "heello");

        let matched = key_array(keys(&mut store, &[bs(b"h?llo")]).unwrap());
        let set: HashSet<_> = matched.into_iter().collect();
        assert!(set.contains(b"hello".as_slice()));
        assert!(set.contains(b"hallo".as_slice()));
        assert!(!set.contains(b"hllo".as_slice()));
        assert!(!set.contains(b"heello".as_slice()));

        let mut empty = Store::default();
        assert!(key_array(keys(&mut empty, &[bs(b"*")]).unwrap()).is_empty());
    }

    #[test]
    fn scan_full_iteration_covers_all_keys() {
        let mut store = Store::default();
        for i in 0..50 {
            set_string(&mut store, &format!("k{i:02}"));
        }

        let mut cursor = 0u64;
        let mut seen = HashSet::new();
        loop {
            let cursor_buf = cursor.to_string().into_bytes();
            let (next, page) =
                scan_page(scan(&mut store, &[bs(&cursor_buf), bs(b"COUNT"), bs(b"3")]).unwrap());
            seen.extend(page);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn scan_match_type_and_combined_filters() {
        let mut store = Store::default();
        set_string(&mut store, "str:1");
        set_string(&mut store, "str:2");
        let mut list = QuickList::default();
        list.push_back(b"v");
        let _ = store.set(
            CompactString::new("list:1"),
            SenkoValue::List(Box::new(list)),
            Default::default(),
        );
        let mut zset = ZSetObject::default();
        let _ = zset.add(1.0, CompactString::new("m"), ZAddOptions::default());
        let _ = store.set(
            CompactString::new("z:1"),
            SenkoValue::ZSet(Box::new(zset)),
            Default::default(),
        );

        let strings = collect_scan_pages(&mut store, &[bs(b"TYPE"), bs(b"string")]);
        assert!(strings.iter().all(|key| key.starts_with(b"str:")));

        let zsets = collect_scan_pages(&mut store, &[bs(b"TYPE"), bs(b"zset")]);
        assert_eq!(zsets, vec![b"z:1".to_vec()]);

        let (next, invalid) =
            scan_page(scan(&mut store, &[bs(b"0"), bs(b"TYPE"), bs(b"bogus")]).unwrap());
        assert_eq!(next, 0);
        assert!(invalid.is_empty());

        let combined = collect_scan_pages(
            &mut store,
            &[bs(b"MATCH"), bs(b"str:*"), bs(b"TYPE"), bs(b"string")],
        );
        assert_eq!(combined.len(), 2);
        assert!(combined.iter().all(|key| key.starts_with(b"str:")));
    }

    #[test]
    fn scan_during_mutation_stays_safe() {
        let mut store = Store::default();
        for i in 0..20 {
            set_string(&mut store, &format!("k{i:02}"));
        }

        let (next, _) = scan_page(scan(&mut store, &[bs(b"0"), bs(b"COUNT"), bs(b"2")]).unwrap());
        set_string(&mut store, "late");
        let cursor_buf = next.to_string().into_bytes();
        let _ = scan(&mut store, &[bs(&cursor_buf), bs(b"COUNT"), bs(b"2")]).unwrap();
    }

    #[test]
    fn randomkey_and_touch_behave() {
        let mut store = Store::default();
        assert_eq!(randomkey(&mut store, &[]).unwrap(), Response::Value(None));

        set_string(&mut store, "live");
        set_string(&mut store, "gone");
        store.set_expiry(b"gone", current_unix_ms() + 20);
        thread::sleep(Duration::from_millis(30));

        let Response::Value(Some(SenkoValue::Raw(key))) = randomkey(&mut store, &[]).unwrap()
        else {
            panic!("expected random key")
        };
        assert_eq!(key.as_ref(), b"live");

        let before = store.live_entries_snapshot(current_unix_ms());
        let old_lru = before
            .iter()
            .find(|(key, _, _, _)| key.as_str() == "live")
            .unwrap()
            .3;
        assert_eq!(
            touch(&mut store, &[bs(b"live"), bs(b"missing"), bs(b"gone")]).unwrap(),
            Response::Integer(1)
        );
        let after = store.live_entries_snapshot(current_unix_ms());
        let new_lru = after
            .iter()
            .find(|(key, _, _, _)| key.as_str() == "live")
            .unwrap()
            .3;
        assert!(new_lru >= old_lru);
    }

    #[test]
    fn scan_type_set_works() {
        let mut store = Store::default();
        let mut set = SetObject::default();
        let _ = set.add(b"a");
        let _ = store.set(
            CompactString::new("set:1"),
            SenkoValue::Set(Box::new(set)),
            Default::default(),
        );
        set_string(&mut store, "str:1");

        let keys = collect_scan_pages(&mut store, &[bs(b"TYPE"), bs(b"set")]);
        assert_eq!(keys, vec![b"set:1".to_vec()]);
    }
}
