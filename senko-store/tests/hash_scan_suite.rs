use std::collections::HashSet;

use compact_str::CompactString;
use proptest::prelude::*;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::hash::{expiry as hexp, scan},
    store::{Store, current_unix_ms},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn parse_hscan(response: Response) -> (u64, Vec<Vec<u8>>) {
    let Response::Array(top) = response else {
        panic!("expected top-level array response");
    };
    assert_eq!(top.len(), 2);
    let cursor = match &top[0] {
        Response::Value(Some(value)) => std::str::from_utf8(value.as_bytes().as_ref())
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        other => panic!("expected cursor value, got {other:?}"),
    };
    let Response::Array(items) = &top[1] else {
        panic!("expected result array");
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items.iter() {
        match item {
            Response::Value(Some(value)) => out.push(value.as_bytes().to_vec()),
            Response::Value(None) => out.push(Vec::new()),
            other => panic!("expected bulk value, got {other:?}"),
        }
    }
    (cursor, out)
}

fn collect_all(
    mut store: &mut Store,
    key: &[u8],
    pattern: Option<&[u8]>,
    count: usize,
    novalues: bool,
) -> Vec<Vec<u8>> {
    let mut cursor = 0u64;
    let mut out = Vec::new();
    loop {
        let mut args = vec![Frame::BulkString(key)];
        let cursor_buf = cursor.to_string().into_bytes();
        args.push(Frame::BulkString(&cursor_buf));
        if let Some(pattern) = pattern {
            args.push(bs(b"MATCH"));
            args.push(Frame::BulkString(pattern));
        }
        args.push(bs(b"COUNT"));
        let count_buf = count.to_string().into_bytes();
        args.push(Frame::BulkString(&count_buf));
        if novalues {
            args.push(bs(b"NOVALUES"));
        }
        let (next, page) = parse_hscan(scan::hscan(&mut store, &args).unwrap());
        out.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    out
}

#[test]
fn hscan_full_iteration_covers_all_fields() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    for i in 0..64 {
        let _ = hash.set(
            CompactString::from(format!("f{i}")),
            SenkoValue::Int(i as i64),
            None,
        );
    }
    let raw = collect_all(&mut store, b"h", None, 10, false);
    let fields: HashSet<Vec<u8>> = raw.chunks(2).map(|pair| pair[0].clone()).collect();
    assert_eq!(fields.len(), 64);
}

#[test]
fn hscan_match_patterns() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    for field in [b"hello".as_slice(), b"hallo", b"hullo", b"xyz", b"aeon"] {
        let _ = hash.set(
            CompactString::from(std::str::from_utf8(field).unwrap()),
            SenkoValue::Raw(field.into()),
            None,
        );
    }

    let any = collect_all(&mut store, b"h", Some(b"*"), 20, true);
    assert!(any.len() >= 5);
    let hqllo = collect_all(&mut store, b"h", Some(b"h?llo"), 20, true);
    let hqllo_set: HashSet<Vec<u8>> = hqllo.into_iter().collect();
    assert!(hqllo_set.contains(b"hello".as_slice()));
    assert!(hqllo_set.contains(b"hallo".as_slice()));
    assert!(hqllo_set.contains(b"hullo".as_slice()));

    let vowels = collect_all(&mut store, b"h", Some(b"[aeiou]*"), 20, true);
    let vowels_set: HashSet<Vec<u8>> = vowels.into_iter().collect();
    assert!(vowels_set.contains(b"aeon".as_slice()));
}

#[test]
fn hscan_count_paginates_and_novalues_only_fields() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    for i in 0..200 {
        let _ = hash.set(
            CompactString::from(format!("f{i}")),
            SenkoValue::Int(i as i64),
            None,
        );
    }

    let mut cursor = 0u64;
    let mut pages = 0usize;
    loop {
        let cursor_buf = cursor.to_string().into_bytes();
        let args = [
            bs(b"h"),
            Frame::BulkString(&cursor_buf),
            bs(b"COUNT"),
            bs(b"2"),
            bs(b"NOVALUES"),
        ];
        let (next, _page) = parse_hscan(scan::hscan(&mut store, &args).unwrap());
        pages += 1;
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert!(pages > 1);
}

#[test]
fn hscan_skips_expired_fields_mid_scan() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("live"), SenkoValue::Int(1), None);
    let _ = hash.set(CompactString::from("exp"), SenkoValue::Int(2), None);
    let _ = hexp::hpexpire(
        &mut store,
        &[bs(b"h"), bs(b"200"), bs(b"FIELDS"), bs(b"1"), bs(b"exp")],
    )
    .unwrap();
    let now = current_unix_ms();
    let _ = store.advance_expiry_wheel(now + 300);

    let out = collect_all(&mut store, b"h", None, 10, true);
    let set: HashSet<Vec<u8>> = out.into_iter().collect();
    assert!(set.contains(b"live".as_slice()));
    assert!(!set.contains(b"exp".as_slice()));
}

proptest! {
    #[test]
    fn hscan_growth_mid_iteration_yields_all_live_at_least_once(seed in 0u64..10_000, n in 1usize..40) {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("h"));
        for i in 0..n {
            let _ = hash.set(CompactString::from(format!("f{i}")), SenkoValue::Int(i as i64), None);
        }

        let mut cursor = 0u64;
        let mut seen = HashSet::<Vec<u8>>::new();
        let mut grew = false;
        loop {
            let cursor_buf = cursor.to_string().into_bytes();
            let args = [bs(b"h"), Frame::BulkString(&cursor_buf), bs(b"COUNT"), bs(b"3"), bs(b"NOVALUES")];
            let (next, page) = parse_hscan(scan::hscan(&mut store, &args).unwrap());
            for field in page {
                seen.insert(field);
            }
            if !grew && next != 0 {
                let hash = store.get_or_create_hash(CompactString::from("h"));
                let _ = hash.set(CompactString::from(format!("g{seed}")), SenkoValue::Int(42), None);
                grew = true;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        let hash = store.get_hash(b"h").unwrap();
        let live: HashSet<Vec<u8>> = hash.iter_live(current_unix_ms()).map(|(f, _)| f.as_bytes().to_vec()).collect();
        prop_assert!(live.is_subset(&seen));
    }
}

#[test]
fn hscan_listpack_and_hashtable_have_same_fields() {
    let mut listpack_store = Store::default();
    let mut table_store = Store::default();
    {
        let hash = listpack_store.get_or_create_hash(CompactString::from("h"));
        for i in 0..40 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        assert!(hash.is_listpack());
    }
    {
        let hash = table_store.get_or_create_hash(CompactString::from("h"));
        for i in 0..200 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        for i in 40..200 {
            let _ = hash.delete(format!("f{i}").as_bytes());
        }
        let _ = hash.set(
            CompactString::from("f0"),
            SenkoValue::Raw(bytes::Bytes::from(vec![b'x'; 80])),
            None,
        );
        assert!(!hash.is_listpack());
    }

    let lp: HashSet<Vec<u8>> = collect_all(&mut listpack_store, b"h", None, 10, true)
        .into_iter()
        .collect();
    let ht: HashSet<Vec<u8>> = collect_all(&mut table_store, b"h", None, 10, true)
        .into_iter()
        .collect();
    assert_eq!(lp, ht);
}
