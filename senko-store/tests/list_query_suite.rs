use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::list::{basic, query},
    store::{SetOptions, Store},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn set_raw(store: &mut Store, key: &str, value: &[u8]) {
    let _ = store.set(
        CompactString::from(key),
        SenkoValue::Raw(Bytes::copy_from_slice(value)),
        SetOptions::default(),
    );
}

fn rpush_all(store: &mut Store, key: &[u8], values: &[&[u8]]) {
    let mut args = Vec::with_capacity(values.len() + 1);
    args.push(bs(key));
    args.extend(values.iter().copied().map(bs));
    let _ = basic::rpush(store, &args).unwrap();
}

fn bulk_vec(response: Response) -> Vec<Vec<u8>> {
    match response {
        Response::Array(values) => values
            .into_iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("expected bulk value, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

fn opt_bulk(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
        Response::Value(None) => None,
        other => panic!("expected bulk response, got {other:?}"),
    }
}

fn int_vec(response: Response) -> Vec<i64> {
    match response {
        Response::Array(values) => values
            .into_iter()
            .map(|item| match item {
                Response::Integer(value) => value,
                other => panic!("expected integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

#[test]
fn lrange_handles_negative_indices_clamping_and_empty_ranges() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"c", b"d"]);

    assert_eq!(
        bulk_vec(query::lrange(&mut store, &[bs(b"k"), bs(b"-2"), bs(b"-1")]).unwrap()),
        vec![b"c".to_vec(), b"d".to_vec()]
    );
    assert_eq!(
        bulk_vec(query::lrange(&mut store, &[bs(b"k"), bs(b"-99"), bs(b"99")]).unwrap()),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );
    assert!(
        bulk_vec(query::lrange(&mut store, &[bs(b"k"), bs(b"3"), bs(b"1")]).unwrap()).is_empty()
    );
}

#[test]
fn lrange_spans_multiple_quicklist_nodes() {
    let mut store = Store::default();
    {
        let list = store.get_or_create_list(CompactString::from("k"));
        list.fill = 4;
        for i in 0..12 {
            list.push_back(i.to_string().as_bytes());
        }
    }

    let values = bulk_vec(query::lrange(&mut store, &[bs(b"k"), bs(b"3"), bs(b"8")]).unwrap());
    assert_eq!(
        values,
        vec![
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec(),
            b"7".to_vec(),
            b"8".to_vec(),
        ]
    );
}

#[test]
fn lindex_reads_first_last_and_middle_elements() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"c", b"d"]);

    assert_eq!(
        opt_bulk(query::lindex(&mut store, &[bs(b"k"), bs(b"0")]).unwrap()),
        Some(b"a".to_vec())
    );
    assert_eq!(
        opt_bulk(query::lindex(&mut store, &[bs(b"k"), bs(b"-1")]).unwrap()),
        Some(b"d".to_vec())
    );
    assert_eq!(
        opt_bulk(query::lindex(&mut store, &[bs(b"k"), bs(b"2")]).unwrap()),
        Some(b"c".to_vec())
    );
}

#[test]
fn lset_supports_negative_index_and_errors() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"c"]);

    assert_eq!(
        query::lset(&mut store, &[bs(b"k"), bs(b"-1"), bs(b"z")]).unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        opt_bulk(query::lindex(&mut store, &[bs(b"k"), bs(b"-1")]).unwrap()),
        Some(b"z".to_vec())
    );

    assert!(
        matches!(query::lset(&mut store, &[bs(b"k"), bs(b"10"), bs(b"x")]), Err(senko_core::SenkoError::ProtocolMessage(message)) if message.as_str() == "ERR index out of range")
    );
    assert!(
        matches!(query::lset(&mut store, &[bs(b"missing"), bs(b"0"), bs(b"x")]), Err(senko_core::SenkoError::ProtocolMessage(message)) if message.as_str() == "ERR no such key")
    );
}

#[test]
fn linsert_supports_before_and_after_at_head_middle_and_tail() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"c"]);

    assert_eq!(
        query::linsert(
            &mut store,
            &[bs(b"k"), bs(b"BEFORE"), bs(b"a"), bs(b"head")]
        )
        .unwrap(),
        Response::Integer(4)
    );
    assert_eq!(
        query::linsert(&mut store, &[bs(b"k"), bs(b"AFTER"), bs(b"b"), bs(b"mid")]).unwrap(),
        Response::Integer(5)
    );
    assert_eq!(
        query::linsert(&mut store, &[bs(b"k"), bs(b"AFTER"), bs(b"c"), bs(b"tail")]).unwrap(),
        Response::Integer(6)
    );

    assert_eq!(
        bulk_vec(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![
            b"head".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"mid".to_vec(),
            b"c".to_vec(),
            b"tail".to_vec()
        ]
    );
}

#[test]
fn linsert_pivot_not_found_returns_minus_one() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a"]);
    assert_eq!(
        query::linsert(&mut store, &[bs(b"k"), bs(b"BEFORE"), bs(b"x"), bs(b"y")]).unwrap(),
        Response::Integer(-1)
    );
}

#[test]
fn lpos_count_zero_returns_all_matches_in_order() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"a", b"c", b"a"]);
    assert_eq!(
        int_vec(query::lpos(&mut store, &[bs(b"k"), bs(b"a"), bs(b"COUNT"), bs(b"0")]).unwrap()),
        vec![0, 2, 4]
    );
}

#[test]
fn lpos_rank_negative_two_skips_last_match_from_tail() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"a", b"c", b"a"]);
    assert_eq!(
        query::lpos(&mut store, &[bs(b"k"), bs(b"a"), bs(b"RANK"), bs(b"-2")]).unwrap(),
        Response::Integer(2)
    );
}

#[test]
fn lpos_maxlen_stops_early() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"x", b"x", b"x", b"a"]);
    assert_eq!(
        query::lpos(&mut store, &[bs(b"k"), bs(b"a"), bs(b"MAXLEN"), bs(b"3")]).unwrap(),
        Response::Value(None)
    );
}

#[test]
fn lpos_count_specified_with_no_match_returns_empty_array() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b"]);
    assert!(
        int_vec(query::lpos(&mut store, &[bs(b"k"), bs(b"z"), bs(b"COUNT"), bs(b"2")]).unwrap())
            .is_empty()
    );
}

#[test]
fn wrongtype_is_reported_for_query_commands_on_string_key() {
    let mut store = Store::default();
    set_raw(&mut store, "key", b"value");

    assert!(matches!(
        query::lrange(&mut store, &[bs(b"key"), bs(b"0"), bs(b"1")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        query::lindex(&mut store, &[bs(b"key"), bs(b"0")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        query::lset(&mut store, &[bs(b"key"), bs(b"0"), bs(b"x")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        query::linsert(&mut store, &[bs(b"key"), bs(b"BEFORE"), bs(b"x"), bs(b"y")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        query::lpos(&mut store, &[bs(b"key"), bs(b"x")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
}
