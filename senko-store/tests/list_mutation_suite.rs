use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::list::{basic, mutation, query},
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

fn bulk(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
        Response::Value(None) => None,
        other => panic!("expected bulk response, got {other:?}"),
    }
}

fn bulks(response: Response) -> Vec<Vec<u8>> {
    match response {
        Response::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("expected bulk array, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

fn lmpop_parts(response: Response) -> Option<(Vec<u8>, Vec<Vec<u8>>)> {
    match response {
        Response::Value(None) => None,
        Response::Array(items) => {
            assert_eq!(items.len(), 2);
            let key = match &items[0] {
                Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                other => panic!("expected key bulk, got {other:?}"),
            };
            let values = match &items[1] {
                Response::Array(values) => values
                    .iter()
                    .map(|item| match item {
                        Response::Value(Some(SenkoValue::Raw(bytes))) => bytes.to_vec(),
                        other => panic!("expected bulk item, got {other:?}"),
                    })
                    .collect(),
                other => panic!("expected nested array, got {other:?}"),
            };
            Some((key, values))
        }
        other => panic!("expected null or array response, got {other:?}"),
    }
}

#[test]
fn lrem_zero_removes_all_and_positive_count_removes_from_head() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"x", b"b", b"x", b"x"]);
    assert_eq!(
        mutation::lrem(&mut store, &[bs(b"k"), bs(b"2"), bs(b"x")]).unwrap(),
        Response::Integer(2)
    );
    assert_eq!(
        bulks(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![b"a".to_vec(), b"b".to_vec(), b"x".to_vec()]
    );

    assert_eq!(
        mutation::lrem(&mut store, &[bs(b"k"), bs(b"0"), bs(b"x")]).unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        bulks(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
fn lrem_negative_one_removes_from_tail_and_missing_value_returns_zero() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"x", b"a", b"x"]);
    assert_eq!(
        mutation::lrem(&mut store, &[bs(b"k"), bs(b"-1"), bs(b"x")]).unwrap(),
        Response::Integer(1)
    );
    assert_eq!(
        bulks(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![b"x".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        mutation::lrem(&mut store, &[bs(b"k"), bs(b"1"), bs(b"zzz")]).unwrap(),
        Response::Integer(0)
    );
}

#[test]
fn ltrim_empty_result_deletes_key() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b"]);
    assert_eq!(
        mutation::ltrim(&mut store, &[bs(b"k"), bs(b"5"), bs(b"1")]).unwrap(),
        Response::Simple(b"OK")
    );
    assert!(!store.exists(b"k"));
}

#[test]
fn ltrim_across_multiple_quicklist_nodes() {
    let mut store = Store::default();
    {
        let list = store.get_or_create_list(CompactString::from("k"));
        list.fill = 4;
        for i in 0..10 {
            list.push_back(i.to_string().as_bytes());
        }
    }
    assert_eq!(
        mutation::ltrim(&mut store, &[bs(b"k"), bs(b"2"), bs(b"6")]).unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        bulks(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec(),
            b"5".to_vec(),
            b"6".to_vec()
        ]
    );
}

#[test]
fn lmove_same_source_destination_left_right_rotates() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b", b"c"]);
    assert_eq!(
        bulk(
            mutation::lmove(&mut store, &[bs(b"k"), bs(b"k"), bs(b"LEFT"), bs(b"RIGHT")]).unwrap()
        ),
        Some(b"a".to_vec())
    );
    assert_eq!(
        bulks(query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"-1")]).unwrap()),
        vec![b"b".to_vec(), b"c".to_vec(), b"a".to_vec()]
    );
}

#[test]
fn lmove_empty_source_returns_null() {
    let mut store = Store::default();
    assert_eq!(
        mutation::lmove(
            &mut store,
            &[bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT")]
        )
        .unwrap(),
        Response::Value(None)
    );
}

#[test]
fn rpoplpush_matches_lmove_right_left() {
    let mut left = Store::default();
    let mut right = Store::default();
    rpush_all(&mut left, b"src", &[b"a", b"b"]);
    rpush_all(&mut right, b"src", &[b"a", b"b"]);

    let rp = mutation::rpoplpush(&mut left, &[bs(b"src"), bs(b"dst")]).unwrap();
    let lm = mutation::lmove(
        &mut right,
        &[bs(b"src"), bs(b"dst"), bs(b"RIGHT"), bs(b"LEFT")],
    )
    .unwrap();
    assert_eq!(rp, lm);
    assert_eq!(
        bulks(query::lrange(&mut left, &[bs(b"dst"), bs(b"0"), bs(b"-1")]).unwrap()),
        bulks(query::lrange(&mut right, &[bs(b"dst"), bs(b"0"), bs(b"-1")]).unwrap())
    );
}

#[test]
fn lmpop_skips_empty_first_key_and_pops_from_second() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k2", &[b"a", b"b"]);
    assert_eq!(
        lmpop_parts(
            mutation::lmpop(&mut store, &[bs(b"2"), bs(b"k1"), bs(b"k2"), bs(b"LEFT")]).unwrap()
        ),
        Some((b"k2".to_vec(), vec![b"a".to_vec()]))
    );
}

#[test]
fn lmpop_count_three_from_two_element_list_returns_two() {
    let mut store = Store::default();
    rpush_all(&mut store, b"k", &[b"a", b"b"]);
    assert_eq!(
        lmpop_parts(
            mutation::lmpop(
                &mut store,
                &[bs(b"1"), bs(b"k"), bs(b"RIGHT"), bs(b"COUNT"), bs(b"3")]
            )
            .unwrap()
        ),
        Some((b"k".to_vec(), vec![b"b".to_vec(), b"a".to_vec()]))
    );
}

#[test]
fn lmpop_all_keys_empty_returns_null() {
    let mut store = Store::default();
    assert_eq!(
        mutation::lmpop(&mut store, &[bs(b"2"), bs(b"a"), bs(b"b"), bs(b"LEFT")]).unwrap(),
        Response::Value(None)
    );
}

#[test]
fn lmpop_numkeys_mismatch_returns_exact_error() {
    let mut store = Store::default();
    assert!(
        matches!(mutation::lmpop(&mut store, &[bs(b"3"), bs(b"a"), bs(b"b"), bs(b"LEFT")]), Err(senko_core::SenkoError::ProtocolMessage(message)) if message.as_str() == "ERR numkeys does not match number of keys")
    );
}

#[test]
fn wrongtype_on_lmove_when_destination_holds_string() {
    let mut store = Store::default();
    rpush_all(&mut store, b"src", &[b"a"]);
    set_raw(&mut store, "dst", b"value");
    assert!(matches!(
        mutation::lmove(
            &mut store,
            &[bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT")]
        ),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
}
