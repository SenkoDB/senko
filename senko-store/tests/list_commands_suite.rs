use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::list::basic,
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

fn int_of(response: Response) -> i64 {
    match response {
        Response::Integer(value) => value,
        other => panic!("expected integer response, got {other:?}"),
    }
}

fn bulk_of(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
        Response::Value(None) => None,
        other => panic!("expected bulk response, got {other:?}"),
    }
}

fn array_of(response: Response) -> Vec<Option<Vec<u8>>> {
    match response {
        Response::Array(values) => values
            .into_iter()
            .map(|item| match item {
                Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.to_vec()),
                Response::Value(None) => None,
                other => panic!("expected array bulk response, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

#[test]
fn lpush_multiple_args_preserves_redis_order() {
    let mut store = Store::default();
    assert_eq!(
        int_of(basic::lpush(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap()),
        3
    );

    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"c".to_vec())
    );
    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"b".to_vec())
    );
    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"a".to_vec())
    );
}

#[test]
fn rpush_multiple_args_preserves_order() {
    let mut store = Store::default();
    assert_eq!(
        int_of(basic::rpush(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap()),
        3
    );

    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"a".to_vec())
    );
    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"b".to_vec())
    );
    assert_eq!(
        bulk_of(basic::lpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"c".to_vec())
    );
}

#[test]
fn lpushx_missing_key_returns_zero() {
    let mut store = Store::default();
    assert_eq!(
        int_of(basic::lpushx(&mut store, &[bs(b"missing"), bs(b"a")]).unwrap()),
        0
    );
    assert!(!store.exists(b"missing"));
}

#[test]
fn lpop_with_count_greater_than_length_returns_all_elements() {
    let mut store = Store::default();
    let _ = basic::rpush(&mut store, &[bs(b"k"), bs(b"a"), bs(b"b"), bs(b"c")]).unwrap();

    assert_eq!(
        array_of(basic::lpop(&mut store, &[bs(b"k"), bs(b"10")]).unwrap()),
        vec![
            Some(b"a".to_vec()),
            Some(b"b".to_vec()),
            Some(b"c".to_vec())
        ]
    );
    assert!(!store.exists(b"k"));
}

#[test]
fn lpop_count_zero_returns_empty_array() {
    let mut store = Store::default();
    let _ = basic::rpush(&mut store, &[bs(b"k"), bs(b"a")]).unwrap();

    assert!(array_of(basic::lpop(&mut store, &[bs(b"k"), bs(b"0")]).unwrap()).is_empty());
    assert_eq!(int_of(basic::llen(&mut store, &[bs(b"k")]).unwrap()), 1);
}

#[test]
fn rpop_auto_deletes_empty_list() {
    let mut store = Store::default();
    let _ = basic::rpush(&mut store, &[bs(b"k"), bs(b"a")]).unwrap();

    assert_eq!(
        bulk_of(basic::rpop(&mut store, &[bs(b"k")]).unwrap()),
        Some(b"a".to_vec())
    );
    assert!(!store.exists(b"k"));
    assert!(store.get_list(b"k").is_none());
}

#[test]
fn llen_missing_key_returns_zero() {
    let mut store = Store::default();
    assert_eq!(
        int_of(basic::llen(&mut store, &[bs(b"missing")]).unwrap()),
        0
    );
}

#[test]
fn wrongtype_is_reported_for_all_list_commands_on_string_key() {
    let mut store = Store::default();
    set_raw(&mut store, "key", b"value");

    assert!(matches!(
        basic::lpush(&mut store, &[bs(b"key"), bs(b"a")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::rpush(&mut store, &[bs(b"key"), bs(b"a")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::lpushx(&mut store, &[bs(b"key"), bs(b"a")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::rpushx(&mut store, &[bs(b"key"), bs(b"a")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::lpop(&mut store, &[bs(b"key")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::rpop(&mut store, &[bs(b"key")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
    assert!(matches!(
        basic::llen(&mut store, &[bs(b"key")]),
        Err(senko_core::SenkoError::WrongType {
            expected: "list",
            actual: "string"
        })
    ));
}
