use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::hash::basic as hbasic,
    store::{Store, current_unix_ms},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn int_of(response: Response) -> i64 {
    match response {
        Response::Integer(value) => value,
        other => panic!("expected integer response, got {other:?}"),
    }
}

fn bytes_of(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(value)) => Some(value.as_bytes().into_owned()),
        Response::Value(None) => None,
        other => panic!("expected value response, got {other:?}"),
    }
}

#[test]
fn hset_counts_new_vs_updated_fields() {
    let mut store = Store::default();
    assert_eq!(
        int_of(
            hbasic::hset(
                &mut store,
                &[bs(b"h"), bs(b"f1"), bs(b"1"), bs(b"f2"), bs(b"2")]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            hbasic::hset(
                &mut store,
                &[bs(b"h"), bs(b"f2"), bs(b"22"), bs(b"f3"), bs(b"3")]
            )
            .unwrap()
        ),
        1
    );
}

#[test]
fn hget_returns_null_for_expired_field() {
    let mut store = Store::default();
    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("f"),
        SenkoValue::Raw(Bytes::from_static(b"v")),
        Some(now + 20),
    );
    store.advance_expiry_wheel(now + 50);

    assert_eq!(
        bytes_of(hbasic::hget(&mut store, &[bs(b"h"), bs(b"f")]).unwrap()),
        None
    );
}

#[test]
fn hdel_last_field_deletes_hash_key() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"f"), bs(b"1")]).unwrap();
    assert_eq!(
        int_of(hbasic::hdel(&mut store, &[bs(b"h"), bs(b"f")]).unwrap()),
        1
    );
    assert!(store.get_hash(b"h").is_none());
    assert!(!store.exists(b"h"));
}

#[test]
fn hgetall_returns_map_shape() {
    let mut store = Store::default();
    let _ = hbasic::hset(
        &mut store,
        &[bs(b"h"), bs(b"f1"), bs(b"1"), bs(b"f2"), bs(b"2")],
    )
    .unwrap();
    let result = hbasic::hgetall(&mut store, &[bs(b"h")]).unwrap();
    match result {
        Response::Map(items) => {
            assert_eq!(items.len(), 4);
        }
        other => panic!("expected map response, got {other:?}"),
    }
}

#[test]
fn hmget_handles_existing_missing_and_expired_fields() {
    let mut store = Store::default();
    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("live"), SenkoValue::Int(7), None);
    let _ = hash.set(
        CompactString::from("exp"),
        SenkoValue::Raw(Bytes::from_static(b"gone")),
        Some(now + 20),
    );
    store.advance_expiry_wheel(now + 50);

    let response = hbasic::hmget(
        &mut store,
        &[bs(b"h"), bs(b"live"), bs(b"miss"), bs(b"exp")],
    )
    .unwrap();
    match response {
        Response::Array(values) => {
            assert_eq!(values.len(), 3);
            assert_eq!(bytes_of(values[0].clone()), Some(b"7".to_vec()));
            assert_eq!(bytes_of(values[1].clone()), None);
            assert_eq!(bytes_of(values[2].clone()), None);
        }
        other => panic!("expected array response, got {other:?}"),
    }
}

#[test]
fn hsetnx_respects_live_and_expired_fields() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"f"), bs(b"1")]).unwrap();
    assert_eq!(
        int_of(hbasic::hsetnx(&mut store, &[bs(b"h"), bs(b"f"), bs(b"2")]).unwrap()),
        0
    );

    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("expired"),
        SenkoValue::Int(1),
        Some(now),
    );
    assert_eq!(
        int_of(hbasic::hsetnx(&mut store, &[bs(b"h"), bs(b"expired"), bs(b"3")]).unwrap()),
        1
    );
}

#[test]
fn hstrlen_counts_int_digits_without_materialization() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"n"), bs(b"-12345")]).unwrap();
    assert_eq!(
        int_of(hbasic::hstrlen(&mut store, &[bs(b"h"), bs(b"n")]).unwrap()),
        6
    );
}

#[test]
fn listpack_path_is_used_for_small_hash_commands() {
    let mut store = Store::default();
    let _ = hbasic::hset(
        &mut store,
        &[
            bs(b"h"),
            bs(b"a"),
            bs(b"1"),
            bs(b"b"),
            bs(b"2"),
            bs(b"c"),
            bs(b"3"),
        ],
    )
    .unwrap();
    assert!(store.get_hash(b"h").unwrap().is_listpack());

    let _ = hbasic::hget(&mut store, &[bs(b"h"), bs(b"a")]).unwrap();
    let _ = hbasic::hexists(&mut store, &[bs(b"h"), bs(b"b")]).unwrap();
    let _ = hbasic::hlen(&mut store, &[bs(b"h")]).unwrap();
    let _ = hbasic::hkeys(&mut store, &[bs(b"h")]).unwrap();
    let _ = hbasic::hvals(&mut store, &[bs(b"h")]).unwrap();
    let _ = hbasic::hgetall(&mut store, &[bs(b"h")]).unwrap();
    let _ = hbasic::hmget(&mut store, &[bs(b"h"), bs(b"a"), bs(b"x")]).unwrap();
    let _ = hbasic::hmset(&mut store, &[bs(b"h"), bs(b"d"), bs(b"4")]).unwrap();
    let _ = hbasic::hsetnx(&mut store, &[bs(b"h"), bs(b"e"), bs(b"5")]).unwrap();
    let _ = hbasic::hstrlen(&mut store, &[bs(b"h"), bs(b"a")]).unwrap();
    let _ = hbasic::hdel(&mut store, &[bs(b"h"), bs(b"e")]).unwrap();

    assert!(store.get_hash(b"h").unwrap().is_listpack());
}
