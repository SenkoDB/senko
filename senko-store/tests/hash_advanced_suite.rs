use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{SenkoError, SenkoValue};
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::hash::advanced,
    store::{Store, current_unix_ms},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn int_of(response: Response) -> i64 {
    match response {
        Response::Integer(value) => value,
        other => panic!("expected integer, got {other:?}"),
    }
}

fn bytes_of(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(value)) => Some(value.as_bytes().into_owned()),
        Response::Value(None) => None,
        other => panic!("expected value, got {other:?}"),
    }
}

#[test]
fn hincrby_missing_key_and_field_creates_both() {
    let mut store = Store::default();
    let res = advanced::hincrby(&mut store, &[bs(b"h"), bs(b"f"), bs(b"1")]).unwrap();
    assert_eq!(int_of(res), 1);
    assert!(store.get_hash(b"h").is_some());
}

#[test]
fn hincrbyfloat_precision_round_trip() {
    let mut store = Store::default();
    let _ = advanced::hincrbyfloat(&mut store, &[bs(b"h"), bs(b"f"), bs(b"10.5")]).unwrap();
    let res = advanced::hincrbyfloat(&mut store, &[bs(b"h"), bs(b"f"), bs(b"0.1")]).unwrap();
    assert_eq!(bytes_of(res), Some(b"10.6".to_vec()));
}

#[test]
fn hrandfield_positive_count_over_len_returns_all_fields() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("a"), SenkoValue::Int(1), None);
    let _ = hash.set(CompactString::from("b"), SenkoValue::Int(2), None);
    let _ = hash.set(CompactString::from("c"), SenkoValue::Int(3), None);

    let res = advanced::hrandfield(&mut store, &[bs(b"h"), bs(b"100")]).unwrap();
    match res {
        Response::Array(items) => assert_eq!(items.len(), 3),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn hrandfield_negative_count_allows_duplicates() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("only"),
        SenkoValue::Raw(Bytes::from_static(b"v")),
        None,
    );

    let res = advanced::hrandfield(&mut store, &[bs(b"h"), bs(b"-3")]).unwrap();
    match res {
        Response::Array(items) => {
            assert_eq!(items.len(), 3);
            for item in items.iter() {
                assert_eq!(bytes_of(item.clone()), Some(b"only".to_vec()));
            }
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn hrandfield_withvalues_interleaves_field_and_value() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("a"), SenkoValue::Int(1), None);
    let _ = hash.set(CompactString::from("b"), SenkoValue::Int(2), None);

    let res = advanced::hrandfield(&mut store, &[bs(b"h"), bs(b"2"), bs(b"WITHVALUES")]).unwrap();
    match res {
        Response::Array(items) => assert_eq!(items.len(), 4),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn hgetdel_partial_existing_and_missing_fields() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("a"), SenkoValue::Int(1), None);
    let _ = hash.set(CompactString::from("b"), SenkoValue::Int(2), None);

    let res = advanced::hgetdel(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FIELDS"),
            bs(b"3"),
            bs(b"a"),
            bs(b"x"),
            bs(b"b"),
        ],
    )
    .unwrap();
    match res {
        Response::Array(items) => {
            assert_eq!(bytes_of(items[0].clone()), Some(b"1".to_vec()));
            assert_eq!(bytes_of(items[1].clone()), None);
            assert_eq!(bytes_of(items[2].clone()), Some(b"2".to_vec()));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn hgetdel_empty_hash_removes_key() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(CompactString::from("a"), SenkoValue::Int(1), None);

    let _ = advanced::hgetdel(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a")]).unwrap();
    assert!(store.get_hash(b"h").is_none());
}

#[test]
fn hgetex_persist_removes_field_ttl() {
    let mut store = Store::default();
    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("a"),
        SenkoValue::Int(1),
        Some(now + 5_000),
    );

    let _ = advanced::hgetex(
        &mut store,
        &[bs(b"h"), bs(b"PERSIST"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    let hf = store
        .get_hash(b"h")
        .and_then(|h| h.get(b"a", now + 1))
        .expect("field should exist");
    assert_eq!(hf.expires_at, None);
}

#[test]
fn hgetex_expired_field_returns_null_and_no_ttl_update() {
    let mut store = Store::default();
    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("a"),
        SenkoValue::Raw(Bytes::from_static(b"v")),
        Some(now),
    );

    let res = advanced::hgetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"EX"),
            bs(b"10"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    match res {
        Response::Array(items) => assert_eq!(bytes_of(items[0].clone()), None),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn hgetdel_reports_numfields_errors() {
    let mut store = Store::default();

    let mismatch =
        advanced::hgetdel(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"2"), bs(b"a")]).unwrap_err();
    match mismatch {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("numfields")),
        other => panic!("expected protocol message, got {other:?}"),
    }

    let zero =
        advanced::hgetdel(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"0"), bs(b"a")]).unwrap_err();
    match zero {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("invalid number of fields")),
        other => panic!("expected protocol message, got {other:?}"),
    }
}

#[test]
fn hgetex_accepts_expiry_after_fields_and_rejects_negative_ttl() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("a"),
        SenkoValue::Raw(Bytes::from_static(b"v1")),
        None,
    );
    let _ = hash.set(
        CompactString::from("b"),
        SenkoValue::Raw(Bytes::from_static(b"v2")),
        None,
    );

    let res = advanced::hgetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"b"),
            bs(b"EX"),
            bs(b"10"),
        ],
    )
    .unwrap();
    match res {
        Response::Array(items) => {
            assert_eq!(bytes_of(items[0].clone()), Some(b"v1".to_vec()));
            assert_eq!(bytes_of(items[1].clone()), Some(b"v2".to_vec()));
        }
        other => panic!("expected array, got {other:?}"),
    }

    let err = advanced::hgetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"PX"),
            bs(b"-1"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap_err();
    match err {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("invalid expire time")),
        other => panic!("expected protocol message, got {other:?}"),
    }
}
