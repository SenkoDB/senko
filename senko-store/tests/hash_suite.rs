use bytes::Bytes;
use compact_str::CompactString;
use senko_core::{HashObject, SenkoValue};
use senko_store::store::{Store, current_unix_ms};

#[test]
fn listpack_upgrades_to_hashtable_after_threshold() {
    let mut hash = HashObject::default();
    assert!(hash.is_listpack());

    for i in 0..128u32 {
        let field = CompactString::from(format!("f{i}"));
        let value = SenkoValue::Raw(Bytes::from_static(b"v"));
        let inserted = hash.set(field, value, None);
        assert!(inserted);
        assert!(hash.is_listpack());
    }

    let inserted = hash.set(
        CompactString::from("f128"),
        SenkoValue::Raw(Bytes::from_static(b"v")),
        None,
    );
    assert!(inserted);
    assert!(!hash.is_listpack());
}

#[test]
fn per_field_expiry_expires_hash_field() {
    let mut store = Store::default();
    let now = current_unix_ms();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("field"),
        SenkoValue::Raw(Bytes::from_static(b"value")),
        Some(now + 50),
    );

    let live = store.get_hash(b"h").expect("hash must exist");
    assert!(live.exists(b"field", now + 10));

    let _ = store.advance_expiry_wheel(now + 100);
    assert!(store.get_hash(b"h").is_none());
}

#[test]
fn auto_delete_empty_hash_after_last_field_removed() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(CompactString::from("h"));
    let _ = hash.set(
        CompactString::from("field"),
        SenkoValue::Raw(Bytes::from_static(b"value")),
        None,
    );

    let deleted = store
        .get_hash_mut(b"h")
        .expect("hash must exist")
        .delete(b"field");
    assert!(deleted);
    assert!(store.get_hash(b"h").is_none());
    assert!(!store.exists(b"h"));
}

#[test]
fn simd_listpack_scan_finds_only_exact_field_entries() {
    let mut hash = HashObject::default();
    hash.listpack_set(b"foo", b"1");
    hash.listpack_set(b"bar", b"2");
    hash.listpack_set(b"z", b"prefixfoo");

    assert_eq!(hash.listpack_get(b"foo"), Some(&b"1"[..]));
    assert_eq!(hash.listpack_get(b"bar"), Some(&b"2"[..]));
    assert_eq!(hash.listpack_get(b"prefix"), None);
    assert_eq!(hash.listpack_get(b"oo"), None);
}
