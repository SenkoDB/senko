use compact_str::CompactString;

use senko_store::Store;

#[test]
fn get_or_create_set_round_trips_and_removes_empty_key() {
    let mut store = Store::default();

    let set = store.get_or_create_set(CompactString::from("s"));
    assert!(set.add(b"one"));
    assert!(store.get_set(b"s").is_some());

    let set = store.get_set_mut(b"s").unwrap();
    assert!(set.remove(b"one"));
    assert!(set.is_empty());

    store.remove_set_if_empty(b"s");
    assert!(store.get_set(b"s").is_none());
}

#[test]
fn set_accessors_preserve_encoding_state() {
    let mut store = Store::default();
    let set = store.get_or_create_set(CompactString::from("s"));

    for i in 0..129 {
        assert!(set.add(format!("v{i}").as_bytes()));
    }
    assert!(set.is_hashtable());

    let fetched = store.get_set(b"s").unwrap();
    assert!(fetched.is_hashtable());
    assert_eq!(fetched.len(), 129);
}
