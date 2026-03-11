use compact_str::CompactString;
use senko_core::QuickList;
use senko_store::store::Store;

#[test]
fn push_front_pop_front_is_lifo() {
    let mut list = QuickList::default();
    for i in 0..1000 {
        list.push_front(i.to_string().as_bytes());
    }
    for expected in (0..1000).rev() {
        let value = list.pop_front().expect("value must exist");
        assert_eq!(value, expected.to_string().into_bytes());
    }
    assert!(list.is_empty());
}

#[test]
fn push_back_pop_front_is_fifo() {
    let mut list = QuickList::default();
    for i in 0..1000 {
        list.push_back(i.to_string().as_bytes());
    }
    for expected in 0..1000 {
        let value = list.pop_front().expect("value must exist");
        assert_eq!(value, expected.to_string().into_bytes());
    }
    assert!(list.is_empty());
}

#[test]
fn node_split_and_merge_at_fill_boundary() {
    let mut list = QuickList::new(4);
    for value in [b"a", b"b", b"c", b"d", b"e"] {
        list.push_back(value);
    }
    assert_eq!(list.node_count, 2);

    assert_eq!(list.pop_back(), Some(b"e".to_vec()));
    assert_eq!(list.node_count, 2);
    assert_eq!(list.pop_back(), Some(b"d".to_vec()));
    assert_eq!(list.pop_front(), Some(b"a".to_vec()));
    assert_eq!(list.node_count, 1);
    assert_eq!(list.len(), 2);
}

#[test]
fn negative_index_matches_tail_without_removing() {
    let mut list = QuickList::default();
    for value in [b"a", b"b", b"c"] {
        list.push_back(value);
    }

    let last = list.index(-1).expect("last element must exist").to_vec();
    let popped = list.pop_back().expect("tail value must exist");
    assert_eq!(last, popped);
    assert_eq!(list.len(), 2);
}

#[test]
fn trim_removes_all_elements_and_store_deletes_key() {
    let mut store = Store::default();
    {
        let list = store.get_or_create_list(CompactString::from("list"));
        for value in [b"a", b"b", b"c"] {
            list.push_back(value);
        }
        list.trim(10, 20);
    }
    store.remove_list_if_empty(b"list");
    assert!(store.get_list(b"list").is_none());
    assert!(!store.exists(b"list"));
}

#[test]
fn remove_negative_count_removes_last_two_occurrences() {
    let mut list = QuickList::default();
    for value in [b"x", b"a", b"x", b"b", b"x", b"c", b"x"] {
        list.push_back(value);
    }

    assert_eq!(list.remove(-2, b"x"), 2);
    let values: Vec<Vec<u8>> = list.iter().map(|value| value.to_vec()).collect();
    assert_eq!(
        values,
        vec![
            b"x".to_vec(),
            b"a".to_vec(),
            b"x".to_vec(),
            b"b".to_vec(),
            b"c".to_vec()
        ]
    );
}

#[test]
fn pos_honors_rank_count_and_maxlen() {
    let mut list = QuickList::default();
    for value in [b"a", b"b", b"a", b"c", b"a", b"d", b"a"] {
        list.push_back(value);
    }

    assert_eq!(list.pos(b"a", 1, 0, 0).as_slice(), &[0, 2, 4, 6]);
    assert_eq!(list.pos(b"a", 2, 0, 0).as_slice(), &[2, 4, 6]);
    assert_eq!(list.pos(b"a", -1, 2, 0).as_slice(), &[6, 4]);
    assert_eq!(list.pos(b"a", 1, 0, 3).as_slice(), &[0, 2]);
}

#[test]
fn quicklist_drop_runs_cleanly() {
    let mut list = QuickList::new(8);
    for i in 0..256 {
        list.push_back(i.to_string().as_bytes());
    }
    drop(list);
}
