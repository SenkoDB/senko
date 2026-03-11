use compact_str::CompactString;
use senko_proto::Frame;
use senko_store::{
    Response, Store,
    commands::list::blocking::{self, BlockingCommandResult, BlockingOp, BlockingResponseKind},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

#[test]
fn blpop_fast_path_returns_key_and_element() {
    let mut store = Store::default();
    store
        .get_or_create_list(CompactString::from("k"))
        .push_back(b"a");
    let result = blocking::blpop(&mut store, &[bs(b"k"), bs(b"1")]).unwrap();
    assert!(matches!(
        result,
        BlockingCommandResult::Immediate(Response::Array(_))
    ));
}

#[test]
fn brpop_fast_path_pops_from_tail() {
    let mut store = Store::default();
    let list = store.get_or_create_list(CompactString::from("k"));
    list.push_back(b"a");
    list.push_back(b"b");
    let result = blocking::brpop(&mut store, &[bs(b"k"), bs(b"1")]).unwrap();
    let BlockingCommandResult::Immediate(Response::Array(items)) = result else {
        panic!("expected immediate array");
    };
    assert!(matches!(&items[1], Response::Value(Some(value)) if value.as_bytes().as_ref() == b"b"));
}

#[test]
fn blmove_blocks_with_indefinite_timeout_when_source_empty() {
    let mut store = Store::default();
    let result = blocking::blmove(
        &mut store,
        &[bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT"), bs(b"0")],
    )
    .unwrap();
    let BlockingCommandResult::Block(spec) = result else {
        panic!("expected block spec");
    };
    assert!(spec.timeout.is_none());
    assert_eq!(spec.timeout_response, BlockingResponseKind::NullBulk);
}

#[test]
fn blmpop_builds_multikey_block_spec() {
    let mut store = Store::default();
    let result = blocking::blmpop(
        &mut store,
        &[
            bs(b"1.5"),
            bs(b"3"),
            bs(b"k1"),
            bs(b"k2"),
            bs(b"k3"),
            bs(b"LEFT"),
            bs(b"COUNT"),
            bs(b"2"),
        ],
    )
    .unwrap();
    let BlockingCommandResult::Block(spec) = result else {
        panic!("expected block spec");
    };
    assert_eq!(spec.keys.len(), 3);
    assert!(matches!(
        spec.op,
        BlockingOp::MPop {
            direction: _,
            count: 2
        }
    ));
}

#[test]
fn brpoplpush_fast_path_matches_move_right_left() {
    let mut store = Store::default();
    store
        .get_or_create_list(CompactString::from("src"))
        .push_back(b"x");
    let result = blocking::brpoplpush(&mut store, &[bs(b"src"), bs(b"dst"), bs(b"1")]).unwrap();
    assert!(matches!(
        result,
        BlockingCommandResult::Immediate(Response::Value(Some(_)))
    ));
    let dst = store.get_list(b"dst").expect("dst list");
    assert_eq!(dst.index(0), Some(&b"x"[..]));
}
