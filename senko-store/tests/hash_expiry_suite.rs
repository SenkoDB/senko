use senko_core::SenkoError;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::hash::{basic as hbasic, expiry as hexp},
    store::{Store, current_unix_ms},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn int_array_of(response: Response) -> Vec<i64> {
    match response {
        Response::Array(values) => values
            .iter()
            .map(|v| match v {
                Response::Integer(i) => *i,
                other => panic!("expected integer element, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

#[test]
fn hexpire_nx_and_xx_conditions() {
    let mut store = Store::default();
    let _ = hbasic::hset(
        &mut store,
        &[bs(b"h"), bs(b"a"), bs(b"1"), bs(b"b"), bs(b"2")],
    )
    .unwrap();

    let nx_first = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"10"),
            bs(b"NX"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"b"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(nx_first), vec![1, 1]);

    let nx_second = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"20"),
            bs(b"NX"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(nx_second), vec![0]);

    let _ = hbasic::hset(
        &mut store,
        &[bs(b"h2"), bs(b"a"), bs(b"1"), bs(b"b"), bs(b"2")],
    )
    .unwrap();
    let xx = hexp::hexpire(
        &mut store,
        &[
            bs(b"h2"),
            bs(b"10"),
            bs(b"XX"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"b"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(xx), vec![0, 0]);
    let _ = hexp::hexpire(
        &mut store,
        &[bs(b"h2"), bs(b"10"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    let xx_update = hexp::hexpire(
        &mut store,
        &[
            bs(b"h2"),
            bs(b"20"),
            bs(b"XX"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"b"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(xx_update), vec![1, 0]);
}

#[test]
fn hexpire_gt_and_lt_conditions() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();
    let _ = hexp::hexpire(
        &mut store,
        &[bs(b"h"), bs(b"20"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();

    let gt_ok = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"30"),
            bs(b"GT"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(gt_ok), vec![1]);
    let gt_skip = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"10"),
            bs(b"GT"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(gt_skip), vec![0]);

    let lt_ok = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"5"),
            bs(b"LT"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(lt_ok), vec![1]);
    let lt_skip = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"10"),
            bs(b"LT"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap();
    assert_eq!(int_array_of(lt_skip), vec![0]);
}

#[test]
fn httl_hpttl_precision_range() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();
    let _ = hexp::hexpire(
        &mut store,
        &[bs(b"h"), bs(b"5"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();

    let ttl_s = int_array_of(
        hexp::httl(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a")]).unwrap(),
    )[0];
    assert!((4..=5).contains(&ttl_s));

    let ttl_ms = int_array_of(
        hexp::hpttl(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a")]).unwrap(),
    )[0];
    assert!((4_000..=5_000).contains(&ttl_ms));
}

#[test]
fn hpersist_removes_ttl_and_handles_no_ttl_field() {
    let mut store = Store::default();
    let _ = hbasic::hset(
        &mut store,
        &[bs(b"h"), bs(b"a"), bs(b"1"), bs(b"b"), bs(b"2")],
    )
    .unwrap();
    let _ = hexp::hexpire(
        &mut store,
        &[bs(b"h"), bs(b"5"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();

    let persist = hexp::hpersist(
        &mut store,
        &[bs(b"h"), bs(b"FIELDS"), bs(b"2"), bs(b"a"), bs(b"b")],
    )
    .unwrap();
    assert_eq!(int_array_of(persist), vec![1, -1]);

    let ttl = int_array_of(
        hexp::httl(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a")]).unwrap(),
    );
    assert_eq!(ttl, vec![-1]);
}

#[test]
fn field_expiry_wheel_expires_fields_and_deletes_empty_hash() {
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
    let _ = hexp::hpexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"200"),
            bs(b"FIELDS"),
            bs(b"3"),
            bs(b"a"),
            bs(b"b"),
            bs(b"c"),
        ],
    )
    .unwrap();

    let now = current_unix_ms();
    let _ = store.advance_expiry_wheel(now + 300);
    assert!(store.get_hash(b"h").is_none());
}

#[test]
fn mixed_ttl_and_non_ttl_fields_only_expire_ttl_ones() {
    let mut store = Store::default();
    let _ = hbasic::hset(
        &mut store,
        &[bs(b"h"), bs(b"a"), bs(b"1"), bs(b"b"), bs(b"2")],
    )
    .unwrap();
    let _ = hexp::hpexpire(
        &mut store,
        &[bs(b"h"), bs(b"200"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    let now = current_unix_ms();
    let _ = store.advance_expiry_wheel(now + 300);

    let hash = store.get_hash(b"h").expect("hash should still exist");
    assert!(hash.get(b"a", now + 300).is_none());
    assert!(hash.get(b"b", now + 300).is_some());
}

#[test]
fn hgetex_on_expired_field_returns_missing_code_without_updating() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();
    let _ = hexp::hpexpire(
        &mut store,
        &[bs(b"h"), bs(b"200"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    let now = current_unix_ms();
    let _ = store.advance_expiry_wheel(now + 300);

    let res = hexp::hpexpire(
        &mut store,
        &[bs(b"h"), bs(b"500"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    assert_eq!(int_array_of(res), vec![2]);
}

#[test]
fn hexpire_reports_bad_ttl_position_and_duplicate_conditions() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();

    let ttl_err = hexp::hexpire(
        &mut store,
        &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a"), bs(b"60")],
    )
    .unwrap_err();
    match ttl_err {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("integer or out of range")),
        other => panic!("expected protocol message, got {other:?}"),
    }

    let cond_err = hexp::hexpire(
        &mut store,
        &[
            bs(b"h"),
            bs(b"60"),
            bs(b"NX"),
            bs(b"XX"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
        ],
    )
    .unwrap_err();
    match cond_err {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("Multiple condition flags")),
        other => panic!("expected protocol message, got {other:?}"),
    }
}
