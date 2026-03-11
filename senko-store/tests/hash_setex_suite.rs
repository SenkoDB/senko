use senko_core::SenkoError;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::hash::{basic as hbasic, expiry as hexp, setex},
    store::{Store, current_unix_ms},
};

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn int_of(response: Response) -> i64 {
    match response {
        Response::Integer(v) => v,
        other => panic!("expected integer response, got {other:?}"),
    }
}

fn int_array_of(response: Response) -> Vec<i64> {
    match response {
        Response::Array(values) => values
            .iter()
            .map(|item| match item {
                Response::Integer(v) => *v,
                other => panic!("expected integer item, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array response, got {other:?}"),
    }
}

#[test]
fn hsetex_fnx_and_fxx() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();

    let fnx = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FNX"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"x"),
            bs(b"b"),
            bs(b"2"),
        ],
    )
    .unwrap();
    assert_eq!(int_of(fnx), 1);

    let fxx = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FXX"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"3"),
            bs(b"c"),
            bs(b"9"),
        ],
    )
    .unwrap();
    assert_eq!(int_of(fxx), 1);
}

#[test]
fn hsetex_keepttl_preserves_existing_and_new_has_none() {
    let mut store = Store::default();
    let _ = hbasic::hset(&mut store, &[bs(b"h"), bs(b"a"), bs(b"1")]).unwrap();
    let _ = hexp::hpexpire(
        &mut store,
        &[bs(b"h"), bs(b"5000"), bs(b"FIELDS"), bs(b"1"), bs(b"a")],
    )
    .unwrap();
    let before = int_array_of(
        hexp::hpttl(&mut store, &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"a")]).unwrap(),
    )[0];

    let _ = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"KEEPTTL"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"2"),
            bs(b"b"),
            bs(b"3"),
        ],
    )
    .unwrap();
    let after = int_array_of(
        hexp::hpttl(
            &mut store,
            &[bs(b"h"), bs(b"FIELDS"), bs(b"2"), bs(b"a"), bs(b"b")],
        )
        .unwrap(),
    );
    assert!(after[0] <= before && after[0] > 0);
    assert_eq!(after[1], -1);
}

#[test]
fn hsetex_numfields_mismatch_error() {
    let mut store = Store::default();
    let err = setex::hsetex(
        &mut store,
        &[bs(b"h"), bs(b"FIELDS"), bs(b"2"), bs(b"a"), bs(b"1")],
    )
    .unwrap_err();
    match err {
        SenkoError::ProtocolMessage(msg) => {
            assert_eq!(
                msg.as_str(),
                "ERR numfields does not match the number of arguments"
            );
        }
        other => panic!("expected protocol message error, got {other:?}"),
    }
}

#[test]
fn hsetex_triggers_listpack_upgrade_mid_loop() {
    let mut store = Store::default();
    let hash = store.get_or_create_hash(compact_str::CompactString::from("h"));
    for i in 0..128u32 {
        let _ = hash.set(
            compact_str::CompactString::from(format!("f{i}")),
            senko_core::SenkoValue::Int(1),
            None,
        );
    }
    assert!(store.get_hash(b"h").unwrap().is_listpack());

    let written = setex::hsetex(
        &mut store,
        &[bs(b"h"), bs(b"FIELDS"), bs(b"1"), bs(b"f128"), bs(b"x")],
    )
    .unwrap();
    assert_eq!(int_of(written), 1);
    assert!(!store.get_hash(b"h").unwrap().is_listpack());
    let _ = current_unix_ms();
}

#[test]
fn hsetex_accepts_expiry_after_fields() {
    let mut store = Store::default();
    let written = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FIELDS"),
            bs(b"2"),
            bs(b"a"),
            bs(b"1"),
            bs(b"b"),
            bs(b"2"),
            bs(b"EX"),
            bs(b"60"),
        ],
    )
    .unwrap();
    assert_eq!(int_of(written), 2);

    let ttl = int_array_of(
        hexp::httl(
            &mut store,
            &[bs(b"h"), bs(b"FIELDS"), bs(b"2"), bs(b"a"), bs(b"b")],
        )
        .unwrap(),
    );
    assert!(ttl[0] > 0);
    assert!(ttl[1] > 0);
}

#[test]
fn hsetex_reports_conflicting_options_and_zero_fields() {
    let mut store = Store::default();

    let cond_err = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"FNX"),
            bs(b"FXX"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
            bs(b"1"),
        ],
    )
    .unwrap_err();
    match cond_err {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("Only one of FXX or FNX")),
        other => panic!("expected protocol message, got {other:?}"),
    }

    let expiry_err = setex::hsetex(
        &mut store,
        &[
            bs(b"h"),
            bs(b"EX"),
            bs(b"10"),
            bs(b"PX"),
            bs(b"10"),
            bs(b"FIELDS"),
            bs(b"1"),
            bs(b"a"),
            bs(b"1"),
        ],
    )
    .unwrap_err();
    match expiry_err {
        SenkoError::ProtocolMessage(msg) => {
            assert!(msg.contains("Only one of EX, PX, EXAT, PXAT or KEEPTTL"))
        }
        other => panic!("expected protocol message, got {other:?}"),
    }

    let zero_err = setex::hsetex(
        &mut store,
        &[bs(b"h"), bs(b"FIELDS"), bs(b"0"), bs(b"a"), bs(b"1")],
    )
    .unwrap_err();
    match zero_err {
        SenkoError::ProtocolMessage(msg) => assert!(msg.contains("invalid number of fields")),
        other => panic!("expected protocol message, got {other:?}"),
    }
}
