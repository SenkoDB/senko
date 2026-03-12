use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    Response,
    commands::{arithmetic, basic, conditional, lcs, multi, strops},
    store::{SetCondition, SetExpiry, SetOptions, Store, current_unix_ms},
};
use std::{thread, time::Duration};

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

fn set_int(store: &mut Store, key: &str, value: i64) {
    let _ = store.set(
        CompactString::from(key),
        SenkoValue::Int(value),
        SetOptions::default(),
    );
}

fn bytes_of(response: Response) -> Option<Vec<u8>> {
    match response {
        Response::Value(Some(value)) => Some(value.as_bytes().into_owned()),
        Response::Value(None) => None,
        other => panic!("expected bulk-style response, got {other:?}"),
    }
}

fn int_of(response: Response) -> i64 {
    match response {
        Response::Integer(value) => value,
        other => panic!("expected integer response, got {other:?}"),
    }
}

fn simple_of(response: Response) -> &'static [u8] {
    match response {
        Response::Simple(value) => value,
        other => panic!("expected simple response, got {other:?}"),
    }
}

#[test]
fn setnx_target_key_missing() {
    let mut store = Store::default();
    assert_eq!(
        int_of(basic::setnx(&mut store, &[bs(b"novar"), bs(b"foobared")]).unwrap()),
        1
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"novar")]).unwrap()),
        Some(b"foobared".to_vec())
    );
}

#[test]
fn setnx_target_key_exists_and_expiry_paths() {
    let mut store = Store::default();
    set_raw(&mut store, "novar", b"foobared");
    assert_eq!(
        int_of(basic::setnx(&mut store, &[bs(b"novar"), bs(b"blabla")]).unwrap()),
        0
    );

    let now = current_unix_ms();
    let _ = store.set(
        CompactString::from("volatile"),
        SenkoValue::Raw(Bytes::from_static(b"x")),
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::PxAt(now + 10_000),
            get_old: false,
        },
    );
    assert_eq!(
        int_of(basic::setnx(&mut store, &[bs(b"volatile"), bs(b"y")]).unwrap()),
        0
    );

    let _ = store.set(
        CompactString::from("expired"),
        SenkoValue::Raw(Bytes::from_static(b"old")),
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::PxAt(now),
            get_old: false,
        },
    );
    store.advance_expiry_wheel(now + 300);
    assert_eq!(
        int_of(basic::setnx(&mut store, &[bs(b"expired"), bs(b"new")]).unwrap()),
        1
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"expired")]).unwrap()),
        Some(b"new".to_vec())
    );
}

#[test]
fn getex_variants_and_missing_cases() {
    let mut store = Store::default();
    let now = current_unix_ms();

    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::getex(&mut store, &[bs(b"foo"), bs(b"EX"), bs(b"10")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert!(store.ttl_ms(b"foo").unwrap() > 0);

    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::getex(&mut store, &[bs(b"foo"), bs(b"PX"), bs(b"1000")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert!(store.ttl_ms(b"foo").unwrap() > 0);

    set_raw(&mut store, "foo", b"bar");
    let exat = ((now / 1000) + 10).to_string();
    assert_eq!(
        bytes_of(
            basic::getex(&mut store, &[bs(b"foo"), bs(b"EXAT"), bs(exat.as_bytes())]).unwrap()
        ),
        Some(b"bar".to_vec())
    );
    assert!(store.ttl_ms(b"foo").unwrap() > 0);

    set_raw(&mut store, "foo", b"bar");
    let pxat = (now + 10_000).to_string();
    assert_eq!(
        bytes_of(
            basic::getex(&mut store, &[bs(b"foo"), bs(b"PXAT"), bs(pxat.as_bytes())]).unwrap()
        ),
        Some(b"bar".to_vec())
    );
    assert!(store.ttl_ms(b"foo").unwrap() > 0);

    let _ = store.set(
        CompactString::from("foo"),
        SenkoValue::Raw(Bytes::from_static(b"bar")),
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::Ex(10),
            get_old: false,
        },
    );
    assert_eq!(
        bytes_of(basic::getex(&mut store, &[bs(b"foo"), bs(b"PERSIST")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(store.ttl_ms(b"foo"), Some(-1));

    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::getex(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"bar".to_vec())
    );

    assert!(basic::getex(&mut store, &[bs(b"foo"), bs(b"non-existent-option")]).is_err());
    assert!(basic::getex(&mut store, &[]).is_err());

    let _ = store.set(
        CompactString::from("expired-getex"),
        SenkoValue::Raw(Bytes::from_static(b"bar")),
        SetOptions {
            condition: SetCondition::Always,
            expiry: SetExpiry::Px(1),
            get_old: false,
        },
    );
    thread::sleep(Duration::from_millis(3));
    assert_eq!(
        basic::getex(&mut store, &[bs(b"expired-getex")]).unwrap(),
        Response::Value(None)
    );
    assert_eq!(
        basic::get(&mut store, &[bs(b"expired-getex")]).unwrap(),
        Response::Value(None)
    );
}

#[test]
fn getdel_command() {
    let mut store = Store::default();
    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::getdel(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(
        basic::get(&mut store, &[bs(b"foo")]).unwrap(),
        Response::Value(None)
    );
}

#[test]
fn mget_and_getset_cases() {
    let mut store = Store::default();
    set_raw(&mut store, "foo", b"BAR");
    set_raw(&mut store, "bar", b"FOO");

    let mget = multi::mget(&mut store, &[bs(b"foo"), bs(b"baazz"), bs(b"bar")]).unwrap();
    assert_eq!(
        mget,
        Response::Array(Box::new(smallvec::smallvec![
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"BAR")))),
            Response::Value(None),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"FOO")))),
        ]))
    );

    assert_eq!(
        bytes_of(basic::getset(&mut store, &[bs(b"new"), bs(b"xyz")]).unwrap()),
        None
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"new")]).unwrap()),
        Some(b"xyz".to_vec())
    );

    assert_eq!(
        bytes_of(basic::getset(&mut store, &[bs(b"foo"), bs(b"xyz")]).unwrap()),
        Some(b"BAR".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"xyz".to_vec())
    );
}

#[test]
fn mset_and_msetnx_cases() {
    let mut store = Store::default();
    assert_eq!(
        simple_of(
            multi::mset(
                &mut store,
                &[
                    bs(b"x"),
                    bs(b"10"),
                    bs(b"y"),
                    bs(b"foo bar"),
                    bs(b"z"),
                    bs(b"x x x x x x x\n\n\r\n")
                ]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"x")]).unwrap()),
        Some(b"10".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"y")]).unwrap()),
        Some(b"foo bar".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"z")]).unwrap()),
        Some(b"x x x x x x x\n\n\r\n".to_vec())
    );

    assert!(multi::mset(&mut store, &[bs(b"x"), bs(b"10"), bs(b"y")]).is_err());
    assert!(multi::msetnx(&mut store, &[bs(b"x"), bs(b"10"), bs(b"y")]).is_err());

    set_raw(&mut store, "dup", b"x");
    assert_eq!(
        simple_of(
            multi::mset(
                &mut store,
                &[bs(b"dup"), bs(b"xxx"), bs(b"dup"), bs(b"yyy")]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"dup")]).unwrap()),
        Some(b"yyy".to_vec())
    );

    assert_eq!(
        int_of(multi::msetnx(&mut store, &[bs(b"x1"), bs(b"xxx"), bs(b"y2"), bs(b"yyy")]).unwrap()),
        1
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"x1")]).unwrap()),
        Some(b"xxx".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"y2")]).unwrap()),
        Some(b"yyy".to_vec())
    );

    assert_eq!(
        int_of(multi::msetnx(&mut store, &[bs(b"x1"), bs(b"qqq"), bs(b"y3"), bs(b"www")]).unwrap()),
        0
    );
    assert_eq!(
        basic::get(&mut store, &[bs(b"y3")]).unwrap(),
        Response::Value(None)
    );

    let mut fresh = Store::default();
    assert_eq!(
        int_of(
            multi::msetnx(
                &mut fresh,
                &[bs(b"same"), bs(b"xxx"), bs(b"same"), bs(b"yyy")]
            )
            .unwrap()
        ),
        1
    );
    assert_eq!(
        bytes_of(basic::get(&mut fresh, &[bs(b"same")]).unwrap()),
        Some(b"yyy".to_vec())
    );

    assert_eq!(
        int_of(
            multi::msetnx(
                &mut fresh,
                &[bs(b"same"), bs(b"zzz"), bs(b"same"), bs(b"ttt")]
            )
            .unwrap()
        ),
        0
    );
    assert_eq!(
        bytes_of(basic::get(&mut fresh, &[bs(b"same")]).unwrap()),
        Some(b"yyy".to_vec())
    );
}

#[test]
fn msetex_cases_from_redis_suite() {
    let mut store = Store::default();
    let future_sec = (current_unix_ms() / 1000 + 10).to_string();
    let future_ms = (current_unix_ms() + 10_000).to_string();

    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"ex:key1"),
                    bs(b"val1"),
                    bs(b"ex:key2"),
                    bs(b"val2"),
                    bs(b"EX"),
                    bs(b"5")
                ]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"px:key1"),
                    bs(b"val1"),
                    bs(b"px:key2"),
                    bs(b"val2"),
                    bs(b"PX"),
                    bs(b"5000")
                ]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"exat:key1"),
                    bs(b"val3"),
                    bs(b"exat:key2"),
                    bs(b"val4"),
                    bs(b"EXAT"),
                    bs(future_sec.as_bytes())
                ]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"pxat:key1"),
                    bs(b"val3"),
                    bs(b"pxat:key2"),
                    bs(b"val4"),
                    bs(b"PXAT"),
                    bs(future_ms.as_bytes())
                ]
            )
            .unwrap()
        ),
        2
    );
    assert!(store.ttl_ms(b"ex:key1").unwrap() > 0);
    assert!(store.ttl_ms(b"px:key1").unwrap() > 0);
    assert!(store.ttl_ms(b"exat:key1").unwrap() > 0);
    assert!(store.ttl_ms(b"pxat:key1").unwrap() > 0);

    assert_eq!(
        simple_of(
            basic::setex(&mut store, &[bs(b"keepttl:key"), bs(b"100"), bs(b"oldval")]).unwrap()
        ),
        b"OK"
    );
    let old_ttl = store.ttl_ms(b"keepttl:key").unwrap();
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[bs(b"1"), bs(b"keepttl:key"), bs(b"newval"), bs(b"KEEPTTL")]
            )
            .unwrap()
        ),
        1
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"keepttl:key")]).unwrap()),
        Some(b"newval".to_vec())
    );
    assert!(
        store.ttl_ms(b"keepttl:key").unwrap() <= old_ttl
            && store.ttl_ms(b"keepttl:key").unwrap() > 0
    );

    set_raw(&mut store, "xx:existing", b"oldval");
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"nx:new"),
                    bs(b"val1"),
                    bs(b"nx:new2"),
                    bs(b"val2"),
                    bs(b"NX"),
                    bs(b"EX"),
                    bs(b"10")
                ]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"1"),
                    bs(b"xx:existing"),
                    bs(b"newval"),
                    bs(b"NX"),
                    bs(b"EX"),
                    bs(b"10")
                ]
            )
            .unwrap()
        ),
        0
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"1"),
                    bs(b"xx:nonexist"),
                    bs(b"newval"),
                    bs(b"XX"),
                    bs(b"EX"),
                    bs(b"10")
                ]
            )
            .unwrap()
        ),
        0
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"1"),
                    bs(b"xx:existing"),
                    bs(b"newval"),
                    bs(b"XX"),
                    bs(b"EX"),
                    bs(b"10")
                ]
            )
            .unwrap()
        ),
        1
    );

    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"flex:1"),
                    bs(b"val1"),
                    bs(b"flex:2"),
                    bs(b"val2"),
                    bs(b"EX"),
                    bs(b"3"),
                    bs(b"NX")
                ]
            )
            .unwrap()
        ),
        2
    );
    assert_eq!(
        int_of(
            multi::msetex(
                &mut store,
                &[
                    bs(b"2"),
                    bs(b"flex:3"),
                    bs(b"val3"),
                    bs(b"flex:4"),
                    bs(b"val4"),
                    bs(b"PX"),
                    bs(b"3000"),
                    bs(b"XX")
                ]
            )
            .unwrap()
        ),
        0
    );

    assert!(multi::msetex(&mut store, &[]).is_err());
    assert!(
        multi::msetex(
            &mut store,
            &[bs(b"key1"), bs(b"val1"), bs(b"EX"), bs(b"10")]
        )
        .is_err()
    );
    assert!(
        multi::msetex(
            &mut store,
            &[bs(b"2"), bs(b"key1"), bs(b"val1"), bs(b"key2")]
        )
        .is_err()
    );
    assert!(
        multi::msetex(
            &mut store,
            &[bs(b"1"), bs(b"key1"), bs(b"val1"), bs(b"invalid_flag")]
        )
        .is_err()
    );
    assert!(
        multi::msetex(
            &mut store,
            &[
                bs(b"2"),
                bs(b"key1"),
                bs(b"val1"),
                bs(b"key2"),
                bs(b"val2"),
                bs(b"NX"),
                bs(b"XX"),
                bs(b"EX"),
                bs(b"10")
            ]
        )
        .is_err()
    );
    assert!(
        multi::msetex(
            &mut store,
            &[
                bs(b"2"),
                bs(b"key1"),
                bs(b"val1"),
                bs(b"key2"),
                bs(b"val2"),
                bs(b"EX"),
                bs(b"10"),
                bs(b"PX"),
                bs(b"5000")
            ]
        )
        .is_err()
    );
    assert!(
        multi::msetex(
            &mut store,
            &[
                bs(b"2"),
                bs(b"key1"),
                bs(b"val1"),
                bs(b"key2"),
                bs(b"val2"),
                bs(b"KEEPTTL"),
                bs(b"EX"),
                bs(b"10")
            ]
        )
        .is_err()
    );
}

#[test]
fn strlen_cases() {
    let mut store = Store::default();
    assert_eq!(
        int_of(strops::strlen(&mut store, &[bs(b"notakey")]).unwrap()),
        0
    );
    set_int(&mut store, "myinteger", -555);
    assert_eq!(
        int_of(strops::strlen(&mut store, &[bs(b"myinteger")]).unwrap()),
        4
    );
    set_raw(&mut store, "mystring", b"foozzz0123456789 baz");
    assert_eq!(
        int_of(strops::strlen(&mut store, &[bs(b"mystring")]).unwrap()),
        20
    );
}

#[test]
fn setrange_cases_from_redis_suite() {
    let mut store = Store::default();

    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"foo")]).unwrap()),
        3
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"foo".to_vec())
    );

    let mut store = Store::default();
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"")]).unwrap()),
        0
    );
    assert_eq!(
        basic::get(&mut store, &[bs(b"mykey")]).unwrap(),
        Response::Value(None)
    );

    let mut store = Store::default();
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"1"), bs(b"foo")]).unwrap()),
        4
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"\0foo".to_vec())
    );

    set_raw(&mut store, "mykey", b"foo");
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"b")]).unwrap()),
        3
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"boo".to_vec())
    );

    set_raw(&mut store, "mykey", b"foo");
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"")]).unwrap()),
        3
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"foo".to_vec())
    );

    set_raw(&mut store, "mykey", b"foo");
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"1"), bs(b"b")]).unwrap()),
        3
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"fbo".to_vec())
    );

    set_raw(&mut store, "mykey", b"foo");
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"4"), bs(b"bar")]).unwrap()),
        7
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"foo\0bar".to_vec())
    );

    set_int(&mut store, "myint", 1234);
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"myint"), bs(b"0"), bs(b"2")]).unwrap()),
        4
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"myint")]).unwrap()),
        Some(b"2234".to_vec())
    );

    set_int(&mut store, "myint", 1234);
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"myint"), bs(b"0"), bs(b"")]).unwrap()),
        4
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"myint")]).unwrap()),
        Some(b"1234".to_vec())
    );

    set_int(&mut store, "myint", 1234);
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"myint"), bs(b"1"), bs(b"3")]).unwrap()),
        4
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"myint")]).unwrap()),
        Some(b"1334".to_vec())
    );

    set_int(&mut store, "myint", 1234);
    assert_eq!(
        int_of(strops::setrange(&mut store, &[bs(b"myint"), bs(b"5"), bs(b"2")]).unwrap()),
        6
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"myint")]).unwrap()),
        Some(vec![b'1', b'2', b'3', b'4', 0, b'2'])
    );

    assert!(strops::setrange(&mut store, &[bs(b"mykey"), bs(b"-1"), bs(b"world")]).is_err());
    let huge = (512usize * 1024 * 1024 - 4).to_string();
    assert!(
        strops::setrange(
            &mut store,
            &[bs(b"mykey"), bs(huge.as_bytes()), bs(b"world")]
        )
        .is_err()
    );
}

#[test]
fn getrange_and_substr_cases_from_redis_suite() {
    let mut store = Store::default();
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"-1")]).unwrap()),
        Some(Vec::new())
    );

    set_raw(&mut store, "mykey", b"Hello World");
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"3")]).unwrap()),
        Some(b"Hell".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"-1")]).unwrap()),
        Some(b"Hello World".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-4"), bs(b"-1")]).unwrap()),
        Some(b"orld".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"5"), bs(b"3")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"5"), bs(b"5000")]).unwrap()),
        Some(b" World".to_vec())
    );
    assert_eq!(
        bytes_of(
            strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-5000"), bs(b"10000")]).unwrap()
        ),
        Some(b"Hello World".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"0"), bs(b"-100")]).unwrap()),
        Some(b"H".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"1"), bs(b"-100")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-1"), bs(b"-100")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-100"), bs(b"-99")]).unwrap()),
        Some(b"H".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-100"), bs(b"-100")]).unwrap()),
        Some(b"H".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"mykey"), bs(b"-100"), bs(b"-101")]).unwrap()),
        Some(Vec::new())
    );

    set_int(&mut store, "myint", 1234);
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"0"), bs(b"2")]).unwrap()),
        Some(b"123".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"0"), bs(b"-1")]).unwrap()),
        Some(b"1234".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"-3"), bs(b"-1")]).unwrap()),
        Some(b"234".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"5"), bs(b"3")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"3"), bs(b"5000")]).unwrap()),
        Some(b"4".to_vec())
    );
    assert_eq!(
        bytes_of(
            strops::getrange(&mut store, &[bs(b"myint"), bs(b"-5000"), bs(b"10000")]).unwrap()
        ),
        Some(b"1234".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"0"), bs(b"-100")]).unwrap()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"1"), bs(b"-100")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"-1"), bs(b"-100")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"-100"), bs(b"-99")]).unwrap()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"-100"), bs(b"-100")]).unwrap()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        bytes_of(strops::getrange(&mut store, &[bs(b"myint"), bs(b"-100"), bs(b"-101")]).unwrap()),
        Some(Vec::new())
    );

    set_raw(&mut store, "key", b"abcde");
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"key"), bs(b"0"), bs(b"0")]).unwrap()),
        Some(b"a".to_vec())
    );
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"key"), bs(b"0"), bs(b"3")]).unwrap()),
        Some(b"abcd".to_vec())
    );
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"key"), bs(b"-4"), bs(b"-1")]).unwrap()),
        Some(b"bcde".to_vec())
    );
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"key"), bs(b"-1"), bs(b"-3")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"key"), bs(b"7"), bs(b"8")]).unwrap()),
        Some(Vec::new())
    );
    assert_eq!(
        bytes_of(strops::substr(&mut store, &[bs(b"nokey"), bs(b"0"), bs(b"1")]).unwrap()),
        Some(Vec::new())
    );
}

#[test]
fn append_arithmetic_digest_delex_and_lcs_smoke() {
    let mut store = Store::default();
    set_int(&mut store, "foo", 1);
    assert_eq!(
        int_of(strops::append(&mut store, &[bs(b"foo"), bs(b"2")]).unwrap()),
        2
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"12".to_vec())
    );

    assert_eq!(
        int_of(arithmetic::incr(&mut store, &[bs(b"counter")]).unwrap()),
        1
    );
    assert_eq!(
        int_of(arithmetic::incrby(&mut store, &[bs(b"counter"), bs(b"4")]).unwrap()),
        5
    );
    assert_eq!(
        int_of(arithmetic::decr(&mut store, &[bs(b"counter")]).unwrap()),
        4
    );
    assert_eq!(
        int_of(arithmetic::decrby(&mut store, &[bs(b"counter"), bs(b"2")]).unwrap()),
        2
    );
    assert_eq!(
        bytes_of(arithmetic::incrbyfloat(&mut store, &[bs(b"float"), bs(b"1.5")]).unwrap()),
        Some(b"1.5".to_vec())
    );

    set_raw(&mut store, "digest-key", b"hello");
    let digest = bytes_of(conditional::digest(&mut store, &[bs(b"digest-key")]).unwrap()).unwrap();
    assert_eq!(digest.len(), 16);
    assert_eq!(
        int_of(
            conditional::delex(&mut store, &[bs(b"digest-key"), bs(b"IFDEQ"), bs(&digest)])
                .unwrap()
        ),
        1
    );
    assert_eq!(
        basic::get(&mut store, &[bs(b"digest-key")]).unwrap(),
        Response::Value(None)
    );

    set_raw(&mut store, "lcs:a", b"ohmytext");
    set_raw(&mut store, "lcs:b", b"mynewtext");
    assert_eq!(
        int_of(lcs::lcs(&mut store, &[bs(b"lcs:a"), bs(b"lcs:b"), bs(b"LEN")]).unwrap()),
        6
    );
}

#[test]
fn set_and_get_payload_cases_from_redis_suite() {
    let mut store = Store::default();
    assert_eq!(
        simple_of(basic::set(&mut store, &[bs(b"x"), bs(b"foobar")]).unwrap()),
        b"OK"
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"x")]).unwrap()),
        Some(b"foobar".to_vec())
    );

    assert_eq!(
        simple_of(basic::set(&mut store, &[bs(b"empty"), bs(b"")]).unwrap()),
        b"OK"
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"empty")]).unwrap()),
        Some(Vec::new())
    );

    let big = b"abcd".repeat(250_000);
    assert_eq!(
        simple_of(basic::set(&mut store, &[bs(b"big"), bs(&big)]).unwrap()),
        b"OK"
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"big")]).unwrap()),
        Some(big)
    );
}

#[test]
fn extended_set_option_matrix_from_redis_suite() {
    let mut store = Store::default();

    assert!(
        basic::set(
            &mut store,
            &[bs(b"foo"), bs(b"bar"), bs(b"non-existing-option")]
        )
        .is_err()
    );

    assert_eq!(
        basic::set(&mut store, &[bs(b"foo"), bs(b"1"), bs(b"NX")]).unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        basic::set(&mut store, &[bs(b"foo"), bs(b"2"), bs(b"NX")]).unwrap(),
        Response::Value(None)
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"1".to_vec())
    );

    let mut store = Store::default();
    assert_eq!(
        basic::set(&mut store, &[bs(b"foo"), bs(b"1"), bs(b"XX")]).unwrap(),
        Response::Value(None)
    );
    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        basic::set(&mut store, &[bs(b"foo"), bs(b"2"), bs(b"XX")]).unwrap(),
        Response::Simple(b"OK")
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"2".to_vec())
    );

    let mut store = Store::default();
    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::set(&mut store, &[bs(b"foo"), bs(b"bar2"), bs(b"GET")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"bar2".to_vec())
    );
    assert_eq!(
        bytes_of(basic::set(&mut store, &[bs(b"new"), bs(b"bar"), bs(b"GET")]).unwrap()),
        None
    );

    let mut store = Store::default();
    set_raw(&mut store, "foo", b"bar");
    assert_eq!(
        bytes_of(basic::set(&mut store, &[bs(b"foo"), bs(b"baz"), bs(b"GET"), bs(b"XX")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(
        bytes_of(
            basic::set(
                &mut store,
                &[bs(b"missing"), bs(b"bar"), bs(b"GET"), bs(b"XX")]
            )
            .unwrap()
        ),
        None
    );

    let mut store = Store::default();
    assert_eq!(
        bytes_of(basic::set(&mut store, &[bs(b"foo"), bs(b"bar"), bs(b"GET"), bs(b"NX")]).unwrap()),
        None
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(
        bytes_of(basic::set(&mut store, &[bs(b"foo"), bs(b"baz"), bs(b"GET"), bs(b"NX")]).unwrap()),
        Some(b"bar".to_vec())
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"foo")]).unwrap()),
        Some(b"bar".to_vec())
    );

    let mut store = Store::default();
    assert_eq!(
        simple_of(basic::set(&mut store, &[bs(b"foo"), bs(b"bar"), bs(b"EX"), bs(b"10")]).unwrap()),
        b"OK"
    );
    assert!(store.ttl_ms(b"foo").unwrap() > 0);
    assert_eq!(
        simple_of(
            basic::set(
                &mut store,
                &[bs(b"bar"), bs(b"baz"), bs(b"PX"), bs(b"10000")]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert!(store.ttl_ms(b"bar").unwrap() > 0);
    let now = current_unix_ms();
    let exat = ((now / 1000) + 10).to_string();
    let pxat = (now + 10_000).to_string();
    assert_eq!(
        simple_of(
            basic::set(
                &mut store,
                &[bs(b"baz"), bs(b"qux"), bs(b"EXAT"), bs(exat.as_bytes())]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert_eq!(
        simple_of(
            basic::set(
                &mut store,
                &[bs(b"quux"), bs(b"zap"), bs(b"PXAT"), bs(pxat.as_bytes())]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert!(store.ttl_ms(b"baz").unwrap() > 0);
    assert!(store.ttl_ms(b"quux").unwrap() > 0);

    let _ = basic::setex(&mut store, &[bs(b"keep"), bs(b"100"), bs(b"old")]).unwrap();
    let before = store.ttl_ms(b"keep").unwrap();
    assert_eq!(
        simple_of(basic::set(&mut store, &[bs(b"keep"), bs(b"new"), bs(b"KEEPTTL")]).unwrap()),
        b"OK"
    );
    let after = store.ttl_ms(b"keep").unwrap();
    assert!(after > 0 && after <= before);

    set_raw(&mut store, "multi", b"val");
    assert_eq!(
        simple_of(
            basic::set(
                &mut store,
                &[bs(b"multi"), bs(b"bar"), bs(b"XX"), bs(b"PX"), bs(b"10000")]
            )
            .unwrap()
        ),
        b"OK"
    );
    assert!(store.ttl_ms(b"multi").unwrap() > 0);
}

#[test]
fn extended_set_condition_and_digest_cases_from_redis_suite() {
    let mut store = Store::default();

    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFEQ"), bs(b"hello")]
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFEQ"), bs(b"different")]
        )
        .unwrap(),
        Response::Value(None)
    );

    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFNE"), bs(b"different")]
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFNE"), bs(b"hello")]
        )
        .unwrap(),
        Response::Value(None)
    );

    let digest = bytes_of(conditional::digest(&mut store, &[bs(b"mykey")]).unwrap()).unwrap();
    assert_eq!(digest.len(), 16);
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFDEQ"), bs(&digest)]
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    set_raw(&mut store, "mykey", b"hello");
    let wrong_digest = b"0000000000000000".to_vec();
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFDEQ"), bs(&wrong_digest)]
        )
        .unwrap(),
        Response::Value(None)
    );
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFDNE"), bs(&wrong_digest)]
        )
        .unwrap(),
        Response::Simple(b"OK")
    );
    set_raw(&mut store, "mykey", b"hello");
    let upper_digest = digest
        .iter()
        .map(u8::to_ascii_uppercase)
        .collect::<Vec<_>>();
    assert_eq!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"world"), bs(b"IFDEQ"), bs(&upper_digest)]
        )
        .unwrap(),
        Response::Simple(b"OK")
    );

    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        bytes_of(
            basic::set(
                &mut store,
                &[
                    bs(b"mykey"),
                    bs(b"world"),
                    bs(b"IFEQ"),
                    bs(b"hello"),
                    bs(b"GET")
                ]
            )
            .unwrap()
        ),
        Some(b"hello".to_vec())
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        bytes_of(
            basic::set(
                &mut store,
                &[
                    bs(b"mykey"),
                    bs(b"world"),
                    bs(b"IFNE"),
                    bs(b"hello"),
                    bs(b"GET")
                ]
            )
            .unwrap()
        ),
        Some(b"hello".to_vec())
    );
    set_raw(&mut store, "mykey", b"hello");
    let digest = bytes_of(conditional::digest(&mut store, &[bs(b"mykey")]).unwrap()).unwrap();
    assert_eq!(
        bytes_of(
            basic::set(
                &mut store,
                &[
                    bs(b"mykey"),
                    bs(b"world"),
                    bs(b"IFDEQ"),
                    bs(&digest),
                    bs(b"GET")
                ]
            )
            .unwrap()
        ),
        Some(b"hello".to_vec())
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        bytes_of(
            basic::set(
                &mut store,
                &[
                    bs(b"mykey"),
                    bs(b"world"),
                    bs(b"IFDNE"),
                    bs(&digest),
                    bs(b"GET")
                ]
            )
            .unwrap()
        ),
        Some(b"hello".to_vec())
    );

    assert!(
        basic::set(
            &mut store,
            &[bs(b"mykey"), bs(b"new"), bs(b"IFDEQ"), bs(b"short")]
        )
        .is_err()
    );
    assert!(
        basic::set(
            &mut store,
            &[
                bs(b"mykey"),
                bs(b"new"),
                bs(b"IFDNE"),
                bs(b"too-long-digest-123")
            ]
        )
        .is_err()
    );
}

#[test]
fn digest_and_delex_cases_from_redis_suite() {
    let mut store = Store::default();

    set_raw(&mut store, "plain", b"hello world");
    let digest = bytes_of(conditional::digest(&mut store, &[bs(b"plain")]).unwrap()).unwrap();
    assert_eq!(digest.len(), 16);
    assert!(digest.iter().all(u8::is_ascii_hexdigit));

    set_raw(&mut store, "empty", b"");
    assert_eq!(
        bytes_of(conditional::digest(&mut store, &[bs(b"empty")]).unwrap())
            .unwrap()
            .len(),
        16
    );
    set_int(&mut store, "int", 12345);
    assert_eq!(
        bytes_of(conditional::digest(&mut store, &[bs(b"int")]).unwrap())
            .unwrap()
            .len(),
        16
    );
    set_raw(&mut store, "bin", b"\0\x01\x02\x03\xff\xfe");
    assert_eq!(
        bytes_of(conditional::digest(&mut store, &[bs(b"bin")]).unwrap())
            .unwrap()
            .len(),
        16
    );
    set_raw(&mut store, "unicode", "Hello 世界".as_bytes());
    assert_eq!(
        bytes_of(conditional::digest(&mut store, &[bs(b"unicode")]).unwrap())
            .unwrap()
            .len(),
        16
    );

    set_raw(&mut store, "same1", b"identical");
    set_raw(&mut store, "same2", b"identical");
    let d1 = bytes_of(conditional::digest(&mut store, &[bs(b"same1")]).unwrap()).unwrap();
    let d2 = bytes_of(conditional::digest(&mut store, &[bs(b"same2")]).unwrap()).unwrap();
    assert_eq!(d1, d2);

    set_raw(&mut store, "diff1", b"value1");
    set_raw(&mut store, "diff2", b"value2");
    let d1 = bytes_of(conditional::digest(&mut store, &[bs(b"diff1")]).unwrap()).unwrap();
    let d2 = bytes_of(conditional::digest(&mut store, &[bs(b"diff2")]).unwrap()).unwrap();
    assert_ne!(d1, d2);

    set_raw(
        &mut store,
        "leading-zero",
        b"v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd",
    );
    assert_eq!(
        bytes_of(conditional::digest(&mut store, &[bs(b"leading-zero")]).unwrap()),
        Some(b"00006c38adf31777".to_vec())
    );

    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        int_of(conditional::delex(&mut store, &[bs(b"mykey")]).unwrap()),
        1
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        int_of(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFEQ"), bs(b"hello")]).unwrap()),
        1
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        int_of(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFEQ"), bs(b"world")]).unwrap()),
        0
    );
    assert_eq!(
        bytes_of(basic::get(&mut store, &[bs(b"mykey")]).unwrap()),
        Some(b"hello".to_vec())
    );

    assert_eq!(
        int_of(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFNE"), bs(b"world")]).unwrap()),
        1
    );
    set_raw(&mut store, "mykey", b"hello");
    let digest = bytes_of(conditional::digest(&mut store, &[bs(b"mykey")]).unwrap()).unwrap();
    assert_eq!(
        int_of(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFDEQ"), bs(&digest)]).unwrap()),
        1
    );
    set_raw(&mut store, "mykey", b"hello");
    assert_eq!(
        int_of(
            conditional::delex(
                &mut store,
                &[bs(b"mykey"), bs(b"IFDNE"), bs(b"0000000000000000")]
            )
            .unwrap()
        ),
        1
    );

    set_raw(&mut store, "mykey", b"hello");
    let upper_digest = bytes_of(conditional::digest(&mut store, &[bs(b"mykey")]).unwrap())
        .unwrap()
        .iter()
        .map(u8::to_ascii_uppercase)
        .collect::<Vec<_>>();
    assert_eq!(
        int_of(
            conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFDEQ"), bs(&upper_digest)])
                .unwrap()
        ),
        1
    );

    assert_eq!(
        basic::get(&mut store, &[bs(b"missing")]).unwrap(),
        Response::Value(None)
    );
    assert_eq!(
        int_of(
            conditional::delex(&mut store, &[bs(b"missing"), bs(b"IFEQ"), bs(b"hello")]).unwrap()
        ),
        0
    );
    assert!(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"INVALID"), bs(b"hello")]).is_err());
    assert!(conditional::delex(&mut store, &[bs(b"mykey"), bs(b"IFDEQ"), bs(b"short")]).is_err());
}
