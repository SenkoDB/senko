#![allow(clippy::too_many_lines)]

use std::collections::HashSet;

use redis::{Connection, RedisResult, Value};

fn connect() -> Option<Connection> {
    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let client = redis::Client::open(url).ok()?;
    client.get_connection().ok()
}

fn must_connect() -> Connection {
    match connect() {
        Some(mut conn) => {
            let _: RedisResult<String> = redis::cmd("PING").query(&mut conn);
            conn
        }
        None => panic!("compat test requires running Senko at SENKO_REDIS_URL"),
    }
}

fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    if redis::cmd("FLUSHALL").query::<()>(conn).is_ok() {
        return;
    }
    panic!("compat test requires FLUSHDB or FLUSHALL support");
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

fn as_array(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_i64(value: Value) -> i64 {
    match value {
        Value::Int(value) => value,
        other => panic!("expected integer, got {other:?}"),
    }
}

fn raw(conn: &mut Connection, key: &str) -> Vec<u8> {
    redis::cmd("GET").arg(key).query(conn).unwrap()
}

fn simple_rng(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_basic_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let created: i64 = redis::cmd("PFADD").arg("hll").query(&mut conn).unwrap();
    assert_eq!(created, 1);
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("hll")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    assert_eq!(
        redis::cmd("PFADD")
            .arg("hll")
            .arg("a")
            .arg("b")
            .arg("c")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PFADD")
            .arg("hll")
            .arg("a")
            .arg("b")
            .arg("c")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("")
        .query(&mut conn)
        .unwrap();

    let _: i64 = redis::cmd("DEL").arg("hll").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("1")
        .arg("2")
        .arg("3")
        .arg("4")
        .arg("5")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll")
            .query::<i64>(&mut conn)
            .unwrap(),
        5
    );
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("6")
        .arg("7")
        .arg("8")
        .arg("8")
        .arg("9")
        .arg("10")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll")
            .query::<i64>(&mut conn)
            .unwrap(),
        10
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_sparse_dense_and_debug_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("hll-sparse-max-bytes")
        .arg("3000")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL").arg("hll").query(&mut conn).unwrap();

    let mut seed = 1u64;
    let mut n = 0i64;
    while n < 20_000 {
        let mut cmd = redis::cmd("PFADD");
        cmd.arg("hll");
        for _ in 0..100 {
            cmd.arg(format!("v-{}", simple_rng(&mut seed)));
        }
        let _: i64 = cmd.query(&mut conn).unwrap();
        n += 100;
        let card: i64 = redis::cmd("PFCOUNT").arg("hll").query(&mut conn).unwrap();
        let err = (card - n).abs() as f64;
        assert!(err < (card as f64 / 100.0) * 8.0 + 8.0);
        if n < 1000 {
            let encoding: String = redis::cmd("PFDEBUG")
                .arg("ENCODING")
                .arg("hll")
                .query(&mut conn)
                .unwrap();
            assert_eq!(encoding, "sparse");
        } else if n > 10_000 {
            let encoding: String = redis::cmd("PFDEBUG")
                .arg("ENCODING")
                .arg("hll")
                .query(&mut conn)
                .unwrap();
            assert_eq!(encoding, "dense");
            break;
        }
    }

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("hll-sparse-max-bytes")
        .arg("30")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL").arg("small").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("small")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PFDEBUG")
            .arg("ENCODING")
            .arg("small")
            .query::<String>(&mut conn)
            .unwrap(),
        "dense"
    );

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("hll-sparse-max-bytes")
        .arg("3000")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg("hll1")
        .arg("hll2")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD").arg("hll2").query(&mut conn).unwrap();
    let _: String = redis::cmd("PFDEBUG")
        .arg("TODENSE")
        .arg("hll2")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll1")
        .arg("1")
        .arg("2")
        .arg("3")
        .arg("4")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll2")
        .arg("1")
        .arg("2")
        .arg("3")
        .arg("4")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PFDEBUG")
            .arg("ENCODING")
            .arg("hll1")
            .query::<String>(&mut conn)
            .unwrap(),
        "sparse"
    );
    assert_eq!(
        redis::cmd("PFDEBUG")
            .arg("ENCODING")
            .arg("hll2")
            .query::<String>(&mut conn)
            .unwrap(),
        "dense"
    );
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll1")
            .query::<i64>(&mut conn)
            .unwrap(),
        redis::cmd("PFCOUNT")
            .arg("hll2")
            .query::<i64>(&mut conn)
            .unwrap()
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_corruption_detection_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("APPEND")
        .arg("hll")
        .arg("hello")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("PFCOUNT").arg("hll").query::<i64>(&mut conn),
        "INVALIDOBJ",
    );

    let _: i64 = redis::cmd("DEL").arg("hll").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("SETRANGE")
        .arg("hll")
        .arg(0)
        .arg("0123")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("PFCOUNT").arg("hll").query::<i64>(&mut conn),
        "WRONGTYPE",
    );

    let _: i64 = redis::cmd("DEL").arg("hll").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("SETRANGE")
        .arg("hll")
        .arg(4)
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("PFCOUNT").arg("hll").query::<i64>(&mut conn),
        "WRONGTYPE",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_type_and_merge_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("foo{t}")
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("PFADD")
            .arg("foo{t}")
            .arg("1")
            .query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("PFCOUNT").arg("foo{t}").query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("PFMERGE")
            .arg("bar{t}")
            .arg("foo{t}")
            .query::<String>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("PFMERGE")
            .arg("foo{t}")
            .arg("bar{t}")
            .query::<String>(&mut conn),
        "WRONGTYPE",
    );

    let _: i64 = redis::cmd("DEL")
        .arg("hll{t}")
        .arg("hll1{t}")
        .arg("hll2{t}")
        .arg("hll3{t}")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll1{t}")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll2{t}")
        .arg("b")
        .arg("c")
        .arg("d")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll3{t}")
        .arg("c")
        .arg("d")
        .arg("e")
        .query(&mut conn)
        .unwrap();
    let merge_ok: String = redis::cmd("PFMERGE")
        .arg("hll{t}")
        .arg("hll1{t}")
        .arg("hll2{t}")
        .arg("hll3{t}")
        .query(&mut conn)
        .unwrap();
    assert_eq!(merge_ok, "OK");
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll{t}")
            .query::<i64>(&mut conn)
            .unwrap(),
        5
    );

    let _: i64 = redis::cmd("DEL")
        .arg("sourcekey{t}")
        .arg("sourcekey2{t}")
        .arg("destkey{t}")
        .arg("destkey2{t}")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PFMERGE")
            .arg("destkey{t}")
            .arg("sourcekey{t}")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("destkey{t}")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("destkey{t}")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    assert_eq!(
        redis::cmd("PFMERGE")
            .arg("destkey2{t}")
            .arg("sourcekey{t}")
            .arg("sourcekey2{t}")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("destkey2{t}")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    let _: i64 = redis::cmd("DEL").arg("destkey").query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("PFMERGE")
            .arg("destkey")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("destkey")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("destkey")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let _: i64 = redis::cmd("DEL").arg("destkey").query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("PFADD")
            .arg("destkey")
            .arg("a")
            .arg("b")
            .arg("c")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PFMERGE")
            .arg("destkey")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("destkey")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_multi_key_and_debug_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    for x in 1..10_000 {
        let _: i64 = redis::cmd("PFADD")
            .arg("hll1{t}")
            .arg(format!("foo-{x}"))
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("PFADD")
            .arg("hll2{t}")
            .arg(format!("bar-{x}"))
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("PFADD")
            .arg("hll3{t}")
            .arg(format!("zap-{x}"))
            .query(&mut conn)
            .unwrap();
        if x % 1000 == 0 {
            let card: i64 = redis::cmd("PFCOUNT")
                .arg("hll1{t}")
                .arg("hll2{t}")
                .arg("hll3{t}")
                .query(&mut conn)
                .unwrap();
            let real = (x * 3) as i64;
            let err = (card - real).abs() as f64;
            assert!(err < (card as f64 / 100.0) * 8.0 + 8.0);
        }
    }

    let _: i64 = redis::cmd("DEL")
        .arg("hll1{t}")
        .arg("hll2{t}")
        .arg("hll3{t}")
        .query(&mut conn)
        .unwrap();
    let mut seed = 7u64;
    let mut seen = HashSet::new();
    for _ in 0..10_000 {
        for key in ["hll1{t}", "hll2{t}", "hll3{t}"] {
            let value = (simple_rng(&mut seed) % 20_000).to_string();
            seen.insert(value.clone());
            let _: i64 = redis::cmd("PFADD")
                .arg(key)
                .arg(value)
                .query(&mut conn)
                .unwrap();
        }
    }
    let real = seen.len() as i64;
    let card: i64 = redis::cmd("PFCOUNT")
        .arg("hll1{t}")
        .arg("hll2{t}")
        .arg("hll3{t}")
        .query(&mut conn)
        .unwrap();
    let err = (card - real).abs() as f64;
    assert!(err < (card as f64 / 100.0) * 8.0 + 8.0);

    let regs = as_array(
        redis::cmd("PFDEBUG")
            .arg("GETREG")
            .arg("hll1{t}")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(regs.len(), 16_384);
    let _first_reg = as_i64(regs[0].clone());

    let _: i64 = redis::cmd("DEL").arg("hll").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PFCOUNT").arg("hll").query(&mut conn).unwrap();
    let byte15: Vec<u8> = redis::cmd("GETRANGE")
        .arg("hll")
        .arg(15)
        .arg(15)
        .query(&mut conn)
        .unwrap();
    assert_eq!(byte15, vec![0x00]);
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let byte15: Vec<u8> = redis::cmd("GETRANGE")
        .arg("hll")
        .arg(15)
        .arg(15)
        .query(&mut conn)
        .unwrap();
    assert_eq!(byte15, vec![0x00]);
    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("1")
        .arg("2")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    let byte15: Vec<u8> = redis::cmd("GETRANGE")
        .arg("hll")
        .arg(15)
        .arg(15)
        .query(&mut conn)
        .unwrap();
    assert_eq!(byte15, vec![0x80]);
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_pfselftest_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let ok: String = redis::cmd("PFSELFTEST").query(&mut conn).unwrap();
    assert_eq!(ok, "OK");
}

#[test]
#[ignore = "requires running Senko instance"]
fn hyperloglog_pfdebug_decode_encode_and_simd_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("PFADD")
        .arg("hll")
        .arg("1")
        .arg("2")
        .arg("3")
        .query(&mut conn)
        .unwrap();

    let decoded: String = redis::cmd("PFDEBUG")
        .arg("DECODE")
        .arg("hll")
        .query(&mut conn)
        .unwrap();
    assert!(decoded.contains("sparse") || decoded.contains("dense"));

    let before: Vec<u8> = raw(&mut conn, "hll");
    let ok: String = redis::cmd("PFDEBUG")
        .arg("ENCODE")
        .arg("hll")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let after: Vec<u8> = raw(&mut conn, "hll");
    assert!(!after.is_empty());
    assert_eq!(
        redis::cmd("PFCOUNT")
            .arg("hll")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    assert!(!before.is_empty());

    let ok: String = redis::cmd("PFDEBUG")
        .arg("SIMD")
        .arg("OFF")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let scalar: i64 = redis::cmd("PFCOUNT").arg("hll").query(&mut conn).unwrap();
    let ok: String = redis::cmd("PFDEBUG")
        .arg("SIMD")
        .arg("ON")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let simd: i64 = redis::cmd("PFCOUNT").arg("hll").query(&mut conn).unwrap();
    assert_eq!(scalar, simd);
}
