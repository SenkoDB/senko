#![allow(clippy::too_many_lines)]

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rand::{Rng, SeedableRng, rngs::SmallRng};
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

fn create_zset(conn: &mut Connection, key: &str, items: &[(&str, &str)]) {
    let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    if items.is_empty() {
        return;
    }
    let mut cmd = redis::cmd("ZADD");
    cmd.arg(key);
    for (score, member) in items {
        cmd.arg(score).arg(member);
    }
    let _: i64 = cmd.query(conn).unwrap();
}

fn encoding(conn: &mut Connection, key: &str) -> String {
    redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg(key)
        .query(conn)
        .unwrap()
}

fn assert_encoding(conn: &mut Connection, expected: &str, key: &str) {
    let encoding = encoding(conn, key);
    assert!(
        encoding.contains(expected),
        "expected encoding containing {expected:?}, got {encoding:?} for key {key:?}"
    );
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

fn as_string(value: Value) -> String {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
        Value::SimpleString(text) => text,
        Value::Int(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        other => panic!("expected string-like value, got {other:?}"),
    }
}

fn zmpop_value(value: Value) -> (String, Vec<(String, String)>) {
    match value {
        Value::Array(values) => {
            assert_eq!(values.len(), 2);
            let key = as_string(values[0].clone());
            let Value::Array(items) = values[1].clone() else {
                panic!("expected nested array");
            };
            let pairs = items
                .into_iter()
                .map(|item| match item {
                    Value::Array(pair) => {
                        assert_eq!(pair.len(), 2);
                        (as_string(pair[0].clone()), as_string(pair[1].clone()))
                    }
                    other => panic!("expected pair array, got {other:?}"),
                })
                .collect();
            (key, pairs)
        }
        other => panic!("expected zmpop array, got {other:?}"),
    }
}

fn zscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("ZSCAN");
        cmd.arg(key).arg(&cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        let (next, page): (String, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    out
}

fn chi_square_uniform(samples: &[String], expected_items: &[&str]) -> f64 {
    let expected = samples.len() as f64 / expected_items.len() as f64;
    let mut counts = HashMap::<String, usize>::new();
    for sample in samples {
        *counts.entry(sample.clone()).or_default() += 1;
    }
    expected_items
        .iter()
        .map(|item| {
            let observed = *counts.get(*item).unwrap_or(&0) as f64;
            let delta = observed - expected;
            delta * delta / expected
        })
        .sum()
}

#[derive(Clone, Copy)]
enum EncodingMode {
    Listpack,
    Large,
}

impl EncodingMode {
    fn expected_encodings(self) -> &'static [&'static str] {
        match self {
            Self::Listpack => &["listpack"],
            Self::Large => &["skiplist", "bptree"],
        }
    }

    fn max_entries(self) -> i64 {
        match self {
            Self::Listpack => 128,
            Self::Large => 0,
        }
    }

    fn max_value(self) -> i64 {
        match self {
            Self::Listpack => 64,
            Self::Large => 0,
        }
    }
}

fn assert_encoding_any(conn: &mut Connection, expected: &[&str], key: &str) {
    let actual = encoding(conn, key);
    assert!(
        expected.iter().any(|needle| actual.contains(needle)),
        "expected encoding containing one of {expected:?}, got {actual:?} for key {key:?}"
    );
}

fn config_get_i64(conn: &mut Connection, key: &str) -> i64 {
    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg(key)
        .query(conn)
        .unwrap();
    values[1].parse().unwrap()
}

fn config_set(conn: &mut Connection, key: &str, value: impl ToString) {
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg(key)
        .arg(value.to_string())
        .query(conn)
        .unwrap();
}

fn with_zset_encoding(conn: &mut Connection, mode: EncodingMode, f: impl FnOnce(&mut Connection)) {
    let original_entries = config_get_i64(conn, "zset-max-listpack-entries");
    let original_value = config_get_i64(conn, "zset-max-listpack-value");
    config_set(conn, "zset-max-listpack-entries", mode.max_entries());
    config_set(conn, "zset-max-listpack-value", mode.max_value());
    f(conn);
    config_set(conn, "zset-max-listpack-entries", original_entries);
    config_set(conn, "zset-max-listpack-value", original_value)
}

fn create_default_zset(conn: &mut Connection) {
    create_zset(
        conn,
        "zset",
        &[
            ("-inf", "a"),
            ("1", "b"),
            ("2", "c"),
            ("3", "d"),
            ("4", "e"),
            ("5", "f"),
            ("+inf", "g"),
        ],
    );
}

fn create_long_zset(conn: &mut Connection, key: &str, len: usize) {
    let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    for i in 0..len {
        let _: i64 = redis::cmd("ZADD")
            .arg(key)
            .arg(i)
            .arg(format!("i{i}"))
            .query(conn)
            .unwrap();
    }
}

fn create_default_lex_zset(conn: &mut Connection) {
    create_zset(
        conn,
        "zset",
        &[
            ("0", "alpha"),
            ("0", "bar"),
            ("0", "cool"),
            ("0", "down"),
            ("0", "elephant"),
            ("0", "foo"),
            ("0", "great"),
            ("0", "hill"),
            ("0", "omega"),
        ],
    );
}

fn create_long_lex_zset(conn: &mut Connection) {
    create_zset(
        conn,
        "zset",
        &[
            ("0", "alpha"),
            ("0", "bar"),
            ("0", "cool"),
            ("0", "down"),
            ("0", "elephant"),
            ("0", "foo"),
            ("0", "great"),
            ("0", "hill"),
            ("0", "island"),
            ("0", "jacket"),
            ("0", "key"),
            ("0", "lip"),
            ("0", "max"),
            ("0", "null"),
            ("0", "omega"),
            ("0", "point"),
            ("0", "query"),
            ("0", "result"),
            ("0", "sea"),
            ("0", "tree"),
        ],
    );
}

fn make_rng() -> SmallRng {
    SmallRng::seed_from_u64(0x5eed_cafe_f00d)
}

fn rand_alpha(rng: &mut SmallRng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    (0..len)
        .map(|_| (b'a' + rng.gen_range(0..26)) as char)
        .collect()
}

fn assert_sorted_pairs(entries: &[(String, String)]) {
    let mut last: Option<(f64, String)> = None;
    for (member, score) in entries {
        let score_val: f64 = score.parse().unwrap();
        if let Some((prev_score, prev_member)) = &last {
            assert!(
                *prev_score < score_val
                    || (*prev_score == score_val && prev_member.as_str() < member.as_str()),
                "out of order: previous=({prev_member},{prev_score}) current=({member},{score_val})"
            );
        }
        last = Some((score_val, member.clone()));
    }
}

fn as_pairs(flat: Vec<String>) -> Vec<(String, String)> {
    flat.chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect()
}

fn create_set(conn: &mut Connection, key: &str, entries: &[&str]) {
    let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    if !entries.is_empty() {
        let _: i64 = redis::cmd("SADD")
            .arg(key)
            .arg(entries)
            .query(conn)
            .unwrap();
    }
}

fn zscore(conn: &mut Connection, key: &str, member: &str) -> Option<String> {
    redis::cmd("ZSCORE")
        .arg(key)
        .arg(member)
        .query(conn)
        .unwrap()
}

fn exists(conn: &mut Connection, key: &str) -> i64 {
    redis::cmd("EXISTS").arg(key).query(conn).unwrap()
}

fn zlist_alike_sort(entries: &mut [(f64, String)]) {
    entries.sort_by(|(sa, a), (sb, b)| match sa.total_cmp(sb) {
        Ordering::Equal => a.cmp(b),
        ord => ord,
    });
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_basics_and_options_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("zset-max-listpack-entries")
        .arg(128)
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("zset-max-listpack-value")
        .arg(64)
        .query(&mut conn)
        .unwrap();

    create_zset(&mut conn, "zs", &[("10", "x")]);
    assert_encoding(&mut conn, "listpack", "zs");
    let _: i64 = redis::cmd("ZADD")
        .arg("zs")
        .arg(20)
        .arg("y")
        .arg(30)
        .arg("z")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("zs")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["x", "y", "z"]
    );

    let changed: i64 = redis::cmd("ZADD")
        .arg("zs")
        .arg("GT")
        .arg("CH")
        .arg(5)
        .arg("foo")
        .arg(11)
        .arg("x")
        .arg(21)
        .arg("y")
        .arg(29)
        .arg("z")
        .query(&mut conn)
        .unwrap();
    assert_eq!(changed, 3);
    assert_eq!(
        redis::cmd("ZSCORE")
            .arg("zs")
            .arg("x")
            .query::<String>(&mut conn)
            .unwrap(),
        "11"
    );

    let changed: i64 = redis::cmd("ZADD")
        .arg("zs")
        .arg("LT")
        .arg("XX")
        .arg("CH")
        .arg(10)
        .arg("x")
        .arg(20)
        .arg("y")
        .arg(28)
        .arg("z")
        .query(&mut conn)
        .unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        redis::cmd("ZSCORE")
            .arg("zs")
            .arg("z")
            .query::<String>(&mut conn)
            .unwrap(),
        "28"
    );

    let skipped: Option<String> = redis::cmd("ZADD")
        .arg("zs")
        .arg("LT")
        .arg("INCR")
        .arg(1)
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert!(skipped.is_none());

    let incr: String = redis::cmd("ZINCRBY")
        .arg("zs")
        .arg(2)
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_eq!(incr, "13");

    assert_eq!(
        redis::cmd("ZMSCORE")
            .arg("zs")
            .arg("x")
            .arg("missing")
            .query::<Vec<Option<String>>>(&mut conn)
            .unwrap(),
        vec![Some("13".into()), None]
    );
    assert_eq!(
        redis::cmd("ZCARD")
            .arg("zs")
            .query::<i64>(&mut conn)
            .unwrap(),
        4
    );
    assert_eq!(
        redis::cmd("ZRANK")
            .arg("zs")
            .arg("foo")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("ZREVRANK")
            .arg("zs")
            .arg("foo")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_ranges_and_store_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_zset(
        &mut conn,
        "z1",
        &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")],
    );

    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "1", "b", "2", "c", "3", "d", "4"]
    );
    assert_eq!(
        redis::cmd("ZREVRANGE")
            .arg("z1")
            .arg(0)
            .arg(1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["d", "4", "c", "3"]
    );
    assert_eq!(
        redis::cmd("ZRANGEBYSCORE")
            .arg("z1")
            .arg("(1")
            .arg("3")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["b", "c"]
    );

    create_zset(
        &mut conn,
        "lex",
        &[("0", "alpha"), ("0", "bar"), ("0", "cool"), ("0", "down")],
    );
    assert_eq!(
        redis::cmd("ZRANGEBYLEX")
            .arg("lex")
            .arg("[bar")
            .arg("[down")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["bar", "cool", "down"]
    );

    let stored: i64 = redis::cmd("ZRANGESTORE")
        .arg("z2")
        .arg("z1")
        .arg("1")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(stored, 2);
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z2")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["b", "2", "c", "3"]
    );

    let byscore: i64 = redis::cmd("ZRANGESTORE")
        .arg("z3")
        .arg("z1")
        .arg("4")
        .arg("1")
        .arg("BYSCORE")
        .arg("REV")
        .arg("LIMIT")
        .arg(0)
        .arg(2)
        .query(&mut conn)
        .unwrap();
    assert_eq!(byscore, 2);
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z3")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["c", "3", "d", "4"]
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_algebra_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_zset(&mut conn, "a", &[("1", "a"), ("2", "b"), ("3", "c")]);
    create_zset(&mut conn, "b", &[("1", "b"), ("2", "c"), ("3", "d")]);

    assert_eq!(
        redis::cmd("ZUNION")
            .arg(2)
            .arg("a")
            .arg("b")
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "1", "b", "3", "d", "3", "c", "5"]
    );
    assert_eq!(
        redis::cmd("ZINTER")
            .arg(2)
            .arg("a")
            .arg("b")
            .arg("WEIGHTS")
            .arg(2)
            .arg(3)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["b", "7", "c", "12"]
    );
    assert_eq!(
        redis::cmd("ZINTERCARD")
            .arg(2)
            .arg("a")
            .arg("b")
            .arg("LIMIT")
            .arg(1)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("ZDIFF")
            .arg(2)
            .arg("a")
            .arg("b")
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "1"]
    );

    let stored: i64 = redis::cmd("ZUNIONSTORE")
        .arg("dst")
        .arg(2)
        .arg("a")
        .arg("b")
        .arg("AGGREGATE")
        .arg("MIN")
        .query(&mut conn)
        .unwrap();
    assert_eq!(stored, 4);
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("dst")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "1", "b", "1", "c", "2", "d", "3"]
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_pop_and_blocking_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_zset(
        &mut conn,
        "p",
        &[("0", "a"), ("1", "b"), ("2", "c"), ("3", "d")],
    );
    assert_eq!(
        redis::cmd("ZPOPMIN")
            .arg("p")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "0"]
    );
    assert_eq!(
        redis::cmd("ZPOPMAX")
            .arg("p")
            .arg(2)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["d", "3", "c", "2"]
    );

    create_zset(&mut conn, "m2", &[("1", "x"), ("2", "y")]);
    let mpop = redis::cmd("ZMPOP")
        .arg(2)
        .arg("missing")
        .arg("m2")
        .arg("MIN")
        .arg("COUNT")
        .arg(2)
        .query::<Value>(&mut conn)
        .unwrap();
    assert_eq!(
        zmpop_value(mpop),
        (
            "m2".into(),
            vec![("x".into(), "1".into()), ("y".into(), "2".into())]
        )
    );

    let timeout: Value = redis::cmd("BZPOPMIN")
        .arg("never")
        .arg("0.2")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(timeout, Value::Nil));

    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let (tx, rx) = mpsc::channel();
    let pop_url = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(pop_url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let started = Instant::now();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("bk")
            .arg("5")
            .query(&mut bg)
            .unwrap();
        tx.send((started.elapsed(), value)).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("ZADD")
        .arg("bk")
        .arg(1)
        .arg("wake")
        .query(&mut conn)
        .unwrap();
    let (elapsed, value) = rx.recv().unwrap();
    assert!(elapsed < Duration::from_secs(2));
    assert_eq!(value, vec!["bk", "wake", "1"]);

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let client = redis::Client::open(url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Value = redis::cmd("BZMPOP")
            .arg("5")
            .arg(2)
            .arg("bk2")
            .arg("bk3")
            .arg("MAX")
            .arg("COUNT")
            .arg(2)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("ZADD")
        .arg("bk3")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        zmpop_value(rx.recv().unwrap()),
        (
            "bk3".into(),
            vec![("c".into(), "3".into()), ("b".into(), "2".into())]
        )
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_misc_and_scan_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_zset(
        &mut conn,
        "rand",
        &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")],
    );
    let distinct: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("rand")
        .arg(4)
        .query(&mut conn)
        .unwrap();
    assert_eq!(distinct.len(), 4);
    assert_eq!(
        distinct.iter().cloned().collect::<HashSet<_>>().len(),
        distinct.len()
    );

    let repeating: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("rand")
        .arg(-1000)
        .query(&mut conn)
        .unwrap();
    assert_eq!(repeating.len(), 1000);
    assert!(
        chi_square_uniform(&repeating, &["a", "b", "c", "d"]) < 25.0,
        "distribution too skewed: {repeating:?}"
    );

    let withscores: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("rand")
        .arg(2)
        .arg("WITHSCORES")
        .query(&mut conn)
        .unwrap();
    assert_eq!(withscores.len() % 2, 0);

    create_zset(
        &mut conn,
        "rr",
        &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("4", "e")],
    );
    assert_eq!(
        redis::cmd("ZREMRANGEBYSCORE")
            .arg("rr")
            .arg("(1")
            .arg("4")
            .query::<i64>(&mut conn)
            .unwrap(),
        4
    );
    assert_eq!(
        redis::cmd("ZCARD")
            .arg("rr")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    create_zset(&mut conn, "rr2", &[("1", "a"), ("2", "b"), ("3", "c")]);
    assert_eq!(
        redis::cmd("ZREMRANGEBYRANK")
            .arg("rr2")
            .arg(-2)
            .arg(-1)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );

    create_zset(&mut conn, "rr3", &[("0", "aa"), ("0", "ab"), ("0", "ba")]);
    assert_eq!(
        redis::cmd("ZREMRANGEBYLEX")
            .arg("rr3")
            .arg("[aa")
            .arg("[az")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );

    let mut cmd = redis::cmd("ZADD");
    cmd.arg("scan");
    for i in 0..130 {
        cmd.arg(i).arg(format!("m{i:03}"));
    }
    let _: i64 = cmd.query(&mut conn).unwrap();

    let page = zscan_collect(&mut conn, "scan", Some("m1*"), Some(7));
    assert!(!page.is_empty());
    assert_eq!(page.len() % 2, 0);
    for pair in page.chunks(2) {
        assert!(pair[0].starts_with("m1"));
    }
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_error_cases_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    assert_err_contains(
        redis::cmd("ZADD")
            .arg("bad")
            .arg("nan")
            .arg("x")
            .query::<i64>(&mut conn),
        "float",
    );
    assert_err_contains(
        redis::cmd("ZINCRBY")
            .arg("bad")
            .arg("nan")
            .arg("x")
            .query::<String>(&mut conn),
        "float",
    );
    assert_err_contains(
        redis::cmd("ZADD")
            .arg("bad")
            .arg("NX")
            .arg("XX")
            .arg(1)
            .arg("x")
            .query::<i64>(&mut conn),
        "compatible",
    );
    assert_err_contains(
        redis::cmd("ZRANGE")
            .arg("bad")
            .arg(0)
            .arg(-1)
            .arg("LIMIT")
            .arg(0)
            .arg(1)
            .query::<Vec<String>>(&mut conn),
        "LIMIT",
    );
    assert_err_contains(
        redis::cmd("ZUNION")
            .arg(2)
            .arg("a")
            .arg("b")
            .arg("WEIGHTS")
            .arg(1)
            .query::<Vec<String>>(&mut conn),
        "syntax",
    );

    let _: String = redis::cmd("SET")
        .arg("s")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("ZPOPMIN")
            .arg("s")
            .query::<Vec<String>>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("BZPOPMIN")
            .arg("s")
            .arg("0.01")
            .query::<Value>(&mut conn),
        "WRONGTYPE",
    );
}

fn expected_strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

fn run_basics(conn: &mut Connection, mode: EncodingMode) {
    with_zset_encoding(conn, mode, |conn| {
        flush(conn);

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("x")
            .query(conn)
            .unwrap();
        assert_encoding_any(conn, mode.expected_encodings(), "ztmp");

        flush(conn);
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .arg(30)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["x", "y", "z"])
        );
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(1)
            .arg("y")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["y", "x", "z"])
        );

        assert_err_contains(
            redis::cmd("ZADD")
                .arg("myzset")
                .arg("nan")
                .arg("abc")
                .query::<i64>(conn),
            "float",
        );
        assert_err_contains(
            redis::cmd("ZINCRBY")
                .arg("myzset")
                .arg("nan")
                .arg("abc")
                .query::<String>(conn),
            "float",
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("xx")
                .arg(10)
                .arg("x")
                .arg(20)
                .query::<i64>(conn),
            "syntax",
        );

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        assert_eq!(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("xx")
                .arg(10)
                .arg("x")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("TYPE")
                .arg("ztmp")
                .query::<String>(conn)
                .unwrap(),
            "none"
        );

        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .arg(30)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("xx")
                .arg(20)
                .arg("y")
                .arg(40)
                .arg("new")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("ZCARD").arg("ztmp").query::<i64>(conn).unwrap(),
            3
        );

        assert_eq!(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg(10)
                .arg("x")
                .arg(20)
                .arg("y")
                .arg(30)
                .arg("z")
                .query::<i64>(conn)
                .unwrap(),
            0
        );

        let changed: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("gt")
            .arg("ch")
            .arg(5)
            .arg("foo")
            .arg(11)
            .arg("x")
            .arg(21)
            .arg("y")
            .arg(29)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(changed, 3);
        assert_eq!(zscore(conn, "ztmp", "x").unwrap(), "11");
        assert_eq!(zscore(conn, "ztmp", "y").unwrap(), "21");
        assert_eq!(zscore(conn, "ztmp", "z").unwrap(), "30");

        let changed: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("lt")
            .arg("ch")
            .arg(5)
            .arg("foo")
            .arg(11)
            .arg("x")
            .arg(21)
            .arg("y")
            .arg(29)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(changed, 2);
        assert_eq!(zscore(conn, "ztmp", "x").unwrap(), "11");
        assert_eq!(zscore(conn, "ztmp", "y").unwrap(), "21");
        assert_eq!(zscore(conn, "ztmp", "z").unwrap(), "29");

        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("xx")
                .arg("nx")
                .arg(10)
                .arg("x")
                .query::<i64>(conn),
            "compatible",
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("gt")
                .arg("nx")
                .arg(10)
                .arg("x")
                .query::<i64>(conn),
            "compatible",
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("lt")
                .arg("nx")
                .arg(10)
                .arg("x")
                .query::<i64>(conn),
            "compatible",
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("lt")
                .arg("gt")
                .arg(10)
                .arg("x")
                .query::<i64>(conn),
            "compatible",
        );

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("nx")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .arg(30)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZCARD").arg("ztmp").query::<i64>(conn).unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("nx")
                .arg(11)
                .arg("x")
                .arg(21)
                .arg("y")
                .arg(100)
                .arg("a")
                .arg(200)
                .arg("b")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(zscore(conn, "ztmp", "x").unwrap(), "10");
        assert_eq!(zscore(conn, "ztmp", "a").unwrap(), "100");

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(28)
            .arg("x")
            .query(conn)
            .unwrap();
        let res: Option<String> = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("lt")
            .arg("incr")
            .arg(1)
            .arg("x")
            .query(conn)
            .unwrap();
        assert!(res.is_none());
        let res: Option<String> = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("gt")
            .arg("incr")
            .arg(-1)
            .arg("x")
            .query(conn)
            .unwrap();
        assert!(res.is_none());
        assert_eq!(zscore(conn, "ztmp", "x").unwrap(), "28");

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("+inf")
            .arg("x")
            .arg("-inf")
            .arg("y")
            .query(conn)
            .unwrap();
        for args in [
            vec!["lt", "incr", "1", "x"],
            vec!["gt", "incr", "-1", "x"],
            vec!["lt", "incr", "-1", "x"],
            vec!["gt", "incr", "1", "x"],
            vec!["lt", "incr", "1", "y"],
            vec!["gt", "incr", "-1", "y"],
            vec!["lt", "incr", "-1", "y"],
            vec!["gt", "incr", "1", "y"],
        ] {
            let out: Option<String> = redis::cmd("ZADD")
                .arg("ztmp")
                .arg(args)
                .query(conn)
                .unwrap();
            assert!(out.is_none());
        }
        let sx = zscore(conn, "ztmp", "x").unwrap();
        let sy = zscore(conn, "ztmp", "y").unwrap();
        assert!(sx == "inf" || sx == "+inf");
        assert!(sy == "-inf");

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .arg(30)
            .arg("z")
            .query(conn)
            .unwrap();
        let out: String = redis::cmd("ZADD")
            .arg("ztmp")
            .arg("INCR")
            .arg(15)
            .arg("x")
            .query(conn)
            .unwrap();
        assert_eq!(out, "25");
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("ztmp")
                .arg("INCR")
                .arg(15)
                .arg("x")
                .arg(10)
                .arg("y")
                .query::<String>(conn),
            "single",
        );

        let _: i64 = redis::cmd("DEL").arg("myzset").query(conn).unwrap();
        assert_eq!(
            redis::cmd("ZADD")
                .arg("myzset")
                .arg(10)
                .arg("a")
                .arg(20)
                .arg("b")
                .arg(30)
                .arg("c")
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("myzset")
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "10", "b", "20", "c", "30"])
        );
        assert_eq!(
            redis::cmd("ZADD")
                .arg("myzset")
                .arg(5)
                .arg("x")
                .arg(20)
                .arg("b")
                .arg(30)
                .arg("c")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("badz")
                .arg(10)
                .arg("a")
                .arg(20)
                .arg("b")
                .arg("30.badscore")
                .arg("c")
                .query::<i64>(conn),
            "float",
        );
        assert_eq!(exists(conn, "badz"), 0);
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("myzset")
                .arg(10)
                .arg("a")
                .arg(20)
                .arg("b")
                .arg(30)
                .arg("c")
                .arg(40)
                .query::<i64>(conn),
            "syntax",
        );
        assert_err_contains(
            redis::cmd("ZINCRBY")
                .arg("myzset")
                .arg(10)
                .arg("a")
                .arg(20)
                .arg("b")
                .query::<String>(conn),
            "wrong number",
        );

        assert_eq!(
            redis::cmd("ZCARD")
                .arg("myzset")
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZCARD")
                .arg("missing")
                .query::<i64>(conn)
                .unwrap(),
            0
        );

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .query(conn)
            .unwrap();
        assert_eq!(exists(conn, "ztmp"), 1);
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("z")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("y")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("x")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(exists(conn, "ztmp"), 0);

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(10)
            .arg("a")
            .arg(20)
            .arg("b")
            .arg(30)
            .arg("c")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("x")
                .arg("y")
                .arg("a")
                .arg("b")
                .arg("k")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("foo")
                .arg("bar")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("ZREM")
                .arg("ztmp")
                .arg("c")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(exists(conn, "ztmp"), 0);

        let _: i64 = redis::cmd("DEL").arg("ztmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("ztmp")
            .arg(1)
            .arg("a")
            .arg(2)
            .arg("b")
            .arg(3)
            .arg("c")
            .arg(4)
            .arg("d")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "b", "c", "d"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "b", "c"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(1)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "c", "d"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(1)
                .arg(-2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "c"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(-2)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["c", "d"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(-2)
                .arg(-2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["c"])
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(-5)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "b", "c"])
        );
        assert!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(5)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(5)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "b", "c", "d"])
        );
        assert!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-5)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "2", "c", "3", "d", "4"])
        );

        assert_eq!(
            redis::cmd("ZREVRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "c", "b", "a"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGE")
                .arg("ztmp")
                .arg(1)
                .arg(-2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["c", "b"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGE")
                .arg("ztmp")
                .arg(-2)
                .arg(-2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGE")
                .arg("ztmp")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "4", "c", "3", "b", "2", "a", "1"])
        );

        let _: i64 = redis::cmd("DEL").arg("zranktmp").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("zranktmp")
            .arg(10)
            .arg("x")
            .arg(20)
            .arg("y")
            .arg(30)
            .arg("z")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANK")
                .arg("zranktmp")
                .arg("x")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("ZRANK")
                .arg("zranktmp")
                .arg("y")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(
            redis::cmd("ZREVRANK")
                .arg("zranktmp")
                .arg("x")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANK")
                .arg("zranktmp")
                .arg("x")
                .arg("withscore")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["0", "10"])
        );
        assert_eq!(
            redis::cmd("ZREVRANK")
                .arg("zranktmp")
                .arg("z")
                .arg("withscore")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["0", "30"])
        );
        let none: Option<i64> = redis::cmd("ZRANK")
            .arg("zranktmp")
            .arg("foo")
            .query(conn)
            .unwrap();
        assert!(none.is_none());
        let none: Option<Vec<String>> = redis::cmd("ZRANK")
            .arg("zranktmp")
            .arg("foo")
            .arg("withscore")
            .query(conn)
            .unwrap();
        assert!(none.is_none());
        let _: i64 = redis::cmd("ZREM")
            .arg("zranktmp")
            .arg("y")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANK")
                .arg("zranktmp")
                .arg("z")
                .query::<i64>(conn)
                .unwrap(),
            1
        );

        let _: i64 = redis::cmd("DEL").arg("zset").query(conn).unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(1)
            .arg("foo")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["foo"])
        );
        assert_eq!(zscore(conn, "zset", "foo").unwrap(), "1");
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(2)
            .arg("foo")
            .query(conn)
            .unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(1)
            .arg("bar")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["bar", "foo"])
        );
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(10)
            .arg("bar")
            .query(conn)
            .unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(-5)
            .arg("foo")
            .query(conn)
            .unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zset")
            .arg(-5)
            .arg("bar")
            .query(conn)
            .unwrap();
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["foo", "bar"])
        );
        assert_eq!(zscore(conn, "zset", "foo").unwrap(), "-2");
        assert_eq!(zscore(conn, "zset", "bar").unwrap(), "6");
        assert_eq!(
            redis::cmd("ZINCRBY")
                .arg("znew")
                .arg(1.0)
                .arg("x")
                .query::<String>(conn)
                .unwrap(),
            "1"
        );

        let _: i64 = redis::cmd("DEL").arg("myzset").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("myzset")
            .arg("+inf")
            .arg("abc")
            .query(conn)
            .unwrap();
        assert_err_contains(
            redis::cmd("ZINCRBY")
                .arg("myzset")
                .arg("-inf")
                .arg("abc")
                .query::<String>(conn),
            "NaN",
        );

        let _: i64 = redis::cmd("DEL").arg("zhexa").query(conn).unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("zhexa")
            .arg("0x0p+0")
            .arg("zero")
            .query(conn)
            .unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("zhexa")
            .arg("0x1p+0")
            .arg("one")
            .query(conn)
            .unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zhexa")
            .arg("0x0p+0")
            .arg("zero")
            .query(conn)
            .unwrap();
        let _: String = redis::cmd("ZINCRBY")
            .arg("zhexa")
            .arg("0x1p+0")
            .arg("one")
            .query(conn)
            .unwrap();
        assert_eq!(zscore(conn, "zhexa", "zero").unwrap(), "0");
        assert_eq!(zscore(conn, "zhexa", "one").unwrap(), "2");
        assert_err_contains(
            redis::cmd("ZINCRBY")
                .arg("zincr")
                .arg("v")
                .arg("one")
                .query::<String>(conn),
            "valid float",
        );

        create_default_zset(conn);
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg("-inf")
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "b", "c"])
        );
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(0)
                .arg(3)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "c", "d"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg("zset")
                .arg(3)
                .arg(0)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "c", "b"])
        );
        assert_eq!(
            redis::cmd("ZCOUNT")
                .arg("zset")
                .arg(0)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg("(0")
                .arg("(3")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "c"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg("zset")
                .arg("(3")
                .arg("(0")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["c", "b"])
        );
        assert_eq!(
            redis::cmd("ZCOUNT")
                .arg("zset")
                .arg("(0")
                .arg("(3")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        let _: i64 = redis::cmd("ZREM").arg("zset").arg("a").query(conn).unwrap();
        let _: i64 = redis::cmd("ZREM").arg("zset").arg("g").query(conn).unwrap();
        assert!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(4)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(2.4)
                .arg(2.6)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(0)
                .arg(3)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "1", "c", "2", "d", "3"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg("zset")
                .arg(3)
                .arg(0)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "3", "c", "2", "b", "1"])
        );
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(0)
                .arg(10)
                .arg("LIMIT")
                .arg(2)
                .arg(3)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "e", "f"])
        );
        assert!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(0)
                .arg(10)
                .arg("LIMIT")
                .arg(-1)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        create_long_zset(conn, "zset", 30);
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(0)
                .arg(20)
                .arg("LIMIT")
                .arg(12)
                .arg(3)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["i12", "i13", "i14"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYSCORE")
                .arg("zset")
                .arg(20)
                .arg(0)
                .arg("LIMIT")
                .arg(18)
                .arg(5)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["i2", "i1", "i0"])
        );
        assert_eq!(
            redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(2)
                .arg(5)
                .arg("LIMIT")
                .arg(2)
                .arg(3)
                .arg("WITHSCORES")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["i4", "4", "i5", "5"])
        );
        assert_err_contains(
            redis::cmd("ZRANGEBYSCORE")
                .arg("fooz")
                .arg("str")
                .arg(1)
                .query::<Vec<String>>(conn),
            "float",
        );
        assert_err_contains(
            redis::cmd("ZRANGEBYSCORE")
                .arg("fooz")
                .arg(1)
                .arg("NaN")
                .query::<Vec<String>>(conn),
            "float",
        );

        create_default_lex_zset(conn);
        assert_eq!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("[cool")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["alpha", "bar", "cool"])
        );
        assert_eq!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("[bar")
                .arg("[down")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["bar", "cool", "down"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYLEX")
                .arg("zset")
                .arg("[cool")
                .arg("-")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["cool", "bar", "alpha"])
        );
        assert_eq!(
            redis::cmd("ZLEXCOUNT")
                .arg("zset")
                .arg("[ele")
                .arg("[h")
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("(cool")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["alpha", "bar"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYLEX")
                .arg("zset")
                .arg("+")
                .arg("(great")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["omega", "hill"])
        );
        assert_eq!(
            redis::cmd("ZLEXCOUNT")
                .arg("zset")
                .arg("(ele")
                .arg("(great")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZLEXCOUNT")
                .arg("zset")
                .arg("-")
                .arg("+")
                .query::<i64>(conn)
                .unwrap(),
            9
        );
        assert_eq!(
            redis::cmd("ZLEXCOUNT")
                .arg("zset")
                .arg("[bar")
                .arg("(foo")
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("[cool")
                .arg("LIMIT")
                .arg(1)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["bar", "cool"])
        );
        assert!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("[bar")
                .arg("[down")
                .arg("LIMIT")
                .arg(0)
                .arg(0)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("[cool")
                .arg("LIMIT")
                .arg(-1)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        create_long_lex_zset(conn);
        assert_eq!(
            redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("[tree")
                .arg("LIMIT")
                .arg(12)
                .arg(2)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["max", "null"])
        );
        assert_eq!(
            redis::cmd("ZREVRANGEBYLEX")
                .arg("zset")
                .arg("+")
                .arg("[o")
                .arg("LIMIT")
                .arg(0)
                .arg(5)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["tree", "sea", "result", "query", "point"])
        );
        assert_err_contains(
            redis::cmd("ZRANGEBYLEX")
                .arg("fooz")
                .arg("foo")
                .arg("bar")
                .query::<Vec<String>>(conn),
            "string",
        );

        create_zset(
            conn,
            "zset",
            &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")],
        );
        assert_eq!(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg("zset")
                .arg(2)
                .arg(4)
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "e"])
        );
        create_zset(
            conn,
            "zset",
            &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")],
        );
        assert_eq!(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg("zset")
                .arg("-inf")
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["d", "e"])
        );
        create_zset(
            conn,
            "zset",
            &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")],
        );
        assert_eq!(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg("zset")
                .arg(1)
                .arg(5)
                .query::<i64>(conn)
                .unwrap(),
            5
        );
        assert_eq!(exists(conn, "zset"), 0);
        assert_err_contains(
            redis::cmd("ZREMRANGEBYSCORE")
                .arg("fooz")
                .arg("str")
                .arg(1)
                .query::<i64>(conn),
            "float",
        );

        create_zset(
            conn,
            "zset",
            &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")],
        );
        assert_eq!(
            redis::cmd("ZREMRANGEBYRANK")
                .arg("zset")
                .arg(1)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "e"])
        );
        create_zset(
            conn,
            "zset",
            &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")],
        );
        assert_eq!(
            redis::cmd("ZREMRANGEBYRANK")
                .arg("zset")
                .arg(0)
                .arg(10)
                .query::<i64>(conn)
                .unwrap(),
            5
        );
        assert_eq!(exists(conn, "zset"), 0);

        create_default_lex_zset(conn);
        assert_eq!(
            redis::cmd("ZREMRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("[cool")
                .query::<i64>(conn)
                .unwrap(),
            3
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zset")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["down", "elephant", "foo", "great", "hill", "omega"])
        );
        create_default_lex_zset(conn);
        assert_eq!(
            redis::cmd("ZREMRANGEBYLEX")
                .arg("zset")
                .arg("-")
                .arg("+")
                .query::<i64>(conn)
                .unwrap(),
            9
        );
        assert_eq!(exists(conn, "zset"), 0);

        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("dst_key")
                .arg(1)
                .arg("missing_source")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(exists(conn, "dst_key"), 0);
        let _: i64 = redis::cmd("DEL").arg("zseta").query(conn).unwrap();
        assert!(
            redis::cmd("ZUNION")
                .arg(1)
                .arg("zseta")
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert!(
            redis::cmd("ZINTER")
                .arg(1)
                .arg("zseta")
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            redis::cmd("ZINTERCARD")
                .arg(1)
                .arg("zseta")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert!(
            redis::cmd("ZDIFF")
                .arg(1)
                .arg("zseta")
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );

        create_zset(conn, "zseta", &[("1", "a"), ("2", "b"), ("3", "c")]);
        let _: i64 = redis::cmd("DEL").arg("zsetb").query(conn).unwrap();
        assert_eq!(
            redis::cmd("ZUNION")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "2", "c", "3"])
        );
        assert!(
            redis::cmd("ZINTER")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            redis::cmd("ZINTERCARD")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert_eq!(
            redis::cmd("ZDIFF")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "2", "c", "3"])
        );

        create_zset(conn, "zseta", &[("1", "a"), ("2", "b"), ("3", "c")]);
        create_zset(conn, "zsetb", &[("1", "b"), ("2", "c"), ("3", "d")]);
        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "3", "d", "3", "c", "5"])
        );
        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("weights")
                .arg(2)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "2", "b", "7", "d", "9", "c", "12"])
        );
        assert_eq!(
            redis::cmd("ZUNION")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("weights")
                .arg(2)
                .arg(3)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "2", "b", "7", "d", "9", "c", "12"])
        );
        create_set(conn, "seta", &["a", "b", "c"]);
        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("seta")
                .arg("zsetb")
                .arg("weights")
                .arg(2)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "2", "b", "5", "c", "8", "d", "9"])
        );
        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("aggregate")
                .arg("min")
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "1", "c", "2", "d", "3"])
        );
        assert_eq!(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("aggregate")
                .arg("max")
                .query::<i64>(conn)
                .unwrap(),
            4
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1", "b", "2", "c", "3", "d", "3"])
        );
        assert_eq!(
            redis::cmd("ZINTERSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "3", "c", "5"])
        );
        assert_eq!(
            redis::cmd("ZINTER")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "3", "c", "5"])
        );
        assert_eq!(
            redis::cmd("ZINTERCARD")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZINTERCARD")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("limit")
                .arg(1)
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(
            redis::cmd("ZINTERSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("weights")
                .arg(2)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "7", "c", "12"])
        );
        create_set(conn, "seta", &["a", "b", "c"]);
        assert_eq!(
            redis::cmd("ZINTERSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("seta")
                .arg("zsetb")
                .arg("weights")
                .arg(2)
                .arg(3)
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "5", "c", "8"])
        );
        assert_eq!(
            redis::cmd("ZINTERSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("aggregate")
                .arg("min")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "1", "c", "2"])
        );
        assert_eq!(
            redis::cmd("ZINTERSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("aggregate")
                .arg("max")
                .query::<i64>(conn)
                .unwrap(),
            2
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["b", "2", "c", "3"])
        );
        create_zset(conn, "zsetinf1", &[("+inf", "key")]);
        create_zset(conn, "zsetinf2", &[("+inf", "key")]);
        let _: i64 = redis::cmd("ZUNIONSTORE")
            .arg("zsetinf3")
            .arg(2)
            .arg("zsetinf1")
            .arg("zsetinf2")
            .query(conn)
            .unwrap();
        let out = zscore(conn, "zsetinf3", "key").unwrap();
        assert!(out == "inf" || out == "+inf");
        assert_err_contains(
            redis::cmd("ZUNIONSTORE")
                .arg("zsetinf3")
                .arg(2)
                .arg("zsetinf1")
                .arg("zsetinf2")
                .arg("weights")
                .arg("nan")
                .arg("nan")
                .query::<i64>(conn),
            "weight",
        );
        assert_eq!(
            redis::cmd("ZDIFFSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1"])
        );
        assert_eq!(
            redis::cmd("ZDIFF")
                .arg(2)
                .arg("zseta")
                .arg("zsetb")
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1"])
        );
        create_set(conn, "seta", &["a", "b", "c"]);
        assert_eq!(
            redis::cmd("ZDIFFSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("seta")
                .arg("zsetb")
                .query::<i64>(conn)
                .unwrap(),
            1
        );
        assert_eq!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap(),
            expected_strings(&["a", "1"])
        );
        assert_eq!(
            redis::cmd("ZDIFFSTORE")
                .arg("zsetc")
                .arg(2)
                .arg("zseta")
                .arg("zseta")
                .query::<i64>(conn)
                .unwrap(),
            0
        );
        assert!(
            redis::cmd("ZRANGE")
                .arg("zsetc")
                .arg(0)
                .arg(-1)
                .arg("withscores")
                .query::<Vec<String>>(conn)
                .unwrap()
                .is_empty()
        );

        create_zset(conn, "zmscoretest", &[("10", "x"), ("20", "y")]);
        assert_eq!(
            redis::cmd("ZMSCORE")
                .arg("zmscoretest")
                .arg("x")
                .arg("y")
                .query::<Vec<Option<String>>>(conn)
                .unwrap(),
            vec![Some("10".into()), Some("20".into())]
        );
        let _: i64 = redis::cmd("DEL").arg("zmscoretest").query(conn).unwrap();
        assert_eq!(
            redis::cmd("ZMSCORE")
                .arg("zmscoretest")
                .arg("x")
                .arg("y")
                .query::<Vec<Option<String>>>(conn)
                .unwrap(),
            vec![None, None]
        );
        create_zset(conn, "zmscoretest", &[("10", "x")]);
        assert_eq!(
            redis::cmd("ZMSCORE")
                .arg("zmscoretest")
                .arg("x")
                .arg("y")
                .query::<Vec<Option<String>>>(conn)
                .unwrap(),
            vec![Some("10".into()), None]
        );
        assert_err_contains(
            redis::cmd("ZMSCORE")
                .arg("zmscoretest")
                .query::<Vec<Option<String>>>(conn),
            "wrong number",
        );
        assert_err_contains(
            redis::cmd("ZADD")
                .arg("myzset")
                .arg("")
                .arg("abc")
                .query::<i64>(conn),
            "float",
        );
        for (cmd, args) in [
            ("ZUNION", vec!["0", "key"]),
            ("ZINTER", vec!["0", "key"]),
            ("ZDIFF", vec!["0", "key"]),
            ("ZINTERCARD", vec!["0", "key"]),
        ] {
            assert_err_contains(
                redis::cmd(cmd).arg(args).query::<Value>(conn),
                "at least 1 input key",
            );
        }
    });
}

fn run_stressers(conn: &mut Connection, mode: EncodingMode) {
    with_zset_encoding(conn, mode, |conn| {
        flush(conn);
        let elements = match mode {
            EncodingMode::Listpack => 128,
            EncodingMode::Large => 200,
        };
        let mut rng = make_rng();

        let _: i64 = redis::cmd("DEL").arg("zscoretest").query(conn).unwrap();
        let mut aux = Vec::new();
        for i in 0..elements {
            let score: f64 = rng.r#gen();
            aux.push(score);
            let _: i64 = redis::cmd("ZADD")
                .arg("zscoretest")
                .arg(score)
                .arg(i)
                .query(conn)
                .unwrap();
        }
        assert_encoding_any(conn, mode.expected_encodings(), "zscoretest");
        for (i, expected) in aux.iter().enumerate().take(elements) {
            let got: f64 = zscore(conn, "zscoretest", &i.to_string())
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(got, *expected);
        }

        let mut auxarray = HashMap::<String, f64>::new();
        let _: i64 = redis::cmd("DEL").arg("myzset").query(conn).unwrap();
        for i in 0..elements {
            let score = rng.r#gen::<f64>();
            auxarray.insert(i.to_string(), score);
            let _: i64 = redis::cmd("ZADD")
                .arg("myzset")
                .arg(score)
                .arg(i)
                .query(conn)
                .unwrap();
            if rng.gen_bool(0.2) {
                let j = rng.gen_range(0..1000).to_string();
                let next_score = rng.r#gen::<f64>();
                auxarray.insert(j.clone(), next_score);
                let _: i64 = redis::cmd("ZADD")
                    .arg("myzset")
                    .arg(next_score)
                    .arg(j)
                    .query(conn)
                    .unwrap();
            }
        }
        let mut sorted: Vec<(f64, String)> = auxarray.into_iter().map(|(m, s)| (s, m)).collect();
        zlist_alike_sort(&mut sorted);
        let expected: Vec<String> = sorted.into_iter().map(|(_, m)| m).collect();
        let from_server: Vec<String> = redis::cmd("ZRANGE")
            .arg("myzset")
            .arg(0)
            .arg(-1)
            .query(conn)
            .unwrap();
        assert_eq!(from_server, expected);

        let _: i64 = redis::cmd("DEL").arg("zset").query(conn).unwrap();
        for i in 0..elements {
            let _: i64 = redis::cmd("ZADD")
                .arg("zset")
                .arg(rng.r#gen::<f64>())
                .arg(i)
                .query(conn)
                .unwrap();
        }
        for _ in 0..100 {
            let mut min = rng.r#gen::<f64>();
            let mut max = rng.r#gen::<f64>();
            if min > max {
                std::mem::swap(&mut min, &mut max);
            }
            let low: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg("-inf")
                .arg(min)
                .query(conn)
                .unwrap();
            let ok: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(min)
                .arg(max)
                .query(conn)
                .unwrap();
            let high: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg("zset")
                .arg(max)
                .arg("+inf")
                .query(conn)
                .unwrap();
            assert_eq!(
                redis::cmd("ZCOUNT")
                    .arg("zset")
                    .arg("-inf")
                    .arg(min)
                    .query::<i64>(conn)
                    .unwrap() as usize,
                low.len()
            );
            assert_eq!(
                redis::cmd("ZCOUNT")
                    .arg("zset")
                    .arg(min)
                    .arg(max)
                    .query::<i64>(conn)
                    .unwrap() as usize,
                ok.len()
            );
            assert_eq!(
                redis::cmd("ZCOUNT")
                    .arg("zset")
                    .arg(max)
                    .arg("+inf")
                    .query::<i64>(conn)
                    .unwrap() as usize,
                high.len()
            );
            for item in ok {
                let score: f64 = zscore(conn, "zset", &item).unwrap().parse().unwrap();
                assert!(score >= min && score <= max);
            }
        }

        let _: i64 = redis::cmd("DEL").arg("zset").query(conn).unwrap();
        let mut lexset = Vec::new();
        for _ in 0..elements {
            let e = rand_alpha(&mut rng, 0, 30);
            lexset.push(e.clone());
            let _: i64 = redis::cmd("ZADD")
                .arg("zset")
                .arg(0)
                .arg(&e)
                .query(conn)
                .unwrap();
        }
        lexset.sort();
        lexset.dedup();
        for _ in 0..100 {
            let min = rand_alpha(&mut rng, 0, 30);
            let max = rand_alpha(&mut rng, 0, 30);
            let min_inc = rng.gen_bool(0.5);
            let max_inc = rng.gen_bool(0.5);
            let cmin = format!("{}{}", if min_inc { "[" } else { "(" }, min);
            let cmax = format!("{}{}", if max_inc { "[" } else { "(" }, max);
            let output: Vec<String> = redis::cmd("ZRANGEBYLEX")
                .arg("zset")
                .arg(&cmin)
                .arg(&cmax)
                .query(conn)
                .unwrap();
            let expected: Vec<String> = lexset
                .iter()
                .filter(|e| {
                    let ge_min = if min_inc {
                        e.as_str() >= min.as_str()
                    } else {
                        e.as_str() > min.as_str()
                    };
                    let le_max = if max_inc {
                        e.as_str() <= max.as_str()
                    } else {
                        e.as_str() < max.as_str()
                    };
                    ge_min && le_max
                })
                .cloned()
                .collect();
            assert_eq!(output, expected);
        }

        let _: i64 = redis::cmd("DEL").arg("myzset").query(conn).unwrap();
        for j in 0..elements {
            let _: i64 = redis::cmd("ZADD")
                .arg("myzset")
                .arg(rng.r#gen::<f64>())
                .arg(format!("Element-{j}"))
                .query(conn)
                .unwrap();
            let victim = rng.gen_range(0..elements);
            let _: i64 = redis::cmd("ZREM")
                .arg("myzset")
                .arg(format!("Element-{victim}"))
                .query(conn)
                .unwrap();
        }
        let fwd: Vec<String> = redis::cmd("ZRANGE")
            .arg("myzset")
            .arg(0)
            .arg(-1)
            .query(conn)
            .unwrap();
        let rev: Vec<String> = redis::cmd("ZREVRANGE")
            .arg("myzset")
            .arg(0)
            .arg(-1)
            .query(conn)
            .unwrap();
        let rev_reversed: Vec<String> = rev.into_iter().rev().collect();
        assert_eq!(fwd, rev_reversed);

        let _: i64 = redis::cmd("DEL").arg("myzset").query(conn).unwrap();
        for k in 0..2000 {
            let i = k % elements;
            if rng.gen_bool(0.2) {
                let _: i64 = redis::cmd("ZREM").arg("myzset").arg(i).query(conn).unwrap();
            } else {
                let _: i64 = redis::cmd("ZADD")
                    .arg("myzset")
                    .arg(rng.r#gen::<f64>())
                    .arg(i)
                    .query(conn)
                    .unwrap();
            }
            let card: i64 = redis::cmd("ZCARD").arg("myzset").query(conn).unwrap();
            if card > 0 {
                let index = rng.gen_range(0..card as usize) as i64;
                let ele: Vec<String> = redis::cmd("ZRANGE")
                    .arg("myzset")
                    .arg(index)
                    .arg(index)
                    .query(conn)
                    .unwrap();
                let rank: i64 = redis::cmd("ZRANK")
                    .arg("myzset")
                    .arg(&ele[0])
                    .query(conn)
                    .unwrap();
                assert_eq!(rank, index);
            }
        }
    });
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_basics_listpack_compat_full() {
    let mut conn = must_connect();
    run_basics(&mut conn, EncodingMode::Listpack);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_basics_large_compat_full() {
    let mut conn = must_connect();
    run_basics(&mut conn, EncodingMode::Large);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_pop_wrongtype_and_illegal_args_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    for cmd in ["ZPOPMIN", "ZPOPMAX"] {
        assert_err_contains(
            redis::cmd(cmd).arg("foo").query::<Value>(&mut conn),
            "WRONGTYPE",
        );
        assert_err_contains(
            redis::cmd(cmd).arg("foo").arg(0).query::<Value>(&mut conn),
            "WRONGTYPE",
        );
    }
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("foo")
            .arg("min")
            .query::<Value>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("foo")
            .arg("max")
            .arg("count")
            .arg(200)
            .query::<Value>(&mut conn),
        "WRONGTYPE",
    );

    assert_err_contains(
        redis::cmd("ZMPOP").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("ZMPOP").arg(1).query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("myzset")
            .query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(0)
            .arg("myzset")
            .arg("MIN")
            .query::<Value>(&mut conn),
        "numkeys",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg("a")
            .arg("myzset")
            .arg("MIN")
            .query::<Value>(&mut conn),
        "numkeys",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(-1)
            .arg("myzset")
            .arg("MAX")
            .query::<Value>(&mut conn),
        "numkeys",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("myzset")
            .arg("bad_where")
            .query::<Value>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("myzset")
            .arg("MIN")
            .arg("COUNT")
            .query::<Value>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("myzset")
            .arg("MIN")
            .arg("COUNT")
            .arg(0)
            .query::<Value>(&mut conn),
        "count",
    );
    assert_err_contains(
        redis::cmd("ZMPOP")
            .arg(1)
            .arg("myzset")
            .arg("MIN")
            .arg("COUNT")
            .arg(-1)
            .query::<Value>(&mut conn),
        "count",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_blocking_edge_cases_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    let (tx, rx) = mpsc::channel();
    let url1 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url1).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("zset")
            .arg(0)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("zset")
        .arg(0)
        .arg("foo")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("DEL").arg("zset").query(&mut conn).unwrap();
    let _: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("zset")
        .arg(1)
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), expected_strings(&["zset", "bar", "1"]));

    let (tx, rx) = mpsc::channel();
    let url2 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url2).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("z1")
            .arg("z2")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: i64 = redis::cmd("ZADD")
        .arg("z1")
        .arg(0)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), expected_strings(&["z1", "a", "0"]));

    let (tx, rx) = mpsc::channel();
    let url3 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url3).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Value = redis::cmd("BZMPOP")
            .arg(0)
            .arg(2)
            .arg("myzset")
            .arg("myzset2")
            .arg("MIN")
            .arg("COUNT")
            .arg(1)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset2")
        .arg(1)
        .arg("b")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        zmpop_value(rx.recv().unwrap()),
        ("myzset2".into(), vec![("b".into(), "1".into())])
    );

    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let (tx3, rx3) = mpsc::channel();
    let (tx4, rx4) = mpsc::channel();
    for (sender, args) in [
        (
            tx1,
            vec!["0", "2", "myzsetf", "myzsetf2", "min", "count", "1"],
        ),
        (
            tx2,
            vec!["0", "2", "myzsetf", "myzsetf2", "max", "count", "10"],
        ),
        (
            tx3,
            vec!["0", "2", "myzsetf", "myzsetf2", "min", "count", "10"],
        ),
        (
            tx4,
            vec!["0", "2", "myzsetf", "myzsetf2", "max", "count", "1"],
        ),
    ] {
        let urlx = url.clone();
        thread::spawn(move || {
            let client = redis::Client::open(urlx).unwrap();
            let mut bg = client.get_connection().unwrap();
            let value: Value = redis::cmd("BZMPOP").arg(args).query(&mut bg).unwrap();
            sender.send(value).unwrap();
        });
    }
    thread::sleep(Duration::from_millis(100));
    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("myzsetf")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .arg(4)
        .arg("d")
        .arg(5)
        .arg("e")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("myzsetf2")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .arg(4)
        .arg("d")
        .arg(5)
        .arg("e")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(
        zmpop_value(rx1.recv().unwrap()),
        ("myzsetf".into(), vec![("a".into(), "1".into())])
    );
    assert_eq!(
        zmpop_value(rx2.recv().unwrap()),
        (
            "myzsetf".into(),
            vec![
                ("e".into(), "5".into()),
                ("d".into(), "4".into()),
                ("c".into(), "3".into()),
                ("b".into(), "2".into())
            ]
        )
    );
    assert_eq!(
        zmpop_value(rx3.recv().unwrap()),
        (
            "myzsetf2".into(),
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into()),
                ("d".into(), "4".into()),
                ("e".into(), "5".into())
            ]
        )
    );
    let _: i64 = redis::cmd("ZADD")
        .arg("myzsetf2")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        zmpop_value(rx4.recv().unwrap()),
        ("myzsetf2".into(), vec![("c".into(), "3".into())])
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_resp_and_misc_regressions_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: Value = redis::cmd("HELLO").arg(3).query(&mut conn).unwrap();
    create_zset(
        &mut conn,
        "z1",
        &[("0", "a"), ("1", "b"), ("2", "c"), ("3", "d")],
    );
    let resp: Value = redis::cmd("ZPOPMIN")
        .arg("z1")
        .arg(2)
        .query(&mut conn)
        .unwrap();
    match resp {
        Value::Array(items) => assert!(!items.is_empty()),
        other => panic!("expected array, got {other:?}"),
    }
    let resp: Value = redis::cmd("ZRANDMEMBER")
        .arg("z1")
        .arg(2)
        .arg("WITHSCORES")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(resp, Value::Array(_)));
    let _: Value = redis::cmd("HELLO").arg(2).query(&mut conn).unwrap();

    create_zset(
        &mut conn,
        "zz",
        &[(
            "179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.00000000000000000",
            "dblmax",
        )],
    );
    assert_encoding_any(&mut conn, EncodingMode::Listpack.expected_encodings(), "zz");
    let dbl = zscore(&mut conn, "zz", "dblmax").unwrap();
    assert!(dbl.contains("e+") || dbl.contains("1797693"));

    create_zset(&mut conn, "z", &[("-inf", "neginf")]);
    let _: i64 = redis::cmd("ZUNIONSTORE")
        .arg("out")
        .arg(1)
        .arg("z")
        .arg("weights")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("out")
            .arg(0)
            .arg(-1)
            .arg("withscores")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["neginf", "0"])
    );

    create_set(&mut conn, "one", &["100", "101", "102", "103"]);
    create_set(&mut conn, "two", &["100", "200", "201", "202"]);
    create_zset(
        &mut conn,
        "three",
        &[
            ("1", "500"),
            ("1", "501"),
            ("1", "502"),
            ("1", "503"),
            ("1", "100"),
        ],
    );
    let _: i64 = redis::cmd("ZINTERSTORE")
        .arg("to_here")
        .arg(3)
        .arg("one")
        .arg("two")
        .arg("three")
        .arg("WEIGHTS")
        .arg(0)
        .arg(0)
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("to_here")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["100"])
    );

    create_zset(&mut conn, "onez", &[]);
    create_zset(&mut conn, "twoz", &[]);
    let mut rng = make_rng();
    for _ in 0..1000 {
        let _: i64 = redis::cmd("ZADD")
            .arg("onez")
            .arg(rng.r#gen::<f64>())
            .arg(rng.gen_range(0..1000))
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("twoz")
            .arg(rng.r#gen::<f64>())
            .arg(rng.gen_range(0..1000))
            .query(&mut conn)
            .unwrap();
    }
    let _: i64 = redis::cmd("ZUNIONSTORE")
        .arg("dest")
        .arg(2)
        .arg("onez")
        .arg("twoz")
        .query(&mut conn)
        .unwrap();
    let pairs = as_pairs(
        redis::cmd("ZRANGE")
            .arg("dest")
            .arg(0)
            .arg(-1)
            .arg("withscores")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
    );
    assert_sorted_pairs(&pairs);

    for cmd in ["ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"] {
        assert_err_contains(
            redis::cmd(cmd)
                .arg("foo")
                .arg(2)
                .arg("zsetd")
                .arg("zsetf")
                .arg("withscores")
                .query::<Value>(&mut conn),
            "syntax",
        );
    }

    assert_err_contains(
        redis::cmd("BZPOPMIN")
            .arg("z")
            .arg(-1)
            .query::<Value>(&mut conn),
        "negative",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_range_store_and_random_compat_full() {
    let mut conn = must_connect();
    flush(&mut conn);
    create_zset(
        &mut conn,
        "z1",
        &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")],
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .query::<i64>(&mut conn)
            .unwrap(),
        4
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z2")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["a", "1", "b", "2", "c", "3", "d", "4"])
    );
    let _: Value = redis::cmd("HELLO").arg(3).query(&mut conn).unwrap();
    let resp3: Value = redis::cmd("ZRANGE")
        .arg("z2")
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(resp3, Value::Array(_)));
    let _: Value = redis::cmd("HELLO").arg(2).query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(1)
            .arg(2)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z2")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["b", "2", "c", "3"])
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z3")
            .arg("z1")
            .arg("[b")
            .arg("[c")
            .arg("BYLEX")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z3")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["b", "2", "c", "3"])
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z4")
            .arg("z1")
            .arg(1)
            .arg(2)
            .arg("BYSCORE")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z4")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["a", "1", "b", "2"])
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z5")
            .arg("z1")
            .arg(5)
            .arg(0)
            .arg("BYSCORE")
            .arg("REV")
            .arg("LIMIT")
            .arg(0)
            .arg(2)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z5")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["c", "3", "d", "4"])
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z1")
            .arg(5)
            .arg(0)
            .arg("BYSCORE")
            .arg("REV")
            .arg("LIMIT")
            .arg(0)
            .arg(2)
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["d", "4", "c", "3"])
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("missing")
            .arg(0)
            .arg(-1)
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(exists(&mut conn, "z2"), 0);
    let _: i64 = redis::cmd("ZADD")
        .arg("z2")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("foo")
            .arg(0)
            .arg(-1)
            .query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("z2")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["a"])
    );
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(5)
            .arg(6)
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(exists(&mut conn, "z2"), 0);
    assert_err_contains(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("LIMIT")
            .arg(1)
            .arg(2)
            .query::<i64>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query::<i64>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZRANGE")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("LIMIT")
            .arg(1)
            .arg(2)
            .query::<Vec<String>>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZRANGE")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("BYLEX")
            .arg("WITHSCORES")
            .query::<Vec<String>>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZREVRANGE")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("BYSCORE")
            .query::<Vec<String>>(&mut conn),
        "syntax",
    );
    assert_err_contains(
        redis::cmd("ZRANGEBYSCORE")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .arg("REV")
            .query::<Vec<String>>(&mut conn),
        "syntax",
    );

    let original = config_get_i64(&mut conn, "zset-max-listpack-value");
    config_set(&mut conn, "zset-max-listpack-value", 10);
    create_zset(&mut conn, "myzset", &[("1", "a"), ("2", "b"), ("3", "c")]);
    assert_encoding_any(&mut conn, &["listpack"], "myzset");
    for _ in 0..100 {
        let field: String = redis::cmd("ZRANDMEMBER")
            .arg("myzset")
            .query(&mut conn)
            .unwrap();
        assert!(["a", "b", "c"].contains(&field.as_str()));
    }
    config_set(&mut conn, "zset-max-listpack-value", original);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_stressers_listpack_compat_full() {
    let mut conn = must_connect();
    run_stressers(&mut conn, EncodingMode::Listpack);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_stressers_large_compat_full() {
    let mut conn = must_connect();
    run_stressers(&mut conn, EncodingMode::Large);
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_set_and_threshold_regressions_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    for set_type in ["intset", "listpack", "hashtable"] {
        config_set(&mut conn, "set-max-intset-entries", 512);
        config_set(&mut conn, "set-max-listpack-entries", 128);
        config_set(&mut conn, "zset-max-listpack-entries", 128);
        let _: i64 = redis::cmd("DEL")
            .arg("set_small")
            .arg("set_big")
            .arg("zset_small")
            .arg("zset_big")
            .arg("zset_dest")
            .query(&mut conn)
            .unwrap();
        match set_type {
            "intset" => {
                create_set(&mut conn, "set_small", &["1", "2", "3"]);
                create_set(&mut conn, "set_big", &["1", "2", "3", "4", "5"]);
            }
            "listpack" => {
                create_set(&mut conn, "set_small", &["a", "1", "2", "3"]);
                create_set(&mut conn, "set_big", &["a", "1", "2", "3", "4", "5"]);
                let _: i64 = redis::cmd("SREM")
                    .arg("set_small")
                    .arg("a")
                    .query(&mut conn)
                    .unwrap();
                let _: i64 = redis::cmd("SREM")
                    .arg("set_big")
                    .arg("a")
                    .query(&mut conn)
                    .unwrap();
            }
            _ => {
                config_set(&mut conn, "set-max-intset-entries", 0);
                config_set(&mut conn, "set-max-listpack-entries", 0);
                create_set(&mut conn, "set_small", &["1", "2", "3"]);
                create_set(&mut conn, "set_big", &["1", "2", "3", "4", "5"]);
            }
        }
        for zset_mode in [EncodingMode::Listpack, EncodingMode::Large] {
            config_set(
                &mut conn,
                "zset-max-listpack-entries",
                zset_mode.max_entries(),
            );
            create_zset(
                &mut conn,
                "zset_small",
                &[("1", "1"), ("2", "2"), ("3", "3")],
            );
            create_zset(
                &mut conn,
                "zset_big",
                &[("1", "1"), ("2", "2"), ("3", "3"), ("4", "4"), ("5", "5")],
            );
            for (small_or_big, set_key, zset_key) in [
                ("small", "set_small", "zset_big"),
                ("big", "set_big", "zset_small"),
            ] {
                let union: Vec<String> = redis::cmd("ZUNION")
                    .arg(2)
                    .arg(set_key)
                    .arg(zset_key)
                    .query(&mut conn)
                    .unwrap();
                let mut union_sorted = union.clone();
                union_sorted.sort();
                assert_eq!(union_sorted, expected_strings(&["1", "2", "3", "4", "5"]));
                let _: i64 = redis::cmd("ZUNIONSTORE")
                    .arg("zset_dest")
                    .arg(2)
                    .arg(set_key)
                    .arg(zset_key)
                    .query(&mut conn)
                    .unwrap();
                let inter: Vec<String> = redis::cmd("ZINTER")
                    .arg(2)
                    .arg(set_key)
                    .arg(zset_key)
                    .query(&mut conn)
                    .unwrap();
                let mut inter_sorted = inter.clone();
                inter_sorted.sort();
                assert_eq!(inter_sorted, expected_strings(&["1", "2", "3"]));
                assert_eq!(
                    redis::cmd("ZINTERCARD")
                        .arg(2)
                        .arg(set_key)
                        .arg(zset_key)
                        .query::<i64>(&mut conn)
                        .unwrap(),
                    3
                );
                if small_or_big == "small" {
                    assert!(
                        redis::cmd("ZDIFF")
                            .arg(2)
                            .arg(set_key)
                            .arg(zset_key)
                            .query::<Vec<String>>(&mut conn)
                            .unwrap()
                            .is_empty()
                    );
                } else {
                    let diff: Vec<String> = redis::cmd("ZDIFF")
                        .arg(2)
                        .arg(set_key)
                        .arg(zset_key)
                        .query(&mut conn)
                        .unwrap();
                    let mut diff_sorted = diff.clone();
                    diff_sorted.sort();
                    assert_eq!(diff_sorted, expected_strings(&["4", "5"]));
                }
            }
        }
    }

    for mode in [EncodingMode::Listpack, EncodingMode::Large] {
        with_zset_encoding(&mut conn, mode, |conn| {
            for variant in ["single", "multiple", "single_multiple"] {
                let original = config_get_i64(conn, "zset-max-listpack-entries");
                config_set(conn, "zset-max-listpack-entries", 64);
                let _: i64 = redis::cmd("DEL").arg("overflow").query(conn).unwrap();
                match variant {
                    "single" => {
                        for i in 0..64 {
                            let _: i64 = redis::cmd("ZADD")
                                .arg("overflow")
                                .arg(i)
                                .arg(i)
                                .query(conn)
                                .unwrap();
                        }
                    }
                    "multiple" => {
                        let mut cmd = redis::cmd("ZADD");
                        cmd.arg("overflow");
                        for i in 0..64 {
                            cmd.arg(i).arg(i);
                        }
                        let _: i64 = cmd.query(conn).unwrap();
                    }
                    _ => {
                        let _: i64 = redis::cmd("ZADD")
                            .arg("overflow")
                            .arg(1)
                            .arg(1)
                            .query(conn)
                            .unwrap();
                        let mut cmd = redis::cmd("ZADD");
                        cmd.arg("overflow");
                        for i in 0..64 {
                            cmd.arg(i).arg(i);
                        }
                        let _: i64 = cmd.query(conn).unwrap();
                    }
                }
                assert_eq!(
                    redis::cmd("ZCARD")
                        .arg("overflow")
                        .query::<i64>(conn)
                        .unwrap(),
                    64
                );
                let _: i64 = redis::cmd("ZADD")
                    .arg("overflow")
                    .arg(1)
                    .arg("b")
                    .query(conn)
                    .unwrap();
                assert_encoding_any(conn, EncodingMode::Large.expected_encodings(), "overflow");
                config_set(conn, "zset-max-listpack-entries", original);
            }
        });
    }
}

#[test]
#[ignore = "requires running Senko instance with DEBUG enabled"]
fn zset_debug_reload_and_expiry_reprocess_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let mut rng = make_rng();
    let _: i64 = redis::cmd("DEL")
        .arg("zscoretest")
        .query(&mut conn)
        .unwrap();
    let mut aux = Vec::new();
    for i in 0..64 {
        let score = rng.r#gen::<f64>();
        aux.push(score);
        let _: i64 = redis::cmd("ZADD")
            .arg("zscoretest")
            .arg(score)
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    let _: Value = redis::cmd("DEBUG").arg("RELOAD").query(&mut conn).unwrap();
    for (i, expected) in aux.iter().enumerate().take(64) {
        let got: f64 = zscore(&mut conn, "zscoretest", &i.to_string())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(got, *expected);
    }

    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let (tx, rx) = mpsc::channel();
    let url2 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url2).unwrap();
        let mut bg = client.get_connection().unwrap();
        let started = Instant::now();
        let value: Option<Vec<String>> = redis::cmd("BZPOPMIN")
            .arg("zset_expire")
            .arg(1)
            .query(&mut bg)
            .unwrap();
        tx.send((started.elapsed(), value)).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: Value = redis::cmd("DEBUG")
        .arg("SET-ACTIVE-EXPIRE")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("zset_expire")
        .arg(1)
        .arg("one")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("PEXPIRE")
        .arg("zset_expire")
        .arg(100)
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("DEBUG")
        .arg("SLEEP")
        .arg(0.2)
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    let (elapsed, value) = rx.recv().unwrap();
    assert!(value.is_none());
    assert!(elapsed >= Duration::from_millis(900));
    let _: Value = redis::cmd("DEBUG")
        .arg("SET-ACTIVE-EXPIRE")
        .arg(1)
        .query(&mut conn)
        .unwrap();
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_more_blocking_and_random_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    let _: String = redis::cmd("SET")
        .arg("foo_block")
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("BZPOPMIN")
            .arg("foo_block")
            .arg(1)
            .query::<Value>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("BZMPOP")
            .arg(1)
            .arg(1)
            .arg("foo_block")
            .arg("min")
            .query::<Value>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("BZMPOP")
            .arg(1)
            .arg(1)
            .arg("myzset")
            .arg("MIN")
            .arg("COUNT")
            .arg(0)
            .query::<Value>(&mut conn),
        "count",
    );

    let (tx, rx) = mpsc::channel();
    let url1 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url1).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("multi_pop")
            .arg(0)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("multi_pop")
        .arg(0)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("multi_pop")
        .arg(1)
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("ZADD")
        .arg("multi_pop")
        .arg(2)
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(
        rx.recv().unwrap(),
        expected_strings(&["multi_pop", "a", "0"])
    );

    let (tx, rx) = mpsc::channel();
    let url2 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url2).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("varz")
            .arg(0)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: i64 = redis::cmd("ZADD")
        .arg("varz")
        .arg(-1)
        .arg("foo")
        .arg(1)
        .arg("bar")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), expected_strings(&["varz", "foo", "-1"]));
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("varz")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        expected_strings(&["bar"])
    );

    let (tx, rx) = mpsc::channel();
    let url3 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url3).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BZPOPMIN")
            .arg("zero_timeout")
            .arg(0)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(1000));
    let _: i64 = redis::cmd("ZADD")
        .arg("zero_timeout")
        .arg(0)
        .arg("foo")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        rx.recv().unwrap(),
        expected_strings(&["zero_timeout", "foo", "0"])
    );

    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let url4 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url4).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Value = redis::cmd("BZMPOP")
            .arg(0)
            .arg(1)
            .arg("myzset")
            .arg("min")
            .arg("count")
            .arg(10)
            .query(&mut bg)
            .unwrap();
        tx1.send(value).unwrap();
    });
    let url5 = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(url5).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Value = redis::cmd("BZMPOP")
            .arg(0)
            .arg(2)
            .arg("myzset2")
            .arg("myzset3")
            .arg("max")
            .arg("count")
            .arg(10)
            .query(&mut bg)
            .unwrap();
        tx2.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let _: i64 = redis::cmd("ZADD")
        .arg("0")
        .arg(100)
        .arg("timeout_value")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("1")
        .arg(200)
        .arg("numkeys_value")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("min")
        .arg(300)
        .arg("min_token")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("max")
        .arg(400)
        .arg("max_token")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("count")
        .arg(500)
        .arg("count_token")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("10")
        .arg(600)
        .arg("count_value")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1)
        .arg("zset")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset3")
        .arg(1)
        .arg("zset3")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        zmpop_value(rx1.recv().unwrap()),
        ("myzset".into(), vec![("zset".into(), "1".into())])
    );
    assert_eq!(
        zmpop_value(rx2.recv().unwrap()),
        ("myzset3".into(), vec![("zset3".into(), "1".into())])
    );

    create_zset(&mut conn, "rand_count", &[("0", "a")]);
    assert!(
        redis::cmd("ZRANDMEMBER")
            .arg("rand_count")
            .arg(0)
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .is_empty()
    );
    assert!(
        redis::cmd("ZRANDMEMBER")
            .arg("nonexisting_key")
            .arg(100)
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .is_empty()
    );
    assert_err_contains(
        redis::cmd("ZRANDMEMBER")
            .arg("rand_count")
            .arg("-9223372036854775808")
            .query::<Vec<String>>(&mut conn),
        "out of range",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn zset_listpack_threshold_store_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let original = config_get_i64(&mut conn, "zset-max-listpack-entries");
    config_set(&mut conn, "zset-max-listpack-entries", 0);
    create_zset(&mut conn, "z1", &[("1", "a")]);
    assert_encoding_any(&mut conn, EncodingMode::Large.expected_encodings(), "z1");
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .arg(-1)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_encoding_any(&mut conn, EncodingMode::Large.expected_encodings(), "z2");
    config_set(&mut conn, "zset-max-listpack-entries", 1);
    create_zset(&mut conn, "z1", &[("1", "a"), ("2", "b")]);
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z2")
            .arg("z1")
            .arg(0)
            .arg(0)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_encoding_any(&mut conn, EncodingMode::Listpack.expected_encodings(), "z2");
    assert_eq!(
        redis::cmd("ZRANGESTORE")
            .arg("z3")
            .arg("z1")
            .arg(0)
            .arg(1)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_encoding_any(&mut conn, EncodingMode::Large.expected_encodings(), "z3");
    config_set(&mut conn, "zset-max-listpack-entries", original);
}
