#![allow(clippy::too_many_lines)]

use std::collections::{HashMap, HashSet};

use redis::{Commands, Connection, RedisResult, Value};

fn connect() -> Option<Connection> {
    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let client = redis::Client::open(url).ok()?;
    client.get_connection().ok()
}

fn must_connect() -> Connection {
    match connect() {
        Some(mut conn) => {
            let _: RedisResult<String> = redis::cmd("PING").query(&mut conn);
            conn
        }
        None => panic!("compat test requires running senko at senko_REDIS_URL"),
    }
}

fn flush(conn: &mut Connection) {
    let _: () = redis::cmd("FLUSHDB").query(conn).unwrap();
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

fn sscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
) -> Vec<String> {
    let mut cursor: u64 = 0;
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("SSCAN");
        cmd.arg(key).arg(cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        let (next, page): (u64, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    out
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

fn members(conn: &mut Connection, key: &str) -> HashSet<String> {
    redis::cmd("SMEMBERS")
        .arg(key)
        .query::<Vec<String>>(conn)
        .unwrap()
        .into_iter()
        .collect()
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

fn build_generated_sets(conn: &mut Connection, integer_mode: bool) {
    for key in ["set1", "set2", "set3", "set4", "set5", "setres"] {
        let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    }

    for i in 0..200 {
        let value = i.to_string();
        let _: i64 = redis::cmd("SADD")
            .arg("set1")
            .arg(&value)
            .query(conn)
            .unwrap();
        let shifted = (i + 195).to_string();
        let _: i64 = redis::cmd("SADD")
            .arg("set2")
            .arg(&shifted)
            .query(conn)
            .unwrap();
    }
    for value in ["199", "195", "1000", "2000"] {
        let _: i64 = redis::cmd("SADD")
            .arg("set3")
            .arg(value)
            .query(conn)
            .unwrap();
    }
    for i in 5..200 {
        let value = i.to_string();
        let _: i64 = redis::cmd("SADD")
            .arg("set4")
            .arg(&value)
            .query(conn)
            .unwrap();
    }
    let _: i64 = redis::cmd("SADD").arg("set5").arg("0").query(conn).unwrap();

    let marker = if integer_mode { "200" } else { "foo" };
    for key in ["set1", "set2", "set3", "set4", "set5"] {
        let _: i64 = redis::cmd("SADD").arg(key).arg(marker).query(conn).unwrap();
    }
}

#[test]
#[ignore = "requires running senko instance"]
fn set_basics_by_encoding_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("set-max-listpack-entries")
        .arg(128)
        .query(&mut conn)
        .unwrap();

    create_set(&mut conn, "lp", &["foo"]);
    assert_encoding(&mut conn, "listpack", "lp");
    assert_eq!(
        redis::cmd("SADD")
            .arg("lp")
            .arg("bar")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SADD")
            .arg("lp")
            .arg("bar")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("SCARD")
            .arg("lp")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("lp")
            .arg("foo")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SMISMEMBER")
            .arg("lp")
            .arg("foo")
            .arg("bar")
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![1, 1]
    );
    assert_eq!(
        members(&mut conn, "lp"),
        HashSet::from(["foo".to_string(), "bar".to_string()])
    );

    create_set(&mut conn, "int", &["17"]);
    assert_encoding(&mut conn, "intset", "int");
    assert_eq!(
        redis::cmd("SADD")
            .arg("int")
            .arg("16")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SMEMBERS")
            .arg("int")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from(["16".to_string(), "17".to_string()])
    );

    create_set(&mut conn, "ht", &["foo"]);
    for i in 0..130 {
        let member = format!("i{i:03}");
        let _: i64 = redis::cmd("SADD")
            .arg("ht")
            .arg(member)
            .query(&mut conn)
            .unwrap();
    }
    assert_encoding(&mut conn, "hashtable", "ht");
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("ht")
            .arg("foo")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn set_upgrades_and_thresholds_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(&mut conn, "small-int", &["1", "2", "3"]);
    assert_encoding(&mut conn, "intset", "small-int");
    let _: i64 = redis::cmd("SADD")
        .arg("small-int")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_encoding(&mut conn, "listpack", "small-int");

    create_set(&mut conn, "large-int", &["0"]);
    for i in 1..130 {
        let _: i64 = redis::cmd("SADD")
            .arg("large-int")
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    assert_encoding(&mut conn, "intset", "large-int");
    let _: i64 = redis::cmd("SADD")
        .arg("large-int")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_encoding(&mut conn, "hashtable", "large-int");

    create_set(&mut conn, "big-int", &["213244124402402314402033402"]);
    assert_encoding(&mut conn, "listpack", "big-int");
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("big-int")
            .arg("213244124402402314402033402")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    create_set(&mut conn, "overflow-intset", &[]);
    for i in 0..512 {
        let _: i64 = redis::cmd("SADD")
            .arg("overflow-intset")
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    assert_encoding(&mut conn, "intset", "overflow-intset");
    assert_eq!(
        redis::cmd("SADD")
            .arg("overflow-intset")
            .arg(512)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_encoding(&mut conn, "hashtable", "overflow-intset");
}

#[test]
#[ignore = "requires running senko instance"]
fn set_remove_and_variadic_sadd_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(&mut conn, "myset", &["a", "b", "c", "d"]);
    assert_eq!(
        redis::cmd("SREM")
            .arg("myset")
            .arg("k")
            .arg("k")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("SREM")
            .arg("myset")
            .arg("b")
            .arg("d")
            .arg("x")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        members(&mut conn, "myset"),
        HashSet::from(["a".to_string(), "c".to_string()])
    );

    create_set(&mut conn, "destroy", &["1", "2", "3"]);
    assert_eq!(
        redis::cmd("SREM")
            .arg("destroy")
            .arg("1")
            .arg("2")
            .arg("3")
            .arg("4")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("destroy")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let _: i64 = redis::cmd("DEL").arg("variadic").query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("SADD")
            .arg("variadic")
            .arg("a")
            .arg("b")
            .arg("c")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    assert_eq!(
        redis::cmd("SADD")
            .arg("variadic")
            .arg("A")
            .arg("a")
            .arg("b")
            .arg("c")
            .arg("B")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn set_non_set_and_missing_key_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("LPUSH")
        .arg("mylist")
        .arg("foo")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("SMISMEMBER")
            .arg("mylist")
            .arg("bar")
            .query::<Vec<i64>>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SMEMBERS")
            .arg("mylist")
            .query::<Vec<String>>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SCARD").arg("mylist").query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SADD")
            .arg("mylist")
            .arg("bar")
            .query::<i64>(&mut conn),
        "WRONGTYPE",
    );

    assert_eq!(
        redis::cmd("SMISMEMBER")
            .arg("missing")
            .arg("foo")
            .arg("bar")
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![0, 0]
    );
    assert!(redis::cmd("SMEMBERS")
        .arg("missing")
        .query::<Vec<String>>(&mut conn)
        .unwrap()
        .is_empty());
    assert_eq!(
        redis::cmd("SCARD")
            .arg("missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn sintercard_argument_and_wrongtype_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    assert_err_contains(
        redis::cmd("SINTERCARD").query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD").arg(1).query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(0)
            .arg("myset")
            .query::<i64>(&mut conn),
        "numkeys",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(2)
            .arg("only-one")
            .query::<i64>(&mut conn),
        "Number of keys",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(1)
            .arg("myset")
            .arg("bar_arg")
            .query::<i64>(&mut conn),
        "syntax error",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(1)
            .arg("myset")
            .arg("LIMIT")
            .query::<i64>(&mut conn),
        "syntax error",
    );
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(1)
            .arg("myset")
            .arg("LIMIT")
            .arg(-1)
            .query::<i64>(&mut conn),
        "LIMIT",
    );

    let _: String = redis::cmd("SET")
        .arg("key1")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("SINTERCARD")
            .arg(1)
            .arg("key1")
            .query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_eq!(
        redis::cmd("SINTERCARD")
            .arg(1)
            .arg("non-existing-key")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn generated_set_algebra_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    build_generated_sets(&mut conn, false);
    assert_encoding(&mut conn, "listpack", "set3");
    assert_encoding(&mut conn, "hashtable", "set1");
    assert_eq!(members(&mut conn, "set1").len(), 201);

    assert_eq!(
        redis::cmd("SINTER")
            .arg("set1")
            .arg("set2")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            "195".to_string(),
            "196".to_string(),
            "197".to_string(),
            "198".to_string(),
            "199".to_string(),
            "foo".to_string(),
        ])
    );
    assert_eq!(
        redis::cmd("SINTERCARD")
            .arg(2)
            .arg("set1")
            .arg("set2")
            .arg("LIMIT")
            .arg(3)
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    assert_eq!(
        redis::cmd("SINTERSTORE")
            .arg("setres")
            .arg("set1")
            .arg("set2")
            .query::<i64>(&mut conn)
            .unwrap(),
        6
    );
    assert_eq!(
        redis::cmd("SDIFF")
            .arg("set1")
            .arg("set4")
            .arg("set5")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string()
        ])
    );
    assert_eq!(
        redis::cmd("SUNIONSTORE")
            .arg("setres")
            .arg("set1")
            .arg("set2")
            .query::<i64>(&mut conn)
            .unwrap(),
        members(&mut conn, "setres").len() as i64
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn integer_result_encoding_store_variants_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(
        &mut conn,
        "set1",
        &["a", "b", "c", "1", "3", "6", "x", "y", "z"],
    );
    create_set(
        &mut conn,
        "set2",
        &["e", "f", "g", "1", "2", "3", "u", "v", "w"],
    );
    assert_encoding(&mut conn, "listpack", "set1");
    assert_encoding(&mut conn, "listpack", "set2");
    assert_eq!(
        redis::cmd("SINTERSTORE")
            .arg("setres")
            .arg("set1")
            .arg("set2")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        members(&mut conn, "setres"),
        HashSet::from(["1".to_string(), "3".to_string()])
    );
    assert_encoding(&mut conn, "intset", "setres");
}

#[test]
#[ignore = "requires running senko instance"]
fn set_random_sampling_and_histogram_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(
        &mut conn,
        "myset",
        &["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
    );
    assert_eq!(
        redis::cmd("SRANDMEMBER")
            .arg("myset")
            .arg(0)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        redis::cmd("SRANDMEMBER")
            .arg("nonexisting_key")
            .arg(100)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        Vec::<String>::new()
    );

    let positive: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .arg(5)
        .query(&mut conn)
        .unwrap();
    assert_eq!(positive.len(), 5);
    assert_eq!(positive.iter().cloned().collect::<HashSet<_>>().len(), 5);

    let negative: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .arg(-100)
        .query(&mut conn)
        .unwrap();
    assert_eq!(negative.len(), 100);
    let chi = chi_square_uniform(
        &(0..10_000)
            .map(|_| {
                redis::cmd("SRANDMEMBER")
                    .arg("myset")
                    .query::<String>(&mut conn)
                    .unwrap()
            })
            .collect::<Vec<_>>(),
        &["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
    );
    assert!(chi < 27.88, "chi-square too high: {chi}");
}

#[test]
#[ignore = "requires running senko instance"]
fn set_pop_and_move_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(&mut conn, "spopset", &["1", "2", "3"]);
    let popped: HashSet<String> = (0..3)
        .map(|_| {
            redis::cmd("SPOP")
                .arg("spopset")
                .query::<String>(&mut conn)
                .unwrap()
        })
        .collect();
    assert_eq!(
        popped,
        HashSet::from(["1".to_string(), "2".to_string(), "3".to_string()])
    );
    assert_eq!(
        redis::cmd("SCARD")
            .arg("spopset")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    create_set(&mut conn, "spopmany", &["a", "b", "c", "d"]);
    let popped_many: Vec<String> = redis::cmd("SPOP")
        .arg("spopmany")
        .arg(10)
        .query(&mut conn)
        .unwrap();
    assert_eq!(popped_many.len(), 4);
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("spopmany")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    create_set(&mut conn, "src", &["1", "a", "b"]);
    create_set(&mut conn, "dst", &["2", "3", "4"]);
    assert_eq!(
        redis::cmd("SMOVE")
            .arg("src")
            .arg("dst")
            .arg("a")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("src")
            .arg("a")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("SISMEMBER")
            .arg("dst")
            .arg("a")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SMOVE")
            .arg("src")
            .arg("src")
            .arg("1")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("SMOVE")
            .arg("src")
            .arg("dst")
            .arg("missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn set_scan_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    create_set(&mut conn, "intscan", &["1", "2", "3"]);
    let (cursor, page): (String, Vec<String>) = redis::cmd("SSCAN")
        .arg("intscan")
        .arg(0)
        .arg("COUNT")
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(cursor, "0");
    assert_eq!(
        page.into_iter().collect::<HashSet<_>>(),
        HashSet::from(["1".to_string(), "2".to_string(), "3".to_string()])
    );

    create_set(&mut conn, "htscan", &["foo"]);
    for i in 0..200 {
        let member = format!("i{i:03}");
        let _: i64 = redis::cmd("SADD")
            .arg("htscan")
            .arg(&member)
            .query(&mut conn)
            .unwrap();
    }
    assert_encoding(&mut conn, "hashtable", "htscan");
    let all = sscan_collect(&mut conn, "htscan", None, Some(7));
    assert_eq!(all.len(), 201);
    let filtered = sscan_collect(&mut conn, "htscan", Some("i00*"), Some(3));
    assert!(filtered.iter().all(|value| value.starts_with("i00")));
}

#[test]
#[ignore = "requires running senko instance"]
fn set_error_cases_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("s")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    for cmd in [
        "SADD",
        "SREM",
        "SCARD",
        "SISMEMBER",
        "SMISMEMBER",
        "SMEMBERS",
        "SRANDMEMBER",
        "SPOP",
        "SMOVE",
        "SDIFF",
        "SDIFFSTORE",
        "SINTER",
        "SINTERSTORE",
        "SINTERCARD",
        "SUNION",
        "SUNIONSTORE",
        "SSCAN",
    ] {
        let err = redis::cmd(cmd).query::<Value>(&mut conn).unwrap_err();
        assert!(
            err.to_string().contains("wrong number of arguments"),
            "{cmd}: {err}"
        );
    }

    assert_err_contains(
        redis::cmd("SISMEMBER")
            .arg("s")
            .arg("x")
            .query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SMEMBERS")
            .arg("s")
            .query::<Vec<String>>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SCARD").arg("s").query::<i64>(&mut conn),
        "WRONGTYPE",
    );
    assert_err_contains(
        redis::cmd("SSCAN")
            .arg("s")
            .arg(0)
            .query::<(String, Vec<String>)>(&mut conn),
        "WRONGTYPE",
    );

    assert_err_contains(
        redis::cmd("SPOP")
            .arg("missing")
            .arg(-1)
            .query::<Vec<String>>(&mut conn),
        "ERR value is out of range, must be positive",
    );
    assert_err_contains(
        redis::cmd("SRANDMEMBER")
            .arg("missing")
            .arg("-9223372036854775808")
            .query::<Vec<String>>(&mut conn),
        "value is out of range",
    );
}
