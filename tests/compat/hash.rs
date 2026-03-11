#![allow(clippy::too_many_lines)]

use std::{collections::HashSet, thread, time::Duration};

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

fn hscan_collect(
    conn: &mut Connection,
    key: &str,
    pattern: Option<&str>,
    count: Option<usize>,
    novalues: bool,
) -> Vec<String> {
    let mut cursor: u64 = 0;
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("HSCAN");
        cmd.arg(key).arg(cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(count) = count {
            cmd.arg("COUNT").arg(count);
        }
        if novalues {
            cmd.arg("NOVALUES");
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

#[test]
#[ignore = "requires running senko instance"]
fn hash_basic_ops_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let added: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(added, 2);
    assert_eq!(conn.hget::<_, _, String>("h", "a").unwrap(), "1");
    assert_eq!(conn.hget::<_, _, String>("h", "b").unwrap(), "2");

    let mixed: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg("11")
        .arg("c")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    assert_eq!(mixed, 1);

    let removed: i64 = redis::cmd("HDEL")
        .arg("h")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(removed, 3);
    let exists: i64 = redis::cmd("EXISTS").arg("h").query(&mut conn).unwrap();
    assert_eq!(exists, 0);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_arithmetic_and_rand_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let one: i64 = redis::cmd("HINCRBY")
        .arg("h")
        .arg("f")
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(one, 1);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("ovf")
        .arg(i64::MAX)
        .query(&mut conn)
        .unwrap();
    let err = redis::cmd("HINCRBY")
        .arg("h")
        .arg("ovf")
        .arg(1)
        .query::<i64>(&mut conn)
        .unwrap_err();
    assert!(err.to_string().contains("overflow"));

    let err = redis::cmd("HINCRBYFLOAT")
        .arg("h")
        .arg("ff")
        .arg("3.1415e2")
        .query::<String>(&mut conn)
        .unwrap_err();
    assert!(err.to_string().contains("float") || err.to_string().contains("ERR"));

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("fi")
        .arg(10)
        .query(&mut conn)
        .unwrap();
    let out: String = redis::cmd("HINCRBYFLOAT")
        .arg("h")
        .arg("fi")
        .arg("0.5")
        .query(&mut conn)
        .unwrap();
    assert_eq!(out, "10.5");

    let _: i64 = redis::cmd("HSET")
        .arg("r")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    let gt_len: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("r")
        .arg(10)
        .query(&mut conn)
        .unwrap();
    assert_eq!(gt_len.len(), 3);
    let uniq: HashSet<_> = gt_len.iter().cloned().collect();
    assert_eq!(uniq.len(), 3);

    let neg: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("r")
        .arg(-7)
        .query(&mut conn)
        .unwrap();
    assert_eq!(neg.len(), 7);

    let withvals: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("r")
        .arg(2)
        .arg("WITHVALUES")
        .query(&mut conn)
        .unwrap();
    assert_eq!(withvals.len() % 2, 0);

    let _: i64 = redis::cmd("DEL").arg("dist").query(&mut conn).unwrap();
    for i in 0..10 {
        let _: i64 = redis::cmd("HSET")
            .arg("dist")
            .arg(format!("f{i}"))
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    let mut counts = [0u32; 10];
    for _ in 0..10_000 {
        let field: String = redis::cmd("HRANDFIELD")
            .arg("dist")
            .query(&mut conn)
            .unwrap();
        let idx: usize = field.trim_start_matches('f').parse().unwrap();
        counts[idx] += 1;
    }
    let expected = 1000.0f64;
    let chi_sq: f64 = counts
        .iter()
        .map(|c| {
            let d = *c as f64 - expected;
            d * d / expected
        })
        .sum();
    assert!(
        chi_sq < 27.88,
        "chi-square too high: {chi_sq} with counts {counts:?}"
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_expiry_scan_setex_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let nx: Vec<i64> = redis::cmd("HEXPIRE")
        .arg("h")
        .arg(5)
        .arg("NX")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(nx, vec![1]);
    let xx: Vec<i64> = redis::cmd("HEXPIRE")
        .arg("h")
        .arg(10)
        .arg("XX")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(xx, vec![1]);

    let ttl: Vec<i64> = redis::cmd("HTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert!(ttl[0] >= 0);
    assert_eq!(ttl[1], 2);

    let pttl: Vec<i64> = redis::cmd("HPTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert!(pttl[0] > 0);
    let et: Vec<i64> = redis::cmd("HEXPIRETIME")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert!(et[0] > 0);
    let pet: Vec<i64> = redis::cmd("HPEXPIRETIME")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert!(pet[0] > 0);

    let persisted: Vec<i64> = redis::cmd("HPERSIST")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(persisted, vec![1]);
    let ttl2: Vec<i64> = redis::cmd("HTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ttl2, vec![-1]);

    let _: i64 = redis::cmd("HSET")
        .arg("exp")
        .arg("x")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("exp")
        .arg(200)
        .arg("FIELDS")
        .arg(1)
        .arg("x")
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    let val: Option<String> = conn.hget("exp", "x").unwrap();
    assert_eq!(val, None);
    let exists: i64 = redis::cmd("EXISTS").arg("exp").query(&mut conn).unwrap();
    assert_eq!(exists, 0);

    let _hsetex: i64 = redis::cmd("HSETEX")
        .arg("sx")
        .arg("FNX")
        .arg("PX")
        .arg(2000)
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query(&mut conn)
        .unwrap();

    let (c0, page0): (String, Vec<String>) = redis::cmd("HSCAN")
        .arg("sx")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    assert!(!c0.is_empty());
    assert_eq!(page0.len() % 2, 0);
    let (_c1, page1): (String, Vec<String>) = redis::cmd("HSCAN")
        .arg("sx")
        .arg(0)
        .arg("NOVALUES")
        .query(&mut conn)
        .unwrap();
    assert!(page1.len() >= 2);

    let (cx, px): (String, Vec<String>) = redis::cmd("HSCAN")
        .arg("none")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    assert_eq!(cx, "0");
    assert!(px.is_empty());
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_error_cases_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let _: String = redis::cmd("SET")
        .arg("s")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let wrong = redis::cmd("HGET")
        .arg("s")
        .arg("f")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(wrong.to_string().contains("WRONGTYPE"));

    let arity_commands = [
        "HSET",
        "HGET",
        "HDEL",
        "HEXISTS",
        "HLEN",
        "HKEYS",
        "HVALS",
        "HGETALL",
        "HMGET",
        "HMSET",
        "HSETNX",
        "HSTRLEN",
        "HINCRBY",
        "HINCRBYFLOAT",
        "HRANDFIELD",
        "HGETDEL",
        "HGETEX",
        "HEXPIRE",
        "HPEXPIRE",
        "HEXPIREAT",
        "HPEXPIREAT",
        "HTTL",
        "HPTTL",
        "HEXPIRETIME",
        "HPEXPIRETIME",
        "HPERSIST",
        "HSETEX",
        "HSCAN",
    ];
    for cmd in arity_commands {
        let err = redis::cmd(cmd).query::<Value>(&mut conn).unwrap_err();
        assert!(
            err.to_string().contains("wrong number of arguments"),
            "{cmd} did not return wrong-arity error: {err}"
        );
    }

    let neg = redis::cmd("HEXPIRE")
        .arg("h")
        .arg(-1)
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(neg.to_string().contains("invalid expire time"));

    let mismatch = redis::cmd("HSETEX")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(mismatch.to_string().contains("numfields"));
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_ordering_and_resp2_resp3_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .arg("f3")
        .arg("v3")
        .query(&mut conn)
        .unwrap();

    let hmget: Vec<Option<String>> = redis::cmd("HMGET")
        .arg("h")
        .arg("f3")
        .arg("missing")
        .arg("f1")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        hmget,
        vec![Some("v3".to_string()), None, Some("v1".to_string())]
    );

    let all: Vec<String> = redis::cmd("HGETALL").arg("h").query(&mut conn).unwrap();
    assert_eq!(all.len() % 2, 0);
    let as_map: std::collections::HashMap<String, String> = all
        .chunks_exact(2)
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    assert_eq!(as_map.get("f1").unwrap(), "v1");
    assert_eq!(as_map.get("f2").unwrap(), "v2");
    assert_eq!(as_map.get("f3").unwrap(), "v3");

    let _: Value = redis::cmd("HELLO").arg(3).query(&mut conn).unwrap();
    let withvalues_resp3: Value = redis::cmd("HRANDFIELD")
        .arg("h")
        .arg(2)
        .arg("WITHVALUES")
        .query(&mut conn)
        .unwrap();
    match withvalues_resp3 {
        Value::Array(ref v) => {
            assert_eq!(v.len(), 2);
        }
        _ => panic!("expected RESP3 aggregate response for WITHVALUES"),
    }
    let _: Value = redis::cmd("HELLO").arg(2).query(&mut conn).unwrap();
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_expiry_condition_variants_and_codes_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query(&mut conn)
        .unwrap();

    let nx: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(2000)
        .arg("NX")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    assert_eq!(nx, vec![1, 1]);
    let nx_skip: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(3000)
        .arg("NX")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(nx_skip, vec![0]);

    let xx: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(4000)
        .arg("XX")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(xx, vec![1, 0]);

    let gt_ok: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(5000)
        .arg("GT")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(gt_ok, vec![1]);
    let gt_skip: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(1000)
        .arg("GT")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(gt_skip, vec![0]);

    let lt_ok: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(900)
        .arg("LT")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(lt_ok, vec![1]);
    let lt_skip: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(1000)
        .arg("LT")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(lt_skip, vec![0]);

    let missing_codes: Vec<i64> = redis::cmd("HTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("missing")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing_codes[0], 2);
    assert_eq!(missing_codes[1], -1);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hgetdel_hgetex_hsetex_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let written: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("FNX")
        .arg("PX")
        .arg(3000)
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(written, 2);

    let fxx_written: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("FXX")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("v11")
        .arg("f3")
        .arg("v3")
        .query(&mut conn)
        .unwrap();
    assert_eq!(fxx_written, 1);

    let before: Vec<i64> = redis::cmd("HPTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("KEEPTTL")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("v111")
        .arg("f4")
        .arg("v4")
        .query(&mut conn)
        .unwrap();
    let after: Vec<i64> = redis::cmd("HPTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("f4")
        .query(&mut conn)
        .unwrap();
    assert!(after[0] > 0 && after[0] <= before[0]);
    assert_eq!(after[1], -1);

    let got: Vec<Option<String>> = redis::cmd("HGETEX")
        .arg("h")
        .arg("PX")
        .arg(500)
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("missing")
        .query(&mut conn)
        .unwrap();
    assert_eq!(got[0], Some("v111".to_string()));
    assert_eq!(got[1], None);

    let deleted: Vec<Option<String>> = redis::cmd("HGETDEL")
        .arg("h")
        .arg("FIELDS")
        .arg(3)
        .arg("f1")
        .arg("f2")
        .arg("missing")
        .query(&mut conn)
        .unwrap();
    assert_eq!(deleted[0], Some("v111".to_string()));
    assert_eq!(deleted[1], Some("v2".to_string()));
    assert_eq!(deleted[2], None);
    let rem: i64 = redis::cmd("HLEN").arg("h").query(&mut conn).unwrap();
    assert!(rem >= 1);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_scan_compat_full_match_novalues_and_missing() {
    let mut conn = must_connect();
    flush(&mut conn);
    for i in 0..120 {
        let _: i64 = redis::cmd("HSET")
            .arg("scan")
            .arg(format!("hello{i}"))
            .arg(format!("v{i}"))
            .query(&mut conn)
            .unwrap();
    }
    for i in 0..20 {
        let _: i64 = redis::cmd("HSET")
            .arg("scan")
            .arg(format!("aeiou{i}"))
            .arg(format!("x{i}"))
            .query(&mut conn)
            .unwrap();
    }

    let all = hscan_collect(&mut conn, "scan", None, Some(11), false);
    assert!(all.len() >= 240);

    let only_fields = hscan_collect(&mut conn, "scan", None, Some(7), true);
    assert!(only_fields.len() >= 140);

    let star = hscan_collect(&mut conn, "scan", Some("*"), Some(10), true);
    assert!(star.len() >= only_fields.len());

    let hqllo = hscan_collect(&mut conn, "scan", Some("h?llo*"), Some(10), true);
    assert!(hqllo.iter().any(|f| f.starts_with("hello")));

    let vowels = hscan_collect(&mut conn, "scan", Some("[aeiou]*"), Some(10), true);
    assert!(vowels.iter().any(|f| f.starts_with("aeiou")));

    let (cursor, empty): (u64, Vec<String>) = redis::cmd("HSCAN")
        .arg("no-such-key")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    assert_eq!(cursor, 0);
    assert!(empty.is_empty());
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_encoding_small_vs_large_and_basic_collections() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("hash-max-listpack-value")
        .arg(64)
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("hash-max-listpack-entries")
        .arg(512)
        .query(&mut conn)
        .unwrap();

    let _: i64 = redis::cmd("HSET")
        .arg("small")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    let small_enc: String = redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg("small")
        .query(&mut conn)
        .unwrap();
    assert!(small_enc.contains("listpack"));

    let long_field = "x".repeat(80);
    let _: i64 = redis::cmd("HSET")
        .arg("big")
        .arg(long_field)
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let big_enc: String = redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg("big")
        .query(&mut conn)
        .unwrap();
    assert!(big_enc.contains("hashtable"));

    let keys: Vec<String> = redis::cmd("HKEYS").arg("small").query(&mut conn).unwrap();
    let vals: Vec<String> = redis::cmd("HVALS").arg("small").query(&mut conn).unwrap();
    let all: Vec<String> = redis::cmd("HGETALL").arg("small").query(&mut conn).unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(vals.len(), 3);
    assert_eq!(all.len(), 6);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hsetnx_hmset_hstrlen_corner_cases() {
    let mut conn = must_connect();
    flush(&mut conn);

    let first: i64 = redis::cmd("HSETNX")
        .arg("h")
        .arg("f")
        .arg("v1")
        .query(&mut conn)
        .unwrap();
    let second: i64 = redis::cmd("HSETNX")
        .arg("h")
        .arg("f")
        .arg("v2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(first, 1);
    assert_eq!(second, 0);
    assert_eq!(conn.hget::<_, _, String>("h", "f").unwrap(), "v1");

    let ok: String = redis::cmd("HMSET")
        .arg("h")
        .arg("a")
        .arg("100")
        .arg("b")
        .arg("-1")
        .arg("c")
        .arg("")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");

    let len_a: i64 = redis::cmd("HSTRLEN")
        .arg("h")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let len_b: i64 = redis::cmd("HSTRLEN")
        .arg("h")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let len_c: i64 = redis::cmd("HSTRLEN")
        .arg("h")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let len_missing: i64 = redis::cmd("HSTRLEN")
        .arg("h")
        .arg("missing")
        .query(&mut conn)
        .unwrap();
    assert_eq!(len_a, 3);
    assert_eq!(len_b, 2);
    assert_eq!(len_c, 0);
    assert_eq!(len_missing, 0);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hrandfield_zero_nonexisting_and_overflow_errors() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let zero: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("h")
        .arg(0)
        .query(&mut conn)
        .unwrap();
    assert!(zero.is_empty());

    let non_existing: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("missing")
        .arg(10)
        .query(&mut conn)
        .unwrap();
    assert!(non_existing.is_empty());

    let e1 = redis::cmd("HRANDFIELD")
        .arg("h")
        .arg("-9223372036854775808")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(e1.to_string().contains("out of range"));
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hgetdel_and_hgetex_input_validation() {
    let mut conn = must_connect();
    flush(&mut conn);

    let e_hgetdel = redis::cmd("HGETDEL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(
        e_hgetdel.to_string().contains("numfields")
            || e_hgetdel.to_string().contains("wrong number")
    );

    let e_hgetdel_zero = redis::cmd("HGETDEL")
        .arg("h")
        .arg("FIELDS")
        .arg(0)
        .arg("a")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(
        e_hgetdel_zero.to_string().contains("Number of fields")
            || e_hgetdel_zero.to_string().contains("numfields")
    );

    let e_hgetex = redis::cmd("HGETEX")
        .arg("h")
        .arg("PX")
        .arg(-1)
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(e_hgetex.to_string().contains("invalid expire time"));

    let e_hgetex_fields = redis::cmd("HGETEX")
        .arg("h")
        .arg("FIELDS")
        .arg(0)
        .arg("a")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(
        e_hgetex_fields
            .to_string()
            .contains("invalid number of fields")
            || e_hgetex_fields.to_string().contains("wrong number")
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hsetex_input_validation_and_conditions() {
    let mut conn = must_connect();
    flush(&mut conn);

    let e_dual_cond = redis::cmd("HSETEX")
        .arg("h")
        .arg("FNX")
        .arg("FXX")
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .arg("1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(e_dual_cond.to_string().contains("Only one of FXX or FNX"));

    let e_dual_exp = redis::cmd("HSETEX")
        .arg("h")
        .arg("EX")
        .arg(10)
        .arg("PX")
        .arg(10)
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .arg("1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(e_dual_exp
        .to_string()
        .contains("Only one of EX, PX, EXAT, PXAT or KEEPTTL"));

    let e_num = redis::cmd("HSETEX")
        .arg("h")
        .arg("FIELDS")
        .arg(0)
        .arg("a")
        .arg("1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(e_num.to_string().contains("invalid number of fields"));

    let wrote: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("FNX")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(wrote, 2);

    let fxx_noop: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("FXX")
        .arg("FIELDS")
        .arg(2)
        .arg("a")
        .arg("10")
        .arg("c")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    assert_eq!(fxx_noop, 1);
    assert_eq!(conn.hget::<_, _, String>("h", "a").unwrap(), "10");
    let c: Option<String> = conn.hget("h", "c").unwrap();
    assert_eq!(c, None);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_ttl_progress_and_persist_codes() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query(&mut conn)
        .unwrap();
    let _: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(2000)
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query(&mut conn)
        .unwrap();

    let pttl_1: Vec<i64> = redis::cmd("HPTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("f2")
        .query(&mut conn)
        .unwrap();
    assert!(pttl_1[0] > 0 && pttl_1[0] <= 2000);
    assert_eq!(pttl_1[1], -1);

    thread::sleep(Duration::from_millis(250));
    let pttl_2: Vec<i64> = redis::cmd("HPTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query(&mut conn)
        .unwrap();
    assert!(pttl_2[0] > 0 && pttl_2[0] < pttl_1[0]);

    let persist: Vec<i64> = redis::cmd("HPERSIST")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("missing")
        .query(&mut conn)
        .unwrap();
    assert_eq!(persist, vec![1, 2]);

    let persist_again: Vec<i64> = redis::cmd("HPERSIST")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query(&mut conn)
        .unwrap();
    assert_eq!(persist_again, vec![-1]);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hscan_count_and_expired_field_visibility() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .arg("f3")
        .arg("v3")
        .query(&mut conn)
        .unwrap();
    let _: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h")
        .arg(100)
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_millis(150));

    let mut cursor = 0u64;
    let mut seen = Vec::new();
    loop {
        let (next, arr): (u64, Vec<String>) = redis::cmd("HSCAN")
            .arg("h")
            .arg(cursor)
            .arg("COUNT")
            .arg(1)
            .query(&mut conn)
            .unwrap();
        seen.extend(arr);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    let flat = seen.join(" ");
    assert!(!flat.contains("f1"));
    assert!(flat.contains("f2"));
    assert!(flat.contains("f3"));
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_expiry_parser_rigid_and_multi_condition_errors() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f1")
        .arg("v1")
        .query(&mut conn)
        .unwrap();

    let e_bad_pos = redis::cmd("HEXPIRE")
        .arg("h")
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .arg(60)
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(
        e_bad_pos.to_string().contains("integer or out of range")
            || e_bad_pos.to_string().contains("wrong number")
    );

    let e_multi_cond = redis::cmd("HEXPIRE")
        .arg("h")
        .arg(60)
        .arg("NX")
        .arg("XX")
        .arg("FIELDS")
        .arg(1)
        .arg("f1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(
        e_multi_cond
            .to_string()
            .contains("Multiple condition flags")
            || e_multi_cond.to_string().contains("unknown argument")
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_hsetex_hgetex_flexible_ordering() {
    let mut conn = must_connect();
    flush(&mut conn);

    let w1: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("EX")
        .arg(60)
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(w1, 2);

    let w2: i64 = redis::cmd("HSETEX")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("v11")
        .arg("f2")
        .arg("v22")
        .arg("EX")
        .arg(60)
        .query(&mut conn)
        .unwrap();
    assert_eq!(w2, 2);

    let ttl: Vec<i64> = redis::cmd("HTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("f2")
        .query(&mut conn)
        .unwrap();
    assert!(ttl[0] > 0 && ttl[1] > 0);

    let g1: Vec<Option<String>> = redis::cmd("HGETEX")
        .arg("h")
        .arg("EX")
        .arg(30)
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("f2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(g1, vec![Some("v11".to_string()), Some("v22".to_string())]);

    let g2: Vec<Option<String>> = redis::cmd("HGETEX")
        .arg("h")
        .arg("FIELDS")
        .arg(2)
        .arg("f1")
        .arg("f2")
        .arg("EX")
        .arg(30)
        .query(&mut conn)
        .unwrap();
    assert_eq!(g2, vec![Some("v11".to_string()), Some("v22".to_string())]);
}

#[test]
#[ignore = "requires running senko instance"]
fn hash_keyword_like_field_names() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("EX")
        .arg("v1")
        .arg("PX")
        .arg("v2")
        .arg("FIELDS")
        .arg("v3")
        .arg("NX")
        .arg("v4")
        .arg("60")
        .arg("v5")
        .query(&mut conn)
        .unwrap();

    let ret: Vec<i64> = redis::cmd("HEXPIRE")
        .arg("h")
        .arg(120)
        .arg("FIELDS")
        .arg(5)
        .arg("EX")
        .arg("PX")
        .arg("FIELDS")
        .arg("NX")
        .arg("60")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ret, vec![1, 1, 1, 1, 1]);

    let ttl: Vec<i64> = redis::cmd("HTTL")
        .arg("h")
        .arg("FIELDS")
        .arg(5)
        .arg("EX")
        .arg("PX")
        .arg("FIELDS")
        .arg("NX")
        .arg("60")
        .query(&mut conn)
        .unwrap();
    for t in ttl {
        assert!(t > 0);
    }
}
