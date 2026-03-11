#![allow(clippy::too_many_lines)]

use std::{
    collections::HashSet,
    thread,
    time::{Duration, Instant},
};

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

fn bulk_to_string(value: Value) -> Option<String> {
    match value {
        Value::Nil => None,
        Value::BulkString(bytes) => Some(String::from_utf8(bytes).unwrap()),
        Value::SimpleString(text) => Some(text),
        other => panic!("expected string-ish value, got {other:?}"),
    }
}

fn array_optional_strings(value: Value) -> Vec<Option<String>> {
    match value {
        Value::Array(values) => values.into_iter().map(bulk_to_string).collect(),
        other => panic!("expected array, got {other:?}"),
    }
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

fn scan_collect(
    conn: &mut Connection,
    pattern: Option<&str>,
    type_filter: Option<&str>,
) -> Vec<String> {
    let mut cursor = 0u64;
    let mut out = Vec::new();
    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor);
        if let Some(pattern) = pattern {
            cmd.arg("MATCH").arg(pattern);
        }
        if let Some(type_filter) = type_filter {
            cmd.arg("TYPE").arg(type_filter);
        }
        cmd.arg("COUNT").arg(7);
        let (next, page): (u64, Vec<String>) = cmd.query(conn).unwrap();
        out.extend(page);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    out
}

fn encoding(conn: &mut Connection, key: &str) -> Option<String> {
    redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg(key)
        .query(conn)
        .ok()
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_key_lifecycle_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: () = redis::cmd("SET")
        .arg("g:str")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("g:hash")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("g:list")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("g:set")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("g:zset")
        .arg(1)
        .arg("m")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("g:stream")
        .arg("*")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();

    assert_eq!(
        redis::cmd("EXISTS")
            .arg("g:missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("g:str")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("g:str")
            .arg("g:str")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );

    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:str")
            .query::<String>(&mut conn)
            .unwrap(),
        "string"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:list")
            .query::<String>(&mut conn)
            .unwrap(),
        "list"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:set")
            .query::<String>(&mut conn)
            .unwrap(),
        "set"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:zset")
            .query::<String>(&mut conn)
            .unwrap(),
        "zset"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:hash")
            .query::<String>(&mut conn)
            .unwrap(),
        "hash"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:stream")
            .query::<String>(&mut conn)
            .unwrap(),
        "stream"
    );
    assert_eq!(
        redis::cmd("TYPE")
            .arg("g:none")
            .query::<String>(&mut conn)
            .unwrap(),
        "none"
    );

    let deleted: i64 = redis::cmd("DEL")
        .arg("g:missing")
        .arg("g:str")
        .arg("g:hash")
        .arg("g:list")
        .arg("g:set")
        .arg("g:zset")
        .arg("g:stream")
        .query(&mut conn)
        .unwrap();
    assert_eq!(deleted, 6);
    for key in ["g:str", "g:hash", "g:list", "g:set", "g:zset", "g:stream"] {
        assert_eq!(
            redis::cmd("EXISTS")
                .arg(key)
                .query::<i64>(&mut conn)
                .unwrap(),
            0
        );
    }

    let _: () = redis::cmd("SET")
        .arg("u:1")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("u:2")
        .arg("y")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("UNLINK")
            .arg("u:1")
            .arg("u:missing")
            .arg("u:2")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_rename_renamenx_copy_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: () = redis::cmd("SET")
        .arg("r:src")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PEXPIRE")
        .arg("r:src")
        .arg(5_000)
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("RENAME")
            .arg("r:src")
            .arg("r:dst")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_eq!(
        redis::cmd("GET")
            .arg("r:dst")
            .query::<String>(&mut conn)
            .unwrap(),
        "v"
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("r:src")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert!(
        redis::cmd("PTTL")
            .arg("r:dst")
            .query::<i64>(&mut conn)
            .unwrap()
            > 0
    );
    assert_eq!(
        redis::cmd("RENAME")
            .arg("r:dst")
            .arg("r:dst")
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    assert_err_contains(
        redis::cmd("RENAME")
            .arg("r:missing")
            .arg("r:none")
            .query::<String>(&mut conn),
        "ERR no such key",
    );

    let _: () = redis::cmd("SET")
        .arg("r:nxsrc")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("r:nxdst")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("RENAMENX")
            .arg("r:nxsrc")
            .arg("r:nxdst")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("RENAMENX")
            .arg("r:nxsrc")
            .arg("r:nxfree")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    let _: () = redis::cmd("SET")
        .arg("c:s")
        .arg("abc")
        .query(&mut conn)
        .unwrap();
    let copied: i64 = redis::cmd("COPY")
        .arg("c:s")
        .arg("c:s2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(copied, 1);
    let _: usize = redis::cmd("APPEND")
        .arg("c:s")
        .arg("d")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg("c:s")
            .query::<String>(&mut conn)
            .unwrap(),
        "abcd"
    );
    assert_eq!(
        redis::cmd("GET")
            .arg("c:s2")
            .query::<String>(&mut conn)
            .unwrap(),
        "abc"
    );
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:s2")
            .arg("c:s2")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let _: i64 = redis::cmd("RPUSH")
        .arg("c:list")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:list")
            .arg("c:list2")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let _: i64 = redis::cmd("RPUSH")
        .arg("c:list")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("c:list2")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "b"]
    );

    let _: i64 = redis::cmd("HSET")
        .arg("c:hash")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:hash")
            .arg("c:hash2")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let _: i64 = redis::cmd("HSET")
        .arg("c:hash")
        .arg("f2")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("HGETALL")
            .arg("c:hash2")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["f", "1"]
    );

    let _: i64 = redis::cmd("SADD")
        .arg("c:set")
        .arg("1")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:set")
            .arg("c:set2")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let _: i64 = redis::cmd("SADD")
        .arg("c:set")
        .arg("3")
        .query(&mut conn)
        .unwrap();
    let copied_set: HashSet<String> = redis::cmd("SMEMBERS")
        .arg("c:set2")
        .query::<Vec<String>>(&mut conn)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(
        copied_set,
        HashSet::from(["1".to_string(), "2".to_string()])
    );

    let _: i64 = redis::cmd("ZADD")
        .arg("c:z")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:z")
            .arg("c:z2")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let _: i64 = redis::cmd("ZADD")
        .arg("c:z")
        .arg(3)
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("ZRANGE")
            .arg("c:z2")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "b"]
    );

    let _: () = redis::cmd("SET")
        .arg("c:dst")
        .arg("occupied")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:s")
            .arg("c:dst")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("COPY")
            .arg("c:s")
            .arg("c:dst")
            .arg("REPLACE")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_expiry_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: () = redis::cmd("SET")
        .arg("e:ex")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("e:px")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("e:exa")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("e:pxa")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("EXPIRE")
            .arg("e:ex")
            .arg(2)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PEXPIRE")
            .arg("e:px")
            .arg(2_000)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        redis::cmd("EXPIREAT")
            .arg("e:exa")
            .arg(now_secs + 2)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("PEXPIREAT")
            .arg("e:pxa")
            .arg(now_ms + 2_000)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert!(
        redis::cmd("TTL")
            .arg("e:ex")
            .query::<i64>(&mut conn)
            .unwrap()
            > 0
    );
    assert!(
        redis::cmd("PTTL")
            .arg("e:px")
            .query::<i64>(&mut conn)
            .unwrap()
            > 0
    );
    assert!(
        redis::cmd("EXPIRETIME")
            .arg("e:exa")
            .query::<i64>(&mut conn)
            .unwrap()
            >= now_secs as i64
    );
    assert!(
        redis::cmd("PEXPIRETIME")
            .arg("e:pxa")
            .query::<i64>(&mut conn)
            .unwrap()
            >= now_ms as i64
    );

    let _: () = redis::cmd("SET")
        .arg("e:nx")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("EXPIRE")
            .arg("e:nx")
            .arg(5)
            .arg("NX")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("EXPIRE")
            .arg("e:nx")
            .arg(6)
            .arg("NX")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("EXPIRE")
            .arg("e:nx")
            .arg(7)
            .arg("XX")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let pttl_before = redis::cmd("PTTL")
        .arg("e:nx")
        .query::<i64>(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PEXPIRE")
            .arg("e:nx")
            .arg(pttl_before + 500)
            .arg("GT")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    let pttl_after_gt = redis::cmd("PTTL")
        .arg("e:nx")
        .query::<i64>(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PEXPIRE")
            .arg("e:nx")
            .arg(pttl_after_gt.saturating_sub(250))
            .arg("LT")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    let _: () = redis::cmd("SET")
        .arg("e:persist")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PEXPIRE")
        .arg("e:persist")
        .arg(5_000)
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("PERSIST")
            .arg("e:persist")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("TTL")
            .arg("e:persist")
            .query::<i64>(&mut conn)
            .unwrap(),
        -1
    );
    assert_eq!(
        redis::cmd("TTL")
            .arg("e:missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        -2
    );
    assert_eq!(
        redis::cmd("PTTL")
            .arg("e:missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        -2
    );
    assert_eq!(
        redis::cmd("EXPIRETIME")
            .arg("e:missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        -2
    );
    assert_eq!(
        redis::cmd("PEXPIRETIME")
            .arg("e:missing")
            .query::<i64>(&mut conn)
            .unwrap(),
        -2
    );

    let _: () = redis::cmd("SET")
        .arg("e:zero")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("EXPIRE")
            .arg("e:zero")
            .arg(0)
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("e:zero")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    assert_err_contains(
        redis::cmd("EXPIRE")
            .arg("e:x")
            .arg(-1)
            .query::<i64>(&mut conn),
        "ERR invalid expire time in 'expire' command",
    );
    assert_err_contains(
        redis::cmd("PEXPIRE")
            .arg("e:x")
            .arg(-1)
            .query::<i64>(&mut conn),
        "ERR invalid expire time in 'pexpire' command",
    );

    let _: () = redis::cmd("SET")
        .arg("e:gone")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PEXPIRE")
        .arg("e:gone")
        .arg(200)
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        redis::cmd("GET")
            .arg("e:gone")
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        None
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_scan_search_and_touch_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    for key in ["hello", "hallo", "hxllo", "heeeello", "scan:str"] {
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg("v")
            .query(&mut conn)
            .unwrap();
    }
    let _: i64 = redis::cmd("SADD")
        .arg("scan:set")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("scan:hash")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("scan:zset")
        .arg(1)
        .arg("m")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PEXPIRE")
        .arg("scan:expired")
        .arg(10)
        .query::<i64>(&mut conn)
        .unwrap_or_else(|_| {
            let _: () = redis::cmd("SET")
                .arg("scan:expired")
                .arg("v")
                .query(&mut conn)
                .unwrap();
            redis::cmd("PEXPIRE")
                .arg("scan:expired")
                .arg(10)
                .query(&mut conn)
                .unwrap()
        });
    thread::sleep(Duration::from_millis(20));

    let keys_all: HashSet<String> = redis::cmd("KEYS")
        .arg("*")
        .query::<Vec<String>>(&mut conn)
        .unwrap()
        .into_iter()
        .collect();
    assert!(keys_all.contains("hello"));
    assert!(keys_all.contains("scan:str"));
    assert!(!keys_all.contains("scan:expired"));
    assert_eq!(
        redis::cmd("KEYS")
            .arg("h?llo")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            "hello".to_string(),
            "hallo".to_string(),
            "hxllo".to_string()
        ])
    );
    assert_eq!(
        redis::cmd("KEYS")
            .arg("h*llo")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            "hello".to_string(),
            "hallo".to_string(),
            "hxllo".to_string(),
            "heeeello".to_string()
        ])
    );
    assert_eq!(
        redis::cmd("KEYS")
            .arg("h[ae]llo")
            .query::<Vec<String>>(&mut conn)
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from(["hello".to_string(), "hallo".to_string()])
    );

    let scanned: HashSet<String> = scan_collect(&mut conn, None, None).into_iter().collect();
    assert!(scanned.contains("scan:str"));
    assert!(scanned.contains("scan:set"));
    assert!(scanned.contains("scan:hash"));
    assert!(scanned.contains("scan:zset"));

    let only_strings: HashSet<String> = scan_collect(&mut conn, None, Some("string"))
        .into_iter()
        .collect();
    assert!(only_strings.contains("scan:str"));
    assert!(!only_strings.contains("scan:set"));

    let matched_strings: HashSet<String> = scan_collect(&mut conn, Some("scan:*"), Some("string"))
        .into_iter()
        .collect();
    assert_eq!(matched_strings, HashSet::from(["scan:str".to_string()]));

    let random = redis::cmd("RANDOMKEY")
        .query::<Option<String>>(&mut conn)
        .unwrap();
    assert!(random.as_ref().is_some_and(|key| keys_all.contains(key)));
    flush(&mut conn);
    assert_eq!(
        redis::cmd("RANDOMKEY")
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        None
    );

    let _: () = redis::cmd("SET")
        .arg("touch:key")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_secs(11));
    let before: i64 = redis::cmd("OBJECT")
        .arg("IDLETIME")
        .arg("touch:key")
        .query(&mut conn)
        .unwrap();
    assert!(before > 0);
    let touched: i64 = redis::cmd("TOUCH")
        .arg("touch:key")
        .arg("touch:missing")
        .query(&mut conn)
        .unwrap();
    assert_eq!(touched, 1);
    let after: i64 = redis::cmd("OBJECT")
        .arg("IDLETIME")
        .arg("touch:key")
        .query(&mut conn)
        .unwrap();
    assert!(after <= before);
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_object_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: () = redis::cmd("SET")
        .arg("obj:int")
        .arg("42")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("obj:embstr")
        .arg("hello")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("obj:raw")
        .arg("x".repeat(45))
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("obj:float")
        .arg("1.5")
        .query(&mut conn)
        .unwrap();

    let mut hset_lp = redis::cmd("HSET");
    hset_lp.arg("obj:hash:lp");
    for i in 0..5 {
        hset_lp.arg(format!("f{i}")).arg(i);
    }
    let _: i64 = hset_lp.query(&mut conn).unwrap();

    let mut hset_ht = redis::cmd("HSET");
    hset_ht.arg("obj:hash:ht");
    for i in 0..200 {
        hset_ht.arg(format!("f{i}")).arg(i);
    }
    let _: i64 = hset_ht.query(&mut conn).unwrap();

    let _: i64 = redis::cmd("RPUSH")
        .arg("obj:list:lp")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let mut rpush = redis::cmd("RPUSH");
    rpush.arg("obj:list:ql");
    for i in 0..256 {
        rpush.arg(format!("v{i}"));
    }
    let _: i64 = rpush.query(&mut conn).unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("obj:set:int")
        .arg(1)
        .arg(2)
        .arg(3)
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("obj:set:lp")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let mut sadd = redis::cmd("SADD");
    sadd.arg("obj:set:ht");
    for i in 0..200 {
        sadd.arg(format!("m{i}"));
    }
    let _: i64 = sadd.query(&mut conn).unwrap();

    let _: i64 = redis::cmd("ZADD")
        .arg("obj:z:lp")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let mut zadd = redis::cmd("ZADD");
    zadd.arg("obj:z:sl");
    for i in 0..200 {
        zadd.arg(i).arg(format!("m{i}"));
    }
    let _: i64 = zadd.query(&mut conn).unwrap();

    let _: String = redis::cmd("XADD")
        .arg("obj:stream")
        .arg("*")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();

    assert_eq!(encoding(&mut conn, "obj:int").unwrap(), "int");
    assert_eq!(encoding(&mut conn, "obj:embstr").unwrap(), "embstr");
    assert_eq!(encoding(&mut conn, "obj:raw").unwrap(), "raw");
    assert_eq!(encoding(&mut conn, "obj:float").unwrap(), "embstr");
    assert_eq!(encoding(&mut conn, "obj:hash:lp").unwrap(), "listpack");
    assert_eq!(encoding(&mut conn, "obj:hash:ht").unwrap(), "hashtable");
    assert_eq!(encoding(&mut conn, "obj:list:lp").unwrap(), "listpack");
    assert_eq!(encoding(&mut conn, "obj:list:ql").unwrap(), "quicklist");
    assert_eq!(encoding(&mut conn, "obj:set:int").unwrap(), "intset");
    assert_eq!(encoding(&mut conn, "obj:set:lp").unwrap(), "listpack");
    assert_eq!(encoding(&mut conn, "obj:set:ht").unwrap(), "hashtable");
    assert_eq!(encoding(&mut conn, "obj:z:lp").unwrap(), "listpack");
    assert_eq!(encoding(&mut conn, "obj:z:sl").unwrap(), "skiplist");
    assert_eq!(encoding(&mut conn, "obj:stream").unwrap(), "stream");
    assert_eq!(encoding(&mut conn, "obj:missing"), None);

    let _: () = redis::cmd("SET")
        .arg("obj:idle")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_secs(11));
    assert!(
        redis::cmd("OBJECT")
            .arg("IDLETIME")
            .arg("obj:idle")
            .query::<i64>(&mut conn)
            .unwrap()
            > 0
    );
    assert_eq!(
        redis::cmd("OBJECT")
            .arg("REFCOUNT")
            .arg("obj:idle")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );
    assert_eq!(
        redis::cmd("OBJECT")
            .arg("FREQ")
            .arg("obj:idle")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    let help: Vec<String> = redis::cmd("OBJECT").arg("HELP").query(&mut conn).unwrap();
    assert!(help.len() >= 6);
    assert_err_contains(
        redis::cmd("OBJECT").arg("NOPE").query::<String>(&mut conn),
        "ERR unknown subcommand 'NOPE' for 'object' command",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_dump_restore_and_sort_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: () = redis::cmd("SET")
        .arg("dump:s")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("dump:h")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("dump:l")
        .arg("3")
        .arg("1")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("SADD")
        .arg("dump:set")
        .arg("2")
        .arg("3")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("ZADD")
        .arg("dump:z")
        .arg(9)
        .arg("2")
        .arg(1)
        .arg("1")
        .arg(4)
        .arg("3")
        .query(&mut conn)
        .unwrap();

    for (source, target, kind) in [
        ("dump:s", "restore:s", "string"),
        ("dump:h", "restore:h", "hash"),
        ("dump:l", "restore:l", "list"),
        ("dump:set", "restore:set", "set"),
        ("dump:z", "restore:z", "zset"),
    ] {
        let payload: Vec<u8> = redis::cmd("DUMP").arg(source).query(&mut conn).unwrap();
        assert_eq!(
            redis::cmd("RESTORE")
                .arg(target)
                .arg(0)
                .arg(payload)
                .query::<String>(&mut conn)
                .unwrap(),
            "OK"
        );
        assert_eq!(
            redis::cmd("TYPE")
                .arg(target)
                .query::<String>(&mut conn)
                .unwrap(),
            kind
        );
    }

    let _: () = redis::cmd("SET")
        .arg("dump:exp")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("PEXPIRE")
        .arg("dump:exp")
        .arg(10)
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_millis(20));
    assert_eq!(
        redis::cmd("DUMP")
            .arg("dump:exp")
            .query::<Option<Vec<u8>>>(&mut conn)
            .unwrap(),
        None
    );

    let payload: Vec<u8> = redis::cmd("DUMP").arg("dump:s").query(&mut conn).unwrap();
    let mut bad = payload.clone();
    *bad.last_mut().unwrap() ^= 0x01;
    assert_err_contains(
        redis::cmd("RESTORE")
            .arg("dump:bad")
            .arg(0)
            .arg(bad)
            .query::<String>(&mut conn),
        "ERR DUMP payload version or checksum are wrong",
    );
    assert_err_contains(
        redis::cmd("RESTORE")
            .arg("dump:s")
            .arg(0)
            .arg(payload.clone())
            .query::<String>(&mut conn),
        "BUSYKEY Target key name already exists",
    );
    assert_eq!(
        redis::cmd("RESTORE")
            .arg("dump:ttl")
            .arg(500)
            .arg(payload)
            .query::<String>(&mut conn)
            .unwrap(),
        "OK"
    );
    thread::sleep(Duration::from_millis(650));
    assert_eq!(
        redis::cmd("GET")
            .arg("dump:ttl")
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        None
    );

    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:l")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:l")
            .arg("DESC")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["3", "2", "1"]
    );
    let _: i64 = redis::cmd("RPUSH")
        .arg("dump:alpha")
        .arg("b")
        .arg("a")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:alpha")
            .arg("ALPHA")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:l")
            .arg("LIMIT")
            .arg(1)
            .arg(2)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["2", "3"]
    );
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:set")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:z")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["1", "2", "3"]
    );

    let _: () = redis::cmd("SET")
        .arg("weight_1")
        .arg("30")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("weight_2")
        .arg("10")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("weight_3")
        .arg("20")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:set")
            .arg("BY")
            .arg("weight_*")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["2", "3", "1"]
    );

    let _: () = redis::cmd("SET")
        .arg("data_1")
        .arg("one")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("data_2")
        .arg("two")
        .query(&mut conn)
        .unwrap();
    let got = array_optional_strings(
        redis::cmd("SORT")
            .arg("dump:set")
            .arg("ALPHA")
            .arg("GET")
            .arg("data_*")
            .arg("GET")
            .arg("#")
            .query::<Value>(&mut conn)
            .unwrap(),
    );
    assert_eq!(
        got,
        vec![
            Some("one".into()),
            Some("1".into()),
            Some("two".into()),
            Some("2".into()),
            None,
            Some("3".into())
        ]
    );

    assert_eq!(
        redis::cmd("SORT")
            .arg("dump:l")
            .arg("STORE")
            .arg("sorted:list")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("sorted:list")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["1", "2", "3"]
    );
    assert_err_contains(
        redis::cmd("SORT_RO")
            .arg("dump:l")
            .arg("STORE")
            .arg("sorted:ro")
            .query::<Vec<String>>(&mut conn),
        "ERR STORE option not allowed in SORT_RO",
    );
}

#[test]
#[ignore = "requires running Senko instance"]
fn generic_error_cases_and_wait_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let wrong_arity_cases: [(&str, &[&str], &str); 24] = [
        ("DEL", &[], "wrong number of arguments for 'del' command"),
        (
            "UNLINK",
            &[],
            "wrong number of arguments for 'unlink' command",
        ),
        (
            "EXISTS",
            &[],
            "wrong number of arguments for 'exists' command",
        ),
        ("TYPE", &[], "wrong number of arguments for 'type' command"),
        (
            "RENAME",
            &["a"],
            "wrong number of arguments for 'rename' command",
        ),
        (
            "RENAMENX",
            &["a"],
            "wrong number of arguments for 'renamenx' command",
        ),
        (
            "COPY",
            &["a"],
            "wrong number of arguments for 'copy' command",
        ),
        (
            "EXPIRE",
            &["a"],
            "wrong number of arguments for 'expire' command",
        ),
        (
            "PEXPIRE",
            &["a"],
            "wrong number of arguments for 'pexpire' command",
        ),
        (
            "EXPIREAT",
            &["a"],
            "wrong number of arguments for 'expireat' command",
        ),
        (
            "PEXPIREAT",
            &["a"],
            "wrong number of arguments for 'pexpireat' command",
        ),
        ("TTL", &[], "wrong number of arguments for 'ttl' command"),
        ("PTTL", &[], "wrong number of arguments for 'pttl' command"),
        (
            "EXPIRETIME",
            &[],
            "wrong number of arguments for 'expiretime' command",
        ),
        (
            "PEXPIRETIME",
            &[],
            "wrong number of arguments for 'pexpiretime' command",
        ),
        (
            "PERSIST",
            &[],
            "wrong number of arguments for 'persist' command",
        ),
        ("KEYS", &[], "wrong number of arguments for 'keys' command"),
        ("SCAN", &[], "wrong number of arguments for 'scan' command"),
        (
            "RANDOMKEY",
            &["x"],
            "wrong number of arguments for 'randomkey' command",
        ),
        (
            "TOUCH",
            &[],
            "wrong number of arguments for 'touch' command",
        ),
        ("DUMP", &[], "wrong number of arguments for 'dump' command"),
        (
            "RESTORE",
            &["k"],
            "wrong number of arguments for 'restore' command",
        ),
        (
            "MOVE",
            &["k"],
            "wrong number of arguments for 'move' command",
        ),
        (
            "WAIT",
            &["1"],
            "wrong number of arguments for 'wait' command",
        ),
    ];
    for (command, args, needle) in wrong_arity_cases {
        let mut cmd = redis::cmd(command);
        for arg in args {
            cmd.arg(arg);
        }
        assert_err_contains(cmd.query::<Value>(&mut conn), needle);
    }
    assert_err_contains(
        redis::cmd("WAITAOF")
            .arg("1")
            .arg("2")
            .query::<Value>(&mut conn),
        "wrong number of arguments for 'waitaof' command",
    );
    assert_err_contains(
        redis::cmd("SORT").query::<Value>(&mut conn),
        "wrong number of arguments for 'sort' command",
    );
    assert_err_contains(
        redis::cmd("SORT_RO").query::<Value>(&mut conn),
        "wrong number of arguments for 'sort_ro' command",
    );
    assert_err_contains(
        redis::cmd("OBJECT").query::<Value>(&mut conn),
        "wrong number of arguments for 'object' command",
    );
    assert_err_contains(
        redis::cmd("OBJECT")
            .arg("ENCODING")
            .query::<Value>(&mut conn),
        "wrong number of arguments for 'object|encoding' command",
    );

    let start = Instant::now();
    assert_eq!(
        redis::cmd("WAIT")
            .arg(1)
            .arg(1000)
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert!(start.elapsed() < Duration::from_millis(100));
    let waitaof: Vec<i64> = redis::cmd("WAITAOF")
        .arg(1)
        .arg(1)
        .arg(1000)
        .query(&mut conn)
        .unwrap();
    assert_eq!(waitaof, vec![0, 0]);
    assert_err_contains(
        redis::cmd("MIGRATE")
            .arg("127.0.0.1")
            .arg(6379)
            .arg("k")
            .arg(0)
            .arg(1000)
            .arg("COPY")
            .query::<String>(&mut conn),
        "ERR MIGRATE not supported in Senko Phase 1",
    );

    let _: i64 = redis::cmd("RPUSH")
        .arg("sort:bad")
        .arg("a")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("SORT")
            .arg("sort:bad")
            .query::<Vec<String>>(&mut conn),
        "ERR One or more scores can't be converted into double",
    );
}
