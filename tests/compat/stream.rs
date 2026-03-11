#![allow(clippy::too_many_lines)]

use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use redis::{Connection, RedisResult, Value};

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

fn as_string(value: Value) -> String {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
        Value::SimpleString(text) => text,
        Value::Int(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        other => panic!("expected string-like value, got {other:?}"),
    }
}

fn as_array(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    }
}

fn flat_strings(value: Value) -> Vec<String> {
    as_array(value).into_iter().map(as_string).collect()
}

fn stream_entries(value: Value) -> Vec<(String, Vec<String>)> {
    as_array(value)
        .into_iter()
        .map(|entry| {
            let parts = as_array(entry);
            assert_eq!(parts.len(), 2);
            (as_string(parts[0].clone()), flat_strings(parts[1].clone()))
        })
        .collect()
}

fn xread_streams(value: Value) -> Vec<(String, Vec<(String, Vec<String>)>)> {
    as_array(value)
        .into_iter()
        .map(|stream| {
            let parts = as_array(stream);
            assert_eq!(parts.len(), 2);
            (
                as_string(parts[0].clone()),
                stream_entries(parts[1].clone()),
            )
        })
        .collect()
}

fn pending_summary(value: Value) -> (i64, Option<String>, Option<String>, Vec<(String, i64)>) {
    let parts = as_array(value);
    assert_eq!(parts.len(), 4);
    let count = match parts[0] {
        Value::Int(value) => value,
        ref other => panic!("expected integer, got {other:?}"),
    };
    let min = match &parts[1] {
        Value::Nil => None,
        value => Some(as_string(value.clone())),
    };
    let max = match &parts[2] {
        Value::Nil => None,
        value => Some(as_string(value.clone())),
    };
    let per_consumer = as_array(parts[3].clone())
        .into_iter()
        .map(|entry| {
            let pair = as_array(entry);
            assert_eq!(pair.len(), 2);
            let count = match pair[1] {
                Value::Int(value) => value,
                ref other => panic!("expected integer, got {other:?}"),
            };
            (as_string(pair[0].clone()), count)
        })
        .collect();
    (count, min, max, per_consumer)
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_basic_ops_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let id1: String = redis::cmd("XADD")
        .arg("s")
        .arg("*")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let id2: String = redis::cmd("XADD")
        .arg("s")
        .arg("*")
        .arg("f")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert!(id1 < id2);

    let explicit_ok: String = redis::cmd("XADD")
        .arg("s")
        .arg("999999999999-0")
        .arg("f")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_eq!(explicit_ok, "999999999999-0");
    assert_err_contains(
        redis::cmd("XADD")
            .arg("s")
            .arg("999999999999-0")
            .arg("f")
            .arg("x")
            .query::<String>(&mut conn),
        "ERR The ID specified in XADD is equal or smaller than the target stream top item",
    );

    for i in 0..100 {
        let _: String = redis::cmd("XADD")
            .arg("trim-maxlen")
            .arg("MAXLEN")
            .arg("=")
            .arg("50")
            .arg("*")
            .arg("f")
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    let len: i64 = redis::cmd("XLEN")
        .arg("trim-maxlen")
        .query(&mut conn)
        .unwrap();
    assert_eq!(len, 50);

    for i in 0..100 {
        let _: String = redis::cmd("XADD")
            .arg("trim-approx")
            .arg("MAXLEN")
            .arg("~")
            .arg("50")
            .arg("*")
            .arg("f")
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }
    let approx_len: i64 = redis::cmd("XLEN")
        .arg("trim-approx")
        .query(&mut conn)
        .unwrap();
    assert!(approx_len <= 150);

    let _: String = redis::cmd("XADD")
        .arg("trim-minid")
        .arg("1-0")
        .arg("f")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("trim-minid")
        .arg("2-0")
        .arg("f")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let trimmed: i64 = redis::cmd("XTRIM")
        .arg("trim-minid")
        .arg("MINID")
        .arg("=")
        .arg("2-0")
        .query(&mut conn)
        .unwrap();
    assert_eq!(trimmed, 1);

    let _: String = redis::cmd("XADD")
        .arg("delete-me")
        .arg("1-0")
        .arg("f")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("delete-me")
        .arg("2-0")
        .arg("f")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let deleted: i64 = redis::cmd("XDEL")
        .arg("delete-me")
        .arg("1-0")
        .query(&mut conn)
        .unwrap();
    assert_eq!(deleted, 1);
    let len_after: i64 = redis::cmd("XLEN")
        .arg("delete-me")
        .query(&mut conn)
        .unwrap();
    assert_eq!(len_after, 1);

    let range = stream_entries(
        redis::cmd("XRANGE")
            .arg("delete-me")
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].0, "2-0");

    let rev = stream_entries(
        redis::cmd("XREVRANGE")
            .arg("delete-me")
            .arg("+")
            .arg("-")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(rev.len(), 1);
    assert_eq!(rev[0].0, "2-0");

    let paged = stream_entries(
        redis::cmd("XRANGE")
            .arg("trim-maxlen")
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg("5")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(paged.len(), 5);
    assert!(paged.windows(2).all(|w| w[0].0 < w[1].0));

    let ok: String = redis::cmd("XSETID")
        .arg("setid")
        .arg("5-0")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    assert_err_contains(
        redis::cmd("XSETID")
            .arg("setid")
            .arg("4-0")
            .query::<String>(&mut conn),
        "equal or smaller",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_group_and_pending_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("XADD")
        .arg("g1")
        .arg("1-0")
        .arg("f")
        .arg("old")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("g1")
        .arg("grp$")
        .arg("$")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("g1")
        .arg("2-0")
        .arg("f")
        .arg("new")
        .query(&mut conn)
        .unwrap();
    let delivered = xread_streams(
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("grp$")
            .arg("c1")
            .arg("STREAMS")
            .arg("g1")
            .arg(">")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(delivered[0].1.len(), 1);
    assert_eq!(delivered[0].1[0].0, "2-0");

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("g2")
        .arg("grp0")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XADD")
        .arg("g2")
        .arg("1-0")
        .arg("f")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let delivered0 = xread_streams(
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("grp0")
            .arg("c1")
            .arg("STREAMS")
            .arg("g2")
            .arg(">")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(delivered0[0].1[0].0, "1-0");

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("cycle")
        .arg("g")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    let cycle_id: String = redis::cmd("XADD")
        .arg("cycle")
        .arg("*")
        .arg("f")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("cycle")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();
    let summary_before = pending_summary(
        redis::cmd("XPENDING")
            .arg("cycle")
            .arg("g")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(summary_before.0, 1);
    let acked: i64 = redis::cmd("XACK")
        .arg("cycle")
        .arg("g")
        .arg(&cycle_id)
        .query(&mut conn)
        .unwrap();
    assert_eq!(acked, 1);
    let summary_after = pending_summary(
        redis::cmd("XPENDING")
            .arg("cycle")
            .arg("g")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(summary_after.0, 0);

    let _: String = redis::cmd("XADD")
        .arg("cycle")
        .arg("*")
        .arg("f")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    let first = xread_streams(
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c")
            .arg("STREAMS")
            .arg("cycle")
            .arg(">")
            .query(&mut conn)
            .unwrap(),
    );
    let redelivered = xread_streams(
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c")
            .arg("STREAMS")
            .arg("cycle")
            .arg(first[0].1[0].0.clone())
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(redelivered[0].1[0].0, first[0].1[0].0);

    let _: String = redis::cmd("XADD")
        .arg("noack")
        .arg("1-0")
        .arg("f")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("noack")
        .arg("g")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("NOACK")
        .arg("STREAMS")
        .arg("noack")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();
    let noack_summary = pending_summary(
        redis::cmd("XPENDING")
            .arg("noack")
            .arg("g")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(noack_summary.0, 0);

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("meta")
        .arg("g")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    for _ in 0..3 {
        let _: String = redis::cmd("XADD")
            .arg("meta")
            .arg("*")
            .arg("f")
            .arg("v")
            .query(&mut conn)
            .unwrap();
    }
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("COUNT")
        .arg("2")
        .arg("STREAMS")
        .arg("meta")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();

    let pending_detail: Value = redis::cmd("XPENDING")
        .arg("meta")
        .arg("g")
        .arg("IDLE")
        .arg("0")
        .arg("-")
        .arg("+")
        .arg("10")
        .query(&mut conn)
        .unwrap();
    assert_eq!(as_array(pending_detail).len(), 2);

    let groups: Value = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg("meta")
        .query(&mut conn)
        .unwrap();
    assert_eq!(as_array(groups).len(), 1);
    let consumers: Value = redis::cmd("XINFO")
        .arg("CONSUMERS")
        .arg("meta")
        .arg("g")
        .query(&mut conn)
        .unwrap();
    assert_eq!(as_array(consumers).len(), 1);
    let stream_info: Value = redis::cmd("XINFO")
        .arg("STREAM")
        .arg("meta")
        .query(&mut conn)
        .unwrap();
    assert!(!as_array(stream_info).is_empty());

    let removed: i64 = redis::cmd("XGROUP")
        .arg("DELCONSUMER")
        .arg("meta")
        .arg("g")
        .arg("c1")
        .query(&mut conn)
        .unwrap();
    assert_eq!(removed, 2);
    let destroyed: i64 = redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg("meta")
        .arg("g")
        .query(&mut conn)
        .unwrap();
    assert_eq!(destroyed, 1);
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_claim_and_delete_modes_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("claim")
        .arg("g")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    let id: String = redis::cmd("XADD")
        .arg("claim")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("STREAMS")
        .arg("claim")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();

    let claimed = redis::cmd("XCLAIM")
        .arg("claim")
        .arg("g")
        .arg("c2")
        .arg("0")
        .arg(&id)
        .query::<Value>(&mut conn)
        .unwrap();
    assert_eq!(stream_entries(claimed).len(), 1);
    let detail = redis::cmd("XPENDING")
        .arg("claim")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg("10")
        .query::<Value>(&mut conn)
        .unwrap();
    let owner = as_string(as_array(as_array(detail)[0].clone())[1].clone());
    assert_eq!(owner, "c2");

    let not_claimed = redis::cmd("XCLAIM")
        .arg("claim")
        .arg("g")
        .arg("c3")
        .arg("999999999")
        .arg(&id)
        .query::<Value>(&mut conn)
        .unwrap();
    assert!(as_array(not_claimed).is_empty());

    let auto = redis::cmd("XAUTOCLAIM")
        .arg("claim")
        .arg("g")
        .arg("c4")
        .arg("0")
        .arg("0-0")
        .arg("COUNT")
        .arg("10")
        .query::<Value>(&mut conn)
        .unwrap();
    let auto_parts = as_array(auto);
    assert_eq!(auto_parts.len(), 3);

    let deleted_id: String = redis::cmd("XADD")
        .arg("deleted-pel")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("deleted-pel")
        .arg("g")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("STREAMS")
        .arg("deleted-pel")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("XDEL")
        .arg("deleted-pel")
        .arg(&deleted_id)
        .query(&mut conn)
        .unwrap();
    let deleted_auto = redis::cmd("XAUTOCLAIM")
        .arg("deleted-pel")
        .arg("g")
        .arg("c2")
        .arg("0")
        .arg("0-0")
        .query::<Value>(&mut conn)
        .unwrap();
    let deleted_parts = as_array(deleted_auto);
    assert_eq!(as_array(deleted_parts[2].clone()).len(), 1);

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("ackdel")
        .arg("g")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    let ackdel_id: String = redis::cmd("XADD")
        .arg("ackdel")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("STREAMS")
        .arg("ackdel")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("XACKDEL")
        .arg("ackdel")
        .arg("g")
        .arg("DELREF")
        .arg("IDS")
        .arg("1")
        .arg(&ackdel_id)
        .query(&mut conn)
        .unwrap();
    let ackdel_range = stream_entries(
        redis::cmd("XRANGE")
            .arg("ackdel")
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .unwrap(),
    );
    assert!(ackdel_range.is_empty());

    let keep_id: String = redis::cmd("XADD")
        .arg("ackkeep")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("ackkeep")
        .arg("g")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _ = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c1")
        .arg("STREAMS")
        .arg("ackkeep")
        .arg(">")
        .query::<Value>(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("XACKDEL")
        .arg("ackkeep")
        .arg("g")
        .arg("KEEPREF")
        .arg("IDS")
        .arg("1")
        .arg(&keep_id)
        .query(&mut conn)
        .unwrap();
    let keep_range = stream_entries(
        redis::cmd("XRANGE")
            .arg("ackkeep")
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(keep_range.len(), 1);

    assert_err_contains(
        redis::cmd("XDELEX")
            .arg("ackkeep")
            .arg("IDS")
            .arg("2")
            .arg("1-0")
            .query::<i64>(&mut conn),
        "numids does not match actual number of IDs",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_blocking_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let timeout: Value = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg("10")
        .arg("STREAMS")
        .arg("b")
        .arg("0-0")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(timeout, Value::Nil));

    let (tx, rx) = mpsc::channel();
    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    thread::spawn(move || {
        let client = redis::Client::open(url).unwrap();
        let mut blocked = client.get_connection().unwrap();
        let value: Value = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg("1000")
            .arg("STREAMS")
            .arg("b")
            .arg("0-0")
            .query(&mut blocked)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: String = redis::cmd("XADD")
        .arg("b")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let woke = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(xread_streams(woke)[0].1[0].0, "1-0");

    let fanout_url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let (fan_tx, fan_rx) = mpsc::channel();
    for _ in 0..3 {
        let tx = fan_tx.clone();
        let url = fanout_url.clone();
        thread::spawn(move || {
            let client = redis::Client::open(url).unwrap();
            let mut blocked = client.get_connection().unwrap();
            let value: Value = redis::cmd("XREAD")
                .arg("BLOCK")
                .arg("1000")
                .arg("STREAMS")
                .arg("fan")
                .arg("0-0")
                .query(&mut blocked)
                .unwrap();
            tx.send(value).unwrap();
        });
    }
    thread::sleep(Duration::from_millis(50));
    let _: String = redis::cmd("XADD")
        .arg("fan")
        .arg("1-0")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    for _ in 0..3 {
        let value = fan_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(xread_streams(value)[0].1[0].0, "1-0");
    }

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("bg")
        .arg("g")
        .arg("$")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    let (group_tx, group_rx) = mpsc::channel();
    let group_url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    thread::spawn(move || {
        let client = redis::Client::open(group_url).unwrap();
        let mut blocked = client.get_connection().unwrap();
        let value: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c1")
            .arg("BLOCK")
            .arg("1000")
            .arg("STREAMS")
            .arg("bg")
            .arg(">")
            .query(&mut blocked)
            .unwrap();
        group_tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let new_id: String = redis::cmd("XADD")
        .arg("bg")
        .arg("*")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let group_value = group_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(xread_streams(group_value)[0].1[0].0, new_id);
    let summary = pending_summary(
        redis::cmd("XPENDING")
            .arg("bg")
            .arg("g")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(summary.0, 1);

    let block0_url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let (block0_tx, block0_rx) = mpsc::channel();
    thread::spawn(move || {
        let client = redis::Client::open(block0_url).unwrap();
        let mut blocked = client.get_connection().unwrap();
        let start = Instant::now();
        let value: Value = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg("0")
            .arg("STREAMS")
            .arg("b0")
            .arg("$")
            .query(&mut blocked)
            .unwrap();
        block0_tx.send((start.elapsed(), value)).unwrap();
    });
    thread::sleep(Duration::from_millis(75));
    let _: String = redis::cmd("XADD")
        .arg("b0")
        .arg("*")
        .arg("f")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    let (elapsed, value) = block0_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(elapsed >= Duration::from_millis(50));
    assert_eq!(xread_streams(value)[0].1.len(), 1);
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_error_and_wrongtype_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("not-stream")
        .arg("value")
        .query(&mut conn)
        .unwrap();

    assert_err_contains(
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg("missing")
            .arg("g")
            .arg("0")
            .query::<String>(&mut conn),
        "ERR no such key",
    );
    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("dup")
        .arg("g")
        .arg("0")
        .arg("MKSTREAM")
        .query(&mut conn)
        .unwrap();
    assert_err_contains(
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg("dup")
            .arg("g")
            .arg("0")
            .query::<String>(&mut conn),
        "BUSYGROUP Consumer Group name already exists",
    );
    assert_err_contains(
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("missing")
            .arg("c")
            .arg("STREAMS")
            .arg("dup")
            .arg(">")
            .query::<Value>(&mut conn),
        "NOGROUP No such consumer group",
    );
    assert_err_contains(
        redis::cmd("XREAD")
            .arg("STREAMS")
            .arg("s1")
            .arg("s2")
            .arg("0-0")
            .query::<Value>(&mut conn),
        "Unbalanced XREAD list of streams",
    );
    assert_err_contains(
        redis::cmd("XACKDEL")
            .arg("dup")
            .arg("g")
            .arg("IDS")
            .arg("2")
            .arg("1-0")
            .query::<i64>(&mut conn),
        "numids does not match actual number of IDs",
    );

    for command in [
        "XLEN",
        "XDEL",
        "XRANGE",
        "XREVRANGE",
        "XTRIM",
        "XGROUP",
        "XINFO",
        "XPENDING",
        "XACK",
        "XCLAIM",
        "XAUTOCLAIM",
    ] {
        let result = match command {
            "XLEN" => redis::cmd(command)
                .arg("not-stream")
                .query::<i64>(&mut conn),
            "XDEL" => redis::cmd(command)
                .arg("not-stream")
                .arg("1-0")
                .query::<i64>(&mut conn),
            "XRANGE" | "XREVRANGE" => redis::cmd(command)
                .arg("not-stream")
                .arg("-")
                .arg("+")
                .query::<Value>(&mut conn),
            "XTRIM" => redis::cmd(command)
                .arg("not-stream")
                .arg("MAXLEN")
                .arg("=")
                .arg("1")
                .query::<i64>(&mut conn),
            "XGROUP" => redis::cmd(command)
                .arg("DESTROY")
                .arg("not-stream")
                .arg("g")
                .query::<i64>(&mut conn),
            "XINFO" => redis::cmd(command)
                .arg("STREAM")
                .arg("not-stream")
                .query::<Value>(&mut conn),
            "XPENDING" => redis::cmd(command)
                .arg("not-stream")
                .arg("g")
                .query::<Value>(&mut conn),
            "XACK" => redis::cmd(command)
                .arg("not-stream")
                .arg("g")
                .arg("1-0")
                .query::<i64>(&mut conn),
            "XCLAIM" => redis::cmd(command)
                .arg("not-stream")
                .arg("g")
                .arg("c")
                .arg("0")
                .arg("1-0")
                .query::<Value>(&mut conn),
            "XAUTOCLAIM" => redis::cmd(command)
                .arg("not-stream")
                .arg("g")
                .arg("c")
                .arg("0")
                .arg("0-0")
                .query::<Value>(&mut conn),
            _ => unreachable!(),
        };
        assert_err_contains(result, "WRONGTYPE");
    }

    assert_err_contains(
        redis::cmd("XADD").query::<String>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XLEN").query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XDEL").arg("k").query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XRANGE").arg("k").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XREVRANGE").arg("k").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XGROUP")
            .arg("CREATE")
            .query::<String>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XINFO").arg("STREAM").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XPENDING").arg("k").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XACK").arg("k").arg("g").query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XACKDEL").arg("k").query::<i64>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XCLAIM").arg("k").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
    assert_err_contains(
        redis::cmd("XAUTOCLAIM").arg("k").query::<Value>(&mut conn),
        "wrong number of arguments",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn stream_large_stress_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    for i in 0..100_000 {
        let _: String = redis::cmd("XADD")
            .arg("stress")
            .arg("*")
            .arg("f")
            .arg(i)
            .query(&mut conn)
            .unwrap();
    }

    let mut cursor = "-".to_string();
    let mut seen = 0usize;
    loop {
        let page = stream_entries(
            redis::cmd("XRANGE")
                .arg("stress")
                .arg(&cursor)
                .arg("+")
                .arg("COUNT")
                .arg("100")
                .query(&mut conn)
                .unwrap(),
        );
        if page.is_empty() {
            break;
        }
        seen += page.len();
        let last = page.last().unwrap().0.clone();
        if page.len() < 100 {
            break;
        }
        cursor = last;
    }
    assert!(seen >= 100_000);

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg("stress")
        .arg("g")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let mut delivered = 0usize;
    loop {
        let value: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("g")
            .arg("c1")
            .arg("COUNT")
            .arg("100")
            .arg("STREAMS")
            .arg("stress")
            .arg(">")
            .query(&mut conn)
            .unwrap_or(Value::Nil);
        if matches!(value, Value::Nil) {
            break;
        }
        delivered += xread_streams(value)[0].1.len();
    }
    assert_eq!(delivered, 100_000);

    let pending = redis::cmd("XPENDING")
        .arg("stress")
        .arg("g")
        .arg("-")
        .arg("+")
        .arg("100000")
        .arg("c1")
        .query::<Value>(&mut conn)
        .unwrap();
    let ids = as_array(pending)
        .into_iter()
        .map(|entry| as_string(as_array(entry)[0].clone()))
        .collect::<Vec<_>>();
    for chunk in ids.chunks(100) {
        let mut cmd = redis::cmd("XACK");
        cmd.arg("stress").arg("g");
        for id in chunk {
            cmd.arg(id);
        }
        let _: i64 = cmd.query(&mut conn).unwrap();
    }
    let summary = pending_summary(
        redis::cmd("XPENDING")
            .arg("stress")
            .arg("g")
            .query(&mut conn)
            .unwrap(),
    );
    assert_eq!(summary.0, 0);

    let _: i64 = redis::cmd("XTRIM")
        .arg("stress")
        .arg("MAXLEN")
        .arg("=")
        .arg("10000")
        .query(&mut conn)
        .unwrap();
    let len: i64 = redis::cmd("XLEN").arg("stress").query(&mut conn).unwrap();
    assert_eq!(len, 10_000);
}
