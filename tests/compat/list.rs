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

fn strings(value: Value) -> Vec<String> {
    match value {
        Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                Value::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
                Value::SimpleString(text) => text,
                other => panic!("expected bulk/simple string, got {other:?}"),
            })
            .collect(),
        other => panic!("expected array value, got {other:?}"),
    }
}

fn lmpop_value(value: Value) -> (String, Vec<String>) {
    match value {
        Value::Array(values) => {
            assert_eq!(values.len(), 2);
            let key = match &values[0] {
                Value::BulkString(bytes) => String::from_utf8(bytes.clone()).unwrap(),
                Value::SimpleString(text) => text.clone(),
                other => panic!("expected key string, got {other:?}"),
            };
            let elems = strings(values[1].clone());
            (key, elems)
        }
        other => panic!("expected LMPOP array, got {other:?}"),
    }
}

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn list_basic_push_pop_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let len: i64 = redis::cmd("LPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(len, 3);
    let range: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(range, vec!["c", "b", "a"]);

    let len: i64 = redis::cmd("RPUSH")
        .arg("r")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(len, 3);
    let range: Vec<String> = redis::cmd("LRANGE")
        .arg("r")
        .arg(0)
        .arg(-1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(range, vec!["a", "b", "c"]);

    let popped: Vec<String> = redis::cmd("LPOP").arg("r").arg(2).query(&mut conn).unwrap();
    assert_eq!(popped, vec!["a", "b"]);
    let popped: Vec<String> = redis::cmd("RPOP").arg("l").arg(2).query(&mut conn).unwrap();
    assert_eq!(popped, vec!["a", "b"]);

    let empty: Vec<String> = redis::cmd("LPOP").arg("r").arg(0).query(&mut conn).unwrap();
    assert!(empty.is_empty());

    let missing: i64 = redis::cmd("LPUSHX")
        .arg("missing")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing, 0);
    let exists: i64 = redis::cmd("LPUSHX")
        .arg("r")
        .arg("z")
        .query(&mut conn)
        .unwrap();
    assert_eq!(exists, 2);
    let exists: i64 = redis::cmd("RPUSHX")
        .arg("r")
        .arg("tail")
        .query(&mut conn)
        .unwrap();
    assert_eq!(exists, 3);
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("r")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["z", "c", "tail"]
    );

    let _: i64 = redis::cmd("DEL").arg("autodel").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("autodel")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let value: String = redis::cmd("LPOP").arg("autodel").query(&mut conn).unwrap();
    assert_eq!(value, "x");
    let exists: i64 = redis::cmd("EXISTS")
        .arg("autodel")
        .query(&mut conn)
        .unwrap();
    assert_eq!(exists, 0);
}

#[test]
#[ignore = "requires running senko instance"]
fn list_range_index_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query(&mut conn)
        .unwrap();

    assert_eq!(
        redis::cmd("LRANGE")
            .arg("l")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("l")
            .arg(1)
            .arg(2)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["b", "c"]
    );
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("l")
            .arg(-2)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["c", "d"]
    );
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("l")
            .arg(-99)
            .arg(99)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["a", "b", "c", "d"]
    );
    assert!(redis::cmd("LRANGE")
        .arg("l")
        .arg(3)
        .arg(1)
        .query::<Vec<String>>(&mut conn)
        .unwrap()
        .is_empty());

    assert_eq!(
        redis::cmd("LINDEX")
            .arg("l")
            .arg(0)
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        Some("a".into())
    );
    assert_eq!(
        redis::cmd("LINDEX")
            .arg("l")
            .arg(-1)
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        Some("d".into())
    );
    assert_eq!(
        redis::cmd("LINDEX")
            .arg("l")
            .arg(2)
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        Some("c".into())
    );
    assert_eq!(
        redis::cmd("LINDEX")
            .arg("l")
            .arg(99)
            .query::<Option<String>>(&mut conn)
            .unwrap(),
        None
    );

    let ok: String = redis::cmd("LSET")
        .arg("l")
        .arg(-1)
        .arg("z")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let err = redis::cmd("LSET")
        .arg("l")
        .arg(99)
        .arg("x")
        .query::<String>(&mut conn)
        .unwrap_err();
    assert!(err.to_string().contains("ERR index out of range"));
    let err = redis::cmd("LSET")
        .arg("missing")
        .arg(0)
        .arg("x")
        .query::<String>(&mut conn)
        .unwrap_err();
    assert!(err.to_string().contains("ERR no such key"));
}

#[test]
#[ignore = "requires running senko instance"]
fn list_mutation_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();

    assert_eq!(
        redis::cmd("LINSERT")
            .arg("l")
            .arg("BEFORE")
            .arg("a")
            .arg("head")
            .query::<i64>(&mut conn)
            .unwrap(),
        4
    );
    assert_eq!(
        redis::cmd("LINSERT")
            .arg("l")
            .arg("AFTER")
            .arg("b")
            .arg("mid")
            .query::<i64>(&mut conn)
            .unwrap(),
        5
    );
    assert_eq!(
        redis::cmd("LINSERT")
            .arg("l")
            .arg("AFTER")
            .arg("c")
            .arg("tail")
            .query::<i64>(&mut conn)
            .unwrap(),
        6
    );
    assert_eq!(
        redis::cmd("LINSERT")
            .arg("l")
            .arg("BEFORE")
            .arg("missing")
            .arg("x")
            .query::<i64>(&mut conn)
            .unwrap(),
        -1
    );

    let _: i64 = redis::cmd("DEL").arg("r").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("r")
        .arg("x")
        .arg("a")
        .arg("x")
        .arg("b")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("LREM")
            .arg("r")
            .arg(0)
            .arg("x")
            .query::<i64>(&mut conn)
            .unwrap(),
        3
    );
    let _: i64 = redis::cmd("RPUSH")
        .arg("r")
        .arg("x")
        .arg("x")
        .arg("y")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("LREM")
            .arg("r")
            .arg(2)
            .arg("x")
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("LREM")
            .arg("r")
            .arg(-1)
            .arg("x")
            .query::<i64>(&mut conn)
            .unwrap(),
        1
    );

    let _: i64 = redis::cmd("RPUSH")
        .arg("trim")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let ok: String = redis::cmd("LTRIM")
        .arg("trim")
        .arg(9)
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    assert_eq!(
        redis::cmd("EXISTS")
            .arg("trim")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let _: i64 = redis::cmd("DEL").arg("pos").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("pos")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("c")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("LPOS")
            .arg("pos")
            .arg("a")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("pos")
            .arg("a")
            .arg("RANK")
            .arg(-2)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("pos")
            .arg("a")
            .arg("COUNT")
            .arg(0)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![0, 2, 4]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("pos")
            .arg("z")
            .arg("COUNT")
            .arg(2)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        Vec::<i64>::new()
    );
    let missing: Option<i64> = redis::cmd("LPOS")
        .arg("pos")
        .arg("a")
        .arg("MAXLEN")
        .arg(1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing, Some(0));
}

#[test]
#[ignore = "requires running senko instance"]
fn list_lpos_extended_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("x-large")
        .arg("2")
        .arg("3")
        .arg("c")
        .arg("c")
        .query(&mut conn)
        .unwrap();

    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("a")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(1)
            .query::<i64>(&mut conn)
            .unwrap(),
        2
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(2)
            .query::<i64>(&mut conn)
            .unwrap(),
        6
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(4)
            .query::<Option<i64>>(&mut conn)
            .unwrap(),
        None
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(-1)
            .query::<i64>(&mut conn)
            .unwrap(),
        7
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(-2)
            .query::<i64>(&mut conn)
            .unwrap(),
        6
    );
    assert_err_contains(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("RANK")
            .arg(0)
            .query::<Value>(&mut conn),
        "RANK can't be zero",
    );

    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(0)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![2, 6, 7]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(1)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![2]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(2)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![2, 6]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(0)
            .arg("RANK")
            .arg(2)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![6, 7]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(2)
            .arg("RANK")
            .arg(-1)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![7, 6]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("a")
            .arg("COUNT")
            .arg(0)
            .arg("MAXLEN")
            .arg(1)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![0]
    );
    assert!(redis::cmd("LPOS")
        .arg("mylist")
        .arg("c")
        .arg("COUNT")
        .arg(0)
        .arg("MAXLEN")
        .arg(1)
        .query::<Vec<i64>>(&mut conn)
        .unwrap()
        .is_empty());
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(0)
            .arg("MAXLEN")
            .arg(3)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![2]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(0)
            .arg("MAXLEN")
            .arg(3)
            .arg("RANK")
            .arg(-1)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![7, 6]
    );
    assert_eq!(
        redis::cmd("LPOS")
            .arg("mylist")
            .arg("c")
            .arg("COUNT")
            .arg(0)
            .arg("MAXLEN")
            .arg(7)
            .arg("RANK")
            .arg(2)
            .query::<Vec<i64>>(&mut conn)
            .unwrap(),
        vec![6]
    );

    let none: Option<i64> = redis::cmd("LPOS")
        .arg("missing")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    assert_eq!(none, None);
    assert!(redis::cmd("LPOS")
        .arg("missing")
        .arg("c")
        .arg("COUNT")
        .arg(0)
        .query::<Vec<i64>>(&mut conn)
        .unwrap()
        .is_empty());
}

#[test]
#[ignore = "requires running senko instance"]
fn list_cross_list_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    for (src, dst, expected) in [
        ("LEFT", "LEFT", vec!["a", "x"]),
        ("LEFT", "RIGHT", vec!["x", "a"]),
        ("RIGHT", "LEFT", vec!["b", "x"]),
        ("RIGHT", "RIGHT", vec!["x", "b"]),
    ] {
        let _: i64 = redis::cmd("DEL")
            .arg("src")
            .arg("dst")
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("RPUSH")
            .arg("src")
            .arg("a")
            .arg("b")
            .query(&mut conn)
            .unwrap();
        let _: i64 = redis::cmd("RPUSH")
            .arg("dst")
            .arg("x")
            .query(&mut conn)
            .unwrap();
        let _: String = redis::cmd("LMOVE")
            .arg("src")
            .arg("dst")
            .arg(src)
            .arg(dst)
            .query(&mut conn)
            .unwrap();
        assert_eq!(
            redis::cmd("LRANGE")
                .arg("dst")
                .arg(0)
                .arg(-1)
                .query::<Vec<String>>(&mut conn)
                .unwrap(),
            expected
        );
    }

    let _: i64 = redis::cmd("DEL").arg("same").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("same")
        .arg("a")
        .arg("b")
        .arg("c")
        .query(&mut conn)
        .unwrap();
    let moved: String = redis::cmd("LMOVE")
        .arg("same")
        .arg("same")
        .arg("LEFT")
        .arg("RIGHT")
        .query(&mut conn)
        .unwrap();
    assert_eq!(moved, "a");
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("same")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["b", "c", "a"]
    );

    let missing: Option<String> = redis::cmd("LMOVE")
        .arg("missing")
        .arg("dst")
        .arg("LEFT")
        .arg("RIGHT")
        .query(&mut conn)
        .unwrap();
    assert_eq!(missing, None);

    let _: i64 = redis::cmd("DEL")
        .arg("src")
        .arg("dst")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let rp: String = redis::cmd("RPOPLPUSH")
        .arg("src")
        .arg("dst")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg("src2")
        .arg("dst2")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("src2")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let lm: String = redis::cmd("LMOVE")
        .arg("src2")
        .arg("dst2")
        .arg("RIGHT")
        .arg("LEFT")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rp, lm);

    let _: i64 = redis::cmd("DEL")
        .arg("k1")
        .arg("k2")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("k2")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let popped = redis::cmd("LMPOP")
        .arg(2)
        .arg("k1")
        .arg("k2")
        .arg("LEFT")
        .query::<Value>(&mut conn)
        .unwrap();
    assert_eq!(lmpop_value(popped), ("k2".into(), vec!["a".into()]));

    let none: Value = redis::cmd("LMPOP")
        .arg(2)
        .arg("x")
        .arg("y")
        .arg("LEFT")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(none, Value::Nil));

    let _: i64 = redis::cmd("DEL").arg("dup").query(&mut conn).unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("dup")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query(&mut conn)
        .unwrap();
    let first = redis::cmd("LMPOP")
        .arg(2)
        .arg("dup")
        .arg("dup")
        .arg("LEFT")
        .arg("COUNT")
        .arg(2)
        .query::<Value>(&mut conn)
        .unwrap();
    assert_eq!(
        lmpop_value(first),
        ("dup".into(), vec!["a".into(), "b".into()])
    );
    let second = redis::cmd("LMPOP")
        .arg(2)
        .arg("dup")
        .arg("dup")
        .arg("RIGHT")
        .arg("COUNT")
        .arg(10)
        .query::<Value>(&mut conn)
        .unwrap();
    assert_eq!(
        lmpop_value(second),
        ("dup".into(), vec!["d".into(), "c".into()])
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn list_blocking_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let start = Instant::now();
    let timeout_result: Value = redis::cmd("BLPOP")
        .arg("missing")
        .arg("0.3")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(timeout_result, Value::Nil));
    assert!(start.elapsed() >= Duration::from_millis(250));

    let (tx, rx) = mpsc::channel();
    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    thread::spawn(move || {
        let client = redis::Client::open(url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let started = Instant::now();
        let value: Vec<String> = redis::cmd("BLPOP")
            .arg("bk")
            .arg("5")
            .query(&mut bg)
            .unwrap();
        tx.send((started.elapsed(), value)).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("LPUSH")
        .arg("bk")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let (elapsed, value) = rx.recv().unwrap();
    assert!(elapsed < Duration::from_secs(2));
    assert_eq!(value, vec!["bk", "x"]);

    let _: i64 = redis::cmd("RPUSH")
        .arg("tail")
        .arg("a")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    assert_eq!(
        redis::cmd("BRPOP")
            .arg("tail")
            .arg("1")
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["tail", "b"]
    );

    let (tx, rx) = mpsc::channel();
    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    thread::spawn(move || {
        let client = redis::Client::open(url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: String = redis::cmd("BLMOVE")
            .arg("src")
            .arg("dst")
            .arg("RIGHT")
            .arg("LEFT")
            .arg("5")
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("moved")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), "moved");
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("dst")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["moved"]
    );

    let (tx, rx) = mpsc::channel();
    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    thread::spawn(move || {
        let client = redis::Client::open(url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Value = redis::cmd("BLMPOP")
            .arg("5")
            .arg(3)
            .arg("m1")
            .arg("m2")
            .arg("m3")
            .arg("LEFT")
            .arg("COUNT")
            .arg(3)
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("LPUSH")
        .arg("m2")
        .arg("b")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let value = rx.recv().unwrap();
    assert_eq!(
        lmpop_value(value),
        ("m2".into(), vec!["a".into(), "b".into()])
    );

    let mut receivers = Vec::new();
    for _ in 0..5 {
        let (tx, rx) = mpsc::channel();
        let url = std::env::var("senko_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
        thread::spawn(move || {
            let client = redis::Client::open(url).unwrap();
            let mut bg = client.get_connection().unwrap();
            let value: Vec<String> = redis::cmd("BLPOP")
                .arg("fifo")
                .arg("5")
                .query(&mut bg)
                .unwrap();
            tx.send(value).unwrap();
        });
        receivers.push(rx);
    }
    thread::sleep(Duration::from_millis(50));
    for i in 0..5 {
        let _: i64 = redis::cmd("RPUSH")
            .arg("fifo")
            .arg(format!("v{i}"))
            .query(&mut conn)
            .unwrap();
    }
    let got: Vec<Vec<String>> = receivers.into_iter().map(|rx| rx.recv().unwrap()).collect();
    assert_eq!(got.len(), 5);

    let _: i64 = redis::cmd("LPUSH")
        .arg("fast")
        .arg("x")
        .query(&mut conn)
        .unwrap();
    let fast: Vec<String> = redis::cmd("BLPOP")
        .arg("fast")
        .arg("5")
        .query(&mut conn)
        .unwrap();
    assert_eq!(fast, vec!["fast", "x"]);
}

#[test]
#[ignore = "requires running senko instance"]
fn list_blocking_extended_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let url =
        std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    let (tx, rx) = mpsc::channel();
    let zero_url = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(zero_url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let started = Instant::now();
        let value: Vec<String> = redis::cmd("BLPOP")
            .arg("forever")
            .arg("0")
            .query(&mut bg)
            .unwrap();
        tx.send((started.elapsed(), value)).unwrap();
    });
    thread::sleep(Duration::from_millis(150));
    let _: i64 = redis::cmd("LPUSH")
        .arg("forever")
        .arg("woke")
        .query(&mut conn)
        .unwrap();
    let (elapsed, value) = rx.recv().unwrap();
    assert!(elapsed >= Duration::from_millis(100));
    assert_eq!(value, vec!["forever", "woke"]);

    assert_err_contains(
        redis::cmd("BLPOP")
            .arg("neg")
            .arg("-1")
            .query::<Value>(&mut conn),
        "ERR timeout is negative",
    );
    assert_err_contains(
        redis::cmd("BRPOP")
            .arg("neg")
            .arg("-1")
            .query::<Value>(&mut conn),
        "ERR timeout is negative",
    );

    let (tx, rx) = mpsc::channel();
    let float_url = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(float_url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: Vec<String> = redis::cmd("BLPOP")
            .arg("float-timeout")
            .arg("0.1")
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(30));
    let _: i64 = redis::cmd("RPUSH")
        .arg("float-timeout")
        .arg("foo")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), vec!["float-timeout", "foo"]);

    let (tx, rx) = mpsc::channel();
    let same_key_url = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(same_key_url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let first: Vec<String> = redis::cmd("BLPOP")
            .arg("list1")
            .arg("list2")
            .arg("list2")
            .arg("list1")
            .arg("5")
            .query(&mut bg)
            .unwrap();
        let second: Vec<String> = redis::cmd("BLPOP")
            .arg("list1")
            .arg("list2")
            .arg("list2")
            .arg("list1")
            .arg("5")
            .query(&mut bg)
            .unwrap();
        tx.send((first, second)).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("LPUSH")
        .arg("list2")
        .arg("b")
        .query(&mut conn)
        .unwrap();
    let _: i64 = redis::cmd("LPUSH")
        .arg("list1")
        .arg("a")
        .query(&mut conn)
        .unwrap();
    let (first, second) = rx.recv().unwrap();
    assert_eq!(first, vec!["list2", "b"]);
    assert_eq!(second, vec!["list1", "a"]);

    let (tx, rx) = mpsc::channel();
    let move_url = url.clone();
    thread::spawn(move || {
        let client = redis::Client::open(move_url).unwrap();
        let mut bg = client.get_connection().unwrap();
        let value: String = redis::cmd("BRPOPLPUSH")
            .arg("src-compat")
            .arg("dst-compat")
            .arg("0")
            .query(&mut bg)
            .unwrap();
        tx.send(value).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let _: i64 = redis::cmd("LPUSH")
        .arg("src-compat")
        .arg("one")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rx.recv().unwrap(), "one");
    assert_eq!(
        redis::cmd("LRANGE")
            .arg("dst-compat")
            .arg(0)
            .arg(-1)
            .query::<Vec<String>>(&mut conn)
            .unwrap(),
        vec!["one"]
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn list_error_and_stress_compat() {
    let mut conn = must_connect();
    flush(&mut conn);
    let _: String = redis::cmd("SET")
        .arg("s")
        .arg("value")
        .query(&mut conn)
        .unwrap();

    for cmd in [
        "LPUSH",
        "RPUSH",
        "LPUSHX",
        "RPUSHX",
        "LPOP",
        "RPOP",
        "LLEN",
        "LRANGE",
        "LINDEX",
        "LSET",
        "LINSERT",
        "LREM",
        "LTRIM",
        "LPOS",
        "LMOVE",
        "RPOPLPUSH",
        "BLPOP",
        "BRPOP",
        "BLMOVE",
        "BRPOPLPUSH",
        "LMPOP",
        "BLMPOP",
    ] {
        let result = match cmd {
            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => {
                redis::cmd(cmd).arg("s").arg("x").query::<Value>(&mut conn)
            }
            "LPOP" | "RPOP" | "LLEN" => redis::cmd(cmd).arg("s").query::<Value>(&mut conn),
            "LRANGE" => redis::cmd(cmd)
                .arg("s")
                .arg(0)
                .arg(1)
                .query::<Value>(&mut conn),
            "LINDEX" => redis::cmd(cmd).arg("s").arg(0).query::<Value>(&mut conn),
            "LSET" => redis::cmd(cmd)
                .arg("s")
                .arg(0)
                .arg("x")
                .query::<Value>(&mut conn),
            "LINSERT" => redis::cmd(cmd)
                .arg("s")
                .arg("BEFORE")
                .arg("x")
                .arg("y")
                .query::<Value>(&mut conn),
            "LREM" => redis::cmd(cmd)
                .arg("s")
                .arg(0)
                .arg("x")
                .query::<Value>(&mut conn),
            "LTRIM" => redis::cmd(cmd)
                .arg("s")
                .arg(0)
                .arg(1)
                .query::<Value>(&mut conn),
            "LPOS" => redis::cmd(cmd).arg("s").arg("x").query::<Value>(&mut conn),
            "LMOVE" => redis::cmd(cmd)
                .arg("s")
                .arg("d")
                .arg("LEFT")
                .arg("RIGHT")
                .query::<Value>(&mut conn),
            "RPOPLPUSH" => redis::cmd(cmd).arg("s").arg("d").query::<Value>(&mut conn),
            "BLPOP" | "BRPOP" => redis::cmd(cmd).arg("s").arg("0").query::<Value>(&mut conn),
            "BLMOVE" => redis::cmd(cmd)
                .arg("s")
                .arg("d")
                .arg("LEFT")
                .arg("RIGHT")
                .arg("0")
                .query::<Value>(&mut conn),
            "BRPOPLPUSH" => redis::cmd(cmd)
                .arg("s")
                .arg("d")
                .arg("0")
                .query::<Value>(&mut conn),
            "LMPOP" => redis::cmd(cmd)
                .arg(1)
                .arg("s")
                .arg("LEFT")
                .arg("COUNT")
                .arg(1)
                .query::<Value>(&mut conn),
            _ => redis::cmd(cmd)
                .arg("0")
                .arg(1)
                .arg("s")
                .arg("LEFT")
                .arg("COUNT")
                .arg(1)
                .query::<Value>(&mut conn),
        };
        assert!(result.unwrap_err().to_string().contains("WRONGTYPE"));
    }

    let err = redis::cmd("BLPOP")
        .arg("k")
        .arg("-1")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(err.to_string().contains("ERR timeout is negative"));
    let err = redis::cmd("LMPOP")
        .arg(3)
        .arg("a")
        .arg("b")
        .arg("LEFT")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("ERR numkeys does not match number of keys"));
    let err = redis::cmd("BLMPOP")
        .arg("1")
        .arg(3)
        .arg("a")
        .arg("b")
        .arg("LEFT")
        .query::<Value>(&mut conn)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("ERR numkeys does not match number of keys"));

    for (cmd, args) in [
        ("LPUSH", vec!["only-key"]),
        ("RPUSH", vec!["only-key"]),
        ("LPUSHX", vec!["only-key"]),
        ("RPUSHX", vec!["only-key"]),
        ("LLEN", Vec::<&str>::new()),
        ("LRANGE", vec!["only-key", "0"]),
        ("LINDEX", vec!["only-key"]),
        ("LSET", vec!["only-key", "0"]),
        ("LINSERT", vec!["only-key", "BEFORE", "pivot"]),
        ("LREM", vec!["only-key", "0"]),
        ("LTRIM", vec!["only-key", "0"]),
        ("LMOVE", vec!["src", "dst", "LEFT"]),
        ("RPOPLPUSH", vec!["src"]),
        ("BLPOP", vec!["only-key"]),
        ("BRPOP", vec!["only-key"]),
        ("BLMOVE", vec!["src", "dst", "LEFT", "RIGHT"]),
        ("BRPOPLPUSH", vec!["src", "dst"]),
        ("LMPOP", vec!["1", "only-key"]),
        ("BLMPOP", vec!["1", "1", "only-key"]),
    ] {
        let mut command = redis::cmd(cmd);
        for arg in args {
            command.arg(arg);
        }
        assert_err_contains(
            command.query::<Value>(&mut conn),
            "wrong number of arguments",
        );
    }

    assert_err_contains(
        redis::cmd("LSET")
            .arg("missing-list")
            .arg(0)
            .arg("x")
            .query::<Value>(&mut conn),
        "ERR no such key",
    );
    assert_err_contains(
        redis::cmd("LINSERT")
            .arg("missing-list")
            .arg("MIDDLE")
            .arg("pivot")
            .arg("x")
            .query::<Value>(&mut conn),
        "ERR syntax error",
    );
    assert_err_contains(
        redis::cmd("LMPOP")
            .arg(0)
            .arg("x")
            .arg("LEFT")
            .query::<Value>(&mut conn),
        "ERR numkeys",
    );

    let _: i64 = redis::cmd("DEL").arg("big").query(&mut conn).unwrap();
    let mut pipe = redis::pipe();
    for i in 0..100_000 {
        pipe.cmd("RPUSH").arg("big").arg(i.to_string()).ignore();
    }
    let _: () = pipe.query(&mut conn).unwrap();
    let all: Vec<String> = redis::cmd("LRANGE")
        .arg("big")
        .arg(0)
        .arg(-1)
        .query(&mut conn)
        .unwrap();
    assert_eq!(all.len(), 100_000);
    let removed: i64 = redis::cmd("LREM")
        .arg("big")
        .arg(0)
        .arg("nonexistent")
        .query(&mut conn)
        .unwrap();
    assert_eq!(removed, 0);
    let ok: String = redis::cmd("LTRIM")
        .arg("big")
        .arg(10_000)
        .arg(89_999)
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let len: i64 = redis::cmd("LLEN").arg("big").query(&mut conn).unwrap();
    assert_eq!(len, 80_000);
}
