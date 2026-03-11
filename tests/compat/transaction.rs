use std::{thread, time::Duration};

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

fn bulk_to_string(value: Value) -> String {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes).unwrap(),
        Value::SimpleString(text) => text,
        Value::Okay => "OK".to_string(),
        Value::Int(value) => value.to_string(),
        other => panic!("expected string-ish value, got {other:?}"),
    }
}

fn value_array(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    }
}

fn assert_err_exact<T: std::fmt::Debug>(result: RedisResult<T>, expected: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(expected),
        "expected error containing {expected:?}, got {err}"
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_basic_multi_exec_and_empty_exec() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:a")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:b")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("GET").arg("tx:a").query(&mut conn).unwrap();
    let result: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    let values = value_array(result);
    assert_eq!(values.len(), 3);
    assert_eq!(bulk_to_string(values[0].clone()), "OK");
    assert_eq!(bulk_to_string(values[1].clone()), "OK");
    assert_eq!(bulk_to_string(values[2].clone()), "1");

    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let empty: Value = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert!(value_array(empty).is_empty());
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_discard_and_outside_errors() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:discard")
        .arg("value")
        .query(&mut conn)
        .unwrap();
    let ok: String = redis::cmd("DISCARD").query(&mut conn).unwrap();
    assert_eq!(ok, "OK");
    let missing: Option<String> = redis::cmd("GET")
        .arg("tx:discard")
        .query(&mut conn)
        .unwrap();
    assert!(missing.is_none());

    assert_err_exact(
        redis::cmd("EXEC").query::<Value>(&mut conn),
        "ERR EXEC without MULTI",
    );
    assert_err_exact(
        redis::cmd("DISCARD").query::<Value>(&mut conn),
        "ERR DISCARD without MULTI",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_nested_multi_and_incr_runtime_isolation() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    assert_err_exact(
        redis::cmd("MULTI").query::<Value>(&mut conn),
        "ERR MULTI calls can not be nested",
    );
    let _: Value = redis::cmd("INCR")
        .arg("tx:counter")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("GET")
        .arg("tx:counter")
        .query(&mut conn)
        .unwrap();
    let result = value_array(redis::cmd("EXEC").query(&mut conn).unwrap());
    assert_eq!(result.len(), 2);
    assert_eq!(bulk_to_string(result[0].clone()), "1");
    assert_eq!(bulk_to_string(result[1].clone()), "1");

    let _: String = redis::cmd("SET")
        .arg("tx:foo")
        .arg("notanumber")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    let _: Value = redis::cmd("INCR").arg("tx:foo").query(&mut conn).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:foo")
        .arg("42")
        .query(&mut conn)
        .unwrap();
    let _: Value = redis::cmd("GET").arg("tx:foo").query(&mut conn).unwrap();
    let exec = value_array(redis::cmd("EXEC").query(&mut conn).unwrap());
    assert_eq!(exec.len(), 3);
    assert!(
        matches!(
            exec[0],
            Value::Nil
                | Value::BulkString(_)
                | Value::SimpleString(_)
                | Value::Okay
                | Value::Int(_)
                | Value::Array(_)
                | Value::Map(_)
                | Value::Set(_)
                | Value::Double(_)
                | Value::Boolean(_)
                | Value::VerbatimString { .. }
                | Value::BigNumber(_)
                | Value::Push { .. }
                | Value::ServerError(_)
        ),
        "expected redis value"
    );
    assert_eq!(bulk_to_string(exec[1].clone()), "OK");
    assert_eq!(bulk_to_string(exec[2].clone()), "42");
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_syntax_error_aborts_exec() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: Value = redis::cmd("MULTI").query(&mut conn).unwrap();
    assert_err_exact(
        redis::cmd("HSET")
            .arg("tx:hset-only-key")
            .query::<Value>(&mut conn),
        "wrong number of arguments for 'hset' command",
    );
    let _: Value = redis::cmd("SET")
        .arg("tx:aborted")
        .arg("v")
        .query(&mut conn)
        .unwrap();
    assert_err_exact(
        redis::cmd("EXEC").query::<Value>(&mut conn),
        "EXECABORT Transaction discarded because of previous errors.",
    );
    let value: Option<String> = redis::cmd("GET")
        .arg("tx:aborted")
        .query(&mut conn)
        .unwrap();
    assert!(value.is_none());
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_atomicity_and_pipeline() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    flush(&mut conn_a);

    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:a")
        .arg("1")
        .query(&mut conn_a)
        .unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:b")
        .arg("2")
        .query(&mut conn_a)
        .unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:c")
        .arg("3")
        .query(&mut conn_a)
        .unwrap();

    let seen_a: Option<String> = redis::cmd("GET").arg("tx:a").query(&mut conn_b).unwrap();
    let seen_b: Option<String> = redis::cmd("GET").arg("tx:b").query(&mut conn_b).unwrap();
    let seen_c: Option<String> = redis::cmd("GET").arg("tx:c").query(&mut conn_b).unwrap();
    assert!(seen_a.is_none() && seen_b.is_none() && seen_c.is_none());

    let result = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert_eq!(result.len(), 3);
    assert_eq!(
        redis::cmd("MGET")
            .arg(&["tx:a", "tx:b", "tx:c"])
            .query::<Vec<Option<String>>>(&mut conn_b)
            .unwrap(),
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );

    let mut pipe = redis::pipe();
    pipe.cmd("MULTI")
        .cmd("SET")
        .arg("tx:p")
        .arg("v")
        .cmd("GET")
        .arg("tx:p")
        .cmd("EXEC");
    let responses: Vec<Value> = pipe.query(&mut conn_a).unwrap();
    assert_eq!(responses.len(), 4);
    assert!(matches!(
        responses[0],
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    ));
    assert!(matches!(
        responses[1],
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    ));
    assert!(matches!(
        responses[2],
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    ));
    let exec_values = value_array(responses[3].clone());
    assert_eq!(exec_values.len(), 2);
    assert_eq!(bulk_to_string(exec_values[0].clone()), "OK");
    assert_eq!(bulk_to_string(exec_values[1].clone()), "v");
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_watch_success_failure_and_clearing() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    flush(&mut conn_a);

    let ok: String = redis::cmd("WATCH")
        .arg("tx:watch")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:watch")
        .arg("txval")
        .query(&mut conn_a)
        .unwrap();
    let success = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert_eq!(success.len(), 1);
    assert_eq!(bulk_to_string(success[0].clone()), "OK");
    let stored: String = redis::cmd("GET")
        .arg("tx:watch")
        .query(&mut conn_b)
        .unwrap();
    assert_eq!(stored, "txval");

    let ok: String = redis::cmd("WATCH")
        .arg("tx:watch")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let _: String = redis::cmd("SET")
        .arg("tx:watch")
        .arg("external")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:watch")
        .arg("nope")
        .query(&mut conn_a)
        .unwrap();
    let aborted: Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
    assert!(matches!(aborted, Value::Nil));
    let stored: String = redis::cmd("GET")
        .arg("tx:watch")
        .query(&mut conn_b)
        .unwrap();
    assert_eq!(stored, "external");

    let ok: String = redis::cmd("WATCH")
        .arg("tx:watch")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let discard: String = redis::cmd("DISCARD").query(&mut conn_a).unwrap();
    assert_eq!(discard, "OK");
    let _: String = redis::cmd("SET")
        .arg("tx:watch")
        .arg("after")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let after = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert!(after.is_empty());
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_unwatch_retry_and_cross_connection_cases() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    flush(&mut conn_a);

    let _: String = redis::cmd("SET")
        .arg("tx:k")
        .arg("0")
        .query(&mut conn_a)
        .unwrap();
    let ok: String = redis::cmd("WATCH").arg("tx:k").query(&mut conn_a).unwrap();
    assert_eq!(ok, "OK");
    let ok: String = redis::cmd("UNWATCH").query(&mut conn_a).unwrap();
    assert_eq!(ok, "OK");
    let _: String = redis::cmd("SET")
        .arg("tx:k")
        .arg("5")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let unwatch_exec = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert!(unwatch_exec.is_empty());

    let mut attempts = 0usize;
    loop {
        attempts += 1;
        let _: String = redis::cmd("WATCH")
            .arg("tx:retry")
            .query(&mut conn_a)
            .unwrap();
        let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
        let _: Value = redis::cmd("INCR")
            .arg("tx:retry")
            .query(&mut conn_a)
            .unwrap();
        let exec: Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
        if !matches!(exec, Value::Nil) {
            break;
        }
        assert!(attempts < 8, "retry loop did not succeed");
    }
    let final_value: i64 = redis::cmd("GET")
        .arg("tx:retry")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(final_value, 1);

    let _: String = redis::cmd("WATCH")
        .arg("tx:cross")
        .query(&mut conn_a)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg("tx:cross")
        .arg("newval")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("GET")
        .arg("tx:cross")
        .query(&mut conn_a)
        .unwrap();
    let exec: Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
    assert!(matches!(exec, Value::Nil));
    let current: String = redis::cmd("GET")
        .arg("tx:cross")
        .query(&mut conn_b)
        .unwrap();
    assert_eq!(current, "newval");
}

#[test]
#[ignore = "requires running senko instance"]
fn transaction_watch_edge_cases() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    flush(&mut conn_a);

    let _: String = redis::cmd("WATCH")
        .arg("tx:missing")
        .query(&mut conn_a)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg("tx:missing")
        .arg("created")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:missing")
        .arg("txval")
        .query(&mut conn_a)
        .unwrap();
    let exec: Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
    assert!(matches!(exec, Value::Nil));

    let _: String = redis::cmd("SET")
        .arg("tx:expire")
        .arg("v")
        .arg("EX")
        .arg(1)
        .query(&mut conn_a)
        .unwrap();
    let _: String = redis::cmd("WATCH")
        .arg("tx:expire")
        .query(&mut conn_a)
        .unwrap();
    thread::sleep(Duration::from_secs(2));
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("SET")
        .arg("tx:expire")
        .arg("new")
        .query(&mut conn_a)
        .unwrap();
    let exec: Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
    assert!(matches!(exec, Value::Nil));

    let _: String = redis::cmd("SET")
        .arg("tx:uw")
        .arg("v")
        .query(&mut conn_a)
        .unwrap();
    let _: String = redis::cmd("WATCH").arg("tx:uw").query(&mut conn_a).unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let queued: Value = redis::cmd("UNWATCH").query(&mut conn_a).unwrap();
    assert!(matches!(
        queued,
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    ));
    let exec = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert_eq!(exec.len(), 1);
    assert_eq!(bulk_to_string(exec[0].clone()), "OK");

    let _: String = redis::cmd("WATCH").arg("tx:wm").query(&mut conn_a).unwrap();
    let _: String = redis::cmd("WATCH").arg("tx:wn").query(&mut conn_a).unwrap();
    let _: String = redis::cmd("UNWATCH").query(&mut conn_a).unwrap();
    let _: String = redis::cmd("SET")
        .arg("tx:wm")
        .arg("1")
        .query(&mut conn_b)
        .unwrap();
    let _: String = redis::cmd("SET")
        .arg("tx:wn")
        .arg("2")
        .query(&mut conn_b)
        .unwrap();
    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let both = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert!(both.is_empty());

    let _: Value = redis::cmd("MULTI").query(&mut conn_a).unwrap();
    let watch_queued: Value = redis::cmd("WATCH")
        .arg("tx:inside")
        .query(&mut conn_a)
        .unwrap();
    assert!(matches!(
        watch_queued,
        Value::Okay | Value::SimpleString(_) | Value::BulkString(_)
    ));
    let watch_exec = value_array(redis::cmd("EXEC").query(&mut conn_a).unwrap());
    assert_eq!(watch_exec.len(), 1);
}
