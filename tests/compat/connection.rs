use std::{
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};

use redis::{Connection, RedisResult, Value};

fn connect_with_env(var: &str, default: Option<&str>) -> Option<Connection> {
    let url = std::env::var(var)
        .ok()
        .or_else(|| default.map(str::to_owned))?;
    let client = redis::Client::open(url).ok()?;
    client.get_connection().ok()
}

fn connect() -> Option<Connection> {
    connect_with_env("senko_REDIS_URL", Some("redis://127.0.0.1:6379/"))
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

fn auth_connect() -> Option<Connection> {
    connect_with_env("senko_AUTH_REDIS_URL", None)
}

fn auth_connect_noauth() -> Option<Connection> {
    connect_with_env("senko_AUTH_REDIS_URL_NOAUTH", None)
}

fn raw_socket() -> TcpStream {
    raw_socket_with_env("senko_REDIS_URL", "redis://127.0.0.1:6379/")
}

fn raw_socket_with_env(var: &str, default: &str) -> TcpStream {
    let url = std::env::var(var).unwrap_or_else(|_| default.to_string());
    let addr = url
        .trim_start_matches("redis://")
        .split('@')
        .next_back()
        .unwrap()
        .split('/')
        .next()
        .unwrap();
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    stream
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

fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, expected: &str) {
    let err = result.unwrap_err();
    let rendered = err.to_string();
    let normalized = expected.strip_prefix("ERR ").unwrap_or(expected);
    let redis_rs_rendered = rendered
        .strip_prefix('"')
        .and_then(|value| value.split_once("\": "))
        .map(|(kind, message)| format!("{kind} {message}"));
    assert!(
        rendered.contains(expected)
            || rendered.contains(normalized)
            || redis_rs_rendered
                .as_ref()
                .is_some_and(|value| value.contains(expected) || value.contains(normalized)),
        "expected error containing {expected:?}, got {err}"
    );
}

fn send_resp(stream: &mut TcpStream, parts: &[&str]) {
    let mut payload = format!("*{}\r\n", parts.len());
    for part in parts {
        payload.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(payload.as_bytes()).unwrap();
}

fn read_raw(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut buf = [0u8; 4096];
    match stream.read(&mut buf) {
        Ok(0) => None,
        Ok(n) => Some(buf[..n].to_vec()),
        Err(err)
            if err.kind() == std::io::ErrorKind::WouldBlock
                || err.kind() == std::io::ErrorKind::TimedOut =>
        {
            None
        }
        Err(err) => panic!("read failed: {err}"),
    }
}

fn read_text(stream: &mut TcpStream) -> String {
    String::from_utf8(read_raw(stream).expect("expected response bytes")).unwrap()
}

fn expect_no_bytes(stream: &mut TcpStream) {
    assert!(read_raw(stream).is_none(), "expected no response bytes");
}

fn read_integer_reply(stream: &mut TcpStream) -> i64 {
    let reply = read_text(stream);
    let value = reply
        .strip_prefix(':')
        .and_then(|text| text.strip_suffix("\r\n"))
        .expect("expected integer reply");
    value.parse::<i64>().unwrap()
}

fn client_id_raw(stream: &mut TcpStream) -> i64 {
    send_resp(stream, &["CLIENT", "ID"]);
    read_integer_reply(stream)
}

fn socket_ping(stream: &mut TcpStream) -> String {
    send_resp(stream, &["PING"]);
    read_text(stream)
}

#[test]
#[ignore = "requires running senko instance"]
fn connection_basic_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let pong: String = redis::cmd("PING").query(&mut conn).unwrap();
    assert_eq!(pong, "PONG");

    let echoed: String = redis::cmd("PING").arg("hello").query(&mut conn).unwrap();
    assert_eq!(echoed, "hello");

    let echoed: String = redis::cmd("ECHO").arg("test").query(&mut conn).unwrap();
    assert_eq!(echoed, "test");

    assert_err_contains(
        redis::cmd("ECHO").query::<Value>(&mut conn),
        "ERR wrong number of arguments for 'echo' command",
    );

    let ok: String = redis::cmd("SELECT").arg(0).query(&mut conn).unwrap();
    assert_eq!(ok, "OK");
    assert_err_contains(
        redis::cmd("SELECT").arg(1).query::<Value>(&mut conn),
        "ERR DB index is out of range",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn connection_quit_compat() {
    let mut conn = must_connect();
    let quit: String = redis::cmd("QUIT").query(&mut conn).unwrap();
    assert_eq!(quit, "OK");
    assert!(redis::cmd("PING").query::<String>(&mut conn).is_err());
}

#[test]
#[ignore = "requires running senko instance"]
fn connection_reset_clears_state_compat() {
    let mut stream = raw_socket();

    send_resp(&mut stream, &["HELLO", "3"]);
    let hello = read_text(&mut stream);
    assert!(hello.starts_with('%'));
    assert!(hello.contains("server"));
    assert!(hello.contains("proto"));

    send_resp(&mut stream, &["CLIENT", "SETNAME", "reset-me"]);
    assert_eq!(read_text(&mut stream), "+OK\r\n");

    send_resp(&mut stream, &["MULTI"]);
    assert_eq!(read_text(&mut stream), "+OK\r\n");

    send_resp(&mut stream, &["CLIENT", "REPLY", "SKIP"]);
    expect_no_bytes(&mut stream);

    send_resp(&mut stream, &["RESET"]);
    assert_eq!(read_text(&mut stream), "+RESET\r\n");

    send_resp(&mut stream, &["CLIENT", "GETNAME"]);
    assert_eq!(read_text(&mut stream), "$-1\r\n");

    send_resp(&mut stream, &["SET", "conn:reset", "1"]);
    assert_eq!(read_text(&mut stream), "+OK\r\n");

    send_resp(&mut stream, &["HSET", "conn:hash", "field", "value"]);
    let hset_reply = read_text(&mut stream);
    assert!(hset_reply.starts_with(':'));

    send_resp(&mut stream, &["HGETALL", "conn:hash"]);
    let hgetall = read_text(&mut stream);
    assert!(
        hgetall.starts_with('*'),
        "RESET should switch back to RESP2"
    );

    assert_eq!(socket_ping(&mut stream), "+PONG\r\n");
}

#[test]
#[ignore = "requires running senko auth-enabled instance"]
fn connection_auth_compat() {
    let Some(mut authed) = auth_connect() else {
        return;
    };
    let Some(mut noauth) = auth_connect_noauth() else {
        return;
    };

    flush(&mut authed);

    assert_err_contains(
        redis::cmd("GET").arg("foo").query::<Value>(&mut noauth),
        "NOAUTH Authentication required.",
    );
    assert_err_contains(
        redis::cmd("AUTH").arg("wrong").query::<Value>(&mut noauth),
        "WRONGPASS invalid username-password pair or user is disabled.",
    );

    let password = std::env::var("senko_REQUIREPASS").unwrap();
    let ok: String = redis::cmd("AUTH")
        .arg(&password)
        .query(&mut noauth)
        .unwrap();
    assert_eq!(ok, "OK");

    let ok: String = redis::cmd("SET")
        .arg("conn:auth")
        .arg("v")
        .query(&mut noauth)
        .unwrap();
    assert_eq!(ok, "OK");

    let Some(mut userpass) = auth_connect_noauth() else {
        return;
    };
    let ok: String = redis::cmd("AUTH")
        .arg("default")
        .arg(&password)
        .query(&mut userpass)
        .unwrap();
    assert_eq!(ok, "OK");

    let Some(mut bad_user) = auth_connect_noauth() else {
        return;
    };
    assert_err_contains(
        redis::cmd("AUTH")
            .arg("not-default")
            .arg(&password)
            .query::<Value>(&mut bad_user),
        "WRONGPASS invalid username-password pair or user is disabled.",
    );
}

#[test]
#[ignore = "requires running senko instance without requirepass"]
fn connection_auth_without_password_configured_compat() {
    let mut conn = must_connect();
    assert_err_contains(
        redis::cmd("AUTH").arg("anything").query::<Value>(&mut conn),
        "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn connection_hello_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let hello2: Value = redis::cmd("HELLO").arg(2).query(&mut conn).unwrap();
    match hello2 {
        Value::Array(entries) => {
            let rendered = format!("{entries:?}");
            assert!(rendered.contains("server"));
            assert!(rendered.contains("version"));
            assert!(rendered.contains("proto"));
            assert!(rendered.contains("id"));
            assert!(rendered.contains("mode"));
            assert!(rendered.contains("role"));
            assert!(rendered.contains("modules"));
        }
        other => panic!("expected RESP2 array, got {other:?}"),
    }

    let hello3: Value = redis::cmd("HELLO").arg(3).query(&mut conn).unwrap();
    match hello3 {
        Value::Map(entries) => {
            let rendered = format!("{entries:?}");
            assert!(rendered.contains("server"));
            assert!(rendered.contains("version"));
            assert!(rendered.contains("proto"));
            assert!(rendered.contains("id"));
            assert!(rendered.contains("mode"));
            assert!(rendered.contains("role"));
            assert!(rendered.contains("modules"));
        }
        other => panic!("expected RESP3 map, got {other:?}"),
    }

    let _: i64 = redis::cmd("HSET")
        .arg("conn:hello:hash")
        .arg("field")
        .arg("value")
        .query(&mut conn)
        .unwrap();
    let hgetall: Value = redis::cmd("HGETALL")
        .arg("conn:hello:hash")
        .query(&mut conn)
        .unwrap();
    assert!(matches!(hgetall, Value::Map(_)));

    assert_err_contains(
        redis::cmd("HELLO").arg(4).query::<Value>(&mut conn),
        "NOPROTO unsupported protocol version",
    );
}

#[test]
#[ignore = "requires running senko auth-enabled instance"]
fn connection_inline_hello_auth_compat() {
    let password = match std::env::var("senko_REQUIREPASS") {
        Ok(password) => password,
        Err(_) => return,
    };

    let Some(mut inline_noauth) = auth_connect_noauth() else {
        return;
    };
    let hello: Value = redis::cmd("HELLO")
        .arg(3)
        .arg("AUTH")
        .arg("default")
        .arg(&password)
        .query(&mut inline_noauth)
        .unwrap();
    assert!(matches!(hello, Value::Map(_)));

    let _: String = redis::cmd("PING").query(&mut inline_noauth).unwrap();

    let Some(mut bad_inline) = auth_connect_noauth() else {
        return;
    };
    assert_err_contains(
        redis::cmd("HELLO")
            .arg(3)
            .arg("AUTH")
            .arg("default")
            .arg("bad")
            .query::<Value>(&mut bad_inline),
        "WRONGPASS invalid username-password pair or user is disabled.",
    );
    assert_err_contains(
        redis::cmd("PING").query::<Value>(&mut bad_inline),
        "NOAUTH Authentication required.",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn client_basics_compat() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    flush(&mut conn_a);

    let id_a: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn_a).unwrap();
    let id_b: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn_b).unwrap();
    assert!(id_a > 0);
    assert!(id_b > 0);
    assert_ne!(id_a, id_b);

    let ok: String = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("myconn")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let name: String = redis::cmd("CLIENT")
        .arg("GETNAME")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(name, "myconn");

    let ok: String = redis::cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-NAME")
        .arg("redis-rs")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let ok: String = redis::cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-VER")
        .arg("1.0.0")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");

    let info: String = redis::cmd("CLIENT").arg("INFO").query(&mut conn_a).unwrap();
    for field in [
        "id=",
        "addr=",
        "laddr=",
        "name=myconn",
        "flags=",
        "db=0",
        "cmd=client|info",
        "user=default",
        "library-name=redis-rs",
        "library-ver=1.0.0",
    ] {
        assert!(info.contains(field), "CLIENT INFO missing {field}");
    }

    let list: String = redis::cmd("CLIENT").arg("LIST").query(&mut conn_a).unwrap();
    assert!(list.contains(&format!("id={id_a}")));
    assert!(list.contains("name=myconn"));
    assert!(list.ends_with('\n'));

    let filtered: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("ID")
        .arg(id_a)
        .query(&mut conn_a)
        .unwrap();
    assert!(filtered.contains(&format!("id={id_a}")));
    assert!(!filtered.contains(&format!("id={id_b}")));

    let normal_only: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("NORMAL")
        .query(&mut conn_a)
        .unwrap();
    assert!(normal_only.contains(&format!("id={id_a}")));

    let ok: String = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(ok, "OK");
    let cleared: Option<String> = redis::cmd("CLIENT")
        .arg("GETNAME")
        .query(&mut conn_a)
        .unwrap();
    assert!(cleared.is_none());

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("bad name")
            .query::<Value>(&mut conn_a),
        "ERR Client names cannot contain spaces, newlines or special characters.",
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn client_reply_modes_compat() {
    let mut stream = raw_socket();

    send_resp(&mut stream, &["CLIENT", "REPLY", "OFF"]);
    expect_no_bytes(&mut stream);

    send_resp(&mut stream, &["SET", "client:reply", "1"]);
    expect_no_bytes(&mut stream);

    send_resp(&mut stream, &["CLIENT", "REPLY", "ON"]);
    assert_eq!(read_text(&mut stream), "+OK\r\n");

    send_resp(&mut stream, &["CLIENT", "REPLY", "SKIP"]);
    expect_no_bytes(&mut stream);

    send_resp(&mut stream, &["SET", "client:reply", "2"]);
    expect_no_bytes(&mut stream);

    send_resp(&mut stream, &["GET", "client:reply"]);
    assert_eq!(read_text(&mut stream), "$1\r\n2\r\n");
}

#[test]
#[ignore = "requires running senko instance"]
fn client_no_evict_caching_getredir_trackinginfo_compat() {
    let mut conn = must_connect();

    let ok: String = redis::cmd("CLIENT")
        .arg("NO-EVICT")
        .arg("ON")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");
    let ok: String = redis::cmd("CLIENT")
        .arg("NO-EVICT")
        .arg("OFF")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("CACHING")
            .arg("YES")
            .query::<Value>(&mut conn),
        "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
    );

    let redir: i64 = redis::cmd("CLIENT")
        .arg("GETREDIR")
        .query(&mut conn)
        .unwrap();
    assert_eq!(redir, -1);

    let info: Value = redis::cmd("CLIENT")
        .arg("TRACKINGINFO")
        .query(&mut conn)
        .unwrap();
    let rendered = format!("{info:?}");
    assert!(rendered.contains("off"));
    assert!(rendered.contains("-1"));
}

#[test]
#[ignore = "requires running senko instance; sleeps for LRU granularity"]
fn client_no_touch_preserves_object_idletime_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let _: String = redis::cmd("SET")
        .arg("client:notouch")
        .arg("value")
        .query(&mut conn)
        .unwrap();
    thread::sleep(Duration::from_secs(11));

    let before: i64 = redis::cmd("OBJECT")
        .arg("IDLETIME")
        .arg("client:notouch")
        .query(&mut conn)
        .unwrap();
    assert!(before >= 10);

    let _: String = redis::cmd("CLIENT")
        .arg("NO-TOUCH")
        .arg("ON")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("GET")
        .arg("client:notouch")
        .query(&mut conn)
        .unwrap();
    let after_no_touch: i64 = redis::cmd("OBJECT")
        .arg("IDLETIME")
        .arg("client:notouch")
        .query(&mut conn)
        .unwrap();
    assert!(after_no_touch >= 10);

    let _: String = redis::cmd("CLIENT")
        .arg("NO-TOUCH")
        .arg("OFF")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("GET")
        .arg("client:notouch")
        .query(&mut conn)
        .unwrap();
    let after_touch: i64 = redis::cmd("OBJECT")
        .arg("IDLETIME")
        .arg("client:notouch")
        .query(&mut conn)
        .unwrap();
    assert!(
        after_touch <= 1,
        "idletime should reset when NO-TOUCH is OFF"
    );
}

#[test]
#[ignore = "requires running senko instance"]
fn client_kill_compat() {
    let mut conn_a = must_connect();
    let mut conn_b = must_connect();
    let mut conn_c = must_connect();

    let id_a: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn_a).unwrap();
    let id_b: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn_b).unwrap();
    let _id_c: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn_c).unwrap();

    let killed_self_skipped: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(id_b)
        .query(&mut conn_b)
        .unwrap();
    assert_eq!(killed_self_skipped, 0);
    let _: String = redis::cmd("PING").query(&mut conn_b).unwrap();

    let killed: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(id_a)
        .query(&mut conn_b)
        .unwrap();
    assert_eq!(killed, 1);
    assert!(redis::cmd("PING").query::<String>(&mut conn_a).is_err());

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("KILL")
            .arg("TYPE")
            .arg("NORMAL")
            .arg("MAXAGE")
            .arg(0)
            .query::<Value>(&mut conn_b),
        "ERR maxage should be greater than 0",
    );
    let _: String = redis::cmd("PING").query(&mut conn_c).unwrap();
}

#[test]
#[ignore = "requires running senko instance"]
fn client_pause_unpause_compat() {
    let mut conn_a = must_connect();
    flush(&mut conn_a);

    let pause_ok: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("ALL")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(pause_ok, "OK");

    let start = Instant::now();
    let join = thread::spawn(move || {
        let mut conn_b = must_connect();
        let _: String = redis::cmd("SET")
            .arg("client:pause:all")
            .arg("1")
            .query(&mut conn_b)
            .unwrap();
        start.elapsed()
    });
    let elapsed = join.join().unwrap();
    assert!(elapsed >= Duration::from_millis(200));

    let pause_ok: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1_000)
        .arg("ALL")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(pause_ok, "OK");

    let join = thread::spawn(|| {
        let mut conn_b = must_connect();
        let start = Instant::now();
        let _: String = redis::cmd("SET")
            .arg("client:pause:unpause")
            .arg("1")
            .query(&mut conn_b)
            .unwrap();
        start.elapsed()
    });
    thread::sleep(Duration::from_millis(100));
    let unpause_ok: String = redis::cmd("CLIENT")
        .arg("UNPAUSE")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(unpause_ok, "OK");
    let elapsed = join.join().unwrap();
    assert!(elapsed < Duration::from_millis(500));

    let pause_ok: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(400)
        .arg("WRITE")
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(pause_ok, "OK");

    let mut conn_b = must_connect();
    let read_start = Instant::now();
    let _: Option<String> = redis::cmd("GET")
        .arg("client:pause:all")
        .query(&mut conn_b)
        .unwrap();
    assert!(read_start.elapsed() < Duration::from_millis(100));

    let write_start = Instant::now();
    let _: String = redis::cmd("SET")
        .arg("client:pause:write")
        .arg("1")
        .query(&mut conn_b)
        .unwrap();
    assert!(write_start.elapsed() >= Duration::from_millis(250));

    let pause_zero: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(0)
        .query(&mut conn_a)
        .unwrap();
    assert_eq!(pause_zero, "OK");
}

#[test]
#[ignore = "requires running senko instance"]
fn client_unblock_compat() {
    let mut blocked_stream = raw_socket();
    let blocked_id = client_id_raw(&mut blocked_stream);

    send_resp(&mut blocked_stream, &["BLPOP", "client:unblock", "30"]);
    thread::sleep(Duration::from_millis(100));

    let mut control = must_connect();
    let unblocked: i64 = redis::cmd("CLIENT")
        .arg("UNBLOCK")
        .arg(blocked_id)
        .query(&mut control)
        .unwrap();
    assert_eq!(unblocked, 1);
    let timed_out = read_text(&mut blocked_stream);
    assert!(timed_out.starts_with("*-1\r\n") || timed_out.starts_with("$-1\r\n"));

    let mut blocked_stream = raw_socket();
    let blocked_id = client_id_raw(&mut blocked_stream);
    send_resp(
        &mut blocked_stream,
        &["BLPOP", "client:unblock:error", "30"],
    );
    thread::sleep(Duration::from_millis(100));

    let errored: i64 = redis::cmd("CLIENT")
        .arg("UNBLOCK")
        .arg(blocked_id)
        .arg("ERROR")
        .query(&mut control)
        .unwrap();
    assert_eq!(errored, 1);
    let error = read_text(&mut blocked_stream);
    assert!(error.contains("UNBLOCKED client unblocked via CLIENT UNBLOCK"));

    let missing: i64 = redis::cmd("CLIENT")
        .arg("UNBLOCK")
        .arg(9_999_999)
        .query(&mut control)
        .unwrap();
    assert_eq!(missing, 0);

    let self_unblock: i64 = redis::cmd("CLIENT")
        .arg("UNBLOCK")
        .arg(
            redis::cmd("CLIENT")
                .arg("ID")
                .query::<i64>(&mut control)
                .unwrap(),
        )
        .query(&mut control)
        .unwrap();
    assert_eq!(self_unblock, 0);
}

#[test]
#[ignore = "requires running senko instance"]
fn client_tracking_compat() {
    let mut reader = raw_socket();
    send_resp(&mut reader, &["HELLO", "3"]);
    let hello = read_text(&mut reader);
    assert!(hello.starts_with('%'));

    let mut writer = must_connect();
    flush(&mut writer);

    send_resp(&mut reader, &["CLIENT", "TRACKING", "ON"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");

    let _: String = redis::cmd("SET")
        .arg("track:foo")
        .arg("one")
        .query(&mut writer)
        .unwrap();
    send_resp(&mut reader, &["GET", "track:foo"]);
    let get_reply = read_text(&mut reader);
    assert!(get_reply.contains("one"));
    let _: String = redis::cmd("SET")
        .arg("track:foo")
        .arg("two")
        .query(&mut writer)
        .unwrap();
    let invalidation = read_text(&mut reader);
    assert!(invalidation.starts_with('>'));
    assert!(invalidation.contains("invalidate"));
    assert!(invalidation.contains("track:foo"));

    let _: String = redis::cmd("SET")
        .arg("track:foo")
        .arg("three")
        .query(&mut writer)
        .unwrap();
    expect_no_bytes(&mut reader);

    send_resp(&mut reader, &["GET", "track:foo"]);
    let _ = read_text(&mut reader);
    let _: String = redis::cmd("SET")
        .arg("track:foo")
        .arg("four")
        .query(&mut writer)
        .unwrap();
    let invalidation = read_text(&mut reader);
    assert!(invalidation.contains("track:foo"));

    send_resp(&mut reader, &["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");

    send_resp(&mut reader, &["CLIENT", "TRACKING", "ON", "NOLOOP"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["GET", "track:noloop"]);
    let _ = read_text(&mut reader);
    send_resp(&mut reader, &["SET", "track:noloop", "mine"]);
    let set_reply = read_text(&mut reader);
    assert_eq!(set_reply, "+OK\r\n");
    expect_no_bytes(&mut reader);

    send_resp(&mut reader, &["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");

    send_resp(&mut reader, &["CLIENT", "TRACKING", "ON", "OPTIN"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["GET", "track:optin"]);
    let _ = read_text(&mut reader);
    let _: String = redis::cmd("SET")
        .arg("track:optin")
        .arg("writer")
        .query(&mut writer)
        .unwrap();
    expect_no_bytes(&mut reader);
    send_resp(&mut reader, &["CLIENT", "CACHING", "YES"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["GET", "track:optin"]);
    let _ = read_text(&mut reader);
    let _: String = redis::cmd("SET")
        .arg("track:optin")
        .arg("writer2")
        .query(&mut writer)
        .unwrap();
    assert!(read_text(&mut reader).contains("track:optin"));

    send_resp(&mut reader, &["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");

    send_resp(&mut reader, &["CLIENT", "TRACKING", "ON", "OPTOUT"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["CLIENT", "CACHING", "NO"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["GET", "track:optout"]);
    let _ = read_text(&mut reader);
    let _: String = redis::cmd("SET")
        .arg("track:optout")
        .arg("writer")
        .query(&mut writer)
        .unwrap();
    expect_no_bytes(&mut reader);

    let mut redirect = raw_socket();
    send_resp(&mut redirect, &["HELLO", "3"]);
    let _ = read_text(&mut redirect);
    let redirect_id = client_id_raw(&mut redirect);

    send_resp(
        &mut reader,
        &[
            "CLIENT",
            "TRACKING",
            "ON",
            "REDIRECT",
            &redirect_id.to_string(),
        ],
    );
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    send_resp(&mut reader, &["GET", "track:redir"]);
    let _ = read_text(&mut reader);
    let _: String = redis::cmd("SET")
        .arg("track:redir")
        .arg("writer")
        .query(&mut writer)
        .unwrap();
    let redirected = read_text(&mut redirect);
    assert!(redirected.contains("track:redir"));
    expect_no_bytes(&mut reader);

    send_resp(&mut reader, &["CLIENT", "TRACKING", "OFF"]);
    assert_eq!(read_text(&mut reader), "+OK\r\n");

    send_resp(
        &mut reader,
        &[
            "CLIENT",
            "TRACKING",
            "ON",
            "BCAST",
            "PREFIX",
            "track:bcast:",
        ],
    );
    assert_eq!(read_text(&mut reader), "+OK\r\n");
    let tracking_info = redis::cmd("CLIENT")
        .arg("TRACKINGINFO")
        .query::<Value>(&mut writer)
        .unwrap();
    let rendered = format!("{tracking_info:?}");
    assert!(rendered.contains("off") || rendered.contains("on"));
    let _: String = redis::cmd("SET")
        .arg("track:bcast:key")
        .arg("writer")
        .query(&mut writer)
        .unwrap();
    let bcast = read_text(&mut reader);
    assert!(bcast.contains("track:bcast:key"));
}

#[test]
#[ignore = "requires running senko instance"]
fn client_error_cases_compat() {
    let mut conn = must_connect();

    let cases: [(&[&str], &str); 12] = [
        (
            &["ID", "extra"],
            "ERR wrong number of arguments for 'client|id' command",
        ),
        (
            &["GETNAME", "extra"],
            "ERR wrong number of arguments for 'client|getname' command",
        ),
        (
            &["SETNAME"],
            "ERR wrong number of arguments for 'client|setname' command",
        ),
        (
            &["SETINFO"],
            "ERR wrong number of arguments for 'client|setinfo' command",
        ),
        (
            &["INFO", "extra"],
            "ERR wrong number of arguments for 'client|info' command",
        ),
        (&["LIST", "TYPE"], "ERR syntax error"),
        (
            &["NO-EVICT"],
            "ERR wrong number of arguments for 'client|no-evict' command",
        ),
        (
            &["NO-TOUCH"],
            "ERR wrong number of arguments for 'client|no-touch' command",
        ),
        (
            &["REPLY"],
            "ERR wrong number of arguments for 'client|reply' command",
        ),
        (
            &["GETREDIR", "extra"],
            "ERR wrong number of arguments for 'client|getredir' command",
        ),
        (
            &["TRACKINGINFO", "extra"],
            "ERR wrong number of arguments for 'client|trackinginfo' command",
        ),
        (
            &["UNBLOCK"],
            "ERR wrong number of arguments for 'client|unblock' command",
        ),
    ];

    for (args, expected) in cases {
        let mut cmd = redis::cmd("CLIENT");
        for arg in args {
            cmd.arg(arg);
        }
        assert_err_contains(cmd.query::<Value>(&mut conn), expected);
    }

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("UNKNOWN_SUBCOMMAND")
            .query::<Value>(&mut conn),
        "ERR unknown subcommand 'UNKNOWN_SUBCOMMAND'. Try CLIENT HELP.",
    );

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("CACHING")
            .arg("YES")
            .query::<Value>(&mut conn),
        "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
    );

    assert_err_contains(
        redis::cmd("CLIENT")
            .arg("TRACKING")
            .arg("ON")
            .arg("BCAST")
            .arg("OPTIN")
            .query::<Value>(&mut conn),
        "ERR OPTIN and OPTOUT are not compatible with BCAST",
    );
}
