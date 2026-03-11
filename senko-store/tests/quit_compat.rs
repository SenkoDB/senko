use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use redis::Connection;

fn redis_url() -> String {
    std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn connect() -> Option<Connection> {
    let client = redis::Client::open(redis_url()).ok()?;
    client.get_connection().ok()
}

fn must_connect() -> Connection {
    connect().unwrap_or_else(|| panic!("compat test requires running Senko at SENKO_REDIS_URL"))
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

fn raw_socket() -> TcpStream {
    let url = redis_url();
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
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    stream
}

fn send_resp(stream: &mut TcpStream, parts: &[&str]) {
    let mut payload = format!("*{}\r\n", parts.len());
    for part in parts {
        payload.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(payload.as_bytes()).unwrap();
}

fn read_raw(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut buf = [0u8; 8192];
    match stream.read(&mut buf) {
        Ok(0) => None,
        Ok(count) => Some(buf[..count].to_vec()),
        Err(err)
            if err.kind() == std::io::ErrorKind::WouldBlock
                || err.kind() == std::io::ErrorKind::TimedOut =>
        {
            None
        }
        Err(err) => panic!("socket read failed: {err}"),
    }
}

fn read_text(stream: &mut TcpStream) -> Option<String> {
    read_raw(stream).map(|bytes| String::from_utf8(bytes).unwrap())
}

#[test]
#[ignore = "requires running Senko instance"]
fn quit_returns_ok_and_closes_connection() {
    let mut conn = must_connect();
    flush(&mut conn);

    let quit: String = redis::cmd("QUIT").query(&mut conn).unwrap();
    assert_eq!(quit, "OK");
    assert!(redis::cmd("PING").query::<String>(&mut conn).is_err());
}

#[test]
#[ignore = "requires running Senko instance"]
fn pipelined_commands_after_quit_are_not_executed() {
    let mut cleanup = must_connect();
    flush(&mut cleanup);

    let mut stream = raw_socket();
    send_resp(&mut stream, &["QUIT"]);
    send_resp(&mut stream, &["SET", "foo", "bar"]);

    assert_eq!(read_text(&mut stream).as_deref(), Some("+OK\r\n"));
    assert!(read_text(&mut stream).is_none());

    let mut verify = must_connect();
    let value: Option<String> = redis::cmd("GET").arg("foo").query(&mut verify).unwrap();
    assert_eq!(value, None);
}

#[test]
#[ignore = "requires running Senko instance"]
fn pipelined_large_command_after_quit_is_not_executed() {
    let mut cleanup = must_connect();
    flush(&mut cleanup);

    let large = "x".repeat(1024);
    let mut stream = raw_socket();
    send_resp(&mut stream, &["QUIT"]);
    send_resp(&mut stream, &["SET", "foo", &large]);

    assert_eq!(read_text(&mut stream).as_deref(), Some("+OK\r\n"));
    assert!(read_text(&mut stream).is_none());

    let mut verify = must_connect();
    let value: Option<String> = redis::cmd("GET").arg("foo").query(&mut verify).unwrap();
    assert_eq!(value, None);
}
