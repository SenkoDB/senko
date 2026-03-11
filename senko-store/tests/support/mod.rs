use redis::{Connection, RedisResult};

pub fn connect() -> Option<Connection> {
    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let client = redis::Client::open(url).ok()?;
    client.get_connection().ok()
}

pub fn must_connect() -> Connection {
    match connect() {
        Some(mut conn) => {
            let _: RedisResult<String> = redis::cmd("PING").query(&mut conn);
            conn
        }
        None => panic!("compat test requires running Senko at SENKO_REDIS_URL"),
    }
}

pub fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    if redis::cmd("FLUSHALL").query::<()>(conn).is_ok() {
        return;
    }
    panic!("compat test requires FLUSHDB or FLUSHALL support");
}

pub fn assert_err_contains<T: std::fmt::Debug>(result: RedisResult<T>, needle: &str) {
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(needle),
        "expected error containing {needle:?}, got {err}"
    );
}

pub fn encoding(conn: &mut Connection, key: &str) -> String {
    redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg(key)
        .query(conn)
        .unwrap()
}
