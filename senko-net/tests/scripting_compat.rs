use std::time::{SystemTime, UNIX_EPOCH};

use redis::{Connection, RedisResult};

fn redis_url() -> String {
    std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn connect() -> Option<Connection> {
    let client = redis::Client::open(redis_url()).ok()?;
    client.get_connection().ok()
}

fn must_connect() -> Connection {
    match connect() {
        Some(mut conn) => {
            let _: RedisResult<String> = redis::cmd("PING").query(&mut conn);
            conn
        }
        None => panic!("scripting compat test requires running Senko at SENKO_REDIS_URL"),
    }
}

fn unique_name(prefix: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}:{stamp}")
}

#[test]
#[ignore = "requires running Senko"]
fn function_load_is_visible_across_connections() {
    let library = unique_name("mylib");
    let function = unique_name("f");

    let mut conn = must_connect();
    let _: String = redis::cmd("FUNCTION")
        .arg("FLUSH")
        .query(&mut conn)
        .unwrap();
    let code = format!(
        "#!lua name={library}\nredis.register_function('{function}', function(keys, args) return 1 end)"
    );
    let _: String = redis::cmd("FUNCTION")
        .arg("LOAD")
        .arg(code)
        .query(&mut conn)
        .unwrap();

    for _ in 0..16 {
        let mut other = must_connect();
        let value: i64 = redis::cmd("FCALL")
            .arg(&function)
            .arg(0)
            .query(&mut other)
            .unwrap();
        assert_eq!(value, 1);
    }

    let mut other = must_connect();
    let _: String = redis::cmd("FUNCTION")
        .arg("DELETE")
        .arg(&library)
        .query(&mut other)
        .unwrap();

    for _ in 0..8 {
        let mut third = must_connect();
        let err = redis::cmd("FCALL")
            .arg(&function)
            .arg(0)
            .query::<i64>(&mut third)
            .unwrap_err();
        assert!(err.to_string().contains("Function not found"));
    }
}

#[test]
#[ignore = "requires running Senko"]
fn script_load_is_visible_across_connections() {
    let mut conn = must_connect();
    let _: String = redis::cmd("SCRIPT").arg("FLUSH").query(&mut conn).unwrap();
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 42")
        .query(&mut conn)
        .unwrap();

    for _ in 0..16 {
        let mut other = must_connect();
        let value: i64 = redis::cmd("EVALSHA")
            .arg(&sha)
            .arg(0)
            .query(&mut other)
            .unwrap();
        assert_eq!(value, 42);
    }

    let mut other = must_connect();
    let _: String = redis::cmd("SCRIPT").arg("FLUSH").query(&mut other).unwrap();

    for _ in 0..8 {
        let mut third = must_connect();
        let err = redis::cmd("EVALSHA")
            .arg(&sha)
            .arg(0)
            .query::<i64>(&mut third)
            .unwrap_err();
        assert!(err.to_string().contains("No matching script"));
    }
}
