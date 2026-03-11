use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use redis::{Connection, InfoDict, RedisResult, Value};

fn redis_url() -> String {
    std::env::var("senko_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
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
        None => panic!("compat test requires running senko at senko_REDIS_URL"),
    }
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
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    let _: () = redis::cmd("FLUSHALL")
        .query(conn)
        .expect("compat test requires FLUSHDB or FLUSHALL");
}

fn send_resp(stream: &mut TcpStream, parts: &[&str]) {
    let mut payload = format!("*{}\r\n", parts.len());
    for part in parts {
        payload.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(payload.as_bytes()).unwrap();
}

fn read_text(stream: &mut TcpStream) -> String {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).expect("read failed");
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn poll_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(predicate(), "condition not met within {timeout:?}");
}

fn value_contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::BulkString(bytes) => std::str::from_utf8(bytes)
            .map(|text| text.contains(needle))
            .unwrap_or(false),
        Value::SimpleString(text) => text.contains(needle),
        Value::Array(values) | Value::Set(values) => values
            .iter()
            .any(|value| value_contains_text(value, needle)),
        Value::Map(entries) => entries.iter().any(|(key, value)| {
            value_contains_text(key, needle) || value_contains_text(value, needle)
        }),
        _ => false,
    }
}

fn unique_path(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("senko-{prefix}-{stamp}"))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server_bin() -> PathBuf {
    if let Ok(path) = std::env::var("senko_SERVER_BIN") {
        return PathBuf::from(path);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/debug/senko-server")
        .canonicalize()
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug/senko-server")
        })
}

fn spawn_server(port: u16, workdir: &Path, config_contents: &str) -> (Child, String, PathBuf) {
    fs::create_dir_all(workdir).unwrap();
    let config_path = workdir.join("senko.toml");
    fs::write(&config_path, config_contents).unwrap();
    let child = Command::new(server_bin())
        .arg("--config")
        .arg(&config_path)
        .current_dir(workdir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let url = format!("redis://127.0.0.1:{port}/");
    poll_until(Duration::from_secs(5), || {
        redis::Client::open(url.clone())
            .ok()
            .and_then(|client| client.get_connection().ok())
            .and_then(|mut conn| redis::cmd("PING").query::<String>(&mut conn).ok())
            .is_some()
    });
    (child, url, config_path)
}

#[test]
#[ignore = "requires running senko instance"]
fn info_compat_uses_redis_info_shape() {
    let mut conn = must_connect();
    flush(&mut conn);

    let before = redis::cmd("INFO").query::<String>(&mut conn).unwrap();
    let before_info = InfoDict::new(&before);
    assert!(before.contains("# Server\r\n"));
    assert!(before.contains("# Memory\r\n"));
    assert!(before.contains("# Replication\r\n"));
    assert!(before.contains("# Keyspace\r\n") || !before.contains("db0:"));
    assert_eq!(
        before_info.get::<String>("redis_version"),
        Some("8.0.0".into())
    );
    assert_eq!(before_info.get::<String>("role"), Some("master".into()));
    assert!(
        before_info
            .get::<i64>("connected_clients")
            .unwrap_or_default()
            >= 1
    );
    assert!(before_info.get::<i64>("used_memory").unwrap_or_default() > 0);
    assert!(
        before_info
            .get::<i64>("instantaneous_ops_per_sec")
            .unwrap_or_default()
            >= 0
    );
    let uptime_before = before_info
        .get::<i64>("uptime_in_seconds")
        .unwrap_or_default();

    let no_keys = redis::cmd("INFO")
        .arg("keyspace")
        .query::<String>(&mut conn)
        .unwrap();
    assert!(!no_keys.contains("db0:keys="));

    let _: () = redis::cmd("SET")
        .arg("server:info:key")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let with_keys = redis::cmd("INFO")
        .arg("keyspace")
        .query::<String>(&mut conn)
        .unwrap();
    assert!(with_keys.contains("db0:keys="));

    thread::sleep(Duration::from_secs(1));
    let after = redis::cmd("INFO").query::<String>(&mut conn).unwrap();
    let after_info = InfoDict::new(&after);
    let uptime_after = after_info
        .get::<i64>("uptime_in_seconds")
        .unwrap_or_default();
    assert!(uptime_after >= uptime_before);
}

#[test]
#[ignore = "requires running senko instance"]
fn config_command_and_info_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let maxmemory: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .query(&mut conn)
        .unwrap();
    assert_eq!(maxmemory.first().map(String::as_str), Some("maxmemory"));

    let ok: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("100mb")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");

    let ok: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("200mb")
        .arg("hz")
        .arg("20")
        .query(&mut conn)
        .unwrap();
    assert_eq!(ok, "OK");

    let info = InfoDict::new(
        &redis::cmd("INFO")
            .arg("memory")
            .query::<String>(&mut conn)
            .unwrap(),
    );
    assert!(info.get::<i64>("maxmemory").unwrap_or_default() >= 100 * 1024 * 1024);

    let _: () = redis::cmd("PING").query(&mut conn).unwrap();
    let reset: String = redis::cmd("CONFIG")
        .arg("RESETSTAT")
        .query(&mut conn)
        .unwrap();
    assert_eq!(reset, "OK");
    let stats = InfoDict::new(
        &redis::cmd("INFO")
            .arg("stats")
            .query::<String>(&mut conn)
            .unwrap(),
    );
    assert_eq!(stats.get::<i64>("total_commands_processed"), Some(0));
}

#[test]
#[ignore = "requires running senko instance"]
fn command_flush_slowlog_latency_memory_and_monitor_compat() {
    let mut conn = must_connect();
    flush(&mut conn);

    let count: i64 = redis::cmd("COMMAND").arg("COUNT").query(&mut conn).unwrap();
    assert!(count >= 100);

    let keys: Vec<String> = redis::cmd("COMMAND")
        .arg("GETKEYS")
        .arg("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query(&mut conn)
        .unwrap();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);

    let list: Vec<String> = redis::cmd("COMMAND").arg("LIST").query(&mut conn).unwrap();
    let unique = list.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(list.len(), unique.len());

    let docs = redis::cmd("COMMAND")
        .arg("DOCS")
        .arg("get")
        .query::<String>(&mut conn)
        .unwrap_or_default();
    assert!(docs.contains("summary") || docs.contains("string"));

    for index in 0..100 {
        let _: () = redis::cmd("SET")
            .arg(format!("server:flush:{index}"))
            .arg(index)
            .query(&mut conn)
            .unwrap();
    }
    let _: () = redis::cmd("FLUSHDB").query(&mut conn).unwrap();
    let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(size, 0);

    for index in 0..100 {
        let _: () = redis::cmd("SET")
            .arg(format!("server:flush:async:{index}"))
            .arg(index)
            .query(&mut conn)
            .unwrap();
    }
    let _: () = redis::cmd("FLUSHDB").arg("ASYNC").query(&mut conn).unwrap();
    poll_until(Duration::from_secs(2), || {
        redis::cmd("DBSIZE")
            .query::<i64>(&mut conn)
            .map(|size| size == 0)
            .unwrap_or(false)
    });

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-log-slower-than")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("server:slowlog")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query(&mut conn).unwrap();
    assert!(len > 0);
    let entries = redis::cmd("SLOWLOG")
        .arg("GET")
        .arg("1")
        .query::<String>(&mut conn)
        .unwrap_or_default();
    assert!(entries.contains("server:slowlog") || !entries.is_empty());
    let _: () = redis::cmd("SLOWLOG").arg("RESET").query(&mut conn).unwrap();
    assert_eq!(
        redis::cmd("SLOWLOG")
            .arg("LEN")
            .query::<i64>(&mut conn)
            .unwrap(),
        0
    );

    let latest = redis::cmd("LATENCY")
        .arg("LATEST")
        .query::<String>(&mut conn)
        .unwrap_or_default();
    assert!(!latest.is_empty());
    let history = redis::cmd("LATENCY")
        .arg("HISTORY")
        .arg("unknown-event")
        .query::<Vec<String>>(&mut conn)
        .unwrap_or_default();
    assert!(history.is_empty());
    let _: i64 = redis::cmd("LATENCY").arg("RESET").query(&mut conn).unwrap();

    let _: () = redis::cmd("SET")
        .arg("server:memory")
        .arg("value")
        .query(&mut conn)
        .unwrap();
    let usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("server:memory")
        .query(&mut conn)
        .unwrap();
    assert!(usage > 0);
    let missing = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("server:missing")
        .query::<Option<i64>>(&mut conn)
        .unwrap();
    assert!(missing.is_none());
    let stats = redis::cmd("MEMORY")
        .arg("STATS")
        .query::<String>(&mut conn)
        .unwrap_or_default();
    assert!(stats.contains("peak.allocated") || !stats.is_empty());
    let doctor: String = redis::cmd("MEMORY").arg("DOCTOR").query(&mut conn).unwrap();
    assert!(!doctor.is_empty());
    let malloc_stats: String = redis::cmd("MEMORY")
        .arg("MALLOC-STATS")
        .query(&mut conn)
        .unwrap();
    assert!(!malloc_stats.is_empty());
    let purge: String = redis::cmd("MEMORY").arg("PURGE").query(&mut conn).unwrap();
    assert_eq!(purge, "OK");

    let mut monitor = raw_socket();
    send_resp(&mut monitor, &["MONITOR"]);
    assert_eq!(read_text(&mut monitor), "+OK\r\n");

    let mut other = must_connect();
    let _: () = redis::cmd("PING").query(&mut other).unwrap();
    let _: () = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query(&mut other)
        .unwrap();

    let first = read_text(&mut monitor);
    let second = read_text(&mut monitor);
    let joined = format!("{first}{second}");
    assert!(joined.contains("\"PING\"") || joined.contains("\"SET\""));
    assert!(joined.contains("[0 "));

    send_resp(&mut monitor, &["GET", "foo"]);
    let monitor_err = read_text(&mut monitor);
    assert!(monitor_err.contains("MONITOR") || monitor_err.starts_with('-'));
}

#[test]
#[ignore = "requires running senko instance"]
fn save_and_bgsave_update_lastsave_and_create_file() {
    let mut conn = must_connect();
    flush(&mut conn);

    let dir_cfg: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("dir")
        .query(&mut conn)
        .unwrap();
    let file_cfg: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("dbfilename")
        .query(&mut conn)
        .unwrap();
    let dir = PathBuf::from(dir_cfg.get(1).cloned().unwrap_or_else(|| ".".to_string()));
    let dbfilename = file_cfg
        .get(1)
        .cloned()
        .unwrap_or_else(|| "dump.rdb".to_string());
    let rdb_path = dir.join(dbfilename);

    let before: i64 = redis::cmd("LASTSAVE").query(&mut conn).unwrap();
    let _: () = redis::cmd("SET")
        .arg("server:save")
        .arg("1")
        .query(&mut conn)
        .unwrap();
    let save_ok: String = redis::cmd("SAVE").query(&mut conn).unwrap();
    assert_eq!(save_ok, "OK");
    assert!(rdb_path.exists());
    let after_save: i64 = redis::cmd("LASTSAVE").query(&mut conn).unwrap();
    assert!(after_save >= before);

    let started: String = redis::cmd("BGSAVE").query(&mut conn).unwrap();
    assert_eq!(started, "Background saving started");
    poll_until(Duration::from_secs(5), || {
        let info = redis::cmd("INFO")
            .arg("persistence")
            .query::<String>(&mut conn)
            .unwrap_or_default();
        info.contains("rdb_bgsave_in_progress:0")
    });
    assert!(rdb_path.exists());
}

#[test]
#[ignore = "requires local server binary"]
fn acl_roundtrip_and_shutdown_on_spawned_server() {
    let port = free_port();
    let workdir = unique_path("server-acl");
    let aclfile = workdir.join("users.acl");
    let config = format!(
        "bind_addr = \"127.0.0.1:{port}\"\nnum_shards = 1\naclfile = \"{}\"\n",
        aclfile.display()
    );
    let (mut child, url, _config_path) = spawn_server(port, &workdir, &config);
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();

    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("bench")
        .arg("on")
        .arg(">secret")
        .arg("~bench:*")
        .arg("+get")
        .arg("+set")
        .query(&mut conn)
        .unwrap();
    let user = redis::cmd("ACL")
        .arg("GETUSER")
        .arg("bench")
        .query::<Value>(&mut conn)
        .unwrap();
    assert!(value_contains_text(&user, "flags"));
    assert!(value_contains_text(&user, "commands"));
    assert!(value_contains_text(&user, "bench:*"));

    let dryrun: String = redis::cmd("ACL")
        .arg("DRYRUN")
        .arg("bench")
        .arg("GET")
        .arg("bench:key")
        .query(&mut conn)
        .unwrap();
    assert_eq!(dryrun, "OK");

    let _: () = redis::cmd("ACL").arg("SAVE").query(&mut conn).unwrap();
    assert!(aclfile.exists());
    let _: () = redis::cmd("ACL").arg("LOAD").query(&mut conn).unwrap();

    let denied_client =
        redis::Client::open(format!("redis://bench:secret@127.0.0.1:{port}/")).unwrap();
    let mut denied_conn = denied_client.get_connection().unwrap();
    let denied = redis::cmd("GET")
        .arg("other:key")
        .query::<Value>(&mut denied_conn);
    assert!(denied.is_err());
    let log = redis::cmd("ACL")
        .arg("LOG")
        .query::<Value>(&mut conn)
        .unwrap();
    assert!(value_contains_text(&log, "key"));
    assert!(value_contains_text(&log, "other:key"));

    let shutdown: RedisResult<String> = redis::cmd("SHUTDOWN").arg("NOSAVE").query(&mut conn);
    assert!(shutdown.is_err());
    let status = child.wait().unwrap();
    assert!(status.success());
}

#[test]
#[ignore = "requires local server binary"]
fn config_rewrite_and_shutdown_save_on_spawned_server() {
    let port = free_port();
    let workdir = unique_path("server-config");
    let config = format!("bind_addr = \"127.0.0.1:{port}\"\nnum_shards = 1\n");
    let (mut child, url, config_path) = spawn_server(port, &workdir, &config);
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();

    let _: () = redis::cmd("SET")
        .arg("restart:key")
        .arg("value")
        .query(&mut conn)
        .unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("64mb")
        .query(&mut conn)
        .unwrap();
    let rewrite: String = redis::cmd("CONFIG")
        .arg("REWRITE")
        .query(&mut conn)
        .unwrap();
    assert_eq!(rewrite, "OK");
    let rewritten = fs::read_to_string(&config_path).unwrap();
    assert!(rewritten.contains("maxmemory"));

    let shutdown: RedisResult<String> = redis::cmd("SHUTDOWN").arg("SAVE").query(&mut conn);
    assert!(shutdown.is_err());
    let status = child.wait().unwrap();
    assert!(status.success());

    let rdb_path = workdir.join("dump.rdb");
    assert!(rdb_path.exists());
}
