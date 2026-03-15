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
    raw_socket_for_url(&redis_url())
}

fn raw_socket_for_url(url: &str) -> TcpStream {
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawResp {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RawResp>>),
}

fn read_resp(stream: &mut TcpStream) -> RawResp {
    let mut prefix = [0u8; 1];
    stream.read_exact(&mut prefix).expect("read prefix failed");
    match prefix[0] {
        b'+' => RawResp::Simple(read_resp_line(stream)),
        b'-' => RawResp::Error(read_resp_line(stream)),
        b':' => RawResp::Integer(read_resp_line(stream).parse::<i64>().unwrap()),
        b'$' => {
            let len = read_resp_line(stream).parse::<isize>().unwrap();
            if len < 0 {
                RawResp::Bulk(None)
            } else {
                let mut payload = vec![0u8; len as usize];
                stream.read_exact(&mut payload).expect("read bulk failed");
                expect_crlf(stream);
                RawResp::Bulk(Some(payload))
            }
        }
        b'*' => {
            let len = read_resp_line(stream).parse::<isize>().unwrap();
            if len < 0 {
                RawResp::Array(None)
            } else {
                let mut items = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    items.push(read_resp(stream));
                }
                RawResp::Array(Some(items))
            }
        }
        other => panic!("unexpected RESP prefix: {}", other as char),
    }
}

fn read_resp_line(stream: &mut TcpStream) -> String {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("read line failed");
        if byte[0] == b'\r' {
            expect_lf(stream);
            break;
        }
        line.push(byte[0]);
    }
    String::from_utf8(line).unwrap()
}

fn expect_crlf(stream: &mut TcpStream) {
    let mut crlf = [0u8; 2];
    stream.read_exact(&mut crlf).expect("read CRLF failed");
    assert_eq!(&crlf, b"\r\n");
}

fn expect_lf(stream: &mut TcpStream) {
    let mut lf = [0u8; 1];
    stream.read_exact(&mut lf).expect("read LF failed");
    assert_eq!(lf[0], b'\n');
}

fn client_id(stream: &mut TcpStream) -> i64 {
    match send_raw(stream, &["CLIENT", "ID"]) {
        RawResp::Integer(id) => id,
        other => panic!("expected CLIENT ID integer, got {other:?}"),
    }
}

fn client_list(stream: &mut TcpStream) -> String {
    match send_raw(stream, &["CLIENT", "LIST"]) {
        RawResp::Bulk(Some(bytes)) => String::from_utf8(bytes).unwrap(),
        other => panic!("expected CLIENT LIST bulk reply, got {other:?}"),
    }
}

fn find_clients_on_distinct_shards(url: &str) -> (TcpStream, TcpStream) {
    let mut first = raw_socket_for_url(url);
    let started = Instant::now();
    let mut attempt = 0usize;

    while started.elapsed() < Duration::from_secs(5) {
        let mut candidate = raw_socket_for_url(url);
        let candidate_id = client_id(&mut candidate);
        let visible = client_list(&mut first);
        if !visible.contains(&format!("id={candidate_id}")) {
            return (first, candidate);
        }
        attempt += 1;
        if attempt % 4 == 0 {
            thread::sleep(Duration::from_millis(10));
        }
    }

    panic!("failed to place clients on different accepting shards");
}

fn key_for_shard(url: &str, prefix: &str, target_shard: u16, num_shards: u16) -> String {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    for seed in 0..10_000usize {
        let candidate = format!("{prefix}:{seed}");
        let slot: i64 = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(&candidate)
            .query(&mut conn)
            .unwrap();
        if (slot as u16 % num_shards) == target_shard {
            return candidate;
        }
    }
    panic!("failed to find key for shard {target_shard}");
}

fn send_raw(stream: &mut TcpStream, parts: &[&str]) -> RawResp {
    send_resp(stream, parts);
    read_resp(stream)
}

fn bulk_text(value: &RawResp) -> Option<String> {
    match value {
        RawResp::Bulk(Some(bytes)) => Some(String::from_utf8(bytes.clone()).unwrap()),
        _ => None,
    }
}

fn array_bulk_texts(value: &RawResp) -> Vec<String> {
    match value {
        RawResp::Array(Some(items)) => items.iter().filter_map(bulk_text).collect(),
        other => panic!("expected bulk string array, got {other:?}"),
    }
}

fn stream_reply_names(value: &RawResp) -> Vec<String> {
    match value {
        RawResp::Array(Some(streams)) => streams
            .iter()
            .map(|stream| match stream {
                RawResp::Array(Some(parts)) if parts.len() == 2 => {
                    bulk_text(&parts[0]).expect("expected stream name")
                }
                other => panic!("expected XREAD stream tuple, got {other:?}"),
            })
            .collect(),
        other => panic!("expected XREAD array reply, got {other:?}"),
    }
}

fn first_stream_entry_id(value: &RawResp) -> String {
    match value {
        RawResp::Array(Some(streams)) => match streams.first() {
            Some(RawResp::Array(Some(parts))) if parts.len() == 2 => match &parts[1] {
                RawResp::Array(Some(entries)) => match entries.first() {
                    Some(RawResp::Array(Some(entry_parts))) if entry_parts.len() == 2 => {
                        bulk_text(&entry_parts[0]).expect("expected stream entry id")
                    }
                    other => panic!("expected XREAD entry tuple, got {other:?}"),
                },
                other => panic!("expected XREAD entry array, got {other:?}"),
            },
            other => panic!("expected XREAD stream tuple, got {other:?}"),
        },
        other => panic!("expected XREAD array reply, got {other:?}"),
    }
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

#[test]
#[ignore = "requires local server binary"]
fn routed_keyspace_is_shared_across_clients_on_different_shards() {
    let port = free_port();
    let workdir = unique_path("server-routing");
    let config = format!("[network]\nbind = [\"127.0.0.1\"]\nport = {port}\nio_threads = 2\n");
    let (mut child, url, _config_path) = spawn_server(port, &workdir, &config);
    let (mut client_a, mut client_b) = find_clients_on_distinct_shards(&url);
    let prefix = format!(
        "route:{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let key_a = key_for_shard(&url, &format!("{prefix}:multi:a"), 0, 2);
    let key_b = key_for_shard(&url, &format!("{prefix}:multi:b"), 1, 2);
    let set_a = key_for_shard(&url, &format!("{prefix}:set:a"), 0, 2);
    let set_b = key_for_shard(&url, &format!("{prefix}:set:b"), 1, 2);
    let set_dst = key_for_shard(&url, &format!("{prefix}:set:dst"), 0, 2);
    let copy_src = key_for_shard(&url, &format!("{prefix}:copy:src"), 0, 2);
    let copy_dst = key_for_shard(&url, &format!("{prefix}:copy:dst"), 1, 2);
    let rename_src = key_for_shard(&url, &format!("{prefix}:rename:src"), 0, 2);
    let rename_dst = key_for_shard(&url, &format!("{prefix}:rename:dst"), 1, 2);
    let renamenx_src = key_for_shard(&url, &format!("{prefix}:renamenx:src"), 0, 2);
    let renamenx_dst = key_for_shard(&url, &format!("{prefix}:renamenx:dst"), 1, 2);
    let list_src = key_for_shard(&url, &format!("{prefix}:list:src"), 0, 2);
    let list_dst = key_for_shard(&url, &format!("{prefix}:list:dst"), 1, 2);
    let smove_src = key_for_shard(&url, &format!("{prefix}:smove:src"), 0, 2);
    let smove_dst = key_for_shard(&url, &format!("{prefix}:smove:dst"), 1, 2);
    let bitop_src_a = key_for_shard(&url, &format!("{prefix}:bitop:src:a"), 0, 2);
    let bitop_src_b = key_for_shard(&url, &format!("{prefix}:bitop:src:b"), 1, 2);
    let bitop_dst = key_for_shard(&url, &format!("{prefix}:bitop:dst"), 0, 2);
    let lcs_a = key_for_shard(&url, &format!("{prefix}:lcs:a"), 0, 2);
    let lcs_b = key_for_shard(&url, &format!("{prefix}:lcs:b"), 1, 2);
    let hll_a = key_for_shard(&url, &format!("{prefix}:hll:a"), 0, 2);
    let hll_b = key_for_shard(&url, &format!("{prefix}:hll:b"), 1, 2);
    let hll_dst = key_for_shard(&url, &format!("{prefix}:hll:dst"), 0, 2);
    let lmpop_a = key_for_shard(&url, &format!("{prefix}:lmpop:a"), 0, 2);
    let lmpop_b = key_for_shard(&url, &format!("{prefix}:lmpop:b"), 1, 2);
    let zset_a = key_for_shard(&url, &format!("{prefix}:zset:a"), 0, 2);
    let zset_b = key_for_shard(&url, &format!("{prefix}:zset:b"), 1, 2);
    let zset_dst = key_for_shard(&url, &format!("{prefix}:zset:dst"), 0, 2);
    let zset_range_dst = key_for_shard(&url, &format!("{prefix}:zset:range:dst"), 1, 2);
    let zmpop_a = key_for_shard(&url, &format!("{prefix}:zmpop:a"), 0, 2);
    let zmpop_b = key_for_shard(&url, &format!("{prefix}:zmpop:b"), 1, 2);
    let block_list_a = key_for_shard(&url, &format!("{prefix}:block:list:a"), 0, 2);
    let block_list_b = key_for_shard(&url, &format!("{prefix}:block:list:b"), 1, 2);
    let block_zset_a = key_for_shard(&url, &format!("{prefix}:block:zset:a"), 0, 2);
    let block_zset_b = key_for_shard(&url, &format!("{prefix}:block:zset:b"), 1, 2);
    let xread_a = key_for_shard(&url, &format!("{prefix}:xread:a"), 0, 2);
    let xread_b = key_for_shard(&url, &format!("{prefix}:xread:b"), 1, 2);
    let xgroup_a = key_for_shard(&url, &format!("{prefix}:xgroup:a"), 0, 2);
    let xgroup_b = key_for_shard(&url, &format!("{prefix}:xgroup:b"), 1, 2);
    let geo_src = key_for_shard(&url, &format!("{prefix}:geo:src"), 0, 2);
    let geo_dst = key_for_shard(&url, &format!("{prefix}:geo:dst"), 1, 2);
    let sort_src = key_for_shard(&url, &format!("{prefix}:sort:src"), 0, 2);
    let sort_dst = key_for_shard(&url, &format!("{prefix}:sort:dst"), 1, 2);
    let sort_weight_base = format!("{prefix}:sort:weight");
    let sort_weight_a = key_for_shard(&url, &sort_weight_base, 1, 2);
    let sort_weight_b = key_for_shard(&url, &sort_weight_base, 0, 2);
    let sort_member_a = sort_weight_a
        .strip_prefix(&format!("{sort_weight_base}:"))
        .unwrap()
        .to_string();
    let sort_member_b = sort_weight_b
        .strip_prefix(&format!("{sort_weight_base}:"))
        .unwrap()
        .to_string();

    assert_eq!(
        send_raw(&mut client_a, &["SET", &format!("{prefix}:str"), "v1"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["GET", &format!("{prefix}:str")]),
        RawResp::Bulk(Some(b"v1".to_vec()))
    );

    assert_eq!(
        send_raw(
            &mut client_b,
            &["HSET", &format!("{prefix}:hash"), "field", "value"]
        ),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["HGET", &format!("{prefix}:hash"), "field"]),
        RawResp::Bulk(Some(b"value".to_vec()))
    );

    assert_eq!(
        send_raw(
            &mut client_a,
            &["LPUSH", &format!("{prefix}:list"), "one", "two"]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_b,
            &["LRANGE", &format!("{prefix}:list"), "0", "-1"]
        )),
        vec!["two".to_string(), "one".to_string()]
    );

    assert_eq!(
        send_raw(&mut client_b, &["SADD", &format!("{prefix}:set"), "a", "b"]),
        RawResp::Integer(2)
    );
    let mut members = array_bulk_texts(&send_raw(
        &mut client_a,
        &["SMEMBERS", &format!("{prefix}:set")],
    ));
    members.sort();
    assert_eq!(members, vec!["a".to_string(), "b".to_string()]);

    assert_eq!(
        send_raw(
            &mut client_a,
            &["ZADD", &format!("{prefix}:zset"), "1", "one", "2", "two"]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_b,
            &["ZRANGE", &format!("{prefix}:zset"), "0", "-1"]
        )),
        vec!["one".to_string(), "two".to_string()]
    );

    assert!(bulk_text(&send_raw(
        &mut client_a,
        &["XADD", &format!("{prefix}:stream"), "*", "field", "value"]
    ))
    .is_some());
    assert_eq!(
        send_raw(&mut client_b, &["XLEN", &format!("{prefix}:stream")]),
        RawResp::Integer(1)
    );
    let stream_entries = send_raw(
        &mut client_b,
        &["XRANGE", &format!("{prefix}:stream"), "-", "+"],
    );
    let RawResp::Array(Some(entries)) = stream_entries else {
        panic!("expected XRANGE array");
    };
    assert_eq!(entries.len(), 1);

    assert_eq!(
        send_raw(&mut client_a, &["MSET", &key_a, "left", &key_b, "right"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["MGET", &key_a, &key_b])),
        vec!["left".to_string(), "right".to_string()]
    );
    assert_eq!(
        send_raw(&mut client_a, &["EXISTS", &key_a, &key_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_b, &["TOUCH", &key_a, &key_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_a, &["DEL", &key_a, &key_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_b, &["MGET", &key_a, &key_b]),
        RawResp::Array(Some(vec![RawResp::Bulk(None), RawResp::Bulk(None)]))
    );

    assert_eq!(
        send_raw(
            &mut client_a,
            &["MSETNX", &key_a, "nx-left", &key_b, "nx-right"]
        ),
        RawResp::Integer(1)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["MGET", &key_a, &key_b])),
        vec!["nx-left".to_string(), "nx-right".to_string()]
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &[
                "MSETNX",
                &key_a,
                "should-not-write",
                &key_b,
                "should-not-write"
            ]
        ),
        RawResp::Integer(0)
    );

    assert_eq!(
        send_raw(&mut client_a, &["DEL", &key_a, &key_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &[
                "MSETEX",
                "2",
                &key_a,
                "ttl-left",
                &key_b,
                "ttl-right",
                "EX",
                "30"
            ]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_a, &["MGET", &key_a, &key_b])),
        vec!["ttl-left".to_string(), "ttl-right".to_string()]
    );

    assert_eq!(
        send_raw(&mut client_a, &["SET", &copy_src, "copy-value"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["COPY", &copy_src, &copy_dst]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["GET", &copy_dst]),
        RawResp::Bulk(Some(b"copy-value".to_vec()))
    );
    assert_eq!(
        send_raw(&mut client_a, &["SET", &copy_dst, "occupied"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["COPY", &copy_src, &copy_dst]),
        RawResp::Integer(0)
    );
    assert_eq!(
        send_raw(&mut client_b, &["COPY", &copy_src, &copy_dst, "REPLACE"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["GET", &copy_dst]),
        RawResp::Bulk(Some(b"copy-value".to_vec()))
    );

    assert_eq!(
        send_raw(&mut client_a, &["SET", &rename_src, "rename-value"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["RENAME", &rename_src, &rename_dst]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_a, &["GET", &rename_src]),
        RawResp::Bulk(None)
    );
    assert_eq!(
        send_raw(&mut client_b, &["GET", &rename_dst]),
        RawResp::Bulk(Some(b"rename-value".to_vec()))
    );

    assert_eq!(
        send_raw(&mut client_a, &["SET", &renamenx_src, "renamenx-value"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["RENAMENX", &renamenx_src, &renamenx_dst]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["GET", &renamenx_src]),
        RawResp::Bulk(None)
    );
    assert_eq!(
        send_raw(&mut client_b, &["GET", &renamenx_dst]),
        RawResp::Bulk(Some(b"renamenx-value".to_vec()))
    );
    assert_eq!(
        send_raw(&mut client_a, &["SET", &renamenx_src, "renamenx-next"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["RENAMENX", &renamenx_src, &renamenx_dst]),
        RawResp::Integer(0)
    );
    assert_eq!(
        send_raw(&mut client_a, &["GET", &renamenx_src]),
        RawResp::Bulk(Some(b"renamenx-next".to_vec()))
    );

    assert_eq!(
        send_raw(&mut client_a, &["RPUSH", &list_src, "one", "two"]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_b, &["LPUSH", &list_dst, "dest"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(
            &mut client_a,
            &["LMOVE", &list_src, &list_dst, "RIGHT", "LEFT"]
        ),
        RawResp::Bulk(Some(b"two".to_vec()))
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["LRANGE", &list_src, "0", "-1"])),
        vec!["one".to_string()]
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_a, &["LRANGE", &list_dst, "0", "-1"])),
        vec!["two".to_string(), "dest".to_string()]
    );

    assert_eq!(
        send_raw(&mut client_a, &["SADD", &smove_src, "left"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_b, &["SADD", &smove_dst, "right"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["SMOVE", &smove_src, &smove_dst, "left"]),
        RawResp::Integer(1)
    );
    let mut members = array_bulk_texts(&send_raw(&mut client_b, &["SMEMBERS", &smove_dst]));
    members.sort();
    assert_eq!(members, vec!["left".to_string(), "right".to_string()]);
    assert_eq!(
        send_raw(&mut client_a, &["SMEMBERS", &smove_src]),
        RawResp::Array(Some(vec![]))
    );

    assert_eq!(
        send_raw(&mut client_a, &["SET", &bitop_src_a, "A"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["SET", &bitop_src_b, "a"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(
            &mut client_a,
            &["BITOP", "OR", &bitop_dst, &bitop_src_a, &bitop_src_b]
        ),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_b, &["GET", &bitop_dst]),
        RawResp::Bulk(Some(b"a".to_vec()))
    );

    assert_eq!(
        send_raw(&mut client_a, &["SET", &lcs_a, "ohmytext"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_b, &["SET", &lcs_b, "mynewtext"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_a, &["LCS", &lcs_a, &lcs_b]),
        RawResp::Bulk(Some(b"mytext".to_vec()))
    );

    assert_eq!(
        send_raw(&mut client_a, &["PFADD", &hll_a, "one"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_b, &["PFADD", &hll_b, "two"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(&mut client_a, &["PFCOUNT", &hll_a, &hll_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_b, &["PFMERGE", &hll_dst, &hll_a, &hll_b]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_a, &["PFCOUNT", &hll_dst]),
        RawResp::Integer(2)
    );

    assert_eq!(
        send_raw(&mut client_b, &["RPUSH", &lmpop_b, "first", "second"]),
        RawResp::Integer(2)
    );
    let lmpop = send_raw(
        &mut client_a,
        &["LMPOP", "2", &lmpop_a, &lmpop_b, "LEFT", "COUNT", "1"],
    );
    let RawResp::Array(Some(items)) = lmpop else {
        panic!("expected LMPOP array");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["LRANGE", &lmpop_b, "0", "-1"])),
        vec!["second".to_string()]
    );

    assert_eq!(
        send_raw(
            &mut client_a,
            &["ZADD", &zset_a, "1", "one", "2", "two", "3", "shared"]
        ),
        RawResp::Integer(3)
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &["ZADD", &zset_b, "4", "shared", "5", "three"]
        ),
        RawResp::Integer(2)
    );
    let mut members = array_bulk_texts(&send_raw(&mut client_a, &["ZDIFF", "2", &zset_a, &zset_b]));
    members.sort();
    assert_eq!(members, vec!["one".to_string(), "two".to_string()]);
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_b,
            &["ZINTER", "2", &zset_a, &zset_b, "WITHSCORES"]
        )),
        vec!["shared".to_string(), "7".to_string()]
    );
    assert_eq!(
        send_raw(
            &mut client_a,
            &["ZINTERCARD", "2", &zset_a, &zset_b, "LIMIT", "1"]
        ),
        RawResp::Integer(1)
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &[
                "ZUNIONSTORE",
                &zset_dst,
                "2",
                &zset_a,
                &zset_b,
                "WEIGHTS",
                "2",
                "1",
                "AGGREGATE",
                "MAX"
            ]
        ),
        RawResp::Integer(4)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_a,
            &["ZRANGE", &zset_dst, "0", "-1", "WITHSCORES"]
        )),
        vec![
            "one".to_string(),
            "2".to_string(),
            "two".to_string(),
            "4".to_string(),
            "three".to_string(),
            "5".to_string(),
            "shared".to_string(),
            "6".to_string()
        ]
    );
    assert_eq!(
        send_raw(
            &mut client_a,
            &["ZRANGESTORE", &zset_range_dst, &zset_a, "0", "1"]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_b,
            &["ZRANGE", &zset_range_dst, "0", "-1"]
        )),
        vec!["one".to_string(), "two".to_string()]
    );

    assert_eq!(
        send_raw(
            &mut client_b,
            &["ZADD", &zmpop_b, "1", "alpha", "2", "beta"]
        ),
        RawResp::Integer(2)
    );
    let zmpop = send_raw(
        &mut client_a,
        &["ZMPOP", "2", &zmpop_a, &zmpop_b, "MIN", "COUNT", "1"],
    );
    let RawResp::Array(Some(items)) = zmpop else {
        panic!("expected ZMPOP array");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["ZRANGE", &zmpop_b, "0", "-1"])),
        vec!["beta".to_string()]
    );

    let block_list_key_b = block_list_b.clone();
    let block_list_reply = thread::spawn(move || {
        let mut socket = client_a;
        send_raw(
            &mut socket,
            &["BLPOP", &block_list_a, &block_list_key_b, "1"],
        )
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        send_raw(&mut client_b, &["RPUSH", &block_list_b, "ready"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        block_list_reply.join().unwrap(),
        RawResp::Array(Some(vec![
            RawResp::Bulk(Some(block_list_b.as_bytes().to_vec())),
            RawResp::Bulk(Some(b"ready".to_vec()))
        ]))
    );
    let (client_a, mut client_b) = find_clients_on_distinct_shards(&url);

    let block_zset_key_b = block_zset_b.clone();
    let block_zset_reply = thread::spawn(move || {
        let mut socket = client_a;
        send_raw(
            &mut socket,
            &["BZPOPMIN", &block_zset_a, &block_zset_key_b, "1"],
        )
    });
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        send_raw(&mut client_b, &["ZADD", &block_zset_b, "2", "member"]),
        RawResp::Integer(1)
    );
    assert_eq!(
        block_zset_reply.join().unwrap(),
        RawResp::Array(Some(vec![
            RawResp::Bulk(Some(block_zset_b.as_bytes().to_vec())),
            RawResp::Bulk(Some(b"member".to_vec())),
            RawResp::Bulk(Some(b"2".to_vec()))
        ]))
    );
    let (client_a, mut client_b) = find_clients_on_distinct_shards(&url);

    assert!(bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xread_a, "*", "seed", "a"]
    ))
    .is_some());
    assert!(bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xread_b, "*", "seed", "b"]
    ))
    .is_some());
    let xread_key_b = xread_b.clone();
    let xread_reply = thread::spawn(move || {
        let mut socket = client_a;
        send_raw(
            &mut socket,
            &[
                "XREAD",
                "BLOCK",
                "1000",
                "STREAMS",
                &xread_a,
                &xread_key_b,
                "$",
                "$",
            ],
        )
    });
    thread::sleep(Duration::from_millis(50));
    assert!(bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xread_b, "*", "field", "value"]
    ))
    .is_some());
    let xread_block = xread_reply.join().unwrap();
    assert_eq!(stream_reply_names(&xread_block), vec![xread_b.clone()]);
    let (client_a, mut client_b) = find_clients_on_distinct_shards(&url);

    let group_seed_a = bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xgroup_a, "*", "seed", "a"],
    ))
    .unwrap();
    let group_seed_b = bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xgroup_b, "*", "seed", "b"],
    ))
    .unwrap();
    assert_eq!(
        send_raw(
            &mut client_b,
            &["XGROUP", "CREATE", &xgroup_a, "g", &group_seed_a]
        ),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &["XGROUP", "CREATE", &xgroup_b, "g", &group_seed_b]
        ),
        RawResp::Simple("OK".to_string())
    );
    let xgroup_key_b = xgroup_b.clone();
    let xgroup_reply = thread::spawn(move || {
        let mut socket = client_a;
        send_raw(
            &mut socket,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c1",
                "BLOCK",
                "1000",
                "STREAMS",
                &xgroup_a,
                &xgroup_key_b,
                ">",
                ">",
            ],
        )
    });
    thread::sleep(Duration::from_millis(50));
    assert!(bulk_text(&send_raw(
        &mut client_b,
        &["XADD", &xgroup_b, "*", "field", "group-value"]
    ))
    .is_some());
    let xgroup_block = xgroup_reply.join().unwrap();
    assert_eq!(stream_reply_names(&xgroup_block), vec![xgroup_b.clone()]);
    let delivered_id = first_stream_entry_id(&xgroup_block);
    assert_eq!(
        send_raw(&mut client_b, &["XACK", &xgroup_b, "g", &delivered_id]),
        RawResp::Integer(1)
    );
    let (mut client_a, mut client_b) = find_clients_on_distinct_shards(&url);

    assert_eq!(
        send_raw(
            &mut client_a,
            &[
                "GEOADD",
                &geo_src,
                "13.361389",
                "38.115556",
                "Palermo",
                "15.087269",
                "37.502669",
                "Catania"
            ]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &[
                "GEOSEARCHSTORE",
                &geo_dst,
                &geo_src,
                "FROMMEMBER",
                "Palermo",
                "BYRADIUS",
                "200",
                "km"
            ]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_a, &["ZCARD", &geo_dst]),
        RawResp::Integer(2)
    );

    assert_eq!(
        send_raw(
            &mut client_a,
            &["RPUSH", &sort_src, &sort_member_a, &sort_member_b]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(&mut client_b, &["SET", &sort_weight_a, "2"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        send_raw(&mut client_a, &["SET", &sort_weight_b, "1"]),
        RawResp::Simple("OK".to_string())
    );
    assert_eq!(
        array_bulk_texts(&send_raw(
            &mut client_b,
            &[
                "SORT_RO",
                &sort_src,
                "BY",
                &format!("{sort_weight_base}:*"),
                "GET",
                "#"
            ]
        )),
        vec![sort_member_b.clone(), sort_member_a.clone()]
    );
    assert_eq!(
        send_raw(
            &mut client_a,
            &[
                "SORT",
                &sort_src,
                "BY",
                &format!("{sort_weight_base}:*"),
                "STORE",
                &sort_dst
            ]
        ),
        RawResp::Integer(2)
    );
    assert_eq!(
        array_bulk_texts(&send_raw(&mut client_b, &["LRANGE", &sort_dst, "0", "-1"])),
        vec![sort_member_b.clone(), sort_member_a.clone()]
    );

    let mut keys = array_bulk_texts(&send_raw(
        &mut client_a,
        &["KEYS", &format!("{prefix}:multi:*")],
    ));
    keys.sort();
    assert_eq!(keys, vec![key_a.clone(), key_b.clone()]);
    let scan = send_raw(
        &mut client_b,
        &[
            "SCAN",
            "0",
            "MATCH",
            &format!("{prefix}:multi:*"),
            "COUNT",
            "100",
        ],
    );
    let RawResp::Array(Some(items)) = scan else {
        panic!("expected SCAN array");
    };
    assert_eq!(items.len(), 2);
    let mut keys = match &items[1] {
        RawResp::Array(Some(values)) => values.iter().filter_map(bulk_text).collect::<Vec<_>>(),
        other => panic!("expected SCAN key array, got {other:?}"),
    };
    keys.sort();
    assert_eq!(keys, vec![key_a.clone(), key_b.clone()]);
    let random = send_raw(&mut client_a, &["RANDOMKEY"]);
    let Some(random) = bulk_text(&random) else {
        panic!("expected RANDOMKEY bulk");
    };
    assert!(random.starts_with(&prefix));

    assert_eq!(
        send_raw(&mut client_a, &["SADD", &set_a, "a", "b", "c"]),
        RawResp::Integer(3)
    );
    assert_eq!(
        send_raw(&mut client_b, &["SADD", &set_b, "b", "c", "d"]),
        RawResp::Integer(3)
    );

    let mut members = array_bulk_texts(&send_raw(&mut client_a, &["SDIFF", &set_a, &set_b]));
    members.sort();
    assert_eq!(members, vec!["a".to_string()]);

    let mut members = array_bulk_texts(&send_raw(&mut client_b, &["SINTER", &set_a, &set_b]));
    members.sort();
    assert_eq!(members, vec!["b".to_string(), "c".to_string()]);

    assert_eq!(
        send_raw(&mut client_a, &["SINTERCARD", "2", &set_a, &set_b]),
        RawResp::Integer(2)
    );
    assert_eq!(
        send_raw(
            &mut client_b,
            &["SINTERCARD", "2", &set_a, &set_b, "LIMIT", "1"]
        ),
        RawResp::Integer(1)
    );

    let mut members = array_bulk_texts(&send_raw(&mut client_a, &["SUNION", &set_a, &set_b]));
    members.sort();
    assert_eq!(
        members,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );

    assert_eq!(
        send_raw(&mut client_b, &["SDIFFSTORE", &set_dst, &set_a, &set_b]),
        RawResp::Integer(1)
    );
    let mut members = array_bulk_texts(&send_raw(&mut client_a, &["SMEMBERS", &set_dst]));
    members.sort();
    assert_eq!(members, vec!["a".to_string()]);

    assert_eq!(
        send_raw(&mut client_b, &["SINTERSTORE", &set_dst, &set_a, &set_b]),
        RawResp::Integer(2)
    );
    let mut members = array_bulk_texts(&send_raw(&mut client_a, &["SMEMBERS", &set_dst]));
    members.sort();
    assert_eq!(members, vec!["b".to_string(), "c".to_string()]);

    assert_eq!(
        send_raw(&mut client_a, &["SUNIONSTORE", &set_dst, &set_a, &set_b]),
        RawResp::Integer(4)
    );
    let mut members = array_bulk_texts(&send_raw(&mut client_b, &["SMEMBERS", &set_dst]));
    members.sort();
    assert_eq!(
        members,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    let shutdown: RedisResult<String> = redis::cmd("SHUTDOWN").arg("NOSAVE").query(&mut conn);
    assert!(shutdown.is_err());
    let status = child.wait().unwrap();
    assert!(status.success());
}
