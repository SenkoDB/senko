use std::{
    hint::black_box,
    io::{Read, Write},
    net::TcpStream,
    thread,
    time::Duration,
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use redis::Connection;

const INFO_ALL_OPS: usize = 50_000;
const INFO_SERVER_OPS: usize = 500_000;
const ACL_ALLOWED_OPS: usize = 1_000_000;
const ACL_KEY_PATTERN_OPS: usize = 500_000;
const CONFIG_GET_OPS: usize = 2_000_000;
const MONITOR_DISABLED_OPS: usize = 1_000_000;
const MONITOR_ACTIVE_OPS: usize = 250_000;
const SLOWLOG_OPS: usize = 500_000;
const MEMORY_USAGE_OPS: usize = 500_000;
const COMMAND_INFO_OPS: usize = 2_000_000;
const DBSIZE_OPS: usize = 10_000;

fn redis_url() -> String {
    std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn connect() -> Connection {
    redis::Client::open(redis_url())
        .expect("bench requires valid redis url")
        .get_connection()
        .expect("bench requires running Senko instance")
}

fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    let _: () = redis::cmd("FLUSHALL")
        .query(conn)
        .expect("bench requires FLUSHDB or FLUSHALL");
}

fn raw_socket() -> TcpStream {
    let addr = redis_url()
        .trim_start_matches("redis://")
        .split('@')
        .next_back()
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
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

fn read_available(stream: &mut TcpStream) {
    let mut buf = [0u8; 8192];
    let _ = stream.read(&mut buf);
}

fn bench_info_all(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("server_info_all");
    group.throughput(Throughput::Elements(INFO_ALL_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_info_all", INFO_ALL_OPS), |b| {
        b.iter(|| {
            for _ in 0..INFO_ALL_OPS {
                let body: String = redis::cmd("INFO").query(&mut conn).unwrap();
                black_box(body);
            }
        });
    });
    group.finish();
}

fn bench_info_server(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("server_info_server");
    group.throughput(Throughput::Elements(INFO_SERVER_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_info_server", INFO_SERVER_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..INFO_SERVER_OPS {
                    let body: String = redis::cmd("INFO").arg("server").query(&mut conn).unwrap();
                    black_box(body);
                }
            });
        },
    );
    group.finish();
}

fn bench_acl_check_allowed(c: &mut Criterion) {
    let mut baseline = connect();
    let mut authed = connect();
    flush(&mut baseline);
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("benchacl")
        .arg("on")
        .arg(">secret")
        .arg("~*")
        .arg("+get")
        .arg("+set")
        .query(&mut baseline)
        .unwrap();
    let _: String = redis::cmd("AUTH")
        .arg("benchacl")
        .arg("secret")
        .query(&mut authed)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("bench:acl:key")
        .arg("1")
        .query(&mut baseline)
        .unwrap();

    let mut group = c.benchmark_group("server_acl_check_allowed");
    group.throughput(Throughput::Elements(ACL_ALLOWED_OPS as u64));
    group.bench_function("baseline", |b| {
        b.iter(|| {
            for _ in 0..ACL_ALLOWED_OPS {
                let value: String = redis::cmd("GET")
                    .arg("bench:acl:key")
                    .query(&mut baseline)
                    .unwrap();
                black_box(value);
            }
        });
    });
    group.bench_function("acl_allowed", |b| {
        b.iter(|| {
            for _ in 0..ACL_ALLOWED_OPS {
                let value: String = redis::cmd("GET")
                    .arg("bench:acl:key")
                    .query(&mut authed)
                    .unwrap();
                black_box(value);
            }
        });
    });
    group.finish();
}

fn bench_acl_check_key_pattern(c: &mut Criterion) {
    let mut conn = connect();
    let mut authed = connect();
    flush(&mut conn);
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("benchpat")
        .arg("on")
        .arg(">secret")
        .arg("~bench:*")
        .arg("+get")
        .query(&mut conn)
        .unwrap();
    let _: String = redis::cmd("AUTH")
        .arg("benchpat")
        .arg("secret")
        .query(&mut authed)
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("bench:key")
        .arg("1")
        .query(&mut conn)
        .unwrap();

    let mut group = c.benchmark_group("server_acl_check_key_pattern");
    group.throughput(Throughput::Elements(ACL_KEY_PATTERN_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_acl_check_key_pattern", ACL_KEY_PATTERN_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..ACL_KEY_PATTERN_OPS {
                    let value: String = redis::cmd("GET")
                        .arg("bench:key")
                        .query(&mut authed)
                        .unwrap();
                    black_box(value);
                }
            });
        },
    );
    group.finish();
}

fn bench_config_get(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("server_config_get");
    group.throughput(Throughput::Elements(CONFIG_GET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_config_get", CONFIG_GET_OPS), |b| {
        b.iter(|| {
            for _ in 0..CONFIG_GET_OPS {
                let values: Vec<String> = redis::cmd("CONFIG")
                    .arg("GET")
                    .arg("maxmemory")
                    .query(&mut conn)
                    .unwrap();
                black_box(values);
            }
        });
    });
    group.finish();
}

fn bench_monitor_disabled(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("server_monitor_disabled");
    group.throughput(Throughput::Elements(MONITOR_DISABLED_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_monitor_disabled", MONITOR_DISABLED_OPS),
        |b| {
            b.iter(|| {
                for idx in 0..MONITOR_DISABLED_OPS {
                    let ok: String = redis::cmd("SET")
                        .arg("bench:monitor:key")
                        .arg((idx & 255).to_string())
                        .query(&mut conn)
                        .unwrap();
                    black_box(ok);
                }
            });
        },
    );
    group.finish();
}

fn bench_monitor_active(c: &mut Criterion) {
    let mut writer = connect();
    flush(&mut writer);
    let mut monitor = raw_socket();
    send_resp(&mut monitor, &["MONITOR"]);
    read_available(&mut monitor);
    let handle = thread::spawn(move || {
        let mut monitor = monitor;
        loop {
            read_available(&mut monitor);
        }
    });

    let mut group = c.benchmark_group("server_monitor_active");
    group.throughput(Throughput::Elements(MONITOR_ACTIVE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_monitor_active", MONITOR_ACTIVE_OPS),
        |b| {
            b.iter(|| {
                for idx in 0..MONITOR_ACTIVE_OPS {
                    let ok: String = redis::cmd("SET")
                        .arg("bench:monitor:active")
                        .arg((idx & 255).to_string())
                        .query(&mut writer)
                        .unwrap();
                    black_box(ok);
                }
            });
        },
    );
    let _ = handle.thread().id();
    group.finish();
}

fn bench_slowlog_collection(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("slowlog-log-slower-than")
        .arg("0")
        .query(&mut conn)
        .unwrap();
    let mut group = c.benchmark_group("server_slowlog_collection");
    group.throughput(Throughput::Elements(SLOWLOG_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_slowlog_collection", SLOWLOG_OPS),
        |b| {
            b.iter(|| {
                for idx in 0..SLOWLOG_OPS {
                    let ok: String = redis::cmd("SET")
                        .arg("bench:slowlog:key")
                        .arg((idx & 255).to_string())
                        .query(&mut conn)
                        .unwrap();
                    black_box(ok);
                }
            });
        },
    );
    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    for idx in 0..100 {
        let _: i64 = redis::cmd("HSET")
            .arg("bench:memory:hash")
            .arg(format!("f{idx}"))
            .arg("value")
            .query(&mut conn)
            .unwrap();
    }
    let mut group = c.benchmark_group("server_memory_usage");
    group.throughput(Throughput::Elements(MEMORY_USAGE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_memory_usage", MEMORY_USAGE_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..MEMORY_USAGE_OPS {
                    let bytes: i64 = redis::cmd("MEMORY")
                        .arg("USAGE")
                        .arg("bench:memory:hash")
                        .query(&mut conn)
                        .unwrap();
                    black_box(bytes);
                }
            });
        },
    );
    group.finish();
}

fn bench_command_info(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("server_command_info");
    group.throughput(Throughput::Elements(COMMAND_INFO_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_command_info", COMMAND_INFO_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..COMMAND_INFO_OPS {
                    let info: String = redis::cmd("COMMAND")
                        .arg("INFO")
                        .arg("get")
                        .query(&mut conn)
                        .unwrap_or_default();
                    black_box(info);
                }
            });
        },
    );
    group.finish();
}

fn bench_dbsize(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    for idx in 0..1_000_000usize {
        let _: String = redis::cmd("SET")
            .arg(format!("bench:dbsize:{idx}"))
            .arg("1")
            .query(&mut conn)
            .unwrap();
    }
    let mut group = c.benchmark_group("server_dbsize");
    group.throughput(Throughput::Elements(DBSIZE_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_dbsize", DBSIZE_OPS), |b| {
        b.iter(|| {
            for _ in 0..DBSIZE_OPS {
                let size: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
                black_box(size);
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_info_all,
    bench_info_server,
    bench_acl_check_allowed,
    bench_acl_check_key_pattern,
    bench_config_get,
    bench_monitor_disabled,
    bench_monitor_active,
    bench_slowlog_collection,
    bench_memory_usage,
    bench_command_info,
    bench_dbsize
);
criterion_main!(benches);
