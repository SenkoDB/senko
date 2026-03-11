use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use redis::{Connection, Value};

const PING_OPS: usize = 10_000_000;
const HELLO_OPS: usize = 1_000_000;
const CLIENT_ID_OPS: usize = 5_000_000;
const CLIENT_INFO_OPS: usize = 1_000_000;
const CLIENT_LIST_OPS: usize = 100_000;
const AUTH_OPS: usize = 5_000_000;
const TRACKING_GET_OPS: usize = 5_000_000;
const TRACKING_INVALIDATE_OPS: usize = 1_000_000;
const PAUSE_OPS: usize = 100_000;

fn redis_url() -> String {
    std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

fn auth_redis_url() -> String {
    std::env::var("SENKO_AUTH_REDIS_URL_NOAUTH")
        .or_else(|_| std::env::var("SENKO_AUTH_REDIS_URL"))
        .expect("bench_auth_correct requires SENKO_AUTH_REDIS_URL_NOAUTH or SENKO_AUTH_REDIS_URL")
}

fn requirepass() -> String {
    std::env::var("SENKO_REQUIREPASS").expect("bench_auth_correct requires SENKO_REQUIREPASS")
}

fn connect() -> Connection {
    let client = redis::Client::open(redis_url()).expect("bench requires valid redis url");
    client
        .get_connection()
        .expect("bench requires running Senko instance")
}

fn connect_auth() -> Connection {
    let client =
        redis::Client::open(auth_redis_url()).expect("bench requires valid auth redis url");
    client
        .get_connection()
        .expect("bench requires running auth-enabled Senko instance")
}

fn flush(conn: &mut Connection) {
    if redis::cmd("FLUSHDB").query::<()>(conn).is_ok() {
        return;
    }
    let _: () = redis::cmd("FLUSHALL")
        .query(conn)
        .expect("bench requires FLUSHDB or FLUSHALL");
}

fn bench_ping(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_ping");
    group.throughput(Throughput::Elements(PING_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_ping", PING_OPS), |b| {
        b.iter(|| {
            for _ in 0..PING_OPS {
                let pong: String = redis::cmd("PING").query(&mut conn).unwrap();
                black_box(pong);
            }
        });
    });
    group.finish();
}

fn bench_ping_inline(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_ping_inline");
    group.throughput(Throughput::Elements(PING_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_ping_inline", PING_OPS), |b| {
        b.iter(|| {
            for _ in 0..PING_OPS {
                let pong: String = redis::cmd("PING").arg("inline").query(&mut conn).unwrap();
                black_box(pong);
            }
        });
    });
    group.finish();
}

fn bench_hello_2(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_hello_2");
    group.throughput(Throughput::Elements(HELLO_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hello_2", HELLO_OPS), |b| {
        b.iter(|| {
            for _ in 0..HELLO_OPS {
                let value: Value = redis::cmd("HELLO").arg(2).query(&mut conn).unwrap();
                black_box(value);
            }
        });
    });
    group.finish();
}

fn bench_client_id(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_client_id");
    group.throughput(Throughput::Elements(CLIENT_ID_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_client_id", CLIENT_ID_OPS), |b| {
        b.iter(|| {
            for _ in 0..CLIENT_ID_OPS {
                let id: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn).unwrap();
                black_box(id);
            }
        });
    });
    group.finish();
}

fn bench_client_info(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_client_info");
    group.throughput(Throughput::Elements(CLIENT_INFO_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_client_info", CLIENT_INFO_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..CLIENT_INFO_OPS {
                    let info: String = redis::cmd("CLIENT").arg("INFO").query(&mut conn).unwrap();
                    black_box(info);
                }
            });
        },
    );
    group.finish();
}

fn bench_client_list_100(c: &mut Criterion) {
    let mut control = connect();
    let mut keepalive = Vec::with_capacity(100);
    for _ in 0..100 {
        keepalive.push(connect());
    }
    let mut group = c.benchmark_group("connection_client_list_100");
    group.throughput(Throughput::Elements(CLIENT_LIST_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_client_list_100", CLIENT_LIST_OPS),
        |b| {
            b.iter(|| {
                for _ in 0..CLIENT_LIST_OPS {
                    let list: String = redis::cmd("CLIENT")
                        .arg("LIST")
                        .query(&mut control)
                        .unwrap();
                    black_box(&list);
                }
                black_box(&keepalive);
            });
        },
    );
    group.finish();
}

fn bench_auth_correct(c: &mut Criterion) {
    let password = requirepass();
    let mut conn = connect_auth();
    let mut group = c.benchmark_group("connection_auth_correct");
    group.throughput(Throughput::Elements(AUTH_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_auth_correct", AUTH_OPS), |b| {
        b.iter(|| {
            for _ in 0..AUTH_OPS {
                let ok: String = redis::cmd("AUTH").arg(&password).query(&mut conn).unwrap();
                black_box(ok);
            }
        });
    });
    group.finish();
}

fn bench_tracking_track(c: &mut Criterion) {
    let mut off_conn = connect();
    let mut on_conn = connect();
    let mut writer = connect();
    flush(&mut off_conn);
    let _: String = redis::cmd("SET")
        .arg("bench:tracking:key")
        .arg("0")
        .query(&mut writer)
        .unwrap();
    let _: Value = redis::cmd("HELLO").arg(3).query(&mut on_conn).unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("TRACKING")
        .arg("ON")
        .query(&mut on_conn)
        .unwrap();

    let mut group = c.benchmark_group("connection_tracking_track");
    group.throughput(Throughput::Elements(TRACKING_GET_OPS as u64));
    group.bench_function(BenchmarkId::new("tracking_off", TRACKING_GET_OPS), |b| {
        b.iter(|| {
            for _ in 0..TRACKING_GET_OPS {
                let value: String = redis::cmd("GET")
                    .arg("bench:tracking:key")
                    .query(&mut off_conn)
                    .unwrap();
                black_box(value);
            }
        });
    });
    group.bench_function(BenchmarkId::new("tracking_on", TRACKING_GET_OPS), |b| {
        b.iter(|| {
            for i in 0..TRACKING_GET_OPS {
                let value: String = redis::cmd("GET")
                    .arg("bench:tracking:key")
                    .query(&mut on_conn)
                    .unwrap();
                black_box(value);
                let _: String = redis::cmd("SET")
                    .arg("bench:tracking:key")
                    .arg((i & 255).to_string())
                    .query(&mut writer)
                    .unwrap();
            }
        });
    });
    group.finish();
}

fn bench_tracking_invalidate(c: &mut Criterion) {
    let mut writer = connect();
    flush(&mut writer);
    let _: String = redis::cmd("SET")
        .arg("bench:invalidate:key")
        .arg("seed")
        .query(&mut writer)
        .unwrap();

    let mut tracked_clients = Vec::with_capacity(100);
    for _ in 0..100 {
        let mut conn = connect();
        let _: Value = redis::cmd("HELLO").arg(3).query(&mut conn).unwrap();
        let _: String = redis::cmd("CLIENT")
            .arg("TRACKING")
            .arg("ON")
            .query(&mut conn)
            .unwrap();
        let _: String = redis::cmd("GET")
            .arg("bench:invalidate:key")
            .query(&mut conn)
            .unwrap();
        tracked_clients.push(conn);
    }

    let mut group = c.benchmark_group("connection_tracking_invalidate");
    group.throughput(Throughput::Elements(TRACKING_INVALIDATE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_tracking_invalidate", TRACKING_INVALIDATE_OPS),
        |b| {
            b.iter(|| {
                for i in 0..TRACKING_INVALIDATE_OPS {
                    let _: String = redis::cmd("SET")
                        .arg("bench:invalidate:key")
                        .arg((i & 255).to_string())
                        .query(&mut writer)
                        .unwrap();
                    for conn in &mut tracked_clients {
                        let _: String = redis::cmd("GET")
                            .arg("bench:invalidate:key")
                            .query(conn)
                            .unwrap();
                        black_box(conn);
                    }
                }
            });
        },
    );
    group.finish();
}

fn bench_pause_unpause(c: &mut Criterion) {
    let mut conn = connect();
    let mut group = c.benchmark_group("connection_pause_unpause");
    group.throughput(Throughput::Elements(PAUSE_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_pause_unpause", PAUSE_OPS), |b| {
        b.iter(|| {
            for _ in 0..PAUSE_OPS {
                let ok: String = redis::cmd("CLIENT")
                    .arg("PAUSE")
                    .arg(0)
                    .query(&mut conn)
                    .unwrap();
                black_box(&ok);
                let ok: String = redis::cmd("CLIENT")
                    .arg("UNPAUSE")
                    .query(&mut conn)
                    .unwrap();
                black_box(ok);
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_ping,
    bench_ping_inline,
    bench_hello_2,
    bench_client_id,
    bench_client_info,
    bench_client_list_100,
    bench_auth_correct,
    bench_tracking_track,
    bench_tracking_invalidate,
    bench_pause_unpause,
);
criterion_main!(benches);
