use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use redis::Connection;

const MULTI_EXEC_3_SETS_OPS: usize = 1_000_000;
const MULTI_EXEC_10_CMDS_OPS: usize = 500_000;
const WATCH_NO_CONFLICT_OPS: usize = 1_000_000;
const WATCH_CONFLICT_RATE_OPS: usize = 200_000;
const DISCARD_OPS: usize = 2_000_000;
const TX_VS_BARE_OPS: usize = 1_000_000;

fn connect() -> Connection {
    let url =
        std::env::var("SENKO_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let client = redis::Client::open(url).expect("bench requires valid redis url");
    client
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

fn bench_multi_exec_3_sets(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("transaction_multi_exec_3_sets");
    group.throughput(Throughput::Elements(MULTI_EXEC_3_SETS_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_multi_exec_3_sets", MULTI_EXEC_3_SETS_OPS),
        |b| {
            b.iter(|| {
                for i in 0..MULTI_EXEC_3_SETS_OPS {
                    let k0 = format!("tx:b3:{}:0", i & 1023);
                    let k1 = format!("tx:b3:{}:1", i & 1023);
                    let k2 = format!("tx:b3:{}:2", i & 1023);
                    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(&k0)
                        .arg("v0")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(&k1)
                        .arg("v1")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(&k2)
                        .arg("v2")
                        .query(&mut conn)
                        .unwrap();
                    let _: redis::Value = redis::cmd("EXEC").query(&mut conn).unwrap();
                }
            });
        },
    );
    group.finish();
}

fn bench_multi_exec_10_cmds(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("transaction_multi_exec_10_cmds");
    group.throughput(Throughput::Elements(MULTI_EXEC_10_CMDS_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_multi_exec_10_cmds", MULTI_EXEC_10_CMDS_OPS),
        |b| {
            b.iter(|| {
                for i in 0..MULTI_EXEC_10_CMDS_OPS {
                    let base = i & 1023;
                    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:mixed:{base}:0"))
                        .arg("0")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:mixed:{base}:1"))
                        .arg("1")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("INCR")
                        .arg(format!("tx:mixed:{base}:0"))
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("GET")
                        .arg(format!("tx:mixed:{base}:0"))
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:mixed:{base}:2"))
                        .arg("2")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("GET")
                        .arg(format!("tx:mixed:{base}:1"))
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:mixed:{base}:3"))
                        .arg("3")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("INCR")
                        .arg(format!("tx:mixed:{base}:3"))
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("GET")
                        .arg(format!("tx:mixed:{base}:2"))
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:mixed:{base}:4"))
                        .arg("4")
                        .query(&mut conn)
                        .unwrap();
                    let _: redis::Value = redis::cmd("EXEC").query(&mut conn).unwrap();
                }
            });
        },
    );
    group.finish();
}

fn bench_watch_no_conflict(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("transaction_watch_no_conflict");
    group.throughput(Throughput::Elements(WATCH_NO_CONFLICT_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_watch_no_conflict", WATCH_NO_CONFLICT_OPS),
        |b| {
            b.iter(|| {
                for i in 0..WATCH_NO_CONFLICT_OPS {
                    let key = format!("tx:watch:{}", i & 1023);
                    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn).unwrap();
                    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(&key)
                        .arg("v")
                        .query(&mut conn)
                        .unwrap();
                    let _: redis::Value = redis::cmd("EXEC").query(&mut conn).unwrap();
                }
            });
        },
    );
    group.finish();
}

fn bench_watch_conflict_rate(c: &mut Criterion) {
    let mut conn_a = connect();
    let mut conn_b = connect();
    flush(&mut conn_a);
    let mut group = c.benchmark_group("transaction_watch_conflict_rate");
    group.throughput(Throughput::Elements(WATCH_CONFLICT_RATE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_watch_conflict_rate", WATCH_CONFLICT_RATE_OPS),
        |b| {
            let mut conflicts = 0usize;
            b.iter(|| {
                for i in 0..WATCH_CONFLICT_RATE_OPS {
                    let key = format!("tx:conflict:{}", i & 255);
                    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn_a).unwrap();
                    if i % 10 == 0 {
                        conflicts += 1;
                        let _: () = redis::cmd("SET")
                            .arg(&key)
                            .arg("external")
                            .query(&mut conn_b)
                            .unwrap();
                    }
                    let _: () = redis::cmd("MULTI").query(&mut conn_a).unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(&key)
                        .arg("tx")
                        .query(&mut conn_a)
                        .unwrap();
                    let _: redis::Value = redis::cmd("EXEC").query(&mut conn_a).unwrap();
                }
                std::hint::black_box(conflicts);
            });
        },
    );
    group.finish();
}

fn bench_discard(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("transaction_discard");
    group.throughput(Throughput::Elements(DISCARD_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_discard", DISCARD_OPS), |b| {
        b.iter(|| {
            for i in 0..DISCARD_OPS {
                let base = i & 1023;
                let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();
                for slot in 0..5 {
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:discard:{base}:{slot}"))
                        .arg(slot)
                        .query(&mut conn)
                        .unwrap();
                }
                let _: () = redis::cmd("DISCARD").query(&mut conn).unwrap();
            }
        });
    });
    group.finish();
}

fn bench_tx_vs_bare(c: &mut Criterion) {
    let mut conn = connect();
    flush(&mut conn);
    let mut group = c.benchmark_group("transaction_tx_vs_bare");
    group.throughput(Throughput::Elements(TX_VS_BARE_OPS as u64));

    group.bench_function(
        BenchmarkId::new("bench_tx_vs_bare_bare", TX_VS_BARE_OPS),
        |b| {
            b.iter(|| {
                for i in 0..TX_VS_BARE_OPS {
                    let base = i & 1023;
                    let mut pipe = redis::pipe();
                    pipe.cmd("SET")
                        .arg(format!("tx:bare:{base}:0"))
                        .arg("0")
                        .ignore()
                        .cmd("SET")
                        .arg(format!("tx:bare:{base}:1"))
                        .arg("1")
                        .ignore()
                        .cmd("SET")
                        .arg(format!("tx:bare:{base}:2"))
                        .arg("2")
                        .ignore();
                    let _: () = pipe.query(&mut conn).unwrap();
                }
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("bench_tx_vs_bare_tx", TX_VS_BARE_OPS),
        |b| {
            b.iter(|| {
                for i in 0..TX_VS_BARE_OPS {
                    let base = i & 1023;
                    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:tx:{base}:0"))
                        .arg("0")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:tx:{base}:1"))
                        .arg("1")
                        .query(&mut conn)
                        .unwrap();
                    let _: () = redis::cmd("SET")
                        .arg(format!("tx:tx:{base}:2"))
                        .arg("2")
                        .query(&mut conn)
                        .unwrap();
                    let _: redis::Value = redis::cmd("EXEC").query(&mut conn).unwrap();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_multi_exec_3_sets,
    bench_multi_exec_10_cmds,
    bench_watch_no_conflict,
    bench_watch_conflict_rate,
    bench_discard,
    bench_tx_vs_bare
);
criterion_main!(benches);
