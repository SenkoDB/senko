use bytes::Bytes;
use compact_str::CompactString;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{
    commands::hash::{advanced, basic, scan},
    store::{Store, current_unix_ms},
};

const HSET_OPS: usize = 10_000_000;
const HGET_OPS: usize = 10_000_000;
const HDEL_OPS: usize = 5_000_000;
const HGETALL_16_OPS: usize = 1_000_000;
const HGETALL_256_OPS: usize = 500_000;
const HINCRBY_OPS: usize = 10_000_000;
const HMGET_16_OPS: usize = 1_000_000;
const HRANDFIELD_OPS: usize = 500_000;
const HSCAN_OPS: usize = 100_000;
const FIELD_EXPIRY_ADVANCE_OPS: usize = 10_000;

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn bench_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_ops");

    group.throughput(Throughput::Elements(HSET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hset_new_field", HSET_OPS), |b| {
        b.iter(|| {
            let mut store = Store::default();
            let hash = store.get_or_create_hash(CompactString::from("h"));
            for i in 0..HSET_OPS {
                let _ = hash.set(
                    CompactString::from(format!("f{i}")),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
        });
    });

    group.throughput(Throughput::Elements(HSET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hset_update_field", HSET_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("h"));
        let _ = hash.set(CompactString::from("field"), SenkoValue::Int(0), None);
        b.iter(|| {
            for i in 0..HSET_OPS {
                let _ = hash.set(
                    CompactString::from("field"),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
        });
    });

    group.throughput(Throughput::Elements(HGET_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_hget_hit_hashtable", HGET_OPS),
        |b| {
            let mut store = Store::default();
            let hash = store.get_or_create_hash(CompactString::from("h"));
            for i in 0..1000 {
                let _ = hash.set(
                    CompactString::from(format!("f{i}")),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
            b.iter(|| {
                for i in 0..HGET_OPS {
                    let field = format!("f{}", i % 1000);
                    let _ =
                        basic::hget(&mut store, &[bs(b"h"), Frame::BulkString(field.as_bytes())]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(HGET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hget_hit_listpack", HGET_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("h"));
        for i in 0..16 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        b.iter(|| {
            for i in 0..HGET_OPS {
                let field = format!("f{}", i % 16);
                let _ = basic::hget(&mut store, &[bs(b"h"), Frame::BulkString(field.as_bytes())]);
            }
        });
    });

    group.throughput(Throughput::Elements(HGET_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hget_miss", HGET_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("h"));
        for i in 0..1000 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        b.iter(|| {
            for i in 0..HGET_OPS {
                let field = format!("x{}", i % 1000);
                let _ = basic::hget(&mut store, &[bs(b"h"), Frame::BulkString(field.as_bytes())]);
            }
        });
    });

    group.throughput(Throughput::Elements(HDEL_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hdel", HDEL_OPS), |b| {
        b.iter(|| {
            let mut store = Store::default();
            let hash = store.get_or_create_hash(CompactString::from("h"));
            for i in 0..1000 {
                let _ = hash.set(
                    CompactString::from(format!("f{i}")),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
            for i in 0..HDEL_OPS {
                let field = format!("f{}", i % 1000);
                let _ = basic::hdel(&mut store, &[bs(b"h"), Frame::BulkString(field.as_bytes())]);
            }
        });
    });

    group.throughput(Throughput::Elements(HGETALL_16_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hgetall_16", HGETALL_16_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("h16"));
        for i in 0..16 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        b.iter(|| {
            for _ in 0..HGETALL_16_OPS {
                let _ = basic::hgetall(&mut store, &[bs(b"h16")]);
            }
        });
    });

    group.throughput(Throughput::Elements(HGETALL_256_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_hgetall_256", HGETALL_256_OPS),
        |b| {
            let mut store = Store::default();
            let hash = store.get_or_create_hash(CompactString::from("h256"));
            for i in 0..256 {
                let _ = hash.set(
                    CompactString::from(format!("f{i}")),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
            b.iter(|| {
                for _ in 0..HGETALL_256_OPS {
                    let _ = basic::hgetall(&mut store, &[bs(b"h256")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(HINCRBY_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hincrby", HINCRBY_OPS), |b| {
        let mut store = Store::default();
        let _ = basic::hset(&mut store, &[bs(b"hinc"), bs(b"n"), bs(b"0")]);
        b.iter(|| {
            for _ in 0..HINCRBY_OPS {
                let _ = advanced::hincrby(&mut store, &[bs(b"hinc"), bs(b"n"), bs(b"1")]);
            }
        });
    });

    group.throughput(Throughput::Elements(HMGET_16_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hmget_16", HMGET_16_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("hm"));
        let mut fields = Vec::new();
        for i in 0..16 {
            let f = format!("f{i}");
            let _ = hash.set(
                CompactString::from(f.as_str()),
                SenkoValue::Int(i as i64),
                None,
            );
            fields.push(f);
        }
        b.iter(|| {
            for _ in 0..HMGET_16_OPS {
                let mut args = Vec::with_capacity(17);
                args.push(bs(b"hm"));
                for field in &fields {
                    args.push(Frame::BulkString(field.as_bytes()));
                }
                let _ = basic::hmget(&mut store, &args);
            }
        });
    });

    group.throughput(Throughput::Elements(HRANDFIELD_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_hrandfield_positive", HRANDFIELD_OPS),
        |b| {
            let mut store = Store::default();
            let hash = store.get_or_create_hash(CompactString::from("hr"));
            for i in 0..100 {
                let _ = hash.set(
                    CompactString::from(format!("f{i}")),
                    SenkoValue::Int(i as i64),
                    None,
                );
            }
            b.iter(|| {
                for _ in 0..HRANDFIELD_OPS {
                    let _ = advanced::hrandfield(&mut store, &[bs(b"hr"), bs(b"10")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(HSCAN_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_hscan_full", HSCAN_OPS), |b| {
        let mut store = Store::default();
        let hash = store.get_or_create_hash(CompactString::from("hs"));
        for i in 0..10 {
            let _ = hash.set(
                CompactString::from(format!("f{i}")),
                SenkoValue::Int(i as i64),
                None,
            );
        }
        b.iter(|| {
            for _ in 0..HSCAN_OPS {
                let cursor = b"0";
                let _ = scan::hscan(
                    &mut store,
                    &[
                        bs(b"hs"),
                        Frame::BulkString(cursor),
                        bs(b"COUNT"),
                        bs(b"10"),
                    ],
                );
            }
        });
    });

    group.throughput(Throughput::Elements(FIELD_EXPIRY_ADVANCE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_field_expiry_advance", FIELD_EXPIRY_ADVANCE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..1000 {
                let key = format!("k{i}");
                for j in 0..10 {
                    let field = format!("f{j}");
                    {
                        let hash = store.get_or_create_hash(CompactString::from(key.as_str()));
                        let _ = hash.set(
                            CompactString::from(field.as_str()),
                            SenkoValue::Raw(Bytes::from_static(b"v")),
                            Some(current_unix_ms() + 200),
                        );
                    }
                    store.schedule_hash_field_expiry(
                        CompactString::from(key.as_str()),
                        CompactString::from(field.as_str()),
                        current_unix_ms() + 200,
                    );
                }
            }
            b.iter(|| {
                let _ = store.advance_expiry_wheel(current_unix_ms() + 500);
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_hash);
criterion_main!(benches);
