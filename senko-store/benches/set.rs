use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_proto::Frame;
use senko_store::{
    commands::set::{algebra, basic, scan},
    store::Store,
};

const SADD_INTSET_OPS: usize = 10_000_000;
const SADD_HASHTABLE_OPS: usize = 10_000_000;
const SISMEMBER_INTSET_OPS: usize = 10_000_000;
const SISMEMBER_HASHTABLE_OPS: usize = 10_000_000;
const SMEMBERS_SMALL_OPS: usize = 1_000_000;
const SMEMBERS_LARGE_OPS: usize = 100_000;
const SPOP_SINGLE_OPS: usize = 5_000_000;
const SRANDMEMBER_10_OPS: usize = 1_000_000;
const SDIFF_INTSET_OPS: usize = 500_000;
const SDIFF_HASHTABLE_OPS: usize = 500_000;
const SINTER_EARLY_EXIT_OPS: usize = 1_000_000;
const SINTER_LARGE_ROARING_OPS: usize = 10_000;
const SUNION_LARGE_OPS: usize = 10_000;
const SSCAN_FULL_OPS: usize = 100_000;

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_ops");

    group.throughput(Throughput::Elements(SADD_INTSET_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sadd_intset", SADD_INTSET_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for i in 0..SADD_INTSET_OPS {
                    let key = b"s";
                    let member = i.to_string();
                    let _ =
                        basic::sadd(&mut store, &[bs(key), Frame::BulkString(member.as_bytes())]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SADD_HASHTABLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sadd_hashtable", SADD_HASHTABLE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..200 {
                let member = format!("seed{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"sh"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for i in 0..SADD_HASHTABLE_OPS {
                    let member = format!("m{}", i % 200);
                    let _ = basic::sadd(
                        &mut store,
                        &[bs(b"sh"), Frame::BulkString(member.as_bytes())],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SISMEMBER_INTSET_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sismember_intset", SISMEMBER_INTSET_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..256 {
                let member = i.to_string();
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"si"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for i in 0..SISMEMBER_INTSET_OPS {
                    let member = (i % 256).to_string();
                    let _ = basic::sismember(
                        &mut store,
                        &[bs(b"si"), Frame::BulkString(member.as_bytes())],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SISMEMBER_HASHTABLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sismember_hashtable", SISMEMBER_HASHTABLE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..1000 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"shm"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for i in 0..SISMEMBER_HASHTABLE_OPS {
                    let member = format!("v{}", i % 1000);
                    let _ = basic::sismember(
                        &mut store,
                        &[bs(b"shm"), Frame::BulkString(member.as_bytes())],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SMEMBERS_SMALL_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_smembers_small", SMEMBERS_SMALL_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..16 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"ss"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SMEMBERS_SMALL_OPS {
                    let _ = basic::smembers(&mut store, &[bs(b"ss")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SMEMBERS_LARGE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_smembers_large", SMEMBERS_LARGE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..1000 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"sl"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SMEMBERS_LARGE_OPS {
                    let _ = basic::smembers(&mut store, &[bs(b"sl")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SPOP_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_spop_single", SPOP_SINGLE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..10_000 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"pop"), Frame::BulkString(member.as_bytes())],
                );
            }
            let mut next = 10_000usize;
            b.iter(|| {
                for _ in 0..SPOP_SINGLE_OPS {
                    let _ = basic::spop(&mut store, &[bs(b"pop")]);
                    let member = format!("v{next}");
                    let _ = basic::sadd(
                        &mut store,
                        &[bs(b"pop"), Frame::BulkString(member.as_bytes())],
                    );
                    next += 1;
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SRANDMEMBER_10_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_srandmember_10", SRANDMEMBER_10_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..1000 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"rand"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SRANDMEMBER_10_OPS {
                    let _ = basic::srandmember(&mut store, &[bs(b"rand"), bs(b"10")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SDIFF_INTSET_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sdiff_intset_sorted", SDIFF_INTSET_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..256 {
                let a = i.to_string();
                let bmember = (i + 128).to_string();
                let _ = basic::sadd(&mut store, &[bs(b"a"), Frame::BulkString(a.as_bytes())]);
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"b"), Frame::BulkString(bmember.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SDIFF_INTSET_OPS {
                    let _ = algebra::sdiff(&mut store, &[bs(b"a"), bs(b"b")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SDIFF_HASHTABLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sdiff_hashtable", SDIFF_HASHTABLE_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..1000 {
                let a = format!("a{i}");
                let bmember = format!("a{}", i + 500);
                let _ = basic::sadd(&mut store, &[bs(b"ha"), Frame::BulkString(a.as_bytes())]);
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"hb"), Frame::BulkString(bmember.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SDIFF_HASHTABLE_OPS {
                    let _ = algebra::sdiff(&mut store, &[bs(b"ha"), bs(b"hb")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SINTER_EARLY_EXIT_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sinter_early_exit", SINTER_EARLY_EXIT_OPS),
        |b| {
            let mut store = Store::default();
            let _ = basic::sadd(&mut store, &[bs(b"filled"), bs(b"x")]);
            let _ = basic::sadd(&mut store, &[bs(b"other"), bs(b"y")]);
            let _ = basic::sadd(&mut store, &[bs(b"empty_seed"), bs(b"tmp")]);
            let _ = basic::srem(&mut store, &[bs(b"empty_seed"), bs(b"tmp")]);
            b.iter(|| {
                for _ in 0..SINTER_EARLY_EXIT_OPS {
                    let _ =
                        algebra::sinter(&mut store, &[bs(b"missing"), bs(b"filled"), bs(b"other")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SINTER_LARGE_ROARING_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sinter_large_roaring", SINTER_LARGE_ROARING_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..10_000 {
                let a = i.to_string();
                let bmember = (i + 5_000).to_string();
                let _ = basic::sadd(&mut store, &[bs(b"ra"), Frame::BulkString(a.as_bytes())]);
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"rb"), Frame::BulkString(bmember.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SINTER_LARGE_ROARING_OPS {
                    let _ = algebra::sinter(&mut store, &[bs(b"ra"), bs(b"rb")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SUNION_LARGE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sunion_large", SUNION_LARGE_OPS),
        |b| {
            let mut store = Store::default();
            for set_idx in 0..5 {
                for i in 0..1000 {
                    let member = format!("v{}", i + set_idx * 300);
                    let key = format!("u{set_idx}");
                    let _ = basic::sadd(
                        &mut store,
                        &[
                            Frame::BulkString(key.as_bytes()),
                            Frame::BulkString(member.as_bytes()),
                        ],
                    );
                }
            }
            b.iter(|| {
                for _ in 0..SUNION_LARGE_OPS {
                    let _ = algebra::sunion(
                        &mut store,
                        &[bs(b"u0"), bs(b"u1"), bs(b"u2"), bs(b"u3"), bs(b"u4")],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(SSCAN_FULL_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_sscan_full_hashtable", SSCAN_FULL_OPS),
        |b| {
            let mut store = Store::default();
            for i in 0..100 {
                let member = format!("v{i}");
                let _ = basic::sadd(
                    &mut store,
                    &[bs(b"scan"), Frame::BulkString(member.as_bytes())],
                );
            }
            b.iter(|| {
                for _ in 0..SSCAN_FULL_OPS {
                    let _ = scan::sscan(
                        &mut store,
                        &[bs(b"scan"), bs(b"0"), bs(b"COUNT"), bs(b"10")],
                    );
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_set);
criterion_main!(benches);
