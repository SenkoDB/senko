use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_core::StreamRadixTree;
use senko_proto::Frame;
use senko_store::{
    Store,
    commands::stream::{
        basic::xadd,
        claim::xautoclaim,
        group::{xack, xgroup, xpending},
        read::{xread, xreadgroup},
    },
};

const XADD_SINGLE_OPS: usize = 5_000_000;
const XADD_SAME_FIELDS_OPS: usize = 5_000_000;
const XADD_TRIM_OPS: usize = 1_000_000;
const XRANGE_100_OPS: usize = 1_000_000;
const XREAD_NO_BLOCK_OPS: usize = 1_000_000;
const XREADGROUP_DELIVER_OPS: usize = 1_000_000;
const XACK_100_OPS: usize = 500_000;
const XPENDING_SUMMARY_OPS: usize = 1_000_000;
const XAUTOCLAIM_FULL_OPS: usize = 100_000;
const RADIX_INSERT_OPS: usize = 10_000_000;
const RADIX_RANGE_OPS: usize = 1_000_000;

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn frame_buf(value: &str) -> Frame<'static> {
    Frame::BulkString(Box::leak(value.as_bytes().to_vec().into_boxed_slice()))
}

fn populate_stream(store: &mut Store, key: &str, count: usize) {
    for i in 0..count {
        let id = format!("{i}-0");
        let _ = xadd(
            store,
            &[
                frame_buf(key),
                frame_buf(&id),
                bs(b"f"),
                frame_buf(&i.to_string()),
            ],
        );
    }
}

fn build_pending_stream(count: usize) -> Store {
    let mut store = Store::default();
    let _ = xgroup(
        &mut store,
        &[
            bs(b"CREATE"),
            bs(b"bench-pel"),
            bs(b"g"),
            bs(b"0"),
            bs(b"MKSTREAM"),
        ],
    );
    for i in 0..count {
        let id = format!("{}-0", i + 1);
        let _ = xadd(
            &mut store,
            &[
                bs(b"bench-pel"),
                frame_buf(&id),
                bs(b"f"),
                frame_buf(&i.to_string()),
            ],
        );
    }
    let _ = xreadgroup(
        &mut store,
        &[
            bs(b"GROUP"),
            bs(b"g"),
            bs(b"c1"),
            bs(b"COUNT"),
            frame_buf(&count.to_string()),
            bs(b"STREAMS"),
            bs(b"bench-pel"),
            bs(b">"),
        ],
    );
    if let Some(group) = store
        .get_stream_mut(b"bench-pel")
        .and_then(|stream| stream.groups.get_mut("g"))
    {
        if let Some(state) = group.consumers.get_mut("c1") {
            for entry in state.pel.values_mut() {
                entry.delivery_time = 0;
            }
        }
    }
    store
}

fn bench_stream(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream");

    group.throughput(Throughput::Elements(XADD_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xadd_single", XADD_SINGLE_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for _ in 0..XADD_SINGLE_OPS {
                    let _ = xadd(
                        &mut store,
                        &[
                            bs(b"bench"),
                            bs(b"*"),
                            bs(b"f1"),
                            bs(b"v1"),
                            bs(b"f2"),
                            bs(b"v2"),
                        ],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(XADD_SAME_FIELDS_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xadd_same_fields", XADD_SAME_FIELDS_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for _ in 0..XADD_SAME_FIELDS_OPS {
                    let _ = xadd(
                        &mut store,
                        &[
                            bs(b"bench"),
                            bs(b"*"),
                            bs(b"f1"),
                            bs(b"v1"),
                            bs(b"f2"),
                            bs(b"v2"),
                        ],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(XADD_TRIM_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xadd_maxlen_trim", XADD_TRIM_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for i in 0..XADD_TRIM_OPS {
                    let _ = xadd(
                        &mut store,
                        &[
                            bs(b"bench"),
                            bs(b"MAXLEN"),
                            bs(b"~"),
                            bs(b"1000"),
                            bs(b"*"),
                            bs(b"f"),
                            frame_buf(&i.to_string()),
                        ],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(XRANGE_100_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_xrange_100", XRANGE_100_OPS), |b| {
        let mut store = Store::default();
        populate_stream(&mut store, "bench-range", 10_000);
        b.iter(|| {
            for _ in 0..XRANGE_100_OPS {
                let _ = senko_store::commands::stream::basic::xrange(
                    &mut store,
                    &[
                        bs(b"bench-range"),
                        bs(b"-"),
                        bs(b"+"),
                        bs(b"COUNT"),
                        bs(b"100"),
                    ],
                );
            }
        });
    });

    group.throughput(Throughput::Elements(XREAD_NO_BLOCK_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xread_no_block", XREAD_NO_BLOCK_OPS),
        |b| {
            let mut store = Store::default();
            populate_stream(&mut store, "bench-read", 10_000);
            b.iter(|| {
                for _ in 0..XREAD_NO_BLOCK_OPS {
                    let _ = xread(
                        &mut store,
                        &[
                            bs(b"COUNT"),
                            bs(b"10"),
                            bs(b"STREAMS"),
                            bs(b"bench-read"),
                            bs(b"0-0"),
                        ],
                    );
                }
            });
        },
    );

    group.throughput(Throughput::Elements(XREADGROUP_DELIVER_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xreadgroup_deliver", XREADGROUP_DELIVER_OPS),
        |b| {
            b.iter_batched(
                || {
                    let mut store = Store::default();
                    let _ = xgroup(
                        &mut store,
                        &[
                            bs(b"CREATE"),
                            bs(b"bench-group"),
                            bs(b"g"),
                            bs(b"0"),
                            bs(b"MKSTREAM"),
                        ],
                    );
                    populate_stream(&mut store, "bench-group", 10_000);
                    store
                },
                |mut store| {
                    for _ in 0..XREADGROUP_DELIVER_OPS {
                        let _ = xreadgroup(
                            &mut store,
                            &[
                                bs(b"GROUP"),
                                bs(b"g"),
                                bs(b"c1"),
                                bs(b"COUNT"),
                                bs(b"10"),
                                bs(b"STREAMS"),
                                bs(b"bench-group"),
                                bs(b">"),
                            ],
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        },
    );

    group.throughput(Throughput::Elements((XACK_100_OPS * 100) as u64));
    group.bench_function(BenchmarkId::new("bench_xack_100", XACK_100_OPS), |b| {
        b.iter_batched(
            || build_pending_stream(10_000),
            |mut store| {
                let ids = (1..=10_000).map(|i| format!("{i}-0")).collect::<Vec<_>>();
                for chunk in ids.chunks(100).take(XACK_100_OPS) {
                    let mut args = vec![bs(b"bench-pel"), bs(b"g")];
                    for id in chunk {
                        args.push(frame_buf(id));
                    }
                    let _ = xack(&mut store, &args);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(XPENDING_SUMMARY_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xpending_summary", XPENDING_SUMMARY_OPS),
        |b| {
            let mut store = build_pending_stream(1_000);
            b.iter(|| {
                for _ in 0..XPENDING_SUMMARY_OPS {
                    let _ = xpending(&mut store, &[bs(b"bench-pel"), bs(b"g")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(XAUTOCLAIM_FULL_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_xautoclaim_full", XAUTOCLAIM_FULL_OPS),
        |b| {
            b.iter_batched(
                || build_pending_stream(1_000),
                |mut store| {
                    for _ in 0..XAUTOCLAIM_FULL_OPS {
                        let _ = xautoclaim(
                            &mut store,
                            &[
                                bs(b"bench-pel"),
                                bs(b"g"),
                                bs(b"c2"),
                                bs(b"0"),
                                bs(b"0-0"),
                                bs(b"COUNT"),
                                bs(b"1000"),
                            ],
                        );
                    }
                },
                BatchSize::SmallInput,
            );
        },
    );

    group.throughput(Throughput::Elements(RADIX_INSERT_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_radix_tree_insert", RADIX_INSERT_OPS),
        |b| {
            b.iter(|| {
                let mut tree = StreamRadixTree::new();
                for i in 0..RADIX_INSERT_OPS {
                    let id = senko_core::StreamId {
                        ms: i as u64,
                        seq: 0,
                    };
                    let _ = tree.insert(id, &[(b"f".as_slice(), b"v".as_slice())]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(RADIX_RANGE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_radix_tree_range", RADIX_RANGE_OPS),
        |b| {
            let mut tree = StreamRadixTree::new();
            for i in 0..100_000u64 {
                let _ = tree.insert(
                    senko_core::StreamId { ms: i, seq: 0 },
                    &[(b"f".as_slice(), b"v".as_slice())],
                );
            }
            b.iter(|| {
                for _ in 0..RADIX_RANGE_OPS {
                    let _ = tree
                        .range(
                            senko_core::StreamId { ms: 50_000, seq: 0 },
                            senko_core::StreamId { ms: 50_099, seq: 0 },
                            Some(100),
                        )
                        .count();
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_stream);
criterion_main!(benches);
