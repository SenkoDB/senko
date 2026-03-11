use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_core::QuickList;
use senko_proto::Frame;
use senko_store::{
    Store,
    commands::list::{basic, blocking, mutation, query},
};

const LPUSH_SINGLE_OPS: usize = 10_000_000;
const RPUSH_SINGLE_OPS: usize = 10_000_000;
const LPUSH_BATCH_10_OPS: usize = 1_000_000;
const LPOP_SINGLE_OPS: usize = 10_000_000;
const RPOP_SINGLE_OPS: usize = 10_000_000;
const LRANGE_10_OPS: usize = 1_000_000;
const LRANGE_100_OPS: usize = 500_000;
const LINDEX_HEAD_OPS: usize = 10_000_000;
const LINDEX_TAIL_OPS: usize = 10_000_000;
const LINDEX_MIDDLE_OPS: usize = 5_000_000;
const LREM_ALL_OPS: usize = 100_000;
const LMOVE_OPS: usize = 5_000_000;
const LPOS_RANK_OPS: usize = 1_000_000;
const BLPOP_FAST_OPS: usize = 5_000_000;

fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
    Frame::BulkString(input)
}

fn populate_list(store: &mut Store, key: &str, len: usize) {
    let list = store.get_or_create_list(compact_str::CompactString::from(key));
    for i in 0..len {
        list.push_back(i.to_string().as_bytes());
    }
}

fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_ops");

    group.throughput(Throughput::Elements(LPUSH_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_lpush_single", LPUSH_SINGLE_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for _ in 0..LPUSH_SINGLE_OPS {
                    let _ = basic::lpush(&mut store, &[bs(b"k"), bs(b"x")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(RPUSH_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_rpush_single", RPUSH_SINGLE_OPS),
        |b| {
            b.iter(|| {
                let mut store = Store::default();
                for _ in 0..RPUSH_SINGLE_OPS {
                    let _ = basic::rpush(&mut store, &[bs(b"k"), bs(b"x")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements((LPUSH_BATCH_10_OPS * 10) as u64));
    group.bench_function(
        BenchmarkId::new("bench_lpush_batch_10", LPUSH_BATCH_10_OPS),
        |b| {
            let args = [
                bs(b"k"),
                bs(b"0"),
                bs(b"1"),
                bs(b"2"),
                bs(b"3"),
                bs(b"4"),
                bs(b"5"),
                bs(b"6"),
                bs(b"7"),
                bs(b"8"),
                bs(b"9"),
            ];
            b.iter(|| {
                let mut store = Store::default();
                for _ in 0..LPUSH_BATCH_10_OPS {
                    let _ = basic::lpush(&mut store, &args);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(LPOP_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_lpop_single", LPOP_SINGLE_OPS),
        |b| {
            b.iter(|| {
                let mut list = QuickList::default();
                for i in 0..10_000 {
                    list.push_back(i.to_string().as_bytes());
                }
                for i in 0..LPOP_SINGLE_OPS {
                    if list.is_empty() {
                        for j in 0..10_000 {
                            list.push_back((i + j).to_string().as_bytes());
                        }
                    }
                    let _ = list.pop_front();
                }
            });
        },
    );

    group.throughput(Throughput::Elements(RPOP_SINGLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_rpop_single", RPOP_SINGLE_OPS),
        |b| {
            b.iter(|| {
                let mut list = QuickList::default();
                for i in 0..10_000 {
                    list.push_back(i.to_string().as_bytes());
                }
                for i in 0..RPOP_SINGLE_OPS {
                    if list.is_empty() {
                        for j in 0..10_000 {
                            list.push_back((i + j).to_string().as_bytes());
                        }
                    }
                    let _ = list.pop_back();
                }
            });
        },
    );

    group.throughput(Throughput::Elements(LRANGE_10_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_lrange_10", LRANGE_10_OPS), |b| {
        let mut store = Store::default();
        populate_list(&mut store, "k", 128);
        b.iter(|| {
            for _ in 0..LRANGE_10_OPS {
                let _ = query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"9")]);
            }
        });
    });

    group.throughput(Throughput::Elements(LRANGE_100_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_lrange_100", LRANGE_100_OPS), |b| {
        let mut store = Store::default();
        {
            let list = store.get_or_create_list(compact_str::CompactString::from("k"));
            list.fill = 16;
            for i in 0..1000 {
                list.push_back(i.to_string().as_bytes());
            }
        }
        b.iter(|| {
            for _ in 0..LRANGE_100_OPS {
                let _ = query::lrange(&mut store, &[bs(b"k"), bs(b"0"), bs(b"99")]);
            }
        });
    });

    group.throughput(Throughput::Elements(LINDEX_HEAD_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_lindex_head", LINDEX_HEAD_OPS),
        |b| {
            let mut store = Store::default();
            populate_list(&mut store, "k", 1000);
            b.iter(|| {
                for _ in 0..LINDEX_HEAD_OPS {
                    let _ = query::lindex(&mut store, &[bs(b"k"), bs(b"0")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(LINDEX_TAIL_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_lindex_tail", LINDEX_TAIL_OPS),
        |b| {
            let mut store = Store::default();
            populate_list(&mut store, "k", 1000);
            b.iter(|| {
                for _ in 0..LINDEX_TAIL_OPS {
                    let _ = query::lindex(&mut store, &[bs(b"k"), bs(b"-1")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(LINDEX_MIDDLE_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_lindex_middle", LINDEX_MIDDLE_OPS),
        |b| {
            let mut store = Store::default();
            populate_list(&mut store, "k", 1000);
            b.iter(|| {
                for _ in 0..LINDEX_MIDDLE_OPS {
                    let _ = query::lindex(&mut store, &[bs(b"k"), bs(b"500")]);
                }
            });
        },
    );

    group.throughput(Throughput::Elements(LREM_ALL_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_lrem_all", LREM_ALL_OPS), |b| {
        b.iter(|| {
            let mut store = Store::default();
            let list = store.get_or_create_list(compact_str::CompactString::from("k"));
            for i in 0..1000 {
                if i % 10 == 0 {
                    list.push_back(b"target");
                } else {
                    list.push_back(b"other");
                }
            }
            for _ in 0..LREM_ALL_OPS {
                let _ = mutation::lrem(&mut store, &[bs(b"k"), bs(b"0"), bs(b"target")]);
                let list = store.get_or_create_list(compact_str::CompactString::from("k"));
                for _ in 0..100 {
                    list.push_back(b"target");
                }
            }
        });
    });

    group.throughput(Throughput::Elements(LMOVE_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_lmove", LMOVE_OPS), |b| {
        let mut store = Store::default();
        populate_list(&mut store, "src", 1000);
        populate_list(&mut store, "dst", 1000);
        b.iter(|| {
            for i in 0..LMOVE_OPS {
                if store.get_list(b"src").is_none() {
                    populate_list(&mut store, "src", 1000 + (i % 10));
                }
                let _ = mutation::lmove(
                    &mut store,
                    &[bs(b"src"), bs(b"dst"), bs(b"RIGHT"), bs(b"LEFT")],
                );
            }
        });
    });

    group.throughput(Throughput::Elements(LPOS_RANK_OPS as u64));
    group.bench_function(BenchmarkId::new("bench_lpos_rank", LPOS_RANK_OPS), |b| {
        let mut store = Store::default();
        let list = store.get_or_create_list(compact_str::CompactString::from("k"));
        for i in 0..1000 {
            if i % 20 == 0 {
                list.push_back(b"target");
            } else {
                list.push_back(b"x");
            }
        }
        b.iter(|| {
            for _ in 0..LPOS_RANK_OPS {
                let _ = query::lpos(
                    &mut store,
                    &[bs(b"k"), bs(b"target"), bs(b"RANK"), bs(b"3")],
                );
            }
        });
    });

    group.throughput(Throughput::Elements(BLPOP_FAST_OPS as u64));
    group.bench_function(
        BenchmarkId::new("bench_blpop_no_block", BLPOP_FAST_OPS),
        |b| {
            let mut store = Store::default();
            populate_list(&mut store, "k", 1000);
            b.iter(|| {
                for i in 0..BLPOP_FAST_OPS {
                    let _ = blocking::blpop(&mut store, &[bs(b"k"), bs(b"1")]);
                    if store.get_list(b"k").is_none() {
                        populate_list(&mut store, "k", 1000 + (i % 10));
                    }
                }
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_list);
criterion_main!(benches);
