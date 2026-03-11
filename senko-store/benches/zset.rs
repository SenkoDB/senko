use compact_str::CompactString;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use senko_store::zset::{BPTree, ScoreBound, ZAddOptions, ZSetObject};

fn member(i: usize) -> CompactString {
    CompactString::from(format!("m{i:05}"))
}

fn build_bptree(size: usize) -> BPTree {
    let mut tree = BPTree::new();
    for i in 0..size {
        let _ = tree.insert(i as f64, member(i));
    }
    tree
}

fn build_zset(size: usize) -> ZSetObject {
    let mut zset = ZSetObject::default();
    for i in 0..size {
        let _ = zset.add(i as f64, member(i), ZAddOptions::default());
    }
    zset
}

fn bench_zset(c: &mut Criterion) {
    let mut group = c.benchmark_group("zset");

    group.throughput(Throughput::Elements(64));
    group.bench_function("bench_zadd_new_listpack", |b| {
        b.iter_batched(
            ZSetObject::default,
            |mut zset| {
                for i in 0..64 {
                    let _ = zset.add(i as f64, member(i), ZAddOptions::default());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("bench_zadd_new_bptree", |b| {
        b.iter_batched(
            || build_zset(10_000),
            |mut zset| {
                let _ = zset.add(
                    10_001.0,
                    CompactString::from("tail"),
                    ZAddOptions::default(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("bench_zadd_update_bptree", |b| {
        b.iter_batched(
            || build_zset(10_000),
            |mut zset| {
                for i in 0..1_000 {
                    let _ = zset.add(20_000.0 + i as f64, member(i), ZAddOptions::default());
                }
            },
            BatchSize::SmallInput,
        );
    });

    let zscore_set = build_zset(10_000);
    group.bench_function("bench_zscore_bptree", |b| {
        b.iter(|| {
            for i in 0..1_000 {
                let _ = zscore_set.score(member(i).as_bytes());
            }
        });
    });

    let zrank_set = build_zset(10_000);
    group.bench_function("bench_zrank_bptree", |b| {
        b.iter(|| {
            for i in 0..1_000 {
                let _ = zrank_set.rank(member(i).as_bytes(), false);
            }
        });
    });

    let range_rank = build_zset(10_000);
    group.bench_function("bench_zrange_rank_100", |b| {
        b.iter(|| {
            let _: Vec<_> = range_rank.range_by_rank(500, 599, false, None).collect();
        });
    });

    let range_score = build_zset(10_000);
    group.bench_function("bench_zrange_score_100", |b| {
        b.iter(|| {
            let _: Vec<_> = range_score
                .range_by_score(
                    ScoreBound::Inclusive(500.0),
                    ScoreBound::Inclusive(599.0),
                    false,
                    None,
                )
                .collect();
        });
    });

    group.bench_function("bench_zpopmin", |b| {
        b.iter_batched(
            || build_zset(10_000),
            |mut zset| {
                for _ in 0..1_000 {
                    let _ = zset.pop_min(1);
                }
            },
            BatchSize::SmallInput,
        );
    });

    let range_limit = build_zset(10_000);
    group.bench_function("bench_zrangebyscore_limit", |b| {
        b.iter(|| {
            let _: Vec<_> = range_limit
                .range_by_score(
                    ScoreBound::Inclusive(0.0),
                    ScoreBound::Inclusive(500.0),
                    false,
                    Some((0, 10)),
                )
                .collect();
        });
    });

    group.bench_function("bench_zremrangebyscore", |b| {
        b.iter_batched(
            || build_zset(10_000),
            |mut zset| {
                let victims: Vec<_> = zset
                    .range_by_score(
                        ScoreBound::Inclusive(0.0),
                        ScoreBound::Inclusive(999.0),
                        false,
                        None,
                    )
                    .map(|(_, member)| member)
                    .collect();
                for member in victims {
                    let _ = zset.remove(member.as_bytes());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("bench_zinter_two_sets", |b| {
        let a = build_zset(1_000);
        let bset = build_zset(1_000);
        b.iter(|| {
            let mut hits = 0usize;
            for (score, member) in a.range_by_rank(0, -1, false, None) {
                if bset.score(member.as_bytes()).is_some() {
                    let _ = score;
                    hits += 1;
                }
            }
            hits
        });
    });

    group.bench_function("bench_zunion_five_sets", |b| {
        let sets: Vec<_> = (0..5).map(|i| build_zset(500 + i)).collect();
        b.iter(|| {
            let mut out = std::collections::HashMap::<CompactString, f64>::new();
            for set in &sets {
                for (score, member) in set.range_by_rank(0, -1, false, None) {
                    out.entry(member)
                        .and_modify(|s| *s += score)
                        .or_insert(score);
                }
            }
            out.len()
        });
    });

    let scan_set = build_zset(100);
    group.bench_function("bench_zscan_full", |b| {
        b.iter(|| {
            let _: Vec<_> = scan_set.range_by_rank(0, -1, false, None).collect();
        });
    });

    group.bench_function("bench_bptree_insert_random", |b| {
        b.iter_batched(
            SmallRng::from_entropy,
            |mut rng| {
                let mut tree = BPTree::new();
                for i in 0..10_000 {
                    let _ = tree.insert(rng.gen_range(0.0..1.0), member(i));
                }
            },
            BatchSize::SmallInput,
        );
    });

    let tree = build_bptree(100_000);
    group.bench_function("bench_bptree_range_scan", |b| {
        b.iter(|| {
            let _ = tree
                .range_by_score(
                    ScoreBound::Inclusive(10_000.0),
                    ScoreBound::Inclusive(11_000.0),
                )
                .count();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_zset);
criterion_main!(benches);
