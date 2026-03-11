use bytes::Bytes;
use compact_str::CompactString;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_core::SenkoValue;
use senko_store::{SetCondition, SetExpiry, SetOptions, Store};

const OPS: usize = 1_000_000;

fn bench_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_ops");
    group.throughput(Throughput::Elements(OPS as u64));

    group.bench_function(BenchmarkId::new("set", OPS), |b| {
        b.iter(|| {
            let mut store = Store::default();
            for index in 0..OPS {
                let _ = store.set(
                    CompactString::from(format!("key{index}")),
                    SenkoValue::from(Bytes::from_static(b"value")),
                    SetOptions {
                        condition: SetCondition::Always,
                        expiry: SetExpiry::None,
                        get_old: false,
                    },
                );
            }
        });
    });

    let mut seeded = Store::default();
    for index in 0..OPS {
        let _ = seeded.set(
            CompactString::from(format!("key{index}")),
            SenkoValue::from(index as i64),
            SetOptions::default(),
        );
    }
    group.bench_function(BenchmarkId::new("get", OPS), |b| {
        b.iter(|| {
            for index in 0..OPS {
                let key = format!("key{index}");
                let _ = seeded.get(key.as_bytes());
            }
        });
    });

    group.bench_function(BenchmarkId::new("delete", OPS), |b| {
        b.iter(|| {
            let mut store = Store::default();
            for index in 0..OPS {
                let _ = store.set(
                    CompactString::from(format!("key{index}")),
                    SenkoValue::from(index as i64),
                    SetOptions::default(),
                );
            }
            for index in 0..OPS {
                let key = format!("key{index}");
                let _ = store.delete(key.as_bytes());
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_store);
criterion_main!(benches);
