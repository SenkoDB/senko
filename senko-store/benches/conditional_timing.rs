use bytes::Bytes;
use compact_str::CompactString;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use senko_core::SenkoValue;
use senko_store::{SetOptions, Store, commands::conditional};

fn delex_digest_timing(c: &mut Criterion) {
    let mut group = c.benchmark_group("delex_ifdeq_timing");
    group
        .sample_size(200)
        .significance_level(0.01)
        .confidence_level(0.99);

    let mut store = Store::default();

    let correct = {
        let digest = blake3::hash(b"value");
        let mut out = [0u8; 64];
        let hex = b"0123456789abcdef";
        for (i, b) in digest.as_bytes().iter().enumerate() {
            out[i * 2] = hex[(b >> 4) as usize];
            out[i * 2 + 1] = hex[(b & 0x0f) as usize];
        }
        out
    };
    let mut wrong = correct;
    wrong[0] = b'0';

    group.bench_with_input(BenchmarkId::new("correct", 64), &correct, |b, d| {
        b.iter(|| {
            let _ = store.set(
                CompactString::from("k"),
                SenkoValue::Raw(Bytes::from_static(b"value")),
                SetOptions::default(),
            );
            let _ = conditional::delex(
                &mut store,
                &[
                    senko_proto::Frame::BulkString(b"k"),
                    senko_proto::Frame::BulkString(b"IFDEQ"),
                    senko_proto::Frame::BulkString(d),
                ],
            );
        });
    });

    group.bench_with_input(BenchmarkId::new("wrong", 64), &wrong, |b, d| {
        b.iter(|| {
            let _ = store.set(
                CompactString::from("k"),
                SenkoValue::Raw(Bytes::from_static(b"value")),
                SetOptions::default(),
            );
            let _ = conditional::delex(
                &mut store,
                &[
                    senko_proto::Frame::BulkString(b"k"),
                    senko_proto::Frame::BulkString(b"IFDEQ"),
                    senko_proto::Frame::BulkString(d),
                ],
            );
        });
    });

    group.finish();
}

criterion_group!(benches, delex_digest_timing);
criterion_main!(benches);
