use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use senko_proto::{ParseStatus, RespParser};

fn parse_get_baseline(c: &mut Criterion) {
    let parser = RespParser::new();
    let frame = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let mut group = c.benchmark_group("resp_parser");
    group.bench_with_input(BenchmarkId::new("get", frame.len()), frame, |b, input| {
        b.iter(|| {
            let status = parser.parse(input).expect("parse success");
            match status {
                ParseStatus::Complete(_, consumed) => assert_eq!(consumed, input.len()),
                ParseStatus::Incomplete(_) => panic!("benchmark frame unexpectedly incomplete"),
            }
        });
    });
    group.finish();
}

criterion_group!(benches, parse_get_baseline);
criterion_main!(benches);
