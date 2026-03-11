use std::{cmp, hint::black_box, sync::Arc, thread, time::Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use senko_net::pubsub::fanout::{CrossShardBus, ShardChannelRouter, ShardFanOut};
use senko_pubsub::{BroadcastSlot, ChannelRegistry, RING_SIZE};

const SMALL_PAYLOAD: &[u8] = &[b'x'; 64];
const BATCH_LIMIT: usize = RING_SIZE - 1;

fn publish_batches(
    registry: &mut ChannelRegistry,
    channel: &[u8],
    payload: Bytes,
    slots: &[Arc<BroadcastSlot>],
    iters: u64,
) {
    let mut remaining = iters;
    while remaining > 0 {
        let batch = cmp::min(remaining as usize, BATCH_LIMIT);
        for _ in 0..batch {
            black_box(registry.publish(channel, payload.clone()));
        }
        for slot in slots {
            for _ in 0..batch {
                black_box(slot.recv());
            }
        }
        remaining -= batch as u64;
    }
}

fn exact_registry(subscribers: usize) -> (ChannelRegistry, Vec<Arc<BroadcastSlot>>) {
    let mut registry = ChannelRegistry::default();
    let mut slots = Vec::with_capacity(subscribers);
    for conn_id in 0..subscribers as u64 {
        slots.push(registry.subscribe(b"bench:exact", conn_id));
    }
    (registry, slots)
}

fn pattern_registry(
    patterns: usize,
    matching: usize,
) -> (ChannelRegistry, Vec<Arc<BroadcastSlot>>) {
    let mut registry = ChannelRegistry::default();
    let mut slots = Vec::with_capacity(patterns);
    for idx in 0..matching {
        let pattern = format!("bench.match.{idx}.*");
        slots.push(registry.psubscribe(pattern.as_bytes(), idx as u64));
    }
    for idx in matching..patterns {
        let pattern = format!("bench.nomatch.{idx}.*");
        slots.push(registry.psubscribe(pattern.as_bytes(), idx as u64));
    }
    (registry, slots)
}

fn fanout_shards(num_shards: usize) -> Vec<ShardFanOut> {
    let bus = Arc::new(CrossShardBus::new(num_shards));
    (0..num_shards)
        .map(|shard_id| ShardFanOut::new(shard_id, Arc::clone(&bus)))
        .collect()
}

fn flush_bus(shards: &mut [ShardFanOut]) {
    loop {
        let drained: usize = shards.iter_mut().map(ShardFanOut::drain_bus).sum();
        if drained == 0 {
            break;
        }
    }
}

fn publish_fanout_batches(
    fanout: &mut ShardFanOut,
    channel: &[u8],
    payload: Bytes,
    slots: &[Arc<BroadcastSlot>],
    iters: u64,
) {
    let mut remaining = iters;
    while remaining > 0 {
        let batch = cmp::min(remaining as usize, BATCH_LIMIT);
        for _ in 0..batch {
            black_box(fanout.publish(channel, payload.clone()));
        }
        for slot in slots {
            for _ in 0..batch {
                black_box(slot.recv());
            }
        }
        remaining -= batch as u64;
    }
}

fn spublish_batches(
    fanout: &mut ShardFanOut,
    channel: &[u8],
    payload: Bytes,
    slots: &[Arc<BroadcastSlot>],
    iters: u64,
) {
    let mut remaining = iters;
    while remaining > 0 {
        let batch = cmp::min(remaining as usize, BATCH_LIMIT);
        for _ in 0..batch {
            black_box(fanout.spublish_local(channel, payload.clone()));
        }
        for slot in slots {
            for _ in 0..batch {
                black_box(slot.recv());
            }
        }
        remaining -= batch as u64;
    }
}

fn bench_publish_1_subscriber(c: &mut Criterion) {
    let (mut registry, slots) = exact_registry(1);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_publish_1_subscriber");
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("bench_publish_1_subscriber", 1), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            publish_batches(
                &mut registry,
                b"bench:exact",
                payload.clone(),
                &slots,
                iters,
            );
            start.elapsed()
        });
    });
    group.finish();
}

fn bench_publish_100_subscribers(c: &mut Criterion) {
    let (mut registry, slots) = exact_registry(100);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_publish_100_subscribers");
    group.throughput(Throughput::Elements(100));
    group.bench_function(
        BenchmarkId::new("bench_publish_100_subscribers", 100),
        |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                publish_batches(
                    &mut registry,
                    b"bench:exact",
                    payload.clone(),
                    &slots,
                    iters,
                );
                start.elapsed()
            });
        },
    );
    group.finish();
}

fn bench_publish_10k_subscribers(c: &mut Criterion) {
    let (mut registry, slots) = exact_registry(10_000);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_publish_10k_subscribers");
    group.sample_size(10);
    group.throughput(Throughput::Elements(10_000));
    group.bench_function(
        BenchmarkId::new("bench_publish_10k_subscribers", 10_000),
        |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                publish_batches(
                    &mut registry,
                    b"bench:exact",
                    payload.clone(),
                    &slots,
                    iters,
                );
                start.elapsed()
            });
        },
    );
    group.finish();
}

fn bench_publish_0_subscribers(c: &mut Criterion) {
    let mut registry = ChannelRegistry::default();
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_publish_0_subscribers");
    group.throughput(Throughput::Elements(1));
    group.bench_function("bench_publish_0_subscribers", |b| {
        b.iter(|| black_box(registry.publish(b"bench:none", payload.clone())));
    });
    group.finish();
}

fn bench_psubscribe_10_patterns(c: &mut Criterion) {
    let (mut registry, _) = pattern_registry(10, 0);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_psubscribe_10_patterns");
    group.bench_function("bench_psubscribe_10_patterns", |b| {
        b.iter(|| black_box(registry.publish(b"bench.channel", payload.clone())));
    });
    group.finish();
}

fn bench_psubscribe_1000_patterns_nomatch(c: &mut Criterion) {
    let (mut registry, _) = pattern_registry(1_000, 0);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_psubscribe_1000_patterns_nomatch");
    group.sample_size(10);
    group.bench_function("bench_psubscribe_1000_patterns_nomatch", |b| {
        b.iter(|| black_box(registry.publish(b"bench.channel", payload.clone())));
    });
    group.finish();
}

fn bench_psubscribe_1000_patterns_100_match(c: &mut Criterion) {
    let (mut registry, slots) = pattern_registry(1_000, 100);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_psubscribe_1000_patterns_100_match");
    group.sample_size(10);
    group.bench_function("bench_psubscribe_1000_patterns_100_match", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            publish_batches(
                &mut registry,
                b"bench.match.42.channel",
                payload.clone(),
                &slots[..100],
                iters,
            );
            start.elapsed()
        });
    });
    group.finish();
}

fn bench_spublish_vs_publish(c: &mut Criterion) {
    let mut fanout = ShardFanOut::new(0, Arc::new(CrossShardBus::new(1)));
    let router = ShardChannelRouter::standalone();
    let mut exact_slots = Vec::with_capacity(1_000);
    let mut shard_slots = Vec::with_capacity(1_000);
    for conn_id in 0..1_000u64 {
        exact_slots.push(fanout.subscribe(b"bench:fanout", conn_id));
        shard_slots.push(
            fanout
                .subscribe_shard(&router, b"bench:fanout", conn_id + 10_000)
                .unwrap(),
        );
    }
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_spublish_vs_publish");
    group.sample_size(10);
    group.bench_function("publish", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            publish_fanout_batches(
                &mut fanout,
                b"bench:fanout",
                payload.clone(),
                &exact_slots,
                iters,
            );
            start.elapsed()
        });
    });
    group.bench_function("spublish", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            spublish_batches(
                &mut fanout,
                b"bench:fanout",
                payload.clone(),
                &shard_slots,
                iters,
            );
            start.elapsed()
        });
    });
    group.finish();
}

fn bench_subscribe_churn(c: &mut Criterion) {
    let mut registry = ChannelRegistry::default();
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_subscribe_churn");
    group.bench_function("bench_subscribe_churn", |b| {
        let mut next_conn_id = 0u64;
        b.iter(|| {
            let channel = format!("bench:churn:{}", next_conn_id & 255);
            let slot = registry.subscribe(channel.as_bytes(), next_conn_id);
            black_box(registry.publish(channel.as_bytes(), payload.clone()));
            black_box(slot.recv());
            black_box(registry.unsubscribe(channel.as_bytes(), next_conn_id));
            next_conn_id = next_conn_id.wrapping_add(1);
        });
    });
    group.finish();
}

fn bench_message_latency(c: &mut Criterion) {
    let mut registry = ChannelRegistry::default();
    let slot = registry.subscribe(b"bench:latency", 1);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);
    let mut group = c.benchmark_group("pubsub_message_latency");
    group.sample_size(20);
    group.bench_function("bench_message_latency", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(registry.publish(b"bench:latency", payload.clone()));
                black_box(slot.recv());
            }
            start.elapsed()
        });
    });
    group.finish();
}

fn bench_cross_shard_latency(c: &mut Criterion) {
    let mut shards = fanout_shards(8);
    let slot = shards[7].subscribe(b"bench:cross", 7);
    flush_bus(&mut shards);
    let payload = Bytes::copy_from_slice(SMALL_PAYLOAD);

    let mut group = c.benchmark_group("pubsub_cross_shard_latency");
    group.sample_size(10);
    group.bench_function("bench_cross_shard_latency", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(shards[0].publish(b"bench:cross", payload.clone()));
                black_box(shards[7].drain_bus());
                black_box(slot.recv());
            }
            start.elapsed()
        });
    });
    group.finish();
}

fn bench_ring_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_ring_throughput");
    group.sample_size(10);
    group.bench_function("bench_ring_throughput", |b| {
        b.iter_custom(|iters| {
            let slot = Arc::new(BroadcastSlot::new(99));
            let producer = Arc::clone(&slot);
            let consumer = Arc::clone(&slot);
            let cores = core_affinity::get_core_ids().unwrap_or_default();
            let producer_core = cores.first().copied();
            let consumer_core = cores.get(1).copied().or(producer_core);

            let start = Instant::now();
            let producer_thread = thread::spawn(move || {
                if let Some(core) = producer_core {
                    let _ = core_affinity::set_for_current(core);
                }
                for index in 0..iters {
                    loop {
                        let message = Arc::new(senko_pubsub::PubSubMessage {
                            channel: "bench:ring".into(),
                            payload: Bytes::copy_from_slice(&index.to_le_bytes()),
                            kind: senko_pubsub::MessageKind::Message,
                        });
                        if producer.publish(message).is_ok() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            });
            let consumer_thread = thread::spawn(move || {
                if let Some(core) = consumer_core {
                    let _ = core_affinity::set_for_current(core);
                }
                for _ in 0..iters {
                    loop {
                        if consumer.recv().is_some() {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            });
            producer_thread.join().unwrap();
            consumer_thread.join().unwrap();
            start.elapsed()
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_publish_1_subscriber,
    bench_publish_100_subscribers,
    bench_publish_10k_subscribers,
    bench_publish_0_subscribers,
    bench_psubscribe_10_patterns,
    bench_psubscribe_1000_patterns_nomatch,
    bench_psubscribe_1000_patterns_100_match,
    bench_spublish_vs_publish,
    bench_subscribe_churn,
    bench_message_latency,
    bench_cross_shard_latency,
    bench_ring_throughput,
);
criterion_main!(benches);
