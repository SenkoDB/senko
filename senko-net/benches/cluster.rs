use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use compact_str::CompactString;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rand::{SeedableRng, rngs::SmallRng};
use senko_cluster::{
    ClusterState, ClusterTopology, FLAG_LOCAL, NodeId, NodeMeta, RouteOptions, SlotEntry,
    SlotTable, SlotTableSnapshot, crc16_slot, route_with_options,
};
use senko_net::cluster::gossip::GossipState;
use senko_net::cluster::migration::MigrationManager;
use senko_net::cluster::replication::{ReplicaApplyState, ShardReplication};
use senko_store::Store;

fn bench_slot_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_slot_routing");
    group.throughput(Throughput::Elements(10_000_000));
    group.bench_function("bench_slot_routing", |b| {
        b.iter(|| {
            let mut total = 0u64;
            for _ in 0..10_000_000 {
                total = total.wrapping_add(u64::from(crc16_slot(b"{tenant42}:session:abcdef")));
            }
            black_box(total);
        });
    });
    group.finish();
}

fn bench_slot_table_read(c: &mut Criterion) {
    let snapshot = distributed_snapshot(3);
    let table = SlotTable::from_snapshot(&snapshot);
    let mut group = c.benchmark_group("cluster_slot_table_read");
    group.throughput(Throughput::Elements(5_000_000));
    group.bench_function("no_writer", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for slot in 0..5_000_000usize {
                let entry = table.load_entry((slot % 16_384) as u16);
                sum = sum.wrapping_add(u64::from(entry.node_index));
            }
            black_box(sum);
        });
    });
    group.bench_function("periodic_update", |b| {
        let table = SlotTable::from_snapshot(&snapshot);
        let mut flip = false;
        b.iter(|| {
            let mut snapshot = distributed_snapshot(3);
            if flip {
                snapshot.set_entry(
                    0,
                    SlotEntry {
                        node_index: 1,
                        shard_index: 0,
                        flags: 0,
                    },
                );
                table.apply_snapshot(&snapshot);
            }
            flip = !flip;
            let mut sum = 0u64;
            for slot in 0..5_000_000usize {
                sum = sum.wrapping_add(u64::from(
                    table.load_entry((slot % 16_384) as u16).node_index,
                ));
            }
            black_box(sum);
        });
    });
    group.finish();
}

fn bench_routed_set_single_node(c: &mut Criterion) {
    let mut store = Store::new(None);
    let snapshot = local_snapshot();
    let mut group = c.benchmark_group("cluster_routed_set_single_node");
    group.throughput(Throughput::Elements(1_000_000));
    group.bench_function("bench_routed_set_single_node", |b| {
        b.iter(|| {
            for idx in 0..1_000_000 {
                let key = format!("{{tenant}}:single:{idx}");
                let route = route_with_options(
                    &snapshot,
                    key.as_bytes(),
                    true,
                    RouteOptions {
                        current_shard: 0,
                        asking: false,
                    },
                );
                black_box(&route);
                let _ = store.set(
                    CompactString::from(key),
                    senko_core::SenkoValue::Raw(Bytes::from_static(b"value")),
                    Default::default(),
                );
            }
        });
    });
    group.finish();
}

fn bench_routed_set_multi_node(c: &mut Criterion, primaries: usize, name: &str) {
    let snapshot = distributed_snapshot(primaries);
    let mut stores = (0..primaries).map(|_| Store::new(None)).collect::<Vec<_>>();
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(500_000));
    group.bench_function(name, |b| {
        b.iter(|| {
            for idx in 0..500_000 {
                let key = format!("key:{idx}");
                let route = route_with_options(
                    &snapshot,
                    key.as_bytes(),
                    true,
                    RouteOptions {
                        current_shard: 0,
                        asking: false,
                    },
                );
                let shard = match route {
                    senko_cluster::RouteDecision::LocalShard(shard)
                    | senko_cluster::RouteDecision::CrossShard(shard) => shard % stores.len(),
                    senko_cluster::RouteDecision::Moved(_, _) => {
                        crc16_slot(key.as_bytes()) as usize % stores.len()
                    }
                    _ => 0,
                };
                let _ = stores[shard].set(
                    CompactString::from(key),
                    senko_core::SenkoValue::Raw(Bytes::from_static(b"value")),
                    Default::default(),
                );
            }
        });
    });
    group.finish();
}

fn bench_routed_set_3_node(c: &mut Criterion) {
    bench_routed_set_multi_node(c, 3, "cluster_routed_set_3_node");
}

fn bench_routed_set_6_node(c: &mut Criterion) {
    bench_routed_set_multi_node(c, 6, "cluster_routed_set_6_node");
}

fn bench_gossip_convergence(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_gossip_convergence");
    group.bench_function("bench_gossip_convergence", |b| {
        b.iter(|| {
            let mut rng = SmallRng::seed_from_u64(0xA11CE);
            let nodes = (0..20)
                .map(|idx| {
                    let mut meta = node_meta(idx as u8 + 1, 7000 + idx as u16);
                    meta.slots
                        .insert_range((idx as u32 * 100)..(idx as u32 * 100 + 50));
                    GossipState::new(meta)
                })
                .collect::<Vec<_>>();
            let mut states = nodes;
            let changed = {
                let mut meta = node_meta(99, 7099);
                meta.slots.insert_range(0..512);
                meta
            };
            states[0].cluster_mut().insert_node(changed.clone());

            for _ in 0..5 {
                let outbound = states
                    .iter_mut()
                    .flat_map(|state| state.tick(1_000, &mut rng))
                    .collect::<Vec<_>>();
                for env in outbound {
                    for state in &mut states {
                        if state
                            .cluster()
                            .iter()
                            .any(|node| node.cluster_addr == env.addr)
                        {
                            let _ = state.handle_message(
                                env.message.clone(),
                                env.addr,
                                1_000,
                                &mut rng,
                            );
                        }
                    }
                }
            }
            black_box(states);
        });
    });
    group.finish();
}

fn bench_migration_throughput(c: &mut Criterion) {
    let mut source = Store::new(None);
    let mut target = Store::new(None);
    let shard = Arc::new(ShardReplication::new(0, 16 * 1024 * 1024));
    let mut group = c.benchmark_group("cluster_migration_throughput");
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("bench_migration_throughput", |b| {
        b.iter(|| {
            let slot = crc16_slot(b"{bench}:0");
            let mut snapshot = SlotTableSnapshot::default();
            let mut manager = MigrationManager::default();
            let mut source_index = senko_net::cluster::migration::SlotIndex::new();
            let mut target_index = senko_net::cluster::migration::SlotIndex::new();
            for idx in 0..100_000 {
                let key = format!("{{bench}}:{idx}");
                let _ = source.set(
                    CompactString::from(key.clone()),
                    senko_core::SenkoValue::Raw(Bytes::from_static(b"value")),
                    Default::default(),
                );
            }
            source_index.rebuild_from_store(&mut source);
            let _ = manager.set_slot_migrating(
                &mut snapshot,
                &source_index,
                slot,
                NodeId::new([2; 20]),
                1,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001),
                0,
            );
            loop {
                let step = manager
                    .migrate_slot_chunk(
                        slot,
                        &mut source,
                        &mut target,
                        &mut source_index,
                        &mut target_index,
                        &mut snapshot,
                    )
                    .unwrap();
                if step.complete {
                    break;
                }
            }
            black_box(&shard);
        });
    });
    group.finish();
}

fn bench_failover_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_failover_time");
    group.bench_function("bench_failover_time", |b| {
        b.iter(|| {
            let primary = node_meta(1, 7200);
            let mut replica = node_meta(2, 7201);
            replica.role = senko_cluster::NodeRole::Replica {
                primary: primary.id,
            };
            let voter = node_meta(3, 7202);
            let mut state = GossipState::new(replica.clone());
            state.insert_node(primary.clone());
            state.insert_node(voter.clone());
            if let Some(failed) = state.cluster_mut().get_node_mut(&primary.id) {
                failed.state = senko_cluster::NodeState::Failed;
            }
            state.set_primary_progress(replica.id, [1; 16], 100);
            let mut rng = SmallRng::seed_from_u64(0xFACE);
            for step in 0..6 {
                let outbound = state.tick(20_000 + step * 800, &mut rng);
                for env in outbound {
                    black_box(env);
                }
            }
            black_box(state.cluster().get_node(&replica.id).unwrap().role.clone());
        });
    });
    group.finish();
}

fn bench_replication_lag(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_replication_lag");
    group.bench_function("bench_replication_lag", |b| {
        b.iter(|| {
            let shard = ShardReplication::new(0, 4 * 1024 * 1024);
            let mut replica_store = Store::new(None);
            let mut replica =
                ReplicaApplyState::new([1; 16], 1, "127.0.0.1:6379".into(), false).unwrap();
            let mut offset = 0u64;
            for idx in 0..10_000 {
                let payload = set_wire(&format!("repl:{idx}"), "value");
                offset = shard.append_command(&payload).unwrap();
                if idx % 2 == 0 {
                    let frame = shard.next_frame(replica.replication_offset).unwrap();
                    let _ = replica
                        .apply_frame(&frame, std::slice::from_mut(&mut replica_store), idx as u64)
                        .unwrap();
                }
            }
            black_box(offset.saturating_sub(replica.replication_offset));
        });
    });
    group.finish();
}

fn local_snapshot() -> SlotTableSnapshot {
    let mut snapshot = SlotTableSnapshot::default();
    for slot in 0..16_384u16 {
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 0,
                shard_index: 0,
                flags: FLAG_LOCAL,
            },
        );
    }
    snapshot
}

fn distributed_snapshot(primaries: usize) -> SlotTableSnapshot {
    let mut cluster = ClusterState::new(NodeId::new([1; 20]));
    let mut local = node_meta(1, 7000);
    local.slots.insert_range(0..0);
    cluster.insert_node(local.clone());
    for idx in 1..primaries {
        cluster.insert_node(node_meta(idx as u8 + 1, 7000 + idx as u16));
    }
    let topology = ClusterTopology::new(cluster).unwrap();
    let mut snapshot = SlotTableSnapshot::default();
    topology.populate_snapshot_routes(&mut snapshot);
    let mut start = 0usize;
    for idx in 0..primaries {
        let end = start + (16_384 / primaries) + usize::from(idx < (16_384 % primaries));
        for slot in start..end {
            snapshot.set_entry(
                slot as u16,
                SlotEntry {
                    node_index: idx as u16,
                    shard_index: 0,
                    flags: if idx == 0 { FLAG_LOCAL } else { 0 },
                },
            );
        }
        start = end;
    }
    snapshot
}

fn node_meta(id_byte: u8, port: u16) -> NodeMeta {
    NodeMeta {
        id: NodeId::new([id_byte; 20]),
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        cluster_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 10_000),
        role: senko_cluster::NodeRole::Primary,
        state: senko_cluster::NodeState::Connected,
        ping_sent: 0,
        pong_recv: 0,
        config_epoch: 1,
        slots: Default::default(),
    }
}

fn set_wire(key: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*3\r\n${}\r\nSET\r\n", 3).as_bytes());
    out.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
    out.extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
    out
}

criterion_group!(
    benches,
    bench_slot_routing,
    bench_slot_table_read,
    bench_routed_set_single_node,
    bench_routed_set_3_node,
    bench_routed_set_6_node,
    bench_gossip_convergence,
    bench_migration_throughput,
    bench_failover_time,
    bench_replication_lag
);
criterion_main!(benches);
