use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use compact_str::CompactString;
use rand::{rngs::SmallRng, SeedableRng};
use redis::Client;
use senko_cluster::{
    crc16_slot, route, ClusterState, NodeId, NodeRole, NodeState, RouteDecision, SlotEntry,
    SlotTableSnapshot,
};
use senko_core::SenkoValue;
use senko_net::cluster::gossip::GossipState;
use senko_net::cluster::migration::route_migration_command;
use senko_net::commands::cluster::{execute as execute_cluster, ClusterCommandState};
use senko_proto::Frame;
use senko_store::Store;

pub struct senkoNode {
    pub index: usize,
    pub store: Rc<RefCell<Store>>,
    pub cluster: Rc<RefCell<ClusterCommandState>>,
    pub gossip: GossipState,
    pub addr: SocketAddr,
    pub cluster_addr: SocketAddr,
    pub alive: bool,
}

pub struct ClusterHarness {
    pub nodes: Vec<senkoNode>,
    pub n_primaries: usize,
    pub replicas_per_primary: usize,
}

impl ClusterHarness {
    pub fn start(n_primaries: usize, replicas_per_primary: usize) -> Self {
        let total = n_primaries * (replicas_per_primary + 1);
        let mut nodes = Vec::with_capacity(total);
        let base_port = 7300u16;
        for index in 0..total {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base_port + index as u16);
            let cluster_addr =
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 17_300 + index as u16);
            let cluster = Rc::new(RefCell::new(
                ClusterCommandState::enabled(addr, 0, unique_conf_path(index)).unwrap(),
            ));
            let local_meta = cluster.borrow().local_meta().unwrap().clone();
            let gossip = GossipState::new(local_meta);
            nodes.push(senkoNode {
                index,
                store: Rc::new(RefCell::new(Store::new(None))),
                cluster,
                gossip,
                addr,
                cluster_addr,
                alive: true,
            });
        }

        let mut harness = Self {
            nodes,
            n_primaries,
            replicas_per_primary,
        };
        harness.assign_roles_and_slots();
        harness.sync_views();
        harness
    }

    pub fn primary(&mut self, idx: usize) -> &mut senkoNode {
        &mut self.nodes[idx * (self.replicas_per_primary + 1)]
    }

    pub fn kill(&mut self, node_idx: usize) {
        let node_id = self.node_id(node_idx);
        self.nodes[node_idx].alive = false;
        for node in &mut self.nodes {
            if let Some(meta) = node.gossip.cluster_mut().get_node_mut(&node_id) {
                meta.state = NodeState::Failed;
            }
            if let Some(topology) = node.cluster.borrow_mut().topology_mut() {
                if let Some(meta) = topology.state_mut().get_node_mut(&node_id) {
                    meta.state = NodeState::Failed;
                }
            }
        }
    }

    pub fn restart(&mut self, node_idx: usize) {
        let node_id = self.node_id(node_idx);
        self.nodes[node_idx].alive = true;
        for node in &mut self.nodes {
            if let Some(meta) = node.gossip.cluster_mut().get_node_mut(&node_id) {
                meta.state = NodeState::Connected;
            }
            if let Some(topology) = node.cluster.borrow_mut().topology_mut() {
                if let Some(meta) = topology.state_mut().get_node_mut(&node_id) {
                    meta.state = NodeState::Connected;
                }
            }
        }
    }

    pub fn wait_healthy(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            self.sync_views();
            if self.nodes.iter().all(|node| {
                !node.alive
                    || node
                        .cluster
                        .borrow()
                        .topology()
                        .unwrap()
                        .state()
                        .nodes()
                        .values()
                        .filter(|meta| matches!(meta.role, NodeRole::Primary))
                        .all(|meta| matches!(meta.state, NodeState::Connected))
            }) {
                return;
            }
        }
        panic!("cluster did not become healthy within {:?}", timeout);
    }

    pub fn client(&self, node_idx: usize) -> Client {
        Client::open(format!("redis://{}/", self.nodes[node_idx].addr)).unwrap()
    }

    pub fn node_id(&self, node_idx: usize) -> NodeId {
        self.nodes[node_idx]
            .cluster
            .borrow()
            .local_node_id()
            .unwrap()
    }

    pub fn route_from(&self, node_idx: usize, key: &[u8], write: bool) -> RouteDecision {
        let snapshot = self.nodes[node_idx].cluster.borrow();
        route(snapshot.slot_table(), key, write)
    }

    pub fn route_asking(&mut self, node_idx: usize, key: &[u8], write: bool) -> RouteDecision {
        let mut state = self.nodes[node_idx].cluster.borrow_mut();
        let mut asking = std::mem::take(state.asking_mut());
        let decision = route_migration_command(state.slot_table(), key, write, 0, &mut asking);
        *state.asking_mut() = asking;
        decision
    }

    pub fn arm_asking(&mut self, node_idx: usize) {
        self.nodes[node_idx].cluster.borrow_mut().asking_mut().arm();
    }

    pub fn execute_cluster(&self, node_idx: usize, args: &[&str]) -> Result<Vec<u8>, Vec<u8>> {
        let owned = args
            .iter()
            .map(|arg| arg.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let frames = owned
            .iter()
            .map(|arg| Frame::BulkString(arg.as_slice()))
            .collect::<Vec<_>>();
        execute_cluster(
            b"CLUSTER",
            &frames,
            true,
            &self.nodes[node_idx].cluster,
            &self.nodes[node_idx].store,
            None,
        )
        .expect("CLUSTER command should be handled")
        .map(|outcome| outcome.response)
    }

    pub fn set(&mut self, node_idx: usize, key: &str, value: &str) {
        let _ = self.nodes[node_idx].store.borrow_mut().set(
            CompactString::from(key),
            SenkoValue::Raw(Bytes::copy_from_slice(value.as_bytes())),
            Default::default(),
        );
    }

    pub fn get(&mut self, node_idx: usize, key: &str) -> Option<Vec<u8>> {
        self.nodes[node_idx]
            .store
            .borrow_mut()
            .get(key.as_bytes())
            .map(|value| value.as_bytes().to_vec())
    }

    pub fn sync_views(&mut self) {
        let metas = self
            .nodes
            .iter()
            .map(|node| node.cluster.borrow().local_meta().unwrap().clone())
            .collect::<Vec<_>>();

        self.sync_command_views_from_metas(&metas);

        for node in &mut self.nodes {
            let local_id = node.gossip.local_node_id();
            let mut state = ClusterState::new(local_id);
            for meta in &metas {
                state.insert_node(meta.clone());
            }
            node.gossip = GossipState::new(
                state
                    .get_node(&local_id)
                    .expect("local meta missing")
                    .clone(),
            );
            for meta in &metas {
                if meta.id != local_id {
                    node.gossip.insert_node(meta.clone());
                }
            }
        }
    }

    pub fn run_failover_round(&mut self, now_ms: u64) {
        let mut rng = SmallRng::seed_from_u64(0xACED_1234);
        let outbound = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter(|(_, node)| node.alive)
            .flat_map(|(index, node)| {
                let source_addr = node.cluster_addr;
                node.gossip
                    .tick(now_ms, &mut rng)
                    .into_iter()
                    .map(move |env| (index, source_addr, env))
            })
            .collect::<Vec<_>>();

        for (_source_index, source_addr, envelope) in outbound {
            for node in &mut self.nodes {
                if node.alive && node.cluster_addr == envelope.addr {
                    let _ = node.gossip.handle_message(
                        envelope.message.clone(),
                        source_addr,
                        now_ms,
                        &mut rng,
                    );
                }
            }
        }

        let local_metas = self
            .nodes
            .iter()
            .map(|node| {
                node.gossip
                    .cluster()
                    .get_node(&node.gossip.local_node_id())
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        for (index, meta) in local_metas.into_iter().enumerate() {
            let mut cluster = self.nodes[index].cluster.borrow_mut();
            if let Some(local) = cluster.local_meta_mut() {
                *local = meta;
            }
        }
        let metas = self
            .nodes
            .iter()
            .map(|node| node.cluster.borrow().local_meta().unwrap().clone())
            .collect::<Vec<_>>();
        self.sync_command_views_from_metas(&metas);
    }

    pub fn rebalance_add_node(&mut self, new_primary_index: usize, slots_to_take: &[u16]) {
        for slot in slots_to_take {
            let owner = self
                .nodes
                .iter()
                .position(|node| {
                    node.cluster
                        .borrow()
                        .local_meta()
                        .unwrap()
                        .slots
                        .contains(*slot as u32)
                })
                .unwrap();
            if owner == new_primary_index {
                continue;
            }
            {
                let mut owner_state = self.nodes[owner].cluster.borrow_mut();
                owner_state
                    .local_meta_mut()
                    .unwrap()
                    .slots
                    .remove(*slot as u32);
            }
            {
                let mut target_state = self.nodes[new_primary_index].cluster.borrow_mut();
                target_state
                    .local_meta_mut()
                    .unwrap()
                    .slots
                    .insert(*slot as u32);
            }
        }
        self.sync_views();
    }

    fn sync_command_views_from_metas(&mut self, metas: &[senko_cluster::NodeMeta]) {
        for node in &mut self.nodes {
            let local_id = node.cluster.borrow().local_node_id().unwrap();
            let mut cluster_state = node.cluster.borrow_mut();
            let topology = cluster_state.topology_mut().unwrap();
            topology.state_mut().nodes_mut().clear();
            for meta in metas {
                topology.state_mut().insert_node(meta.clone());
            }
            topology.rebuild_indexes().unwrap();
            let mut snapshot = SlotTableSnapshot::default();
            topology.populate_snapshot_routes(&mut snapshot);
            for meta in metas {
                if let Some(node_index) = topology.node_index(&meta.id) {
                    for slot in meta.slots.iter() {
                        snapshot.set_entry(
                            slot as u16,
                            SlotEntry {
                                node_index,
                                shard_index: 0,
                                flags: if meta.id == local_id {
                                    senko_cluster::FLAG_LOCAL
                                } else {
                                    0
                                },
                            },
                        );
                    }
                }
            }
            *cluster_state.slot_table_mut() = snapshot;
        }
    }

    fn assign_roles_and_slots(&mut self) {
        let primary_slots = even_slot_ranges(self.n_primaries);
        for primary_idx in 0..self.n_primaries {
            let node_idx = primary_idx * (self.replicas_per_primary + 1);
            {
                let mut state = self.nodes[node_idx].cluster.borrow_mut();
                let local = state.local_meta_mut().unwrap();
                local.role = NodeRole::Primary;
                local.state = NodeState::Connected;
                for slot in primary_slots[primary_idx].clone() {
                    local.slots.insert(slot);
                }
            }

            let primary_id = self.node_id(node_idx);
            for replica in 0..self.replicas_per_primary {
                let replica_idx = node_idx + replica + 1;
                let mut state = self.nodes[replica_idx].cluster.borrow_mut();
                let local = state.local_meta_mut().unwrap();
                local.role = NodeRole::Replica {
                    primary: primary_id,
                };
                local.state = NodeState::Connected;
                local.slots.clear();
            }
        }
    }
}

fn unique_conf_path(index: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "senko-cluster-test-{}-{}-{}.conf",
        std::process::id(),
        senko_store::store::current_unix_ms(),
        index
    ))
}

fn even_slot_ranges(parts: usize) -> Vec<std::ops::Range<u32>> {
    let base = 16_384 / parts;
    let rem = 16_384 % parts;
    let mut out = Vec::with_capacity(parts);
    let mut start = 0u32;
    for index in 0..parts {
        let len = base + usize::from(index < rem);
        let end = start + len as u32;
        out.push(start..end);
        start = end;
    }
    out
}

pub fn same_slot(keys: &[&[u8]]) -> bool {
    let Some(first) = keys.first() else {
        return true;
    };
    let slot = crc16_slot(first);
    keys.iter().all(|key| crc16_slot(key) == slot)
}
