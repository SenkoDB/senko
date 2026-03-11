use std::cell::RefCell;
use std::fmt::Write as _;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::HashMap;
use roaring::RoaringBitmap;
use senko_cluster::{
    ClusterState, ClusterTopology, FLAG_LOCAL, NodeId, NodeMeta, NodeRole, NodeState, SlotEntry,
    SlotTableSnapshot, TopologyError,
};
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::{Response, Store};
use smallvec::{SmallVec, smallvec};

use crate::cluster::migration::{AskingState, MigrationManager, SlotIndex};
use crate::connection::{
    bulk_string, current_unix_ms, error_bytes, error_message, frame_bytes, serialize_response,
    simple_string,
};

const CLUSTER_DISABLED_ERROR: &str = "ERR This instance has cluster support disabled";
const DEFAULT_FORGET_TTL_MS: u64 = 60_000;

#[derive(Debug)]
pub struct ClusterCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterMessageStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub ping_sent: u64,
    pub pong_sent: u64,
    pub meet_sent: u64,
    pub fail_received: u64,
    pub total_cluster_links_buffer_limit_exceeded: u64,
    pub cluster_link_sendbuf_limit_exceeded: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterLinkInfo {
    pub direction: CompactString,
    pub id: u64,
    pub addr: SocketAddr,
    pub create_time_ms: u64,
    pub events: CompactString,
    pub send_buffer_allocated: usize,
    pub send_buffer_used: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeetSeed {
    pub addr: SocketAddr,
    pub cluster_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailoverMode {
    Graceful,
    Force,
    Takeover,
}

#[derive(Debug)]
pub struct ClusterCommandState {
    enabled: bool,
    shard_index: usize,
    local_addr: SocketAddr,
    cluster_addr: SocketAddr,
    save_config_path: PathBuf,
    topology: Option<ClusterTopology>,
    slot_table: SlotTableSnapshot,
    slot_index: SlotIndex,
    migration: MigrationManager,
    asking: AskingState,
    forgotten_nodes: HashMap<NodeId, u64, RandomState>,
    replication_offsets: HashMap<NodeId, u64, RandomState>,
    links: Vec<ClusterLinkInfo>,
    meet_queue: Vec<MeetSeed>,
    pending_replication_target: Option<NodeId>,
    message_stats: ClusterMessageStats,
    replica_no_failover: bool,
}

impl ClusterCommandState {
    pub fn disabled(local_addr: SocketAddr, shard_index: usize) -> Self {
        let cluster_addr =
            SocketAddr::new(local_addr.ip(), local_addr.port().saturating_add(10_000));
        Self {
            enabled: false,
            shard_index,
            local_addr,
            cluster_addr,
            save_config_path: PathBuf::from("cluster.conf"),
            topology: None,
            slot_table: SlotTableSnapshot::default(),
            slot_index: SlotIndex::new(),
            migration: MigrationManager::default(),
            asking: AskingState::default(),
            forgotten_nodes: HashMap::with_hasher(RandomState::new()),
            replication_offsets: HashMap::with_hasher(RandomState::new()),
            links: Vec::new(),
            meet_queue: Vec::new(),
            pending_replication_target: None,
            message_stats: ClusterMessageStats::default(),
            replica_no_failover: false,
        }
    }

    pub fn enabled(
        local_addr: SocketAddr,
        shard_index: usize,
        save_config_path: PathBuf,
    ) -> Result<Self, TopologyError> {
        let cluster_addr =
            SocketAddr::new(local_addr.ip(), local_addr.port().saturating_add(10_000));
        let local = NodeMeta::new(NodeId::generate(), local_addr, cluster_addr);
        let state = ClusterState::with_local_node(local);
        let topology = ClusterTopology::new(state)?;
        let mut slot_table = SlotTableSnapshot::default();
        topology.populate_snapshot_routes(&mut slot_table);
        Ok(Self {
            enabled: true,
            shard_index,
            local_addr,
            cluster_addr,
            save_config_path,
            topology: Some(topology),
            slot_table,
            slot_index: SlotIndex::new(),
            migration: MigrationManager::default(),
            asking: AskingState::default(),
            forgotten_nodes: HashMap::with_hasher(RandomState::new()),
            replication_offsets: HashMap::with_hasher(RandomState::new()),
            links: Vec::new(),
            meet_queue: Vec::new(),
            pending_replication_target: None,
            message_stats: ClusterMessageStats::default(),
            replica_no_failover: false,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn local_node_id(&self) -> Option<NodeId> {
        self.topology
            .as_ref()
            .map(|topology| topology.state().local_node_id())
    }

    pub fn slot_table(&self) -> &SlotTableSnapshot {
        &self.slot_table
    }

    pub fn slot_table_mut(&mut self) -> &mut SlotTableSnapshot {
        &mut self.slot_table
    }

    pub fn migration(&self) -> &MigrationManager {
        &self.migration
    }

    pub fn migration_mut(&mut self) -> &mut MigrationManager {
        &mut self.migration
    }

    pub fn slot_index(&self) -> &SlotIndex {
        &self.slot_index
    }

    pub fn slot_index_mut(&mut self) -> &mut SlotIndex {
        &mut self.slot_index
    }

    pub fn message_stats_mut(&mut self) -> &mut ClusterMessageStats {
        &mut self.message_stats
    }

    pub fn links_mut(&mut self) -> &mut Vec<ClusterLinkInfo> {
        &mut self.links
    }

    pub fn replication_offsets_mut(&mut self) -> &mut HashMap<NodeId, u64, RandomState> {
        &mut self.replication_offsets
    }

    pub fn topology(&self) -> Option<&ClusterTopology> {
        self.topology.as_ref()
    }

    pub fn topology_mut(&mut self) -> Option<&mut ClusterTopology> {
        self.topology.as_mut()
    }

    fn ensure_enabled(&self) -> Result<(), Vec<u8>> {
        if self.enabled {
            Ok(())
        } else {
            Err(error_message(CLUSTER_DISABLED_ERROR))
        }
    }

    fn topology_ref(&self) -> Result<&ClusterTopology, Vec<u8>> {
        self.topology
            .as_ref()
            .ok_or_else(|| error_message(CLUSTER_DISABLED_ERROR))
    }

    fn topology_mut_ref(&mut self) -> Result<&mut ClusterTopology, Vec<u8>> {
        self.topology
            .as_mut()
            .ok_or_else(|| error_message(CLUSTER_DISABLED_ERROR))
    }

    pub fn local_meta(&self) -> Option<&NodeMeta> {
        let topology = self.topology.as_ref()?;
        topology.state().get_node(&topology.state().local_node_id())
    }

    pub fn local_meta_mut(&mut self) -> Option<&mut NodeMeta> {
        let local_id = self.local_node_id()?;
        self.topology.as_mut()?.state_mut().get_node_mut(&local_id)
    }

    pub fn asking_mut(&mut self) -> &mut AskingState {
        &mut self.asking
    }

    fn local_node_index(&self) -> Result<u16, Vec<u8>> {
        self.topology_ref()?
            .local_node_index()
            .ok_or_else(|| error_message("ERR cluster topology missing local node"))
    }

    fn rebuild_topology(&mut self) -> Result<(), Vec<u8>> {
        let Some(topology) = self.topology.as_mut() else {
            return Ok(());
        };
        topology
            .rebuild_indexes()
            .map_err(|_| error_message("ERR invalid cluster topology"))?;
        topology.populate_snapshot_routes(&mut self.slot_table);
        Ok(())
    }

    fn local_shard_id(&self) -> Result<NodeId, Vec<u8>> {
        let local = self
            .local_meta()
            .ok_or_else(|| error_message("ERR local cluster node missing"))?;
        Ok(match local.role {
            NodeRole::Primary => local.id,
            NodeRole::Replica { primary } => primary,
        })
    }

    fn increment_epoch(&mut self) -> Result<u64, Vec<u8>> {
        let topology = self.topology_mut_ref()?;
        let next_epoch = topology.state().current_epoch().saturating_add(1);
        topology.state_mut().set_current_epoch(next_epoch);
        let local_id = topology.state().local_node_id();
        if let Some(local) = topology.state_mut().get_node_mut(&local_id) {
            local.config_epoch = next_epoch;
        }
        Ok(next_epoch)
    }

    fn set_local_slot_owned(&mut self, slot: u16, owned: bool) -> Result<(), Vec<u8>> {
        let local_index = self.local_node_index()?;
        if let Some(local) = self.local_meta_mut() {
            if owned {
                local.slots.insert(slot as u32);
                self.slot_table.set_entry(
                    slot,
                    SlotEntry {
                        node_index: local_index,
                        shard_index: self.shard_index as u16,
                        flags: FLAG_LOCAL,
                    },
                );
            } else {
                local.slots.remove(slot as u32);
                self.slot_table.set_entry(slot, SlotEntry::default());
                self.slot_table.clear_migrating_slot(slot);
            }
        }
        Ok(())
    }

    fn claim_slots_from(&mut self, from: NodeId) -> Result<(), Vec<u8>> {
        let local_index = self.local_node_index()?;
        let local_id = self.local_node_id().unwrap_or(NodeId::ZERO);
        let shard_index = self.shard_index as u16;
        let slots = {
            let topology = self.topology_ref()?;
            topology
                .state()
                .get_node(&from)
                .map(|node| {
                    node.slots
                        .iter()
                        .map(|slot| slot as u16)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        if let Some(source) = self.topology_mut_ref()?.state_mut().get_node_mut(&from) {
            source.slots.clear();
            source.role = NodeRole::Replica { primary: local_id };
        }
        if let Some(local) = self.local_meta_mut() {
            for slot in &slots {
                local.slots.insert(*slot as u32);
            }
        }
        for slot in &slots {
            self.slot_table.set_entry(
                *slot,
                SlotEntry {
                    node_index: local_index,
                    shard_index,
                    flags: FLAG_LOCAL,
                },
            );
        }
        Ok(())
    }

    fn clear_local_slots(&mut self) -> Result<(), Vec<u8>> {
        let slots = self
            .local_meta()
            .map(|node| {
                node.slots
                    .iter()
                    .map(|slot| slot as u16)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for slot in slots {
            self.set_local_slot_owned(slot, false)?;
        }
        Ok(())
    }

    fn all_slot_owners(&self) -> Vec<Option<(NodeId, NodeState)>> {
        let mut out = vec![None; 16_384];
        let Some(topology) = self.topology.as_ref() else {
            return out;
        };
        for node in topology.state().nodes().values() {
            if matches!(node.role, NodeRole::Replica { .. }) {
                continue;
            }
            for slot in node.slots.iter() {
                out[slot as usize] = Some((node.id, node.state));
            }
        }
        out
    }
}

pub fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    meta_resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
    max_memory: Option<usize>,
) -> Option<Result<ClusterCommandOutcome, Vec<u8>>> {
    if !eq_ascii(command, b"CLUSTER") {
        return None;
    }
    Some(dispatch_cluster(
        args, meta_resp3, cluster, store, max_memory,
    ))
}

fn dispatch_cluster(
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
    max_memory: Option<usize>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;

    if eq_ascii(subcommand, b"INFO") {
        return cluster_info(rest, resp3, cluster);
    }
    if eq_ascii(subcommand, b"NODES") {
        return cluster_nodes(rest, resp3, cluster, None);
    }
    if eq_ascii(subcommand, b"SHARDS") {
        return cluster_shards(rest, resp3, cluster);
    }
    if eq_ascii(subcommand, b"MYID") {
        return cluster_myid(rest, cluster);
    }
    if eq_ascii(subcommand, b"MYSHARDID") {
        return cluster_myshardid(rest, cluster);
    }
    if eq_ascii(subcommand, b"MEET") {
        return cluster_meet(rest, cluster);
    }
    if eq_ascii(subcommand, b"FORGET") {
        return cluster_forget(rest, cluster);
    }
    if eq_ascii(subcommand, b"REPLICATE") {
        return cluster_replicate(rest, cluster);
    }
    if eq_ascii(subcommand, b"FAILOVER") {
        return cluster_failover(rest, cluster);
    }
    if eq_ascii(subcommand, b"RESET") {
        return cluster_reset(rest, cluster, store, max_memory);
    }
    if eq_ascii(subcommand, b"SAVECONFIG") {
        return cluster_saveconfig(rest, cluster);
    }
    if eq_ascii(subcommand, b"SETSLOT") {
        return cluster_setslot(rest, cluster, store);
    }
    if eq_ascii(subcommand, b"GETKEYSINSLOT") {
        return cluster_getkeysinslot(rest, resp3, cluster, store);
    }
    if eq_ascii(subcommand, b"COUNTKEYSINSLOT") {
        return cluster_countkeysinslot(rest, resp3, cluster, store);
    }
    if eq_ascii(subcommand, b"LINKS") {
        return cluster_links(rest, resp3, cluster);
    }
    if eq_ascii(subcommand, b"DELSLOTSRANGE") {
        return cluster_slots_range(rest, cluster, false);
    }
    if eq_ascii(subcommand, b"ADDSLOTSRANGE") {
        return cluster_slots_range(rest, cluster, true);
    }
    if eq_ascii(subcommand, b"DELSLOTS") {
        return cluster_slots(rest, cluster, false);
    }
    if eq_ascii(subcommand, b"ADDSLOTS") {
        return cluster_slots(rest, cluster, true);
    }
    if eq_ascii(subcommand, b"FLUSHSLOTS") {
        return cluster_flushslots(rest, cluster, store);
    }
    if eq_ascii(subcommand, b"REPLICAS") {
        return cluster_replicas(rest, resp3, cluster);
    }

    Err(error_message(&format!(
        "ERR Unknown CLUSTER subcommand or wrong number of arguments for '{}'",
        String::from_utf8_lossy(subcommand)
    )))
}

fn cluster_info(
    args: &[Frame<'_>],
    _resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|info' command",
        ));
    }

    let state = cluster.borrow();
    let info = if !state.is_enabled() {
        format_cluster_info_disabled()
    } else {
        format_cluster_info(&state)
    };
    Ok(ok_outcome(bulk_string(info.as_bytes())))
}

fn cluster_nodes(
    args: &[Frame<'_>],
    _resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    filter_primary: Option<NodeId>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|nodes' command",
        ));
    }

    let state = cluster.borrow();
    state.ensure_enabled()?;
    let text = format_cluster_nodes(&state, filter_primary);
    Ok(ok_outcome(bulk_string(text.as_bytes())))
}

fn cluster_shards(
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|shards' command",
        ));
    }
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let response = build_shards_response(&state);
    Ok(ok_outcome(serialize_response(&response, resp3)))
}

fn cluster_myid(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|myid' command",
        ));
    }
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let myid = state
        .local_node_id()
        .ok_or_else(|| error_message("ERR local cluster node missing"))?;
    Ok(ok_outcome(bulk_string(myid.to_string().as_bytes())))
}

fn cluster_myshardid(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|myshardid' command",
        ));
    }
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let shard_id = state.local_shard_id()?;
    Ok(ok_outcome(bulk_string(shard_id.to_string().as_bytes())))
}

fn cluster_meet(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.len() != 2 && args.len() != 3 {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|meet' command",
        ));
    }

    let ip = parse_ip(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)
        .map_err(|_| error_message("ERR Invalid node address specified"))?;
    let port = parse_u16(frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?)
        .map_err(|_| error_message("ERR Invalid node address specified"))?;
    let cluster_port = if args.len() == 3 {
        parse_u16(frame_bytes(&args[2]).map_err(|error| error_bytes(&error))?)
            .map_err(|_| error_message("ERR Invalid node address specified"))?
    } else {
        port.saturating_add(10_000)
    };

    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    let addr = SocketAddr::new(ip, port);
    let cluster_addr = SocketAddr::new(ip, cluster_port);
    let mut meta = NodeMeta::new(NodeId::generate(), addr, cluster_addr);
    meta.state = NodeState::Handshaking;
    state.topology_mut_ref()?.state_mut().insert_node(meta);
    state.rebuild_topology()?;
    state.meet_queue.push(MeetSeed { addr, cluster_addr });
    state.message_stats.messages_sent = state.message_stats.messages_sent.saturating_add(1);
    state.message_stats.meet_sent = state.message_stats.meet_sent.saturating_add(1);
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_forget(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let [node_id] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|forget' command",
        ));
    };
    let node_id = parse_node_id(frame_bytes(node_id).map_err(|error| error_bytes(&error))?)?;
    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    if Some(node_id) == state.local_node_id() {
        return Err(error_message("ERR I tried hard but I can't forget myself"));
    }
    let now = current_unix_ms();
    state
        .forgotten_nodes
        .insert(node_id, now.saturating_add(DEFAULT_FORGET_TTL_MS));
    let _ = state.topology_mut_ref()?.state_mut().remove_node(&node_id);
    state.rebuild_topology()?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_replicate(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let [primary_id] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|replicate' command",
        ));
    };
    let primary_id = parse_node_id(frame_bytes(primary_id).map_err(|error| error_bytes(&error))?)?;
    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    let primary = state
        .topology_ref()?
        .state()
        .get_node(&primary_id)
        .cloned()
        .ok_or_else(|| error_message("ERR Unknown node"))?;
    if !matches!(primary.role, NodeRole::Primary) {
        return Err(error_message("ERR The requested node is not a primary"));
    }
    if state
        .local_meta()
        .is_some_and(|local| !local.slots.is_empty())
    {
        return Err(error_message(
            "ERR To set a node as replica it must have no assigned slots",
        ));
    }
    if let Some(local) = state.local_meta_mut() {
        local.role = NodeRole::Replica {
            primary: primary_id,
        };
    }
    state.pending_replication_target = Some(primary_id);
    let _ = state.increment_epoch()?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_failover(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let mode = match args {
        [] => FailoverMode::Graceful,
        [flag] => {
            let flag = frame_bytes(flag).map_err(|error| error_bytes(&error))?;
            if eq_ascii(flag, b"FORCE") {
                FailoverMode::Force
            } else if eq_ascii(flag, b"TAKEOVER") {
                FailoverMode::Takeover
            } else {
                return Err(error_message("ERR syntax error"));
            }
        }
        _ => return Err(error_message("ERR syntax error")),
    };

    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    let local = state
        .local_meta()
        .cloned()
        .ok_or_else(|| error_message("ERR local cluster node missing"))?;
    let NodeRole::Replica { primary } = local.role else {
        return Err(error_message(
            "ERR You should send CLUSTER FAILOVER to a replica",
        ));
    };
    if mode == FailoverMode::Graceful {
        let local_offset = state
            .replication_offsets
            .get(&local.id)
            .copied()
            .unwrap_or(0);
        let primary_offset = state
            .replication_offsets
            .get(&primary)
            .copied()
            .unwrap_or(0);
        if local_offset < primary_offset {
            return Err(error_message("ERR Replica is not caught up to primary"));
        }
    }
    let epoch = state.increment_epoch()?;
    if let Some(local_mut) = state.local_meta_mut() {
        local_mut.role = NodeRole::Primary;
        local_mut.config_epoch = epoch;
    }
    state.claim_slots_from(primary)?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_reset(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
    max_memory: Option<usize>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let hard = match args {
        [] => false,
        [flag] => {
            let flag = frame_bytes(flag).map_err(|error| error_bytes(&error))?;
            if eq_ascii(flag, b"SOFT") {
                false
            } else if eq_ascii(flag, b"HARD") {
                true
            } else {
                return Err(error_message("ERR syntax error"));
            }
        }
        _ => return Err(error_message("ERR syntax error")),
    };

    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    let local_addr = state.local_addr;
    let cluster_addr = state.cluster_addr;
    let save_config_path = state.save_config_path.clone();
    let local_id = if hard {
        NodeId::generate()
    } else {
        state.local_node_id().unwrap_or_else(NodeId::generate)
    };
    let mut local = NodeMeta::new(local_id, local_addr, cluster_addr);
    local.config_epoch = 0;
    let cluster_state = ClusterState::with_local_node(local);
    let topology = ClusterTopology::new(cluster_state)
        .map_err(|_| error_message("ERR invalid cluster topology"))?;
    state.topology = Some(topology);
    state.slot_table = SlotTableSnapshot::default();
    if let Some(topology) = state.topology.as_ref() {
        let mut slot_table = SlotTableSnapshot::default();
        topology.populate_snapshot_routes(&mut slot_table);
        state.slot_table = slot_table;
    }
    state.slot_index = SlotIndex::new();
    state.migration = MigrationManager::default();
    state.asking = AskingState::default();
    state.forgotten_nodes.clear();
    state.replication_offsets.clear();
    state.links.clear();
    state.meet_queue.clear();
    state.pending_replication_target = None;
    state.message_stats = ClusterMessageStats::default();
    state.save_config_path = save_config_path;
    if hard {
        *store.borrow_mut() = Store::new(max_memory);
    }
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_saveconfig(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|saveconfig' command",
        ));
    }
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let contents = render_cluster_conf(&state);
    fs::write(&state.save_config_path, contents)
        .map_err(|_| error_message("ERR Unable to save cluster configuration"))?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_setslot(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.len() != 3 {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|setslot' command",
        ));
    }

    let slot = parse_slot(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)?;
    let mode = frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?;
    let node_id = parse_node_id(frame_bytes(&args[2]).map_err(|error| error_bytes(&error))?)?;

    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    state.slot_index.rebuild_from_store(&mut store.borrow_mut());
    if eq_ascii(mode, b"MIGRATING") {
        let target = state
            .topology_ref()?
            .state()
            .get_node(&node_id)
            .cloned()
            .ok_or_else(|| error_message("ERR Unknown node"))?;
        let target_index = state
            .topology_ref()?
            .node_index(&node_id)
            .ok_or_else(|| error_message("ERR Unknown node"))?;
        let shard_index = state.shard_index;
        let slot_index = std::mem::take(&mut state.slot_index);
        let mut migration = std::mem::take(&mut state.migration);
        let result = migration.set_slot_migrating(
            &mut state.slot_table,
            &slot_index,
            slot,
            target.id,
            target_index,
            target.addr,
            shard_index,
        );
        state.slot_index = slot_index;
        state.migration = migration;
        result.map_err(|error| error_message(&format!("ERR {error}")))?;
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    if eq_ascii(mode, b"IMPORTING") {
        let source_index = state
            .topology_ref()?
            .node_index(&node_id)
            .ok_or_else(|| error_message("ERR Unknown node"))?;
        let shard_index = state.shard_index;
        let mut migration = std::mem::take(&mut state.migration);
        migration.set_slot_importing(
            &mut state.slot_table,
            slot,
            node_id,
            source_index,
            shard_index,
        );
        state.migration = migration;
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    if eq_ascii(mode, b"NODE") {
        let local_owner = Some(node_id) == state.local_node_id();
        let owner_index = state
            .topology_ref()?
            .node_index(&node_id)
            .unwrap_or_default();
        let shard_index = state.shard_index;
        let mut migration = std::mem::take(&mut state.migration);
        migration.finalize_slot(
            &mut state.slot_table,
            slot,
            owner_index,
            shard_index,
            local_owner,
        );
        state.migration = migration;
        if local_owner {
            state.set_local_slot_owned(slot, true)?;
        } else if state
            .local_meta()
            .is_some_and(|local| local.slots.contains(slot as u32))
        {
            state.set_local_slot_owned(slot, false)?;
        }
        let _ = state.increment_epoch()?;
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    Err(error_message("ERR syntax error"))
}

fn cluster_getkeysinslot(
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.len() != 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
        ));
    }
    let slot = parse_slot(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)?;
    let count = parse_usize(frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?)?;

    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    state.slot_index.rebuild_from_store(&mut store.borrow_mut());
    let mut items = SmallVec::<[Response; 16]>::new();
    for key in state.slot_index.get_keys_in_slot(slot, count) {
        items.push(Response::Value(Some(SenkoValue::Raw(
            Bytes::copy_from_slice(key.as_bytes()),
        ))));
    }
    Ok(ok_outcome(serialize_response(
        &Response::Array(Box::new(items)),
        resp3,
    )))
}

fn cluster_countkeysinslot(
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.len() != 1 {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
        ));
    }
    let slot = parse_slot(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?)?;
    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    state.slot_index.rebuild_from_store(&mut store.borrow_mut());
    let response = Response::Integer(state.slot_index.count_keys_in_slot(slot) as i64);
    Ok(ok_outcome(serialize_response(&response, resp3)))
}

fn cluster_links(
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|links' command",
        ));
    }
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let mut entries = SmallVec::<[Response; 16]>::new();
    for link in &state.links {
        entries.push(Response::Map(Box::new(smallvec![
            bulk_value(b"direction"),
            bulk_value(link.direction.as_bytes()),
            bulk_value(b"id"),
            Response::Integer(link.id as i64),
            bulk_value(b"addr"),
            bulk_value(link.addr.to_string().as_bytes()),
            bulk_value(b"create-time"),
            Response::Integer(link.create_time_ms as i64),
            bulk_value(b"events"),
            bulk_value(link.events.as_bytes()),
            bulk_value(b"send-buffer-allocated"),
            Response::Integer(link.send_buffer_allocated as i64),
            bulk_value(b"send-buffer-used"),
            Response::Integer(link.send_buffer_used as i64),
        ])));
    }
    Ok(ok_outcome(serialize_response(
        &Response::Array(Box::new(entries)),
        resp3,
    )))
}

fn cluster_slots_range(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
    add: bool,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(error_message("ERR syntax error"));
    }

    let mut slots = Vec::new();
    let mut index = 0usize;
    while index < args.len() {
        let start = parse_slot(frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?)?;
        let end = parse_slot(frame_bytes(&args[index + 1]).map_err(|error| error_bytes(&error))?)?;
        if start > end {
            return Err(error_message("ERR Slot range is invalid"));
        }
        for slot in start..=end {
            slots.push(slot);
        }
        index += 2;
    }
    update_local_slots(cluster, &slots, add)?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_slots(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
    add: bool,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message("ERR syntax error"));
    }
    let slots = args
        .iter()
        .map(|frame| {
            parse_slot(frame_bytes(frame).map_err(|error| error_bytes(&error))?)
                .map_err(error_message_text_vec)
        })
        .collect::<Result<Vec<_>, _>>()?;
    update_local_slots(cluster, &slots, add)?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_flushslots(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
    store: &Rc<RefCell<Store>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|flushslots' command",
        ));
    }
    if store.borrow().entry_count() != 0 {
        return Err(error_message(
            "ERR DB must be empty to perform CLUSTER FLUSHSLOTS",
        ));
    }
    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    state.clear_local_slots()?;
    let _ = state.increment_epoch()?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn cluster_replicas(
    args: &[Frame<'_>],
    _resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ClusterCommandOutcome, Vec<u8>> {
    let [node_id] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'cluster|replicas' command",
        ));
    };
    let node_id = parse_node_id(frame_bytes(node_id).map_err(|error| error_bytes(&error))?)?;
    let state = cluster.borrow();
    state.ensure_enabled()?;
    let text = format_cluster_nodes(&state, Some(node_id));
    Ok(ok_outcome(bulk_string(text.as_bytes())))
}

fn update_local_slots(
    cluster: &Rc<RefCell<ClusterCommandState>>,
    slots: &[u16],
    add: bool,
) -> Result<(), Vec<u8>> {
    let mut state = cluster.borrow_mut();
    state.ensure_enabled()?;
    let local_id = state.local_node_id().unwrap_or(NodeId::ZERO);
    for slot in slots {
        let owner = state
            .topology_ref()?
            .state()
            .nodes()
            .values()
            .find(|node| node.slots.contains(*slot as u32))
            .map(|node| node.id);
        if add && owner.is_some() && owner != Some(local_id) {
            return Err(error_message(
                "ERR Slot is already busy and assigned to another node",
            ));
        }
    }
    for slot in slots {
        state.set_local_slot_owned(*slot, add)?;
    }
    let _ = state.increment_epoch()?;
    Ok(())
}

fn format_cluster_info_disabled() -> String {
    [
        "cluster_enabled:0",
        "cluster_state:fail",
        "cluster_slots_assigned:0",
        "cluster_slots_ok:0",
        "cluster_slots_pfail:0",
        "cluster_slots_fail:0",
        "cluster_known_nodes:0",
        "cluster_size:0",
        "cluster_current_epoch:0",
        "cluster_my_epoch:0",
        "cluster_stats_messages_sent:0",
        "cluster_stats_messages_received:0",
        "cluster_stats_messages_ping_sent:0",
        "cluster_stats_messages_pong_sent:0",
        "cluster_stats_messages_meet_sent:0",
        "cluster_stats_messages_fail_received:0",
        "total_cluster_links_buffer_limit_exceeded:0",
        "cluster_link_sendbuf_limit_exceeded:0",
    ]
    .join("\r\n")
        + "\r\n"
}

fn format_cluster_info(state: &ClusterCommandState) -> String {
    let owners = state.all_slot_owners();
    let mut assigned = 0u64;
    let mut ok = 0u64;
    let mut pfail = 0u64;
    let mut fail = 0u64;
    let mut cluster_state = "ok";
    for owner in owners {
        match owner {
            Some((_node, NodeState::Connected)) => {
                assigned += 1;
                ok += 1;
            }
            Some((_node, NodeState::PFail)) => {
                assigned += 1;
                pfail += 1;
                cluster_state = "fail";
            }
            Some((_node, NodeState::Failed)) => {
                assigned += 1;
                fail += 1;
                cluster_state = "fail";
            }
            Some((_node, _)) => {
                assigned += 1;
                cluster_state = "fail";
            }
            None => {
                cluster_state = "fail";
            }
        }
    }
    let topology = state
        .topology_ref()
        .expect("enabled state must have topology");
    let local = state
        .local_meta()
        .expect("enabled state must have local node");
    let cluster_size = topology
        .state()
        .nodes()
        .values()
        .filter(|node| matches!(node.role, NodeRole::Primary))
        .count();
    let mut out = String::new();
    let lines = [
        ("cluster_enabled", "1".to_owned()),
        ("cluster_state", cluster_state.to_owned()),
        ("cluster_slots_assigned", assigned.to_string()),
        ("cluster_slots_ok", ok.to_string()),
        ("cluster_slots_pfail", pfail.to_string()),
        ("cluster_slots_fail", fail.to_string()),
        (
            "cluster_known_nodes",
            topology.state().nodes().len().to_string(),
        ),
        ("cluster_size", cluster_size.to_string()),
        (
            "cluster_current_epoch",
            topology.state().current_epoch().to_string(),
        ),
        ("cluster_my_epoch", local.config_epoch.to_string()),
        (
            "cluster_stats_messages_sent",
            state.message_stats.messages_sent.to_string(),
        ),
        (
            "cluster_stats_messages_received",
            state.message_stats.messages_received.to_string(),
        ),
        (
            "cluster_stats_messages_ping_sent",
            state.message_stats.ping_sent.to_string(),
        ),
        (
            "cluster_stats_messages_pong_sent",
            state.message_stats.pong_sent.to_string(),
        ),
        (
            "cluster_stats_messages_meet_sent",
            state.message_stats.meet_sent.to_string(),
        ),
        (
            "cluster_stats_messages_fail_received",
            state.message_stats.fail_received.to_string(),
        ),
        (
            "total_cluster_links_buffer_limit_exceeded",
            state
                .message_stats
                .total_cluster_links_buffer_limit_exceeded
                .to_string(),
        ),
        (
            "cluster_link_sendbuf_limit_exceeded",
            state
                .message_stats
                .cluster_link_sendbuf_limit_exceeded
                .to_string(),
        ),
    ];
    for (key, value) in lines {
        let _ = writeln!(out, "{key}:{value}\r");
    }
    out
}

fn format_cluster_nodes(state: &ClusterCommandState, filter_primary: Option<NodeId>) -> String {
    let Some(topology) = state.topology.as_ref() else {
        return String::new();
    };
    let local_id = topology.state().local_node_id();
    let mut ids = topology.state().nodes().keys().copied().collect::<Vec<_>>();
    ids.sort_unstable();
    let mut out = String::new();
    for node_id in ids {
        let Some(node) = topology.state().get_node(&node_id) else {
            continue;
        };
        if let Some(primary) = filter_primary {
            match node.role {
                NodeRole::Replica {
                    primary: replica_primary,
                } if replica_primary == primary => {}
                _ => continue,
            }
        }
        let addr = format!(
            "{}:{}@{}",
            node.addr.ip(),
            node.addr.port(),
            node.cluster_addr.port()
        );
        let flags = node_flags(node, node.id == local_id, state.replica_no_failover);
        let primary = match node.role {
            NodeRole::Primary => "-".to_owned(),
            NodeRole::Replica { primary } => primary.to_string(),
        };
        let link_state = if matches!(node.state, NodeState::Connected) {
            "connected"
        } else {
            "disconnected"
        };
        let mut line = format!(
            "{} {} {} {} {} {} {} {}",
            node.id,
            addr,
            flags,
            primary,
            node.ping_sent,
            node.pong_recv,
            node.config_epoch,
            link_state
        );
        let slots = slot_ranges(&node.slots);
        if !slots.is_empty() {
            line.push(' ');
            line.push_str(&slots.join(" "));
        }
        line.push_str("\r\n");
        out.push_str(&line);
    }
    out
}

fn build_shards_response(state: &ClusterCommandState) -> Response {
    let mut primaries =
        HashMap::<NodeId, Vec<NodeId>, RandomState>::with_hasher(RandomState::new());
    let Some(topology) = state.topology.as_ref() else {
        return Response::Array(Box::new(SmallVec::new()));
    };
    for node in topology.state().nodes().values() {
        match node.role {
            NodeRole::Primary => {
                primaries.entry(node.id).or_default();
            }
            NodeRole::Replica { primary } => {
                primaries.entry(primary).or_default().push(node.id);
            }
        }
    }
    let mut shards = SmallVec::<[Response; 16]>::new();
    let mut primary_ids = primaries.keys().copied().collect::<Vec<_>>();
    primary_ids.sort_unstable();
    for primary_id in primary_ids {
        let Some(primary) = topology.state().get_node(&primary_id) else {
            continue;
        };
        let mut nodes = SmallVec::<[Response; 16]>::new();
        nodes.push(node_map(state, primary));
        let mut replicas = primaries.remove(&primary_id).unwrap_or_default();
        replicas.sort_unstable();
        for replica_id in replicas {
            if let Some(replica) = topology.state().get_node(&replica_id) {
                nodes.push(node_map(state, replica));
            }
        }
        let mut slots = SmallVec::<[Response; 16]>::new();
        for (start, end) in slot_ranges_pairs(&primary.slots) {
            slots.push(Response::Array(Box::new(smallvec![
                Response::Integer(start as i64),
                Response::Integer(end as i64),
            ])));
        }
        shards.push(Response::Map(Box::new(smallvec![
            bulk_value(b"slots"),
            Response::Array(Box::new(slots)),
            bulk_value(b"nodes"),
            Response::Array(Box::new(nodes)),
        ])));
    }
    Response::Array(Box::new(shards))
}

fn node_map(state: &ClusterCommandState, node: &NodeMeta) -> Response {
    let role = match node.role {
        NodeRole::Primary => "master",
        NodeRole::Replica { .. } => "replica",
    };
    let health = match node.state {
        NodeState::Connected => "online",
        NodeState::PFail => "pfail",
        NodeState::Failed => "fail",
        NodeState::Handshaking => "handshake",
        NodeState::Disconnected => "disconnected",
    };
    let offset = state
        .replication_offsets
        .get(&node.id)
        .copied()
        .unwrap_or(0);
    Response::Map(Box::new(smallvec![
        bulk_value(b"id"),
        bulk_value(node.id.to_string().as_bytes()),
        bulk_value(b"ip"),
        bulk_value(node.addr.ip().to_string().as_bytes()),
        bulk_value(b"port"),
        Response::Integer(node.addr.port() as i64),
        bulk_value(b"role"),
        bulk_value(role.as_bytes()),
        bulk_value(b"health"),
        bulk_value(health.as_bytes()),
        bulk_value(b"replication-offset"),
        Response::Integer(offset as i64),
    ]))
}

fn node_flags(node: &NodeMeta, myself: bool, replica_no_failover: bool) -> String {
    let mut flags = Vec::new();
    if myself {
        flags.push("myself");
    }
    match node.role {
        NodeRole::Primary => flags.push("master"),
        NodeRole::Replica { .. } => flags.push("slave"),
    }
    match node.state {
        NodeState::PFail => flags.push("fail?"),
        NodeState::Failed => flags.push("fail"),
        NodeState::Handshaking => flags.push("handshake"),
        NodeState::Disconnected => flags.push("noaddr"),
        NodeState::Connected => {}
    }
    if replica_no_failover {
        flags.push("nofailover");
    }
    if flags.is_empty() {
        "noflags".to_owned()
    } else {
        flags.join(",")
    }
}

fn render_cluster_conf(state: &ClusterCommandState) -> String {
    let mut out = format_cluster_nodes(state, None);
    let epoch = state
        .topology
        .as_ref()
        .map(|topology| topology.state().current_epoch())
        .unwrap_or(0);
    let _ = writeln!(out, "vars currentEpoch {}", epoch);
    out
}

fn slot_ranges(slots: &RoaringBitmap) -> Vec<String> {
    slot_ranges_pairs(slots)
        .into_iter()
        .map(|(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect()
}

fn slot_ranges_pairs(slots: &RoaringBitmap) -> Vec<(u32, u32)> {
    let mut pairs = Vec::new();
    let mut iter = slots.iter();
    let Some(mut start) = iter.next() else {
        return pairs;
    };
    let mut end = start;
    for slot in iter {
        if slot == end + 1 {
            end = slot;
        } else {
            pairs.push((start, end));
            start = slot;
            end = slot;
        }
    }
    pairs.push((start, end));
    pairs
}

fn ok_outcome(response: Vec<u8>) -> ClusterCommandOutcome {
    ClusterCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn bulk_value(value: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value))))
}

fn parse_node_id(raw: &[u8]) -> Result<NodeId, Vec<u8>> {
    let text = std::str::from_utf8(raw).map_err(|_| error_message("ERR Invalid node ID"))?;
    NodeId::from_str(text).map_err(|_| error_message("ERR Invalid node ID"))
}

fn parse_ip(raw: &[u8]) -> Result<IpAddr, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(())
}

fn parse_u16(raw: &[u8]) -> Result<u16, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(())
}

fn parse_usize(raw: &[u8]) -> Result<usize, Vec<u8>> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))
}

fn parse_slot(raw: &[u8]) -> Result<u16, Vec<u8>> {
    let slot = std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u16>().ok())
        .ok_or_else(|| error_message("ERR Slot is not an integer or out of range"))?;
    if slot >= 16_384 {
        return Err(error_message("ERR Slot is out of range"));
    }
    Ok(slot)
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn error_message_text_vec(message: Vec<u8>) -> Vec<u8> {
    message
}

#[cfg(test)]
mod tests {
    use super::{
        ClusterCommandState, build_shards_response, dispatch_cluster, format_cluster_info,
    };
    use crate::connection::serialize_response;
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_cluster::{NodeMeta, NodeRole, NodeState};
    use senko_core::SenkoValue;
    use senko_proto::Frame;
    use senko_store::Store;
    use std::cell::RefCell;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::rc::Rc;

    fn bs<'a>(bytes: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(bytes)
    }

    fn enabled_state() -> Rc<RefCell<ClusterCommandState>> {
        Rc::new(RefCell::new(
            ClusterCommandState::enabled(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
                0,
                PathBuf::from("/tmp/senko-cluster-test.conf"),
            )
            .unwrap(),
        ))
    }

    #[test]
    fn disabled_cluster_info_reports_cluster_enabled_zero() {
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            0,
        )));
        let store = Rc::new(RefCell::new(Store::new(None)));
        let outcome = dispatch_cluster(&[bs(b"INFO")], false, &cluster, &store, None).unwrap();
        let text = String::from_utf8(outcome.response).unwrap();
        assert!(text.contains("cluster_enabled:0"));
    }

    #[test]
    fn disabled_cluster_nodes_returns_error() {
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            0,
        )));
        let store = Rc::new(RefCell::new(Store::new(None)));
        let err = dispatch_cluster(&[bs(b"NODES")], false, &cluster, &store, None).unwrap_err();
        assert!(
            String::from_utf8(err)
                .unwrap()
                .contains("cluster support disabled")
        );
    }

    #[test]
    fn cluster_info_contains_required_fields() {
        let cluster = enabled_state();
        {
            let mut state = cluster.borrow_mut();
            for slot in 0..10 {
                state.set_local_slot_owned(slot, true).unwrap();
            }
            let _ = state.increment_epoch().unwrap();
            state.message_stats.messages_sent = 7;
            state.message_stats.messages_received = 3;
        }
        let info = format_cluster_info(&cluster.borrow());
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_slots_assigned:10"));
        assert!(info.contains("cluster_stats_messages_sent:7"));
    }

    #[test]
    fn cluster_nodes_formats_myself_and_replica_lines() {
        let cluster = enabled_state();
        {
            let mut state = cluster.borrow_mut();
            let primary_id = state.local_node_id().unwrap();
            let mut replica = NodeMeta::new(
                senko_cluster::NodeId::new([9; 20]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6380),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16380),
            );
            replica.role = NodeRole::Replica {
                primary: primary_id,
            };
            state
                .topology_mut_ref()
                .unwrap()
                .state_mut()
                .insert_node(replica);
            state.rebuild_topology().unwrap();
            state.set_local_slot_owned(0, true).unwrap();
        }
        let store = Rc::new(RefCell::new(Store::new(None)));
        let outcome = dispatch_cluster(&[bs(b"NODES")], false, &cluster, &store, None).unwrap();
        let text = String::from_utf8(outcome.response).unwrap();
        assert!(text.contains("myself,master"));
        assert!(text.contains("slave"));
        assert!(text.contains("0"));
    }

    #[test]
    fn cluster_meet_adds_node() {
        let cluster = enabled_state();
        let store = Rc::new(RefCell::new(Store::new(None)));
        let _ = dispatch_cluster(
            &[bs(b"MEET"), bs(b"127.0.0.1"), bs(b"7001")],
            false,
            &cluster,
            &store,
            None,
        )
        .unwrap();
        assert_eq!(
            cluster.borrow().topology().unwrap().state().nodes().len(),
            2
        );
    }

    #[test]
    fn cluster_replicate_changes_local_role() {
        let cluster = enabled_state();
        let primary_id = {
            let mut state = cluster.borrow_mut();
            let mut primary = NodeMeta::new(
                senko_cluster::NodeId::new([8; 20]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6381),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16381),
            );
            primary.state = NodeState::Connected;
            state
                .topology_mut_ref()
                .unwrap()
                .state_mut()
                .insert_node(primary.clone());
            state.rebuild_topology().unwrap();
            primary.id
        };
        let store = Rc::new(RefCell::new(Store::new(None)));
        let _ = dispatch_cluster(
            &[bs(b"REPLICATE"), bs(primary_id.to_string().as_bytes())],
            false,
            &cluster,
            &store,
            None,
        )
        .unwrap();
        assert!(matches!(
            cluster.borrow().local_meta().unwrap().role,
            NodeRole::Replica { primary } if primary == primary_id
        ));
    }

    #[test]
    fn cluster_failover_promotes_replica_and_claims_slots() {
        let cluster = enabled_state();
        let primary_id = {
            let mut state = cluster.borrow_mut();
            let local_id = state.local_node_id().unwrap();
            let mut primary = NodeMeta::new(
                senko_cluster::NodeId::new([7; 20]),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6382),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16382),
            );
            primary.slots.insert_range(0..10);
            state
                .topology_mut_ref()
                .unwrap()
                .state_mut()
                .insert_node(primary.clone());
            state.rebuild_topology().unwrap();
            state.local_meta_mut().unwrap().role = NodeRole::Replica {
                primary: primary.id,
            };
            state.replication_offsets.insert(local_id, 10);
            state.replication_offsets.insert(primary.id, 10);
            primary.id
        };
        let store = Rc::new(RefCell::new(Store::new(None)));
        let _ = dispatch_cluster(&[bs(b"FAILOVER")], false, &cluster, &store, None).unwrap();
        assert!(matches!(
            cluster.borrow().local_meta().unwrap().role,
            NodeRole::Primary
        ));
        assert!(cluster.borrow().local_meta().unwrap().slots.contains(0));
        assert!(matches!(
            cluster
                .borrow()
                .topology()
                .unwrap()
                .state()
                .get_node(&primary_id)
                .unwrap()
                .role,
            NodeRole::Replica { .. }
        ));
    }

    #[test]
    fn cluster_reset_hard_clears_data_and_changes_node_id() {
        let cluster = enabled_state();
        let old_id = cluster.borrow().local_node_id().unwrap();
        let store = Rc::new(RefCell::new(Store::new(None)));
        let _ = store.borrow_mut().set(
            CompactString::from("k"),
            SenkoValue::Raw(Bytes::from_static(b"v")),
            Default::default(),
        );
        let _ =
            dispatch_cluster(&[bs(b"RESET"), bs(b"HARD")], false, &cluster, &store, None).unwrap();
        assert_ne!(cluster.borrow().local_node_id().unwrap(), old_id);
        assert_eq!(store.borrow().entry_count(), 0);
    }

    #[test]
    fn cluster_shards_returns_resp_structure() {
        let cluster = enabled_state();
        {
            let mut state = cluster.borrow_mut();
            for slot in 0..3 {
                state.set_local_slot_owned(slot, true).unwrap();
            }
        }
        let response = build_shards_response(&cluster.borrow());
        let encoded = serialize_response(&response, true);
        let text = String::from_utf8_lossy(&encoded);
        assert!(text.starts_with("%") || text.starts_with("*"));
    }

    #[test]
    fn cluster_getkeysinslot_and_countkeysinslot_reflect_store() {
        let cluster = enabled_state();
        let store = Rc::new(RefCell::new(Store::new(None)));
        let key = b"{tenant}:a";
        let _ = store.borrow_mut().set(
            CompactString::from_utf8(key).unwrap(),
            SenkoValue::Raw(Bytes::from_static(b"v")),
            Default::default(),
        );
        let slot = senko_cluster::crc16_slot(key);
        let count = dispatch_cluster(
            &[bs(b"COUNTKEYSINSLOT"), bs(slot.to_string().as_bytes())],
            true,
            &cluster,
            &store,
            None,
        )
        .unwrap();
        let keys = dispatch_cluster(
            &[
                bs(b"GETKEYSINSLOT"),
                bs(slot.to_string().as_bytes()),
                bs(b"10"),
            ],
            true,
            &cluster,
            &store,
            None,
        )
        .unwrap();
        assert!(String::from_utf8_lossy(&count.response).contains(":1"));
        assert!(String::from_utf8_lossy(&keys.response).contains("{tenant}:a"));
    }
}
