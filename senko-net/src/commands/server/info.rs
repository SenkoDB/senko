#![allow(clippy::too_many_arguments)]

use std::{
    cell::RefCell,
    fmt::Write as _,
    fs,
    path::PathBuf,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use compio::runtime::spawn_blocking;
use crossfire::compat::{BlockingRxTrait, MRx as Receiver, MTx as Sender, TryRecvError};
use hashbrown::HashMap;
use senko_cluster::NodeId;
use senko_core::{ProbMergeValue, SenkoConfig, SenkoValue};
use senko_proto::Frame;
use senko_pubsub::BroadcastSlot;
use senko_scripting::{LuaEngine, functions::RestoreMode};
use senko_store::{Response, Store};
use smallvec::{SmallVec, smallvec};

use crate::{
    blocked::BlockedKeyRegistry,
    commands::cluster::ClusterCommandState,
    commands::connection::client_ops::{PauseMode, PauseState},
    commands::server::config as live_config,
    connection::{
        ClientConnectionMap, bulk_string, error_bytes, error_message, frame_bytes,
        serialize_response,
    },
    pubsub::fanout::ShardFanOut,
    transaction::{ConnectionMap, WatchRegistry},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Primary,
    Replica,
}

const REDIS_VERSION: &str = "8.0.0";
const MONOTONIC_CLOCK: &str = "POSIX clock_gettime";
const MULTIPLEXING_API: &str = "io_uring";
const ATOMICVAR_API: &str = "atomic-builtin";
const MEM_ALLOCATOR: &str = "system";
const CACHE_TTL_MS: u64 = 100;
const PROC_JIFFY_HZ: f64 = 100.0;

static SERVER_STATE: std::sync::OnceLock<Arc<ServerState>> = std::sync::OnceLock::new();
static QUERY_BUS: std::sync::OnceLock<Arc<ShardQueryBus>> = std::sync::OnceLock::new();
static SCRIPTING_METADATA_LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();

#[derive(Debug)]
pub struct ServerCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

#[derive(Debug)]
pub struct ShardStats {
    pub commands_processed: AtomicU64,
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,
    pub expired_keys: AtomicU64,
    pub evicted_keys: AtomicU64,
    pub net_input_bytes: AtomicU64,
    pub net_output_bytes: AtomicU64,
    pub connections_received: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub blocking_keys: AtomicU64,
    pub connected_clients: AtomicU64,
    pub client_recent_max_input_buffer: AtomicU64,
    pub client_recent_max_output_buffer: AtomicU64,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            commands_processed: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            net_input_bytes: AtomicU64::new(0),
            net_output_bytes: AtomicU64::new(0),
            connections_received: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            blocking_keys: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            client_recent_max_input_buffer: AtomicU64::new(0),
            client_recent_max_output_buffer: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ShardSnapshot {
    key_count: u64,
    expiry_count: u64,
    used_memory: u64,
    blocking_keys: u64,
    pubsub_channels: u64,
    pubsub_patterns: u64,
    pubsub_shardchannels: u64,
}

#[derive(Debug)]
pub(crate) enum ShardQuery {
    Snapshot {
        reply: Sender<ShardSnapshot>,
    },
    Pause {
        reply: Sender<()>,
    },
    Resume {
        reply: Sender<()>,
    },
    Flush {
        reply: Option<Sender<()>>,
    },
    ExportRdb {
        reply: Sender<Vec<senko_store::ReplicationSnapshotEntry>>,
    },
    MemoryUsage {
        key: Vec<u8>,
        reply: Sender<Option<u64>>,
    },
    FetchValue {
        key: Vec<u8>,
        reply: Sender<Option<ProbMergeValue>>,
    },
    ScriptLoad {
        script: Bytes,
        reply: Sender<Result<String, String>>,
    },
    ScriptFlush {
        reply: Sender<Result<(), String>>,
    },
    FunctionLoad {
        source: Bytes,
        replace: bool,
        reply: Sender<Result<(), String>>,
    },
    FunctionDelete {
        library_name: String,
        reply: Sender<Result<(), String>>,
    },
    FunctionFlush {
        reply: Sender<Result<(), String>>,
    },
    FunctionRestore {
        payload: Bytes,
        mode: RestoreMode,
        reply: Sender<Result<(), String>>,
    },
    KillScript {
        reply: Sender<Result<bool, String>>,
    },
    ShardPubSubSubscribe {
        channel: Bytes,
        conn_id: u64,
        reply: Sender<Result<Arc<BroadcastSlot>, String>>,
    },
    ShardPubSubUnsubscribe {
        channel: Bytes,
        conn_id: u64,
        reply: Sender<Result<(), String>>,
    },
    ShardPubSubPublish {
        channel: Bytes,
        payload: Bytes,
        reply: Sender<Result<u64, String>>,
    },
}

#[derive(Debug)]
struct ShardQueryBus {
    senders: Box<[Sender<ShardQuery>]>,
    receivers: Box<[Mutex<Option<Receiver<ShardQuery>>>]>,
}

impl ShardQueryBus {
    fn new(num_shards: usize) -> Self {
        let mut senders = Vec::with_capacity(num_shards);
        let mut receivers = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (sender, receiver) = crossfire::compat::mpmc::bounded_blocking(64);
            senders.push(sender);
            receivers.push(Mutex::new(Some(receiver)));
        }
        Self {
            senders: senders.into_boxed_slice(),
            receivers: receivers.into_boxed_slice(),
        }
    }

    fn take_receiver(&self, shard_id: usize) -> Receiver<ShardQuery> {
        self.receivers[shard_id]
            .lock()
            .expect("query receiver lock poisoned")
            .take()
            .expect("query receiver already taken")
    }

    fn sender(&self, shard_id: usize) -> &Sender<ShardQuery> {
        &self.senders[shard_id]
    }

    fn shard_count(&self) -> usize {
        self.senders.len()
    }
}

#[derive(Debug)]
struct ServerState {
    build_id: String,
    run_id: String,
    replid: Mutex<String>,
    replication_role: AtomicU64,
    replica_primary_host: Mutex<Option<String>>,
    replica_primary_port: AtomicU64,
    startup_time_ms: u64,
    process_id: u32,
    startup_memory: u64,
    peak_memory: AtomicU64,
    last_save_time: AtomicU64,
    rdb_bgsave_in_progress: AtomicBool,
    rdb_last_bgsave_ok: AtomicBool,
    rdb_last_bgsave_time_sec: AtomicU64,
    aof_last_bgrewrite_ok: AtomicBool,
    bgsave_scheduled: AtomicBool,
    shard_stats: Box<[ShardStats]>,
    error_counts: Mutex<HashMap<String, u64, ahash::RandomState>>,
    aggregate_cache: Mutex<Option<CachedAggregate>>,
    rate_state: Mutex<RateState>,
    num_shards: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AggregateSnapshotForDiagnostics {
    pub key_count: u64,
    pub expiry_count: u64,
    pub store_used_memory: u64,
    pub connected_clients: u64,
    pub used_memory: u64,
    pub used_memory_peak: u64,
    pub used_memory_overhead: u64,
    pub used_memory_startup: u64,
    pub allocator_allocated: u64,
    pub allocator_active: u64,
    pub allocator_resident: u64,
    pub fragmentation_ratio: f64,
    pub fragmentation_bytes: u64,
    pub rss_overhead_ratio: f64,
    pub rss_overhead_bytes: u64,
    pub dataset_percentage: f64,
    pub peak_percentage: f64,
}

#[derive(Debug, Clone)]
struct CachedAggregate {
    at_ms: u64,
    snapshot: AggregateSnapshot,
}

#[derive(Debug, Clone, Default)]
struct AggregateSnapshot {
    at_ms: u64,
    key_count: u64,
    expiry_count: u64,
    store_used_memory: u64,
    connected_clients: u64,
    recent_max_input_buffer: u64,
    recent_max_output_buffer: u64,
    total_connections_received: u64,
    total_commands_processed: u64,
    total_net_input_bytes: u64,
    total_net_output_bytes: u64,
    rejected_connections: u64,
    expired_keys: u64,
    evicted_keys: u64,
    keyspace_hits: u64,
    keyspace_misses: u64,
    total_blocking_keys: u64,
    pubsub_channels: u64,
    pubsub_patterns: u64,
    pubsub_shardchannels: u64,
    instantaneous_ops_per_sec: u64,
    instantaneous_input_kbps: f64,
    instantaneous_output_kbps: f64,
}

#[derive(Debug, Default)]
struct RateState {
    at_ms: u64,
    commands_processed: u64,
    net_input_bytes: u64,
    net_output_bytes: u64,
    ops_per_sec: u64,
    input_kbps: f64,
    output_kbps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InfoSection {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cpu,
    Modules,
    Commandstats,
    Errorstats,
    Cluster,
    Keyspace,
}

pub fn init(config: &SenkoConfig) {
    let startup_time_ms = current_unix_ms();
    let startup_time_sec = startup_time_ms / 1_000;
    let startup_memory = read_process_rss().max(1);
    let state = Arc::new(ServerState {
        build_id: env!("SENKO_BUILD_ID").to_owned(),
        run_id: NodeId::generate().to_string(),
        replid: Mutex::new(NodeId::generate().to_string()),
        replication_role: AtomicU64::new(ReplicationRole::Primary as u64),
        replica_primary_host: Mutex::new(None),
        replica_primary_port: AtomicU64::new(0),
        startup_time_ms,
        process_id: std::process::id(),
        startup_memory,
        peak_memory: AtomicU64::new(startup_memory),
        last_save_time: AtomicU64::new(startup_time_sec),
        rdb_bgsave_in_progress: AtomicBool::new(false),
        rdb_last_bgsave_ok: AtomicBool::new(true),
        rdb_last_bgsave_time_sec: AtomicU64::new(u64::MAX),
        aof_last_bgrewrite_ok: AtomicBool::new(true),
        bgsave_scheduled: AtomicBool::new(false),
        shard_stats: (0..config.num_shards)
            .map(|_| ShardStats::default())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        error_counts: Mutex::new(HashMap::with_hasher(ahash::RandomState::new())),
        aggregate_cache: Mutex::new(None),
        rate_state: Mutex::new(RateState {
            at_ms: startup_time_ms,
            ..RateState::default()
        }),
        num_shards: config.num_shards,
    });
    let _ = SERVER_STATE.set(state);
    let _ = QUERY_BUS.set(Arc::new(ShardQueryBus::new(config.num_shards)));
}

fn state() -> &'static Arc<ServerState> {
    SERVER_STATE.get().expect("server state not initialized")
}

pub fn bgsave_in_progress() -> bool {
    state().rdb_bgsave_in_progress.load(Ordering::Relaxed)
}

pub fn set_bgsave_in_progress(value: bool) {
    state()
        .rdb_bgsave_in_progress
        .store(value, Ordering::Relaxed);
}

pub fn schedule_bgsave() {
    state().bgsave_scheduled.store(true, Ordering::Relaxed);
}

pub fn take_scheduled_bgsave() -> bool {
    state().bgsave_scheduled.swap(false, Ordering::Relaxed)
}

pub fn set_aof_last_bgrewrite_status_ok() {
    state().aof_last_bgrewrite_ok.store(true, Ordering::Relaxed);
}

pub fn record_save_success(duration: Duration) {
    let elapsed = duration.as_secs();
    state()
        .last_save_time
        .store(current_unix_ms() / 1_000, Ordering::Relaxed);
    state().rdb_last_bgsave_ok.store(true, Ordering::Relaxed);
    state()
        .rdb_last_bgsave_time_sec
        .store(elapsed, Ordering::Relaxed);
    state()
        .rdb_bgsave_in_progress
        .store(false, Ordering::Relaxed);
}

pub fn record_save_failure() {
    state().rdb_last_bgsave_ok.store(false, Ordering::Relaxed);
    state()
        .rdb_bgsave_in_progress
        .store(false, Ordering::Relaxed);
}

pub fn replication_role() -> ReplicationRole {
    match state().replication_role.load(Ordering::Relaxed) {
        1 => ReplicationRole::Replica,
        _ => ReplicationRole::Primary,
    }
}

pub fn set_replication_primary() {
    state()
        .replication_role
        .store(ReplicationRole::Primary as u64, Ordering::Relaxed);
    *state()
        .replica_primary_host
        .lock()
        .expect("replica primary host lock poisoned") = None;
    state().replica_primary_port.store(0, Ordering::Relaxed);
}

pub fn set_replication_replica(host: String, port: u16) {
    state()
        .replication_role
        .store(ReplicationRole::Replica as u64, Ordering::Relaxed);
    *state()
        .replica_primary_host
        .lock()
        .expect("replica primary host lock poisoned") = Some(host);
    state()
        .replica_primary_port
        .store(port as u64, Ordering::Relaxed);
}

pub fn replica_primary_target() -> Option<(String, u16)> {
    let host = state()
        .replica_primary_host
        .lock()
        .expect("replica primary host lock poisoned")
        .clone()?;
    Some((
        host,
        state().replica_primary_port.load(Ordering::Relaxed) as u16,
    ))
}

pub fn current_replication_id() -> String {
    state().replid.lock().expect("replid lock poisoned").clone()
}

pub fn regenerate_replication_id() -> String {
    let mut replid = state().replid.lock().expect("replid lock poisoned");
    *replid = NodeId::generate().to_string();
    replid.clone()
}

fn query_bus() -> &'static Arc<ShardQueryBus> {
    QUERY_BUS.get().expect("query bus not initialized")
}

fn scripting_metadata_lock() -> &'static Mutex<()> {
    SCRIPTING_METADATA_LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn take_query_receiver(shard_id: usize) -> Receiver<ShardQuery> {
    query_bus().take_receiver(shard_id)
}

pub(crate) fn drain_shard_queries(
    shard_id: usize,
    receiver: &Receiver<ShardQuery>,
    store: &Rc<RefCell<Store>>,
    engine: &Rc<RefCell<LuaEngine>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    pause_state: &Rc<RefCell<PauseState>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> usize {
    let mut drained = 0usize;
    while let Ok(query) = receiver.try_recv() {
        drained += 1;
        match query {
            ShardQuery::Snapshot { reply } => {
                let snapshot = ShardSnapshot {
                    key_count: store.borrow().entry_count() as u64,
                    expiry_count: store.borrow().expiry_count() as u64,
                    used_memory: store.borrow().used_memory() as u64,
                    blocking_keys: blocked.borrow().waiters.len() as u64,
                    pubsub_channels: if shard_id == 0 {
                        shard_pubsub.borrow().pubsub_channels(None).len() as u64
                    } else {
                        0
                    },
                    pubsub_patterns: if shard_id == 0 {
                        shard_pubsub.borrow().pubsub_numpat() as u64
                    } else {
                        0
                    },
                    pubsub_shardchannels: if shard_id == 0 {
                        shard_pubsub.borrow().shard_channels(None).len() as u64
                    } else {
                        0
                    },
                };
                state().shard_stats[shard_id]
                    .blocking_keys
                    .store(snapshot.blocking_keys, Ordering::Relaxed);
                let _ = client_connections;
                let _ = reply.send(snapshot);
            }
            ShardQuery::Pause { reply } => {
                pause_state.borrow_mut().set(None, PauseMode::All);
                let _ = reply.send(());
            }
            ShardQuery::Resume { reply } => {
                for waker in pause_state.borrow_mut().clear() {
                    waker.wake();
                }
                let _ = reply.send(());
            }
            ShardQuery::Flush { reply } => {
                store.borrow_mut().clear();
                watch_registry
                    .borrow_mut()
                    .mark_all_dirty(&mut connections.borrow_mut());
                let _ = blocked.borrow_mut().clear_all();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            ShardQuery::ExportRdb { reply } => {
                let snapshot = store.borrow_mut().replication_snapshot();
                let _ = reply.send(snapshot);
            }
            ShardQuery::MemoryUsage { key, reply } => {
                let usage = store
                    .borrow_mut()
                    .memory_usage(key.as_slice())
                    .map(|value| value as u64);
                let _ = reply.send(usage);
            }
            ShardQuery::FetchValue { key, reply } => {
                let value = match store.borrow_mut().get_cloned(key.as_slice()) {
                    Some(SenkoValue::CountMinSketch(sketch)) => {
                        Some(ProbMergeValue::CountMinSketch(sketch))
                    }
                    Some(SenkoValue::TDigest(digest)) => Some(ProbMergeValue::TDigest(digest)),
                    _ => None,
                };
                let _ = reply.send(value);
            }
            ShardQuery::ScriptLoad { script, reply } => {
                let result = std::str::from_utf8(script.as_ref())
                    .map_err(|error| error.to_string())
                    .and_then(|script| {
                        engine
                            .borrow_mut()
                            .script_load(script)
                            .map_err(|error| error.client_message())
                    });
                let _ = reply.send(result);
            }
            ShardQuery::ScriptFlush { reply } => {
                let result = engine
                    .borrow_mut()
                    .script_flush()
                    .map_err(|error| error.client_message());
                let _ = reply.send(result);
            }
            ShardQuery::FunctionLoad {
                source,
                replace,
                reply,
            } => {
                let result = std::str::from_utf8(source.as_ref())
                    .map_err(|error| error.to_string())
                    .and_then(|source| {
                        engine
                            .borrow_mut()
                            .function_load(source, replace)
                            .map_err(|error| error.client_message())
                    });
                let _ = reply.send(result);
            }
            ShardQuery::FunctionDelete {
                library_name,
                reply,
            } => {
                let result = engine
                    .borrow_mut()
                    .function_delete(library_name.as_str())
                    .map_err(|error| error.client_message());
                let _ = reply.send(result);
            }
            ShardQuery::FunctionFlush { reply } => {
                let result = engine
                    .borrow_mut()
                    .function_flush()
                    .map_err(|error| error.client_message());
                let _ = reply.send(result);
            }
            ShardQuery::FunctionRestore {
                payload,
                mode,
                reply,
            } => {
                let result = engine
                    .borrow_mut()
                    .function_restore(payload.as_ref(), mode)
                    .map_err(|error| error.client_message());
                let _ = reply.send(result);
            }
            ShardQuery::KillScript { reply } => {
                let result = match engine.borrow().request_kill() {
                    Ok(()) => Ok(true),
                    Err(senko_scripting::LuaError::NotBusy) => Ok(false),
                    Err(error) => Err(error.client_message()),
                };
                let _ = reply.send(result);
            }
            ShardQuery::ShardPubSubSubscribe {
                channel,
                conn_id,
                reply,
            } => {
                let slot = shard_pubsub
                    .borrow_mut()
                    .subscribe_shard_local(channel.as_ref(), conn_id);
                let _ = reply.send(Ok(slot));
            }
            ShardQuery::ShardPubSubUnsubscribe {
                channel,
                conn_id,
                reply,
            } => {
                let _ = shard_pubsub
                    .borrow_mut()
                    .unsubscribe_shard_local(channel.as_ref(), conn_id);
                let _ = reply.send(Ok(()));
            }
            ShardQuery::ShardPubSubPublish {
                channel,
                payload,
                reply,
            } => {
                let delivered = shard_pubsub
                    .borrow_mut()
                    .spublish_local(channel.as_ref(), payload);
                let _ = reply.send(Ok(delivered));
            }
        }
    }
    drained
}

pub fn on_connection_open(shard_id: usize) {
    let stats = &state().shard_stats[shard_id];
    stats.connected_clients.fetch_add(1, Ordering::Relaxed);
    stats.connections_received.fetch_add(1, Ordering::Relaxed);
}

pub fn on_connection_close(shard_id: usize) {
    let stats = &state().shard_stats[shard_id];
    let _ = stats
        .connected_clients
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_sub(1))
        });
}

pub fn on_command_processed(shard_id: usize) {
    state().shard_stats[shard_id]
        .commands_processed
        .fetch_add(1, Ordering::Relaxed);
}

pub fn on_read(shard_id: usize, bytes: usize, input_buffer_len: usize) {
    let stats = &state().shard_stats[shard_id];
    stats
        .net_input_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    observe_max(
        &stats.client_recent_max_input_buffer,
        input_buffer_len as u64,
    );
}

pub fn on_write(shard_id: usize, bytes: usize, output_buffer_len: usize) {
    let stats = &state().shard_stats[shard_id];
    stats
        .net_output_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    observe_max(
        &stats.client_recent_max_output_buffer,
        output_buffer_len as u64,
    );
}

pub fn on_expired_keys(shard_id: usize, count: usize) {
    if count == 0 {
        return;
    }
    state().shard_stats[shard_id]
        .expired_keys
        .fetch_add(count as u64, Ordering::Relaxed);
}

pub fn reset_runtime_stats() {
    for stats in state().shard_stats.iter() {
        stats.commands_processed.store(0, Ordering::Relaxed);
        stats.keyspace_hits.store(0, Ordering::Relaxed);
        stats.keyspace_misses.store(0, Ordering::Relaxed);
        stats.expired_keys.store(0, Ordering::Relaxed);
        stats.evicted_keys.store(0, Ordering::Relaxed);
        stats.net_input_bytes.store(0, Ordering::Relaxed);
        stats.net_output_bytes.store(0, Ordering::Relaxed);
        stats.connections_received.store(0, Ordering::Relaxed);
        stats.rejected_connections.store(0, Ordering::Relaxed);
        stats.blocking_keys.store(0, Ordering::Relaxed);
        stats
            .client_recent_max_input_buffer
            .store(0, Ordering::Relaxed);
        stats
            .client_recent_max_output_buffer
            .store(0, Ordering::Relaxed);
    }
    state()
        .aggregate_cache
        .lock()
        .expect("aggregate cache lock poisoned")
        .take();
    *state().rate_state.lock().expect("rate state lock poisoned") = RateState {
        at_ms: current_unix_ms(),
        ..RateState::default()
    };
    state()
        .error_counts
        .lock()
        .expect("error counter lock poisoned")
        .clear();
}

pub fn record_error_response(response: &[u8]) {
    if response.first().copied() != Some(b'-') {
        return;
    }
    let end = response[1..]
        .iter()
        .position(|byte| *byte == b' ' || *byte == b'\r')
        .map(|idx| idx + 1)
        .unwrap_or(response.len());
    let prefix = std::str::from_utf8(&response[1..end]).unwrap_or("ERR");
    let mut errors = state()
        .error_counts
        .lock()
        .expect("error counter lock poisoned");
    *errors.entry(prefix.to_owned()).or_insert(0) += 1;
}

pub async fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    config: &SenkoConfig,
    last_time_us: &mut u64,
) -> Option<Result<ServerCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"INFO") {
        return Some(handle_info(args, cluster).await);
    }
    if eq_ascii(command, b"TIME") {
        return Some(handle_time(args, resp3, last_time_us));
    }
    if eq_ascii(command, b"DBSIZE") {
        return Some(handle_dbsize(args, resp3).await);
    }
    if eq_ascii(command, b"ROLE") {
        return Some(handle_role(args, resp3));
    }
    if eq_ascii(command, b"LASTSAVE") {
        return Some(handle_lastsave(args, resp3));
    }
    if eq_ascii(command, b"LOLWUT") {
        return Some(handle_lolwut(args, config));
    }
    None
}

async fn handle_info(
    args: &[Frame<'_>],
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    let sections = parse_info_sections(args)?;
    let aggregate = aggregate_snapshot().await;
    let cluster_enabled = cluster.borrow().is_enabled();
    let body = render_info(&sections, &aggregate, cluster_enabled);
    Ok(outcome(bulk_string(body.as_bytes())))
}

fn handle_time(
    args: &[Frame<'_>],
    resp3: bool,
    last_time_us: &mut u64,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'time' command",
        ));
    }
    let now_us = current_unix_us();
    let monotonic = now_us.max(*last_time_us);
    *last_time_us = monotonic;
    let seconds = monotonic / 1_000_000;
    let micros = monotonic % 1_000_000;
    let response = Response::Array(Box::new(smallvec![
        bulk_response(seconds.to_string().into_bytes()),
        bulk_response(micros.to_string().into_bytes()),
    ]));
    Ok(outcome(serialize_response(&response, resp3)))
}

async fn handle_dbsize(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'dbsize' command",
        ));
    }
    let aggregate = aggregate_snapshot().await;
    Ok(outcome(serialize_response(
        &Response::Integer(aggregate.key_count as i64),
        resp3,
    )))
}

fn handle_role(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'role' command",
        ));
    }
    let response = Response::Array(Box::new(smallvec![
        bulk_response(b"master".to_vec()),
        Response::Integer(0),
        Response::Array(Box::new(SmallVec::new())),
    ]));
    Ok(outcome(serialize_response(&response, resp3)))
}

fn handle_lastsave(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'lastsave' command",
        ));
    }
    Ok(outcome(serialize_response(
        &Response::Integer(state().last_save_time.load(Ordering::Relaxed) as i64),
        resp3,
    )))
}

fn handle_lolwut(
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    let version = parse_lolwut_version(args)?;
    let body = render_lolwut(version, env!("CARGO_PKG_VERSION"), config.num_shards);
    Ok(outcome(bulk_string(body.as_bytes())))
}

fn parse_info_sections(args: &[Frame<'_>]) -> Result<Vec<InfoSection>, Vec<u8>> {
    if args.is_empty() {
        return Ok(default_sections(true));
    }
    let mut out = Vec::new();
    for arg in args {
        let token = frame_bytes(arg).map_err(|error| error_bytes(&error))?;
        if eq_ascii(token, b"default") {
            extend_unique(&mut out, &default_sections(false));
            continue;
        }
        if eq_ascii(token, b"everything") {
            extend_unique(&mut out, &default_sections(true));
            continue;
        }
        if eq_ascii(token, b"all") {
            extend_unique(&mut out, &all_sections());
            continue;
        }
        if let Some(section) = parse_info_section(token)
            && !out.contains(&section)
        {
            out.push(section);
        }
    }
    Ok(out)
}

fn parse_info_section(token: &[u8]) -> Option<InfoSection> {
    if eq_ascii(token, b"server") {
        Some(InfoSection::Server)
    } else if eq_ascii(token, b"clients") {
        Some(InfoSection::Clients)
    } else if eq_ascii(token, b"memory") {
        Some(InfoSection::Memory)
    } else if eq_ascii(token, b"persistence") {
        Some(InfoSection::Persistence)
    } else if eq_ascii(token, b"stats") {
        Some(InfoSection::Stats)
    } else if eq_ascii(token, b"replication") {
        Some(InfoSection::Replication)
    } else if eq_ascii(token, b"cpu") {
        Some(InfoSection::Cpu)
    } else if eq_ascii(token, b"modules") {
        Some(InfoSection::Modules)
    } else if eq_ascii(token, b"commandstats") {
        Some(InfoSection::Commandstats)
    } else if eq_ascii(token, b"errorstats") {
        Some(InfoSection::Errorstats)
    } else if eq_ascii(token, b"cluster") {
        Some(InfoSection::Cluster)
    } else if eq_ascii(token, b"keyspace") {
        Some(InfoSection::Keyspace)
    } else {
        None
    }
}

fn parse_lolwut_version(args: &[Frame<'_>]) -> Result<u8, Vec<u8>> {
    match args {
        [] => Ok(1),
        [name, value]
            if eq_ascii(
                frame_bytes(name).map_err(|error| error_bytes(&error))?,
                b"VERSION",
            ) =>
        {
            let raw = frame_bytes(value).map_err(|error| error_bytes(&error))?;
            let parsed = std::str::from_utf8(raw)
                .ok()
                .and_then(|text| text.parse::<u8>().ok())
                .unwrap_or(1);
            Ok(parsed.clamp(1, 5))
        }
        _ => Err(error_message("ERR syntax error")),
    }
}

async fn aggregate_snapshot() -> AggregateSnapshot {
    let now_ms = current_unix_ms();
    if let Some(cached) = state()
        .aggregate_cache
        .lock()
        .expect("aggregate cache lock poisoned")
        .clone()
        && now_ms.saturating_sub(cached.at_ms) <= CACHE_TTL_MS
    {
        return cached.snapshot;
    }

    let snapshot = query_all_shards(now_ms).await;
    let mut cache = state()
        .aggregate_cache
        .lock()
        .expect("aggregate cache lock poisoned");
    *cache = Some(CachedAggregate {
        at_ms: now_ms,
        snapshot: snapshot.clone(),
    });
    snapshot
}

pub async fn aggregate_snapshot_for_diagnostics() -> AggregateSnapshotForDiagnostics {
    let aggregate = aggregate_snapshot().await;
    let memory = collect_memory_metrics(&aggregate);
    AggregateSnapshotForDiagnostics {
        key_count: aggregate.key_count,
        expiry_count: aggregate.expiry_count,
        store_used_memory: aggregate.store_used_memory,
        connected_clients: aggregate.connected_clients,
        used_memory: memory.used_memory,
        used_memory_peak: memory.used_memory_peak,
        used_memory_overhead: memory.used_memory_overhead,
        used_memory_startup: memory.used_memory_startup,
        allocator_allocated: memory.allocator_allocated,
        allocator_active: memory.allocator_active,
        allocator_resident: memory.allocator_resident,
        fragmentation_ratio: ratio(memory.used_memory_rss, memory.used_memory_dataset),
        fragmentation_bytes: memory.mem_fragmentation_bytes,
        rss_overhead_ratio: ratio(memory.used_memory_rss, memory.used_memory),
        rss_overhead_bytes: memory.rss_overhead_bytes,
        dataset_percentage: percentage(memory.used_memory_dataset, memory.used_memory),
        peak_percentage: percentage(memory.used_memory, memory.used_memory_peak),
    }
}

async fn query_all_shards(now_ms: u64) -> AggregateSnapshot {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::Snapshot {
            reply: reply_tx.clone(),
        });
    }
    drop(reply_tx);

    let deadline = std::time::Instant::now() + Duration::from_millis(25);
    let mut shard_snapshots = Vec::with_capacity(bus.shard_count());
    while shard_snapshots.len() < bus.shard_count() && std::time::Instant::now() < deadline {
        match reply_rx.try_recv() {
            Ok(snapshot) => shard_snapshots.push(snapshot),
            Err(TryRecvError::Empty) => compio::time::sleep(Duration::from_millis(1)).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }

    let mut aggregate = AggregateSnapshot {
        at_ms: now_ms,
        ..AggregateSnapshot::default()
    };
    for snapshot in shard_snapshots {
        aggregate.key_count += snapshot.key_count;
        aggregate.expiry_count += snapshot.expiry_count;
        aggregate.store_used_memory += snapshot.used_memory;
        aggregate.total_blocking_keys += snapshot.blocking_keys;
        aggregate.pubsub_channels += snapshot.pubsub_channels;
        aggregate.pubsub_patterns += snapshot.pubsub_patterns;
        aggregate.pubsub_shardchannels += snapshot.pubsub_shardchannels;
    }

    for stats in &state().shard_stats {
        aggregate.connected_clients += stats.connected_clients.load(Ordering::Relaxed);
        aggregate.recent_max_input_buffer = aggregate
            .recent_max_input_buffer
            .max(stats.client_recent_max_input_buffer.load(Ordering::Relaxed));
        aggregate.recent_max_output_buffer = aggregate.recent_max_output_buffer.max(
            stats
                .client_recent_max_output_buffer
                .load(Ordering::Relaxed),
        );
        aggregate.total_connections_received += stats.connections_received.load(Ordering::Relaxed);
        aggregate.total_commands_processed += stats.commands_processed.load(Ordering::Relaxed);
        aggregate.total_net_input_bytes += stats.net_input_bytes.load(Ordering::Relaxed);
        aggregate.total_net_output_bytes += stats.net_output_bytes.load(Ordering::Relaxed);
        aggregate.rejected_connections += stats.rejected_connections.load(Ordering::Relaxed);
        aggregate.expired_keys += stats.expired_keys.load(Ordering::Relaxed);
        aggregate.evicted_keys += stats.evicted_keys.load(Ordering::Relaxed);
        aggregate.keyspace_hits += stats.keyspace_hits.load(Ordering::Relaxed);
        aggregate.keyspace_misses += stats.keyspace_misses.load(Ordering::Relaxed);
    }

    let (ops_per_sec, input_kbps, output_kbps) = instantaneous_rates(
        now_ms,
        aggregate.total_commands_processed,
        aggregate.total_net_input_bytes,
        aggregate.total_net_output_bytes,
    );
    aggregate.instantaneous_ops_per_sec = ops_per_sec;
    aggregate.instantaneous_input_kbps = input_kbps;
    aggregate.instantaneous_output_kbps = output_kbps;
    aggregate
}

pub async fn flush_all_shards_sync() -> Result<(), Vec<u8>> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::Flush {
            reply: Some(reply_tx.clone()),
        });
    }
    drop(reply_tx);
    wait_for_unit_replies(reply_rx, bus.shard_count()).await
}

pub fn flush_all_shards_async() {
    let bus = query_bus();
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::Flush { reply: None });
    }
}

pub async fn memory_usage_for_key(key: &[u8], _samples: usize) -> Option<u64> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::MemoryUsage {
            key: key.to_vec(),
            reply: reply_tx.clone(),
        });
    }
    drop(reply_tx);
    let deadline = std::time::Instant::now() + Duration::from_millis(50);
    while std::time::Instant::now() < deadline {
        match reply_rx.try_recv() {
            Ok(Some(usage)) => return Some(usage),
            Ok(None) => {}
            Err(TryRecvError::Empty) => compio::time::sleep(Duration::from_millis(1)).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    None
}

pub(crate) fn fetch_prob_merge_values_for_key(
    other_than_shard_id: usize,
    key: &[u8],
) -> Vec<ProbMergeValue> {
    let bus = query_bus();
    let expected = bus.shard_count().saturating_sub(1);
    if expected == 0 {
        return Vec::new();
    }
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(expected);
    for shard_id in 0..bus.shard_count() {
        if shard_id == other_than_shard_id {
            continue;
        }
        let _ = bus.sender(shard_id).send(ShardQuery::FetchValue {
            key: key.to_vec(),
            reply: reply_tx.clone(),
        });
    }
    drop(reply_tx);

    let deadline = std::time::Instant::now() + Duration::from_millis(25);
    let mut values = Vec::new();
    let mut replies = 0usize;
    while std::time::Instant::now() < deadline && replies < expected {
        match reply_rx.try_recv() {
            Ok(Some(value)) => {
                replies += 1;
                values.push(value);
            }
            Ok(None) => {
                replies += 1;
            }
            Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    values
}

pub async fn script_load_all(script: Bytes) -> Result<String, Vec<u8>> {
    let replies = run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::ScriptLoad {
                script: script.clone(),
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    let mut iter = replies.into_iter();
    let Some(first) = iter.next() else {
        return Err(error_message("ERR shard coordination timeout"));
    };
    if iter.all(|value| value == first) {
        Ok(first)
    } else {
        Err(error_message(
            "ERR inconsistent script cache state across shards",
        ))
    }
}

pub async fn script_flush_all() -> Result<(), Vec<u8>> {
    run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::ScriptFlush {
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(())
}

pub async fn function_load_all(source: Bytes, replace: bool) -> Result<(), Vec<u8>> {
    run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::FunctionLoad {
                source: source.clone(),
                replace,
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(())
}

pub async fn function_delete_all(library_name: String) -> Result<(), Vec<u8>> {
    run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::FunctionDelete {
                library_name: library_name.clone(),
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(())
}

pub async fn function_flush_all() -> Result<(), Vec<u8>> {
    run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::FunctionFlush {
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(())
}

pub async fn function_restore_all(payload: Bytes, mode: RestoreMode) -> Result<(), Vec<u8>> {
    run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::FunctionRestore {
                payload: payload.clone(),
                mode,
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(())
}

pub async fn kill_running_script() -> Result<bool, Vec<u8>> {
    let replies = run_locked_scripting_query(move |bus| {
        let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
        for shard_id in 0..bus.shard_count() {
            let _ = bus.sender(shard_id).send(ShardQuery::KillScript {
                reply: reply_tx.clone(),
            });
        }
        drop(reply_tx);
        wait_for_result_replies_sync(reply_rx, bus.shard_count())
    })
    .await?;
    Ok(replies.into_iter().any(|value| value))
}

pub fn shard_pubsub_subscribe(
    shard_id: usize,
    channel: Bytes,
    conn_id: u64,
) -> Result<Arc<BroadcastSlot>, Vec<u8>> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(1);
    bus.sender(shard_id)
        .send(ShardQuery::ShardPubSubSubscribe {
            channel,
            conn_id,
            reply: reply_tx,
        })
        .map_err(|_| error_message("ERR shard coordination timeout"))?;
    wait_for_single_sync_reply(reply_rx)
}

pub fn shard_pubsub_unsubscribe(
    shard_id: usize,
    channel: Bytes,
    conn_id: u64,
) -> Result<(), Vec<u8>> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(1);
    bus.sender(shard_id)
        .send(ShardQuery::ShardPubSubUnsubscribe {
            channel,
            conn_id,
            reply: reply_tx,
        })
        .map_err(|_| error_message("ERR shard coordination timeout"))?;
    wait_for_single_sync_reply(reply_rx)
}

pub fn shard_pubsub_publish(
    shard_id: usize,
    channel: Bytes,
    payload: Bytes,
) -> Result<u64, Vec<u8>> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(1);
    bus.sender(shard_id)
        .send(ShardQuery::ShardPubSubPublish {
            channel,
            payload,
            reply: reply_tx,
        })
        .map_err(|_| error_message("ERR shard coordination timeout"))?;
    wait_for_single_sync_reply(reply_rx)
}

pub async fn save_rdb_snapshot(config: &SenkoConfig) -> Result<(), String> {
    let bus = query_bus();
    let (pause_tx, pause_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::Pause {
            reply: pause_tx.clone(),
        });
    }
    drop(pause_tx);
    wait_for_unit_replies(pause_rx, bus.shard_count())
        .await
        .map_err(|error| String::from_utf8_lossy(&error).into_owned())?;

    let export_result = export_rdb_records().await;

    let (resume_tx, resume_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::Resume {
            reply: resume_tx.clone(),
        });
    }
    drop(resume_tx);
    let _ = wait_for_unit_replies(resume_rx, bus.shard_count()).await;

    let records = export_result?;
    let path = config.dir.join(&config.dbfilename);
    let tmp = config
        .dir
        .join(format!("{}.tmp.{}", config.dbfilename, std::process::id()));
    let payload = encode_rdb_snapshot(&records);
    spawn_blocking(move || -> Result<(), String> {
        fs::create_dir_all(path.parent().unwrap_or_else(|| std::path::Path::new(".")))
            .map_err(|error| error.to_string())?;
        fs::write(&tmp, payload).map_err(|error| error.to_string())?;
        fs::rename(&tmp, &path).map_err(|error| error.to_string())
    })
    .await
    .map_err(|_| "snapshot task failed".to_owned())?
}

async fn export_rdb_records() -> Result<Vec<senko_store::ReplicationSnapshotEntry>, String> {
    let bus = query_bus();
    let (reply_tx, reply_rx) = crossfire::compat::mpmc::bounded_blocking(bus.shard_count());
    for shard_id in 0..bus.shard_count() {
        let _ = bus.sender(shard_id).send(ShardQuery::ExportRdb {
            reply: reply_tx.clone(),
        });
    }
    drop(reply_tx);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut out = Vec::new();
    let mut received = 0usize;
    while received < bus.shard_count() && std::time::Instant::now() < deadline {
        match reply_rx.try_recv() {
            Ok(mut shard_records) => {
                received += 1;
                out.append(&mut shard_records);
            }
            Err(TryRecvError::Empty) => compio::time::sleep(Duration::from_millis(1)).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    if received == bus.shard_count() {
        Ok(out)
    } else {
        Err("timed out collecting shard snapshot".to_owned())
    }
}

async fn wait_for_unit_replies(reply_rx: Receiver<()>, expected: usize) -> Result<(), Vec<u8>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut received = 0usize;
    while received < expected && std::time::Instant::now() < deadline {
        match reply_rx.try_recv() {
            Ok(()) => received += 1,
            Err(TryRecvError::Empty) => compio::time::sleep(Duration::from_millis(1)).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    if received == expected {
        Ok(())
    } else {
        Err(error_message("ERR shard coordination timeout"))
    }
}

fn wait_for_result_replies_sync<T: Send + 'static>(
    reply_rx: Receiver<Result<T, String>>,
    expected: usize,
) -> Result<Vec<T>, Vec<u8>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut received = 0usize;
    let mut out = Vec::with_capacity(expected);
    while received < expected && std::time::Instant::now() < deadline {
        match reply_rx.try_recv() {
            Ok(Ok(value)) => {
                received += 1;
                out.push(value);
            }
            Ok(Err(error)) => return Err(error_message(&error)),
            Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    if received == expected {
        Ok(out)
    } else {
        Err(error_message("ERR shard coordination timeout"))
    }
}

async fn run_locked_scripting_query<T, F>(op: F) -> Result<T, Vec<u8>>
where
    T: Send + 'static,
    F: FnOnce(&Arc<ShardQueryBus>) -> Result<T, Vec<u8>> + Send + 'static,
{
    let bus = Arc::clone(query_bus());
    spawn_blocking(move || {
        let _guard = scripting_metadata_lock()
            .lock()
            .expect("scripting metadata lock poisoned");
        op(&bus)
    })
    .await
    .map_err(|_| error_message("ERR scripting metadata task failed"))?
}

fn wait_for_single_sync_reply<T: Send + 'static>(
    reply_rx: Receiver<Result<T, String>>,
) -> Result<T, Vec<u8>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match reply_rx.try_recv() {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => return Err(error_message(&error)),
            Err(TryRecvError::Empty) if std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                return Err(error_message("ERR shard coordination timeout"));
            }
        }
    }
}

fn encode_rdb_snapshot(records: &[senko_store::ReplicationSnapshotEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + records.len() * 32);
    out.extend_from_slice(b"SENKORDB1");
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for record in records {
        out.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
        out.extend_from_slice(record.key.as_bytes());
        out.extend_from_slice(&record.expires_at.unwrap_or(u64::MAX).to_le_bytes());
        out.extend_from_slice(&(record.dump.len() as u32).to_le_bytes());
        out.extend_from_slice(&record.dump);
    }
    out
}

fn instantaneous_rates(
    now_ms: u64,
    commands_processed: u64,
    input_bytes: u64,
    output_bytes: u64,
) -> (u64, f64, f64) {
    let mut state = state().rate_state.lock().expect("rate state lock poisoned");
    let elapsed_ms = now_ms.saturating_sub(state.at_ms).max(1);
    let cmd_delta = commands_processed.saturating_sub(state.commands_processed);
    let in_delta = input_bytes.saturating_sub(state.net_input_bytes);
    let out_delta = output_bytes.saturating_sub(state.net_output_bytes);

    state.ops_per_sec = cmd_delta.saturating_mul(1_000) / elapsed_ms;
    state.input_kbps = (in_delta as f64 * 1_000.0) / elapsed_ms as f64 / 1024.0;
    state.output_kbps = (out_delta as f64 * 1_000.0) / elapsed_ms as f64 / 1024.0;
    state.at_ms = now_ms;
    state.commands_processed = commands_processed;
    state.net_input_bytes = input_bytes;
    state.net_output_bytes = output_bytes;
    (state.ops_per_sec, state.input_kbps, state.output_kbps)
}

fn render_info(
    sections: &[InfoSection],
    aggregate: &AggregateSnapshot,
    cluster_enabled: bool,
) -> String {
    let config = live_config::snapshot();
    let mut out = String::new();
    if sections.is_empty() {
        return out;
    }

    let memory = collect_memory_metrics(aggregate);
    let cpu = read_cpu_metrics();
    let executable = read_executable_path();
    let os = current_os_string();
    let total_system_memory = read_total_system_memory();
    let uptime_seconds = (aggregate.at_ms.saturating_sub(state().startup_time_ms)) / 1_000;
    let last_save_time = state().last_save_time.load(Ordering::Relaxed);
    let last_bgsave_time = state().rdb_last_bgsave_time_sec.load(Ordering::Relaxed);

    for (index, section) in sections.iter().enumerate() {
        if index > 0 {
            out.push_str("\r\n");
        }
        match section {
            InfoSection::Server => {
                out.push_str("# Server\r\n");
                let _ = write!(
                    out,
                    concat!(
                        "redis_version:{}\r\n",
                        "redis_git_sha1:00000000\r\n",
                        "redis_git_dirty:0\r\n",
                        "redis_build_id:{}\r\n",
                        "redis_mode:{}\r\n",
                        "os:{}\r\n",
                        "arch_bits:{}\r\n",
                        "monotonic_clock:{}\r\n",
                        "multiplexing_api:{}\r\n",
                        "atomicvar_api:{}\r\n",
                        "gcc_version:0.0.0\r\n",
                        "process_id:{}\r\n",
                        "run_id:{}\r\n",
                        "tcp_port:{}\r\n",
                        "server_time_usec:{}\r\n",
                        "uptime_in_seconds:{}\r\n",
                        "uptime_in_days:{}\r\n",
                        "hz:{}\r\n",
                        "configured_hz:{}\r\n",
                        "aof_rewrites:0\r\n",
                        "rdb_saves:0\r\n",
                        "rdb_last_bgsave_time_sec:-1\r\n",
                        "executable:{}\r\n",
                        "config_file:\r\n",
                        "io_threads_active:{}\r\n"
                    ),
                    REDIS_VERSION,
                    state().build_id,
                    if cluster_enabled {
                        "cluster"
                    } else {
                        "standalone"
                    },
                    os,
                    usize::BITS,
                    MONOTONIC_CLOCK,
                    MULTIPLEXING_API,
                    ATOMICVAR_API,
                    state().process_id,
                    state().run_id,
                    config.bind_addr.port(),
                    current_unix_us(),
                    uptime_seconds,
                    uptime_seconds / 86_400,
                    config.hz,
                    config.hz,
                    executable,
                    state().num_shards,
                );
            }
            InfoSection::Clients => {
                out.push_str("# Clients\r\n");
                write_stats_section(&mut out, aggregate, false, &config);
            }
            InfoSection::Memory => {
                out.push_str("# Memory\r\n");
                let _ = write!(
                    out,
                    concat!(
                        "used_memory:{}\r\n",
                        "used_memory_human:{}\r\n",
                        "used_memory_rss:{}\r\n",
                        "used_memory_rss_human:{}\r\n",
                        "used_memory_peak:{}\r\n",
                        "used_memory_peak_human:{}\r\n",
                        "used_memory_peak_perc:{:.2}%\r\n",
                        "used_memory_overhead:{}\r\n",
                        "used_memory_startup:{}\r\n",
                        "used_memory_dataset:{}\r\n",
                        "used_memory_dataset_perc:{:.2}%\r\n",
                        "allocator_allocated:{}\r\n",
                        "allocator_active:{}\r\n",
                        "allocator_resident:{}\r\n",
                        "total_system_memory:{}\r\n",
                        "total_system_memory_human:{}\r\n",
                        "used_memory_lua:0\r\n",
                        "used_memory_vm_eval:0\r\n",
                        "used_memory_lua_human:0B\r\n",
                        "used_memory_scripts_eval:0\r\n",
                        "number_of_cached_scripts:0\r\n",
                        "number_of_functions:0\r\n",
                        "number_of_libraries:0\r\n",
                        "used_memory_vm_functions:0\r\n",
                        "used_memory_vm_total:0\r\n",
                        "used_memory_vm_total_human:0B\r\n",
                        "used_memory_functions:0\r\n",
                        "used_memory_scripts:0\r\n",
                        "used_memory_scripts_human:0B\r\n",
                        "maxmemory:{}\r\n",
                        "maxmemory_human:{}\r\n",
                        "maxmemory_policy:{}\r\n",
                        "allocator_frag_ratio:{:.2}\r\n",
                        "allocator_frag_bytes:{}\r\n",
                        "allocator_rss_ratio:{:.2}\r\n",
                        "allocator_rss_bytes:{}\r\n",
                        "rss_overhead_ratio:{:.2}\r\n",
                        "rss_overhead_bytes:{}\r\n",
                        "mem_fragmentation_ratio:{:.2}\r\n",
                        "mem_fragmentation_bytes:{}\r\n",
                        "mem_not_counted_for_evict:0\r\n",
                        "mem_replication_backlog:0\r\n",
                        "mem_total_replication_buffers:0\r\n",
                        "mem_clients_slaves:0\r\n",
                        "mem_clients_normal:0\r\n",
                        "mem_cluster_links:0\r\n",
                        "mem_aof_buffer:0\r\n",
                        "mem_allocator:{}\r\n",
                        "active_defrag_running:0\r\n",
                        "lazyfree_pending_objects:0\r\n",
                        "lazyfreed_objects:0\r\n"
                    ),
                    memory.used_memory,
                    human_bytes(memory.used_memory),
                    memory.used_memory_rss,
                    human_bytes(memory.used_memory_rss),
                    memory.used_memory_peak,
                    human_bytes(memory.used_memory_peak),
                    percentage(memory.used_memory, memory.used_memory_peak),
                    memory.used_memory_overhead,
                    memory.used_memory_startup,
                    memory.used_memory_dataset,
                    percentage(memory.used_memory_dataset, memory.used_memory),
                    memory.allocator_allocated,
                    memory.allocator_active,
                    memory.allocator_resident,
                    total_system_memory,
                    human_bytes(total_system_memory),
                    config.max_memory.unwrap_or(0) as u64,
                    human_bytes(config.max_memory.unwrap_or(0) as u64),
                    config.maxmemory_policy,
                    ratio(memory.allocator_active, memory.allocator_allocated),
                    memory.allocator_frag_bytes,
                    ratio(memory.allocator_resident, memory.allocator_active),
                    memory.allocator_rss_bytes,
                    ratio(memory.used_memory_rss, memory.used_memory),
                    memory.rss_overhead_bytes,
                    ratio(memory.used_memory_rss, memory.used_memory_dataset),
                    memory.mem_fragmentation_bytes,
                    MEM_ALLOCATOR,
                );
            }
            InfoSection::Persistence => {
                out.push_str("# Persistence\r\n");
                let _ = write!(
                    out,
                    concat!(
                        "loading:0\r\n",
                        "async_loading:0\r\n",
                        "current_cow_peak:0\r\n",
                        "current_cow_size:0\r\n",
                        "current_cow_size_age:0\r\n",
                        "current_fork_perc:0.00\r\n",
                        "current_save_keys_processed:0\r\n",
                        "current_save_keys_total:0\r\n",
                        "rdb_changes_since_last_save:{}\r\n",
                        "rdb_bgsave_in_progress:{}\r\n",
                        "rdb_last_save_time:{}\r\n",
                        "rdb_last_bgsave_status:{}\r\n",
                        "rdb_last_bgsave_time_sec:{}\r\n",
                        "rdb_current_bgsave_time_sec:-1\r\n",
                        "rdb_saves:0\r\n",
                        "rdb_last_cow_size:0\r\n",
                        "aof_enabled:0\r\n",
                        "aof_rewrite_in_progress:0\r\n",
                        "aof_rewrite_scheduled:{}\r\n",
                        "aof_last_rewrite_time_sec:-1\r\n",
                        "aof_current_rewrite_time_sec:-1\r\n",
                        "aof_last_bgrewrite_status:{}\r\n",
                        "aof_last_write_status:ok\r\n",
                        "aof_last_cow_size:0\r\n",
                        "module_fork_in_progress:0\r\n",
                        "module_fork_last_cow_size:0\r\n"
                    ),
                    aggregate.total_commands_processed,
                    if state().rdb_bgsave_in_progress.load(Ordering::Relaxed) {
                        1
                    } else {
                        0
                    },
                    last_save_time,
                    if state().rdb_last_bgsave_ok.load(Ordering::Relaxed) {
                        "ok"
                    } else {
                        "err"
                    },
                    if last_bgsave_time == u64::MAX {
                        -1
                    } else {
                        last_bgsave_time as i64
                    },
                    if state().bgsave_scheduled.load(Ordering::Relaxed) {
                        1
                    } else {
                        0
                    },
                    if state().aof_last_bgrewrite_ok.load(Ordering::Relaxed) {
                        "ok"
                    } else {
                        "err"
                    },
                );
            }
            InfoSection::Stats => {
                out.push_str("# Stats\r\n");
                write_stats_section(&mut out, aggregate, true, &config);
            }
            InfoSection::Replication => {
                out.push_str("# Replication\r\n");
                let role = replication_role();
                let replid = current_replication_id();
                let _ = write!(
                    out,
                    "role:{}\r\n",
                    if role == ReplicationRole::Primary {
                        "master"
                    } else {
                        "slave"
                    }
                );
                if role == ReplicationRole::Replica {
                    let host = state()
                        .replica_primary_host
                        .lock()
                        .expect("replica primary host lock poisoned")
                        .clone()
                        .unwrap_or_else(|| "?".to_owned());
                    let port = state().replica_primary_port.load(Ordering::Relaxed);
                    let _ = write!(
                        out,
                        concat!(
                            "master_host:{}\r\n",
                            "master_port:{}\r\n",
                            "master_link_status:down\r\n",
                            "master_last_io_seconds_ago:-1\r\n",
                            "master_sync_in_progress:0\r\n",
                            "slave_read_repl_offset:0\r\n",
                            "slave_repl_offset:0\r\n",
                            "slave_priority:100\r\n",
                            "slave_read_only:1\r\n",
                            "replica_announced:1\r\n",
                        ),
                        host, port,
                    );
                }
                let _ = write!(
                    out,
                    concat!(
                        "connected_slaves:0\r\n",
                        "master_failover_state:no-failover\r\n",
                        "master_replid:{}\r\n",
                        "master_replid2:0000000000000000000000000000000000000000\r\n",
                        "master_repl_offset:0\r\n",
                        "second_repl_offset:-1\r\n",
                        "repl_backlog_active:0\r\n",
                        "repl_backlog_size:{}\r\n",
                        "repl_backlog_first_byte_offset:0\r\n",
                        "repl_backlog_histlen:0\r\n"
                    ),
                    replid,
                    crate::cluster::replication::DEFAULT_REPL_BACKLOG_SIZE,
                );
            }
            InfoSection::Cpu => {
                out.push_str("# CPU\r\n");
                let _ = write!(
                    out,
                    concat!(
                        "used_cpu_sys:{:.2}\r\n",
                        "used_cpu_user:{:.2}\r\n",
                        "used_cpu_sys_children:{:.2}\r\n",
                        "used_cpu_user_children:{:.2}\r\n",
                        "used_cpu_sys_main_thread:{:.2}\r\n",
                        "used_cpu_user_main_thread:{:.2}\r\n"
                    ),
                    cpu.sys, cpu.user, cpu.sys_children, cpu.user_children, cpu.sys, cpu.user,
                );
            }
            InfoSection::Modules => {
                out.push_str("# Modules\r\n");
                out.push_str(&crate::modules::info_section());
            }
            InfoSection::Commandstats => {
                out.push_str("# Commandstats\r\n");
            }
            InfoSection::Errorstats => {
                out.push_str("# Errorstats\r\n");
                let mut errors = state()
                    .error_counts
                    .lock()
                    .expect("error counter lock poisoned")
                    .iter()
                    .map(|(name, count)| (name.clone(), *count))
                    .collect::<Vec<_>>();
                errors.sort_by(|left, right| left.0.cmp(&right.0));
                for (name, count) in errors {
                    let _ = write!(out, "errorstat_{name}:count={count}\r\n");
                }
            }
            InfoSection::Cluster => {
                out.push_str("# Cluster\r\n");
                let _ = write!(
                    out,
                    "cluster_enabled:{}\r\n",
                    if cluster_enabled { 1 } else { 0 }
                );
            }
            InfoSection::Keyspace => {
                out.push_str("# Keyspace\r\n");
                if aggregate.key_count > 0 {
                    let _ = write!(
                        out,
                        "db0:keys={},expires={},avg_ttl=0\r\n",
                        aggregate.key_count, aggregate.expiry_count,
                    );
                }
            }
        }
    }
    out
}

fn write_stats_section(
    out: &mut String,
    aggregate: &AggregateSnapshot,
    _include_connected: bool,
    config: &SenkoConfig,
) {
    let _ = write!(
        out,
        concat!(
            "connected_clients:{}\r\n",
            "cluster_connections:0\r\n",
            "maxclients:{}\r\n",
            "client_recent_max_input_buffer:{}\r\n",
            "client_recent_max_output_buffer:{}\r\n",
            "total_connections_received:{}\r\n",
            "total_commands_processed:{}\r\n",
            "instantaneous_ops_per_sec:{}\r\n",
            "total_net_input_bytes:{}\r\n",
            "total_net_output_bytes:{}\r\n",
            "total_net_repl_input_bytes:0\r\n",
            "total_net_repl_output_bytes:0\r\n",
            "instantaneous_input_kbps:{:.2}\r\n",
            "instantaneous_output_kbps:{:.2}\r\n",
            "rejected_connections:{}\r\n",
            "sync_full:0\r\n",
            "sync_partial_ok:0\r\n",
            "sync_partial_err:0\r\n",
            "expired_keys:{}\r\n",
            "expired_stale_perc:0.00\r\n",
            "expired_time_cap_reached_count:0\r\n",
            "expire_cycle_cpu_milliseconds:0\r\n",
            "evicted_keys:{}\r\n",
            "evicted_clients:0\r\n",
            "total_eviction_exceeded_time:0\r\n",
            "current_eviction_exceeded_time:0\r\n",
            "keyspace_hits:{}\r\n",
            "keyspace_misses:{}\r\n",
            "pubsub_channels:{}\r\n",
            "pubsub_patterns:{}\r\n",
            "pubsub_shardchannels:{}\r\n",
            "latest_fork_usec:0\r\n",
            "total_forks:0\r\n",
            "migrate_cached_sockets:0\r\n",
            "slave_expires_tracked_keys:0\r\n",
            "active_defrag_running:0\r\n",
            "tracking_clients:0\r\n",
            "tracking_table_max_keys:0\r\n",
            "total_blocking_keys:{}\r\n",
            "total_blocking_keys_on_nokey:0\r\n"
        ),
        aggregate.connected_clients,
        config.max_connections,
        aggregate.recent_max_input_buffer,
        aggregate.recent_max_output_buffer,
        aggregate.total_connections_received,
        aggregate.total_commands_processed,
        aggregate.instantaneous_ops_per_sec,
        aggregate.total_net_input_bytes,
        aggregate.total_net_output_bytes,
        aggregate.instantaneous_input_kbps,
        aggregate.instantaneous_output_kbps,
        aggregate.rejected_connections,
        aggregate.expired_keys,
        aggregate.evicted_keys,
        aggregate.keyspace_hits,
        aggregate.keyspace_misses,
        aggregate.pubsub_channels,
        aggregate.pubsub_patterns,
        aggregate.pubsub_shardchannels,
        aggregate.total_blocking_keys,
    );
}

#[derive(Debug, Clone, Copy)]
struct MemoryMetrics {
    used_memory: u64,
    used_memory_rss: u64,
    used_memory_peak: u64,
    used_memory_overhead: u64,
    used_memory_startup: u64,
    used_memory_dataset: u64,
    allocator_allocated: u64,
    allocator_active: u64,
    allocator_resident: u64,
    allocator_frag_bytes: u64,
    allocator_rss_bytes: u64,
    rss_overhead_bytes: u64,
    mem_fragmentation_bytes: u64,
}

fn collect_memory_metrics(aggregate: &AggregateSnapshot) -> MemoryMetrics {
    let rss = read_process_rss().max(1);
    let used_memory = rss.max(aggregate.store_used_memory).max(1);
    let peak = update_peak_memory(used_memory);
    let startup = state().startup_memory.max(1);
    let dataset = aggregate.store_used_memory;
    let overhead = used_memory.saturating_sub(dataset);
    let allocator_allocated = dataset;
    let allocator_active = used_memory;
    let allocator_resident = rss;
    let allocator_frag_bytes = allocator_active.saturating_sub(allocator_allocated);
    let allocator_rss_bytes = allocator_resident.saturating_sub(allocator_active);
    let rss_overhead_bytes = rss.saturating_sub(used_memory);
    let mem_fragmentation_bytes = rss.saturating_sub(dataset);
    MemoryMetrics {
        used_memory,
        used_memory_rss: rss,
        used_memory_peak: peak,
        used_memory_overhead: overhead,
        used_memory_startup: startup,
        used_memory_dataset: dataset,
        allocator_allocated,
        allocator_active,
        allocator_resident,
        allocator_frag_bytes,
        allocator_rss_bytes,
        rss_overhead_bytes,
        mem_fragmentation_bytes,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CpuMetrics {
    sys: f64,
    user: f64,
    sys_children: f64,
    user_children: f64,
}

fn read_cpu_metrics() -> CpuMetrics {
    let Ok(stat) = fs::read_to_string("/proc/self/stat") else {
        return CpuMetrics::default();
    };
    let Some(end_comm) = stat.rfind(") ") else {
        return CpuMetrics::default();
    };
    let fields = stat[end_comm + 2..].split_whitespace().collect::<Vec<_>>();
    if fields.len() < 15 {
        return CpuMetrics::default();
    }
    let user = fields
        .get(11)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let sys = fields
        .get(12)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let user_children = fields
        .get(13)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let sys_children = fields
        .get(14)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    CpuMetrics {
        sys: sys as f64 / PROC_JIFFY_HZ,
        user: user as f64 / PROC_JIFFY_HZ,
        sys_children: sys_children as f64 / PROC_JIFFY_HZ,
        user_children: user_children as f64 / PROC_JIFFY_HZ,
    }
}

fn read_process_rss() -> u64 {
    read_proc_status_kb("VmRSS").saturating_mul(1024)
}

fn read_total_system_memory() -> u64 {
    read_proc_meminfo_kb("MemTotal").saturating_mul(1024)
}

fn read_proc_status_kb(name: &str) -> u64 {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(value) = parse_proc_kb_line(line, name) {
            return value;
        }
    }
    0
}

fn read_proc_meminfo_kb(name: &str) -> u64 {
    let Ok(meminfo) = fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in meminfo.lines() {
        if let Some(value) = parse_proc_kb_line(line, name) {
            return value;
        }
    }
    0
}

fn parse_proc_kb_line(line: &str, name: &str) -> Option<u64> {
    let prefix = format!("{name}:");
    let value = line.strip_prefix(&prefix)?.trim();
    value
        .split_whitespace()
        .next()
        .and_then(|part| part.parse::<u64>().ok())
}

fn read_executable_path() -> String {
    fs::read_link("/proc/self/exe")
        .unwrap_or_else(|_| PathBuf::from("/proc/self/exe"))
        .display()
        .to_string()
}

fn current_os_string() -> String {
    let name = match std::env::consts::OS {
        "linux" => "Linux",
        "macos" => "Darwin",
        "windows" => "Windows",
        other => other,
    };
    let release = fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    format!("{name} {release} {}", std::env::consts::ARCH)
}

fn update_peak_memory(current: u64) -> u64 {
    let peak = &state().peak_memory;
    observe_max(peak, current);
    peak.load(Ordering::Relaxed)
}

fn observe_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn default_sections(include_everything: bool) -> Vec<InfoSection> {
    let sections = vec![
        InfoSection::Server,
        InfoSection::Clients,
        InfoSection::Memory,
        InfoSection::Persistence,
        InfoSection::Stats,
        InfoSection::Replication,
        InfoSection::Cpu,
        InfoSection::Modules,
        InfoSection::Commandstats,
        InfoSection::Errorstats,
        InfoSection::Cluster,
        InfoSection::Keyspace,
    ];
    if !include_everything {
        return sections;
    }
    sections
}

fn all_sections() -> Vec<InfoSection> {
    vec![
        InfoSection::Server,
        InfoSection::Clients,
        InfoSection::Memory,
        InfoSection::Persistence,
        InfoSection::Stats,
        InfoSection::Replication,
        InfoSection::Cpu,
        InfoSection::Errorstats,
        InfoSection::Cluster,
        InfoSection::Keyspace,
    ]
}

fn extend_unique(out: &mut Vec<InfoSection>, items: &[InfoSection]) {
    for item in items {
        if !out.contains(item) {
            out.push(*item);
        }
    }
}

fn render_lolwut(version: u8, pkg_version: &str, num_shards: usize) -> String {
    let art = match version {
        2 => "    /\\\n   / /\n  / / /\n /_/ /_\n   /_/\n",
        3 => {
            "  .-\\\n /   \\\n/  /\\ \\\n\\  \\/ /
 \\   /
  `-'
"
        }
        4 => {
            "      /\\\n  /\\ /  \\\n /  \\    \\\n/ /\\ \\    \\\n\\ \\/ /   /
 \\  /___/
  \\/
"
        }
        5 => "\\        /\n \\      / /\n  \\    / / /\n   \\  / / /\n    \\/_/_/\n      /_/\n",
        _ => "    /\\\n   /  \\\n  / /\\ \\\n / /  \\ \\\n/_/    \\_\\\n",
    };
    format!(
        "{art}\nthreads: {num_shards}\n\nSenko ver. {pkg_version}, a flash of light in the darkness.\n"
    )
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "K", "M", "G", "T", "P"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}{}", bytes, UNITS[unit])
    } else {
        format!("{value:.2}{}", UNITS[unit])
    }
}

fn percentage(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        1.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn bulk_response(bytes: Vec<u8>) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::from(bytes))))
}

fn outcome(response: Vec<u8>) -> ServerCommandOutcome {
    ServerCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn current_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use redis::InfoDict;

    use super::{
        AggregateSnapshot, InfoSection, NodeId, REDIS_VERSION, ServerState, ShardStats,
        current_unix_us, default_sections, parse_info_sections, ratio, render_info, render_lolwut,
        state,
    };

    fn ensure_state() {
        if super::SERVER_STATE.get().is_none() {
            let _ = super::SERVER_STATE.set(std::sync::Arc::new(ServerState {
                build_id: "deadbeefcafebabe".to_owned(),
                run_id: NodeId::generate().to_string(),
                replid: std::sync::Mutex::new(NodeId::generate().to_string()),
                replication_role: std::sync::atomic::AtomicU64::new(
                    super::ReplicationRole::Primary as u64,
                ),
                replica_primary_host: std::sync::Mutex::new(None),
                replica_primary_port: std::sync::atomic::AtomicU64::new(0),
                startup_time_ms: 1,
                process_id: 42,
                startup_memory: 1024,
                peak_memory: std::sync::atomic::AtomicU64::new(1024),
                last_save_time: std::sync::atomic::AtomicU64::new(1),
                rdb_bgsave_in_progress: std::sync::atomic::AtomicBool::new(false),
                rdb_last_bgsave_ok: std::sync::atomic::AtomicBool::new(true),
                rdb_last_bgsave_time_sec: std::sync::atomic::AtomicU64::new(u64::MAX),
                aof_last_bgrewrite_ok: std::sync::atomic::AtomicBool::new(true),
                bgsave_scheduled: std::sync::atomic::AtomicBool::new(false),
                shard_stats: vec![ShardStats::default()].into_boxed_slice(),
                error_counts: std::sync::Mutex::new(hashbrown::HashMap::with_hasher(
                    ahash::RandomState::new(),
                )),
                aggregate_cache: std::sync::Mutex::new(None),
                rate_state: std::sync::Mutex::new(super::RateState::default()),
                num_shards: 1,
            }));
        }
    }

    #[test]
    fn info_default_sections_include_expected_headers() {
        ensure_state();
        let aggregate = AggregateSnapshot {
            at_ms: 2_000,
            key_count: 1,
            expiry_count: 0,
            store_used_memory: 4_096,
            connected_clients: 1,
            total_commands_processed: 100,
            total_connections_received: 1,
            ..AggregateSnapshot::default()
        };
        let rendered = render_info(&default_sections(true), &aggregate, false);
        let info = InfoDict::new(&rendered);
        assert_eq!(
            info.get::<String>("redis_version"),
            Some(REDIS_VERSION.into())
        );
        assert_eq!(info.get::<String>("role"), Some("master".into()));
        assert_eq!(info.get::<String>("cluster_enabled"), Some("0".into()));
        assert!(rendered.contains("# Server\r\n"));
        assert!(rendered.contains("# Memory\r\n"));
        assert!(rendered.contains("db0:keys=1,expires=0,avg_ttl=0\r\n"));
    }

    #[test]
    fn info_server_only_renders_single_section() {
        ensure_state();
        let aggregate = AggregateSnapshot {
            at_ms: 2_000,
            ..AggregateSnapshot::default()
        };
        let rendered = render_info(&[InfoSection::Server], &aggregate, false);
        assert!(rendered.starts_with("# Server\r\n"));
        assert!(!rendered.contains("# Memory\r\n"));
        assert!(rendered.contains("redis_version:8.0.0\r\n"));
    }

    #[test]
    fn keyspace_line_is_absent_when_no_keys_exist() {
        ensure_state();
        let aggregate = AggregateSnapshot::default();
        let rendered = render_info(&[InfoSection::Keyspace], &aggregate, false);
        assert!(rendered.starts_with("# Keyspace\r\n"));
        assert!(!rendered.contains("db0:"));
    }

    #[test]
    fn info_section_filter_keeps_request_order() {
        let sections = parse_info_sections(&[
            senko_proto::Frame::BulkString(b"memory"),
            senko_proto::Frame::BulkString(b"keyspace"),
        ])
        .unwrap();
        assert_eq!(sections, vec![InfoSection::Memory, InfoSection::Keyspace]);
    }

    #[test]
    fn lolwut_footer_matches_expected_phrase() {
        let rendered = render_lolwut(3, "0.1.0", 4);
        assert!(rendered.ends_with("\nSenko ver. 0.1.0, a flash of light in the darkness.\n"));
    }

    #[test]
    fn peak_memory_updates_monotonically() {
        ensure_state();
        state().peak_memory.store(128, Ordering::Relaxed);
        let after_first = super::update_peak_memory(512);
        let after_second = super::update_peak_memory(64);
        assert!(after_first >= 512);
        assert!(after_second >= after_first);
    }

    #[test]
    fn helper_ratios_do_not_panic_on_zero() {
        assert_eq!(ratio(1, 0), 1.0);
    }

    #[test]
    fn current_time_uses_microseconds() {
        let now = current_unix_us();
        assert!(now > 0);
    }
}
