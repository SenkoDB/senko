#![deny(unsafe_code)]

pub mod cli;
pub mod client;
pub mod commands;
pub mod conf_parser;
pub mod conf_writer;
pub mod config;
pub mod detector;
pub mod election;
pub mod failover;
pub mod gossip;
pub mod monitor;
pub mod notify;
pub mod runid;
pub mod server;
pub mod state;

use std::{
    cell::RefCell,
    net::{SocketAddr, ToSocketAddrs},
    path::Path,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::HashMap;
use senko_core::{SenkoError, SenkoResult};

use crate::{
    config::SentinelConfig,
    election::ElectionBook,
    monitor::MonitorEngine,
    notify::Notifier,
    state::{
        FailoverState, InstanceFlags, MasterState, Role, SentinelId, SentinelWorld, WorldSnapshot,
        new_world,
    },
};

pub struct SentinelStats {
    pub total_connections_received: AtomicU64,
    pub total_commands_processed: AtomicU64,
    pub total_net_input_bytes: AtomicU64,
    pub total_net_output_bytes: AtomicU64,
}

impl Default for SentinelStats {
    fn default() -> Self {
        Self {
            total_connections_received: AtomicU64::new(0),
            total_commands_processed: AtomicU64::new(0),
            total_net_input_bytes: AtomicU64::new(0),
            total_net_output_bytes: AtomicU64::new(0),
        }
    }
}

pub struct SentinelRuntime {
    pub world: SentinelWorld,
    pub config: SentinelConfig,
    pub monitor: MonitorEngine,
    pub notifier: Notifier,
    pub elections: ElectionBook,
    pub stats: SentinelStats,
    pub started_at_ms: u64,
    pub connected_clients: usize,
}

impl SentinelRuntime {
    pub fn new(mut config: SentinelConfig) -> SenkoResult<Self> {
        config.validate().map_err(config_error)?;
        if config.config_file.is_none() {
            config.config_file = Some(Path::new("sentinel.toml").to_path_buf());
        }
        let my_id = config.load_or_create_id().map_err(config_error)?;
        config.runtime.myid = Some(my_id.to_string());
        let now = current_unix_ms();
        let mut masters = HashMap::with_hasher(RandomState::new());
        for master in &config.masters {
            let (host, port) = config.effective_master_addr(master);
            let config_epoch = config
                .runtime
                .masters
                .get(&master.name)
                .map(|state| state.config_epoch)
                .unwrap_or(0);
            masters.insert(
                master.name.clone(),
                MasterState {
                    name: master.name.clone(),
                    addr: resolve_master_addr(host, port)?,
                    quorum: master.quorum,
                    flags: InstanceFlags::MASTER,
                    config_epoch,
                    leader: None,
                    leader_epoch: 0,
                    replicas: HashMap::with_hasher(RandomState::new()),
                    sentinels: HashMap::with_hasher(RandomState::new()),
                    last_ping_sent: 0,
                    last_ok_ping: now,
                    down_since: None,
                    failover_state: FailoverState::None,
                    failover_epoch: 0,
                    selected_replica: None,
                    role_reported: Role::Master,
                    info_refresh: 0,
                    link_pending_commands: 0,
                    link_refcount: 0,
                    cached_info: Vec::new(),
                },
            );
        }
        let snapshot = WorldSnapshot {
            epoch: 0,
            my_id: my_id.clone(),
            masters,
            timestamp: now,
        };
        let world = new_world(snapshot);
        let mut monitor = MonitorEngine::new(config.sentinel_hz() as u64);
        for master in world.load().masters.values() {
            monitor.register_master(&master.name, master.addr);
        }
        Ok(Self {
            world,
            config,
            monitor,
            notifier: Notifier::default(),
            elections: ElectionBook::default(),
            stats: SentinelStats::default(),
            started_at_ms: now,
            connected_clients: 0,
        })
    }

    pub fn snapshot(&self) -> Arc<WorldSnapshot> {
        self.world.load_full()
    }

    pub fn my_id(&self) -> SentinelId {
        self.snapshot().my_id.clone()
    }

    pub fn down_after_ms(&self, master_name: &str) -> u64 {
        self.config.down_after_milliseconds(master_name)
    }

    pub fn failover_timeout(&self, master_name: &str) -> u64 {
        self.config.failover_timeout(master_name)
    }

    pub fn parallel_syncs(&self, master_name: &str) -> u32 {
        self.config.parallel_syncs(master_name)
    }

    pub fn record_command(&self) {
        self.stats
            .total_commands_processed
            .fetch_add(1, Ordering::Relaxed);
    }
}

pub type SharedRuntime = Rc<RefCell<SentinelRuntime>>;

pub async fn run(config: SentinelConfig) -> SenkoResult<()> {
    let runtime = Rc::new(RefCell::new(SentinelRuntime::new(config)?));
    server::run(runtime).await
}

pub async fn run_from_path(path: impl AsRef<Path>) -> SenkoResult<()> {
    run(config::load_config(path.as_ref()).map_err(config_error)?).await
}

#[inline]
pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[inline]
pub fn current_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub fn format_addr(addr: SocketAddr) -> CompactString {
    CompactString::from(addr.to_string())
}

fn resolve_master_addr(host: &str, port: u16) -> SenkoResult<SocketAddr> {
    if let Ok(ip) = host.parse() {
        return Ok(SocketAddr::new(ip, port));
    }
    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or(SenkoError::Protocol("master address did not resolve"))
}

fn config_error(error: crate::config::ConfigError) -> SenkoError {
    match error {
        crate::config::ConfigError::Io(error) => SenkoError::Io(error),
        other => SenkoError::ProtocolMessage(other.to_string().into()),
    }
}
