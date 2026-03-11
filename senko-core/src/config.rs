use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use phf::phf_map;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use toml::Value;

const INCLUDE_MAX_DEPTH: usize = 8;
const KNOWN_PLUGINS: &[&str] = &["json", "bloom", "search", "ts", "vector"];

#[derive(Debug)]
pub enum ConfigError {
    IoError(io::Error),
    ParseError(toml::de::Error),
    ValidationError(String),
    IncludeError {
        path: PathBuf,
        source: Box<ConfigError>,
    },
    ConflictError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IoError(error) => write!(f, "io error: {error}"),
            Self::ParseError(error) => write!(f, "parse error: {error}"),
            Self::ValidationError(message) => f.write_str(message),
            Self::IncludeError { path, source } => {
                write!(f, "include error in {}: {source}", path.display())
            }
            Self::ConflictError(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(error) => Some(error),
            Self::ParseError(error) => Some(error),
            Self::IncludeError { source, .. } => Some(source.as_ref()),
            Self::ValidationError(_) | Self::ConflictError(_) => None,
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(value: toml::de::Error) -> Self {
        Self::ParseError(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for ByteSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ByteSizeVisitor;

        impl<'de> serde::de::Visitor<'de> for ByteSizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an integer byte count or a string with units")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ByteSize(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                u64::try_from(value)
                    .map(ByteSize)
                    .map_err(|_| E::custom("byte size must be non-negative"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ByteSize::from_str(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(ByteSizeVisitor)
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("byte size cannot be empty".to_owned());
        }
        if let Ok(raw) = trimmed.parse::<u64>() {
            return Ok(Self(raw));
        }

        let lower = trimmed.to_ascii_lowercase();
        let split = lower
            .find(|ch: char| !ch.is_ascii_digit())
            .ok_or_else(|| "invalid byte size".to_owned())?;
        let (number, suffix) = lower.split_at(split);
        if number.is_empty() || suffix.is_empty() {
            return Err(format!("invalid byte size: {value}"));
        }
        let quantity = number
            .parse::<u64>()
            .map_err(|_| format!("invalid byte size: {value}"))?;
        let factor = match suffix {
            "b" => 1,
            "kb" => 1024,
            "mb" => 1024_u64.pow(2),
            "gb" => 1024_u64.pow(3),
            "k" => 1000,
            "m" => 1000_u64.pow(2),
            "g" => 1000_u64.pow(3),
            _ => return Err(format!("unsupported byte size suffix: {suffix}")),
        };
        quantity
            .checked_mul(factor)
            .map(Self)
            .ok_or_else(|| "byte size overflow".to_owned())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsAuthClients {
    #[default]
    Yes,
    No,
    Optional,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsAuthField {
    #[default]
    Off,
    CN,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Verbose,
    #[default]
    Notice,
    Warning,
    Nothing,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OomScoreAdj {
    #[default]
    No,
    Yes,
    Relative,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AclPubSubDefault {
    Allchannels,
    #[default]
    Resetchannels,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProtectedConfigAccess {
    Yes,
    #[default]
    No,
    Local,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClientClass {
    #[default]
    Normal,
    Replica,
    PubSub,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SanitizePayload {
    No,
    #[default]
    Clients,
    Yes,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DisklessLoad {
    #[default]
    Disabled,
    WhenDbEmpty,
    SwapDb,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PropagationErrorBehavior {
    #[default]
    Ignore,
    Panic,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EndpointType {
    #[default]
    Ip,
    Hostname,
    UnknownEndpoint,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatsEnabled {
    #[default]
    No,
    Yes,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum MaxMemoryPolicy {
    VolatileLru,
    AllkeysLru,
    VolatileLfu,
    AllkeysLfu,
    VolatileLrm,
    AllkeysLrm,
    VolatileRandom,
    AllkeysRandom,
    VolatileTtl,
    #[default]
    NoEviction,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MaxMemoryClients {
    Disabled,
    Bytes(ByteSize),
    Percentage(f64),
}

impl Default for MaxMemoryClients {
    fn default() -> Self {
        Self::Disabled
    }
}

impl Serialize for MaxMemoryClients {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Disabled => serializer.serialize_u64(0),
            Self::Bytes(value) => value.serialize(serializer),
            Self::Percentage(value) => serializer.serialize_f64(*value),
        }
    }
}

impl<'de> Deserialize<'de> for MaxMemoryClients {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = MaxMemoryClients;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("0, a byte size, or a percentage")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if value == 0 {
                    MaxMemoryClients::Disabled
                } else {
                    MaxMemoryClients::Bytes(ByteSize(value))
                })
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(
                    u64::try_from(value).map_err(|_| E::custom("value must be non-negative"))?,
                )
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if value == 0.0 {
                    MaxMemoryClients::Disabled
                } else {
                    MaxMemoryClients::Percentage(value)
                })
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let trimmed = value.trim();
                if let Some(percent) = trimmed.strip_suffix('%') {
                    let parsed = percent
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| E::custom("invalid percentage"))?;
                    return Ok(if parsed == 0.0 {
                        MaxMemoryClients::Disabled
                    } else {
                        MaxMemoryClients::Percentage(parsed)
                    });
                }
                let bytes = ByteSize::from_str(trimmed).map_err(E::custom)?;
                Ok(if bytes.0 == 0 {
                    MaxMemoryClients::Disabled
                } else {
                    MaxMemoryClients::Bytes(bytes)
                })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AppendFsync {
    Always,
    #[default]
    EverySec,
    No,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ShutdownAction {
    Save,
    Nosave,
    #[default]
    Default,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct ClientOutputBufferLimit {
    pub class: ClientClass,
    pub hard_limit: ByteSize,
    pub soft_limit: ByteSize,
    pub soft_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct SavePoint {
    pub seconds: u64,
    pub changes: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReplicaOf {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SenkoConfig {
    pub network: NetworkConfig,
    pub tls: TlsConfig,
    pub general: GeneralConfig,
    pub security: SecurityConfig,
    pub persistence: PersistenceConfig,
    pub replication: ReplicationConfig,
    pub cluster: ClusterConfig,
    pub memory: MemoryConfig,
    pub eviction: EvictionConfig,
    pub encoding: EncodingConfig,
    pub pubsub: PubSubConfig,
    pub slowlog: SlowlogConfig,
    pub latency: LatencyConfig,
    pub clients: ClientsConfig,
    pub lazyfree: LazyFreeConfig,
    pub aof: AofConfig,
    pub plugins: PluginConfig,
    #[serde(skip)]
    pub bind_addr: SocketAddr,
    #[serde(skip)]
    pub config_file: Option<PathBuf>,
    #[serde(skip)]
    pub num_shards: usize,
    #[serde(skip)]
    pub max_connections: usize,
    #[serde(skip)]
    pub max_memory: Option<usize>,
    #[serde(skip)]
    pub auth_password: Option<String>,
    #[serde(skip)]
    pub aclfile: Option<PathBuf>,
    #[serde(skip)]
    pub unixsocket: Option<PathBuf>,
    #[serde(skip)]
    pub unixsocketperm: u32,
    #[serde(skip)]
    pub timeout: u64,
    #[serde(skip)]
    pub loglevel: String,
    #[serde(skip)]
    pub logfile: String,
    #[serde(skip)]
    pub syslog_enabled: bool,
    #[serde(skip)]
    pub syslog_ident: String,
    #[serde(skip)]
    pub syslog_facility: String,
    #[serde(skip)]
    pub databases: u64,
    #[serde(skip)]
    pub maxmemory_policy: String,
    #[serde(skip)]
    pub maxmemory_samples: u64,
    #[serde(skip)]
    pub maxmemory_eviction_tenacity: u64,
    #[serde(skip)]
    pub tcp_backlog: u32,
    #[serde(skip)]
    pub tcp_nodelay: bool,
    #[serde(skip)]
    pub tcp_keepalive: u64,
    #[serde(skip)]
    pub acllog_max_len: usize,
    #[serde(skip)]
    pub hz: u64,
    #[serde(skip)]
    pub dynamic_hz: bool,
    #[serde(skip)]
    pub aof_use_rdb_preamble: bool,
    #[serde(skip)]
    pub appendonly: bool,
    #[serde(skip)]
    pub appendfilename: String,
    #[serde(skip)]
    pub appendfsync: String,
    #[serde(skip)]
    pub no_appendfsync_on_rewrite: bool,
    #[serde(skip)]
    pub auto_aof_rewrite_percentage: u64,
    #[serde(skip)]
    pub auto_aof_rewrite_min_size: u64,
    #[serde(skip)]
    pub save: String,
    #[serde(skip)]
    pub rdbcompression: bool,
    #[serde(skip)]
    pub rdbchecksum: bool,
    #[serde(skip)]
    pub dbfilename: String,
    #[serde(skip)]
    pub dir: PathBuf,
    #[serde(skip)]
    pub repl_backlog_size: u64,
    #[serde(skip)]
    pub repl_backlog_ttl: u64,
    #[serde(skip)]
    pub replica_serve_stale_data: bool,
    #[serde(skip)]
    pub replica_read_only: bool,
    #[serde(skip)]
    pub replica_lazy_flush: bool,
    #[serde(skip)]
    pub slowlog_log_slower_than: i64,
    #[serde(skip)]
    pub slowlog_max_len: usize,
    #[serde(skip)]
    pub latency_monitor_threshold: i64,
    #[serde(skip)]
    pub lazyfree_lazy_eviction: bool,
    #[serde(skip)]
    pub lazyfree_lazy_expire: bool,
    #[serde(skip)]
    pub lazyfree_lazy_server_del: bool,
    #[serde(skip)]
    pub activerehashing: bool,
    #[serde(skip)]
    pub list_max_listpack_size: i64,
    #[serde(skip)]
    pub list_compress_depth: u64,
    #[serde(skip)]
    pub hash_max_listpack_entries: u64,
    #[serde(skip)]
    pub hash_max_listpack_value: u64,
    #[serde(skip)]
    pub set_max_intset_entries: u64,
    #[serde(skip)]
    pub set_max_listpack_entries: u64,
    #[serde(skip)]
    pub set_max_listpack_value: u64,
    #[serde(skip)]
    pub zset_max_listpack_entries: u64,
    #[serde(skip)]
    pub zset_max_listpack_value: u64,
    #[serde(skip)]
    pub stream_node_max_bytes: u64,
    #[serde(skip)]
    pub stream_node_max_entries: u64,
    #[serde(skip)]
    pub activedefrag: bool,
    #[serde(skip)]
    pub active_defrag_ignore_bytes: u64,
    #[serde(skip)]
    pub active_defrag_threshold_lower: u64,
    #[serde(skip)]
    pub proto_max_bulk_len: u64,
    #[serde(skip)]
    pub lua_time_limit: u64,
    #[serde(skip)]
    pub lua_replicate_commands: bool,
    #[serde(skip)]
    pub cluster_enabled: bool,
    #[serde(skip)]
    pub cluster_config_file: String,
    #[serde(skip)]
    pub cluster_node_timeout: u64,
    #[serde(skip)]
    pub cluster_announce_ip: String,
    #[serde(skip)]
    pub cluster_announce_port: u16,
    #[serde(skip)]
    pub cluster_announce_bus_port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct NetworkConfig {
    pub bind: Vec<String>,
    pub port: u16,
    pub unixsocket: Option<PathBuf>,
    pub unixsocketperm: u32,
    pub tcp_backlog: u32,
    pub timeout: u64,
    pub tcp_keepalive: u64,
    pub protected_mode: bool,
    pub bind_source_addr: Option<String>,
    pub io_threads: usize,
    pub so_reuseport: bool,
    pub max_new_connections_per_cycle: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    pub port: u16,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub key_file_pass: Option<String>,
    pub ca_cert_file: Option<PathBuf>,
    pub ca_cert_dir: Option<PathBuf>,
    pub auth_clients: TlsAuthClients,
    pub auth_clients_user: TlsAuthField,
    pub replication: bool,
    pub cluster: bool,
    pub protocols: Vec<String>,
    pub ciphers: Option<String>,
    pub ciphersuites: Option<String>,
    pub prefer_server_ciphers: bool,
    pub session_caching: bool,
    pub session_cache_size: usize,
    pub session_cache_timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct GeneralConfig {
    pub daemonize: bool,
    pub pidfile: Option<PathBuf>,
    pub loglevel: LogLevel,
    pub logfile: Option<PathBuf>,
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub syslog_facility: String,
    pub databases: u8,
    pub always_show_logo: bool,
    pub set_proc_title: bool,
    pub proc_title_template: String,
    pub hz: u32,
    pub dynamic_hz: bool,
    pub activerehashing: bool,
    pub disable_thp: bool,
    pub oom_score_adj: OomScoreAdj,
    pub oom_score_adj_values: [i32; 3],
    pub include: Vec<String>,
    pub ignore_warnings: Vec<String>,
    #[serde(skip)]
    pub config_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    pub requirepass: Option<String>,
    pub aclfile: Option<PathBuf>,
    pub acllog_max_len: usize,
    pub acl_pubsub_default: AclPubSubDefault,
    pub users: Vec<String>,
    pub enable_protected_configs: ProtectedConfigAccess,
    pub enable_debug_command: ProtectedConfigAccess,
    pub enable_module_command: ProtectedConfigAccess,
    pub client_output_buffer_limit: Vec<ClientOutputBufferLimit>,
    pub client_query_buffer_limit: ByteSize,
    pub proto_max_bulk_len: ByteSize,
    pub tracking_table_max_keys: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PersistenceConfig {
    pub save: Vec<SavePoint>,
    pub stop_writes_on_bgsave_error: bool,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub dbfilename: String,
    pub dir: PathBuf,
    pub rdb_del_sync_files: bool,
    pub sanitize_dump_payload: SanitizePayload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ReplicationConfig {
    pub replicaof: Option<ReplicaOf>,
    pub masterauth: Option<String>,
    pub masteruser: Option<String>,
    pub replica_serve_stale_data: bool,
    pub replica_read_only: bool,
    pub repl_diskless_sync: bool,
    pub repl_diskless_sync_delay: u64,
    pub repl_diskless_sync_max_replicas: u32,
    pub repl_diskless_load: DisklessLoad,
    pub repl_ping_replica_period: u64,
    pub repl_timeout: u64,
    pub repl_disable_tcp_nodelay: bool,
    pub repl_backlog_size: ByteSize,
    pub repl_backlog_ttl: u64,
    pub replica_priority: u32,
    pub min_replicas_to_write: u32,
    pub min_replicas_max_lag: u64,
    pub replica_announce_ip: Option<String>,
    pub replica_announce_port: Option<u16>,
    pub propagation_error_behavior: PropagationErrorBehavior,
    pub replica_ignore_maxmemory: bool,
    pub replica_full_sync_buffer_limit: ByteSize,
    pub shutdown_timeout: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub config_file: PathBuf,
    pub node_timeout: u64,
    pub port: u16,
    pub replica_validity_factor: u32,
    pub migration_barrier: u32,
    pub allow_replica_migration: bool,
    pub require_full_coverage: bool,
    pub replica_no_failover: bool,
    pub allow_reads_when_down: bool,
    pub allow_pubsubshard_when_down: bool,
    pub link_sendbuf_limit: ByteSize,
    pub announce_ip: Option<String>,
    pub announce_port: Option<u16>,
    pub announce_tls_port: Option<u16>,
    pub announce_bus_port: Option<u16>,
    pub announce_hostname: Option<String>,
    pub announce_human_nodename: Option<String>,
    pub preferred_endpoint_type: EndpointType,
    pub compatibility_sample_ratio: u8,
    pub slot_stats_enabled: SlotStatsEnabled,
    pub slot_migration_write_pause_timeout: u64,
    pub slot_migration_handoff_max_lag_bytes: ByteSize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MemoryConfig {
    pub maxmemory: ByteSize,
    pub maxmemory_policy: MaxMemoryPolicy,
    pub maxmemory_samples: u8,
    pub maxmemory_eviction_tenacity: u8,
    pub maxclients: u32,
    pub maxmemory_clients: MaxMemoryClients,
    pub activedefrag: bool,
    pub active_defrag_ignore_bytes: ByteSize,
    pub active_defrag_threshold_lower: u8,
    pub active_defrag_threshold_upper: u8,
    pub active_defrag_cycle_min: u8,
    pub active_defrag_cycle_max: u8,
    pub active_defrag_max_scan_fields: usize,
    pub lfu_log_factor: u8,
    pub lfu_decay_time: u32,
    pub active_expire_effort: u8,
    pub jemalloc_bg_thread: bool,
    pub server_cpulist: Option<String>,
    pub bio_cpulist: Option<String>,
    pub aof_rewrite_cpulist: Option<String>,
    pub bgsave_cpulist: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct EvictionConfig {}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct EncodingConfig {
    pub hash_max_listpack_entries: u64,
    pub hash_max_listpack_value: u64,
    pub list_max_listpack_size: i32,
    pub list_compress_depth: u32,
    pub set_max_intset_entries: u64,
    pub set_max_listpack_entries: u64,
    pub set_max_listpack_value: u64,
    pub zset_max_listpack_entries: u64,
    pub zset_max_listpack_value: u64,
    pub hll_sparse_max_bytes: u64,
    pub stream_node_max_bytes: u64,
    pub stream_node_max_entries: u64,
    pub stream_idmp_duration: u64,
    pub stream_idmp_maxsize: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PubSubConfig {
    pub notify_keyspace_events: String,
    pub subscriber_ring_size: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SlowlogConfig {
    pub log_slower_than: i64,
    pub max_len: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LatencyConfig {
    pub monitor_threshold: u64,
    pub tracking: bool,
    pub tracking_info_percentiles: Vec<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClientsConfig {
    pub lookahead: usize,
    pub key_memory_histograms: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LazyFreeConfig {
    pub lazy_eviction: bool,
    pub lazy_expire: bool,
    pub lazy_server_del: bool,
    pub replica_lazy_flush: bool,
    pub lazy_user_del: bool,
    pub lazy_user_flush: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct AofConfig {
    pub enabled: bool,
    pub filename: String,
    pub dirname: String,
    pub fsync: AppendFsync,
    pub no_appendfsync_on_rewrite: bool,
    pub auto_aof_rewrite_percentage: u32,
    pub auto_aof_rewrite_min_size: ByteSize,
    pub aof_load_truncated: bool,
    pub aof_load_corrupt_tail_max_size: ByteSize,
    pub aof_use_rdb_preamble: bool,
    pub aof_timestamp_enabled: bool,
    pub aof_rewrite_incremental_fsync: bool,
    pub rdb_save_incremental_fsync: bool,
    pub shutdown_on_sigint: ShutdownAction,
    pub shutdown_on_sigterm: ShutdownAction,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PluginConfig {
    pub enabled: Vec<String>,
    pub extra: BTreeMap<String, Value>,
}

impl Default for SenkoConfig {
    fn default() -> Self {
        let mut config = Self {
            network: NetworkConfig::default(),
            tls: TlsConfig::default(),
            general: GeneralConfig::default(),
            security: SecurityConfig::default(),
            persistence: PersistenceConfig::default(),
            replication: ReplicationConfig::default(),
            cluster: ClusterConfig::default(),
            memory: MemoryConfig::default(),
            eviction: EvictionConfig::default(),
            encoding: EncodingConfig::default(),
            pubsub: PubSubConfig::default(),
            slowlog: SlowlogConfig::default(),
            latency: LatencyConfig::default(),
            clients: ClientsConfig::default(),
            lazyfree: LazyFreeConfig::default(),
            aof: AofConfig::default(),
            plugins: PluginConfig::default(),
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            config_file: None,
            num_shards: 1,
            max_connections: 10_000,
            max_memory: None,
            auth_password: None,
            aclfile: None,
            unixsocket: None,
            unixsocketperm: 0o700,
            timeout: 0,
            loglevel: "notice".to_owned(),
            logfile: String::new(),
            syslog_enabled: false,
            syslog_ident: "senko".to_owned(),
            syslog_facility: "local0".to_owned(),
            databases: 1,
            maxmemory_policy: "no-eviction".to_owned(),
            maxmemory_samples: 5,
            maxmemory_eviction_tenacity: 10,
            tcp_backlog: 511,
            tcp_nodelay: true,
            tcp_keepalive: 300,
            acllog_max_len: 128,
            hz: 10,
            dynamic_hz: true,
            aof_use_rdb_preamble: true,
            appendonly: false,
            appendfilename: "appendonly.aof".to_owned(),
            appendfsync: "everysec".to_owned(),
            no_appendfsync_on_rewrite: false,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 * 1024 * 1024,
            save: String::new(),
            rdbcompression: true,
            rdbchecksum: true,
            dbfilename: "dump.rdb".to_owned(),
            dir: PathBuf::from("./"),
            repl_backlog_size: 1024 * 1024,
            repl_backlog_ttl: 3600,
            replica_serve_stale_data: true,
            replica_read_only: true,
            replica_lazy_flush: false,
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            lazyfree_lazy_eviction: false,
            lazyfree_lazy_expire: false,
            lazyfree_lazy_server_del: false,
            activerehashing: true,
            list_max_listpack_size: -2,
            list_compress_depth: 0,
            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            stream_node_max_bytes: 4096,
            stream_node_max_entries: 100,
            activedefrag: false,
            active_defrag_ignore_bytes: 100 * 1024 * 1024,
            active_defrag_threshold_lower: 10,
            proto_max_bulk_len: 512 * 1024 * 1024,
            lua_time_limit: 5000,
            lua_replicate_commands: true,
            cluster_enabled: false,
            cluster_config_file: "nodes-6379.conf".to_owned(),
            cluster_node_timeout: 15_000,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 6379,
            cluster_announce_bus_port: 16_379,
        };
        config.normalize();
        config
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind: vec!["127.0.0.1".to_owned(), "::1".to_owned()],
            port: 6379,
            unixsocket: None,
            unixsocketperm: 0o700,
            tcp_backlog: 511,
            timeout: 0,
            tcp_keepalive: 300,
            protected_mode: true,
            bind_source_addr: None,
            io_threads: num_cpus::get().max(1),
            so_reuseport: true,
            max_new_connections_per_cycle: 10,
        }
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            port: 0,
            cert_file: None,
            key_file: None,
            key_file_pass: None,
            ca_cert_file: None,
            ca_cert_dir: None,
            auth_clients: TlsAuthClients::Yes,
            auth_clients_user: TlsAuthField::Off,
            replication: false,
            cluster: false,
            protocols: vec!["TLSv1.2".to_owned(), "TLSv1.3".to_owned()],
            ciphers: Some("DEFAULT:!MEDIUM".to_owned()),
            ciphersuites: None,
            prefer_server_ciphers: false,
            session_caching: true,
            session_cache_size: 20_480,
            session_cache_timeout: 300,
        }
    }
}

impl Default for GeneralConfig {
    fn default() -> Self {
        Self {
            daemonize: false,
            pidfile: None,
            loglevel: LogLevel::Notice,
            logfile: None,
            syslog_enabled: false,
            syslog_ident: "senko".to_owned(),
            syslog_facility: "local0".to_owned(),
            databases: 1,
            always_show_logo: false,
            set_proc_title: true,
            proc_title_template: "{title} {listen-addr} {server-mode}".to_owned(),
            hz: 10,
            dynamic_hz: true,
            activerehashing: true,
            disable_thp: true,
            oom_score_adj: OomScoreAdj::No,
            oom_score_adj_values: [0, 200, 800],
            include: Vec::new(),
            ignore_warnings: Vec::new(),
            config_file: None,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            requirepass: None,
            aclfile: None,
            acllog_max_len: 128,
            acl_pubsub_default: AclPubSubDefault::Resetchannels,
            users: Vec::new(),
            enable_protected_configs: ProtectedConfigAccess::No,
            enable_debug_command: ProtectedConfigAccess::No,
            enable_module_command: ProtectedConfigAccess::No,
            client_output_buffer_limit: Vec::new(),
            client_query_buffer_limit: ByteSize(1024_u64.pow(3)),
            proto_max_bulk_len: ByteSize(512 * 1024 * 1024),
            tracking_table_max_keys: 1_000_000,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            save: Vec::new(),
            stop_writes_on_bgsave_error: true,
            rdbcompression: true,
            rdbchecksum: true,
            dbfilename: "dump.rdb".to_owned(),
            dir: PathBuf::from("./"),
            rdb_del_sync_files: false,
            sanitize_dump_payload: SanitizePayload::Clients,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replicaof: None,
            masterauth: None,
            masteruser: None,
            replica_serve_stale_data: true,
            replica_read_only: true,
            repl_diskless_sync: true,
            repl_diskless_sync_delay: 5,
            repl_diskless_sync_max_replicas: 0,
            repl_diskless_load: DisklessLoad::Disabled,
            repl_ping_replica_period: 10,
            repl_timeout: 60,
            repl_disable_tcp_nodelay: false,
            repl_backlog_size: ByteSize(1024 * 1024),
            repl_backlog_ttl: 3600,
            replica_priority: 100,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,
            replica_announce_ip: None,
            replica_announce_port: None,
            propagation_error_behavior: PropagationErrorBehavior::Ignore,
            replica_ignore_maxmemory: true,
            replica_full_sync_buffer_limit: ByteSize(0),
            shutdown_timeout: 10,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            config_file: PathBuf::from("nodes-6379.conf"),
            node_timeout: 15_000,
            port: 0,
            replica_validity_factor: 10,
            migration_barrier: 1,
            allow_replica_migration: true,
            require_full_coverage: true,
            replica_no_failover: false,
            allow_reads_when_down: false,
            allow_pubsubshard_when_down: true,
            link_sendbuf_limit: ByteSize(0),
            announce_ip: None,
            announce_port: None,
            announce_tls_port: None,
            announce_bus_port: None,
            announce_hostname: None,
            announce_human_nodename: None,
            preferred_endpoint_type: EndpointType::Ip,
            compatibility_sample_ratio: 0,
            slot_stats_enabled: SlotStatsEnabled::No,
            slot_migration_write_pause_timeout: 10_000,
            slot_migration_handoff_max_lag_bytes: ByteSize(1024 * 1024),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maxmemory: ByteSize(0),
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            maxmemory_samples: 5,
            maxmemory_eviction_tenacity: 10,
            maxclients: 10_000,
            maxmemory_clients: MaxMemoryClients::Disabled,
            activedefrag: false,
            active_defrag_ignore_bytes: ByteSize(100 * 1024 * 1024),
            active_defrag_threshold_lower: 10,
            active_defrag_threshold_upper: 100,
            active_defrag_cycle_min: 1,
            active_defrag_cycle_max: 25,
            active_defrag_max_scan_fields: 1000,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            active_expire_effort: 1,
            jemalloc_bg_thread: true,
            server_cpulist: None,
            bio_cpulist: None,
            aof_rewrite_cpulist: None,
            bgsave_cpulist: None,
        }
    }
}

impl Default for EncodingConfig {
    fn default() -> Self {
        Self {
            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            list_max_listpack_size: -2,
            list_compress_depth: 0,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            hll_sparse_max_bytes: 3000,
            stream_node_max_bytes: 4096,
            stream_node_max_entries: 100,
            stream_idmp_duration: 100,
            stream_idmp_maxsize: 100,
        }
    }
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            notify_keyspace_events: String::new(),
            subscriber_ring_size: 256,
        }
    }
}

impl Default for SlowlogConfig {
    fn default() -> Self {
        Self {
            log_slower_than: 10_000,
            max_len: 128,
        }
    }
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            monitor_threshold: 0,
            tracking: true,
            tracking_info_percentiles: vec![50.0, 99.0, 99.9],
        }
    }
}

impl Default for ClientsConfig {
    fn default() -> Self {
        Self {
            lookahead: 16,
            key_memory_histograms: false,
        }
    }
}

impl Default for LazyFreeConfig {
    fn default() -> Self {
        Self {
            lazy_eviction: false,
            lazy_expire: false,
            lazy_server_del: false,
            replica_lazy_flush: false,
            lazy_user_del: false,
            lazy_user_flush: false,
        }
    }
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            filename: "appendonly.aof".to_owned(),
            dirname: "appendonlydir".to_owned(),
            fsync: AppendFsync::EverySec,
            no_appendfsync_on_rewrite: false,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: ByteSize(64 * 1024 * 1024),
            aof_load_truncated: true,
            aof_load_corrupt_tail_max_size: ByteSize(0),
            aof_use_rdb_preamble: true,
            aof_timestamp_enabled: false,
            aof_rewrite_incremental_fsync: true,
            rdb_save_incremental_fsync: true,
            shutdown_on_sigint: ShutdownAction::Default,
            shutdown_on_sigterm: ShutdownAction::Default,
        }
    }
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            enabled: Vec::new(),
            extra: BTreeMap::default(),
        }
    }
}

pub static CONFIG_ALIASES: phf::Map<&'static str, &'static str> = phf_map! {
    "bind" => "network.bind",
    "port" => "network.port",
    "unixsocket" => "network.unixsocket",
    "unixsocketperm" => "network.unixsocketperm",
    "tcp-backlog" => "network.tcp_backlog",
    "timeout" => "network.timeout",
    "tcp-keepalive" => "network.tcp_keepalive",
    "protected-mode" => "network.protected_mode",
    "io-threads" => "network.io_threads",
    "tls-port" => "tls.port",
    "tls-cert-file" => "tls.cert_file",
    "tls-key-file" => "tls.key_file",
    "tls-key-file-pass" => "tls.key_file_pass",
    "tls-ca-cert-file" => "tls.ca_cert_file",
    "tls-ca-cert-dir" => "tls.ca_cert_dir",
    "tls-auth-clients" => "tls.auth_clients",
    "tls-auth-clients-user" => "tls.auth_clients_user",
    "daemonize" => "general.daemonize",
    "pidfile" => "general.pidfile",
    "loglevel" => "general.loglevel",
    "logfile" => "general.logfile",
    "syslog-enabled" => "general.syslog_enabled",
    "syslog-ident" => "general.syslog_ident",
    "syslog-facility" => "general.syslog_facility",
    "databases" => "general.databases",
    "hz" => "general.hz",
    "dynamic-hz" => "general.dynamic_hz",
    "activerehashing" => "general.activerehashing",
    "requirepass" => "security.requirepass",
    "aclfile" => "security.aclfile",
    "acllog-max-len" => "security.acllog_max_len",
    "proto-max-bulk-len" => "security.proto_max_bulk_len",
    "save" => "persistence.save",
    "dbfilename" => "persistence.dbfilename",
    "dir" => "persistence.dir",
    "replicaof" => "replication.replicaof",
    "masterauth" => "replication.masterauth",
    "masteruser" => "replication.masteruser",
    "repl-backlog-size" => "replication.repl_backlog_size",
    "repl-backlog-ttl" => "replication.repl_backlog_ttl",
    "cluster-enabled" => "cluster.enabled",
    "cluster-config-file" => "cluster.config_file",
    "cluster-node-timeout" => "cluster.node_timeout",
    "cluster-announce-ip" => "cluster.announce_ip",
    "cluster-announce-port" => "cluster.announce_port",
    "cluster-announce-bus-port" => "cluster.announce_bus_port",
    "maxmemory" => "memory.maxmemory",
    "maxmemory-policy" => "memory.maxmemory_policy",
    "maxmemory-samples" => "memory.maxmemory_samples",
    "maxmemory-eviction-tenacity" => "memory.maxmemory_eviction_tenacity",
    "maxclients" => "memory.maxclients",
    "activedefrag" => "memory.activedefrag",
    "active-defrag-ignore-bytes" => "memory.active_defrag_ignore_bytes",
    "active-expire-effort" => "memory.active_expire_effort",
    "hash-max-listpack-entries" => "encoding.hash_max_listpack_entries",
    "hash-max-listpack-value" => "encoding.hash_max_listpack_value",
    "list-max-listpack-size" => "encoding.list_max_listpack_size",
    "list-compress-depth" => "encoding.list_compress_depth",
    "set-max-intset-entries" => "encoding.set_max_intset_entries",
    "set-max-listpack-entries" => "encoding.set_max_listpack_entries",
    "set-max-listpack-value" => "encoding.set_max_listpack_value",
    "zset-max-listpack-entries" => "encoding.zset_max_listpack_entries",
    "zset-max-listpack-value" => "encoding.zset_max_listpack_value",
    "stream-node-max-bytes" => "encoding.stream_node_max_bytes",
    "stream-node-max-entries" => "encoding.stream_node_max_entries",
    "notify-keyspace-events" => "pubsub.notify_keyspace_events",
    "slowlog-log-slower-than" => "slowlog.log_slower_than",
    "slowlog-max-len" => "slowlog.max_len",
    "latency-monitor-threshold" => "latency.monitor_threshold",
    "latency-tracking" => "latency.tracking",
    "lazyfree-lazy-eviction" => "lazyfree.lazy_eviction",
    "lazyfree-lazy-expire" => "lazyfree.lazy_expire",
    "lazyfree-lazy-server-del" => "lazyfree.lazy_server_del",
    "replica-lazy-flush" => "lazyfree.replica_lazy_flush",
    "appendonly" => "aof.enabled",
    "appendfilename" => "aof.filename",
    "appenddirname" => "aof.dirname",
    "appendfsync" => "aof.fsync",
    "no-appendfsync-on-rewrite" => "aof.no_appendfsync_on_rewrite",
    "auto-aof-rewrite-percentage" => "aof.auto_aof_rewrite_percentage",
    "auto-aof-rewrite-min-size" => "aof.auto_aof_rewrite_min_size",
    "aof-use-rdb-preamble" => "aof.aof_use_rdb_preamble",
};

#[derive(Clone, Copy)]
enum FieldKind {
    Bool,
    U8,
    U16,
    U32,
    U64,
    I32,
    I64,
    Usize,
    String,
    Path,
    StringList,
    ByteSize,
    FloatList,
    SaveList,
    ReplicaOf,
    LogLevel,
    MaxMemoryPolicy,
    AppendFsync,
}

fn field_kind(path: &str) -> Option<FieldKind> {
    Some(match path {
        "network.bind" => FieldKind::StringList,
        "network.port" => FieldKind::U16,
        "network.unixsocket" => FieldKind::Path,
        "network.unixsocketperm" => FieldKind::U32,
        "network.tcp_backlog" => FieldKind::U32,
        "network.timeout" => FieldKind::U64,
        "network.tcp_keepalive" => FieldKind::U64,
        "network.protected_mode" => FieldKind::Bool,
        "network.bind_source_addr" => FieldKind::String,
        "network.io_threads" => FieldKind::Usize,
        "network.so_reuseport" => FieldKind::Bool,
        "network.max_new_connections_per_cycle" => FieldKind::Usize,
        "tls.port" => FieldKind::U16,
        "tls.cert_file" | "tls.key_file" | "tls.ca_cert_file" | "tls.ca_cert_dir" => {
            FieldKind::Path
        }
        "tls.key_file_pass" => FieldKind::String,
        "general.daemonize" => FieldKind::Bool,
        "general.pidfile" | "general.logfile" => FieldKind::Path,
        "general.loglevel" => FieldKind::LogLevel,
        "general.syslog_enabled"
        | "general.always_show_logo"
        | "general.set_proc_title"
        | "general.dynamic_hz"
        | "general.activerehashing"
        | "general.disable_thp" => FieldKind::Bool,
        "general.syslog_ident" | "general.syslog_facility" | "general.proc_title_template" => {
            FieldKind::String
        }
        "general.databases" => FieldKind::U8,
        "general.hz" => FieldKind::U32,
        "general.include" | "general.ignore_warnings" | "plugins.enabled" => FieldKind::StringList,
        "security.requirepass" => FieldKind::String,
        "security.aclfile" => FieldKind::Path,
        "security.acllog_max_len" | "security.tracking_table_max_keys" => FieldKind::Usize,
        "security.users" => FieldKind::StringList,
        "security.client_query_buffer_limit" | "security.proto_max_bulk_len" => FieldKind::ByteSize,
        "persistence.save" => FieldKind::SaveList,
        "persistence.stop_writes_on_bgsave_error"
        | "persistence.rdbcompression"
        | "persistence.rdbchecksum"
        | "persistence.rdb_del_sync_files" => FieldKind::Bool,
        "persistence.dbfilename" => FieldKind::String,
        "persistence.dir" => FieldKind::Path,
        "replication.replicaof" => FieldKind::ReplicaOf,
        "replication.masterauth" | "replication.masteruser" | "replication.replica_announce_ip" => {
            FieldKind::String
        }
        "replication.replica_serve_stale_data"
        | "replication.replica_read_only"
        | "replication.repl_diskless_sync"
        | "replication.repl_disable_tcp_nodelay"
        | "replication.replica_ignore_maxmemory" => FieldKind::Bool,
        "replication.repl_diskless_sync_delay"
        | "replication.repl_ping_replica_period"
        | "replication.repl_timeout"
        | "replication.repl_backlog_ttl"
        | "replication.min_replicas_max_lag"
        | "replication.shutdown_timeout" => FieldKind::U64,
        "replication.repl_diskless_sync_max_replicas"
        | "replication.replica_priority"
        | "replication.min_replicas_to_write" => FieldKind::U32,
        "replication.repl_backlog_size" | "replication.replica_full_sync_buffer_limit" => {
            FieldKind::ByteSize
        }
        "replication.replica_announce_port" => FieldKind::U16,
        "cluster.enabled"
        | "cluster.allow_replica_migration"
        | "cluster.require_full_coverage"
        | "cluster.replica_no_failover"
        | "cluster.allow_reads_when_down"
        | "cluster.allow_pubsubshard_when_down" => FieldKind::Bool,
        "cluster.config_file" => FieldKind::Path,
        "cluster.node_timeout" | "cluster.slot_migration_write_pause_timeout" => FieldKind::U64,
        "cluster.port"
        | "cluster.announce_port"
        | "cluster.announce_tls_port"
        | "cluster.announce_bus_port" => FieldKind::U16,
        "cluster.replica_validity_factor" | "cluster.migration_barrier" => FieldKind::U32,
        "cluster.link_sendbuf_limit" | "cluster.slot_migration_handoff_max_lag_bytes" => {
            FieldKind::ByteSize
        }
        "cluster.announce_ip" | "cluster.announce_hostname" | "cluster.announce_human_nodename" => {
            FieldKind::String
        }
        "cluster.compatibility_sample_ratio" => FieldKind::U8,
        "memory.maxmemory" => FieldKind::ByteSize,
        "memory.maxmemory_policy" => FieldKind::MaxMemoryPolicy,
        "memory.maxmemory_samples"
        | "memory.maxmemory_eviction_tenacity"
        | "memory.active_defrag_threshold_lower"
        | "memory.active_defrag_threshold_upper"
        | "memory.active_defrag_cycle_min"
        | "memory.active_defrag_cycle_max"
        | "memory.lfu_log_factor"
        | "memory.active_expire_effort" => FieldKind::U8,
        "memory.maxclients" => FieldKind::U32,
        "memory.activedefrag" | "memory.jemalloc_bg_thread" => FieldKind::Bool,
        "memory.active_defrag_ignore_bytes" => FieldKind::ByteSize,
        "memory.active_defrag_max_scan_fields" => FieldKind::Usize,
        "memory.lfu_decay_time" => FieldKind::U32,
        "memory.server_cpulist"
        | "memory.bio_cpulist"
        | "memory.aof_rewrite_cpulist"
        | "memory.bgsave_cpulist" => FieldKind::String,
        "encoding.hash_max_listpack_entries"
        | "encoding.hash_max_listpack_value"
        | "encoding.set_max_intset_entries"
        | "encoding.set_max_listpack_entries"
        | "encoding.set_max_listpack_value"
        | "encoding.zset_max_listpack_entries"
        | "encoding.zset_max_listpack_value"
        | "encoding.hll_sparse_max_bytes"
        | "encoding.stream_node_max_bytes"
        | "encoding.stream_node_max_entries"
        | "encoding.stream_idmp_duration"
        | "encoding.stream_idmp_maxsize" => FieldKind::U64,
        "encoding.list_max_listpack_size" => FieldKind::I32,
        "encoding.list_compress_depth" => FieldKind::U32,
        "pubsub.notify_keyspace_events" => FieldKind::String,
        "pubsub.subscriber_ring_size" | "clients.lookahead" => FieldKind::Usize,
        "slowlog.log_slower_than" => FieldKind::I64,
        "slowlog.max_len" => FieldKind::Usize,
        "latency.monitor_threshold" => FieldKind::U64,
        "latency.tracking" | "clients.key_memory_histograms" => FieldKind::Bool,
        "latency.tracking_info_percentiles" => FieldKind::FloatList,
        "lazyfree.lazy_eviction"
        | "lazyfree.lazy_expire"
        | "lazyfree.lazy_server_del"
        | "lazyfree.replica_lazy_flush"
        | "lazyfree.lazy_user_del"
        | "lazyfree.lazy_user_flush" => FieldKind::Bool,
        "aof.enabled"
        | "aof.no_appendfsync_on_rewrite"
        | "aof.aof_load_truncated"
        | "aof.aof_use_rdb_preamble"
        | "aof.aof_timestamp_enabled"
        | "aof.aof_rewrite_incremental_fsync"
        | "aof.rdb_save_incremental_fsync" => FieldKind::Bool,
        "aof.filename" | "aof.dirname" => FieldKind::String,
        "aof.fsync" => FieldKind::AppendFsync,
        "aof.auto_aof_rewrite_percentage" => FieldKind::U32,
        "aof.auto_aof_rewrite_min_size" | "aof.aof_load_corrupt_tail_max_size" => {
            FieldKind::ByteSize
        }
        _ => return None,
    })
}

pub fn load_config(path: Option<&Path>) -> Result<SenkoConfig, ConfigError> {
    let mut config = SenkoConfig::default();
    if let Some(path) = path {
        let value = load_value_with_includes(path, 0)?;
        config = value.try_into()?;
        config.general.config_file = Some(path.to_path_buf());
    }
    config.normalize();
    validate_config(&config)?;
    Ok(config)
}

pub fn validate_config(config: &SenkoConfig) -> Result<(), ConfigError> {
    if config.network.port == 0 && config.network.unixsocket.is_none() {
        return Err(ConfigError::ValidationError(
            "at least one listener must be enabled: network.port > 0 or network.unixsocket set"
                .to_owned(),
        ));
    }
    if config.tls.port > 0 && (config.tls.cert_file.is_none() || config.tls.key_file.is_none()) {
        return Err(ConfigError::ValidationError(
            "tls.port requires tls.cert_file and tls.key_file".to_owned(),
        ));
    }
    if config.security.aclfile.is_some() && !config.security.users.is_empty() {
        return Err(ConfigError::ConflictError(
            "security.aclfile and security.users are mutually exclusive".to_owned(),
        ));
    }
    if config.encoding.hash_max_listpack_entries == 0 {
        return Err(ConfigError::ValidationError(
            "encoding.hash_max_listpack_entries must be greater than zero".to_owned(),
        ));
    }
    if !((-5..=-1).contains(&config.encoding.list_max_listpack_size)
        || config.encoding.list_max_listpack_size > 0)
    {
        return Err(ConfigError::ValidationError(
            "encoding.list_max_listpack_size must be in [-5,-1] or greater than zero".to_owned(),
        ));
    }
    if !(1..=86_400).contains(&config.encoding.stream_idmp_duration) {
        return Err(ConfigError::ValidationError(
            "encoding.stream_idmp_duration must be in [1, 86400]".to_owned(),
        ));
    }
    if config
        .latency
        .tracking_info_percentiles
        .iter()
        .any(|value| *value <= 0.0 || *value >= 100.0)
    {
        return Err(ConfigError::ValidationError(
            "latency.tracking_info_percentiles values must be in (0.0, 100.0)".to_owned(),
        ));
    }
    if config.cluster.compatibility_sample_ratio > 100 {
        return Err(ConfigError::ValidationError(
            "cluster.compatibility_sample_ratio must be in [0, 100]".to_owned(),
        ));
    }
    if config
        .general
        .oom_score_adj_values
        .iter()
        .any(|value| !(-2000..=2000).contains(value))
    {
        return Err(ConfigError::ValidationError(
            "general.oom_score_adj_values must be in [-2000, 2000]".to_owned(),
        ));
    }
    if !(1..=500).contains(&config.general.hz) {
        return Err(ConfigError::ValidationError(
            "general.hz must be in [1, 500]".to_owned(),
        ));
    }
    if !(1..=10).contains(&config.memory.active_expire_effort) {
        return Err(ConfigError::ValidationError(
            "memory.active_expire_effort must be in [1, 10]".to_owned(),
        ));
    }
    if !(1..=64).contains(&config.memory.maxmemory_samples) {
        return Err(ConfigError::ValidationError(
            "memory.maxmemory_samples must be in [1, 64]".to_owned(),
        ));
    }
    if config.pubsub.subscriber_ring_size < 16
        || config.pubsub.subscriber_ring_size > 65_536
        || !config.pubsub.subscriber_ring_size.is_power_of_two()
    {
        return Err(ConfigError::ValidationError(
            "pubsub.subscriber_ring_size must be a power of 2 in [16, 65536]".to_owned(),
        ));
    }
    for plugin in &config.plugins.enabled {
        if !KNOWN_PLUGINS.iter().any(|known| known == plugin) {
            return Err(ConfigError::ValidationError(format!(
                "plugins.enabled contains unknown plugin '{plugin}'"
            )));
        }
    }
    Ok(())
}

pub fn config_get(config: &SenkoConfig, pattern: &str) -> Vec<(String, String)> {
    let flattened = flatten_config(config);
    let alias_groups = alias_groups();
    let exact = !has_glob(pattern);
    let mut names = BTreeSet::new();

    if exact {
        let canonical = resolve_config_key(pattern);
        if flattened.contains_key(canonical) {
            names.insert(canonical.to_owned());
            if let Some(aliases) = alias_groups.get(canonical) {
                for alias in aliases {
                    names.insert((*alias).to_owned());
                }
            }
        } else if let Some(canonical) = CONFIG_ALIASES.get(pattern) {
            names.insert((*canonical).to_owned());
            if let Some(aliases) = alias_groups.get(canonical) {
                for alias in aliases {
                    names.insert((*alias).to_owned());
                }
            }
        }
    } else {
        for name in flattened.keys() {
            let leaf = name.rsplit('.').next().unwrap_or(name);
            if glob_match(pattern, name) || glob_match(pattern, leaf) {
                names.insert(name.clone());
            }
        }
        for alias in CONFIG_ALIASES.keys() {
            if glob_match(pattern, alias) {
                names.insert((*alias).to_owned());
            }
        }
    }

    names
        .into_iter()
        .filter_map(|name| {
            let canonical = resolve_config_key(&name);
            flattened.get(canonical).cloned().map(|value| (name, value))
        })
        .collect()
}

pub fn config_set(config: &mut SenkoConfig, key: &str, value: &str) -> Result<(), ConfigError> {
    let canonical = resolve_config_key(key);
    if canonical == key && field_kind(canonical).is_none() {
        return Err(ConfigError::ValidationError(format!(
            "unknown config key '{key}'"
        )));
    }
    if is_immutable_config_key(canonical) {
        return Err(ConfigError::ValidationError(format!(
            "config key '{key}' is immutable and requires restart"
        )));
    }

    let mut root = toml::Value::try_from(config.clone())
        .map_err(|error| ConfigError::ValidationError(error.to_string()))?;
    let kind = field_kind(canonical)
        .ok_or_else(|| ConfigError::ValidationError(format!("unknown config key '{key}'")))?;
    let parsed = parse_config_value(kind, value)?;
    set_value_at_path(&mut root, canonical, parsed)?;
    let mut next: SenkoConfig = root
        .try_into()
        .map_err(|error: toml::de::Error| ConfigError::ParseError(error))?;
    next.general.config_file = config.general.config_file.clone();
    next.normalize();
    validate_config(&next)?;
    *config = next;
    Ok(())
}

pub fn render_default_config_toml() -> Result<String, ConfigError> {
    toml::to_string_pretty(&SenkoConfig::default())
        .map_err(|error| ConfigError::ValidationError(error.to_string()))
}

impl SenkoConfig {
    pub fn normalize(&mut self) {
        if self.network.io_threads == 0 {
            self.network.io_threads = num_cpus::get().max(1);
        }
        self.sync_legacy();
    }

    fn sync_legacy(&mut self) {
        let first_bind = self
            .network
            .bind
            .first()
            .and_then(|value| value.parse::<IpAddr>().ok())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        self.bind_addr = SocketAddr::new(first_bind, self.network.port);
        self.config_file = self.general.config_file.clone();
        self.num_shards = self.network.io_threads.max(1);
        self.max_connections = self.memory.maxclients as usize;
        self.max_memory = if self.memory.maxmemory.0 == 0 {
            None
        } else {
            usize::try_from(self.memory.maxmemory.0).ok()
        };
        self.auth_password = self.security.requirepass.clone();
        self.aclfile = self.security.aclfile.clone();
        self.unixsocket = self.network.unixsocket.clone();
        self.unixsocketperm = self.network.unixsocketperm;
        self.timeout = self.network.timeout;
        self.loglevel = toml_value_to_string(&Value::try_from(self.general.loglevel).unwrap());
        self.logfile = self
            .general
            .logfile
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default();
        self.syslog_enabled = self.general.syslog_enabled;
        self.syslog_ident = self.general.syslog_ident.clone();
        self.syslog_facility = self.general.syslog_facility.clone();
        self.databases = u64::from(self.general.databases);
        self.maxmemory_policy =
            toml_value_to_string(&Value::try_from(self.memory.maxmemory_policy).unwrap());
        self.maxmemory_samples = u64::from(self.memory.maxmemory_samples);
        self.maxmemory_eviction_tenacity = u64::from(self.memory.maxmemory_eviction_tenacity);
        self.tcp_backlog = self.network.tcp_backlog;
        self.tcp_nodelay = true;
        self.tcp_keepalive = self.network.tcp_keepalive;
        self.acllog_max_len = self.security.acllog_max_len;
        self.hz = u64::from(self.general.hz);
        self.dynamic_hz = self.general.dynamic_hz;
        self.aof_use_rdb_preamble = self.aof.aof_use_rdb_preamble;
        self.appendonly = self.aof.enabled;
        self.appendfilename = self.aof.filename.clone();
        self.appendfsync = toml_value_to_string(&Value::try_from(self.aof.fsync).unwrap());
        self.no_appendfsync_on_rewrite = self.aof.no_appendfsync_on_rewrite;
        self.auto_aof_rewrite_percentage = u64::from(self.aof.auto_aof_rewrite_percentage);
        self.auto_aof_rewrite_min_size = self.aof.auto_aof_rewrite_min_size.0;
        self.save = self
            .persistence
            .save
            .iter()
            .flat_map(|point| [point.seconds.to_string(), point.changes.to_string()])
            .collect::<Vec<_>>()
            .join(" ");
        self.rdbcompression = self.persistence.rdbcompression;
        self.rdbchecksum = self.persistence.rdbchecksum;
        self.dbfilename = self.persistence.dbfilename.clone();
        self.dir = self.persistence.dir.clone();
        self.repl_backlog_size = self.replication.repl_backlog_size.0;
        self.repl_backlog_ttl = self.replication.repl_backlog_ttl;
        self.replica_serve_stale_data = self.replication.replica_serve_stale_data;
        self.replica_read_only = self.replication.replica_read_only;
        self.replica_lazy_flush = self.lazyfree.replica_lazy_flush;
        self.slowlog_log_slower_than = self.slowlog.log_slower_than;
        self.slowlog_max_len = self.slowlog.max_len;
        self.latency_monitor_threshold = self.latency.monitor_threshold as i64;
        self.lazyfree_lazy_eviction = self.lazyfree.lazy_eviction;
        self.lazyfree_lazy_expire = self.lazyfree.lazy_expire;
        self.lazyfree_lazy_server_del = self.lazyfree.lazy_server_del;
        self.activerehashing = self.general.activerehashing;
        self.list_max_listpack_size = i64::from(self.encoding.list_max_listpack_size);
        self.list_compress_depth = u64::from(self.encoding.list_compress_depth);
        self.hash_max_listpack_entries = self.encoding.hash_max_listpack_entries;
        self.hash_max_listpack_value = self.encoding.hash_max_listpack_value;
        self.set_max_intset_entries = self.encoding.set_max_intset_entries;
        self.set_max_listpack_entries = self.encoding.set_max_listpack_entries;
        self.set_max_listpack_value = self.encoding.set_max_listpack_value;
        self.zset_max_listpack_entries = self.encoding.zset_max_listpack_entries;
        self.zset_max_listpack_value = self.encoding.zset_max_listpack_value;
        self.stream_node_max_bytes = self.encoding.stream_node_max_bytes;
        self.stream_node_max_entries = self.encoding.stream_node_max_entries;
        self.activedefrag = self.memory.activedefrag;
        self.active_defrag_ignore_bytes = self.memory.active_defrag_ignore_bytes.0;
        self.active_defrag_threshold_lower = u64::from(self.memory.active_defrag_threshold_lower);
        self.proto_max_bulk_len = self.security.proto_max_bulk_len.0;
        self.lua_time_limit = 5000;
        self.lua_replicate_commands = true;
        self.cluster_enabled = self.cluster.enabled;
        self.cluster_config_file = self.cluster.config_file.display().to_string();
        self.cluster_node_timeout = self.cluster.node_timeout;
        self.cluster_announce_ip = self.cluster.announce_ip.clone().unwrap_or_default();
        self.cluster_announce_port = self.cluster.announce_port.unwrap_or(self.network.port);
        self.cluster_announce_bus_port = self
            .cluster
            .announce_bus_port
            .unwrap_or(self.network.port.saturating_add(10_000));
    }
}

fn load_value_with_includes(path: &Path, depth: usize) -> Result<Value, ConfigError> {
    if depth >= INCLUDE_MAX_DEPTH {
        return Err(ConfigError::ValidationError(format!(
            "include recursion exceeded max depth of {INCLUDE_MAX_DEPTH}"
        )));
    }
    let contents = fs::read_to_string(path)?;
    let parsed = toml::from_str::<Value>(&contents)?;
    let include_patterns = read_include_patterns(&parsed)?;
    let mut merged = Value::Table(Default::default());
    let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
    for include in include_patterns {
        for include_path in expand_include_paths(base_dir, &include)? {
            if include_path == path {
                continue;
            }
            let include_value =
                load_value_with_includes(&include_path, depth + 1).map_err(|source| {
                    ConfigError::IncludeError {
                        path: include_path.clone(),
                        source: Box::new(source),
                    }
                })?;
            merge_toml(&mut merged, include_value);
        }
    }
    merge_toml(&mut merged, parsed);
    Ok(merged)
}

fn read_include_patterns(value: &Value) -> Result<Vec<String>, ConfigError> {
    let mut includes = Vec::new();
    if let Some(general) = value.get("general").and_then(Value::as_table) {
        if let Some(include) = general.get("include") {
            match include {
                Value::String(single) => includes.push(single.clone()),
                Value::Array(values) => {
                    for value in values {
                        let item = value.as_str().ok_or_else(|| {
                            ConfigError::ValidationError(
                                "general.include entries must be strings".to_owned(),
                            )
                        })?;
                        includes.push(item.to_owned());
                    }
                }
                _ => {
                    return Err(ConfigError::ValidationError(
                        "general.include must be a string or array of strings".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(includes)
}

fn expand_include_paths(base_dir: &Path, pattern: &str) -> Result<Vec<PathBuf>, ConfigError> {
    let resolved = if Path::new(pattern).is_absolute() {
        PathBuf::from(pattern)
    } else {
        base_dir.join(pattern)
    };
    let pattern_string = resolved.to_string_lossy();
    if !has_glob(&pattern_string) {
        return Ok(vec![resolved]);
    }
    let (dir, file_pattern) = split_pattern_dir(&resolved);
    let mut matches = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if glob_match(&file_pattern, &file_name) {
            matches.push(entry.path());
        }
    }
    matches.sort();
    Ok(matches)
}

fn split_pattern_dir(path: &Path) -> (PathBuf, String) {
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let file_pattern = path
        .file_name()
        .map(|part| part.to_string_lossy().into_owned())
        .unwrap_or_else(|| "*".to_owned());
    (parent, file_pattern)
}

fn merge_toml(base: &mut Value, incoming: Value) {
    match (base, incoming) {
        (Value::Table(left), Value::Table(right)) => {
            for (key, value) in right {
                match left.get_mut(&key) {
                    Some(existing) => merge_toml(existing, value),
                    None => {
                        left.insert(key, value);
                    }
                }
            }
        }
        (slot, value) => *slot = value,
    }
}

fn flatten_config(config: &SenkoConfig) -> BTreeMap<String, String> {
    let value = toml::Value::try_from(config.clone()).expect("config should serialize to toml");
    let mut out = BTreeMap::new();
    flatten_value("", &value, &mut out);
    out
}

fn flatten_value(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Table(table) => {
            for (key, value) in table {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_value(&next, value, out);
            }
        }
        _ => {
            out.insert(prefix.to_owned(), toml_value_to_string(value));
        }
    }
}

fn toml_value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Integer(value) => value.to_string(),
        Value::Float(value) => {
            let mut text = value.to_string();
            if text.ends_with(".0") {
                text.truncate(text.len() - 2);
            }
            text
        }
        Value::Boolean(value) => value.to_string(),
        Value::Array(values) => values
            .iter()
            .map(toml_value_to_string)
            .collect::<Vec<_>>()
            .join(","),
        Value::Datetime(value) => value.to_string(),
        Value::Table(_) => String::new(),
    }
}

fn parse_config_value(kind: FieldKind, raw: &str) -> Result<Value, ConfigError> {
    Ok(match kind {
        FieldKind::Bool => Value::Boolean(parse_bool(raw)?),
        FieldKind::U8 => Value::Integer(i64::from(parse_u8(raw)?)),
        FieldKind::U16 => Value::Integer(i64::from(parse_u16(raw)?)),
        FieldKind::U32 => Value::Integer(i64::from(parse_u32(raw)?)),
        FieldKind::U64 => Value::Integer(parse_u64(raw)? as i64),
        FieldKind::I32 => Value::Integer(i64::from(parse_i32(raw)?)),
        FieldKind::I64 => Value::Integer(parse_i64(raw)?),
        FieldKind::Usize => Value::Integer(parse_usize(raw)? as i64),
        FieldKind::String => Value::String(raw.to_owned()),
        FieldKind::Path => {
            if raw.is_empty() {
                Value::String(String::new())
            } else {
                Value::String(raw.to_owned())
            }
        }
        FieldKind::StringList => Value::Array(
            raw.split(',')
                .filter(|item| !item.trim().is_empty())
                .map(|item| Value::String(item.trim().to_owned()))
                .collect(),
        ),
        FieldKind::ByteSize => Value::Integer(
            ByteSize::from_str(raw)
                .map_err(ConfigError::ValidationError)?
                .0 as i64,
        ),
        FieldKind::FloatList => Value::Array(
            raw.split(',')
                .map(|item| {
                    item.trim().parse::<f64>().map(Value::Float).map_err(|_| {
                        ConfigError::ValidationError(format!("invalid float list value '{raw}'"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        FieldKind::SaveList => Value::Array(parse_save_points(raw)?),
        FieldKind::ReplicaOf => {
            let replica = parse_replica_of(raw)?;
            Value::Table(
                [
                    ("host".to_owned(), Value::String(replica.host)),
                    ("port".to_owned(), Value::Integer(i64::from(replica.port))),
                ]
                .into_iter()
                .collect(),
            )
        }
        FieldKind::LogLevel => Value::String(parse_log_level(raw)?),
        FieldKind::MaxMemoryPolicy => Value::String(parse_maxmemory_policy(raw)?),
        FieldKind::AppendFsync => Value::String(parse_appendfsync(raw)?),
    })
}

fn parse_bool(value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "yes" | "true" | "on" => Ok(true),
        "0" | "no" | "false" | "off" => Ok(false),
        _ => Err(ConfigError::ValidationError(format!(
            "invalid boolean value '{value}'"
        ))),
    }
}

fn parse_u8(value: &str) -> Result<u8, ConfigError> {
    value
        .trim()
        .parse::<u8>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid u8 value '{value}'")))
}

fn parse_u16(value: &str) -> Result<u16, ConfigError> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid u16 value '{value}'")))
}

fn parse_u32(value: &str) -> Result<u32, ConfigError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid u32 value '{value}'")))
}

fn parse_u64(value: &str) -> Result<u64, ConfigError> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid u64 value '{value}'")))
}

fn parse_i32(value: &str) -> Result<i32, ConfigError> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid i32 value '{value}'")))
}

fn parse_i64(value: &str) -> Result<i64, ConfigError> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid i64 value '{value}'")))
}

fn parse_usize(value: &str) -> Result<usize, ConfigError> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|_| ConfigError::ValidationError(format!("invalid usize value '{value}'")))
}

fn parse_log_level(value: &str) -> Result<String, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" | "verbose" | "notice" | "warning" | "nothing" => {
            Ok(value.trim().to_ascii_lowercase())
        }
        _ => Err(ConfigError::ValidationError(format!(
            "invalid log level '{value}'"
        ))),
    }
}

fn parse_maxmemory_policy(value: &str) -> Result<String, ConfigError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "volatile-lru" | "allkeys-lru" | "volatile-lfu" | "allkeys-lfu" | "volatile-lrm"
        | "allkeys-lrm" | "volatile-random" | "allkeys-random" | "volatile-ttl" | "noeviction"
        | "no-eviction" => Ok(if normalized == "noeviction" {
            "no-eviction".to_owned()
        } else {
            normalized
        }),
        _ => Err(ConfigError::ValidationError(format!(
            "invalid maxmemory policy '{value}'"
        ))),
    }
}

fn parse_appendfsync(value: &str) -> Result<String, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "always" | "everysec" | "no" => Ok(value.trim().to_ascii_lowercase()),
        _ => Err(ConfigError::ValidationError(format!(
            "invalid appendfsync value '{value}'"
        ))),
    }
}

fn parse_save_points(value: &str) -> Result<Vec<Value>, ConfigError> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let tokens = value.split_whitespace().collect::<Vec<_>>();
    if tokens.len() % 2 != 0 {
        return Err(ConfigError::ValidationError(
            "save schedule must be expressed as pairs of seconds and changes".to_owned(),
        ));
    }
    let mut values = Vec::with_capacity(tokens.len() / 2);
    for chunk in tokens.chunks_exact(2) {
        values.push(Value::Table(
            [
                (
                    "seconds".to_owned(),
                    Value::Integer(parse_u64(chunk[0])? as i64),
                ),
                (
                    "changes".to_owned(),
                    Value::Integer(parse_u64(chunk[1])? as i64),
                ),
            ]
            .into_iter()
            .collect(),
        ));
    }
    Ok(values)
}

pub fn parse_replica_of(value: &str) -> Result<ReplicaOf, ConfigError> {
    let (host, port) = value
        .trim()
        .rsplit_once(':')
        .ok_or_else(|| ConfigError::ValidationError(format!("invalid replicaof '{value}'")))?;
    Ok(ReplicaOf {
        host: host.to_owned(),
        port: parse_u16(port)?,
    })
}

fn resolve_config_key(key: &str) -> &str {
    CONFIG_ALIASES.get(key).copied().unwrap_or(key)
}

fn is_immutable_config_key(key: &str) -> bool {
    key == "network.port"
        || key == "network.bind"
        || key == "network.io_threads"
        || key == "network.so_reuseport"
        || key == "general.databases"
        || key == "cluster.enabled"
        || key == "clients.key_memory_histograms"
        || key == "memory.activedefrag"
        || key.starts_with("tls.")
}

fn set_value_at_path(root: &mut Value, path: &str, value: Value) -> Result<(), ConfigError> {
    let mut current = root;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        let Some(table) = current.as_table_mut() else {
            return Err(ConfigError::ValidationError(format!(
                "invalid config path '{path}'"
            )));
        };
        if parts.peek().is_none() {
            table.insert(part.to_owned(), value);
            return Ok(());
        }
        current = table
            .entry(part.to_owned())
            .or_insert_with(|| Value::Table(Default::default()));
    }
    Ok(())
}

fn alias_groups() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut groups = BTreeMap::<&'static str, Vec<&'static str>>::new();
    for (alias, canonical) in CONFIG_ALIASES.entries() {
        groups.entry(canonical).or_default().push(alias);
    }
    groups
}

fn has_glob(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'*' => {
            glob_match_bytes(&pattern[1..], text)
                || (!text.is_empty() && glob_match_bytes(pattern, &text[1..]))
        }
        b'?' => !text.is_empty() && glob_match_bytes(&pattern[1..], &text[1..]),
        byte => {
            !text.is_empty()
                && byte.eq_ignore_ascii_case(&text[0])
                && glob_match_bytes(&pattern[1..], &text[1..])
        }
    }
}

pub fn parse_duration_seconds(value: &str) -> Result<u64, ConfigError> {
    humantime::parse_duration(value)
        .map(|duration| duration.as_secs())
        .map_err(|error| ConfigError::ValidationError(error.to_string()))
}

pub fn human_duration(value: u64) -> String {
    humantime::format_duration(Duration::from_secs(value)).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "senko-config-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn empty_file_uses_defaults() {
        let dir = temp_dir();
        let path = dir.join("senko.toml");
        fs::write(&path, "").unwrap();
        let config = load_config(Some(&path)).unwrap();
        assert_eq!(config.network.port, 6379);
        assert_eq!(config.memory.maxmemory, ByteSize(0));
    }

    #[test]
    fn partial_file_overrides_defaults() {
        let dir = temp_dir();
        let path = dir.join("senko.toml");
        fs::write(
            &path,
            "[network]\nport = 6380\n\n[memory]\nmaxmemory = \"1gb\"\n",
        )
        .unwrap();
        let config = load_config(Some(&path)).unwrap();
        assert_eq!(config.network.port, 6380);
        assert_eq!(config.memory.maxmemory, ByteSize(1024_u64.pow(3)));
        assert_eq!(config.general.hz, 10);
    }

    #[test]
    fn unknown_key_is_parse_error() {
        let dir = temp_dir();
        let path = dir.join("senko.toml");
        fs::write(&path, "[network]\nunknown = 1\n").unwrap();
        assert!(matches!(
            load_config(Some(&path)),
            Err(ConfigError::ParseError(_))
        ));
    }

    #[test]
    fn invalid_value_is_parse_error() {
        let dir = temp_dir();
        let path = dir.join("senko.toml");
        fs::write(&path, "[network]\nport = \"notanumber\"\n").unwrap();
        assert!(matches!(
            load_config(Some(&path)),
            Err(ConfigError::ParseError(_))
        ));
    }

    #[test]
    fn include_merges_in_order() {
        let dir = temp_dir();
        fs::write(dir.join("a.toml"), "[network]\nport = 6380\n").unwrap();
        fs::write(dir.join("b.toml"), "[network]\nport = 6381\n").unwrap();
        fs::write(
            dir.join("root.toml"),
            "[general]\ninclude = [\"a.toml\", \"b.toml\"]\n",
        )
        .unwrap();
        let config = load_config(Some(&dir.join("root.toml"))).unwrap();
        assert_eq!(config.network.port, 6381);
    }

    #[test]
    fn include_glob_is_alphabetical() {
        let dir = temp_dir();
        fs::write(dir.join("01.toml"), "[network]\nport = 6380\n").unwrap();
        fs::write(dir.join("02.toml"), "[network]\nport = 6381\n").unwrap();
        fs::write(
            dir.join("root.toml"),
            "[general]\ninclude = [\"*.toml\"]\n[network]\nport = 6382\n",
        )
        .unwrap();
        let config = load_config(Some(&dir.join("root.toml"))).unwrap();
        assert_eq!(config.network.port, 6382);
    }

    #[test]
    fn include_depth_limit_is_enforced() {
        let dir = temp_dir();
        for idx in 0..10 {
            let next = if idx == 9 {
                String::new()
            } else {
                format!("[general]\ninclude = [\"{}.toml\"]\n", idx + 1)
            };
            fs::write(dir.join(format!("{idx}.toml")), next).unwrap();
        }
        assert!(matches!(
            load_config(Some(&dir.join("0.toml"))),
            Err(ConfigError::IncludeError { .. }) | Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn aclfile_and_users_conflict() {
        let dir = temp_dir();
        let path = dir.join("senko.toml");
        fs::write(
            &path,
            "[security]\naclfile = \"users.acl\"\nusers = [\"user alice on\"]\n",
        )
        .unwrap();
        assert!(matches!(
            load_config(Some(&path)),
            Err(ConfigError::ConflictError(_))
        ));
    }

    #[test]
    fn validation_rejects_missing_listener() {
        let mut config = SenkoConfig::default();
        config.network.port = 0;
        config.network.unixsocket = None;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn validation_rejects_tls_without_cert() {
        let mut config = SenkoConfig::default();
        config.tls.port = 6380;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn validation_rejects_bad_ring_size() {
        let mut config = SenkoConfig::default();
        config.pubsub.subscriber_ring_size = 100;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn validation_rejects_bad_hz() {
        let mut config = SenkoConfig::default();
        config.general.hz = 0;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn validation_rejects_bad_stream_duration() {
        let mut config = SenkoConfig::default();
        config.encoding.stream_idmp_duration = 0;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn byte_size_deserializes_expected_units() {
        #[derive(Deserialize)]
        struct Wrapper {
            v: ByteSize,
        }

        let one_gb: Wrapper = toml::from_str("v = \"1gb\"").unwrap();
        let five_hundred_twelve_mb: Wrapper = toml::from_str("v = \"512mb\"").unwrap();
        let one_g_redis = ByteSize::from_str("1g").unwrap();
        let one_gb_caps = ByteSize::from_str("1GB").unwrap();
        let integer = ByteSize::from_str("1073741824").unwrap();

        assert_eq!(one_gb.v, ByteSize(1024_u64.pow(3)));
        assert_eq!(five_hundred_twelve_mb.v, ByteSize(512 * 1024 * 1024));
        assert_eq!(one_g_redis, ByteSize(1_000_000_000));
        assert_eq!(one_gb_caps, ByteSize(1024_u64.pow(3)));
        assert_eq!(integer, ByteSize(1024_u64.pow(3)));
    }

    #[test]
    fn config_get_returns_canonical_and_alias() {
        let config = SenkoConfig::default();
        let got = config_get(&config, "port");
        assert!(got.contains(&(String::from("network.port"), String::from("6379"))));
        assert!(got.contains(&(String::from("port"), String::from("6379"))));
    }

    #[test]
    fn config_get_glob_matches_many_keys() {
        let config = SenkoConfig::default();
        let got = config_get(&config, "max*");
        assert!(got.iter().any(|(key, _)| key == "maxmemory"));
        assert!(got.iter().any(|(key, _)| key == "memory.maxmemory"));
    }

    #[test]
    fn config_set_updates_mutable_key() {
        let mut config = SenkoConfig::default();
        config_set(&mut config, "maxmemory", "1gb").unwrap();
        assert_eq!(config.memory.maxmemory, ByteSize(1024_u64.pow(3)));
    }

    #[test]
    fn config_set_rejects_immutable_key() {
        let mut config = SenkoConfig::default();
        assert!(matches!(
            config_set(&mut config, "port", "6380"),
            Err(ConfigError::ValidationError(_))
        ));
    }

    #[test]
    fn config_get_nonexistent_is_empty() {
        let config = SenkoConfig::default();
        assert!(config_get(&config, "nonexistent").is_empty());
    }
}
