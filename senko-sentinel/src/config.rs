use std::{
    collections::HashMap,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use arc_swap::ArcSwap;
use compact_str::CompactString;
use senko_core::config::LogLevel;
use serde::{Deserialize, Serialize};

use crate::{
    conf_parser::parse_sentinel_conf,
    conf_writer::{flush_sentinel_conf_to_disk, write_sentinel_conf},
    runid::load_or_generate_runid,
    state::WorldSnapshot,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SentinelConfig {
    pub network: SentinelNetworkConfig,
    pub general: SentinelGeneralConfig,
    pub security: SentinelSecurityConfig,
    pub masters: Vec<MasterConfig>,
    pub runtime: SentinelRuntimeConfig,
    #[serde(skip)]
    pub config_file: Option<PathBuf>,
    #[serde(skip)]
    pub source_format: ConfigFormat,
    #[serde(skip)]
    pub unknown_directives: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConfigFormat {
    #[default]
    Toml,
    SentinelConf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SentinelNetworkConfig {
    pub port: u16,
    pub bind: Vec<String>,
    pub protected_mode: bool,
    pub announce_ip: Option<String>,
    pub announce_port: Option<u16>,
    pub resolve_hostnames: bool,
    pub announce_hostnames: bool,
    pub unixsocket: Option<PathBuf>,
    pub unixsocketperm: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SentinelGeneralConfig {
    pub dir: PathBuf,
    pub daemonize: bool,
    pub pidfile: Option<PathBuf>,
    pub loglevel: LogLevel,
    pub logfile: Option<PathBuf>,
    pub syslog_enabled: bool,
    pub syslog_ident: String,
    pub syslog_facility: String,
    pub sentinel_hz: u32,
    pub id_file: Option<PathBuf>,
    pub ignore_warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SentinelSecurityConfig {
    pub requirepass: Option<String>,
    pub aclfile: Option<PathBuf>,
    pub acllog_max_len: usize,
    pub users: Vec<String>,
    pub sentinel_user: Option<String>,
    pub sentinel_pass: Option<String>,
    pub deny_scripts_reconfig: bool,
    pub enable_debug_command: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MasterConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub quorum: u32,
    pub down_after_milliseconds: u64,
    pub parallel_syncs: u32,
    pub failover_timeout: u64,
    pub auth_pass: Option<String>,
    pub auth_user: Option<String>,
    pub notification_script: Option<PathBuf>,
    pub client_reconfig_script: Option<PathBuf>,
    pub rename_commands: HashMap<String, String>,
    pub master_reboot_down_after_period: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SentinelRuntimeConfig {
    pub current_epoch: u64,
    pub myid: Option<String>,
    pub masters: HashMap<String, MasterRuntimeState>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MasterRuntimeState {
    pub name: String,
    pub current_host: String,
    pub current_port: u16,
    pub config_epoch: u64,
    pub known_replicas: Vec<KnownReplica>,
    pub known_sentinels: Vec<KnownSentinel>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct KnownReplica {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct KnownSentinel {
    pub host: String,
    pub port: u16,
    pub runid: String,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(io::Error),
    TomlParse(toml::de::Error),
    TomlSerialize(toml::ser::Error),
    ConfParse { line: usize, message: String },
    Validation(String),
    Conflict(String),
    UnknownMaster(String),
    ScriptNotFound(PathBuf),
    ScriptNotExecutable(PathBuf),
}

pub type SentinelError = ConfigError;

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::TomlParse(error) => write!(f, "toml parse error: {error}"),
            Self::TomlSerialize(error) => write!(f, "toml serialize error: {error}"),
            Self::ConfParse { line, message } => {
                write!(f, "sentinel.conf parse error on line {line}: {message}")
            }
            Self::Validation(message) => f.write_str(message),
            Self::Conflict(message) => f.write_str(message),
            Self::UnknownMaster(name) => {
                write!(f, "directive references unknown master: {name}")
            }
            Self::ScriptNotFound(path) => write!(f, "script not found: {}", path.display()),
            Self::ScriptNotExecutable(path) => {
                write!(f, "script is not executable: {}", path.display())
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::TomlParse(error) => Some(error),
            Self::TomlSerialize(error) => Some(error),
            Self::ConfParse { .. }
            | Self::Validation(_)
            | Self::Conflict(_)
            | Self::UnknownMaster(_)
            | Self::ScriptNotFound(_)
            | Self::ScriptNotExecutable(_) => None,
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            network: SentinelNetworkConfig::default(),
            general: SentinelGeneralConfig::default(),
            security: SentinelSecurityConfig::default(),
            masters: Vec::new(),
            runtime: SentinelRuntimeConfig::default(),
            config_file: None,
            source_format: ConfigFormat::Toml,
            unknown_directives: Vec::new(),
        }
    }
}

impl Default for SentinelNetworkConfig {
    fn default() -> Self {
        Self {
            port: 26_379,
            bind: Vec::new(),
            protected_mode: false,
            announce_ip: None,
            announce_port: None,
            resolve_hostnames: false,
            announce_hostnames: false,
            unixsocket: None,
            unixsocketperm: 0o700,
        }
    }
}

impl Default for SentinelGeneralConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/tmp"),
            daemonize: false,
            pidfile: Some(PathBuf::from("/var/run/redis-sentinel.pid")),
            loglevel: LogLevel::Notice,
            logfile: None,
            syslog_enabled: false,
            syslog_ident: "sentinel".to_owned(),
            syslog_facility: "local0".to_owned(),
            sentinel_hz: 1_000,
            id_file: None,
            ignore_warnings: Vec::new(),
        }
    }
}

impl Default for SentinelSecurityConfig {
    fn default() -> Self {
        Self {
            requirepass: None,
            aclfile: None,
            acllog_max_len: 128,
            users: Vec::new(),
            sentinel_user: None,
            sentinel_pass: None,
            deny_scripts_reconfig: true,
            enable_debug_command: false,
        }
    }
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            host: String::new(),
            port: 6379,
            quorum: 2,
            down_after_milliseconds: 30_000,
            parallel_syncs: 1,
            failover_timeout: 180_000,
            auth_pass: None,
            auth_user: None,
            notification_script: None,
            client_reconfig_script: None,
            rename_commands: HashMap::new(),
            master_reboot_down_after_period: 0,
        }
    }
}

impl SentinelConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.masters.is_empty() {
            return Err(ConfigError::Validation(
                "sentinel requires at least one monitored master".to_owned(),
            ));
        }
        if self.security.requirepass.is_some() && self.security.aclfile.is_some() {
            return Err(ConfigError::Conflict(
                "requirepass and aclfile are mutually exclusive".to_owned(),
            ));
        }
        for master in &self.masters {
            validate_master(master)?;
            if self.network.port == master.port
                && matches!(
                    master.host.as_str(),
                    "127.0.0.1" | "localhost" | "0.0.0.0" | "::1"
                )
            {
                eprintln!(
                    "warning: sentinel port {} matches monitored master {}:{}",
                    self.network.port, master.host, master.port
                );
            }
        }
        if self.security.sentinel_pass.is_some() && self.security.requirepass.is_none() {
            eprintln!(
                "warning: sentinel-pass is set without requirepass; inter-sentinel auth may fail"
            );
        }
        Ok(())
    }

    pub fn id_file_path(&self) -> PathBuf {
        self.general
            .id_file
            .clone()
            .unwrap_or_else(|| self.general.dir.join("sentinel-id"))
    }

    pub fn load_or_create_id(&self) -> Result<CompactString, ConfigError> {
        if let Some(myid) = &self.runtime.myid {
            return Ok(CompactString::from(myid.as_str()));
        }
        let runid = load_or_generate_runid(&self.id_file_path())?;
        Ok(CompactString::from(runid.as_hex()))
    }

    pub fn down_after_milliseconds(&self, master_name: &str) -> u64 {
        self.masters
            .iter()
            .find(|master| master.name == master_name)
            .map(|master| master.down_after_milliseconds)
            .unwrap_or(30_000)
    }

    pub fn failover_timeout(&self, master_name: &str) -> u64 {
        self.masters
            .iter()
            .find(|master| master.name == master_name)
            .map(|master| master.failover_timeout)
            .unwrap_or(180_000)
    }

    pub fn parallel_syncs(&self, master_name: &str) -> u32 {
        self.masters
            .iter()
            .find(|master| master.name == master_name)
            .map(|master| master.parallel_syncs)
            .unwrap_or(1)
    }

    pub fn requirepass(&self) -> Option<&str> {
        self.security.requirepass.as_deref()
    }

    pub fn port(&self) -> u16 {
        self.network.port
    }

    pub fn bind_addrs(&self) -> &[String] {
        &self.network.bind
    }

    pub fn sentinel_hz(&self) -> u32 {
        self.general.sentinel_hz
    }

    pub fn find_master(&self, name: &str) -> Option<&MasterConfig> {
        self.masters.iter().find(|master| master.name == name)
    }

    pub fn find_master_mut(&mut self, name: &str) -> Option<&mut MasterConfig> {
        self.masters.iter_mut().find(|master| master.name == name)
    }

    pub fn effective_master_addr<'a>(&'a self, master: &'a MasterConfig) -> (&'a str, u16) {
        self.runtime
            .masters
            .get(&master.name)
            .map(|state| {
                if !state.current_host.is_empty() && state.current_port != 0 {
                    (state.current_host.as_str(), state.current_port)
                } else {
                    (master.host.as_str(), master.port)
                }
            })
            .unwrap_or((master.host.as_str(), master.port))
    }

    pub fn normalized_toml(&self) -> Result<String, ConfigError> {
        let mut rendered = toml::to_string_pretty(self).map_err(ConfigError::TomlSerialize)?;
        if !self.unknown_directives.is_empty() {
            let mut comments =
                String::from("# Unknown sentinel.conf directives preserved during conversion:\n");
            for line in &self.unknown_directives {
                comments.push_str("# ");
                comments.push_str(line);
                comments.push('\n');
            }
            comments.push('\n');
            comments.push_str(&rendered);
            rendered = comments;
        }
        Ok(rendered)
    }
}

pub fn load_config(path: &Path) -> Result<SentinelConfig, ConfigError> {
    load_sentinel_config(path)
}

pub fn load_sentinel_config(path: &Path) -> Result<SentinelConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let format = detect_format(path, &content);
    let mut config = match format {
        ConfigFormat::Toml => toml::from_str::<SentinelConfig>(&content)
            .map_err(ConfigError::TomlParse)
            .or_else(|error| {
                if detect_conf_fallback(&content) {
                    parse_sentinel_conf(&content)
                } else {
                    Err(error)
                }
            })?,
        ConfigFormat::SentinelConf => parse_sentinel_conf(&content).or_else(|error| {
            toml::from_str::<SentinelConfig>(&content)
                .map_err(ConfigError::TomlParse)
                .or(Err(error))
        })?,
    };
    config.config_file = Some(path.to_path_buf());
    config.source_format = format;
    config.validate()?;
    let runid = load_or_generate_runid(&config.id_file_path())?;
    config.runtime.myid = Some(runid.as_hex().to_owned());
    if config.runtime.current_epoch == 0 {
        config.runtime.current_epoch = config
            .runtime
            .masters
            .values()
            .map(|state| state.config_epoch)
            .max()
            .unwrap_or(0);
    }
    Ok(config)
}

fn detect_format(path: &Path, content: &str) -> ConfigFormat {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("toml") => return ConfigFormat::Toml,
        Some("conf") => return ConfigFormat::SentinelConf,
        _ => {}
    }
    if let Some(line) = first_meaningful_line(content) {
        if line.contains('=') {
            return ConfigFormat::Toml;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("port ") || lower.starts_with("sentinel monitor") {
            return ConfigFormat::SentinelConf;
        }
    }
    ConfigFormat::Toml
}

fn detect_conf_fallback(content: &str) -> bool {
    first_meaningful_line(content)
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            lower.starts_with("port ") || lower.starts_with("sentinel ")
        })
        .unwrap_or(false)
}

fn first_meaningful_line(content: &str) -> Option<&str> {
    content
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
}

fn validate_master(master: &MasterConfig) -> Result<(), ConfigError> {
    if master.name.is_empty() {
        return Err(ConfigError::Validation(
            "master name cannot be empty".to_owned(),
        ));
    }
    if !master
        .name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
    {
        return Err(ConfigError::Validation(format!(
            "invalid master name: {}",
            master.name
        )));
    }
    if master.quorum == 0 {
        return Err(ConfigError::Validation(format!(
            "master {} quorum must be at least 1",
            master.name
        )));
    }
    if master.parallel_syncs == 0 {
        return Err(ConfigError::Validation(format!(
            "master {} parallel-syncs must be at least 1",
            master.name
        )));
    }
    if master.down_after_milliseconds < 100 {
        eprintln!(
            "warning: master {} down-after-milliseconds={} is extremely aggressive",
            master.name, master.down_after_milliseconds
        );
    }
    if master.failover_timeout < master.down_after_milliseconds {
        eprintln!(
            "warning: master {} failover-timeout={} is below down-after-milliseconds={}",
            master.name, master.failover_timeout, master.down_after_milliseconds
        );
    }
    if master.master_reboot_down_after_period != 0 && master.master_reboot_down_after_period < 1_000
    {
        return Err(ConfigError::Validation(format!(
            "master {} master-reboot-down-after-period must be 0 or >= 1000",
            master.name
        )));
    }
    for renamed in master.rename_commands.values() {
        if renamed.is_empty() {
            return Err(ConfigError::Validation(format!(
                "master {} rename-command values must not be empty",
                master.name
            )));
        }
    }
    validate_script(master.notification_script.as_deref())?;
    validate_script(master.client_reconfig_script.as_deref())?;
    Ok(())
}

fn validate_script(path: Option<&Path>) -> Result<(), ConfigError> {
    let Some(path) = path else {
        return Ok(());
    };
    if !path.exists() {
        return Err(ConfigError::ScriptNotFound(path.to_path_buf()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)?.permissions().mode();
        if mode & 0o111 == 0 {
            return Err(ConfigError::ScriptNotExecutable(path.to_path_buf()));
        }
    }
    Ok(())
}

pub fn merge_runtime_state(config: &SentinelConfig, snapshot: &WorldSnapshot) -> SentinelConfig {
    let mut merged = config.clone();
    merged.runtime.current_epoch = snapshot.epoch;
    merged.runtime.myid = Some(snapshot.my_id.to_string());
    merged.runtime.masters.clear();
    for master in snapshot.masters.values() {
        merged.runtime.masters.insert(
            master.name.clone(),
            MasterRuntimeState {
                name: master.name.clone(),
                current_host: master.addr.ip().to_string(),
                current_port: master.addr.port(),
                config_epoch: master.config_epoch,
                known_replicas: master
                    .replicas
                    .values()
                    .map(|replica| KnownReplica {
                        host: replica.addr.ip().to_string(),
                        port: replica.addr.port(),
                    })
                    .collect(),
                known_sentinels: master
                    .sentinels
                    .values()
                    .map(|sentinel| KnownSentinel {
                        host: sentinel.addr.ip().to_string(),
                        port: sentinel.addr.port(),
                        runid: sentinel.runid.clone(),
                    })
                    .collect(),
            },
        );
    }
    merged
}

pub fn live_state_path(config: &SentinelConfig) -> PathBuf {
    match (&config.source_format, &config.config_file) {
        (ConfigFormat::SentinelConf, Some(path)) => path.clone(),
        (_, Some(path)) => path.with_extension("conf"),
        (_, None) => config.general.dir.join("sentinel.conf"),
    }
}

pub fn flush_config_atomic(
    config: &SentinelConfig,
    snapshot: &WorldSnapshot,
) -> Result<(), ConfigError> {
    flush_live_config(&merge_runtime_state(config, snapshot))
}

pub fn flush_live_config(config: &SentinelConfig) -> Result<(), ConfigError> {
    let output = write_sentinel_conf(config);
    flush_sentinel_conf_to_disk(&live_state_path(config), &output)
}

pub fn sentinel_set(
    config: &Arc<ArcSwap<SentinelConfig>>,
    master_name: &str,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    let current = config.load_full();
    let mut next = (*current).clone();
    apply_master_mutation(&mut next, master_name, option, value)?;
    next.validate()?;
    let cloned = next.clone();
    config.store(Arc::new(next));
    std::thread::spawn(move || {
        let _ = flush_live_config(&cloned);
    });
    Ok(())
}

pub fn sentinel_config_set(
    config: &Arc<ArcSwap<SentinelConfig>>,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    let current = config.load_full();
    let mut next = (*current).clone();
    apply_global_mutation(&mut next, option, value)?;
    next.validate()?;
    let cloned = next.clone();
    config.store(Arc::new(next));
    std::thread::spawn(move || {
        let _ = flush_live_config(&cloned);
    });
    Ok(())
}

pub fn apply_global_override(
    config: &mut SentinelConfig,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    apply_global_mutation(config, option, value)?;
    config.validate()
}

pub fn apply_master_override(
    config: &mut SentinelConfig,
    master_name: &str,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    apply_master_mutation(config, master_name, option, value)?;
    config.validate()
}

pub fn upsert_master(
    config: &mut SentinelConfig,
    master: MasterConfig,
) -> Result<(), SentinelError> {
    validate_master(&master)?;
    if let Some(existing) = config.find_master_mut(&master.name) {
        *existing = master;
    } else {
        config.masters.push(master);
    }
    config.validate()
}

fn apply_master_mutation(
    config: &mut SentinelConfig,
    master_name: &str,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    let deny_scripts = config.security.deny_scripts_reconfig;
    let master = config
        .find_master_mut(master_name)
        .ok_or_else(|| ConfigError::UnknownMaster(master_name.to_owned()))?;
    match option.to_ascii_lowercase().as_str() {
        "down-after-milliseconds" => {
            master.down_after_milliseconds = value.parse().map_err(invalid_value)?
        }
        "failover-timeout" => master.failover_timeout = value.parse().map_err(invalid_value)?,
        "parallel-syncs" => master.parallel_syncs = value.parse().map_err(invalid_value)?,
        "quorum" => master.quorum = value.parse().map_err(invalid_value)?,
        "auth-pass" => master.auth_pass = Some(value.to_owned()),
        "auth-user" => master.auth_user = Some(value.to_owned()),
        "notification-script" => {
            if deny_scripts {
                return Err(ConfigError::Conflict(
                    "scripts reconfiguration denied by deny-scripts-reconfig".to_owned(),
                ));
            }
            master.notification_script = Some(PathBuf::from(value));
        }
        "client-reconfig-script" => {
            if deny_scripts {
                return Err(ConfigError::Conflict(
                    "scripts reconfiguration denied by deny-scripts-reconfig".to_owned(),
                ));
            }
            master.client_reconfig_script = Some(PathBuf::from(value));
        }
        "master-reboot-down-after-period" => {
            master.master_reboot_down_after_period = value.parse().map_err(invalid_value)?;
        }
        "rename-command" => {
            let mut parts = value.split_whitespace();
            let command = parts.next().ok_or_else(|| {
                ConfigError::Validation("rename-command requires two values".to_owned())
            })?;
            let renamed = parts.next().ok_or_else(|| {
                ConfigError::Validation("rename-command requires two values".to_owned())
            })?;
            if parts.next().is_some() {
                return Err(ConfigError::Validation(
                    "rename-command takes exactly two values".to_owned(),
                ));
            }
            master
                .rename_commands
                .insert(command.to_ascii_uppercase(), renamed.to_owned());
        }
        _ => {
            return Err(ConfigError::Validation(format!(
                "Unknown option for SENTINEL SET: {option}"
            )));
        }
    }
    Ok(())
}

fn apply_global_mutation(
    config: &mut SentinelConfig,
    option: &str,
    value: &str,
) -> Result<(), SentinelError> {
    match option.to_ascii_lowercase().as_str() {
        "sentinel-hz" => {
            let parsed: u32 = value.parse().map_err(invalid_value)?;
            if !(10..=10_000).contains(&parsed) {
                return Err(ConfigError::Validation(
                    "sentinel-hz must be between 10 and 10000".to_owned(),
                ));
            }
            config.general.sentinel_hz = parsed;
        }
        "resolve-hostnames" => config.network.resolve_hostnames = parse_yes_no(value)?,
        "announce-hostnames" => config.network.announce_hostnames = parse_yes_no(value)?,
        "announce-ip" => config.network.announce_ip = Some(value.to_owned()),
        "announce-port" => {
            config.network.announce_port = Some(value.parse().map_err(invalid_value)?)
        }
        "sentinel-user" => config.security.sentinel_user = Some(value.to_owned()),
        "sentinel-pass" => config.security.sentinel_pass = Some(value.to_owned()),
        "loglevel" => {
            config.general.loglevel = match value.to_ascii_lowercase().as_str() {
                "debug" => LogLevel::Debug,
                "verbose" => LogLevel::Verbose,
                "notice" => LogLevel::Notice,
                "warning" => LogLevel::Warning,
                "nothing" => LogLevel::Nothing,
                _ => {
                    return Err(ConfigError::Validation(format!(
                        "invalid loglevel: {value}"
                    )));
                }
            };
        }
        _ => {
            return Err(ConfigError::Validation(format!(
                "Unknown option for SENTINEL CONFIG SET: {option}"
            )));
        }
    }
    Ok(())
}

fn parse_yes_no(value: &str) -> Result<bool, ConfigError> {
    match value.to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(ConfigError::Validation(format!(
            "expected yes/no, got {value}"
        ))),
    }
}

fn invalid_value<T: fmt::Display>(error: T) -> ConfigError {
    ConfigError::Validation(format!("invalid value: {error}"))
}

pub const DEFAULT_SENTINEL_TOML: &str = r#"# Senko Sentinel Configuration
# Run with: senkodb --sentinel sentinel.toml
# Or Redis-compat: senkodb /path/to/sentinel.conf

[network]
port = 26379
# bind = ["127.0.0.1"]   # default: all interfaces
protected_mode = false
# announce_ip = "1.2.3.4"
# announce_port = 26379
resolve_hostnames = false
announce_hostnames = false

[general]
dir = "/tmp"
daemonize = false
# pidfile = "/var/run/senko-sentinel.pid"
loglevel = "notice"
# logfile = "/var/log/senko-sentinel.log"
sentinel_hz = 1000

[security]
# requirepass = "strongpassword"
# aclfile = "/etc/senko/sentinel-users.acl"
acllog_max_len = 128
deny_scripts_reconfig = true

# sentinel_user = "sentinel-user"
# sentinel_pass = "sentinelpass"

[[masters]]
name = "mymaster"
host = "127.0.0.1"
port = 6379
quorum = 2
down_after_milliseconds = 30000
parallel_syncs = 1
failover_timeout = 180000

# auth_pass = "redispassword"
# auth_user = "sentinel-user"
# notification_script = "/var/redis/notify.sh"
# client_reconfig_script = "/var/redis/reconfig.sh"
# master_reboot_down_after_period = 0

# [masters.rename_commands]
# CONFIG = "GUESSME"
# SLAVEOF = "DOITALL"
"#;

pub fn render_default_config_toml() -> String {
    DEFAULT_SENTINEL_TOML.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arc_swap::ArcSwap;

    fn unique_dir() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("senko-sentinel-config-{ts}"));
        let _ = fs::create_dir_all(&dir);
        dir
    }

    #[test]
    fn load_toml_and_defaults() {
        let dir = unique_dir();
        let path = dir.join("sentinel.toml");
        fs::write(
            &path,
            r#"
                [[masters]]
                name = "mymaster"
                host = "127.0.0.1"
                port = 6379
                quorum = 2
            "#,
        )
        .expect("write config");
        let config = load_sentinel_config(&path).expect("load config");
        assert_eq!(config.port(), 26_379);
        assert_eq!(config.down_after_milliseconds("mymaster"), 30_000);
        assert_eq!(config.source_format, ConfigFormat::Toml);
        assert!(config.runtime.myid.is_some());
    }

    #[test]
    fn conflict_requirepass_and_aclfile_is_rejected() {
        let mut config = SentinelConfig::default();
        config.security.requirepass = Some("a".to_owned());
        config.security.aclfile = Some(PathBuf::from("users.acl"));
        config.masters.push(MasterConfig {
            name: "m".to_owned(),
            host: "127.0.0.1".to_owned(),
            ..MasterConfig::default()
        });
        let error = config.validate().expect_err("should fail");
        assert!(matches!(error, ConfigError::Conflict(_)));
    }

    #[test]
    fn sentinel_set_swaps_config_for_readers() {
        let mut base = SentinelConfig::default();
        base.masters.push(MasterConfig {
            name: "m".to_owned(),
            host: "127.0.0.1".to_owned(),
            ..MasterConfig::default()
        });
        let config = Arc::new(ArcSwap::from_pointee(base));
        let seen_new = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let config = config.clone();
            let seen_new = seen_new.clone();
            readers.push(thread::spawn(move || {
                for _ in 0..1_000 {
                    let snapshot = config.load_full();
                    if snapshot.down_after_milliseconds("m") == 45_000 {
                        seen_new.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        sentinel_set(&config, "m", "down-after-milliseconds", "45000").expect("set value");
        for reader in readers {
            reader.join().expect("reader");
        }
        assert_eq!(config.load().down_after_milliseconds("m"), 45_000);
        assert!(seen_new.load(Ordering::Relaxed) > 0);
    }
}
