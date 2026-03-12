use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};

use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use senko_core::{
    ConfigError, SenkoConfig, config::CONFIG_ALIASES, config_set_startup, load_config,
    validate_config,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub config: Option<PathBuf>,
    pub command: Option<Commands>,
    pub overrides: Vec<CliOverride>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliOverride {
    pub key: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commands {
    Start,
    CheckConfig {
        file: PathBuf,
    },
    DefaultConfig,
    Version,
    ConvertConfig {
        input: PathBuf,
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerOptionSpec {
    key: String,
    long: String,
    aliases: Vec<String>,
}

impl Cli {
    pub fn parse() -> Self {
        Self::try_parse_from(env::args_os()).unwrap_or_else(|error| error.exit())
    }

    pub fn try_parse_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let matches = build_command().try_get_matches_from(args)?;
        Ok(parse_matches(&matches))
    }
}

pub fn resolve_config_path(cli: &Cli) -> Option<PathBuf> {
    if let Some(path) = &cli.config {
        return Some(path.clone());
    }
    auto_detect_config_path()
}

pub fn auto_detect_config_path() -> Option<PathBuf> {
    if let Ok(path) = env::var("SENKO_CONFIG") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Some(path);
        }
    }
    let candidates = [
        PathBuf::from("./senko.toml"),
        PathBuf::from("./redis.conf"),
        PathBuf::from("/etc/senko/senko.toml"),
        dirs_config_home().join("senko/senko.toml"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

pub fn load_effective_config(cli: &Cli) -> Result<SenkoConfig, ConfigError> {
    let path = resolve_config_path(cli);
    let mut config = load_config(path.as_deref())?;
    apply_cli_overrides_checked(&mut config, &cli.overrides)?;
    validate_config(&config)?;
    Ok(config)
}

pub fn apply_cli_overrides_checked(
    config: &mut SenkoConfig,
    overrides: &[CliOverride],
) -> Result<(), ConfigError> {
    for override_ in overrides {
        let value = render_override_value(override_);
        config_set_startup(config, &override_.key, &value)?;
    }
    Ok(())
}

fn build_command() -> Command {
    let mut command = Command::new("senkodb")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Senko — a flash of light in the darkness. Redis-compatible in-memory store.")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .env("SENKO_CONFIG")
                .value_parser(value_parser!(PathBuf))
                .help("Path to senko.toml or redis.conf"),
        )
        .arg(
            Arg::new("config-path")
                .value_name("FILE")
                .value_parser(value_parser!(PathBuf))
                .conflicts_with("config")
                .help("Config file path (Redis-compatible positional form)"),
        )
        .subcommand(Command::new("start"))
        .subcommand(
            Command::new("check-config").arg(
                Arg::new("file")
                    .value_name("FILE")
                    .required(true)
                    .value_parser(value_parser!(PathBuf)),
            ),
        )
        .subcommand(Command::new("default-config"))
        .subcommand(Command::new("version"))
        .subcommand(
            Command::new("convert-config")
                .arg(
                    Arg::new("input")
                        .value_name("REDIS_CONF")
                        .required(true)
                        .value_parser(value_parser!(PathBuf)),
                )
                .arg(
                    Arg::new("output")
                        .short('o')
                        .long("output")
                        .value_name("OUTPUT")
                        .value_parser(value_parser!(PathBuf)),
                ),
        );

    for spec in server_option_specs() {
        let mut arg = Arg::new(leak(spec.key.clone()))
            .long(leak(spec.long.clone()))
            .action(ArgAction::Append)
            .num_args(1)
            .allow_hyphen_values(true)
            .env(leak(config_env_name(&spec.long)))
            .value_name("VALUE")
            .help("Override config value");
        for alias in spec.aliases {
            arg = arg.visible_alias(leak(alias));
        }
        command = command.arg(arg);
    }

    command
}

fn parse_matches(matches: &ArgMatches) -> Cli {
    let config = matches
        .get_one::<PathBuf>("config")
        .cloned()
        .or_else(|| matches.get_one::<PathBuf>("config-path").cloned());
    let command = parse_command(matches);
    let overrides = server_option_specs()
        .into_iter()
        .filter_map(|spec| {
            matches
                .get_many::<String>(&spec.key)
                .map(|values| CliOverride {
                    key: spec.key,
                    values: values.cloned().collect(),
                })
        })
        .collect();
    Cli {
        config,
        command,
        overrides,
    }
}

fn parse_command(matches: &ArgMatches) -> Option<Commands> {
    match matches.subcommand() {
        Some(("start", _)) => Some(Commands::Start),
        Some(("check-config", sub)) => Some(Commands::CheckConfig {
            file: sub
                .get_one::<PathBuf>("file")
                .expect("required by clap")
                .clone(),
        }),
        Some(("default-config", _)) => Some(Commands::DefaultConfig),
        Some(("version", _)) => Some(Commands::Version),
        Some(("convert-config", sub)) => Some(Commands::ConvertConfig {
            input: sub
                .get_one::<PathBuf>("input")
                .expect("required by clap")
                .clone(),
            output: sub.get_one::<PathBuf>("output").cloned(),
        }),
        _ => None,
    }
}

fn render_override_value(override_: &CliOverride) -> String {
    match override_.key.as_str() {
        "persistence.save" => override_.values.join(" "),
        "security.client_output_buffer_limit" => override_.values.join(";"),
        _ if override_.values.len() > 1 => override_.values.join(","),
        _ => override_.values.last().cloned().unwrap_or_default(),
    }
}

fn server_option_specs() -> Vec<ServerOptionSpec> {
    let alias_groups = alias_groups();
    SERVER_CONFIG_KEYS
        .iter()
        .copied()
        .map(str::to_owned)
        .map(|key| {
            let aliases = alias_groups.get(key.as_str()).cloned().unwrap_or_default();
            let canonical_flag = canonical_flag_name(&key);
            let long = aliases
                .iter()
                .min_by_key(|alias| alias.len())
                .cloned()
                .unwrap_or_else(|| canonical_flag.clone());
            let mut extra_aliases = aliases
                .into_iter()
                .filter(|alias| alias != &long)
                .collect::<Vec<_>>();
            if canonical_flag != long && !extra_aliases.iter().any(|alias| alias == &canonical_flag)
            {
                extra_aliases.push(canonical_flag);
            }
            ServerOptionSpec {
                key,
                long,
                aliases: extra_aliases,
            }
        })
        .collect()
}

fn alias_groups() -> BTreeMap<String, Vec<String>> {
    let mut groups = BTreeMap::<String, Vec<String>>::new();
    for (alias, canonical) in CONFIG_ALIASES.entries() {
        groups
            .entry((*canonical).to_owned())
            .or_default()
            .push((*alias).to_owned());
    }
    groups
}

fn canonical_flag_name(key: &str) -> String {
    key.replace('.', "-").replace('_', "-")
}

fn config_env_name(long: &str) -> String {
    format!("SENKO_{}", long.replace('-', "_").to_ascii_uppercase())
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

const SERVER_CONFIG_KEYS: &[&str] = &[
    "network.bind",
    "network.port",
    "network.unixsocket",
    "network.unixsocketperm",
    "network.tcp_backlog",
    "network.timeout",
    "network.tcp_keepalive",
    "network.protected_mode",
    "network.bind_source_addr",
    "network.io_threads",
    "network.so_reuseport",
    "network.max_new_connections_per_cycle",
    "tls.port",
    "tls.cert_file",
    "tls.key_file",
    "tls.key_file_pass",
    "tls.ca_cert_file",
    "tls.ca_cert_dir",
    "tls.auth_clients",
    "tls.auth_clients_user",
    "tls.replication",
    "tls.cluster",
    "tls.protocols",
    "tls.ciphers",
    "tls.ciphersuites",
    "tls.prefer_server_ciphers",
    "tls.session_caching",
    "tls.session_cache_size",
    "tls.session_cache_timeout",
    "general.daemonize",
    "general.pidfile",
    "general.loglevel",
    "general.logfile",
    "general.syslog_enabled",
    "general.syslog_ident",
    "general.syslog_facility",
    "general.databases",
    "general.always_show_logo",
    "general.set_proc_title",
    "general.proc_title_template",
    "general.hz",
    "general.dynamic_hz",
    "general.activerehashing",
    "general.disable_thp",
    "general.oom_score_adj",
    "general.oom_score_adj_values",
    "general.include",
    "general.ignore_warnings",
    "security.requirepass",
    "security.aclfile",
    "security.acllog_max_len",
    "security.acl_pubsub_default",
    "security.users",
    "security.enable_protected_configs",
    "security.enable_debug_command",
    "security.enable_module_command",
    "security.client_output_buffer_limit",
    "security.client_query_buffer_limit",
    "security.proto_max_bulk_len",
    "security.tracking_table_max_keys",
    "persistence.save",
    "persistence.stop_writes_on_bgsave_error",
    "persistence.rdbcompression",
    "persistence.rdbchecksum",
    "persistence.dbfilename",
    "persistence.dir",
    "persistence.rdb_del_sync_files",
    "persistence.sanitize_dump_payload",
    "replication.replicaof",
    "replication.masterauth",
    "replication.masteruser",
    "replication.replica_serve_stale_data",
    "replication.replica_read_only",
    "replication.repl_diskless_sync",
    "replication.repl_diskless_sync_delay",
    "replication.repl_diskless_sync_max_replicas",
    "replication.repl_diskless_load",
    "replication.repl_ping_replica_period",
    "replication.repl_timeout",
    "replication.repl_disable_tcp_nodelay",
    "replication.repl_backlog_size",
    "replication.repl_backlog_ttl",
    "replication.replica_priority",
    "replication.min_replicas_to_write",
    "replication.min_replicas_max_lag",
    "replication.replica_announce_ip",
    "replication.replica_announce_port",
    "replication.propagation_error_behavior",
    "replication.replica_ignore_maxmemory",
    "replication.replica_full_sync_buffer_limit",
    "replication.shutdown_timeout",
    "cluster.enabled",
    "cluster.config_file",
    "cluster.node_timeout",
    "cluster.port",
    "cluster.replica_validity_factor",
    "cluster.migration_barrier",
    "cluster.allow_replica_migration",
    "cluster.require_full_coverage",
    "cluster.replica_no_failover",
    "cluster.allow_reads_when_down",
    "cluster.allow_pubsubshard_when_down",
    "cluster.link_sendbuf_limit",
    "cluster.announce_ip",
    "cluster.announce_port",
    "cluster.announce_tls_port",
    "cluster.announce_bus_port",
    "cluster.announce_hostname",
    "cluster.announce_human_nodename",
    "cluster.preferred_endpoint_type",
    "cluster.compatibility_sample_ratio",
    "cluster.slot_stats_enabled",
    "cluster.slot_migration_write_pause_timeout",
    "cluster.slot_migration_handoff_max_lag_bytes",
    "memory.maxmemory",
    "memory.maxmemory_policy",
    "memory.maxmemory_samples",
    "memory.maxmemory_eviction_tenacity",
    "memory.maxclients",
    "memory.maxmemory_clients",
    "memory.activedefrag",
    "memory.active_defrag_ignore_bytes",
    "memory.active_defrag_threshold_lower",
    "memory.active_defrag_threshold_upper",
    "memory.active_defrag_cycle_min",
    "memory.active_defrag_cycle_max",
    "memory.active_defrag_max_scan_fields",
    "memory.lfu_log_factor",
    "memory.lfu_decay_time",
    "memory.active_expire_effort",
    "memory.jemalloc_bg_thread",
    "memory.server_cpulist",
    "memory.bio_cpulist",
    "memory.aof_rewrite_cpulist",
    "memory.bgsave_cpulist",
    "encoding.hash_max_listpack_entries",
    "encoding.hash_max_listpack_value",
    "encoding.list_max_listpack_size",
    "encoding.list_compress_depth",
    "encoding.set_max_intset_entries",
    "encoding.set_max_listpack_entries",
    "encoding.set_max_listpack_value",
    "encoding.zset_max_listpack_entries",
    "encoding.zset_max_listpack_value",
    "encoding.hll_sparse_max_bytes",
    "encoding.stream_node_max_bytes",
    "encoding.stream_node_max_entries",
    "encoding.stream_idmp_duration",
    "encoding.stream_idmp_maxsize",
    "pubsub.notify_keyspace_events",
    "pubsub.subscriber_ring_size",
    "slowlog.log_slower_than",
    "slowlog.max_len",
    "latency.monitor_threshold",
    "latency.tracking",
    "latency.tracking_info_percentiles",
    "clients.lookahead",
    "clients.key_memory_histograms",
    "lazyfree.lazy_eviction",
    "lazyfree.lazy_expire",
    "lazyfree.lazy_server_del",
    "lazyfree.replica_lazy_flush",
    "lazyfree.lazy_user_del",
    "lazyfree.lazy_user_flush",
    "aof.enabled",
    "aof.filename",
    "aof.dirname",
    "aof.fsync",
    "aof.no_appendfsync_on_rewrite",
    "aof.auto_aof_rewrite_percentage",
    "aof.auto_aof_rewrite_min_size",
    "aof.aof_load_truncated",
    "aof.aof_load_corrupt_tail_max_size",
    "aof.aof_use_rdb_preamble",
    "aof.aof_timestamp_enabled",
    "aof.aof_rewrite_incremental_fsync",
    "aof.rdb_save_incremental_fsync",
    "aof.shutdown_on_sigint",
    "aof.shutdown_on_sigterm",
    "plugins.enabled",
];

fn dirs_config_home() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
}

pub fn check_config(path: &Path) -> Result<SenkoConfig, ConfigError> {
    load_config(Some(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_alias_and_canonical_flags() {
        let cli = Cli::try_parse_from([
            "senkodb",
            "--port",
            "6380",
            "--tls-session-cache-size",
            "4096",
        ])
        .expect("parse");

        assert!(
            cli.overrides
                .iter()
                .any(|item| item.key == "network.port" && item.values == vec!["6380".to_owned()])
        );
        assert!(cli.overrides.iter().any(|item| {
            item.key == "tls.session_cache_size" && item.values == vec!["4096".to_owned()]
        }));
    }

    #[test]
    fn repeated_values_are_joined_for_lists() {
        let cli = Cli::try_parse_from([
            "senkodb",
            "--bind",
            "127.0.0.1",
            "--bind",
            "::1",
            "--save",
            "60 1",
            "--save",
            "300 10",
        ])
        .expect("parse");

        let mut config = SenkoConfig::default();
        apply_cli_overrides_checked(&mut config, &cli.overrides).expect("apply");
        assert_eq!(
            config.network.bind,
            vec!["127.0.0.1".to_owned(), "::1".to_owned()]
        );
        assert_eq!(config.persistence.save.len(), 2);
    }

    #[test]
    fn negative_numeric_values_parse_through_cli() {
        let cli = Cli::try_parse_from([
            "senkodb",
            "--list-max-listpack-size",
            "-2",
            "--general-oom-score-adj-values",
            "0,200,800",
        ])
        .expect("parse");

        let mut config = SenkoConfig::default();
        apply_cli_overrides_checked(&mut config, &cli.overrides).expect("apply");
        assert_eq!(config.encoding.list_max_listpack_size, -2);
        assert_eq!(config.general.oom_score_adj_values, [0, 200, 800]);
    }

    #[test]
    fn positional_config_path_is_supported() {
        let cli = Cli::try_parse_from(["senkodb", "./redis.conf"]).expect("parse");
        assert_eq!(cli.config, Some(PathBuf::from("./redis.conf")));
    }

    #[test]
    fn unknown_flag_still_errors() {
        use clap::error::ErrorKind;

        let error = Cli::try_parse_from(["senkodb", "--does-not-exist"]).expect_err("should fail");
        assert_eq!(error.kind(), ErrorKind::UnknownArgument);
    }
}
