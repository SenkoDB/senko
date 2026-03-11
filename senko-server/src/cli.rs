use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{Args, Parser, Subcommand};
use senko_core::{
    ByteSize, ConfigError, SenkoConfig, config_set, load_config, parse_replica_of, validate_config,
};

#[derive(Parser, Debug)]
#[command(
    name = "senkodb",
    version = env!("CARGO_PKG_VERSION"),
    about = "Senko — a flash of light in the darkness. Redis-compatible in-memory store.",
    long_about = None,
)]
pub struct Cli {
    #[arg(
        short = 'c',
        long,
        value_name = "FILE",
        env = "SENKO_CONFIG",
        help = "Path to senko.toml config file"
    )]
    pub config: Option<PathBuf>,

    #[arg(
        long,
        value_name = "FILE",
        env = "SENKO_SENTINEL",
        help = "Path to sentinel.toml config file"
    )]
    pub sentinel: Option<PathBuf>,

    #[command(flatten)]
    pub overrides: CliOverrides,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Start,
    CheckConfig {
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },
    DefaultConfig,
    Version,
    ConvertConfig {
        #[arg(value_name = "REDIS_CONF")]
        input: PathBuf,
        #[arg(short, long, value_name = "OUTPUT")]
        output: Option<PathBuf>,
    },
}

#[derive(Args, Debug, Default)]
pub struct CliOverrides {
    #[arg(long, env = "SENKO_PORT", help = "TCP port (default: 6379)")]
    pub port: Option<u16>,
    #[arg(
        long,
        env = "SENKO_BIND",
        help = "Bind address (repeatable: --bind 0.0.0.0 --bind ::)"
    )]
    pub bind: Vec<String>,
    #[arg(
        long,
        env = "SENKO_IO_THREADS",
        help = "Number of I/O threads (default: CPU count)"
    )]
    pub io_threads: Option<usize>,
    #[arg(long, env = "SENKO_UNIXSOCKET", help = "Unix socket path")]
    pub unixsocket: Option<PathBuf>,
    #[arg(
        long,
        env = "SENKO_REQUIREPASS",
        help = "Password (sets default user password)"
    )]
    pub requirepass: Option<String>,
    #[arg(long, env = "SENKO_ACLFILE", help = "Path to ACL file")]
    pub aclfile: Option<PathBuf>,
    #[arg(long, env = "SENKO_MAXMEMORY", help = "Max memory (e.g. 1gb, 512mb)")]
    pub maxmemory: Option<String>,
    #[arg(long, env = "SENKO_MAXMEMORY_POLICY", help = "Eviction policy")]
    pub maxmemory_policy: Option<String>,
    #[arg(long, env = "SENKO_DIR", help = "Working directory for RDB/AOF")]
    pub dir: Option<PathBuf>,
    #[arg(
        long,
        env = "SENKO_DBFILENAME",
        help = "RDB filename (default: dump.rdb)"
    )]
    pub dbfilename: Option<String>,
    #[arg(long, env = "SENKO_SAVE", help = "Disable RDB saves (--no-save)")]
    pub no_save: bool,
    #[arg(
        long,
        env = "SENKO_REPLICAOF",
        value_name = "HOST:PORT",
        help = "Replicate from master (e.g. 10.0.0.1:6379)"
    )]
    pub replicaof: Option<String>,
    #[arg(long, env = "SENKO_CLUSTER_ENABLED", help = "Enable cluster mode")]
    pub cluster_enabled: bool,
    #[arg(
        long,
        env = "SENKO_LOGLEVEL",
        help = "Log level: debug|verbose|notice|warning|nothing"
    )]
    pub loglevel: Option<String>,
    #[arg(long, env = "SENKO_LOGFILE", help = "Log file path (default: stdout)")]
    pub logfile: Option<PathBuf>,
    #[arg(long, env = "SENKO_TLS_PORT", help = "TLS port (0 = disabled)")]
    pub tls_port: Option<u16>,
    #[arg(long, env = "SENKO_TLS_CERT_FILE")]
    pub tls_cert_file: Option<PathBuf>,
    #[arg(long, env = "SENKO_TLS_KEY_FILE")]
    pub tls_key_file: Option<PathBuf>,
    #[arg(long, help = "Daemonize the server")]
    pub daemonize: bool,
    #[arg(long, env = "SENKO_HZ", help = "Event loop frequency (default: 10)")]
    pub hz: Option<u32>,
    #[arg(
        long,
        env = "SENKO_PLUGINS",
        help = "Enable plugins (comma-separated: json,bloom)"
    )]
    pub plugins: Option<String>,
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
        PathBuf::from("/etc/senko/senko.toml"),
        dirs_config_home().join("senko/senko.toml"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

pub fn load_effective_config(cli: &Cli) -> Result<SenkoConfig, ConfigError> {
    let _ = apply_cli_overrides as fn(&mut SenkoConfig, &CliOverrides);
    let path = resolve_config_path(cli);
    let mut config = load_config(path.as_deref())?;
    apply_cli_overrides_checked(&mut config, &cli.overrides)?;
    validate_config(&config)?;
    Ok(config)
}

pub fn apply_cli_overrides(config: &mut SenkoConfig, overrides: &CliOverrides) {
    apply_cli_overrides_checked(config, overrides).expect("invalid CLI overrides")
}

pub fn apply_cli_overrides_checked(
    config: &mut SenkoConfig,
    overrides: &CliOverrides,
) -> Result<(), ConfigError> {
    if let Some(port) = overrides.port {
        config.network.port = port;
    }
    if !overrides.bind.is_empty() {
        config.network.bind = overrides.bind.clone();
    }
    if let Some(io_threads) = overrides.io_threads {
        config.network.io_threads = io_threads;
    }
    if let Some(unixsocket) = &overrides.unixsocket {
        config.network.unixsocket = Some(unixsocket.clone());
    }
    if let Some(requirepass) = &overrides.requirepass {
        config.security.requirepass = Some(requirepass.clone());
    }
    if let Some(aclfile) = &overrides.aclfile {
        config.security.aclfile = Some(aclfile.clone());
    }
    if let Some(maxmemory) = &overrides.maxmemory {
        config.memory.maxmemory =
            ByteSize::from_str(maxmemory).map_err(ConfigError::ValidationError)?;
    }
    if let Some(policy) = &overrides.maxmemory_policy {
        config_set(config, "memory.maxmemory_policy", policy)?;
    }
    if let Some(dir) = &overrides.dir {
        config.persistence.dir = dir.clone();
    }
    if let Some(dbfilename) = &overrides.dbfilename {
        config.persistence.dbfilename = dbfilename.clone();
    }
    if overrides.no_save {
        config.persistence.save.clear();
    }
    if let Some(replicaof) = &overrides.replicaof {
        config.replication.replicaof = Some(parse_replica_of(replicaof)?);
    }
    if overrides.cluster_enabled {
        config.cluster.enabled = true;
    }
    if let Some(loglevel) = &overrides.loglevel {
        config_set(config, "general.loglevel", loglevel)?;
    }
    if let Some(logfile) = &overrides.logfile {
        config.general.logfile = Some(logfile.clone());
    }
    if let Some(port) = overrides.tls_port {
        config.tls.port = port;
    }
    if let Some(cert) = &overrides.tls_cert_file {
        config.tls.cert_file = Some(cert.clone());
    }
    if let Some(key) = &overrides.tls_key_file {
        config.tls.key_file = Some(key.clone());
    }
    if overrides.daemonize {
        config.general.daemonize = true;
    }
    if let Some(hz) = overrides.hz {
        config.general.hz = hz;
    }
    if let Some(plugins) = &overrides.plugins {
        config.plugins.enabled = plugins
            .split(',')
            .filter(|item| !item.trim().is_empty())
            .map(|item| item.trim().to_owned())
            .collect();
    }
    Ok(())
}

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
    fn cli_flag_wins_over_env_and_file() {
        let mut config = SenkoConfig::default();
        config.network.port = 7000;
        let overrides = CliOverrides {
            port: Some(6380),
            ..CliOverrides::default()
        };
        apply_cli_overrides_checked(&mut config, &overrides).unwrap();
        assert_eq!(config.network.port, 6380);
    }

    #[test]
    fn no_save_clears_schedule() {
        let mut config = SenkoConfig::default();
        config
            .persistence
            .save
            .push(senko_core::config::SavePoint {
                seconds: 60,
                changes: 1,
            });
        let overrides = CliOverrides {
            no_save: true,
            ..CliOverrides::default()
        };
        apply_cli_overrides_checked(&mut config, &overrides).unwrap();
        assert!(config.persistence.save.is_empty());
    }

    #[test]
    fn replicaof_is_parsed() {
        let mut config = SenkoConfig::default();
        let overrides = CliOverrides {
            replicaof: Some("10.0.0.1:6379".to_owned()),
            ..CliOverrides::default()
        };
        apply_cli_overrides_checked(&mut config, &overrides).unwrap();
        let replica = config.replication.replicaof.unwrap();
        assert_eq!(replica.host, "10.0.0.1");
        assert_eq!(replica.port, 6379);
    }
}
