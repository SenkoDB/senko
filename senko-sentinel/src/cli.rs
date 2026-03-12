use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use senko_core::config::LogLevel;

use crate::{
    conf_writer::write_sentinel_conf,
    config::{
        ConfigError, MasterConfig, SentinelConfig, apply_master_override, load_sentinel_config,
        render_default_config_toml, upsert_master,
    },
};

#[allow(clippy::large_enum_variant)]
pub enum SentinelCliAction {
    Run(SentinelConfig),
    Print(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentinelOptionSpec {
    key: String,
    long: String,
    aliases: Vec<String>,
}

pub fn parse_process_args<I>(args: I) -> Result<Option<SentinelCliAction>, String>
where
    I: IntoIterator<Item = OsString>,
{
    let args = args
        .into_iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if args.len() <= 1 {
        return Ok(None);
    }
    if args[1] == "--sentinel" {
        return parse_sentinel_args(&args[2..], None).map(Some);
    }
    let direct = PathBuf::from(&args[1]);
    if detect_direct_sentinel_path(&direct) {
        return parse_sentinel_args(&args[2..], Some(direct)).map(Some);
    }
    Ok(None)
}

pub fn detect_direct_sentinel_path(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };
    if !matches!(ext, "conf" | "toml") || !path.is_file() {
        return false;
    }
    let Ok(content) = fs::read_to_string(path) else {
        return false;
    };
    if ext == "toml" {
        return content.contains("[[masters]]");
    }
    content.to_ascii_lowercase().contains("sentinel monitor")
}

fn parse_sentinel_args(
    raw_args: &[String],
    implied_config: Option<PathBuf>,
) -> Result<SentinelCliAction, String> {
    if raw_args.is_empty() && implied_config.is_none() {
        return Err(
            "missing sentinel config path, flags, or subcommand after --sentinel".to_owned(),
        );
    }

    let mut argv = vec!["sentinel".to_owned()];
    argv.extend(raw_args.iter().cloned());
    let matches = build_command()
        .try_get_matches_from(argv)
        .map_err(|error| error.to_string())?;

    if let Some(action) = parse_subcommand(&matches)? {
        return Ok(action);
    }

    let config_path = matches
        .get_one::<PathBuf>("config")
        .cloned()
        .or_else(|| matches.get_one::<PathBuf>("config-path").cloned())
        .or(implied_config);
    let mut config = if let Some(path) = config_path {
        load_sentinel_config(&path).map_err(display_error)?
    } else {
        SentinelConfig::default()
    };

    apply_global_overrides(&mut config, &matches).map_err(display_error)?;
    apply_monitors(&mut config, &matches).map_err(display_error)?;
    apply_named_master_overrides(&mut config, &matches).map_err(display_error)?;
    config.validate().map_err(display_error)?;
    Ok(SentinelCliAction::Run(config))
}

fn build_command() -> Command {
    let mut command = Command::new("sentinel")
        .disable_help_subcommand(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .value_parser(value_parser!(PathBuf))
                .help("Path to sentinel.toml or sentinel.conf"),
        )
        .arg(
            Arg::new("config-path")
                .value_name("FILE")
                .value_parser(value_parser!(PathBuf))
                .conflicts_with("config")
                .help("Config file path (Redis-compatible positional form)"),
        )
        .arg(
            Arg::new("monitor")
                .long("monitor")
                .action(ArgAction::Append)
                .num_args(4)
                .value_names(["NAME", "HOST", "PORT", "QUORUM"])
                .help("Add or replace a monitored master"),
        )
        .arg(
            Arg::new("down-after-milliseconds")
                .long("down-after-milliseconds")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "MILLISECONDS"]),
        )
        .arg(
            Arg::new("parallel-syncs")
                .long("parallel-syncs")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "COUNT"]),
        )
        .arg(
            Arg::new("failover-timeout")
                .long("failover-timeout")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "MILLISECONDS"]),
        )
        .arg(
            Arg::new("auth-pass")
                .long("auth-pass")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "PASSWORD"]),
        )
        .arg(
            Arg::new("auth-user")
                .long("auth-user")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "USER"]),
        )
        .arg(
            Arg::new("notification-script")
                .long("notification-script")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "PATH"]),
        )
        .arg(
            Arg::new("client-reconfig-script")
                .long("client-reconfig-script")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "PATH"]),
        )
        .arg(
            Arg::new("master-reboot-down-after-period")
                .long("master-reboot-down-after-period")
                .action(ArgAction::Append)
                .num_args(2)
                .value_names(["NAME", "MILLISECONDS"]),
        )
        .arg(
            Arg::new("rename-command")
                .long("rename-command")
                .action(ArgAction::Append)
                .num_args(3)
                .value_names(["NAME", "COMMAND", "RENAMED"]),
        )
        .subcommand(
            Command::new("check-config").arg(
                Arg::new("file")
                    .required(true)
                    .value_name("FILE")
                    .value_parser(value_parser!(PathBuf)),
            ),
        )
        .subcommand(Command::new("default-config"))
        .subcommand(
            Command::new("convert")
                .arg(
                    Arg::new("input")
                        .required(true)
                        .value_name("INPUT")
                        .value_parser(value_parser!(PathBuf)),
                )
                .arg(
                    Arg::new("output")
                        .short('o')
                        .long("output")
                        .value_name("OUTPUT")
                        .value_parser(value_parser!(PathBuf)),
                ),
        )
        .subcommand(
            Command::new("convert-to-conf")
                .arg(
                    Arg::new("input")
                        .required(true)
                        .value_name("INPUT")
                        .value_parser(value_parser!(PathBuf)),
                )
                .arg(
                    Arg::new("output")
                        .short('o')
                        .long("output")
                        .value_name("OUTPUT")
                        .value_parser(value_parser!(PathBuf)),
                ),
        )
        .subcommand(
            Command::new("show-config").arg(
                Arg::new("file")
                    .required(true)
                    .value_name("FILE")
                    .value_parser(value_parser!(PathBuf)),
            ),
        );

    for spec in sentinel_option_specs() {
        let mut arg = Arg::new(leak(spec.key.clone()))
            .long(leak(spec.long.clone()))
            .action(ArgAction::Append)
            .num_args(1)
            .allow_hyphen_values(true)
            .value_name("VALUE")
            .help("Override config value");
        for alias in spec.aliases {
            arg = arg.visible_alias(leak(alias));
        }
        command = command.arg(arg);
    }

    command
}

fn parse_subcommand(matches: &ArgMatches) -> Result<Option<SentinelCliAction>, String> {
    let Some((name, sub)) = matches.subcommand() else {
        return Ok(None);
    };
    match name {
        "check-config" => {
            let path = sub
                .get_one::<PathBuf>("file")
                .expect("required by clap")
                .clone();
            let config = load_sentinel_config(&path).map_err(display_error)?;
            Ok(Some(SentinelCliAction::Print(format!(
                "valid sentinel config: {}\n",
                config
                    .config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| path.display().to_string())
            ))))
        }
        "default-config" => Ok(Some(SentinelCliAction::Print(render_default_config_toml()))),
        "convert" => {
            let input = sub
                .get_one::<PathBuf>("input")
                .expect("required by clap")
                .clone();
            let config = load_sentinel_config(&input).map_err(display_error)?;
            let output = config.normalized_toml().map_err(display_error)?;
            if let Some(path) = sub.get_one::<PathBuf>("output") {
                fs::write(path, &output).map_err(|error| error.to_string())?;
                Ok(Some(SentinelCliAction::Print(String::new())))
            } else {
                Ok(Some(SentinelCliAction::Print(output)))
            }
        }
        "convert-to-conf" => {
            let input = sub
                .get_one::<PathBuf>("input")
                .expect("required by clap")
                .clone();
            let config = load_sentinel_config(&input).map_err(display_error)?;
            let output = write_sentinel_conf(&config);
            if let Some(path) = sub.get_one::<PathBuf>("output") {
                fs::write(path, &output).map_err(|error| error.to_string())?;
                Ok(Some(SentinelCliAction::Print(String::new())))
            } else {
                Ok(Some(SentinelCliAction::Print(output)))
            }
        }
        "show-config" => {
            let path = sub
                .get_one::<PathBuf>("file")
                .expect("required by clap")
                .clone();
            let config = load_sentinel_config(&path).map_err(display_error)?;
            Ok(Some(SentinelCliAction::Print(
                config.normalized_toml().map_err(display_error)?,
            )))
        }
        _ => Err(format!("unknown sentinel subcommand: {name}")),
    }
}

fn apply_global_overrides(
    config: &mut SentinelConfig,
    matches: &ArgMatches,
) -> Result<(), ConfigError> {
    for spec in sentinel_option_specs() {
        let Some(values) = matches.get_many::<String>(&spec.key) else {
            continue;
        };
        let values = values.cloned().collect::<Vec<_>>();
        apply_global_override(config, &spec.key, &values)?;
    }
    Ok(())
}

fn apply_global_override(
    config: &mut SentinelConfig,
    key: &str,
    values: &[String],
) -> Result<(), ConfigError> {
    match key {
        "network.port" => config.network.port = parse_u16(last_value(values)?)?,
        "network.bind" => config.network.bind = values.to_vec(),
        "network.protected_mode" => {
            config.network.protected_mode = parse_bool(last_value(values)?)?
        }
        "network.announce_ip" => config.network.announce_ip = Some(last_value(values)?.to_owned()),
        "network.announce_port" => {
            config.network.announce_port = Some(parse_u16(last_value(values)?)?)
        }
        "network.resolve_hostnames" => {
            config.network.resolve_hostnames = parse_bool(last_value(values)?)?
        }
        "network.announce_hostnames" => {
            config.network.announce_hostnames = parse_bool(last_value(values)?)?
        }
        "network.unixsocket" => {
            config.network.unixsocket = Some(PathBuf::from(last_value(values)?))
        }
        "network.unixsocketperm" => config.network.unixsocketperm = parse_u32(last_value(values)?)?,
        "general.dir" => config.general.dir = PathBuf::from(last_value(values)?),
        "general.daemonize" => config.general.daemonize = parse_bool(last_value(values)?)?,
        "general.pidfile" => config.general.pidfile = Some(PathBuf::from(last_value(values)?)),
        "general.loglevel" => config.general.loglevel = parse_loglevel(last_value(values)?)?,
        "general.logfile" => config.general.logfile = Some(PathBuf::from(last_value(values)?)),
        "general.syslog_enabled" => {
            config.general.syslog_enabled = parse_bool(last_value(values)?)?
        }
        "general.syslog_ident" => config.general.syslog_ident = last_value(values)?.to_owned(),
        "general.syslog_facility" => {
            config.general.syslog_facility = last_value(values)?.to_owned()
        }
        "general.sentinel_hz" => config.general.sentinel_hz = parse_u32(last_value(values)?)?,
        "general.id_file" => config.general.id_file = Some(PathBuf::from(last_value(values)?)),
        "general.ignore_warnings" => config.general.ignore_warnings = values.to_vec(),
        "security.requirepass" => {
            config.security.requirepass = Some(last_value(values)?.to_owned())
        }
        "security.aclfile" => config.security.aclfile = Some(PathBuf::from(last_value(values)?)),
        "security.acllog_max_len" => {
            config.security.acllog_max_len = parse_usize(last_value(values)?)?
        }
        "security.users" => config.security.users = values.to_vec(),
        "security.sentinel_user" => {
            config.security.sentinel_user = Some(last_value(values)?.to_owned())
        }
        "security.sentinel_pass" => {
            config.security.sentinel_pass = Some(last_value(values)?.to_owned())
        }
        "security.deny_scripts_reconfig" => {
            config.security.deny_scripts_reconfig = parse_bool(last_value(values)?)?
        }
        "security.enable_debug_command" => {
            config.security.enable_debug_command = parse_bool(last_value(values)?)?
        }
        _ => {
            return Err(ConfigError::Validation(format!(
                "unsupported sentinel cli option: {key}"
            )));
        }
    }
    Ok(())
}

fn apply_monitors(config: &mut SentinelConfig, matches: &ArgMatches) -> Result<(), ConfigError> {
    let Some(values) = matches.get_many::<String>("monitor") else {
        return Ok(());
    };
    let values = values.cloned().collect::<Vec<_>>();
    for chunk in values.chunks_exact(4) {
        let master = MasterConfig {
            name: chunk[0].clone(),
            host: chunk[1].clone(),
            port: parse_u16(&chunk[2])?,
            quorum: parse_u32(&chunk[3])?,
            ..MasterConfig::default()
        };
        upsert_master(config, master)?;
    }
    Ok(())
}

fn apply_named_master_overrides(
    config: &mut SentinelConfig,
    matches: &ArgMatches,
) -> Result<(), ConfigError> {
    apply_pairs(matches, "down-after-milliseconds", config)?;
    apply_pairs(matches, "parallel-syncs", config)?;
    apply_pairs(matches, "failover-timeout", config)?;
    apply_pairs(matches, "auth-pass", config)?;
    apply_pairs(matches, "auth-user", config)?;
    apply_pairs(matches, "notification-script", config)?;
    apply_pairs(matches, "client-reconfig-script", config)?;
    apply_pairs(matches, "master-reboot-down-after-period", config)?;
    apply_triples(matches, "rename-command", config)?;
    Ok(())
}

fn apply_pairs(
    matches: &ArgMatches,
    option: &str,
    config: &mut SentinelConfig,
) -> Result<(), ConfigError> {
    let Some(values) = matches.get_many::<String>(option) else {
        return Ok(());
    };
    let values = values.cloned().collect::<Vec<_>>();
    for chunk in values.chunks_exact(2) {
        apply_master_override(config, &chunk[0], option, &chunk[1])?;
    }
    Ok(())
}

fn apply_triples(
    matches: &ArgMatches,
    option: &str,
    config: &mut SentinelConfig,
) -> Result<(), ConfigError> {
    let Some(values) = matches.get_many::<String>(option) else {
        return Ok(());
    };
    let values = values.cloned().collect::<Vec<_>>();
    for chunk in values.chunks_exact(3) {
        let payload = format!("{} {}", chunk[1], chunk[2]);
        apply_master_override(config, &chunk[0], option, &payload)?;
    }
    Ok(())
}

fn sentinel_option_specs() -> Vec<SentinelOptionSpec> {
    let alias_groups = sentinel_alias_groups();
    SENTINEL_CONFIG_KEYS
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
            SentinelOptionSpec {
                key,
                long,
                aliases: extra_aliases,
            }
        })
        .collect()
}

fn sentinel_alias_groups() -> BTreeMap<String, Vec<String>> {
    BTreeMap::from([
        ("network.port".to_owned(), vec!["port".to_owned()]),
        ("network.bind".to_owned(), vec!["bind".to_owned()]),
        (
            "network.protected_mode".to_owned(),
            vec!["protected-mode".to_owned()],
        ),
        (
            "network.announce_ip".to_owned(),
            vec!["announce-ip".to_owned()],
        ),
        (
            "network.announce_port".to_owned(),
            vec!["announce-port".to_owned()],
        ),
        (
            "network.resolve_hostnames".to_owned(),
            vec!["resolve-hostnames".to_owned()],
        ),
        (
            "network.announce_hostnames".to_owned(),
            vec!["announce-hostnames".to_owned()],
        ),
        (
            "network.unixsocket".to_owned(),
            vec!["unixsocket".to_owned()],
        ),
        (
            "network.unixsocketperm".to_owned(),
            vec!["unixsocketperm".to_owned()],
        ),
        ("general.dir".to_owned(), vec!["dir".to_owned()]),
        ("general.daemonize".to_owned(), vec!["daemonize".to_owned()]),
        ("general.pidfile".to_owned(), vec!["pidfile".to_owned()]),
        ("general.loglevel".to_owned(), vec!["loglevel".to_owned()]),
        ("general.logfile".to_owned(), vec!["logfile".to_owned()]),
        (
            "general.syslog_enabled".to_owned(),
            vec!["syslog-enabled".to_owned()],
        ),
        (
            "general.syslog_ident".to_owned(),
            vec!["syslog-ident".to_owned()],
        ),
        (
            "general.syslog_facility".to_owned(),
            vec!["syslog-facility".to_owned()],
        ),
        (
            "general.sentinel_hz".to_owned(),
            vec!["sentinel-hz".to_owned()],
        ),
        (
            "general.id_file".to_owned(),
            vec!["sentinel-id-file".to_owned(), "id-file".to_owned()],
        ),
        (
            "general.ignore_warnings".to_owned(),
            vec!["ignore-warnings".to_owned()],
        ),
        (
            "security.requirepass".to_owned(),
            vec!["requirepass".to_owned()],
        ),
        ("security.aclfile".to_owned(), vec!["aclfile".to_owned()]),
        (
            "security.acllog_max_len".to_owned(),
            vec!["acllog-max-len".to_owned()],
        ),
        ("security.users".to_owned(), vec!["user".to_owned()]),
        (
            "security.sentinel_user".to_owned(),
            vec!["sentinel-user".to_owned()],
        ),
        (
            "security.sentinel_pass".to_owned(),
            vec!["sentinel-pass".to_owned()],
        ),
        (
            "security.deny_scripts_reconfig".to_owned(),
            vec!["deny-scripts-reconfig".to_owned()],
        ),
        (
            "security.enable_debug_command".to_owned(),
            vec!["enable-debug-command".to_owned()],
        ),
    ])
}

fn canonical_flag_name(key: &str) -> String {
    key.replace('.', "-").replace('_', "-")
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

const SENTINEL_CONFIG_KEYS: &[&str] = &[
    "network.port",
    "network.bind",
    "network.protected_mode",
    "network.announce_ip",
    "network.announce_port",
    "network.resolve_hostnames",
    "network.announce_hostnames",
    "network.unixsocket",
    "network.unixsocketperm",
    "general.dir",
    "general.daemonize",
    "general.pidfile",
    "general.loglevel",
    "general.logfile",
    "general.syslog_enabled",
    "general.syslog_ident",
    "general.syslog_facility",
    "general.sentinel_hz",
    "general.id_file",
    "general.ignore_warnings",
    "security.requirepass",
    "security.aclfile",
    "security.acllog_max_len",
    "security.users",
    "security.sentinel_user",
    "security.sentinel_pass",
    "security.deny_scripts_reconfig",
    "security.enable_debug_command",
];

fn parse_bool(value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "yes" | "true" | "on" => Ok(true),
        "0" | "no" | "false" | "off" => Ok(false),
        _ => Err(ConfigError::Validation(format!(
            "invalid boolean value: {value}"
        ))),
    }
}

fn parse_loglevel(value: &str) -> Result<LogLevel, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" => Ok(LogLevel::Debug),
        "verbose" => Ok(LogLevel::Verbose),
        "notice" => Ok(LogLevel::Notice),
        "warning" => Ok(LogLevel::Warning),
        "nothing" => Ok(LogLevel::Nothing),
        _ => Err(ConfigError::Validation(format!(
            "invalid loglevel: {value}"
        ))),
    }
}

fn parse_u16(value: &str) -> Result<u16, ConfigError> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| ConfigError::Validation(format!("invalid u16 value: {value}")))
}

fn parse_u32(value: &str) -> Result<u32, ConfigError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| ConfigError::Validation(format!("invalid u32 value: {value}")))
}

fn parse_usize(value: &str) -> Result<usize, ConfigError> {
    value
        .trim()
        .parse::<usize>()
        .map_err(|_| ConfigError::Validation(format!("invalid usize value: {value}")))
}

fn last_value(values: &[String]) -> Result<&str, ConfigError> {
    values
        .last()
        .map(String::as_str)
        .ok_or_else(|| ConfigError::Validation("missing cli value".to_owned()))
}

fn display_error(error: ConfigError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::OsString,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_file(ext: &str, content: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("senko-sentinel-cli-{ts}.{ext}"));
        fs::write(&path, content).expect("write");
        path
    }

    #[test]
    fn detects_direct_sentinel_toml() {
        let path = unique_file(
            "toml",
            r#"[[masters]]
name="m"
host="127.0.0.1"
port=6379
quorum=2"#,
        );
        assert!(detect_direct_sentinel_path(&path));
    }

    #[test]
    fn parses_default_config_subcommand() {
        let args = vec![
            "senkodb".into(),
            "--sentinel".into(),
            "default-config".into(),
        ];
        let action = parse_process_args(args).expect("parse").expect("action");
        match action {
            SentinelCliAction::Print(output) => assert!(output.contains("[[masters]]")),
            SentinelCliAction::Run(_) => panic!("expected print"),
        }
    }

    #[test]
    fn applies_global_and_master_overrides() {
        let args = vec![
            "senkodb".into(),
            "--sentinel".into(),
            "--port".into(),
            "26380".into(),
            "--monitor".into(),
            "mymaster".into(),
            "127.0.0.1".into(),
            "6379".into(),
            "2".into(),
            "--down-after-milliseconds".into(),
            "mymaster".into(),
            "45000".into(),
        ];
        let action = parse_process_args(args).expect("parse").expect("action");
        match action {
            SentinelCliAction::Run(config) => {
                assert_eq!(config.network.port, 26_380);
                assert_eq!(config.masters.len(), 1);
                assert_eq!(config.masters[0].down_after_milliseconds, 45_000);
            }
            SentinelCliAction::Print(_) => panic!("expected run"),
        }
    }

    #[test]
    fn direct_file_mode_accepts_additional_overrides() {
        let path = unique_file(
            "toml",
            r#"[[masters]]
name="mymaster"
host="127.0.0.1"
port=6379
quorum=2"#,
        );
        let args = vec![
            OsString::from("senkodb"),
            path.as_os_str().to_owned(),
            OsString::from("--announce-port"),
            OsString::from("26379"),
        ];
        let action = parse_process_args(args).expect("parse").expect("action");
        match action {
            SentinelCliAction::Run(config) => {
                assert_eq!(config.network.announce_port, Some(26_379));
            }
            SentinelCliAction::Print(_) => panic!("expected run"),
        }
    }
}
