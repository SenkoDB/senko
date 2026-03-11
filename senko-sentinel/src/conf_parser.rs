use std::path::PathBuf;

use crate::config::{ConfigError, KnownReplica, KnownSentinel, MasterConfig, SentinelConfig};

pub fn parse_sentinel_conf(input: &str) -> Result<SentinelConfig, ConfigError> {
    let mut config = SentinelConfig::default();
    for (index, raw_line) in input.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let tokens = tokenize_line(line).map_err(|message| ConfigError::ConfParse {
            line: line_no,
            message,
        })?;
        if tokens.is_empty() {
            continue;
        }
        if tokens[0].eq_ignore_ascii_case("sentinel") {
            parse_sentinel_directive(&mut config, &tokens, line_no, line)?;
        } else {
            parse_global_directive(&mut config, &tokens, line_no, line)?;
        }
    }
    Ok(config)
}

fn parse_global_directive(
    config: &mut SentinelConfig,
    tokens: &[String],
    line: usize,
    raw: &str,
) -> Result<(), ConfigError> {
    match tokens[0].to_ascii_lowercase().as_str() {
        "port" => config.network.port = parse_num(tokens, 1, line, "port")?,
        "bind" => config.network.bind.extend(tokens[1..].iter().cloned()),
        "protected-mode" => config.network.protected_mode = parse_yes_no(tokens, 1, line)?,
        "daemonize" => config.general.daemonize = parse_yes_no(tokens, 1, line)?,
        "pidfile" => config.general.pidfile = Some(PathBuf::from(required(tokens, 1, line)?)),
        "dir" => config.general.dir = PathBuf::from(required(tokens, 1, line)?),
        "loglevel" => {
            config.general.loglevel = match required(tokens, 1, line)?.to_ascii_lowercase().as_str()
            {
                "debug" => senko_core::config::LogLevel::Debug,
                "verbose" => senko_core::config::LogLevel::Verbose,
                "notice" => senko_core::config::LogLevel::Notice,
                "warning" => senko_core::config::LogLevel::Warning,
                "nothing" => senko_core::config::LogLevel::Nothing,
                other => {
                    return Err(ConfigError::ConfParse {
                        line,
                        message: format!("invalid loglevel: {other}"),
                    });
                }
            };
        }
        "logfile" => {
            let value = required(tokens, 1, line)?;
            config.general.logfile = if value.is_empty() {
                None
            } else {
                Some(PathBuf::from(value))
            };
        }
        "syslog-enabled" => config.general.syslog_enabled = parse_yes_no(tokens, 1, line)?,
        "syslog-ident" => config.general.syslog_ident = required(tokens, 1, line)?.to_owned(),
        "syslog-facility" => config.general.syslog_facility = required(tokens, 1, line)?.to_owned(),
        "requirepass" => config.security.requirepass = Some(required(tokens, 1, line)?.to_owned()),
        "aclfile" => config.security.aclfile = Some(PathBuf::from(required(tokens, 1, line)?)),
        "acllog-max-len" => {
            config.security.acllog_max_len = parse_num(tokens, 1, line, "acllog-max-len")?
        }
        "user" => config.security.users.push(raw.to_owned()),
        "unixsocket" => config.network.unixsocket = Some(PathBuf::from(required(tokens, 1, line)?)),
        "unixsocketperm" => {
            config.network.unixsocketperm = parse_num(tokens, 1, line, "unixsocketperm")?
        }
        _ => {
            eprintln!("warning: unknown sentinel.conf directive ignored: {raw}");
            config.unknown_directives.push(raw.to_owned());
        }
    }
    Ok(())
}

fn parse_sentinel_directive(
    config: &mut SentinelConfig,
    tokens: &[String],
    line: usize,
    raw: &str,
) -> Result<(), ConfigError> {
    let subcommand = required(tokens, 1, line)?.to_ascii_lowercase();
    match subcommand.as_str() {
        "announce-ip" => config.network.announce_ip = Some(required(tokens, 2, line)?.to_owned()),
        "announce-port" => {
            config.network.announce_port = Some(parse_num(tokens, 2, line, "announce-port")?)
        }
        "resolve-hostnames" => config.network.resolve_hostnames = parse_yes_no(tokens, 2, line)?,
        "announce-hostnames" => config.network.announce_hostnames = parse_yes_no(tokens, 2, line)?,
        "sentinel-user" => {
            config.security.sentinel_user = Some(required(tokens, 2, line)?.to_owned())
        }
        "sentinel-pass" => {
            config.security.sentinel_pass = Some(required(tokens, 2, line)?.to_owned())
        }
        "deny-scripts-reconfig" => {
            config.security.deny_scripts_reconfig = parse_yes_no(tokens, 2, line)?
        }
        "monitor" => {
            let name = required(tokens, 2, line)?.to_owned();
            let host = required(tokens, 3, line)?.to_owned();
            let port = parse_num(tokens, 4, line, "monitor port")?;
            let quorum = parse_num(tokens, 5, line, "monitor quorum")?;
            {
                let master = upsert_master(config, &name);
                master.host = host.clone();
                master.port = port;
                master.quorum = quorum;
            }
            let runtime = config.runtime.masters.entry(name.clone()).or_default();
            runtime.name = name;
            runtime.current_host = host;
            runtime.current_port = port;
        }
        "down-after-milliseconds" => {
            master_mut(config, required(tokens, 2, line)?)?.down_after_milliseconds =
                parse_num(tokens, 3, line, "down-after-milliseconds")?;
        }
        "parallel-syncs" => {
            master_mut(config, required(tokens, 2, line)?)?.parallel_syncs =
                parse_num(tokens, 3, line, "parallel-syncs")?;
        }
        "failover-timeout" => {
            master_mut(config, required(tokens, 2, line)?)?.failover_timeout =
                parse_num(tokens, 3, line, "failover-timeout")?;
        }
        "auth-pass" => {
            master_mut(config, required(tokens, 2, line)?)?.auth_pass =
                Some(required(tokens, 3, line)?.to_owned());
        }
        "auth-user" => {
            master_mut(config, required(tokens, 2, line)?)?.auth_user =
                Some(required(tokens, 3, line)?.to_owned());
        }
        "notification-script" => {
            master_mut(config, required(tokens, 2, line)?)?.notification_script =
                Some(PathBuf::from(required(tokens, 3, line)?));
        }
        "client-reconfig-script" => {
            master_mut(config, required(tokens, 2, line)?)?.client_reconfig_script =
                Some(PathBuf::from(required(tokens, 3, line)?));
        }
        "rename-command" => {
            let master = master_mut(config, required(tokens, 2, line)?)?;
            master.rename_commands.insert(
                required(tokens, 3, line)?.to_ascii_uppercase(),
                required(tokens, 4, line)?.to_owned(),
            );
        }
        "master-reboot-down-after-period" => {
            master_mut(config, required(tokens, 2, line)?)?.master_reboot_down_after_period =
                parse_num(tokens, 3, line, "master-reboot-down-after-period")?;
        }
        "known-replica" | "known-slave" => {
            let name = required(tokens, 2, line)?;
            ensure_master_exists(config, name)?;
            config
                .runtime
                .masters
                .entry(name.to_owned())
                .or_default()
                .known_replicas
                .push(KnownReplica {
                    host: required(tokens, 3, line)?.to_owned(),
                    port: parse_num(tokens, 4, line, "known-replica port")?,
                });
        }
        "known-sentinel" => {
            let name = required(tokens, 2, line)?;
            ensure_master_exists(config, name)?;
            config
                .runtime
                .masters
                .entry(name.to_owned())
                .or_default()
                .known_sentinels
                .push(KnownSentinel {
                    host: required(tokens, 3, line)?.to_owned(),
                    port: parse_num(tokens, 4, line, "known-sentinel port")?,
                    runid: required(tokens, 5, line)?.to_owned(),
                });
        }
        "current-epoch" => {
            config.runtime.current_epoch = parse_num(tokens, 2, line, "current-epoch")?
        }
        "myid" => config.runtime.myid = Some(required(tokens, 2, line)?.to_owned()),
        _ if subcommand.contains("slave") => {
            eprintln!("warning: deprecated sentinel slave directive ignored/mapped: {raw}");
            config.unknown_directives.push(raw.to_owned());
        }
        _ => {
            eprintln!("warning: unknown sentinel directive ignored: {raw}");
            config.unknown_directives.push(raw.to_owned());
        }
    }
    Ok(())
}

fn ensure_master_exists(config: &SentinelConfig, name: &str) -> Result<(), ConfigError> {
    if config.masters.iter().any(|master| master.name == name) {
        Ok(())
    } else {
        Err(ConfigError::UnknownMaster(name.to_owned()))
    }
}

fn upsert_master<'a>(config: &'a mut SentinelConfig, name: &str) -> &'a mut MasterConfig {
    if let Some(index) = config.masters.iter().position(|master| master.name == name) {
        return &mut config.masters[index];
    }
    config.masters.push(MasterConfig {
        name: name.to_owned(),
        ..MasterConfig::default()
    });
    config.masters.last_mut().expect("just pushed")
}

fn master_mut<'a>(
    config: &'a mut SentinelConfig,
    name: &str,
) -> Result<&'a mut MasterConfig, ConfigError> {
    config
        .masters
        .iter_mut()
        .find(|master| master.name == name)
        .ok_or_else(|| ConfigError::UnknownMaster(name.to_owned()))
}

fn parse_yes_no(tokens: &[String], index: usize, line: usize) -> Result<bool, ConfigError> {
    match required(tokens, index, line)?.to_ascii_lowercase().as_str() {
        "yes" => Ok(true),
        "no" => Ok(false),
        other => Err(ConfigError::ConfParse {
            line,
            message: format!("expected yes/no, got {other}"),
        }),
    }
}

fn parse_num<T>(tokens: &[String], index: usize, line: usize, field: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
{
    required(tokens, index, line)?
        .parse()
        .map_err(|_| ConfigError::ConfParse {
            line,
            message: format!("invalid numeric value for {field}"),
        })
}

fn required(tokens: &[String], index: usize, line: usize) -> Result<&str, ConfigError> {
    tokens
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| ConfigError::ConfParse {
            line,
            message: "wrong number of arguments".to_owned(),
        })
}

fn tokenize_line(line: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote {
            Some(delim) => {
                if ch == delim {
                    quote = None;
                } else if ch == '\\' {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                } else {
                    current.push(ch);
                }
            }
            None => match ch {
                '"' | '\'' => quote = Some(ch),
                '#' => break,
                ch if ch.is_whitespace() => {
                    if !current.is_empty() {
                        out.push(std::mem::take(&mut current));
                    }
                }
                _ => current.push(ch),
            },
        }
    }
    if quote.is_some() {
        return Err("unterminated quoted string".to_owned());
    }
    if !current.is_empty() {
        out.push(current);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_sentinel_conf() {
        let config =
            parse_sentinel_conf("sentinel monitor mymaster 127.0.0.1 6379 2").expect("parse");
        assert_eq!(config.masters.len(), 1);
        assert_eq!(config.masters[0].name, "mymaster");
        assert_eq!(config.masters[0].down_after_milliseconds, 30_000);
    }

    #[test]
    fn parses_runtime_state_and_quotes() {
        let config = parse_sentinel_conf(
            r#"
            port 26379
            Sentinel Monitor mymaster 127.0.0.1 6379 2
            sentinel auth-pass mymaster "my pass word"
            sentinel known-replica mymaster 127.0.0.1 6380
            sentinel known-sentinel mymaster 127.0.0.1 26380 0123456789012345678901234567890123456789
            sentinel myid 1111111111111111111111111111111111111111
            sentinel current-epoch 7
        "#,
        )
        .expect("parse");
        assert_eq!(config.masters[0].auth_pass.as_deref(), Some("my pass word"));
        assert_eq!(config.runtime.current_epoch, 7);
        assert_eq!(
            config.runtime.masters["mymaster"].known_replicas,
            vec![KnownReplica {
                host: "127.0.0.1".to_owned(),
                port: 6380
            }]
        );
    }

    #[test]
    fn unknown_directive_is_preserved() {
        let config = parse_sentinel_conf(
            r#"
            sentinel monitor mymaster 127.0.0.1 6379 2
            foobar baz
        "#,
        )
        .expect("parse");
        assert_eq!(config.unknown_directives, vec!["foobar baz".to_owned()]);
    }
}
