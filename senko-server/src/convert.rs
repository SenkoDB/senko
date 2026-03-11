use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use senko_core::{
    ByteSize, ConfigError, ReplicaOf, SenkoConfig, parse_replica_of, validate_config,
};

pub fn convert_redis_conf_to_toml(input: &Path) -> Result<String, ConfigError> {
    let contents = fs::read_to_string(input)?;
    let mut config = SenkoConfig::default();
    let mut unknown = Vec::new();

    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(key) = parts.next() else {
            continue;
        };
        let values = parts.collect::<Vec<_>>();
        match key {
            "bind" => {
                config.network.bind = values.iter().map(|item| (*item).to_owned()).collect();
            }
            "port" => {
                if let Some(value) = values.first() {
                    config.network.port = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!("invalid port in redis.conf: {line}"))
                    })?;
                }
            }
            "unixsocket" => {
                config.network.unixsocket = values.first().map(PathBuf::from);
            }
            "timeout" => {
                if let Some(value) = values.first() {
                    config.network.timeout = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!(
                            "invalid timeout in redis.conf: {line}"
                        ))
                    })?;
                }
            }
            "tcp-keepalive" => {
                if let Some(value) = values.first() {
                    config.network.tcp_keepalive = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!("invalid tcp-keepalive: {line}"))
                    })?;
                }
            }
            "tcp-backlog" => {
                if let Some(value) = values.first() {
                    config.network.tcp_backlog = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!("invalid tcp-backlog: {line}"))
                    })?;
                }
            }
            "protected-mode" => {
                if let Some(value) = values.first() {
                    config.network.protected_mode = parse_yes_no(value)?;
                }
            }
            "requirepass" => {
                config.security.requirepass = values.first().map(|value| (*value).to_owned());
            }
            "aclfile" => {
                config.security.aclfile = values.first().map(PathBuf::from);
            }
            "maxclients" => {
                if let Some(value) = values.first() {
                    config.memory.maxclients = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!("invalid maxclients: {line}"))
                    })?;
                }
            }
            "maxmemory" => {
                if let Some(value) = values.first() {
                    config.memory.maxmemory = value
                        .parse::<ByteSize>()
                        .map_err(ConfigError::ValidationError)?;
                }
            }
            "maxmemory-policy" => {
                if let Some(value) = values.first() {
                    senko_core::config_set(&mut config, "memory.maxmemory_policy", value)?;
                }
            }
            "save" => {
                if values.len() == 2 {
                    config
                        .persistence
                        .save
                        .push(senko_core::config::SavePoint {
                            seconds: values[0].parse().map_err(|_| {
                                ConfigError::ValidationError(format!(
                                    "invalid save directive: {line}"
                                ))
                            })?,
                            changes: values[1].parse().map_err(|_| {
                                ConfigError::ValidationError(format!(
                                    "invalid save directive: {line}"
                                ))
                            })?,
                        });
                } else {
                    unknown.push(format!("save {}", values.join(" ")));
                }
            }
            "dbfilename" => {
                if let Some(value) = values.first() {
                    config.persistence.dbfilename = (*value).to_owned();
                }
            }
            "dir" => {
                if let Some(value) = values.first() {
                    config.persistence.dir = PathBuf::from(value);
                }
            }
            "appendonly" => {
                if let Some(value) = values.first() {
                    config.aof.enabled = parse_yes_no(value)?;
                }
            }
            "appendfilename" => {
                if let Some(value) = values.first() {
                    config.aof.filename = (*value).to_owned();
                }
            }
            "hz" => {
                if let Some(value) = values.first() {
                    config.general.hz = value
                        .parse()
                        .map_err(|_| ConfigError::ValidationError(format!("invalid hz: {line}")))?;
                }
            }
            "loglevel" => {
                if let Some(value) = values.first() {
                    senko_core::config_set(&mut config, "general.loglevel", value)?;
                }
            }
            "logfile" => {
                config.general.logfile = values.first().map(PathBuf::from);
            }
            "databases" => {
                if let Some(value) = values.first() {
                    config.general.databases = value.parse().map_err(|_| {
                        ConfigError::ValidationError(format!("invalid databases: {line}"))
                    })?;
                }
            }
            "cluster-enabled" => {
                if let Some(value) = values.first() {
                    config.cluster.enabled = parse_yes_no(value)?;
                }
            }
            "replicaof" | "slaveof" => {
                if values.len() == 2 {
                    config.replication.replicaof = Some(ReplicaOf {
                        host: values[0].to_owned(),
                        port: values[1].parse().map_err(|_| {
                            ConfigError::ValidationError(format!("invalid replicaof: {line}"))
                        })?,
                    });
                } else if values.len() == 1 {
                    config.replication.replicaof = Some(parse_replica_of(values[0])?);
                } else {
                    unknown.push(line.to_owned());
                }
            }
            _ => unknown.push(line.to_owned()),
        }
    }

    validate_config(&config)?;
    let mut out = String::new();
    writeln!(
        &mut out,
        "# Generated by: senkodb convert-config redis.conf"
    )
    .unwrap();
    writeln!(&mut out, "# Original file: {}", input.display()).unwrap();
    writeln!(&mut out, "# Converted at: {:?}", SystemTime::now()).unwrap();
    writeln!(&mut out, "# WARNING: Review this file before use.").unwrap();
    for line in &unknown {
        writeln!(&mut out, "# UNKNOWN: {line}").unwrap();
    }
    if !unknown.is_empty() {
        out.push('\n');
    }
    out.push_str(
        &toml::to_string_pretty(&config)
            .map_err(|error| ConfigError::ValidationError(error.to_string()))?,
    );
    Ok(out)
}

pub fn write_converted_config(input: &Path, output: Option<&Path>) -> Result<String, ConfigError> {
    let rendered = convert_redis_conf_to_toml(input)?;
    if let Some(output) = output {
        fs::write(output, &rendered)?;
    }
    Ok(rendered)
}

fn parse_yes_no(value: &str) -> Result<bool, ConfigError> {
    match value {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(ConfigError::ValidationError(format!(
            "invalid yes/no value '{value}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_file(contents: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "senko-convert-{}-{}.conf",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn convert_config_keeps_unknown_as_comments() {
        let input = temp_file("foo bar\nport 6380\n");
        let rendered = convert_redis_conf_to_toml(&input).unwrap();
        assert!(rendered.contains("# UNKNOWN: foo bar"));
        assert!(rendered.contains("port = 6380"));
    }

    #[test]
    fn convert_config_renders_save_points() {
        let input = temp_file("save 60 1\nsave 300 10\n");
        let rendered = convert_redis_conf_to_toml(&input).unwrap();
        assert!(rendered.contains("[[persistence.save]]"));
        assert!(rendered.contains("seconds = 60"));
        assert!(rendered.contains("seconds = 300"));
    }

    #[test]
    fn convert_config_renders_bind_array() {
        let input = temp_file("bind 127.0.0.1 ::1\n");
        let rendered = convert_redis_conf_to_toml(&input).unwrap();
        assert!(rendered.contains("[network]"));
        assert!(rendered.contains("127.0.0.1"));
        assert!(rendered.contains("::1"));
    }
}
