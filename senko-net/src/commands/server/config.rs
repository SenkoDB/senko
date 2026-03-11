use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock, RwLock},
};

use bytes::Bytes;
use senko_core::{
    SenkoConfig, SenkoValue, config::ProtectedConfigAccess, config_get as reflect_config_get,
    config_set as reflect_config_set,
};
use senko_proto::Frame;
use senko_store::Response;
use smallvec::SmallVec;

use crate::{
    acl,
    commands::server::info as server_info,
    connection::{error_bytes, error_message, frame_bytes, serialize_response, simple_string},
};

static LIVE_CONFIG: OnceLock<Arc<RwLock<SenkoConfig>>> = OnceLock::new();

#[derive(Debug)]
pub struct ConfigCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

struct ConfigParamMeta {
    name: &'static str,
    getter: fn(&SenkoConfig) -> String,
    file_value: fn(&SenkoConfig, bool) -> String,
}

pub fn init(config: &SenkoConfig) {
    senko_store::hll::set_sparse_max_bytes(config.encoding.hll_sparse_max_bytes);
    senko_store::hll::set_debug_commands_enabled(!matches!(
        config.security.enable_debug_command,
        ProtectedConfigAccess::No
    ));
    if let Some(live) = LIVE_CONFIG.get() {
        *live.write().expect("live config lock poisoned") = config.clone();
        return;
    }
    let _ = LIVE_CONFIG.set(Arc::new(RwLock::new(config.clone())));
}

pub fn snapshot() -> SenkoConfig {
    LIVE_CONFIG
        .get()
        .expect("live config not initialized")
        .read()
        .expect("live config lock poisoned")
        .clone()
}

pub fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
) -> Option<Result<ConfigCommandOutcome, Vec<u8>>> {
    if !eq_ascii(command, b"CONFIG") {
        return None;
    }
    Some(dispatch_config(args, resp3))
}

fn dispatch_config(args: &[Frame<'_>], resp3: bool) -> Result<ConfigCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'config' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"GET") {
        return config_get(rest, resp3);
    }
    if eq_ascii(subcommand, b"SET") {
        return config_set(rest);
    }
    if eq_ascii(subcommand, b"RESETSTAT") {
        return config_resetstat(rest);
    }
    if eq_ascii(subcommand, b"REWRITE") {
        return config_rewrite(rest);
    }
    Err(error_message(
        "ERR Unknown CONFIG subcommand or wrong number of arguments",
    ))
}

fn config_get(args: &[Frame<'_>], resp3: bool) -> Result<ConfigCommandOutcome, Vec<u8>> {
    let _ = config_registry().first().map(|meta| meta.getter);
    let _ = has_glob_wildcards as fn(&[u8]) -> bool;
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'config|get' command",
        ));
    }
    let config = snapshot();
    let mut out = SmallVec::<[Response; 16]>::new();
    for pattern in args {
        let pattern = frame_bytes(pattern).map_err(|error| error_bytes(&error))?;
        let pattern =
            std::str::from_utf8(pattern).map_err(|_| error_message("ERR invalid pattern"))?;
        let exact = !pattern.contains('*') && !pattern.contains('?');
        for (key, value) in reflect_config_get(&config, pattern)
            .into_iter()
            .filter(|(key, _)| !exact || key.eq_ignore_ascii_case(pattern))
        {
            out.push(bulk_response(key.as_bytes()));
            out.push(bulk_response(value.as_bytes()));
        }
    }
    Ok(outcome(serialize_response(
        &Response::Array(Box::new(out)),
        resp3,
    )))
}

fn config_set(args: &[Frame<'_>]) -> Result<ConfigCommandOutcome, Vec<u8>> {
    let _ = (
        parse_u64 as fn(&str) -> Result<u64, ()>,
        parse_usize as fn(&str) -> Result<usize, ()>,
        parse_i64 as fn(&str) -> Result<i64, ()>,
        parse_memory as fn(&str) -> Result<usize, ()>,
        validate_maxmemory_policy as fn(&str) -> Result<(), ()>,
        validate_loglevel as fn(&str) -> Result<(), ()>,
    );
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(error_message(
            "ERR wrong number of arguments for 'config|set' command",
        ));
    }

    let mut next = snapshot();
    let mut requirepass_update = None;
    let mut acllog_update = None;
    let mut index = 0usize;
    while index < args.len() {
        let param = parse_lower(frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?)?;
        let value = std::str::from_utf8(
            frame_bytes(&args[index + 1]).map_err(|error| error_bytes(&error))?,
        )
        .map_err(|_| config_set_error(&param))?;
        if param == "requirepass" {
            requirepass_update = Some(if value.is_empty() {
                None
            } else {
                Some(value.to_owned())
            });
        }
        if param == "acllog-max-len" {
            acllog_update = value.parse::<usize>().ok();
        }
        reflect_config_set(&mut next, &param, value).map_err(|_| config_set_error(&param))?;
        index += 2;
    }

    senko_store::hll::set_sparse_max_bytes(next.encoding.hll_sparse_max_bytes);
    senko_store::hll::set_debug_commands_enabled(!matches!(
        next.security.enable_debug_command,
        ProtectedConfigAccess::No
    ));
    if let Some(live) = LIVE_CONFIG.get() {
        *live.write().expect("live config lock poisoned") = next;
    }
    if let Some(requirepass) = requirepass_update {
        acl::set_default_user_password(requirepass);
    }
    if let Some(log_max) = acllog_update {
        acl::set_log_max_len(log_max);
    }
    Ok(outcome(simple_string(b"OK")))
}

fn config_resetstat(args: &[Frame<'_>]) -> Result<ConfigCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'config|resetstat' command",
        ));
    }
    server_info::reset_runtime_stats();
    Ok(outcome(simple_string(b"OK")))
}

fn config_rewrite(args: &[Frame<'_>]) -> Result<ConfigCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'config|rewrite' command",
        ));
    }
    let config = snapshot();
    let Some(path) = config.config_file.as_deref() else {
        return Err(error_message(
            "ERR The server is running without a config file",
        ));
    };
    rewrite_config_file(path, &config).map_err(|message| error_message(&message))?;
    Ok(outcome(simple_string(b"OK")))
}

fn rewrite_config_file(path: &Path, config: &SenkoConfig) -> Result<(), String> {
    let defaults = SenkoConfig::default();
    let current = fs::read_to_string(path).unwrap_or_default();
    let yaml = matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yaml" | "yml")
    );

    let mut remaining = diff_params(config, &defaults);
    let mut out = Vec::new();
    for line in current.lines() {
        if let Some(name) = parse_config_line_name(line) {
            if let Some(index) = remaining.iter().position(|param| *param == name) {
                out.push(render_config_line(name, config, yaml));
                remaining.remove(index);
                continue;
            }
            if config_registry().iter().any(|meta| meta.name == name) {
                continue;
            }
        }
        out.push(line.to_owned());
    }
    for name in remaining {
        if !out.is_empty() && !out.last().is_some_and(String::is_empty) {
            out.push(String::new());
        }
        out.push(render_config_line(name, config, yaml));
    }
    let body = if out.is_empty() {
        String::new()
    } else {
        format!("{}\n", out.join("\n"))
    };
    let tmp = temp_path(path);
    fs::write(&tmp, body).map_err(|error| error.to_string())?;
    fs::rename(&tmp, path).map_err(|error| error.to_string())
}

fn diff_params<'a>(config: &SenkoConfig, defaults: &'a SenkoConfig) -> Vec<&'a str> {
    config_registry()
        .iter()
        .filter_map(|meta| {
            let current = (meta.file_value)(config, false);
            let default = (meta.file_value)(defaults, false);
            (current != default).then_some(meta.name)
        })
        .collect()
}

fn parse_config_line_name(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    trimmed
        .split_once('=')
        .map(|(left, _)| left.trim())
        .or_else(|| trimmed.split_once(':').map(|(left, _)| left.trim()))
}

fn render_config_line(name: &str, config: &SenkoConfig, yaml: bool) -> String {
    let meta = config_registry()
        .iter()
        .find(|meta| meta.name == name)
        .expect("config parameter must exist");
    if yaml {
        format!("{name}: {}", (meta.file_value)(config, true))
    } else {
        format!("{name} = {}", (meta.file_value)(config, false))
    }
}

fn config_registry() -> &'static [ConfigParamMeta] {
    static REGISTRY: OnceLock<Box<[ConfigParamMeta]>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        vec![
            meta("bind", get_bind, file_bind),
            meta("port", get_port, file_port),
            meta("unixsocket", get_unixsocket, file_unixsocket),
            meta("unixsocketperm", get_unixsocketperm, file_unixsocketperm),
            meta("timeout", get_timeout, file_timeout),
            meta("tcp-keepalive", get_tcp_keepalive, file_tcp_keepalive),
            meta("loglevel", get_loglevel, file_loglevel),
            meta("logfile", get_logfile, file_logfile),
            meta("syslog-enabled", get_syslog_enabled, file_syslog_enabled),
            meta("syslog-ident", get_syslog_ident, file_syslog_ident),
            meta("syslog-facility", get_syslog_facility, file_syslog_facility),
            meta("databases", get_databases, file_databases),
            meta("maxmemory", get_maxmemory, file_maxmemory),
            meta(
                "maxmemory-policy",
                get_maxmemory_policy,
                file_maxmemory_policy,
            ),
            meta(
                "maxmemory-samples",
                get_maxmemory_samples,
                file_maxmemory_samples,
            ),
            meta(
                "maxmemory-eviction-tenacity",
                get_maxmemory_eviction_tenacity,
                file_maxmemory_eviction_tenacity,
            ),
            meta("maxclients", get_maxclients, file_maxclients),
            meta("tcp-backlog", get_tcp_backlog, file_tcp_backlog),
            meta("requirepass", get_requirepass, file_requirepass),
            meta("aclfile", get_aclfile, file_aclfile),
            meta("acllog-max-len", get_acllog_max_len, file_acllog_max_len),
            meta("hz", get_hz, file_hz),
            meta("dynamic-hz", get_dynamic_hz, file_dynamic_hz),
            meta(
                "aof-use-rdb-preamble",
                get_aof_use_rdb_preamble,
                file_aof_use_rdb_preamble,
            ),
            meta("appendonly", get_appendonly, file_appendonly),
            meta("appendfilename", get_appendfilename, file_appendfilename),
            meta("appendfsync", get_appendfsync, file_appendfsync),
            meta(
                "no-appendfsync-on-rewrite",
                get_no_appendfsync_on_rewrite,
                file_no_appendfsync_on_rewrite,
            ),
            meta(
                "auto-aof-rewrite-percentage",
                get_auto_aof_rewrite_percentage,
                file_auto_aof_rewrite_percentage,
            ),
            meta(
                "auto-aof-rewrite-min-size",
                get_auto_aof_rewrite_min_size,
                file_auto_aof_rewrite_min_size,
            ),
            meta("save", get_save, file_save),
            meta("rdbcompression", get_rdbcompression, file_rdbcompression),
            meta("rdbchecksum", get_rdbchecksum, file_rdbchecksum),
            meta("dbfilename", get_dbfilename, file_dbfilename),
            meta("dir", get_dir, file_dir),
            meta(
                "repl-backlog-size",
                get_repl_backlog_size,
                file_repl_backlog_size,
            ),
            meta(
                "repl-backlog-ttl",
                get_repl_backlog_ttl,
                file_repl_backlog_ttl,
            ),
            meta(
                "replica-serve-stale-data",
                get_replica_serve_stale_data,
                file_replica_serve_stale_data,
            ),
            meta(
                "replica-read-only",
                get_replica_read_only,
                file_replica_read_only,
            ),
            meta(
                "replica-lazy-flush",
                get_replica_lazy_flush,
                file_replica_lazy_flush,
            ),
            meta(
                "slowlog-log-slower-than",
                get_slowlog_log_slower_than,
                file_slowlog_log_slower_than,
            ),
            meta("slowlog-max-len", get_slowlog_max_len, file_slowlog_max_len),
            meta(
                "latency-monitor-threshold",
                get_latency_monitor_threshold,
                file_latency_monitor_threshold,
            ),
            meta(
                "lazyfree-lazy-eviction",
                get_lazyfree_lazy_eviction,
                file_lazyfree_lazy_eviction,
            ),
            meta(
                "lazyfree-lazy-expire",
                get_lazyfree_lazy_expire,
                file_lazyfree_lazy_expire,
            ),
            meta(
                "lazyfree-lazy-server-del",
                get_lazyfree_lazy_server_del,
                file_lazyfree_lazy_server_del,
            ),
            meta("activerehashing", get_activerehashing, file_activerehashing),
            meta(
                "list-max-listpack-size",
                get_list_max_listpack_size,
                file_list_max_listpack_size,
            ),
            meta(
                "list-compress-depth",
                get_list_compress_depth,
                file_list_compress_depth,
            ),
            meta(
                "hash-max-listpack-entries",
                get_hash_max_listpack_entries,
                file_hash_max_listpack_entries,
            ),
            meta(
                "hash-max-listpack-value",
                get_hash_max_listpack_value,
                file_hash_max_listpack_value,
            ),
            meta(
                "set-max-intset-entries",
                get_set_max_intset_entries,
                file_set_max_intset_entries,
            ),
            meta(
                "set-max-listpack-entries",
                get_set_max_listpack_entries,
                file_set_max_listpack_entries,
            ),
            meta(
                "set-max-listpack-value",
                get_set_max_listpack_value,
                file_set_max_listpack_value,
            ),
            meta(
                "zset-max-listpack-entries",
                get_zset_max_listpack_entries,
                file_zset_max_listpack_entries,
            ),
            meta(
                "zset-max-listpack-value",
                get_zset_max_listpack_value,
                file_zset_max_listpack_value,
            ),
            meta(
                "stream-node-max-bytes",
                get_stream_node_max_bytes,
                file_stream_node_max_bytes,
            ),
            meta(
                "stream-node-max-entries",
                get_stream_node_max_entries,
                file_stream_node_max_entries,
            ),
            meta("activedefrag", get_activedefrag, file_activedefrag),
            meta(
                "active-defrag-ignore-bytes",
                get_active_defrag_ignore_bytes,
                file_active_defrag_ignore_bytes,
            ),
            meta(
                "active-defrag-threshold-lower",
                get_active_defrag_threshold_lower,
                file_active_defrag_threshold_lower,
            ),
            meta(
                "proto-max-bulk-len",
                get_proto_max_bulk_len,
                file_proto_max_bulk_len,
            ),
            meta("lua-time-limit", get_lua_time_limit, file_lua_time_limit),
            meta(
                "lua-replicate-commands",
                get_lua_replicate_commands,
                file_lua_replicate_commands,
            ),
            meta("cluster-enabled", get_cluster_enabled, file_cluster_enabled),
            meta(
                "cluster-config-file",
                get_cluster_config_file,
                file_cluster_config_file,
            ),
            meta(
                "cluster-node-timeout",
                get_cluster_node_timeout,
                file_cluster_node_timeout,
            ),
            meta(
                "cluster-announce-ip",
                get_cluster_announce_ip,
                file_cluster_announce_ip,
            ),
            meta(
                "cluster-announce-port",
                get_cluster_announce_port,
                file_cluster_announce_port,
            ),
            meta(
                "cluster-announce-bus-port",
                get_cluster_announce_bus_port,
                file_cluster_announce_bus_port,
            ),
        ]
        .into_boxed_slice()
    })
}

fn meta(
    name: &'static str,
    getter: fn(&SenkoConfig) -> String,
    file_value: fn(&SenkoConfig, bool) -> String,
) -> ConfigParamMeta {
    ConfigParamMeta {
        name,
        getter,
        file_value,
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("conf");
    tmp.set_extension(format!("{ext}.tmp"));
    tmp
}

fn parse_lower(bytes: &[u8]) -> Result<String, Vec<u8>> {
    std::str::from_utf8(bytes)
        .map(|text| text.to_ascii_lowercase())
        .map_err(|_| error_message("ERR syntax error"))
}

fn parse_u64(text: &str) -> Result<u64, ()> {
    text.parse::<u64>().map_err(|_| ())
}

fn parse_usize(text: &str) -> Result<usize, ()> {
    text.parse::<usize>().map_err(|_| ())
}

fn parse_i64(text: &str) -> Result<i64, ()> {
    text.parse::<i64>().map_err(|_| ())
}

fn parse_memory(text: &str) -> Result<usize, ()> {
    let lower = text.trim().to_ascii_lowercase();
    let split = lower
        .find(|char: char| !char.is_ascii_digit())
        .unwrap_or(lower.len());
    let number = lower[..split].parse::<u128>().map_err(|_| ())?;
    let multiplier = match &lower[split..] {
        "" | "b" => 1u128,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        "t" | "tb" => 1024_u128.pow(4),
        _ => return Err(()),
    };
    usize::try_from(number.saturating_mul(multiplier)).map_err(|_| ())
}

fn validate_maxmemory_policy(value: &str) -> Result<(), ()> {
    match value.to_ascii_lowercase().as_str() {
        "noeviction" | "allkeys-lru" | "volatile-lru" | "allkeys-random" | "volatile-random"
        | "volatile-ttl" | "allkeys-lfu" | "volatile-lfu" => Ok(()),
        _ => Err(()),
    }
}

fn validate_loglevel(value: &str) -> Result<(), ()> {
    match value.to_ascii_lowercase().as_str() {
        "debug" | "verbose" | "notice" | "warning" => Ok(()),
        _ => Err(()),
    }
}

fn config_set_error(param: &str) -> Vec<u8> {
    error_message(&format!(
        "ERR CONFIG SET failed (possibly related to argument '{param}') - can't set immutable config"
    ))
}

fn outcome(response: Vec<u8>) -> ConfigCommandOutcome {
    ConfigCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn bool_get(value: bool) -> String {
    if value { "yes" } else { "no" }.to_owned()
}

fn bool_file(value: bool) -> String {
    if value { "true" } else { "false" }.to_owned()
}

fn quote(text: &str) -> String {
    format!("{text:?}")
}

fn bulk_response(bytes: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::from(Bytes::copy_from_slice(bytes))))
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn has_glob_wildcards(pattern: &[u8]) -> bool {
    pattern
        .iter()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']' | b'\\'))
}

macro_rules! getter_num {
    ($name:ident, $field:ident) => {
        fn $name(config: &SenkoConfig) -> String {
            config.$field.to_string()
        }
    };
}

macro_rules! getter_bool {
    ($name:ident, $field:ident) => {
        fn $name(config: &SenkoConfig) -> String {
            bool_get(config.$field)
        }
    };
}

macro_rules! file_num {
    ($name:ident, $field:ident) => {
        fn $name(config: &SenkoConfig, _yaml: bool) -> String {
            config.$field.to_string()
        }
    };
}

macro_rules! file_bool {
    ($name:ident, $field:ident) => {
        fn $name(config: &SenkoConfig, _yaml: bool) -> String {
            bool_file(config.$field)
        }
    };
}

fn get_bind(config: &SenkoConfig) -> String {
    config.bind_addr.ip().to_string()
}

fn file_bind(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.bind_addr.ip().to_string())
}

fn get_port(config: &SenkoConfig) -> String {
    config.bind_addr.port().to_string()
}

fn file_port(config: &SenkoConfig, _yaml: bool) -> String {
    config.bind_addr.port().to_string()
}

fn get_unixsocket(config: &SenkoConfig) -> String {
    config
        .unixsocket
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_default()
}

fn file_unixsocket(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&get_unixsocket(config))
}

getter_num!(get_unixsocketperm, unixsocketperm);
file_num!(file_unixsocketperm, unixsocketperm);
getter_num!(get_timeout, timeout);
file_num!(file_timeout, timeout);
getter_num!(get_tcp_keepalive, tcp_keepalive);
file_num!(file_tcp_keepalive, tcp_keepalive);

fn get_loglevel(config: &SenkoConfig) -> String {
    config.loglevel.clone()
}

fn file_loglevel(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.loglevel)
}

fn get_logfile(config: &SenkoConfig) -> String {
    config.logfile.clone()
}

fn file_logfile(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.logfile)
}

getter_bool!(get_syslog_enabled, syslog_enabled);
file_bool!(file_syslog_enabled, syslog_enabled);

fn get_syslog_ident(config: &SenkoConfig) -> String {
    config.syslog_ident.clone()
}

fn file_syslog_ident(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.syslog_ident)
}

fn get_syslog_facility(config: &SenkoConfig) -> String {
    config.syslog_facility.clone()
}

fn file_syslog_facility(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.syslog_facility)
}

getter_num!(get_databases, databases);
file_num!(file_databases, databases);

fn get_maxmemory(config: &SenkoConfig) -> String {
    config.max_memory.unwrap_or(0).to_string()
}

fn file_maxmemory(config: &SenkoConfig, _yaml: bool) -> String {
    get_maxmemory(config)
}

fn get_maxmemory_policy(config: &SenkoConfig) -> String {
    config.maxmemory_policy.clone()
}

fn file_maxmemory_policy(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.maxmemory_policy)
}

getter_num!(get_maxmemory_samples, maxmemory_samples);
file_num!(file_maxmemory_samples, maxmemory_samples);
getter_num!(get_maxmemory_eviction_tenacity, maxmemory_eviction_tenacity);
file_num!(
    file_maxmemory_eviction_tenacity,
    maxmemory_eviction_tenacity
);
getter_num!(get_maxclients, max_connections);
file_num!(file_maxclients, max_connections);
getter_num!(get_tcp_backlog, tcp_backlog);
file_num!(file_tcp_backlog, tcp_backlog);

fn get_requirepass(_config: &SenkoConfig) -> String {
    acl::default_password_hash_prefix().unwrap_or_default()
}

fn file_requirepass(config: &SenkoConfig, _yaml: bool) -> String {
    quote(config.auth_password.as_deref().unwrap_or(""))
}

fn get_aclfile(config: &SenkoConfig) -> String {
    config
        .aclfile
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_default()
}

fn file_aclfile(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&get_aclfile(config))
}

getter_num!(get_acllog_max_len, acllog_max_len);
file_num!(file_acllog_max_len, acllog_max_len);
getter_num!(get_hz, hz);
file_num!(file_hz, hz);
getter_bool!(get_dynamic_hz, dynamic_hz);
file_bool!(file_dynamic_hz, dynamic_hz);
getter_bool!(get_aof_use_rdb_preamble, aof_use_rdb_preamble);
file_bool!(file_aof_use_rdb_preamble, aof_use_rdb_preamble);
getter_bool!(get_appendonly, appendonly);
file_bool!(file_appendonly, appendonly);

fn get_appendfilename(config: &SenkoConfig) -> String {
    config.appendfilename.clone()
}

fn file_appendfilename(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.appendfilename)
}

fn get_appendfsync(config: &SenkoConfig) -> String {
    config.appendfsync.clone()
}

fn file_appendfsync(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.appendfsync)
}

getter_bool!(get_no_appendfsync_on_rewrite, no_appendfsync_on_rewrite);
file_bool!(file_no_appendfsync_on_rewrite, no_appendfsync_on_rewrite);
getter_num!(get_auto_aof_rewrite_percentage, auto_aof_rewrite_percentage);
file_num!(
    file_auto_aof_rewrite_percentage,
    auto_aof_rewrite_percentage
);
getter_num!(get_auto_aof_rewrite_min_size, auto_aof_rewrite_min_size);
file_num!(file_auto_aof_rewrite_min_size, auto_aof_rewrite_min_size);

fn get_save(config: &SenkoConfig) -> String {
    config.save.clone()
}

fn file_save(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.save)
}

getter_bool!(get_rdbcompression, rdbcompression);
file_bool!(file_rdbcompression, rdbcompression);
getter_bool!(get_rdbchecksum, rdbchecksum);
file_bool!(file_rdbchecksum, rdbchecksum);

fn get_dbfilename(config: &SenkoConfig) -> String {
    config.dbfilename.clone()
}

fn file_dbfilename(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.dbfilename)
}

fn get_dir(config: &SenkoConfig) -> String {
    config.dir.display().to_string()
}

fn file_dir(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&get_dir(config))
}

getter_num!(get_repl_backlog_size, repl_backlog_size);
file_num!(file_repl_backlog_size, repl_backlog_size);
getter_num!(get_repl_backlog_ttl, repl_backlog_ttl);
file_num!(file_repl_backlog_ttl, repl_backlog_ttl);
getter_bool!(get_replica_serve_stale_data, replica_serve_stale_data);
file_bool!(file_replica_serve_stale_data, replica_serve_stale_data);
getter_bool!(get_replica_read_only, replica_read_only);
file_bool!(file_replica_read_only, replica_read_only);
getter_bool!(get_replica_lazy_flush, replica_lazy_flush);
file_bool!(file_replica_lazy_flush, replica_lazy_flush);
getter_num!(get_slowlog_log_slower_than, slowlog_log_slower_than);
file_num!(file_slowlog_log_slower_than, slowlog_log_slower_than);
getter_num!(get_slowlog_max_len, slowlog_max_len);
file_num!(file_slowlog_max_len, slowlog_max_len);
getter_num!(get_latency_monitor_threshold, latency_monitor_threshold);
file_num!(file_latency_monitor_threshold, latency_monitor_threshold);
getter_bool!(get_lazyfree_lazy_eviction, lazyfree_lazy_eviction);
file_bool!(file_lazyfree_lazy_eviction, lazyfree_lazy_eviction);
getter_bool!(get_lazyfree_lazy_expire, lazyfree_lazy_expire);
file_bool!(file_lazyfree_lazy_expire, lazyfree_lazy_expire);
getter_bool!(get_lazyfree_lazy_server_del, lazyfree_lazy_server_del);
file_bool!(file_lazyfree_lazy_server_del, lazyfree_lazy_server_del);
getter_bool!(get_activerehashing, activerehashing);
file_bool!(file_activerehashing, activerehashing);
getter_num!(get_list_max_listpack_size, list_max_listpack_size);
file_num!(file_list_max_listpack_size, list_max_listpack_size);
getter_num!(get_list_compress_depth, list_compress_depth);
file_num!(file_list_compress_depth, list_compress_depth);
getter_num!(get_hash_max_listpack_entries, hash_max_listpack_entries);
file_num!(file_hash_max_listpack_entries, hash_max_listpack_entries);
getter_num!(get_hash_max_listpack_value, hash_max_listpack_value);
file_num!(file_hash_max_listpack_value, hash_max_listpack_value);
getter_num!(get_set_max_intset_entries, set_max_intset_entries);
file_num!(file_set_max_intset_entries, set_max_intset_entries);
getter_num!(get_set_max_listpack_entries, set_max_listpack_entries);
file_num!(file_set_max_listpack_entries, set_max_listpack_entries);
getter_num!(get_set_max_listpack_value, set_max_listpack_value);
file_num!(file_set_max_listpack_value, set_max_listpack_value);
getter_num!(get_zset_max_listpack_entries, zset_max_listpack_entries);
file_num!(file_zset_max_listpack_entries, zset_max_listpack_entries);
getter_num!(get_zset_max_listpack_value, zset_max_listpack_value);
file_num!(file_zset_max_listpack_value, zset_max_listpack_value);
getter_num!(get_stream_node_max_bytes, stream_node_max_bytes);
file_num!(file_stream_node_max_bytes, stream_node_max_bytes);
getter_num!(get_stream_node_max_entries, stream_node_max_entries);
file_num!(file_stream_node_max_entries, stream_node_max_entries);
getter_bool!(get_activedefrag, activedefrag);
file_bool!(file_activedefrag, activedefrag);
getter_num!(get_active_defrag_ignore_bytes, active_defrag_ignore_bytes);
file_num!(file_active_defrag_ignore_bytes, active_defrag_ignore_bytes);
getter_num!(
    get_active_defrag_threshold_lower,
    active_defrag_threshold_lower
);
file_num!(
    file_active_defrag_threshold_lower,
    active_defrag_threshold_lower
);
getter_num!(get_proto_max_bulk_len, proto_max_bulk_len);
file_num!(file_proto_max_bulk_len, proto_max_bulk_len);
getter_num!(get_lua_time_limit, lua_time_limit);
file_num!(file_lua_time_limit, lua_time_limit);
getter_bool!(get_lua_replicate_commands, lua_replicate_commands);
file_bool!(file_lua_replicate_commands, lua_replicate_commands);
getter_bool!(get_cluster_enabled, cluster_enabled);
file_bool!(file_cluster_enabled, cluster_enabled);

fn get_cluster_config_file(config: &SenkoConfig) -> String {
    config.cluster_config_file.clone()
}

fn file_cluster_config_file(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.cluster_config_file)
}

getter_num!(get_cluster_node_timeout, cluster_node_timeout);
file_num!(file_cluster_node_timeout, cluster_node_timeout);

fn get_cluster_announce_ip(config: &SenkoConfig) -> String {
    config.cluster_announce_ip.clone()
}

fn file_cluster_announce_ip(config: &SenkoConfig, _yaml: bool) -> String {
    quote(&config.cluster_announce_ip)
}

getter_num!(get_cluster_announce_port, cluster_announce_port);
file_num!(file_cluster_announce_port, cluster_announce_port);
getter_num!(get_cluster_announce_bus_port, cluster_announce_bus_port);
file_num!(file_cluster_announce_bus_port, cluster_announce_bus_port);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::server::info;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn bs(bytes: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(bytes)
    }

    fn init_test_config() -> SenkoConfig {
        let config = SenkoConfig::default();
        init(&config);
        info::init(&config);
        config
    }

    fn config_test_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("config test lock poisoned")
    }

    #[test]
    fn config_get_single_and_glob() {
        let _guard = config_test_guard();
        let _ = init_test_config();
        let maxmemory = config_get(&[bs(b"maxmemory")], true).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&maxmemory.response),
            "*2\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n"
        );

        let many = config_get(&[bs(b"maxmemory*")], true).unwrap();
        let rendered = String::from_utf8_lossy(&many.response);
        assert!(rendered.contains("maxmemory"));
        assert!(rendered.contains("maxmemory-policy"));
    }

    #[test]
    fn config_get_nonexistent_returns_empty_array() {
        let _guard = config_test_guard();
        let _ = init_test_config();
        let response = config_get(&[bs(b"does-not-exist")], true).unwrap();
        assert_eq!(String::from_utf8_lossy(&response.response), "*0\r\n");
    }

    #[test]
    fn config_set_updates_runtime_values_atomically() {
        let _guard = config_test_guard();
        let _ = init_test_config();
        config_set(&[bs(b"maxmemory"), bs(b"100mb"), bs(b"hz"), bs(b"20")]).unwrap();
        let updated = snapshot();
        assert_eq!(updated.max_memory, Some(100 * 1024 * 1024));
        assert_eq!(updated.hz, 20);

        let err = config_set(&[bs(b"hz"), bs(b"11"), bs(b"port"), bs(b"1234")]).unwrap_err();
        assert!(String::from_utf8_lossy(&err).contains("can't set immutable config"));
        assert_eq!(snapshot().hz, 20);
    }
}
