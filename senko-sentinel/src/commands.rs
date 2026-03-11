use std::{fmt::Write as _, net::SocketAddr};

use ahash::RandomState;
use bytes::BytesMut;
use compact_str::CompactString;
use hashbrown::HashSet;
use senko_core::{SenkoError, SenkoResult};
use senko_proto::{Frame, RespSerializer};

use crate::{
    SharedRuntime,
    config::{MasterConfig, flush_config_atomic},
    current_unix_ms, current_unix_us,
    failover::{advance_failover, begin_failover},
    state::{
        FailoverState, InstanceFlags, LinkStatus, Role, instance_flags_to_string, update_world,
    },
};

pub struct SentinelClient {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: CompactString,
    pub authenticated: bool,
    pub resp_version: u8,
    pub subscriptions: HashSet<CompactString, RandomState>,
    pub psubscriptions: HashSet<CompactString, RandomState>,
    pub output_buf: BytesMut,
}

impl SentinelClient {
    pub fn new(id: u64, addr: SocketAddr, authenticated: bool) -> Self {
        Self {
            id,
            addr,
            name: CompactString::default(),
            authenticated,
            resp_version: 2,
            subscriptions: HashSet::with_hasher(RandomState::new()),
            psubscriptions: HashSet::with_hasher(RandomState::new()),
            output_buf: BytesMut::with_capacity(4096),
        }
    }
}

pub struct CommandResult {
    pub response: BytesMut,
    pub close: bool,
}

pub fn dispatch(
    frame: Frame<'_>,
    runtime: &SharedRuntime,
    client: &mut SentinelClient,
) -> SenkoResult<CommandResult> {
    let args = command_args(frame)?;
    if args.is_empty() {
        return Err(SenkoError::Protocol("empty sentinel command"));
    }
    runtime.borrow().record_command();
    let command = ascii_upper(args[0]);
    let mut out = BytesMut::with_capacity(256);
    if requires_auth(runtime, client, &command) {
        RespSerializer::write_error(&mut out, b"NOAUTH Authentication required.");
        return Ok(CommandResult {
            response: out,
            close: false,
        });
    }
    match command.as_str() {
        "PING" => RespSerializer::write_simple_string(&mut out, b"PONG"),
        "QUIT" => {
            RespSerializer::write_simple_string(&mut out, b"OK");
            return Ok(CommandResult {
                response: out,
                close: true,
            });
        }
        "AUTH" => auth_command(&mut out, runtime, client, &args[1..])?,
        "HELLO" => hello_command(&mut out, runtime, client, &args[1..])?,
        "CLIENT" => client_command(&mut out, client, &args[1..])?,
        "RESET" => RespSerializer::write_simple_string(&mut out, b"OK"),
        "INFO" => write_info(&mut out, runtime, args.get(1).copied())?,
        "COMMAND" => write_command_table(&mut out),
        "SENTINEL" => sentinel_subcommand(&mut out, runtime, &args[1..])?,
        _ => RespSerializer::write_error(&mut out, b"ERR Command not allowed in Sentinel mode"),
    }
    Ok(CommandResult {
        response: out,
        close: false,
    })
}

fn sentinel_subcommand(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    args: &[&[u8]],
) -> SenkoResult<()> {
    if args.is_empty() {
        return Err(SenkoError::Protocol("missing sentinel subcommand"));
    }
    let subcommand = ascii_upper(args[0]);
    match subcommand.as_str() {
        "MASTERS" => write_masters(out, runtime),
        "MASTER" => write_master(out, runtime, required_arg(args, 1)?),
        "REPLICAS" | "SLAVES" => write_replicas(out, runtime, required_arg(args, 1)?),
        "SENTINELS" => write_sentinels(out, runtime, required_arg(args, 1)?),
        "GET-MASTER-ADDR-BY-NAME" => write_master_addr(out, runtime, required_arg(args, 1)?),
        "RESET" => reset_masters(out, runtime, required_arg(args, 1)?),
        "FAILOVER" => force_failover(out, runtime, required_arg(args, 1)?),
        "CKQUORUM" => write_ckquorum(out, runtime, required_arg(args, 1)?),
        "FLUSHCONFIG" => flush_config(out, runtime),
        "MONITOR" => monitor_master(out, runtime, args),
        "REMOVE" => remove_master(out, runtime, required_arg(args, 1)?),
        "SET" => set_master_option(out, runtime, args),
        "INFO-CACHE" => write_info_cache(out, runtime, &args[1..]),
        "PENDING-SCRIPTS" => write_pending_scripts(out, runtime),
        "MYID" => {
            let id = runtime.borrow().my_id();
            RespSerializer::write_bulk_string(out, id.as_bytes());
            Ok(())
        }
        "IS-MASTER-DOWN-BY-ADDR" => is_master_down_by_addr(out, runtime, args),
        _ => {
            RespSerializer::write_error(out, b"ERR unknown sentinel subcommand");
            Ok(())
        }
    }
}

fn command_args(frame: Frame<'_>) -> SenkoResult<Vec<&[u8]>> {
    let Some(aggregate) = frame.aggregate() else {
        return Err(SenkoError::Protocol("expected command array"));
    };
    aggregate
        .iter()
        .map(|frame| match frame? {
            Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes),
            _ => Err(SenkoError::Protocol("command arguments must be strings")),
        })
        .collect()
}

fn required_arg<'a>(args: &'a [&[u8]], index: usize) -> SenkoResult<&'a [u8]> {
    args.get(index)
        .copied()
        .ok_or(SenkoError::Protocol("missing sentinel argument"))
}

fn ascii_upper(input: &[u8]) -> String {
    input
        .iter()
        .map(|byte| byte.to_ascii_uppercase() as char)
        .collect()
}

fn write_command_table(out: &mut BytesMut) {
    RespSerializer::write_array_header(out, 7);
    RespSerializer::write_bulk_string(out, b"AUTH");
    RespSerializer::write_bulk_string(out, b"CLIENT");
    RespSerializer::write_bulk_string(out, b"HELLO");
    RespSerializer::write_bulk_string(out, b"PING");
    RespSerializer::write_bulk_string(out, b"INFO");
    RespSerializer::write_bulk_string(out, b"SENTINEL");
    RespSerializer::write_bulk_string(out, b"QUIT");
}

fn requires_auth(runtime: &SharedRuntime, client: &SentinelClient, command: &str) -> bool {
    if client.authenticated || runtime.borrow().config.requirepass().is_none() {
        return false;
    }
    !matches!(command, "AUTH" | "HELLO" | "PING" | "QUIT" | "SENTINEL")
}

fn auth_command(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    client: &mut SentinelClient,
    args: &[&[u8]],
) -> SenkoResult<()> {
    let Some(requirepass) = runtime.borrow().config.requirepass().map(str::to_owned) else {
        RespSerializer::write_error(out, b"ERR Client sent AUTH, but no password is set");
        return Ok(());
    };
    let password = match args {
        [password] => std::str::from_utf8(password)?,
        [_, password] => std::str::from_utf8(password)?,
        _ => {
            RespSerializer::write_error(out, b"ERR wrong number of arguments for 'auth' command");
            return Ok(());
        }
    };
    if password == requirepass {
        client.authenticated = true;
        RespSerializer::write_simple_string(out, b"OK");
    } else {
        RespSerializer::write_error(out, b"WRONGPASS invalid username-password pair");
    }
    Ok(())
}

fn hello_command(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    client: &mut SentinelClient,
    args: &[&[u8]],
) -> SenkoResult<()> {
    let mut index = 0usize;
    if let Some(protover) = args.first() {
        let parsed: u8 = std::str::from_utf8(protover)?.parse()?;
        client.resp_version = if parsed == 3 { 3 } else { 2 };
        index = 1;
    }
    while index < args.len() {
        match ascii_upper(args[index]).as_str() {
            "AUTH" => {
                if index + 2 >= args.len() {
                    RespSerializer::write_error(out, b"ERR syntax error");
                    return Ok(());
                }
                let password = std::str::from_utf8(args[index + 2])?;
                if runtime
                    .borrow()
                    .config
                    .requirepass()
                    .map(|expected| expected == password)
                    .unwrap_or(false)
                {
                    client.authenticated = true;
                } else if runtime.borrow().config.requirepass().is_some() {
                    RespSerializer::write_error(out, b"WRONGPASS invalid username-password pair");
                    return Ok(());
                }
                index += 3;
            }
            "SETNAME" => {
                if index + 1 >= args.len() {
                    RespSerializer::write_error(out, b"ERR syntax error");
                    return Ok(());
                }
                client.name = CompactString::from(std::str::from_utf8(args[index + 1])?);
                index += 2;
            }
            _ => {
                RespSerializer::write_error(out, b"ERR syntax error");
                return Ok(());
            }
        }
    }
    let snapshot = runtime.borrow().snapshot();
    RespSerializer::write_array_header(out, 14);
    for (key, value) in [
        ("server", "redis"),
        ("version", env!("CARGO_PKG_VERSION")),
        ("proto", if client.resp_version == 3 { "3" } else { "2" }),
        ("id", &client.id.to_string()),
        ("mode", "sentinel"),
        ("role", "sentinel"),
        ("modules", ""),
    ] {
        RespSerializer::write_bulk_string(out, key.as_bytes());
        RespSerializer::write_bulk_string(out, value.as_bytes());
    }
    let _ = snapshot;
    Ok(())
}

fn client_command(
    out: &mut BytesMut,
    client: &mut SentinelClient,
    args: &[&[u8]],
) -> SenkoResult<()> {
    if args.is_empty() {
        RespSerializer::write_error(out, b"ERR wrong number of arguments for 'client' command");
        return Ok(());
    }
    match ascii_upper(args[0]).as_str() {
        "ID" => RespSerializer::write_integer(out, client.id as i64),
        "GETNAME" => {
            if client.name.is_empty() {
                RespSerializer::write_null(out);
            } else {
                RespSerializer::write_bulk_string(out, client.name.as_bytes());
            }
        }
        "SETNAME" => {
            let Some(name) = args.get(1) else {
                RespSerializer::write_error(
                    out,
                    b"ERR wrong number of arguments for 'client|setname' command",
                );
                return Ok(());
            };
            client.name = CompactString::from(std::str::from_utf8(name)?);
            RespSerializer::write_simple_string(out, b"OK");
        }
        "INFO" => {
            let payload = format!(
                "id={} addr={} name={} resp={}",
                client.id, client.addr, client.name, client.resp_version
            );
            RespSerializer::write_bulk_string(out, payload.as_bytes());
        }
        "LIST" => {
            let payload = format!(
                "id={} addr={} name={} resp={}\n",
                client.id, client.addr, client.name, client.resp_version
            );
            RespSerializer::write_bulk_string(out, payload.as_bytes());
        }
        _ => {
            RespSerializer::write_error(out, b"ERR unsupported CLIENT subcommand in Sentinel mode")
        }
    }
    Ok(())
}

fn write_info(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    section: Option<&[u8]>,
) -> SenkoResult<()> {
    let section = section.map(ascii_upper);
    let runtime_ref = runtime.borrow();
    let snapshot = runtime_ref.snapshot();
    let mut payload = String::new();
    if section.as_deref().is_none() || matches!(section.as_deref(), Some("SERVER")) {
        writeln!(&mut payload, "# Server").ok();
        writeln!(&mut payload, "redis_version:{}", env!("CARGO_PKG_VERSION")).ok();
        writeln!(&mut payload, "redis_git_sha1:0000000").ok();
        writeln!(&mut payload, "redis_git_dirty:0").ok();
        writeln!(&mut payload, "redis_build_id:sentinel").ok();
        writeln!(&mut payload, "redis_mode:sentinel").ok();
        writeln!(&mut payload, "os:{}", std::env::consts::OS).ok();
        writeln!(&mut payload, "arch_bits:64").ok();
        writeln!(&mut payload, "process_id:{}", std::process::id()).ok();
        writeln!(&mut payload, "run_id:{}", snapshot.my_id).ok();
        writeln!(&mut payload, "tcp_port:{}", runtime_ref.config.port()).ok();
        writeln!(&mut payload, "server_time_usec:{}", current_unix_us()).ok();
        let uptime = current_unix_ms().saturating_sub(runtime_ref.started_at_ms) / 1_000;
        writeln!(&mut payload, "uptime_in_seconds:{uptime}").ok();
        writeln!(&mut payload, "uptime_in_days:{}", uptime / 86_400).ok();
        writeln!(&mut payload, "hz:{}", runtime_ref.config.sentinel_hz()).ok();
        writeln!(
            &mut payload,
            "configured_hz:{}",
            runtime_ref.config.sentinel_hz()
        )
        .ok();
        writeln!(&mut payload, "aof_enabled:0").ok();
        writeln!(&mut payload, "rdb_changes_since_last_save:0").ok();
        writeln!(&mut payload, "rdb_bgsave_in_progress:0").ok();
        writeln!(&mut payload).ok();
    }
    if section.as_deref().is_none() || matches!(section.as_deref(), Some("CLIENTS")) {
        writeln!(&mut payload, "# Clients").ok();
        writeln!(
            &mut payload,
            "connected_clients:{}",
            runtime_ref.connected_clients
        )
        .ok();
        writeln!(&mut payload, "cluster_connections:0").ok();
        writeln!(&mut payload, "maxclients:10000").ok();
        writeln!(&mut payload, "client_recent_max_input_buffer:0").ok();
        writeln!(&mut payload, "client_recent_max_output_buffer:0").ok();
        writeln!(&mut payload, "blocked_clients:0").ok();
        writeln!(&mut payload, "tracking_clients:0").ok();
        writeln!(&mut payload).ok();
    }
    if section.as_deref().is_none() || matches!(section.as_deref(), Some("STATS")) {
        writeln!(&mut payload, "# Stats").ok();
        writeln!(
            &mut payload,
            "total_connections_received:{}",
            runtime_ref
                .stats
                .total_connections_received
                .load(std::sync::atomic::Ordering::Relaxed)
        )
        .ok();
        writeln!(
            &mut payload,
            "total_commands_processed:{}",
            runtime_ref
                .stats
                .total_commands_processed
                .load(std::sync::atomic::Ordering::Relaxed)
        )
        .ok();
        writeln!(
            &mut payload,
            "total_net_input_bytes:{}",
            runtime_ref
                .stats
                .total_net_input_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        )
        .ok();
        writeln!(
            &mut payload,
            "total_net_output_bytes:{}",
            runtime_ref
                .stats
                .total_net_output_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        )
        .ok();
        writeln!(&mut payload, "instantaneous_ops_per_sec:0").ok();
        writeln!(&mut payload, "sentinel_tilt:0").ok();
        writeln!(&mut payload, "sentinel_tilt_since_seconds:-1").ok();
        writeln!(
            &mut payload,
            "sentinel_running_scripts:{}",
            runtime_ref.notifier.pending_scripts().len()
        )
        .ok();
        writeln!(&mut payload, "sentinel_scripts_queue_length:0").ok();
        writeln!(&mut payload, "sentinel_simulate_failure_flags:0").ok();
        writeln!(&mut payload).ok();
    }
    if section.as_deref().is_none() || matches!(section.as_deref(), Some("SENTINEL")) {
        writeln!(&mut payload, "# Sentinel").ok();
        writeln!(&mut payload, "sentinel_masters:{}", snapshot.masters.len()).ok();
        for (index, master) in snapshot.masters.values().enumerate() {
            let status = if master.flags.contains(InstanceFlags::O_DOWN) {
                "odown"
            } else if master.flags.contains(InstanceFlags::S_DOWN) {
                "sdown"
            } else {
                "ok"
            };
            writeln!(
                &mut payload,
                "sentinel_master{index}:name={},status={status},address={},slaves={},sentinels={}",
                master.name,
                master.addr,
                master.replicas.len(),
                master.sentinels.len()
            )
            .ok();
        }
    }
    RespSerializer::write_bulk_string(out, payload.as_bytes());
    Ok(())
}

fn write_masters(out: &mut BytesMut, runtime: &SharedRuntime) -> SenkoResult<()> {
    let snapshot = runtime.borrow().snapshot();
    RespSerializer::write_array_header(out, snapshot.masters.len());
    for master in snapshot.masters.values() {
        write_master_dict(out, master, &runtime.borrow().config);
    }
    Ok(())
}

fn write_master(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?;
    let runtime_ref = runtime.borrow();
    let snapshot = runtime_ref.snapshot();
    let Some(master) = snapshot.masters.get(key) else {
        RespSerializer::write_error(out, b"ERR No such master with that name");
        return Ok(());
    };
    write_master_dict(out, master, &runtime_ref.config);
    Ok(())
}

fn write_master_dict(
    out: &mut BytesMut,
    master: &crate::state::MasterState,
    config: &crate::config::SentinelConfig,
) {
    let fields = [
        ("name", master.name.clone()),
        ("ip", master.addr.ip().to_string()),
        ("port", master.addr.port().to_string()),
        ("runid", String::new()),
        ("flags", instance_flags_to_string(master.flags)),
        (
            "link-pending-commands",
            master.link_pending_commands.to_string(),
        ),
        ("link-refcount", master.link_refcount.to_string()),
        ("last-ping-sent", master.last_ping_sent.to_string()),
        ("last-ok-ping-reply", master.last_ok_ping.to_string()),
        ("last-ping-reply", master.last_ok_ping.to_string()),
        (
            "down-after-milliseconds",
            config.down_after_milliseconds(&master.name).to_string(),
        ),
        ("info-refresh", master.info_refresh.to_string()),
        (
            "role-reported",
            match master.role_reported {
                Role::Master => "master",
                Role::Slave => "slave",
                Role::Unknown => "unknown",
            }
            .to_owned(),
        ),
        ("role-reported-time", master.info_refresh.to_string()),
        ("config-epoch", master.config_epoch.to_string()),
        ("num-slaves", master.replicas.len().to_string()),
        ("num-other-sentinels", master.sentinels.len().to_string()),
        ("quorum", master.quorum.to_string()),
        (
            "failover-timeout",
            config.failover_timeout(&master.name).to_string(),
        ),
        (
            "parallel-syncs",
            config.parallel_syncs(&master.name).to_string(),
        ),
    ];
    RespSerializer::write_array_header(out, fields.len() * 2);
    for (key, value) in fields {
        RespSerializer::write_bulk_string(out, key.as_bytes());
        RespSerializer::write_bulk_string(out, value.as_bytes());
    }
}

fn write_replicas(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?;
    let runtime_ref = runtime.borrow();
    let snapshot = runtime_ref.snapshot();
    let Some(master) = snapshot.masters.get(key) else {
        RespSerializer::write_error(out, b"ERR No such master with that name");
        return Ok(());
    };
    RespSerializer::write_array_header(out, master.replicas.len());
    for replica in master.replicas.values() {
        let fields = [
            ("name", replica.name.clone()),
            ("ip", replica.addr.ip().to_string()),
            ("port", replica.addr.port().to_string()),
            ("runid", String::new()),
            ("flags", instance_flags_to_string(replica.flags)),
            (
                "master-link-down-time",
                replica.master_link_down_time.to_string(),
            ),
            (
                "master-link-status",
                match replica.master_link_status {
                    LinkStatus::Ok => "ok",
                    LinkStatus::Err => "err",
                }
                .to_owned(),
            ),
            ("master-host", master.addr.ip().to_string()),
            ("master-port", master.addr.port().to_string()),
            ("slave-priority", replica.slave_priority.to_string()),
            ("slave-repl-offset", replica.slave_repl_offset.to_string()),
            ("info-refresh", replica.info_refresh.to_string()),
            (
                "role-reported",
                match replica.role_reported {
                    Role::Master => "master",
                    Role::Slave => "slave",
                    Role::Unknown => "unknown",
                }
                .to_owned(),
            ),
            ("role-reported-time", replica.info_refresh.to_string()),
            ("last-ping-sent", replica.last_ping_sent.to_string()),
            ("last-ok-ping-reply", replica.last_ok_ping.to_string()),
            ("last-ping-reply", replica.last_ok_ping.to_string()),
            (
                "down-after-milliseconds",
                runtime_ref
                    .config
                    .down_after_milliseconds(&master.name)
                    .to_string(),
            ),
        ];
        RespSerializer::write_array_header(out, fields.len() * 2);
        for (field, value) in fields {
            RespSerializer::write_bulk_string(out, field.as_bytes());
            RespSerializer::write_bulk_string(out, value.as_bytes());
        }
    }
    Ok(())
}

fn write_sentinels(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?;
    let runtime_ref = runtime.borrow();
    let snapshot = runtime_ref.snapshot();
    let Some(master) = snapshot.masters.get(key) else {
        RespSerializer::write_error(out, b"ERR No such master with that name");
        return Ok(());
    };
    RespSerializer::write_array_header(out, master.sentinels.len());
    for sentinel in master.sentinels.values() {
        let fields = [
            ("name", sentinel.addr.to_string()),
            ("ip", sentinel.addr.ip().to_string()),
            ("port", sentinel.addr.port().to_string()),
            ("runid", sentinel.runid.clone()),
            ("flags", instance_flags_to_string(sentinel.flags)),
            ("last-ping-sent", "0".to_owned()),
            ("last-ok-ping-reply", sentinel.last_ok_ping.to_string()),
            ("last-ping-reply", sentinel.last_ok_ping.to_string()),
            (
                "down-after-milliseconds",
                runtime_ref
                    .config
                    .down_after_milliseconds(&master.name)
                    .to_string(),
            ),
            ("last-hello-message", sentinel.last_hello.to_string()),
            (
                "voted-leader",
                sentinel
                    .voted_leader
                    .as_ref()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ),
            ("voted-leader-epoch", sentinel.voted_epoch.to_string()),
        ];
        RespSerializer::write_array_header(out, fields.len() * 2);
        for (field, value) in fields {
            RespSerializer::write_bulk_string(out, field.as_bytes());
            RespSerializer::write_bulk_string(out, value.as_bytes());
        }
    }
    Ok(())
}

fn write_master_addr(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?;
    let snapshot = runtime.borrow().snapshot();
    let Some(master) = snapshot.masters.get(key) else {
        RespSerializer::write_null(out);
        return Ok(());
    };
    RespSerializer::write_array_header(out, 2);
    RespSerializer::write_bulk_string(out, master.addr.ip().to_string().as_bytes());
    RespSerializer::write_bulk_string(out, master.addr.port().to_string().as_bytes());
    Ok(())
}

fn reset_masters(out: &mut BytesMut, runtime: &SharedRuntime, pattern: &[u8]) -> SenkoResult<()> {
    let pattern = std::str::from_utf8(pattern)?;
    let mut reset = 0i64;
    let runtime_ref = &mut *runtime.borrow_mut();
    let _ = update_world(&runtime_ref.world, |snapshot| {
        for master in snapshot.masters.values_mut() {
            if glob_matches(pattern, &master.name) {
                reset += 1;
                master.flags.remove(
                    InstanceFlags::S_DOWN
                        | InstanceFlags::O_DOWN
                        | InstanceFlags::FAILOVER_IN_PROGRESS
                        | InstanceFlags::FORCE_FAILOVER,
                );
                master.failover_state = FailoverState::None;
                master.selected_replica = None;
                master.replicas.clear();
                master.sentinels.clear();
            }
        }
    });
    RespSerializer::write_integer(out, reset);
    Ok(())
}

fn force_failover(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?.to_owned();
    let now = current_unix_ms();
    let mut runtime_ref = runtime.borrow_mut();
    let my_id = runtime_ref.my_id();
    let world = runtime_ref.world.clone();
    let epoch = runtime_ref
        .elections
        .start_election(&world, &key, &my_id, now);
    let _ = update_world(&runtime_ref.world, |snapshot| {
        if let Some(master) = snapshot.masters.get_mut(&key) {
            if master.flags.contains(InstanceFlags::FAILOVER_IN_PROGRESS) {
                return;
            }
            master.flags.insert(InstanceFlags::FORCE_FAILOVER);
            begin_failover(master, epoch);
        }
    });
    RespSerializer::write_simple_string(out, b"OK");
    Ok(())
}

fn write_ckquorum(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?;
    let snapshot = runtime.borrow().snapshot();
    let Some(master) = snapshot.masters.get(key) else {
        RespSerializer::write_error(out, b"ERR No such master with that name");
        return Ok(());
    };
    let usable = 1 + master
        .sentinels
        .values()
        .filter(|sentinel| current_unix_ms().saturating_sub(sentinel.last_ok_ping) <= 2_000)
        .count();
    let majority = (master.sentinels.len() + 1) / 2 + 1;
    let message = if usable < master.quorum as usize {
        format!("NOQUORUM {usable} usable Sentinels. Quorum NOT reached for {key}.")
    } else if usable < majority {
        format!("NOQUORUM {usable} usable Sentinels. Majority NOT reached for {key}.")
    } else {
        format!("OK {usable} usable Sentinels. Quorum and failover authorization can be reached")
    };
    if message.starts_with("OK") {
        RespSerializer::write_bulk_string(out, message.as_bytes());
    } else {
        RespSerializer::write_error(out, message.as_bytes());
    }
    Ok(())
}

fn flush_config(out: &mut BytesMut, runtime: &SharedRuntime) -> SenkoResult<()> {
    let runtime_ref = runtime.borrow();
    flush_config_atomic(&runtime_ref.config, &runtime_ref.snapshot()).map_err(config_error)?;
    RespSerializer::write_simple_string(out, b"OK");
    Ok(())
}

fn monitor_master(out: &mut BytesMut, runtime: &SharedRuntime, args: &[&[u8]]) -> SenkoResult<()> {
    if args.len() != 5 {
        RespSerializer::write_error(
            out,
            b"ERR wrong number of arguments for 'sentinel|monitor' command",
        );
        return Ok(());
    }
    let name = std::str::from_utf8(args[1])?.to_owned();
    let host = std::str::from_utf8(args[2])?.to_owned();
    let port: u16 = std::str::from_utf8(args[3])?.parse()?;
    let quorum: u32 = std::str::from_utf8(args[4])?.parse()?;
    let host_ip = host.parse().map_err(SenkoError::from)?;
    {
        let runtime_ref = &mut *runtime.borrow_mut();
        let original = runtime_ref.config.clone();
        runtime_ref.config.masters.push(MasterConfig {
            name: name.clone(),
            host: host.clone(),
            port,
            quorum,
            ..MasterConfig::default()
        });
        if let Err(error) = runtime_ref.config.validate() {
            runtime_ref.config = original;
            return Err(config_error(error));
        }
        let _ = update_world(&runtime_ref.world, |snapshot| {
            snapshot.masters.insert(
                name.clone(),
                crate::state::MasterState {
                    name: name.clone(),
                    addr: SocketAddr::new(host_ip, port),
                    quorum,
                    flags: InstanceFlags::MASTER,
                    config_epoch: 0,
                    leader: None,
                    leader_epoch: 0,
                    replicas: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                    sentinels: hashbrown::HashMap::with_hasher(ahash::RandomState::new()),
                    last_ping_sent: 0,
                    last_ok_ping: current_unix_ms(),
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
        });
        runtime_ref
            .monitor
            .register_master(&name, SocketAddr::new(host_ip, port));
        let snapshot = runtime_ref.snapshot();
        flush_config_atomic(&runtime_ref.config, &snapshot).map_err(config_error)?;
    }
    RespSerializer::write_simple_string(out, b"OK");
    Ok(())
}

fn remove_master(out: &mut BytesMut, runtime: &SharedRuntime, name: &[u8]) -> SenkoResult<()> {
    let key = std::str::from_utf8(name)?.to_owned();
    let runtime_ref = &mut *runtime.borrow_mut();
    let original = runtime_ref.config.clone();
    runtime_ref
        .config
        .masters
        .retain(|master| master.name != key);
    if let Err(error) = runtime_ref.config.validate() {
        runtime_ref.config = original;
        return Err(config_error(error));
    }
    let _ = update_world(&runtime_ref.world, |snapshot| {
        snapshot.masters.remove(&key);
    });
    let snapshot = runtime_ref.snapshot();
    flush_config_atomic(&runtime_ref.config, &snapshot).map_err(config_error)?;
    RespSerializer::write_simple_string(out, b"OK");
    Ok(())
}

fn set_master_option(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    args: &[&[u8]],
) -> SenkoResult<()> {
    if args.len() != 4 {
        RespSerializer::write_error(
            out,
            b"ERR wrong number of arguments for 'sentinel|set' command",
        );
        return Ok(());
    }
    let name = std::str::from_utf8(args[1])?.to_owned();
    let option = ascii_upper(args[2]);
    let value = std::str::from_utf8(args[3])?;
    let runtime_ref = &mut *runtime.borrow_mut();
    let original = runtime_ref.config.clone();
    let deny_scripts_reconfig = runtime_ref.config.security.deny_scripts_reconfig;
    let Some(master) = runtime_ref.config.find_master_mut(&name) else {
        RespSerializer::write_error(out, b"ERR No such master with that name");
        return Ok(());
    };
    match option.as_str() {
        "DOWN-AFTER-MILLISECONDS" => {
            master.down_after_milliseconds = value.parse()?;
        }
        "FAILOVER-TIMEOUT" => {
            master.failover_timeout = value.parse()?;
        }
        "PARALLEL-SYNCS" => {
            master.parallel_syncs = value.parse()?;
        }
        "QUORUM" => {
            master.quorum = value.parse()?;
        }
        "AUTH-PASS" => {
            master.auth_pass = Some(value.to_owned());
        }
        "AUTH-USER" => {
            master.auth_user = Some(value.to_owned());
        }
        "NOTIFICATION-SCRIPT" => {
            if deny_scripts_reconfig {
                RespSerializer::write_error(out, b"ERR scripts reconfiguration denied");
                return Ok(());
            }
            master.notification_script = Some(value.into());
        }
        "CLIENT-RECONFIG-SCRIPT" => {
            if deny_scripts_reconfig {
                RespSerializer::write_error(out, b"ERR scripts reconfiguration denied");
                return Ok(());
            }
            master.client_reconfig_script = Some(value.into());
        }
        "MASTER-REBOOT-DOWN-AFTER-PERIOD" => {
            master.master_reboot_down_after_period = value.parse()?;
        }
        "RENAME-COMMAND" => {
            let mut parts = value.split_whitespace();
            let Some(command) = parts.next() else {
                RespSerializer::write_error(out, b"ERR rename-command requires two values");
                return Ok(());
            };
            let Some(renamed) = parts.next() else {
                RespSerializer::write_error(out, b"ERR rename-command requires two values");
                return Ok(());
            };
            if parts.next().is_some() {
                RespSerializer::write_error(out, b"ERR rename-command requires two values");
                return Ok(());
            }
            master
                .rename_commands
                .insert(command.to_ascii_uppercase(), renamed.to_owned());
        }
        _ => {
            runtime_ref.config = original;
            RespSerializer::write_error(out, b"ERR Unknown option for SENTINEL SET");
            return Ok(());
        }
    }
    if let Err(error) = runtime_ref.config.validate() {
        runtime_ref.config = original;
        return Err(config_error(error));
    }
    if option == "QUORUM" {
        let quorum = runtime_ref
            .config
            .find_master(&name)
            .map(|master| master.quorum)
            .unwrap_or_default();
        let _ = update_world(&runtime_ref.world, |snapshot| {
            if let Some(master) = snapshot.masters.get_mut(&name) {
                master.quorum = quorum;
            }
        });
    }
    let snapshot = runtime_ref.snapshot();
    flush_config_atomic(&runtime_ref.config, &snapshot).map_err(config_error)?;
    RespSerializer::write_simple_string(out, b"OK");
    Ok(())
}

fn write_info_cache(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    names: &[&[u8]],
) -> SenkoResult<()> {
    let runtime_ref = runtime.borrow();
    RespSerializer::write_array_header(out, names.len());
    for name in names {
        let key = std::str::from_utf8(name)?;
        if let Some(master) = runtime_ref.snapshot().masters.get(key) {
            RespSerializer::write_bulk_string(out, &master.cached_info);
        } else {
            RespSerializer::write_bulk_string(out, b"");
        }
    }
    Ok(())
}

fn write_pending_scripts(out: &mut BytesMut, runtime: &SharedRuntime) -> SenkoResult<()> {
    let runtime_ref = runtime.borrow();
    RespSerializer::write_array_header(out, runtime_ref.notifier.pending_scripts().len());
    for script in runtime_ref.notifier.pending_scripts() {
        let fields = [
            ("pid", script.pid.to_string()),
            ("script", script.script.to_string()),
            (
                "args",
                script
                    .args
                    .iter()
                    .map(CompactString::as_str)
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            ("start_time", script.start_time.to_string()),
        ];
        RespSerializer::write_array_header(out, fields.len() * 2);
        for (field, value) in fields {
            RespSerializer::write_bulk_string(out, field.as_bytes());
            RespSerializer::write_bulk_string(out, value.as_bytes());
        }
    }
    Ok(())
}

fn is_master_down_by_addr(
    out: &mut BytesMut,
    runtime: &SharedRuntime,
    args: &[&[u8]],
) -> SenkoResult<()> {
    if args.len() != 5 {
        RespSerializer::write_error(
            out,
            b"ERR wrong number of arguments for 'sentinel|is-master-down-by-addr' command",
        );
        return Ok(());
    }
    let ip = std::str::from_utf8(args[1])?;
    let port: u16 = std::str::from_utf8(args[2])?.parse()?;
    let epoch: u64 = std::str::from_utf8(args[3])?.parse()?;
    let runid = std::str::from_utf8(args[4])?;
    let mut reply_leader = String::new();
    let mut reply_epoch = 0u64;
    let mut down = 0i64;
    let runtime_ref = &mut *runtime.borrow_mut();
    let target = SocketAddr::new(ip.parse().map_err(SenkoError::from)?, port);
    let snapshot = runtime_ref.snapshot();
    if let Some((name, master)) = snapshot
        .masters
        .iter()
        .find(|(_, master)| master.addr == target)
    {
        down = i64::from(master.flags.contains(InstanceFlags::S_DOWN));
        if runid != "*" {
            let (leader, vote_epoch) =
                runtime_ref
                    .elections
                    .process_vote_request(name, CompactString::from(runid), epoch);
            reply_leader = leader.map(|value| value.to_string()).unwrap_or_default();
            reply_epoch = vote_epoch;
        }
    }
    RespSerializer::write_array_header(out, 3);
    RespSerializer::write_integer(out, down);
    RespSerializer::write_bulk_string(out, reply_leader.as_bytes());
    RespSerializer::write_integer(out, reply_epoch as i64);
    Ok(())
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

fn config_error(error: crate::config::ConfigError) -> SenkoError {
    match error {
        crate::config::ConfigError::Io(error) => SenkoError::Io(error),
        other => SenkoError::ProtocolMessage(other.to_string().into()),
    }
}

#[allow(clippy::too_many_lines)]
pub fn drive_failovers(runtime: &SharedRuntime) {
    let now = current_unix_ms();
    let mut runtime_ref = runtime.borrow_mut();
    let snapshot = runtime_ref.snapshot();
    let masters = snapshot.masters.keys().cloned().collect::<Vec<_>>();
    drop(snapshot);
    let my_id = runtime_ref.my_id();
    for name in masters {
        let down_after_ms = runtime_ref.down_after_ms(&name);
        let failover_timeout = runtime_ref.failover_timeout(&name);
        let parallel_syncs = runtime_ref.parallel_syncs(&name);
        let is_leader = runtime_ref
            .elections
            .is_leader(&runtime_ref.world, &name, &my_id);
        let mut events = Vec::new();
        let _ = update_world(&runtime_ref.world, |snapshot| {
            if let Some(master) = snapshot.masters.get_mut(&name) {
                let transition = advance_failover(
                    master,
                    now,
                    down_after_ms,
                    failover_timeout,
                    parallel_syncs,
                    is_leader,
                );
                events.extend(transition.emitted);
            }
        });
        for event in events {
            runtime_ref.notifier.emit(event, name.as_str());
        }
    }
}
