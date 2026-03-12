use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::BytesMut;
use compact_str::CompactString;
use senko_cluster::NodeId;
use senko_core::SenkoConfig;
use senko_proto::{Frame, RespSerializer};
use senko_store::{Response, commands::generic::migrate};
use smallvec::SmallVec;

use crate::{
    cluster::replication::{DEFAULT_REPL_BACKLOG_SIZE, ReplicaAckTracker, ShardReplication},
    commands::server::info::{self, ReplicationRole, ServerCommandOutcome},
    connection::{
        ConnectionMeta, error_bytes, error_message, frame_bytes, serialize_response, simple_string,
    },
};

static FAILOVER_PENDING: AtomicBool = AtomicBool::new(false);
static REPLICATION_RUNTIME: OnceLock<Arc<ReplicationRuntime>> = OnceLock::new();

#[derive(Debug)]
struct ReplicationRuntime {
    shards: Box<[ShardReplicationState]>,
}

#[derive(Debug)]
struct ShardReplicationState {
    backlog: Arc<ShardReplication>,
    ack_tracker: Arc<ReplicaAckTracker>,
    replicas: Mutex<HashSet<u64>>,
}

pub fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    resp3: bool,
    shard_id: usize,
    meta: &mut ConnectionMeta,
    config: &SenkoConfig,
) -> Option<Result<ServerCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"REPLICAOF") || eq_ascii(command, b"SLAVEOF") {
        return Some(handle_replicaof(args));
    }
    if eq_ascii(command, b"WAIT") {
        return Some(handle_wait(args, shard_id, meta));
    }
    if eq_ascii(command, b"PSYNC") {
        return Some(handle_psync(args, shard_id, meta));
    }
    if eq_ascii(command, b"REPLCONF") {
        return Some(handle_replconf(args, shard_id, meta, resp3));
    }
    if eq_ascii(command, b"SYNC") {
        return Some(handle_sync(args));
    }
    if eq_ascii(command, b"FAILOVER") {
        return Some(handle_failover(args));
    }
    if eq_ascii(command, b"RESTORE-ASKING") {
        return Some(handle_restore_asking(args, config));
    }
    if eq_ascii(command, b"MODULE") {
        return Some(handle_module(args, resp3));
    }
    None
}

pub(crate) fn init(config: &SenkoConfig) {
    let backlog_size = usize::try_from(config.repl_backlog_size)
        .ok()
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_REPL_BACKLOG_SIZE);
    let _ = REPLICATION_RUNTIME.set(Arc::new(ReplicationRuntime::new(
        config.num_shards,
        backlog_size,
    )));
}

pub(crate) fn on_disconnect(shard_id: usize, conn_id: u64) {
    let Some(shard) = runtime().and_then(|runtime| runtime.shard(shard_id)) else {
        return;
    };
    if let Ok(mut replicas) = shard.replicas.lock() {
        replicas.remove(&conn_id);
    }
    shard
        .ack_tracker
        .remove_replica(&connection_node_id(conn_id));
}

pub(crate) fn record_write(
    shard_id: usize,
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
) {
    let Some(shard) = runtime().and_then(|runtime| runtime.shard(shard_id)) else {
        return;
    };
    let Ok(payload) = encode_command(command, args) else {
        return;
    };
    let offset = shard
        .backlog
        .append_command(&payload)
        .unwrap_or_else(|_| shard.backlog.backlog().head_offset());
    meta.last_write_replication_offset = offset;
}

pub(crate) fn current_offset(shard_id: usize) -> u64 {
    runtime()
        .and_then(|runtime| runtime.shard(shard_id))
        .map(|shard| shard.backlog.backlog().head_offset())
        .unwrap_or(0)
}

fn handle_replicaof(args: &[Frame<'_>]) -> Result<ServerCommandOutcome, Vec<u8>> {
    match args {
        [no, one]
            if eq_ascii(frame_bytes(no).map_err(|error| error_bytes(&error))?, b"NO")
                && eq_ascii(
                    frame_bytes(one).map_err(|error| error_bytes(&error))?,
                    b"ONE",
                ) =>
        {
            info::set_replication_primary();
            let _ = info::regenerate_replication_id();
            Ok(ok_outcome())
        }
        [host, port] => {
            let host = std::str::from_utf8(frame_bytes(host).map_err(|error| error_bytes(&error))?)
                .map_err(|_| error_message("ERR syntax error"))?
                .to_owned();
            let port = parse_port(port)?;
            if info::replication_role() == ReplicationRole::Replica
                && info::replica_primary_target().is_some_and(|(current_host, current_port)| {
                    current_host == host && current_port == port
                })
            {
                return Ok(ok_outcome());
            }
            info::set_replication_replica(host, port);
            Ok(ok_outcome())
        }
        _ => Err(error_message(
            "ERR wrong number of arguments for 'replicaof' command",
        )),
    }
}

fn handle_wait(
    args: &[Frame<'_>],
    shard_id: usize,
    meta: &ConnectionMeta,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    if args.len() != 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'wait' command",
        ));
    }
    let replicas = usize::try_from(parse_u64(&args[0])?)
        .map_err(|_| error_message("ERR value is not an integer or out of range"))?;
    let timeout_ms = parse_u64(&args[1])?;
    let acknowledged = runtime()
        .and_then(|runtime| runtime.shard(shard_id))
        .map(|shard| {
            shard.ack_tracker.wait_for(
                replicas,
                meta.last_write_replication_offset,
                Duration::from_millis(timeout_ms),
            )
        })
        .unwrap_or(0);
    Ok(outcome(serialize_response(
        &Response::Integer(acknowledged as i64),
        false,
    )))
}

fn handle_psync(
    args: &[Frame<'_>],
    shard_id: usize,
    meta: &mut ConnectionMeta,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    if args.len() != 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'psync' command",
        ));
    }
    let requested_replid = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    let _requested_offset = parse_psync_offset(&args[1])?;
    meta.flags
        .insert(crate::connection::ConnectionFlags::REPLICA);
    if let Some(shard) = runtime().and_then(|runtime| runtime.shard(shard_id))
        && let Ok(mut replicas) = shard.replicas.lock()
    {
        replicas.insert(meta.id);
    }
    let replid = info::current_replication_id();
    let offset = current_offset(shard_id);
    if requested_replid.eq_ignore_ascii_case(replid.as_bytes())
        && parse_psync_offset(&args[1]).unwrap_or(-1) == offset as i64
    {
        return Ok(raw_outcome(b"+CONTINUE\r\n".to_vec()));
    }
    Ok(raw_outcome(
        format!("+FULLRESYNC {replid} {offset}\r\n").into_bytes(),
    ))
}

fn handle_replconf(
    args: &[Frame<'_>],
    shard_id: usize,
    meta: &mut ConnectionMeta,
    resp3: bool,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'replconf' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"LISTENING-PORT") {
        let [port] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        };
        meta.replica_listening_port = Some(parse_port(port)?);
        return Ok(ok_outcome());
    }
    if eq_ascii(subcommand, b"IP-ADDRESS") {
        let [ip] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        };
        let ip = std::str::from_utf8(frame_bytes(ip).map_err(|error| error_bytes(&error))?)
            .map_err(|_| error_message("ERR syntax error"))?;
        meta.replica_ip_address = Some(CompactString::from(ip));
        return Ok(ok_outcome());
    }
    if eq_ascii(subcommand, b"CAPA") {
        let [capa] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        };
        let capa = frame_bytes(capa).map_err(|error| error_bytes(&error))?;
        if eq_ascii(capa, b"EOF") {
            meta.replica_eof = true;
        } else if eq_ascii(capa, b"PSYNC2") {
            meta.replica_psync2 = true;
        }
        return Ok(ok_outcome());
    }
    if eq_ascii(subcommand, b"ACK") {
        let [offset] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        };
        meta.replica_ack_offset = parse_u64(offset)?;
        if meta
            .flags
            .contains(crate::connection::ConnectionFlags::REPLICA)
            && let Some(shard) = runtime().and_then(|runtime| runtime.shard(shard_id))
        {
            shard
                .ack_tracker
                .record_ack(connection_node_id(meta.id), meta.replica_ack_offset);
        }
        return Ok(ServerCommandOutcome {
            response: Vec::new(),
            close_after_write: false,
            suppress_response: true,
            force_send_response: false,
        });
    }
    if eq_ascii(subcommand, b"GETACK") {
        if rest.len() != 1 {
            return Err(error_message(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        }
        return Ok(outcome(serialize_response(
            &Response::Array(Box::new(
                [
                    bulk_response(b"REPLCONF"),
                    bulk_response(b"ACK"),
                    bulk_response(current_offset(shard_id).to_string().as_bytes()),
                ]
                .into_iter()
                .collect::<SmallVec<[Response; 16]>>(),
            )),
            resp3,
        )));
    }
    Ok(ok_outcome())
}

fn handle_sync(args: &[Frame<'_>]) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'sync' command",
        ));
    }
    Err(error_message("ERR SYNC is deprecated, use PSYNC"))
}

fn handle_failover(args: &[Frame<'_>]) -> Result<ServerCommandOutcome, Vec<u8>> {
    if args.len() == 1
        && eq_ascii(
            frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?,
            b"ABORT",
        )
    {
        if FAILOVER_PENDING.swap(false, Ordering::SeqCst) {
            return Ok(ok_outcome());
        }
        return Err(error_message("ERR no failover in progress"));
    }

    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        if eq_ascii(token, b"TO") {
            if index + 2 >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            let _ = frame_bytes(&args[index + 1]).map_err(|error| error_bytes(&error))?;
            let _ = parse_port(&args[index + 2])?;
            index += 3;
            continue;
        }
        if eq_ascii(token, b"FORCE") {
            index += 1;
            continue;
        }
        if eq_ascii(token, b"TIMEOUT") {
            if index + 1 >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            let _ = parse_u64(&args[index + 1])?;
            index += 2;
            continue;
        }
        if eq_ascii(token, b"ABORT") {
            return Err(error_message("ERR syntax error"));
        }
        return Err(error_message("ERR syntax error"));
    }
    FAILOVER_PENDING.store(true, Ordering::SeqCst);
    Ok(ok_outcome())
}

fn handle_restore_asking(
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<ServerCommandOutcome, Vec<u8>> {
    if !config.cluster_enabled {
        return Err(error_message("ERR cluster not enabled"));
    }
    match migrate::restore(&mut senko_store::Store::default(), args) {
        Ok(response) => Ok(outcome(serialize_response(&response, false))),
        Err(error) => Err(error_message(&error.to_string())),
    }
}

fn handle_module(args: &[Frame<'_>], resp3: bool) -> Result<ServerCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'module' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"LIST") {
        if !rest.is_empty() {
            return Err(error_message(
                "ERR wrong number of arguments for 'module|list' command",
            ));
        }
        return Ok(outcome(crate::modules::serialize(
            &crate::modules::list_response(),
            resp3,
        )));
    }
    if eq_ascii(subcommand, b"LOAD") {
        return Err(error_message(
            "ERR Module loading is not supported in Senko Phase 1",
        ));
    }
    if eq_ascii(subcommand, b"LOADEX") {
        return Err(error_message(
            "ERR Module loading is not supported in Senko Phase 1",
        ));
    }
    if eq_ascii(subcommand, b"UNLOAD") {
        return Err(error_message(
            "ERR Module unloading is not supported in Senko Phase 1",
        ));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'MODULE'",
    ))
}

fn parse_port(frame: &Frame<'_>) -> Result<u16, Vec<u8>> {
    let port = parse_u64(frame)?;
    u16::try_from(port).map_err(|_| error_message("ERR value is not an integer or out of range"))
}

fn parse_u64(frame: &Frame<'_>) -> Result<u64, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))
}

fn bulk_response(bytes: &[u8]) -> Response {
    Response::Value(Some(senko_core::SenkoValue::from(bytes)))
}

fn ok_outcome() -> ServerCommandOutcome {
    outcome(simple_string(b"OK"))
}

fn raw_outcome(response: Vec<u8>) -> ServerCommandOutcome {
    outcome(response)
}

fn outcome(response: Vec<u8>) -> ServerCommandOutcome {
    ServerCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

impl ReplicationRuntime {
    fn new(num_shards: usize, backlog_size: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for shard_id in 0..num_shards {
            shards.push(ShardReplicationState {
                backlog: Arc::new(ShardReplication::new(shard_id as u16, backlog_size)),
                ack_tracker: Arc::new(ReplicaAckTracker::default()),
                replicas: Mutex::new(HashSet::new()),
            });
        }
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    fn shard(&self, shard_id: usize) -> Option<&ShardReplicationState> {
        self.shards.get(shard_id)
    }
}

fn runtime() -> Option<&'static Arc<ReplicationRuntime>> {
    REPLICATION_RUNTIME.get()
}

fn encode_command(command: &[u8], args: &[Frame<'_>]) -> Result<Vec<u8>, Vec<u8>> {
    let mut out = BytesMut::new();
    RespSerializer::write_array_header(&mut out, args.len() + 1);
    RespSerializer::write_bulk_string(&mut out, command);
    for arg in args {
        RespSerializer::write_bulk_string(
            &mut out,
            frame_bytes(arg).map_err(|error| error_bytes(&error))?,
        );
    }
    Ok(out.to_vec())
}

fn connection_node_id(conn_id: u64) -> NodeId {
    let mut bytes = [0_u8; 20];
    bytes[12..].copy_from_slice(&conn_id.to_be_bytes());
    NodeId::new(bytes)
}

fn parse_psync_offset(frame: &Frame<'_>) -> Result<i64, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::atomic::Ordering,
    };

    use compact_str::CompactString;
    use senko_core::SenkoConfig;
    use senko_proto::Frame;

    use crate::{
        commands::server::info,
        connection::{ConnectionFlags, ConnectionMeta, ReplyMode},
    };

    use super::{FAILOVER_PENDING, execute};

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    fn meta() -> ConnectionMeta {
        ConnectionMeta {
            id: 1,
            username: CompactString::const_new("default"),
            name: None,
            db: 0,
            flags: ConnectionFlags::empty(),
            created_at: 0,
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            resp_version: 2,
            no_evict: false,
            no_touch: false,
            reply_mode: ReplyMode::Normal,
            watch_count: 0,
            multi_queue_len: -1,
            tracking_redirect: -1,
            tracking_optin: false,
            tracking_optout: false,
            tracking_bcast: false,
            tracking_noloop: false,
            tracking_prefixes: smallvec::SmallVec::new(),
            tracking_caching: None,
            replica_listening_port: None,
            replica_ip_address: None,
            replica_psync2: false,
            replica_eof: false,
            replica_ack_offset: 0,
            last_write_replication_offset: 0,
        }
    }

    #[test]
    fn psync_stub_uses_40_char_replication_id() {
        info::init(&SenkoConfig {
            num_shards: 1,
            ..SenkoConfig::default()
        });
        let mut meta = meta();
        let outcome = execute(
            b"PSYNC",
            &[bs(b"?"), bs(b"-1")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap()
        .unwrap();
        let text = String::from_utf8(outcome.response).unwrap();
        let parts = text.trim().split(' ').collect::<Vec<_>>();
        assert_eq!(parts[0], "+FULLRESYNC");
        assert_eq!(parts[1].len(), 40);
        assert_eq!(parts[2], "0");
    }

    #[test]
    fn replconf_ack_suppresses_response() {
        let mut meta = meta();
        let outcome = execute(
            b"REPLCONF",
            &[bs(b"ACK"), bs(b"0")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap()
        .unwrap();
        assert!(outcome.suppress_response);
    }

    #[test]
    fn failover_abort_without_pending_errors() {
        FAILOVER_PENDING.store(false, Ordering::SeqCst);
        let mut meta = meta();
        let result = execute(
            b"FAILOVER",
            &[bs(b"ABORT")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn replicaof_no_one_returns_ok() {
        info::init(&SenkoConfig {
            num_shards: 1,
            ..SenkoConfig::default()
        });
        let mut meta = meta();
        let outcome = execute(
            b"REPLICAOF",
            &[bs(b"NO"), bs(b"ONE")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(String::from_utf8(outcome.response).unwrap(), "+OK\r\n");
    }

    #[test]
    fn module_list_returns_empty_array() {
        let mut meta = meta();
        let outcome = execute(
            b"MODULE",
            &[bs(b"LIST")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(String::from_utf8(outcome.response).unwrap(), "*0\r\n");
    }

    #[test]
    fn restore_asking_errors_when_cluster_disabled() {
        let mut meta = meta();
        let result = execute(
            b"RESTORE-ASKING",
            &[bs(b"k"), bs(b"0"), bs(b"payload")],
            false,
            0,
            &mut meta,
            &SenkoConfig::default(),
        )
        .unwrap();
        assert!(
            String::from_utf8(result.unwrap_err())
                .unwrap()
                .contains("ERR cluster not enabled")
        );
    }
}
