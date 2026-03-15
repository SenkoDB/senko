#![allow(clippy::too_many_arguments)]

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use bytes::Bytes;
use compact_str::CompactString;
use senko_core::SenkoValue;
use senko_proto::Frame;
use senko_store::Response;
use smallvec::{SmallVec, smallvec};

use crate::{
    blocked::BlockedKeyRegistry,
    commands::connection::client_ops,
    connection::{
        ClientConnectionMap, ConnectionFlags, ConnectionMeta, ConnectionState, ReplyMode,
        bulk_string, current_unix_ms, error_bytes, error_message, frame_bytes, serialize_response,
        simple_string,
    },
    transaction::{TxState, WatchRegistry, WatchState},
};

#[derive(Debug)]
pub(crate) struct ClientCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

pub(crate) fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
    qbuf_len: usize,
    pause_state: &Rc<RefCell<client_ops::PauseState>>,
    tracking_registry: &Rc<RefCell<client_ops::TrackingRegistry>>,
) -> Option<Result<ClientCommandOutcome, Vec<u8>>> {
    if !eq_ascii(command, b"CLIENT") {
        return None;
    }
    Some(dispatch_client(
        args,
        meta,
        client_connections,
        state,
        tx_state,
        blocked,
        watch_registry,
        watch_state,
        qbuf_len,
        pause_state,
        tracking_registry,
    ))
}

fn dispatch_client(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
    qbuf_len: usize,
    pause_state: &Rc<RefCell<client_ops::PauseState>>,
    tracking_registry: &Rc<RefCell<client_ops::TrackingRegistry>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'client' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    meta.last_cmd = Some(CompactString::from(format!(
        "client|{}",
        String::from_utf8_lossy(subcommand).to_ascii_lowercase()
    )));
    meta.last_cmd_at = current_unix_ms();

    if eq_ascii(subcommand, b"ID") {
        return client_id(rest, meta);
    }
    if eq_ascii(subcommand, b"GETNAME") {
        return client_getname(rest, meta);
    }
    if eq_ascii(subcommand, b"SETNAME") {
        return client_setname(rest, meta);
    }
    if eq_ascii(subcommand, b"SETINFO") {
        return client_setinfo(rest, meta);
    }
    if eq_ascii(subcommand, b"INFO") {
        return client_info(rest, meta, watch_state, tx_state, qbuf_len);
    }
    if eq_ascii(subcommand, b"LIST") {
        return client_list(rest, meta, client_connections, qbuf_len);
    }
    if eq_ascii(subcommand, b"NO-EVICT") {
        return client_no_evict(rest, meta);
    }
    if eq_ascii(subcommand, b"NO-TOUCH") {
        return client_no_touch(rest, meta);
    }
    if eq_ascii(subcommand, b"REPLY") {
        return client_reply(rest, meta);
    }
    if eq_ascii(subcommand, b"CACHING") {
        return client_caching(rest, meta);
    }
    if eq_ascii(subcommand, b"GETREDIR") {
        return client_getredir(rest, meta);
    }
    if eq_ascii(subcommand, b"TRACKINGINFO") {
        return client_trackinginfo(rest, meta);
    }
    if eq_ascii(subcommand, b"HELP") {
        return client_help(rest);
    }
    if let Some(result) = client_ops::handle_ops(
        subcommand,
        rest,
        meta,
        client_connections,
        pause_state,
        tracking_registry,
        state,
        tx_state,
        blocked,
        watch_registry,
        watch_state,
    ) {
        return result;
    }

    if eq_ascii(subcommand, b"KILL")
        || eq_ascii(subcommand, b"PAUSE")
        || eq_ascii(subcommand, b"UNPAUSE")
        || eq_ascii(subcommand, b"UNBLOCK")
        || eq_ascii(subcommand, b"TRACKING")
    {
        return client_phase2_placeholder(
            subcommand,
            rest,
            meta,
            state,
            tx_state,
            blocked,
            watch_registry,
            watch_state,
        );
    }

    Err(error_message(&format!(
        "ERR unknown subcommand '{}'. Try CLIENT HELP.",
        String::from_utf8_lossy(subcommand)
    )))
}

fn client_id(args: &[Frame<'_>], meta: &ConnectionMeta) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|id' command",
        ));
    }
    // NOTE: IDs are shard-local. Use ADDR for cross-shard identity.
    let response = serialize_response(&Response::Integer(meta.id as i64), meta.resp_version == 3);
    Ok(ok_outcome(response))
}

fn client_getname(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|getname' command",
        ));
    }
    let response = match &meta.name {
        Some(name) => bulk_string(name.as_bytes()),
        None => serialize_response(&Response::Value(None), meta.resp_version == 3),
    };
    Ok(ok_outcome(response))
}

fn client_setname(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    let [name] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|setname' command",
        ));
    };
    let name = frame_bytes(name).map_err(|error| error_bytes(&error))?;
    if name.is_empty() {
        meta.name = None;
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    validate_client_name(name)?;
    meta.name = Some(CompactString::from_utf8(name).map_err(|_| {
        error_message("ERR Client names cannot contain spaces, newlines or special characters.")
    })?);
    Ok(ok_outcome(simple_string(b"OK")))
}

fn client_setinfo(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if args.len() != 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|setinfo' command",
        ));
    }
    let field = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    let value = frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?;
    validate_client_name(value)?;
    let value = CompactString::from_utf8(value).map_err(|_| {
        error_message("ERR Client names cannot contain spaces, newlines or special characters.")
    })?;
    if eq_ascii(field, b"LIB-NAME") {
        meta.lib_name = Some(value);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    if eq_ascii(field, b"LIB-VER") {
        meta.lib_ver = Some(value);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    Err(error_message("ERR syntax error"))
}

fn client_info(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
    watch_state: &Rc<RefCell<WatchState>>,
    tx_state: &TxState,
    qbuf_len: usize,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|info' command",
        ));
    }
    let line = format_client_line(meta, Some(watch_state), Some(tx_state), qbuf_len);
    Ok(ok_outcome(bulk_string(line.as_bytes())))
}

fn client_list(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    qbuf_len: usize,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    // NOTE: CLIENT LIST is shard-local in Phase 1. Cross-shard aggregation is a Phase 2 concern.
    let filters = parse_client_list_filters(args)?;
    let mut lines = String::new();
    for handle in client_connections.borrow().values() {
        let Some(mut snapshot) = handle.meta.lock().ok().map(|guard| guard.clone()) else {
            continue;
        };
        if snapshot.id == meta.id {
            snapshot = meta.clone();
        }
        if !matches_type_filter(&snapshot, filters.r#type.as_deref()) {
            continue;
        }
        if !filters.ids.is_empty() && !filters.ids.contains(&snapshot.id) {
            continue;
        }
        let qbuf = if snapshot.id == meta.id { qbuf_len } else { 0 };
        lines.push_str(&format_client_line(&snapshot, None, None, qbuf));
    }
    Ok(ok_outcome(bulk_string(lines.as_bytes())))
}

fn client_no_evict(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    meta.no_evict = parse_on_off(args, "client|no-evict")?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn client_no_touch(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    meta.no_touch = parse_on_off(args, "client|no-touch")?;
    Ok(ok_outcome(simple_string(b"OK")))
}

fn client_reply(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    let [mode] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|reply' command",
        ));
    };
    let mode = frame_bytes(mode).map_err(|error| error_bytes(&error))?;
    if eq_ascii(mode, b"ON") {
        meta.reply_mode = ReplyMode::Normal;
        return Ok(ClientCommandOutcome {
            response: simple_string(b"OK"),
            close_after_write: false,
            suppress_response: false,
            force_send_response: true,
        });
    }
    if eq_ascii(mode, b"OFF") {
        meta.reply_mode = ReplyMode::Off;
        return Ok(ClientCommandOutcome {
            response: simple_string(b"OK"),
            close_after_write: false,
            suppress_response: true,
            force_send_response: false,
        });
    }
    if eq_ascii(mode, b"SKIP") {
        meta.reply_mode = ReplyMode::Skip;
        return Ok(ClientCommandOutcome {
            response: simple_string(b"OK"),
            close_after_write: false,
            suppress_response: true,
            force_send_response: false,
        });
    }
    Err(error_message("ERR syntax error"))
}

fn client_caching(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    let [mode] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|caching' command",
        ));
    };
    if !meta.flags.contains(ConnectionFlags::TRACKING)
        || (!meta.tracking_optin && !meta.tracking_optout)
    {
        return Err(error_message(
            "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
        ));
    }
    let mode = frame_bytes(mode).map_err(|error| error_bytes(&error))?;
    if eq_ascii(mode, b"YES") {
        meta.tracking_caching = Some(true);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    if eq_ascii(mode, b"NO") {
        meta.tracking_caching = Some(false);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    Err(error_message("ERR syntax error"))
}

fn client_getredir(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|getredir' command",
        ));
    }
    let response = serialize_response(
        &Response::Integer(meta.tracking_redirect),
        meta.resp_version == 3,
    );
    Ok(ok_outcome(response))
}

fn client_trackinginfo(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|trackinginfo' command",
        ));
    }
    let mut flags = SmallVec::<[Response; 16]>::new();
    if !meta.flags.contains(ConnectionFlags::TRACKING) {
        flags.push(bulk_value(b"off"));
    } else {
        flags.push(bulk_value(b"on"));
        if meta.tracking_bcast {
            flags.push(bulk_value(b"bcast"));
        }
        if meta.tracking_noloop {
            flags.push(bulk_value(b"noloop"));
        }
        if meta.tracking_optin {
            flags.push(bulk_value(b"optin"));
        }
        if meta.tracking_optout {
            flags.push(bulk_value(b"optout"));
        }
    }
    let prefixes = Response::Array(Box::new(SmallVec::from_iter(
        meta.tracking_prefixes
            .iter()
            .map(|prefix| bulk_value(prefix.as_bytes())),
    )));
    let response = Response::Map(Box::new(smallvec![
        bulk_value(b"flags"),
        Response::Array(Box::new(flags)),
        bulk_value(b"redirect"),
        Response::Integer(meta.tracking_redirect),
        bulk_value(b"prefixes"),
        prefixes,
    ]));
    Ok(ok_outcome(serialize_response(
        &response,
        meta.resp_version == 3,
    )))
}

fn client_help(args: &[Frame<'_>]) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|help' command",
        ));
    }
    const HELP: [&[u8]; 14] = [
        b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        b"ID",
        b"GETNAME",
        b"SETNAME <name>",
        b"SETINFO LIB-NAME <name> | LIB-VER <version>",
        b"INFO",
        b"LIST [TYPE <type>] [ID <id> ...]",
        b"NO-EVICT ON|OFF",
        b"NO-TOUCH ON|OFF",
        b"REPLY ON|OFF|SKIP",
        b"CACHING YES|NO",
        b"GETREDIR",
        b"TRACKINGINFO",
        b"HELP",
    ];
    let response = Response::Array(Box::new(SmallVec::from_iter(
        HELP.into_iter().map(bulk_value),
    )));
    Ok(ok_outcome(serialize_response(&response, false)))
}

fn client_phase2_placeholder(
    subcommand: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if eq_ascii(subcommand, b"TRACKING") {
        if args
            .iter()
            .any(|arg| matches!(frame_bytes(arg), Ok(value) if eq_ascii(value, b"ON")))
        {
            meta.flags.insert(ConnectionFlags::TRACKING);
        }
        if args
            .iter()
            .any(|arg| matches!(frame_bytes(arg), Ok(value) if eq_ascii(value, b"OFF")))
        {
            meta.flags.remove(ConnectionFlags::TRACKING);
            meta.tracking_optin = false;
            meta.tracking_optout = false;
            meta.tracking_bcast = false;
            meta.tracking_noloop = false;
            meta.tracking_prefixes.clear();
            meta.tracking_redirect = -1;
        }
    }
    if eq_ascii(subcommand, b"UNBLOCK")
        && let ConnectionState::Blocked { .. } = state
    {
        blocked.borrow_mut().remove_client(meta.id);
        *state = ConnectionState::Reading;
        meta.flags.remove(ConnectionFlags::BLOCKED);
    }
    if eq_ascii(subcommand, b"KILL")
        || eq_ascii(subcommand, b"PAUSE")
        || eq_ascii(subcommand, b"UNPAUSE")
    {
        let _ = tx_state;
        let _ = watch_registry;
        let _ = watch_state;
    }
    Err(error_message(&format!(
        "ERR CLIENT subcommand '{}' not supported in Senko Phase 1",
        String::from_utf8_lossy(subcommand)
    )))
}

pub(crate) struct ClientListFilters {
    r#type: Option<String>,
    ids: HashSet<u64>,
}

pub(crate) fn parse_client_list_filters(args: &[Frame<'_>]) -> Result<ClientListFilters, Vec<u8>> {
    let mut index = 0usize;
    let mut r#type = None;
    let mut ids = HashSet::new();
    while index < args.len() {
        let token = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        index += 1;
        if eq_ascii(token, b"TYPE") {
            if index >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            let value = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            r#type = Some(String::from_utf8_lossy(value).to_ascii_uppercase());
            index += 1;
            continue;
        }
        if eq_ascii(token, b"ID") {
            if index >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            while index < args.len() {
                let value = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
                if eq_ascii(value, b"TYPE") || eq_ascii(value, b"ID") {
                    break;
                }
                let id = std::str::from_utf8(value)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
                    .ok_or_else(|| error_message("ERR syntax error"))?;
                ids.insert(id);
                index += 1;
            }
            continue;
        }
        return Err(error_message("ERR syntax error"));
    }
    Ok(ClientListFilters { r#type, ids })
}

fn matches_type_filter(meta: &ConnectionMeta, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some("PUBSUB") => meta.flags.contains(ConnectionFlags::PUBSUB),
        Some("NORMAL") => {
            !meta.flags.contains(ConnectionFlags::PUBSUB)
                && !meta.flags.contains(ConnectionFlags::REPLICA)
        }
        Some("MASTER") | Some("REPLICA") => false,
        Some(_) => false,
    }
}

fn format_client_line(
    meta: &ConnectionMeta,
    watch_state: Option<&Rc<RefCell<WatchState>>>,
    tx_state: Option<&TxState>,
    qbuf_len: usize,
) -> String {
    let now_ms = current_unix_ms();
    let age = now_ms.saturating_sub(meta.created_at) / 1_000;
    let idle = if meta.last_cmd_at == 0 {
        age
    } else {
        now_ms.saturating_sub(meta.last_cmd_at) / 1_000
    };
    let watch = watch_state
        .map(|state| state.borrow().watched_keys.len() as u32)
        .unwrap_or(meta.watch_count);
    let multi = tx_state.map(tx_queue_len).unwrap_or(meta.multi_queue_len);
    let name = meta.name.as_ref().map_or("", CompactString::as_str);
    let cmd = meta.last_cmd.as_ref().map_or("NULL", CompactString::as_str);
    let library_name = meta.lib_name.as_ref().map_or("", CompactString::as_str);
    let library_ver = meta.lib_ver.as_ref().map_or("", CompactString::as_str);
    format!(
        "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub=0 psub=0 ssub=0 multi={} watch={} qbuf={} qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 events=rw cmd={} user={} library-name={} library-ver={}\n",
        meta.id,
        meta.peer_addr,
        meta.local_addr,
        name,
        age,
        idle,
        format_flags(meta),
        meta.db,
        multi,
        watch,
        qbuf_len,
        cmd,
        meta.username,
        library_name,
        library_ver,
    )
}

fn tx_queue_len(tx_state: &TxState) -> i32 {
    match tx_state {
        TxState::None => -1,
        TxState::Multi { queue, .. } => queue.len() as i32,
    }
}

fn format_flags(meta: &ConnectionMeta) -> String {
    let mut flags = String::new();
    if meta.flags.contains(ConnectionFlags::MULTI) {
        flags.push('x');
    }
    if meta.flags.contains(ConnectionFlags::BLOCKED) {
        flags.push('b');
    }
    if meta.flags.contains(ConnectionFlags::PUBSUB) {
        flags.push('P');
    }
    if meta.flags.contains(ConnectionFlags::TRACKING) {
        flags.push('T');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    flags
}

fn parse_on_off(args: &[Frame<'_>], command: &str) -> Result<bool, Vec<u8>> {
    let [mode] = args else {
        return Err(error_message(&format!(
            "ERR wrong number of arguments for '{}' command",
            command
        )));
    };
    let mode = frame_bytes(mode).map_err(|error| error_bytes(&error))?;
    if eq_ascii(mode, b"ON") {
        return Ok(true);
    }
    if eq_ascii(mode, b"OFF") {
        return Ok(false);
    }
    Err(error_message("ERR syntax error"))
}

fn validate_client_name(name: &[u8]) -> Result<(), Vec<u8>> {
    if name.iter().any(|byte| !matches!(*byte, 33..=126)) {
        return Err(error_message(
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ));
    }
    Ok(())
}

fn bulk_value(value: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value))))
}

pub(crate) fn ok_outcome(response: Vec<u8>) -> ClientCommandOutcome {
    ClientCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::{dispatch_client, format_client_line};
    use crate::{
        blocked::BlockedKeyRegistry,
        commands::connection::client_ops::{PauseState, TrackingRegistry},
        connection::{
            ClientConnectionMap, ConnectionFlags, ConnectionMeta, ConnectionState, ReplyMode,
        },
        transaction::{TxState, WatchRegistry, WatchState},
    };
    use compact_str::CompactString;
    use senko_proto::Frame;
    use smallvec::SmallVec;
    use std::{
        cell::RefCell,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        rc::Rc,
    };

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
            last_cmd: Some(CompactString::from("client")),
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
            tracking_prefixes: SmallVec::new(),
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
    fn setname_validates_ascii() {
        let blocked = Rc::new(RefCell::new(BlockedKeyRegistry::default()));
        let watch_registry = Rc::new(RefCell::new(WatchRegistry::default()));
        let watch_state = Rc::new(RefCell::new(WatchState::default()));
        let client_connections = Rc::new(RefCell::new(ClientConnectionMap::default()));
        let pause_state = Rc::new(RefCell::new(PauseState::default()));
        let tracking_registry = Rc::new(RefCell::new(TrackingRegistry::default()));
        let mut meta = meta();
        let mut state = ConnectionState::Reading;
        let mut tx = TxState::None;
        let err = dispatch_client(
            &[bs(b"SETNAME"), bs(b"bad name")],
            &mut meta,
            &client_connections,
            &mut state,
            &mut tx,
            &blocked,
            &watch_registry,
            &watch_state,
            0,
            &pause_state,
            &tracking_registry,
        )
        .unwrap_err();
        assert!(
            String::from_utf8(err)
                .unwrap()
                .contains("ERR Client names cannot contain spaces")
        );
    }

    #[test]
    fn info_line_contains_required_fields() {
        let line = format_client_line(&meta(), None, None, 0);
        assert!(line.contains("id=1 "));
        assert!(line.contains("addr=127.0.0.1:1234 "));
        assert!(line.contains("cmd=client "));
    }
}
