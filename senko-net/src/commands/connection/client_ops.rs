#![allow(clippy::await_holding_lock, clippy::too_many_arguments)]

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::atomic::Ordering,
    task::Waker,
    time::Instant,
};

use bytes::BytesMut;
use compact_str::CompactString;
use compio::{
    BufResult,
    io::{AsyncWrite, AsyncWriteExt},
    runtime::spawn,
};
use senko_proto::{Frame, RespSerializer};
use smallvec::SmallVec;

use crate::{
    blocked::{BlockedKeyRegistry, UnblockReason},
    commands::connection::client::{ClientCommandOutcome, ok_outcome},
    commands::transaction::clear_watch_state,
    connection::{
        ClientConnectionHandle, ClientConnectionMap, ConnectionFlags, ConnectionMeta,
        ConnectionState, error_bytes, error_message, frame_bytes, serialize_response,
        simple_string,
    },
    transaction::{TxState, WatchRegistry, WatchState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseMode {
    All,
    Write,
}

#[derive(Debug, Default)]
pub struct PauseState {
    pub paused_until: Option<Instant>,
    pub pause_mode: Option<PauseMode>,
    waiters: HashMap<u64, Waker>,
}

impl PauseState {
    pub fn set(&mut self, until: Option<Instant>, mode: PauseMode) {
        self.paused_until = until;
        self.pause_mode = Some(mode);
    }

    pub fn clear(&mut self) -> Vec<Waker> {
        self.paused_until = None;
        self.pause_mode = None;
        self.waiters.drain().map(|(_, waker)| waker).collect()
    }

    pub fn is_paused_for(&self, is_write: bool) -> bool {
        match self.pause_mode {
            Some(PauseMode::All) => self.paused_until.is_some(),
            Some(PauseMode::Write) => self.paused_until.is_some() && is_write,
            None => false,
        }
    }

    pub fn register(&mut self, conn_id: u64, waker: &Waker) {
        self.waiters.insert(conn_id, waker.clone());
    }

    pub fn check_expired(&mut self, now: Instant) -> bool {
        if self.paused_until.is_some_and(|until| until <= now) {
            for waker in self.clear() {
                waker.wake();
            }
            return true;
        }
        false
    }
}

#[derive(Debug, Default)]
pub struct TrackingRegistry {
    key_to_clients: HashMap<CompactString, SmallVec<[u64; 4]>>,
    prefix_to_clients: HashMap<CompactString, SmallVec<[u64; 4]>>,
}

impl TrackingRegistry {
    pub fn disable(&mut self, conn_id: u64) {
        self.key_to_clients.retain(|_, clients| {
            clients.retain(|candidate| *candidate != conn_id);
            !clients.is_empty()
        });
        self.prefix_to_clients.retain(|_, clients| {
            clients.retain(|candidate| *candidate != conn_id);
            !clients.is_empty()
        });
    }

    pub fn configure_bcast(&mut self, conn_id: u64, prefixes: &[CompactString]) {
        self.disable(conn_id);
        for prefix in prefixes {
            let clients = self.prefix_to_clients.entry(prefix.clone()).or_default();
            if !clients.contains(&conn_id) {
                clients.push(conn_id);
            }
        }
    }

    pub fn track_key(&mut self, conn_id: u64, key: CompactString) {
        let clients = self.key_to_clients.entry(key).or_default();
        if !clients.contains(&conn_id) {
            clients.push(conn_id);
        }
    }

    pub fn invalidate(
        &mut self,
        key: &CompactString,
        writer_conn_id: u64,
        client_connections: &Rc<RefCell<ClientConnectionMap>>,
    ) {
        if let Some(clients) = self.key_to_clients.remove(key) {
            for conn_id in clients {
                notify_invalidation(conn_id, key, writer_conn_id, client_connections);
            }
        }
        for (prefix, clients) in &self.prefix_to_clients {
            if key.as_str().starts_with(prefix.as_str()) {
                for conn_id in clients {
                    notify_invalidation(*conn_id, key, writer_conn_id, client_connections);
                }
            }
        }
    }
}

pub(crate) fn handle_ops(
    subcommand: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    pause_state: &Rc<RefCell<PauseState>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Option<Result<ClientCommandOutcome, Vec<u8>>> {
    if eq_ascii(subcommand, b"KILL") {
        return Some(client_kill(
            args,
            meta,
            client_connections,
            blocked,
            watch_registry,
            watch_state,
        ));
    }
    if eq_ascii(subcommand, b"PAUSE") {
        return Some(client_pause(args, pause_state));
    }
    if eq_ascii(subcommand, b"UNPAUSE") {
        return Some(client_unpause(args, pause_state));
    }
    if eq_ascii(subcommand, b"UNBLOCK") {
        return Some(client_unblock(args, client_connections, blocked));
    }
    if eq_ascii(subcommand, b"TRACKING") {
        return Some(client_tracking(
            args,
            meta,
            client_connections,
            tracking_registry,
        ));
    }
    let _ = state;
    let _ = tx_state;
    let _ = blocked;
    let _ = watch_registry;
    let _ = watch_state;
    None
}

pub(crate) fn maybe_track_read(
    command: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) {
    let caching = meta.tracking_caching.take();
    if !meta.flags.contains(ConnectionFlags::TRACKING) || meta.tracking_bcast {
        return;
    }
    if !is_trackable_read(command) {
        return;
    }
    let should_track = if meta.tracking_optin {
        caching == Some(true)
    } else if meta.tracking_optout {
        caching != Some(false)
    } else {
        true
    };
    if !should_track {
        return;
    }
    let Some(first) = args.first() else {
        return;
    };
    let Ok(key) = frame_bytes(first) else {
        return;
    };
    if let Ok(key) = CompactString::from_utf8(key) {
        tracking_registry.borrow_mut().track_key(meta.id, key);
    }
}

pub(crate) fn invalidate_written_keys(
    keys: &[CompactString],
    writer_conn_id: u64,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
) {
    let mut registry = tracking_registry.borrow_mut();
    for key in keys {
        registry.invalidate(key, writer_conn_id, client_connections);
    }
}

pub(crate) fn should_pause_command(command: &[u8], pause_state: &PauseState) -> bool {
    let is_write = is_write_command(command);
    pause_state.is_paused_for(is_write)
        && !eq_ascii(command, b"CLIENT")
        && !eq_ascii(command, b"QUIT")
}

fn client_kill(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    watch_state: &Rc<RefCell<WatchState>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if args.len() == 1 {
        let addr = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
        let Some(handle) = find_by_addr(client_connections, addr) else {
            return Err(error_message("ERR No such client"));
        };
        if let Some(target_id) = kill_handle(handle, meta.id, blocked, true)
            && target_id == meta.id
        {
            clear_watch_state(meta.id, watch_registry, watch_state);
            return Ok(ClientCommandOutcome {
                response: simple_string(b"OK"),
                close_after_write: true,
                suppress_response: false,
                force_send_response: false,
            });
        }
        return Ok(ok_outcome(simple_string(b"OK")));
    }

    let mut skipme = true;
    let mut ids = HashSet::<u64>::new();
    let mut addr = None;
    let mut laddr = None;
    let mut user = None;
    let mut r#type = None;
    let mut maxage = None;
    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        index += 1;
        if eq_ascii(token, b"ID") {
            if index >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            while index < args.len() {
                let value = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
                if is_filter_keyword(value) {
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
        if eq_ascii(token, b"TYPE") {
            r#type = Some(
                frame_bytes(&args[index])
                    .map_err(|error| error_bytes(&error))?
                    .to_vec(),
            );
            index += 1;
            continue;
        }
        if eq_ascii(token, b"USER") {
            user = Some(
                frame_bytes(&args[index])
                    .map_err(|error| error_bytes(&error))?
                    .to_vec(),
            );
            index += 1;
            continue;
        }
        if eq_ascii(token, b"ADDR") {
            addr = Some(
                frame_bytes(&args[index])
                    .map_err(|error| error_bytes(&error))?
                    .to_vec(),
            );
            index += 1;
            continue;
        }
        if eq_ascii(token, b"LADDR") {
            laddr = Some(
                frame_bytes(&args[index])
                    .map_err(|error| error_bytes(&error))?
                    .to_vec(),
            );
            index += 1;
            continue;
        }
        if eq_ascii(token, b"SKIPME") {
            let value = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            skipme = if eq_ascii(value, b"YES") {
                true
            } else if eq_ascii(value, b"NO") {
                false
            } else {
                return Err(error_message("ERR syntax error"));
            };
            index += 1;
            continue;
        }
        if eq_ascii(token, b"MAXAGE") {
            let value = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            maxage = std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.parse::<u64>().ok());
            if maxage.is_none() {
                return Err(error_message("ERR syntax error"));
            }
            index += 1;
            continue;
        }
        return Err(error_message("ERR syntax error"));
    }

    let snapshots: Vec<(u64, ClientConnectionHandle, ConnectionMeta)> = client_connections
        .borrow()
        .iter()
        .filter_map(|(id, handle)| {
            handle
                .meta
                .lock()
                .ok()
                .map(|meta| (*id, handle.clone(), meta.clone()))
        })
        .collect();
    let mut killed = 0i64;
    let now_ms = crate::connection::current_unix_ms();
    for (id, handle, snapshot) in snapshots {
        if skipme && id == meta.id {
            continue;
        }
        if !ids.is_empty() && !ids.contains(&id) {
            continue;
        }
        if let Some(addr) = &addr
            && snapshot.peer_addr.to_string().as_bytes() != addr.as_slice()
        {
            continue;
        }
        if let Some(laddr) = &laddr
            && snapshot.local_addr.to_string().as_bytes() != laddr.as_slice()
        {
            continue;
        }
        if let Some(user) = &user
            && snapshot.username.as_bytes() != user.as_slice()
        {
            continue;
        }
        if let Some(kind) = &r#type
            && !matches_type_filter(&snapshot, kind)
        {
            continue;
        }
        if let Some(maxage) = maxage
            && now_ms.saturating_sub(snapshot.created_at) / 1_000 < maxage
        {
            continue;
        }
        if kill_handle(handle, meta.id, blocked, false).is_some() {
            killed += 1;
        }
    }
    Ok(ok_outcome(serialize_response(
        &senko_store::Response::Integer(killed),
        meta.resp_version == 3,
    )))
}

fn client_pause(
    args: &[Frame<'_>],
    pause_state: &Rc<RefCell<PauseState>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if args.is_empty() || args.len() > 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|pause' command",
        ));
    }
    let timeout = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    let timeout = std::str::from_utf8(timeout)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| error_message("ERR timeout is not an integer or out of range"))?;
    if timeout == 0 {
        for waker in pause_state.borrow_mut().clear() {
            waker.wake();
        }
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    let mode = if args.len() == 1 {
        PauseMode::All
    } else {
        let mode = frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?;
        if eq_ascii(mode, b"WRITE") {
            PauseMode::Write
        } else if eq_ascii(mode, b"ALL") {
            PauseMode::All
        } else {
            return Err(error_message("ERR syntax error"));
        }
    };
    pause_state.borrow_mut().set(
        Some(Instant::now() + std::time::Duration::from_millis(timeout)),
        mode,
    );
    Ok(ok_outcome(simple_string(b"OK")))
}

fn client_unpause(
    args: &[Frame<'_>],
    pause_state: &Rc<RefCell<PauseState>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if !args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|unpause' command",
        ));
    }
    for waker in pause_state.borrow_mut().clear() {
        waker.wake();
    }
    Ok(ok_outcome(simple_string(b"OK")))
}

fn client_unblock(
    args: &[Frame<'_>],
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if args.is_empty() || args.len() > 2 {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|unblock' command",
        ));
    }
    let client_id = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    let client_id = std::str::from_utf8(client_id)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))?;
    let reason = if args.len() == 2 {
        let flag = frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?;
        if eq_ascii(flag, b"ERROR") {
            UnblockReason::Error
        } else if eq_ascii(flag, b"TIMEOUT") {
            UnblockReason::Timeout
        } else {
            return Err(error_message("ERR syntax error"));
        }
    } else {
        UnblockReason::Timeout
    };
    if !client_connections.borrow().contains_key(&client_id) {
        return Ok(ok_outcome(serialize_response(
            &senko_store::Response::Integer(0),
            false,
        )));
    }
    let unblocked = blocked.borrow_mut().force_unblock(client_id, reason);
    Ok(ok_outcome(serialize_response(
        &senko_store::Response::Integer(unblocked as i64),
        false,
    )))
}

fn client_tracking(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<ClientCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'client|tracking' command",
        ));
    }
    let mode = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
    if eq_ascii(mode, b"OFF") {
        meta.flags.remove(ConnectionFlags::TRACKING);
        meta.tracking_optin = false;
        meta.tracking_optout = false;
        meta.tracking_bcast = false;
        meta.tracking_noloop = false;
        meta.tracking_redirect = -1;
        meta.tracking_prefixes.clear();
        meta.tracking_caching = None;
        tracking_registry.borrow_mut().disable(meta.id);
        return Ok(ok_outcome(simple_string(b"OK")));
    }
    if !eq_ascii(mode, b"ON") {
        return Err(error_message("ERR syntax error"));
    }
    let mut redirect = -1i64;
    let mut bcast = false;
    let mut optin = false;
    let mut optout = false;
    let mut noloop = false;
    let mut prefixes = SmallVec::<[CompactString; 4]>::new();
    let mut index = 1usize;
    while index < args.len() {
        let token = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        index += 1;
        if eq_ascii(token, b"REDIRECT") {
            let id = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            redirect = std::str::from_utf8(id)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                .ok_or_else(|| error_message("ERR syntax error"))?;
            if !client_connections.borrow().contains_key(&(redirect as u64)) {
                return Err(error_message(
                    "ERR The client ID you want redirect tracking notifications to does not exist",
                ));
            }
            index += 1;
            continue;
        }
        if eq_ascii(token, b"PREFIX") {
            let prefix = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            prefixes.push(
                CompactString::from_utf8(prefix).map_err(|_| error_message("ERR syntax error"))?,
            );
            index += 1;
            continue;
        }
        if eq_ascii(token, b"BCAST") {
            bcast = true;
            continue;
        }
        if eq_ascii(token, b"OPTIN") {
            optin = true;
            continue;
        }
        if eq_ascii(token, b"OPTOUT") {
            optout = true;
            continue;
        }
        if eq_ascii(token, b"NOLOOP") {
            noloop = true;
            continue;
        }
        return Err(error_message("ERR syntax error"));
    }
    if bcast && (optin || optout) {
        return Err(error_message(
            "ERR You can't use BCAST and OPTIN/OPTOUT at the same time",
        ));
    }
    meta.flags.insert(ConnectionFlags::TRACKING);
    meta.tracking_redirect = redirect;
    meta.tracking_optin = optin;
    meta.tracking_optout = optout;
    meta.tracking_bcast = bcast;
    meta.tracking_noloop = noloop;
    meta.tracking_prefixes = prefixes.clone();
    meta.tracking_caching = None;
    tracking_registry.borrow_mut().disable(meta.id);
    if bcast {
        tracking_registry
            .borrow_mut()
            .configure_bcast(meta.id, &prefixes);
    }
    Ok(ok_outcome(simple_string(b"OK")))
}

fn kill_handle(
    handle: ClientConnectionHandle,
    issuer_id: u64,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    legacy: bool,
) -> Option<u64> {
    let snapshot = handle.meta.lock().ok()?.clone();
    if snapshot.flags.contains(ConnectionFlags::BLOCKED) {
        let _ = blocked
            .borrow_mut()
            .force_unblock(snapshot.id, UnblockReason::Timeout);
    }
    handle.close_after_write.store(true, Ordering::SeqCst);
    if snapshot.id != issuer_id || legacy {
        let writer = handle.writer.clone();
        spawn(async move {
            let mut writer = writer.lock().expect("writer poisoned");
            let _ = (*writer).shutdown().await;
        })
        .detach();
    }
    Some(snapshot.id)
}

fn find_by_addr(
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    addr: &[u8],
) -> Option<ClientConnectionHandle> {
    client_connections.borrow().values().find_map(|handle| {
        let snapshot = handle.meta.lock().ok()?.clone();
        (snapshot.peer_addr.to_string().as_bytes() == addr).then_some(handle.clone())
    })
}

fn notify_invalidation(
    conn_id: u64,
    key: &CompactString,
    writer_conn_id: u64,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
) {
    let Some(handle) = client_connections.borrow().get(&conn_id).cloned() else {
        return;
    };
    let Ok(snapshot) = handle.meta.lock().map(|meta| meta.clone()) else {
        return;
    };
    if snapshot.tracking_noloop && conn_id == writer_conn_id {
        return;
    }
    let target = if snapshot.tracking_redirect >= 0 {
        client_connections
            .borrow()
            .get(&(snapshot.tracking_redirect as u64))
            .cloned()
            .unwrap_or(handle)
    } else {
        handle
    };
    let payload = invalidation_payload(&snapshot, key.as_bytes());
    let writer = target.writer.clone();
    spawn(async move {
        let writer = writer.lock().expect("writer poisoned");
        let BufResult(result, _) = (&*writer).write_all(payload).await;
        let _ = result;
    })
    .detach();
}

fn invalidation_payload(meta: &ConnectionMeta, key: &[u8]) -> Vec<u8> {
    if meta.resp_version == 3 {
        let mut out = BytesMut::new();
        out.extend_from_slice(b">2\r\n+invalidate\r\n*1\r\n");
        RespSerializer::write_bulk_string(&mut out, key);
        out.to_vec()
    } else {
        let channel = b"__redis__:invalidate";
        let mut out = BytesMut::new();
        RespSerializer::write_array_header(&mut out, 3);
        RespSerializer::write_bulk_string(&mut out, b"message");
        RespSerializer::write_bulk_string(&mut out, channel);
        RespSerializer::write_bulk_string(&mut out, key);
        out.to_vec()
    }
}

fn is_trackable_read(command: &[u8]) -> bool {
    eq_ascii(command, b"GET")
        || eq_ascii(command, b"HGET")
        || eq_ascii(command, b"ZSCORE")
        || eq_ascii(command, b"LINDEX")
        || eq_ascii(command, b"LRANGE")
        || eq_ascii(command, b"MGET")
}

fn is_write_command(command: &[u8]) -> bool {
    !eq_ascii(command, b"GET")
        && !eq_ascii(command, b"HGET")
        && !eq_ascii(command, b"ZSCORE")
        && !eq_ascii(command, b"LINDEX")
        && !eq_ascii(command, b"LRANGE")
        && !eq_ascii(command, b"MGET")
        && !eq_ascii(command, b"PING")
        && !eq_ascii(command, b"ECHO")
        && !eq_ascii(command, b"CLIENT")
        && !eq_ascii(command, b"HELLO")
        && !eq_ascii(command, b"AUTH")
        && !eq_ascii(command, b"SELECT")
}

fn is_filter_keyword(token: &[u8]) -> bool {
    eq_ascii(token, b"ID")
        || eq_ascii(token, b"TYPE")
        || eq_ascii(token, b"USER")
        || eq_ascii(token, b"ADDR")
        || eq_ascii(token, b"LADDR")
        || eq_ascii(token, b"SKIPME")
        || eq_ascii(token, b"MAXAGE")
}

fn matches_type_filter(meta: &ConnectionMeta, filter: &[u8]) -> bool {
    if eq_ascii(filter, b"PUBSUB") {
        return meta.flags.contains(ConnectionFlags::PUBSUB);
    }
    if eq_ascii(filter, b"NORMAL") {
        return !meta.flags.contains(ConnectionFlags::PUBSUB)
            && !meta.flags.contains(ConnectionFlags::REPLICA);
    }
    if eq_ascii(filter, b"MASTER") || eq_ascii(filter, b"SLAVE") || eq_ascii(filter, b"REPLICA") {
        return false;
    }
    false
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}
