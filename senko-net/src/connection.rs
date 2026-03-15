#![allow(
    clippy::arc_with_non_send_sync,
    clippy::await_holding_lock,
    clippy::too_many_arguments
)]

use std::{
    cell::RefCell,
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ahash::RandomState;
use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use compio::{
    BufResult,
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};
use futures_util::future::poll_fn;
use futures_util::{FutureExt, pin_mut};
use hashbrown::{HashMap, HashSet};
use senko_core::{SenkoConfig, SenkoError, SenkoResult, SenkoValue, ShardExtensions};
use senko_proto::{AggregateKind, Frame, ParseStatus, RespParser, RespSerializer};
use senko_scripting::{
    LuaEngine, LuaError, RespValue as ScriptRespValue, ScriptContext, ScriptDebugMode,
    ScriptExecutionHooks,
};
use senko_store::{
    Response, Store,
    commands::generic::keys as generic_keys,
    commands::list::blocking::{
        BlockSpec, BlockingCommandResult, BlockingOp as StoreBlockingOp, BlockingResponseKind,
        Direction as BlockSpecDirection, blmove, blmpop, blpop, brpop, brpoplpush,
    },
    commands::stream::read::{
        BlockingCommandResult as StreamBlockingCommandResult,
        GroupBlockingCommandResult as StreamGroupBlockingCommandResult, XReadBlockSpec,
        XReadGroupBlockSpec, xread, xreadgroup,
    },
    commands::zset::blocking::{
        BlockSpec as ZBlockSpec, BlockingCommandResult as ZBlockingCommandResult,
        BlockingOp as StoreZBlockingOp, bzmpop, bzpopmax, bzpopmin,
    },
    commands::zset::pop::ZPopDir as ZBlockSpecDirection,
};
use tracing::debug;

use crate::{
    acl::{self, AclContext},
    blocked::{BlockedClient, BlockedKeyRegistry, BlockedOp},
    commands::cluster::{self, ClusterCommandState},
    commands::connection::basic as connection_basic,
    commands::connection::client as connection_client,
    commands::connection::client_ops::{self, PauseState, TrackingRegistry},
    commands::pubsub::{self, PubSubState},
    commands::server::command_info,
    commands::server::config as live_config,
    commands::server::diagnostics as server_diagnostics,
    commands::server::info as server_info,
    commands::server::persistence as server_persistence,
    commands::server::replication as server_replication,
    commands::transaction::{
        TransactionCommandResult, clear_watch_state, handle_transaction_command,
        queue_transaction_command, queued_command_response, queued_frames_as_refs,
        serialize_exec_array, should_execute_immediately_in_multi,
    },
    dispatch,
    pubsub::fanout::ShardFanOut,
    transaction::{ConnectionMap, TxState, WatchRegistry, WatchState},
};

const READ_CHUNK_SIZE: usize = 16 * 1024;
const RESP_PARSER: RespParser = RespParser::new();

pub type ClientMetaHandle = Arc<Mutex<ConnectionMeta>>;

#[derive(Clone)]
pub struct ClientConnectionHandle {
    pub meta: ClientMetaHandle,
    pub writer: Arc<Mutex<TcpStream>>,
    pub close_after_write: Arc<AtomicBool>,
}

pub type ClientConnectionMap = hashbrown::HashMap<u64, ClientConnectionHandle, ahash::RandomState>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    Reading,
    Parsing,
    Dispatching,
    Writing,
    Closing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    Normal,
    Off,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionFlags(u32);

impl ConnectionFlags {
    pub const AUTHENTICATED: Self = Self(1 << 0);
    pub const MULTI: Self = Self(1 << 1);
    pub const BLOCKED: Self = Self(1 << 2);
    pub const TRACKING: Self = Self(1 << 3);
    pub const PUBSUB: Self = Self(1 << 4);
    pub const REPLICA: Self = Self(1 << 5);
    pub const MONITOR: Self = Self(1 << 6);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionMeta {
    pub id: u64,
    pub username: CompactString,
    pub name: Option<CompactString>,
    pub db: u8,
    pub flags: ConnectionFlags,
    pub created_at: u64,
    pub last_cmd: Option<CompactString>,
    pub last_cmd_at: u64,
    pub lib_name: Option<CompactString>,
    pub lib_ver: Option<CompactString>,
    pub peer_addr: SocketAddr,
    pub local_addr: SocketAddr,
    pub resp_version: u8,
    pub no_evict: bool,
    pub no_touch: bool,
    pub reply_mode: ReplyMode,
    pub watch_count: u32,
    pub multi_queue_len: i32,
    pub tracking_redirect: i64,
    pub tracking_optin: bool,
    pub tracking_optout: bool,
    pub tracking_bcast: bool,
    pub tracking_noloop: bool,
    pub tracking_prefixes: smallvec::SmallVec<[CompactString; 4]>,
    pub tracking_caching: Option<bool>,
    pub replica_listening_port: Option<u16>,
    pub replica_ip_address: Option<CompactString>,
    pub replica_psync2: bool,
    pub replica_eof: bool,
    pub replica_ack_offset: u64,
    pub last_write_replication_offset: u64,
}

pub struct Connection {
    shard_id: usize,
    stream: TcpStream,
    store: Rc<RefCell<Store>>,
    engine: Rc<RefCell<LuaEngine>>,
    shard_extensions: Arc<ShardExtensions>,
    blocked: Rc<RefCell<BlockedKeyRegistry>>,
    cluster: Rc<RefCell<ClusterCommandState>>,
    watch_registry: Rc<RefCell<WatchRegistry>>,
    connections: Rc<RefCell<ConnectionMap>>,
    client_connections: Rc<RefCell<ClientConnectionMap>>,
    pause_state: Rc<RefCell<PauseState>>,
    tracking_registry: Rc<RefCell<TrackingRegistry>>,
    shard_pubsub: Rc<RefCell<ShardFanOut>>,
    parse_buffer: BytesMut,
    phase: ConnectionPhase,
    pub(crate) meta: ConnectionMeta,
    shared_meta: ClientMetaHandle,
    shared_writer: Arc<Mutex<TcpStream>>,
    shared_close_after_write: Arc<AtomicBool>,
    state: ConnectionState,
    tx_state: TxState,
    watch_state: Rc<RefCell<WatchState>>,
    pubsub: Option<PubSubState>,
    monitor: Option<command_info::MonitorSubscription>,
    last_time_us: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ConnectionState {
    Reading,
    Blocked {
        keys: smallvec::SmallVec<[CompactString; 4]>,
        deadline: Option<Instant>,
        pending_response: Option<Response>,
    },
}

impl Connection {
    pub fn new(
        shard_id: usize,
        conn_id: u64,
        stream: TcpStream,
        peer_addr: SocketAddr,
        local_addr: SocketAddr,
        store: Rc<RefCell<Store>>,
        engine: Rc<RefCell<LuaEngine>>,
        shard_extensions: Arc<ShardExtensions>,
        blocked: Rc<RefCell<BlockedKeyRegistry>>,
        cluster: Rc<RefCell<ClusterCommandState>>,
        watch_registry: Rc<RefCell<WatchRegistry>>,
        connections: Rc<RefCell<ConnectionMap>>,
        client_connections: Rc<RefCell<ClientConnectionMap>>,
        pause_state: Rc<RefCell<PauseState>>,
        tracking_registry: Rc<RefCell<TrackingRegistry>>,
        shard_pubsub: Rc<RefCell<ShardFanOut>>,
        _config: &SenkoConfig,
    ) -> Self {
        let watch_state = Rc::new(RefCell::new(WatchState::default()));
        connections
            .borrow_mut()
            .insert(conn_id, Rc::clone(&watch_state));
        let mut flags = ConnectionFlags::empty();
        if crate::acl::connection_starts_authenticated() {
            flags.insert(ConnectionFlags::AUTHENTICATED);
        }
        let meta = ConnectionMeta {
            id: conn_id,
            username: crate::acl::default_username(),
            name: None,
            db: 0,
            flags,
            created_at: current_unix_ms(),
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr,
            local_addr,
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
        };
        let shared_meta = Arc::new(Mutex::new(meta.clone()));
        let shared_writer = Arc::new(Mutex::new(stream.clone()));
        let shared_close_after_write = Arc::new(AtomicBool::new(false));
        client_connections.borrow_mut().insert(
            conn_id,
            ClientConnectionHandle {
                meta: Arc::clone(&shared_meta),
                writer: Arc::clone(&shared_writer),
                close_after_write: Arc::clone(&shared_close_after_write),
            },
        );
        Self {
            shard_id,
            stream,
            store,
            engine,
            shard_extensions,
            blocked,
            cluster,
            watch_registry,
            connections,
            client_connections,
            pause_state,
            tracking_registry,
            shard_pubsub,
            parse_buffer: BytesMut::with_capacity(READ_CHUNK_SIZE),
            phase: ConnectionPhase::Reading,
            meta,
            shared_meta,
            shared_writer,
            shared_close_after_write,
            state: ConnectionState::Reading,
            tx_state: TxState::None,
            watch_state,
            pubsub: None,
            monitor: None,
            last_time_us: current_unix_us(),
        }
    }

    pub async fn run(mut self, config: &SenkoConfig) -> SenkoResult<()> {
        debug!(
            shard = self.shard_id,
            conn_id = self.meta.id,
            peer_addr = %self.meta.peer_addr,
            local_addr = %self.meta.local_addr,
            "connection started"
        );
        loop {
            if self.flush_pubsub_messages().await? {
                self.cleanup_connection_state();
                self.phase = ConnectionPhase::Closing;
                self.stream.shutdown().await?;
                return Ok(());
            }
            if self.flush_monitor_messages().await? {
                self.cleanup_connection_state();
                self.phase = ConnectionPhase::Closing;
                self.stream.shutdown().await?;
                return Ok(());
            }

            let mut offset = 0usize;
            let mut outbound = Vec::new();
            let mut should_close = false;

            loop {
                self.phase = ConnectionPhase::Parsing;
                match RESP_PARSER.parse(&self.parse_buffer[offset..])? {
                    ParseStatus::Complete(frame, used) => {
                        offset += used;
                        self.phase = ConnectionPhase::Dispatching;
                        let pre_reply_mode = self.meta.reply_mode;
                        let diagnostics = diagnostics_command(frame);
                        let started = Instant::now();
                        let outcome = handle_frame(
                            frame,
                            &mut self.meta,
                            &self.store,
                            &self.engine,
                            &self.shard_extensions,
                            &self.blocked,
                            &self.cluster,
                            &self.watch_registry,
                            &self.connections,
                            &self.client_connections,
                            &self.pause_state,
                            &self.tracking_registry,
                            &self.shard_pubsub,
                            &mut self.pubsub,
                            &mut self.monitor,
                            config,
                            &mut self.state,
                            &mut self.tx_state,
                            &self.watch_state,
                            self.parse_buffer.len(),
                            self.shard_id,
                            &mut self.last_time_us,
                        )
                        .await;
                        if let Some((command, args)) = diagnostics {
                            let client_name = self.meta.name.as_ref().map(CompactString::as_str);
                            server_diagnostics::record_command(
                                self.shard_id,
                                &command,
                                &args,
                                &self.meta.peer_addr.to_string(),
                                client_name,
                                started.elapsed(),
                            );
                        }
                        self.meta.watch_count = self.watch_state.borrow().watched_keys.len() as u32;
                        self.sync_shared_meta();
                        if should_write_response(
                            &mut self.meta,
                            pre_reply_mode,
                            outcome.force_send_response,
                            outcome.suppress_response,
                        ) {
                            server_info::record_error_response(&outcome.response);
                            outbound.push(outcome.response);
                        }
                        should_close |= outcome.close_after_write;
                    }
                    ParseStatus::Incomplete(_) => break,
                }
            }

            if offset > 0 {
                let _ = self.parse_buffer.split_to(offset);

                self.phase = ConnectionPhase::Writing;
                if !outbound.is_empty() {
                    let bytes_written = outbound.iter().map(Vec::len).sum::<usize>();
                    server_info::on_write(self.shard_id, bytes_written, bytes_written);
                    let writer = self.shared_writer.lock().expect("writer poisoned");
                    let BufResult(result, _) = (&*writer).write_vectored_all(outbound).await;
                    result?;
                }

                if should_close || self.shared_close_after_write.swap(false, Ordering::SeqCst) {
                    self.cleanup_connection_state();
                    self.phase = ConnectionPhase::Closing;
                    self.stream.shutdown().await?;
                    debug!(
                        shard = self.shard_id,
                        conn_id = self.meta.id,
                        peer_addr = %self.meta.peer_addr,
                        "connection closed after write"
                    );
                    return Ok(());
                }
            }

            self.phase = ConnectionPhase::Reading;
            let read_len = match self.wait_for_activity().await? {
                Activity::Socket(read) => {
                    if read.is_empty() {
                        0
                    } else {
                        self.parse_buffer.extend_from_slice(&read);
                        server_info::on_read(self.shard_id, read.len(), self.parse_buffer.len());
                        read.len()
                    }
                }
                Activity::PubSub => continue,
                Activity::Monitor => continue,
            };
            if read_len == 0 {
                self.cleanup_connection_state();
                self.phase = ConnectionPhase::Closing;
                self.stream.shutdown().await?;
                debug!(
                    shard = self.shard_id,
                    conn_id = self.meta.id,
                    peer_addr = %self.meta.peer_addr,
                    "connection closed by peer"
                );
                return Ok(());
            }
        }
    }

    fn cleanup_connection_state(&mut self) {
        server_info::on_connection_close(self.shard_id);
        pubsub::cleanup_pubsub_state(
            self.meta.id,
            &mut self.pubsub,
            &self.shard_pubsub,
            &self.cluster,
        );
        self.meta.flags.remove(ConnectionFlags::PUBSUB);
        if let Some(subscription) = self.monitor.take() {
            command_info::unsubscribe_monitor(&subscription);
        }
        self.meta.flags.remove(ConnectionFlags::MONITOR);
        self.blocked.borrow_mut().remove_client(self.meta.id);
        if self.meta.flags.contains(ConnectionFlags::REPLICA) {
            server_replication::on_disconnect(self.shard_id, self.meta.id);
        }
        self.watch_registry.borrow_mut().cleanup_conn(self.meta.id);
        self.connections.borrow_mut().remove(&self.meta.id);
        self.client_connections.borrow_mut().remove(&self.meta.id);
    }

    fn sync_shared_meta(&self) {
        if let Ok(mut shared) = self.shared_meta.lock() {
            *shared = self.meta.clone();
        }
    }

    async fn wait_for_activity(&mut self) -> SenkoResult<Activity> {
        if self.pubsub.is_none() && self.monitor.is_none() {
            let BufResult(result, read) =
                self.stream.read(Vec::with_capacity(READ_CHUNK_SIZE)).await;
            result?;
            return Ok(Activity::Socket(read));
        }

        let stream = &mut self.stream;
        if let Some(state) = self.pubsub.as_ref() {
            let read_fut = stream
                .read(Vec::with_capacity(READ_CHUNK_SIZE))
                .map(|BufResult(result, read)| result.map(|_| Activity::Socket(read)));
            let pubsub_fut = poll_fn(|cx| state.poll_ready(cx)).map(|_| Ok(Activity::PubSub));
            pin_mut!(read_fut, pubsub_fut);
            return match futures_util::future::select(read_fut, pubsub_fut).await {
                futures_util::future::Either::Left((result, _)) => Ok(result?),
                futures_util::future::Either::Right((result, _)) => result,
            };
        }
        let read_fut = stream
            .read(Vec::with_capacity(READ_CHUNK_SIZE))
            .map(|BufResult(result, read)| result.map(|_| Activity::Socket(read)));
        let monitor_fut = sleep(Duration::from_millis(10)).map(|_| Ok(Activity::Monitor));
        pin_mut!(read_fut, monitor_fut);
        match futures_util::future::select(read_fut, monitor_fut).await {
            futures_util::future::Either::Left((result, _)) => Ok(result?),
            futures_util::future::Either::Right((result, _)) => result,
        }
    }

    async fn flush_pubsub_messages(&mut self) -> SenkoResult<bool> {
        let Some(state) = self.pubsub.as_ref() else {
            return Ok(false);
        };
        if !state.take_drain_needed() && !state.has_pending_messages() && !state.is_lagged() {
            return Ok(false);
        }
        if state.is_lagged() {
            let payload = pubsub::lagged_disconnect_frame();
            server_info::record_error_response(&payload);
            server_info::on_write(self.shard_id, payload.len(), payload.len());
            let writer = self.shared_writer.lock().expect("writer poisoned");
            let BufResult(result, _) = (&*writer).write_all(payload).await;
            result?;
            return Ok(true);
        }
        let outbound = pubsub::drain_pubsub_messages(state, self.meta.resp_version == 3);
        if outbound.is_empty() {
            return Ok(false);
        }
        let bytes_written = outbound.iter().map(Vec::len).sum::<usize>();
        server_info::on_write(self.shard_id, bytes_written, bytes_written);
        let writer = self.shared_writer.lock().expect("writer poisoned");
        let BufResult(result, _) = (&*writer).write_vectored_all(outbound).await;
        result?;
        Ok(false)
    }

    async fn flush_monitor_messages(&mut self) -> SenkoResult<bool> {
        let Some(subscription) = self.monitor.as_ref() else {
            return Ok(false);
        };
        let outbound = command_info::drain_monitor_messages(subscription);
        if outbound.is_empty() {
            return Ok(false);
        }
        let bytes_written = outbound.iter().map(Vec::len).sum::<usize>();
        server_info::on_write(self.shard_id, bytes_written, bytes_written);
        let writer = self.shared_writer.lock().expect("writer poisoned");
        let BufResult(result, _) = (&*writer).write_vectored_all(outbound).await;
        result?;
        Ok(false)
    }
}

impl ConnectionMeta {
    pub fn for_acl_dryrun(username: CompactString) -> Self {
        Self {
            id: 0,
            username,
            name: None,
            db: 0,
            flags: ConnectionFlags::AUTHENTICATED,
            created_at: 0,
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            local_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
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
}

enum Activity {
    Socket(Vec<u8>),
    PubSub,
    Monitor,
}

struct CommandOutcome {
    response: Vec<u8>,
    close_after_write: bool,
    suppress_response: bool,
    force_send_response: bool,
}

async fn handle_frame(
    frame: Frame<'_>,
    meta: &mut ConnectionMeta,
    store: &Rc<RefCell<Store>>,
    engine: &Rc<RefCell<LuaEngine>>,
    shard_extensions: &Arc<ShardExtensions>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    pause_state: &Rc<RefCell<PauseState>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    pubsub: &mut Option<PubSubState>,
    monitor: &mut Option<command_info::MonitorSubscription>,
    config: &SenkoConfig,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    watch_state: &Rc<RefCell<WatchState>>,
    qbuf_len: usize,
    shard_id: usize,
    last_time_us: &mut u64,
) -> CommandOutcome {
    match dispatch_frame(
        frame,
        meta,
        store,
        engine,
        shard_extensions,
        blocked,
        cluster,
        watch_registry,
        connections,
        client_connections,
        pause_state,
        tracking_registry,
        shard_pubsub,
        pubsub,
        monitor,
        config,
        state,
        tx_state,
        watch_state,
        qbuf_len,
        shard_id,
        last_time_us,
    )
    .await
    {
        Ok((response, close_after_write, suppress_response, force_send_response)) => {
            CommandOutcome {
                response,
                close_after_write,
                suppress_response,
                force_send_response,
            }
        }
        Err(ConnectionControl::Continue(response)) => CommandOutcome {
            response,
            close_after_write: false,
            suppress_response: false,
            force_send_response: false,
        },
    }
}

enum ConnectionControl {
    Continue(Vec<u8>),
}

async fn dispatch_frame(
    frame: Frame<'_>,
    meta: &mut ConnectionMeta,
    store: &Rc<RefCell<Store>>,
    engine: &Rc<RefCell<LuaEngine>>,
    shard_extensions: &Arc<ShardExtensions>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    pause_state: &Rc<RefCell<PauseState>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    pubsub: &mut Option<PubSubState>,
    monitor: &mut Option<command_info::MonitorSubscription>,
    config: &SenkoConfig,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    watch_state: &Rc<RefCell<WatchState>>,
    qbuf_len: usize,
    shard_id: usize,
    last_time_us: &mut u64,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let Frame::Array(aggregate) = frame else {
        return Err(ConnectionControl::Continue(error_bytes(
            &SenkoError::Protocol("expected array frame"),
        )));
    };
    if aggregate.kind() != AggregateKind::Array {
        return Err(ConnectionControl::Continue(error_bytes(
            &SenkoError::Protocol("expected command array"),
        )));
    }

    let mut frames = Vec::with_capacity(aggregate.len());
    for item in aggregate.iter() {
        match item {
            Ok(frame) => frames.push(frame),
            Err(error) => return Err(ConnectionControl::Continue(error_bytes(&error))),
        }
    }
    if frames.is_empty() {
        return Err(ConnectionControl::Continue(error_message(
            "ERR unknown command ''",
        )));
    }

    let command = match command_name(&frames[0]) {
        Ok(command) => command,
        Err(error) => return Err(ConnectionControl::Continue(error_bytes(&error))),
    };
    let args = &frames[1..];
    server_info::on_command_processed(shard_id);
    meta.last_cmd = CompactString::from_utf8(command).ok();
    meta.last_cmd_at = current_unix_ms();
    command_info::publish_monitor(shard_id, meta, command, args);

    if !meta.flags.contains(ConnectionFlags::AUTHENTICATED)
        && !connection_basic::allows_unauthenticated(command)
    {
        return Err(ConnectionControl::Continue(error_message(
            "NOAUTH Authentication required.",
        )));
    }

    if meta.flags.contains(ConnectionFlags::AUTHENTICATED)
        && !eq_ascii(command, b"AUTH")
        && !eq_ascii(command, b"HELLO")
    {
        acl::check_permissions(
            meta,
            command,
            args,
            if matches!(tx_state, TxState::Multi { .. }) {
                AclContext::Multi
            } else {
                AclContext::Toplevel
            },
            qbuf_len,
        )
        .map_err(ConnectionControl::Continue)?;
    }

    match handle_transaction_command(
        meta.id,
        command,
        &frames,
        store,
        watch_registry,
        tx_state,
        watch_state,
    )
    .map_err(ConnectionControl::Continue)?
    {
        TransactionCommandResult::Respond(response) => {
            sync_meta_flags(meta, tx_state, state);
            return Ok((response, false, false, false));
        }
        TransactionCommandResult::Exec { queue } => {
            let mut responses = Vec::with_capacity(queue.len());
            for queued_command in queue {
                if let Some(response) =
                    queued_command_response(meta.id, &queued_command, watch_registry, watch_state)
                {
                    responses.push(response);
                    continue;
                }
                let queued_frames = queued_frames_as_refs(&queued_command.frames);
                match execute_immediate_command(
                    meta,
                    queued_command.frames[0].as_ref(),
                    &queued_frames[1..],
                    store,
                    engine,
                    shard_extensions,
                    blocked,
                    cluster,
                    watch_registry,
                    connections,
                    client_connections,
                    pause_state,
                    tracking_registry,
                    shard_pubsub,
                    pubsub,
                    monitor,
                    config,
                    state,
                    tx_state,
                    watch_state,
                    qbuf_len,
                    shard_id,
                    last_time_us,
                )
                .await
                {
                    Ok((response, _close_after_write, _suppress, _force)) => {
                        responses.push(response);
                    }
                    Err(ConnectionControl::Continue(error)) => {
                        responses.push(error);
                    }
                }
            }
            clear_watch_state(meta.id, watch_registry, watch_state);
            sync_meta_flags(meta, tx_state, state);
            return Ok((serialize_exec_array(&responses), false, false, false));
        }
        TransactionCommandResult::NotHandled => {}
    }

    if let TxState::Multi { .. } = tx_state {
        if should_execute_immediately_in_multi(command) {
            return execute_immediate_command(
                meta,
                command,
                args,
                store,
                engine,
                shard_extensions,
                blocked,
                cluster,
                watch_registry,
                connections,
                client_connections,
                pause_state,
                tracking_registry,
                shard_pubsub,
                pubsub,
                monitor,
                config,
                state,
                tx_state,
                watch_state,
                qbuf_len,
                shard_id,
                last_time_us,
            )
            .await;
        }
        return queue_transaction_command(command, &frames, tx_state)
            .map(|response| {
                sync_meta_flags(meta, tx_state, state);
                (response, false, false, false)
            })
            .map_err(ConnectionControl::Continue);
    }

    execute_immediate_command(
        meta,
        command,
        args,
        store,
        engine,
        shard_extensions,
        blocked,
        cluster,
        watch_registry,
        connections,
        client_connections,
        pause_state,
        tracking_registry,
        shard_pubsub,
        pubsub,
        monitor,
        config,
        state,
        tx_state,
        watch_state,
        qbuf_len,
        shard_id,
        last_time_us,
    )
    .await
}

async fn execute_immediate_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    engine: &Rc<RefCell<LuaEngine>>,
    shard_extensions: &Arc<ShardExtensions>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    pause_state: &Rc<RefCell<PauseState>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    pubsub: &mut Option<PubSubState>,
    monitor: &mut Option<command_info::MonitorSubscription>,
    config: &SenkoConfig,
    state: &mut ConnectionState,
    tx_state: &mut TxState,
    watch_state: &Rc<RefCell<WatchState>>,
    qbuf_len: usize,
    shard_id: usize,
    last_time_us: &mut u64,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if meta.flags.contains(ConnectionFlags::PUBSUB)
        && !pubsub::is_pubsub_context_command_allowed(command)
    {
        return Err(ConnectionControl::Continue(pubsub::pubsub_context_error()));
    }
    if meta.flags.contains(ConnectionFlags::MONITOR)
        && !command_info::monitor_allows_command(command)
    {
        return Err(ConnectionControl::Continue(error_message(
            "ERR only RESET and QUIT are allowed in MONITOR mode",
        )));
    }

    if let Some(result) = connection_basic::execute(
        command,
        args,
        config,
        meta,
        state,
        tx_state,
        blocked,
        watch_registry,
        watch_state,
    ) {
        let outcome = result
            .map(|outcome| {
                if eq_ascii(command, b"RESET") {
                    pubsub::cleanup_pubsub_state(meta.id, pubsub, shard_pubsub, cluster);
                    meta.flags.remove(ConnectionFlags::PUBSUB);
                    if let Some(subscription) = monitor.take() {
                        command_info::unsubscribe_monitor(&subscription);
                    }
                    meta.flags.remove(ConnectionFlags::MONITOR);
                }
                sync_meta_flags(meta, tx_state, state);
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue)?;
        return Ok(outcome);
    }

    if let Some(result) = live_config::execute(command, args, meta.resp_version == 3) {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = command_info::execute(
        command,
        args,
        meta.resp_version == 3,
        meta,
        shard_id,
        monitor,
    ) {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = acl::execute(command, args, meta, client_connections, blocked, config) {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = dispatch_scripting_command(
        meta,
        shard_id,
        command,
        args,
        store,
        engine,
        blocked,
        watch_registry,
        connections,
        client_connections,
        tracking_registry,
        config,
    )
    .await
    {
        return result.map_err(ConnectionControl::Continue);
    }

    if let Some(result) = server_info::execute(
        command,
        args,
        meta.resp_version == 3,
        cluster,
        config,
        last_time_us,
    )
    .await
    {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) =
        server_persistence::execute(command, args, meta.resp_version == 3, config).await
    {
        return result
            .map(|outcome| {
                if should_replicate_command(command) {
                    server_replication::record_write(shard_id, meta, command, args);
                }
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = server_replication::execute(
        command,
        args,
        meta.resp_version == 3,
        shard_id,
        meta,
        config,
    ) {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = server_diagnostics::execute(command, args, meta.resp_version == 3).await {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = pubsub::execute(command, args, meta, pubsub, shard_pubsub, cluster) {
        return result
            .map(|outcome| {
                sync_meta_flags(meta, tx_state, state);
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if client_ops::should_pause_command(command, &pause_state.borrow()) {
        await_pause(meta.id, pause_state).await;
    }

    if let Some(result) = connection_client::execute(
        command,
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
    ) {
        return result
            .map(|outcome| {
                sync_meta_flags(meta, tx_state, state);
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) = cluster::execute(
        command,
        args,
        meta.resp_version == 3,
        cluster,
        store,
        config.max_memory,
    ) {
        return result
            .map(|outcome| {
                (
                    outcome.response,
                    outcome.close_after_write,
                    outcome.suppress_response,
                    outcome.force_send_response,
                )
            })
            .map_err(ConnectionControl::Continue);
    }

    if let Some(result) =
        dispatch_routed_store_command(meta, command, args, shard_id, config, tracking_registry)
            .await
    {
        return result;
    }

    if let Some(blocked_response) = dispatch_blocking_command(
        meta.id,
        command,
        args,
        config,
        shard_id,
        store,
        blocked,
        watch_registry,
        connections,
        meta,
        state,
    )
    .await?
    {
        apply_store_write_side_effects(
            shard_id,
            meta,
            command,
            args,
            &blocked_response,
            store,
            blocked,
            watch_registry,
            connections,
            client_connections,
            tracking_registry,
        );
        return Ok((
            serialize_response(&blocked_response, meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }

    if let Some(response) = dispatch_key_lifecycle_command(
        command,
        args,
        store,
        blocked,
        watch_registry,
        connections,
        meta.resp_version == 3,
    )
    .map_err(ConnectionControl::Continue)?
    {
        if should_replicate_command(command) {
            server_replication::record_write(shard_id, meta, command, args);
        }
        return Ok((response, false, false, false));
    }

    let module_response = {
        let mut store_ref = store.borrow_mut();
        crate::modules::dispatch(
            shard_id,
            command,
            args,
            meta.resp_version == 3,
            shard_extensions,
            &mut store_ref,
        )
    };
    if let Some(module_response) = module_response {
        return match module_response {
            Ok(response) => {
                if response.is_write {
                    notify_keys_written(
                        &response.touched_keys,
                        &mut store.borrow_mut(),
                        watch_registry,
                        connections,
                    );
                    client_ops::invalidate_written_keys(
                        &response.touched_keys,
                        meta.id,
                        tracking_registry,
                        client_connections,
                    );
                    server_replication::record_write(shard_id, meta, command, args);
                }
                Ok((response.response, false, false, false))
            }
            Err(error) => Err(ConnectionControl::Continue(error)),
        };
    }

    let response = {
        let mut store_ref = store.borrow_mut();
        let restore_no_touch = store_ref.no_touch();
        store_ref.set_no_touch(meta.no_touch);
        let response = dispatch::dispatch(&mut store_ref, command, args);
        store_ref.set_no_touch(restore_no_touch);
        response
    };
    match response {
        Ok(response) => {
            client_ops::maybe_track_read(command, args, meta, tracking_registry);
            apply_store_write_side_effects(
                shard_id,
                meta,
                command,
                args,
                &response,
                store,
                blocked,
                watch_registry,
                connections,
                client_connections,
                tracking_registry,
            );
            Ok((
                serialize_response(&response, meta.resp_version == 3),
                false,
                false,
                false,
            ))
        }
        Err(error) => Err(ConnectionControl::Continue(error_bytes(&error))),
    }
}

async fn dispatch_routed_store_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    shard_id: usize,
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Option<Result<(Vec<u8>, bool, bool, bool), ConnectionControl>> {
    if config.num_shards <= 1 {
        return None;
    }

    if let Some(result) = dispatch_global_or_special_multi_shard_command(
        meta,
        command,
        args,
        config,
        tracking_registry,
    )
    .await
    {
        return Some(result);
    }

    if is_local_only_store_command(command) {
        return None;
    }

    let keys = match command_info::extract_command_keys(command, args) {
        Ok(Some(keys)) if !keys.is_empty() => keys,
        Ok(_) => return None,
        Err(error) => return Some(Err(ConnectionControl::Continue(error))),
    };

    let Some(target_shard) = target_shard_for_keys(&keys, config.num_shards) else {
        if let Some(result) =
            dispatch_cross_shard_multi_key_command(meta, command, args, config, tracking_registry)
                .await
        {
            return Some(result);
        }
        return Some(Err(ConnectionControl::Continue(error_message(
            "ERR cross-shard multi-key routing not yet supported",
        ))));
    };

    if target_shard == shard_id {
        return None;
    }

    let routed_args = match args
        .iter()
        .map(|arg| {
            frame_bytes(arg)
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(args) => args,
        Err(error) => return Some(Err(error)),
    };

    let reply = match server_info::execute_store_command_on_shard(
        target_shard,
        Bytes::copy_from_slice(command),
        routed_args,
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    {
        Ok(reply) => reply,
        Err(error) => return Some(Err(ConnectionControl::Continue(error))),
    };

    if command_info::is_write_command(command) {
        if let Some(offset) = reply.replication_offset {
            meta.last_write_replication_offset = offset;
        }
    } else {
        client_ops::maybe_track_read(command, args, meta, tracking_registry);
    }

    Some(Ok((reply.response, false, false, false)))
}

async fn dispatch_cross_shard_multi_key_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Option<Result<(Vec<u8>, bool, bool, bool), ConnectionControl>> {
    if eq_ascii(command, b"MGET") {
        return Some(dispatch_cross_shard_mget(meta, args, config, tracking_registry).await);
    }
    if eq_ascii(command, b"MSET") {
        return Some(dispatch_cross_shard_mset(meta, args, config, tracking_registry).await);
    }
    if eq_ascii(command, b"MSETNX") {
        return Some(dispatch_cross_shard_msetnx(meta, args, config).await);
    }
    if eq_ascii(command, b"MSETEX") {
        return Some(dispatch_cross_shard_msetex(meta, args, config).await);
    }
    if eq_ascii(command, b"BITOP") {
        let source_keys = match collect_bitop_source_keys(args) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        let destination = match parse_required_arg_bytes(
            args,
            1,
            "ERR wrong number of arguments for 'bitop' command",
        ) {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                Some(destination),
                tracking_registry,
            )
            .await,
        );
    }
    if eq_ascii(command, b"LCS") {
        let source_keys = match collect_fixed_arg_keys(args, &[0, 1]) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                None,
                tracking_registry,
            )
            .await,
        );
    }
    if eq_ascii(command, b"PFCOUNT") {
        let source_keys = match collect_all_arg_keys(args) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                None,
                tracking_registry,
            )
            .await,
        );
    }
    if eq_ascii(command, b"PFMERGE") {
        let source_keys = match collect_all_arg_keys(args) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        let destination = match parse_required_arg_bytes(
            args,
            0,
            "ERR wrong number of arguments for 'pfmerge' command",
        ) {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                Some(destination),
                tracking_registry,
            )
            .await,
        );
    }
    if eq_ascii(command, b"COPY") {
        return Some(dispatch_cross_shard_copy(meta, args, config).await);
    }
    if eq_ascii(command, b"RENAME") {
        return Some(dispatch_cross_shard_rename(meta, args, config, false).await);
    }
    if eq_ascii(command, b"RENAMENX") {
        return Some(dispatch_cross_shard_rename(meta, args, config, true).await);
    }
    if eq_ascii(command, b"LMOVE") || eq_ascii(command, b"RPOPLPUSH") {
        return Some(dispatch_cross_shard_list_move(meta, command, args, config).await);
    }
    if eq_ascii(command, b"SMOVE") {
        return Some(dispatch_cross_shard_smove(meta, args, config).await);
    }
    if eq_ascii(command, b"LMPOP") {
        return Some(dispatch_cross_shard_lmpop(meta, args, config).await);
    }
    if eq_ascii(command, b"ZMPOP") {
        return Some(dispatch_cross_shard_zmpop(meta, args, config).await);
    }
    if eq_ascii(command, b"SDIFF")
        || eq_ascii(command, b"SINTER")
        || eq_ascii(command, b"SINTERCARD")
        || eq_ascii(command, b"SUNION")
        || eq_ascii(command, b"SDIFFSTORE")
        || eq_ascii(command, b"SINTERSTORE")
        || eq_ascii(command, b"SUNIONSTORE")
    {
        return Some(dispatch_cross_shard_set_algebra(meta, command, args, config).await);
    }
    if eq_ascii(command, b"ZDIFF")
        || eq_ascii(command, b"ZINTER")
        || eq_ascii(command, b"ZINTERCARD")
        || eq_ascii(command, b"ZUNION")
        || eq_ascii(command, b"ZDIFFSTORE")
        || eq_ascii(command, b"ZINTERSTORE")
        || eq_ascii(command, b"ZUNIONSTORE")
        || eq_ascii(command, b"ZRANGESTORE")
    {
        return Some(
            dispatch_cross_shard_zset_temp_command(meta, command, args, config, tracking_registry)
                .await,
        );
    }
    if eq_ascii(command, b"DEL")
        || eq_ascii(command, b"UNLINK")
        || eq_ascii(command, b"EXISTS")
        || eq_ascii(command, b"TOUCH")
    {
        return Some(dispatch_cross_shard_integer_sum(meta, command, args, config).await);
    }
    None
}

async fn dispatch_cross_shard_mget(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let grouped = match group_key_args(args, config.num_shards) {
        Ok(grouped) => grouped,
        Err(error) => return Err(ConnectionControl::Continue(error)),
    };
    let mut merged = vec![Response::Value(None); args.len()];
    for (shard_id, entries) in grouped {
        let shard_args = entries
            .iter()
            .map(|(_, bytes)| Bytes::copy_from_slice(bytes.as_slice()))
            .collect::<Vec<_>>();
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"MGET"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        let values = parse_mget_response(&reply.response).map_err(ConnectionControl::Continue)?;
        if values.len() != entries.len() {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        }
        for ((index, _), value) in entries.into_iter().zip(values.into_iter()) {
            merged[index] = value;
        }
    }
    client_ops::maybe_track_read(b"MGET", args, meta, tracking_registry);
    Ok((
        serialize_response(
            &Response::Array(Box::new(
                merged
                    .into_iter()
                    .collect::<smallvec::SmallVec<[Response; 16]>>(),
            )),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_mset(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
    _tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let grouped = match group_mset_pairs(args, config.num_shards) {
        Ok(grouped) => grouped,
        Err(error) => return Err(ConnectionControl::Continue(error)),
    };
    let mut max_offset = None;
    for (shard_id, shard_args) in grouped {
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"MSET"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if !parse_ok_response(&reply.response).map_err(ConnectionControl::Continue)? {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        }
        max_offset = max_offset.max(reply.replication_offset);
    }
    if let Some(offset) = max_offset {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&Response::Simple(b"OK"), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_integer_sum(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let grouped = match group_key_args(args, config.num_shards) {
        Ok(grouped) => grouped,
        Err(error) => return Err(ConnectionControl::Continue(error)),
    };
    let mut total = 0i64;
    let mut max_offset = None;
    for (shard_id, entries) in grouped {
        let shard_args = entries
            .iter()
            .map(|(_, bytes)| Bytes::copy_from_slice(bytes.as_slice()))
            .collect::<Vec<_>>();
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(command),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        total += parse_integer_response(&reply.response).map_err(ConnectionControl::Continue)?;
        max_offset = max_offset.max(reply.replication_offset);
    }
    if command_info::is_write_command(command)
        && let Some(offset) = max_offset
    {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&Response::Integer(total), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_msetnx(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if cross_shard_exists_count(args, config.num_shards, meta).await? > 0 {
        return Ok((
            serialize_response(&Response::Integer(0), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    let grouped = match group_mset_pairs(args, config.num_shards) {
        Ok(grouped) => grouped,
        Err(error) => return Err(ConnectionControl::Continue(error)),
    };
    let mut max_offset = None;
    for (shard_id, shard_args) in grouped {
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"MSET"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if !parse_ok_response(&reply.response).map_err(ConnectionControl::Continue)? {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        }
        max_offset = max_offset.max(reply.replication_offset);
    }
    if let Some(offset) = max_offset {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&Response::Integer(1), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_msetex(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let parsed = parse_cross_shard_msetex_args(args).map_err(ConnectionControl::Continue)?;
    let key_args = parsed
        .pair_bytes
        .chunks(2)
        .filter_map(|chunk| chunk.first())
        .map(|key| Frame::BulkString(key.as_ref()))
        .collect::<Vec<_>>();
    let present = cross_shard_exists_count(&key_args, config.num_shards, meta).await?;
    match parsed.condition {
        CrossShardBatchCondition::Nx if present > 0 => {
            return Ok((
                serialize_response(&Response::Integer(0), meta.resp_version == 3),
                false,
                false,
                false,
            ));
        }
        CrossShardBatchCondition::Xx if present != parsed.numkeys as i64 => {
            return Ok((
                serialize_response(&Response::Integer(0), meta.resp_version == 3),
                false,
                false,
                false,
            ));
        }
        _ => {}
    }

    let grouped = group_msetex_pairs(&parsed, config.num_shards);
    let mut max_offset = None;
    for (shard_id, shard_args) in grouped {
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"MSETEX"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        let applied =
            parse_integer_response(&reply.response).map_err(ConnectionControl::Continue)?;
        if applied <= 0 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        }
        max_offset = max_offset.max(reply.replication_offset);
    }
    if let Some(offset) = max_offset {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(
            &Response::Integer(parsed.numkeys as i64),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_global_or_special_multi_shard_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Option<Result<(Vec<u8>, bool, bool, bool), ConnectionControl>> {
    if eq_ascii(command, b"KEYS") {
        return Some(dispatch_global_keys(meta, args, config).await);
    }
    if eq_ascii(command, b"SCAN") {
        return Some(dispatch_global_scan(meta, args, config).await);
    }
    if eq_ascii(command, b"RANDOMKEY") {
        return Some(dispatch_global_randomkey(meta, args, config).await);
    }
    if eq_ascii(command, b"SORT") || eq_ascii(command, b"SORT_RO") {
        return Some(
            dispatch_global_sort_command(meta, command, args, config, tracking_registry).await,
        );
    }
    if eq_ascii(command, b"GEOSEARCHSTORE") {
        let source_keys = match collect_fixed_arg_keys(args, &[1]) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        let destination = match parse_required_arg_bytes(
            args,
            0,
            "ERR wrong number of arguments for 'geosearchstore' command",
        ) {
            Ok(value) => value,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                Some(destination),
                tracking_registry,
            )
            .await,
        );
    }
    if (eq_ascii(command, b"GEORADIUS") || eq_ascii(command, b"GEORADIUSBYMEMBER"))
        && has_geo_store_option(args)
    {
        let source_keys = match collect_fixed_arg_keys(args, &[0]) {
            Ok(keys) => keys,
            Err(error) => return Some(Err(error)),
        };
        let destination = match find_option_destination(args, &[b"STORE", b"STOREDIST"]) {
            Ok(destination) => destination,
            Err(error) => return Some(Err(error)),
        };
        return Some(
            dispatch_cross_shard_temp_store_command(
                meta,
                command,
                args,
                config,
                source_keys,
                destination,
                tracking_registry,
            )
            .await,
        );
    }
    None
}

async fn dispatch_cross_shard_temp_store_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    source_keys: Vec<Bytes>,
    destination: Option<Bytes>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let owned_args = collect_frame_args(args)?;
    let mut temp_store = hydrate_temp_store_from_keys(&source_keys, config, meta).await?;
    let frames = bytes_to_frames(&owned_args);
    let response = dispatch::dispatch(&mut temp_store, command, &frames)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;

    let mut max_offset = None;
    if let Some(destination) = destination {
        max_offset = sync_temp_store_key(&mut temp_store, &destination, config, meta).await?;
    } else if !command_info::is_write_command(command) {
        client_ops::maybe_track_read(command, args, meta, tracking_registry);
    }
    if let Some(offset) = max_offset {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&response, meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_lmpop(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if args.len() < 3 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'lmpop' command",
        )));
    }
    let numkeys = parse_numkeys_arg(
        args,
        0,
        "ERR numkeys should be greater than 0",
        "ERR numkeys does not match number of keys",
    )?;
    let side_index = 1 + numkeys;
    let side = parse_required_arg_bytes(
        args,
        side_index,
        "ERR wrong number of arguments for 'lmpop' command",
    )?;
    let mut pop_args = vec![side.clone()];
    if side_index + 1 < args.len() {
        pop_args.extend(collect_frame_args(&args[side_index + 1..])?);
    }
    for index in 1..=numkeys {
        let key =
            parse_required_arg_bytes(args, index, "ERR numkeys does not match number of keys")?;
        let shard_id = shard_for_key(key.as_ref(), config.num_shards);
        let mut shard_args = vec![Bytes::from_static(b"1"), key.clone()];
        shard_args.extend(pop_args.iter().cloned());
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"LMPOP"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if !is_null_response(&reply.response).map_err(ConnectionControl::Continue)? {
            if let Some(offset) = reply.replication_offset {
                meta.last_write_replication_offset = offset;
            }
            return Ok((reply.response, false, false, false));
        }
    }
    Ok((
        serialize_response(&Response::Value(None), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_zmpop(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if args.len() < 3 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'zmpop' command",
        )));
    }
    let numkeys = parse_numkeys_arg(
        args,
        0,
        "ERR numkeys should be greater than 0",
        "ERR numkeys does not match number of keys",
    )?;
    let side_index = 1 + numkeys;
    let side = parse_required_arg_bytes(
        args,
        side_index,
        "ERR wrong number of arguments for 'zmpop' command",
    )?;
    let mut pop_args = vec![side.clone()];
    if side_index + 1 < args.len() {
        pop_args.extend(collect_frame_args(&args[side_index + 1..])?);
    }
    for index in 1..=numkeys {
        let key =
            parse_required_arg_bytes(args, index, "ERR numkeys does not match number of keys")?;
        let shard_id = shard_for_key(key.as_ref(), config.num_shards);
        let mut shard_args = vec![Bytes::from_static(b"1"), key.clone()];
        shard_args.extend(pop_args.iter().cloned());
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"ZMPOP"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if !is_null_response(&reply.response).map_err(ConnectionControl::Continue)? {
            if let Some(offset) = reply.replication_offset {
                meta.last_write_replication_offset = offset;
            }
            return Ok((reply.response, false, false, false));
        }
    }
    Ok((
        serialize_response(&Response::Value(None), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_zset_temp_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let (source_keys, destination) = collect_zset_temp_command_keys(command, args)?;
    dispatch_cross_shard_temp_store_command(
        meta,
        command,
        args,
        config,
        source_keys,
        destination,
        tracking_registry,
    )
    .await
}

async fn dispatch_cross_shard_copy(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let (source, destination, replace) = parse_cross_shard_copy_args(args)?;
    let source_shard = shard_for_key(source.as_ref(), config.num_shards);
    let destination_shard = shard_for_key(destination.as_ref(), config.num_shards);
    let Some((payload, ttl_ms)) = fetch_dump_payload(&source, source_shard, meta).await? else {
        return Ok((
            serialize_response(&Response::Integer(0), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    };
    if !replace {
        let exists_reply = server_info::execute_store_command_on_shard(
            destination_shard,
            Bytes::copy_from_slice(b"EXISTS"),
            vec![destination.clone()],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if parse_integer_response(&exists_reply.response).map_err(ConnectionControl::Continue)? > 0
        {
            return Ok((
                serialize_response(&Response::Integer(0), meta.resp_version == 3),
                false,
                false,
                false,
            ));
        }
    }
    let restore_reply = server_info::execute_store_command_on_shard(
        destination_shard,
        Bytes::copy_from_slice(b"RESTORE"),
        restore_args(&destination, ttl_ms, &payload, replace),
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    if !parse_ok_response(&restore_reply.response).map_err(ConnectionControl::Continue)? {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    if let Some(offset) = restore_reply.replication_offset {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&Response::Integer(1), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_rename(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
    nx: bool,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let (source, destination) = parse_cross_shard_two_keys(
        args,
        if nx {
            "ERR wrong number of arguments for 'renamenx' command"
        } else {
            "ERR wrong number of arguments for 'rename' command"
        },
        true,
    )?;
    let source_shard = shard_for_key(source.as_ref(), config.num_shards);
    let destination_shard = shard_for_key(destination.as_ref(), config.num_shards);
    let Some((payload, ttl_ms)) = fetch_dump_payload(&source, source_shard, meta).await? else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR no such key",
        )));
    };

    if nx {
        let exists_reply = server_info::execute_store_command_on_shard(
            destination_shard,
            Bytes::copy_from_slice(b"EXISTS"),
            vec![destination.clone()],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if parse_integer_response(&exists_reply.response).map_err(ConnectionControl::Continue)? > 0
        {
            return Ok((
                serialize_response(&Response::Integer(0), meta.resp_version == 3),
                false,
                false,
                false,
            ));
        }
    }

    let restore_reply = server_info::execute_store_command_on_shard(
        destination_shard,
        Bytes::copy_from_slice(b"RESTORE"),
        restore_args(&destination, ttl_ms, &payload, !nx),
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    if !parse_ok_response(&restore_reply.response).map_err(ConnectionControl::Continue)? {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    let delete_reply = server_info::execute_store_command_on_shard(
        source_shard,
        Bytes::copy_from_slice(b"DEL"),
        vec![source.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let deleted =
        parse_integer_response(&delete_reply.response).map_err(ConnectionControl::Continue)?;
    if deleted != 1 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    if let Some(offset) = restore_reply
        .replication_offset
        .max(delete_reply.replication_offset)
    {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(
            if nx {
                &Response::Integer(1)
            } else {
                &Response::Simple(b"OK")
            },
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_list_move(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let (source, destination, pop_command, push_command) =
        parse_cross_shard_list_move_args(command, args)?;
    let source_shard = shard_for_key(source.as_ref(), config.num_shards);
    let destination_shard = shard_for_key(destination.as_ref(), config.num_shards);
    ensure_remote_type(destination_shard, &destination, b"list", meta).await?;
    let pop_reply = server_info::execute_store_command_on_shard(
        source_shard,
        Bytes::copy_from_slice(pop_command),
        vec![source.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let Some(member) =
        parse_optional_bytes_response(&pop_reply.response).map_err(ConnectionControl::Continue)?
    else {
        return Ok((
            serialize_response(&Response::Value(None), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    };
    let push_reply = server_info::execute_store_command_on_shard(
        destination_shard,
        Bytes::copy_from_slice(push_command),
        vec![destination.clone(), Bytes::from(member.clone())],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let _ = parse_integer_response(&push_reply.response).map_err(ConnectionControl::Continue)?;
    if let Some(offset) = pop_reply
        .replication_offset
        .max(push_reply.replication_offset)
    {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(
            &Response::Value(Some(SenkoValue::Raw(Bytes::from(member)))),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_smove(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if args.len() != 3 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'smove' command",
        )));
    }
    let source = frame_bytes(&args[0])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let destination = frame_bytes(&args[1])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let member = frame_bytes(&args[2])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let source_shard = shard_for_key(source.as_ref(), config.num_shards);
    let destination_shard = shard_for_key(destination.as_ref(), config.num_shards);
    ensure_remote_type(source_shard, &source, b"set", meta).await?;
    ensure_remote_type(destination_shard, &destination, b"set", meta).await?;
    let present_reply = server_info::execute_store_command_on_shard(
        source_shard,
        Bytes::copy_from_slice(b"SISMEMBER"),
        vec![source.clone(), member.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    if parse_integer_response(&present_reply.response).map_err(ConnectionControl::Continue)? == 0 {
        return Ok((
            serialize_response(&Response::Integer(0), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    let remove_reply = server_info::execute_store_command_on_shard(
        source_shard,
        Bytes::copy_from_slice(b"SREM"),
        vec![source.clone(), member.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    if parse_integer_response(&remove_reply.response).map_err(ConnectionControl::Continue)? == 0 {
        return Ok((
            serialize_response(&Response::Integer(0), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    let add_reply = server_info::execute_store_command_on_shard(
        destination_shard,
        Bytes::copy_from_slice(b"SADD"),
        vec![destination.clone(), member],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let _ = parse_integer_response(&add_reply.response).map_err(ConnectionControl::Continue)?;
    if let Some(offset) = remove_reply
        .replication_offset
        .max(add_reply.replication_offset)
    {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(&Response::Integer(1), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_cross_shard_set_algebra(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if eq_ascii(command, b"SDIFF") || eq_ascii(command, b"SINTER") || eq_ascii(command, b"SUNION") {
        let keys = args
            .iter()
            .map(|frame| {
                frame_bytes(frame)
                    .map(Bytes::copy_from_slice)
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sets = fetch_set_sources(&keys, config.num_shards, meta).await?;
        let members = if eq_ascii(command, b"SDIFF") {
            compute_cross_shard_sdiff(&sets)
        } else if eq_ascii(command, b"SINTER") {
            compute_cross_shard_sinter(&sets, None)
        } else {
            compute_cross_shard_sunion(&sets)
        };
        return Ok((
            serialize_response(&set_members_response(&members), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }

    if eq_ascii(command, b"SINTERCARD") {
        let (keys, limit) = parse_cross_shard_sintercard_args(args)?;
        let sets = fetch_set_sources(&keys, config.num_shards, meta).await?;
        let members = compute_cross_shard_sinter(&sets, limit);
        return Ok((
            serialize_response(
                &Response::Integer(members.len() as i64),
                meta.resp_version == 3,
            ),
            false,
            false,
            false,
        ));
    }

    let (destination, sources) = parse_cross_shard_set_store_args(command, args)?;
    let sets = fetch_set_sources(&sources, config.num_shards, meta).await?;
    let members = if eq_ascii(command, b"SDIFFSTORE") {
        compute_cross_shard_sdiff(&sets)
    } else if eq_ascii(command, b"SINTERSTORE") {
        compute_cross_shard_sinter(&sets, None)
    } else {
        compute_cross_shard_sunion(&sets)
    };
    let written =
        write_cross_shard_set_result(destination.as_ref(), &members, config.num_shards, meta)
            .await?;
    if let Some(offset) = written {
        meta.last_write_replication_offset = offset;
    }
    Ok((
        serialize_response(
            &Response::Integer(members.len() as i64),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_global_keys(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let pattern =
        parse_required_arg_bytes(args, 0, "ERR wrong number of arguments for 'keys' command")?;
    if args.len() != 1 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'keys' command",
        )));
    }
    let keys = fetch_all_keys_matching(pattern.as_ref(), meta, config.num_shards).await?;
    Ok((
        serialize_response(&key_array_response(&keys), meta.resp_version == 3),
        false,
        false,
        false,
    ))
}

async fn dispatch_global_randomkey(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    if !args.is_empty() {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'randomkey' command",
        )));
    }
    let keys = fetch_all_keys_matching(b"*", meta, config.num_shards).await?;
    let Some(key) = keys.get(random_index(keys.len())) else {
        return Ok((
            serialize_response(&Response::Value(None), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    };
    if !meta.no_touch {
        let _ = server_info::execute_store_command_on_shard(
            shard_for_key(key.as_ref(), config.num_shards),
            Bytes::copy_from_slice(b"TOUCH"),
            vec![key.clone()],
            meta.resp_version == 3,
            false,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
    }
    Ok((
        serialize_response(
            &Response::Value(Some(SenkoValue::Raw(key.clone()))),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_global_scan(
    meta: &mut ConnectionMeta,
    args: &[Frame<'_>],
    config: &SenkoConfig,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let (cursor, pattern, count, type_filter, type_filter_valid) = parse_global_scan_args(args)?;
    if !type_filter_valid {
        return Ok((
            serialize_response(&scan_response(0, &[]), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    let mut keys =
        fetch_all_keys_matching(pattern.as_deref().unwrap_or(b"*"), meta, config.num_shards)
            .await?;
    if let Some(filter) = type_filter {
        keys = filter_keys_by_type(keys, filter.as_ref(), meta, config.num_shards).await?;
    }
    keys.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
    let offset = usize::try_from(cursor).unwrap_or(usize::MAX);
    if offset >= keys.len() {
        return Ok((
            serialize_response(&scan_response(0, &[]), meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    let end = offset.saturating_add(count).min(keys.len());
    let next = if end >= keys.len() { 0 } else { end as u64 };
    Ok((
        serialize_response(
            &scan_response(next, &keys[offset..end]),
            meta.resp_version == 3,
        ),
        false,
        false,
        false,
    ))
}

async fn dispatch_global_sort_command(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) -> Result<(Vec<u8>, bool, bool, bool), ConnectionControl> {
    let source_keys = fetch_all_keys_matching(b"*", meta, config.num_shards).await?;
    let destination = find_option_destination(args, &[b"STORE"])?;
    dispatch_cross_shard_temp_store_command(
        meta,
        command,
        args,
        config,
        source_keys,
        destination,
        tracking_registry,
    )
    .await
}

fn key_array_response(keys: &[Bytes]) -> Response {
    Response::Array(Box::new(
        keys.iter()
            .map(|key| Response::Value(Some(SenkoValue::Raw(key.clone()))))
            .collect::<smallvec::SmallVec<[Response; 16]>>(),
    ))
}

fn scan_response(next: u64, keys: &[Bytes]) -> Response {
    Response::Array(Box::new(smallvec::smallvec![
        Response::Value(Some(SenkoValue::Raw(Bytes::from(next.to_string())))),
        key_array_response(keys),
    ]))
}

async fn fetch_all_keys_matching(
    pattern: &[u8],
    meta: &ConnectionMeta,
    num_shards: usize,
) -> Result<Vec<Bytes>, ConnectionControl> {
    let mut out = Vec::new();
    for shard_id in 0..num_shards {
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"KEYS"),
            vec![Bytes::copy_from_slice(pattern)],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        out.extend(
            parse_bulk_array_response(&reply.response)
                .map_err(ConnectionControl::Continue)?
                .into_iter()
                .map(Bytes::from),
        );
    }
    Ok(out)
}

async fn filter_keys_by_type(
    keys: Vec<Bytes>,
    type_filter: &[u8],
    meta: &ConnectionMeta,
    num_shards: usize,
) -> Result<Vec<Bytes>, ConnectionControl> {
    let mut out = Vec::new();
    for key in keys {
        let reply = server_info::execute_store_command_on_shard(
            shard_for_key(key.as_ref(), num_shards),
            Bytes::copy_from_slice(b"TYPE"),
            vec![key.clone()],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        if parse_type_name_response(&reply.response)
            .map_err(ConnectionControl::Continue)?
            .eq_ignore_ascii_case(type_filter)
        {
            out.push(key);
        }
    }
    Ok(out)
}

fn parse_global_scan_args(
    args: &[Frame<'_>],
) -> Result<(u64, Option<Bytes>, usize, Option<Bytes>, bool), ConnectionControl> {
    if args.is_empty() {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'scan' command",
        )));
    }
    let cursor = parse_u64_bytes(
        frame_bytes(&args[0]).map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
        "ERR invalid cursor",
    )?;
    let mut index = 1usize;
    let mut pattern = None;
    let mut count = 10usize;
    let mut type_filter = None;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if token.eq_ignore_ascii_case(b"MATCH") {
            index += 1;
            if index >= args.len() {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR syntax error",
                )));
            }
            pattern = Some(
                frame_bytes(&args[index])
                    .map(Bytes::copy_from_slice)
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            );
            index += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"COUNT") {
            index += 1;
            if index >= args.len() {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR syntax error",
                )));
            }
            count = parse_usize_bytes(
                frame_bytes(&args[index])
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
                "ERR value is not an integer or out of range",
            )?
            .max(1);
            index += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"TYPE") {
            index += 1;
            if index >= args.len() {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR syntax error",
                )));
            }
            type_filter = Some(
                frame_bytes(&args[index])
                    .map(Bytes::copy_from_slice)
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            );
            index += 1;
            continue;
        }
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    }
    let type_filter_valid = type_filter
        .as_ref()
        .is_none_or(|filter| is_scan_type_filter(filter.as_ref()));
    Ok((cursor, pattern, count, type_filter, type_filter_valid))
}

fn is_scan_type_filter(value: &[u8]) -> bool {
    value.eq_ignore_ascii_case(b"string")
        || value.eq_ignore_ascii_case(b"list")
        || value.eq_ignore_ascii_case(b"set")
        || value.eq_ignore_ascii_case(b"zset")
        || value.eq_ignore_ascii_case(b"hash")
        || value.eq_ignore_ascii_case(b"stream")
}

async fn hydrate_temp_store_from_keys(
    keys: &[Bytes],
    config: &SenkoConfig,
    meta: &ConnectionMeta,
) -> Result<Store, ConnectionControl> {
    let mut store = Store::new(config.max_memory);
    let mut seen = HashSet::<Vec<u8>, RandomState>::with_hasher(RandomState::default());
    for key in keys {
        if !seen.insert(key.to_vec()) {
            continue;
        }
        let shard_id = shard_for_key(key.as_ref(), config.num_shards);
        if let Some((payload, ttl_ms)) = fetch_dump_payload(key, shard_id, meta).await? {
            restore_key_into_temp_store(&mut store, key, &payload, ttl_ms)?;
        }
    }
    Ok(store)
}

fn restore_key_into_temp_store(
    store: &mut Store,
    key: &Bytes,
    payload: &Bytes,
    ttl_ms: u64,
) -> Result<(), ConnectionControl> {
    let args = vec![
        key.clone(),
        Bytes::from(ttl_ms.to_string()),
        payload.clone(),
    ];
    let frames = bytes_to_frames(&args);
    let _ = dispatch::dispatch(store, b"RESTORE", &frames)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    Ok(())
}

async fn sync_temp_store_key(
    store: &mut Store,
    key: &Bytes,
    config: &SenkoConfig,
    meta: &ConnectionMeta,
) -> Result<Option<u64>, ConnectionControl> {
    let shard_id = shard_for_key(key.as_ref(), config.num_shards);
    let Some(entry) = store.clone_entry(key.as_ref()) else {
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"DEL"),
            vec![key.clone()],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        let _ = parse_integer_response(&reply.response).map_err(ConnectionControl::Continue)?;
        return Ok(reply.replication_offset);
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let ttl_ms = entry
        .expires_at
        .map(|deadline| deadline.saturating_sub(now_ms))
        .unwrap_or(0);
    let payload =
        senko_store::commands::generic::migrate::dump_value(&entry.value, entry.expires_at);
    let reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"RESTORE"),
        restore_args(key, ttl_ms, &payload, true),
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    if !parse_ok_response(&reply.response).map_err(ConnectionControl::Continue)? {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    Ok(reply.replication_offset)
}

fn collect_frame_args(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    args.iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
        })
        .collect()
}

fn collect_all_arg_keys(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    collect_frame_args(args)
}

fn collect_fixed_arg_keys(
    args: &[Frame<'_>],
    indexes: &[usize],
) -> Result<Vec<Bytes>, ConnectionControl> {
    indexes
        .iter()
        .filter_map(|index| args.get(*index))
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
        })
        .collect()
}

fn collect_bitop_source_keys(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    if args.len() < 3 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'bitop' command",
        )));
    }
    collect_frame_args(&args[2..])
}

fn parse_required_arg_bytes(
    args: &[Frame<'_>],
    index: usize,
    wrong_arity: &str,
) -> Result<Bytes, ConnectionControl> {
    let Some(frame) = args.get(index) else {
        return Err(ConnectionControl::Continue(error_message(wrong_arity)));
    };
    frame_bytes(frame)
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
}

fn parse_numkeys_arg(
    args: &[Frame<'_>],
    index: usize,
    zero_message: &str,
    mismatch_message: &str,
) -> Result<usize, ConnectionControl> {
    let raw = parse_required_arg_bytes(args, index, mismatch_message)?;
    let numkeys = parse_usize_bytes(raw.as_ref(), zero_message)?;
    if numkeys == 0 {
        return Err(ConnectionControl::Continue(error_message(zero_message)));
    }
    if args.len() < index + 1 + numkeys {
        return Err(ConnectionControl::Continue(error_message(mismatch_message)));
    }
    Ok(numkeys)
}

fn parse_usize_bytes(raw: &[u8], error_message_text: &str) -> Result<usize, ConnectionControl> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| ConnectionControl::Continue(error_message(error_message_text)))
}

fn parse_u64_bytes(raw: &[u8], error_message_text: &str) -> Result<u64, ConnectionControl> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| ConnectionControl::Continue(error_message(error_message_text)))
}

fn has_geo_store_option(args: &[Frame<'_>]) -> bool {
    args.iter().any(|frame| {
        frame_bytes(frame).is_ok_and(|token| {
            token.eq_ignore_ascii_case(b"STORE") || token.eq_ignore_ascii_case(b"STOREDIST")
        })
    })
}

fn find_option_destination(
    args: &[Frame<'_>],
    tokens: &[&[u8]],
) -> Result<Option<Bytes>, ConnectionControl> {
    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if tokens
            .iter()
            .any(|expected| token.eq_ignore_ascii_case(expected))
        {
            return Ok(args
                .get(index + 1)
                .map(|frame| {
                    frame_bytes(frame)
                        .map(Bytes::copy_from_slice)
                        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
                })
                .transpose()?);
        }
        index += 1;
    }
    Ok(None)
}

fn collect_zset_temp_command_keys(
    command: &[u8],
    args: &[Frame<'_>],
) -> Result<(Vec<Bytes>, Option<Bytes>), ConnectionControl> {
    if eq_ascii(command, b"ZRANGESTORE") {
        return Ok((
            collect_fixed_arg_keys(args, &[1])?,
            Some(parse_required_arg_bytes(
                args,
                0,
                "ERR wrong number of arguments for 'zrangestore' command",
            )?),
        ));
    }
    if eq_ascii(command, b"ZDIFF")
        || eq_ascii(command, b"ZINTER")
        || eq_ascii(command, b"ZUNION")
        || eq_ascii(command, b"ZINTERCARD")
    {
        let numkeys = parse_numkeys_arg(
            args,
            0,
            "ERR numkeys should be greater than 0",
            "ERR numkeys does not match number of keys",
        )?;
        let keys = (1..=numkeys)
            .map(|index| {
                parse_required_arg_bytes(args, index, "ERR numkeys does not match number of keys")
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((keys, None));
    }
    let numkeys = parse_numkeys_arg(
        args,
        1,
        "ERR numkeys should be greater than 0",
        "ERR numkeys does not match number of keys",
    )?;
    let keys = (2..2 + numkeys)
        .map(|index| {
            parse_required_arg_bytes(args, index, "ERR numkeys does not match number of keys")
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        keys,
        Some(parse_required_arg_bytes(
            args,
            0,
            "ERR wrong number of arguments for zset store command",
        )?),
    ))
}

fn group_key_args(
    args: &[Frame<'_>],
    num_shards: usize,
) -> Result<Vec<(usize, Vec<(usize, Vec<u8>)>)>, Vec<u8>> {
    let mut grouped = std::collections::BTreeMap::<usize, Vec<(usize, Vec<u8>)>>::new();
    for (index, frame) in args.iter().enumerate() {
        let key = frame_bytes(frame)
            .map_err(|error| error_bytes(&error))?
            .to_vec();
        grouped
            .entry(shard_for_key(&key, num_shards))
            .or_default()
            .push((index, key));
    }
    Ok(grouped.into_iter().collect())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CrossShardBatchCondition {
    Always,
    Nx,
    Xx,
}

struct ParsedCrossShardMsetex {
    numkeys: usize,
    pair_bytes: Vec<Bytes>,
    option_bytes: Vec<Bytes>,
    condition: CrossShardBatchCondition,
}

fn group_mset_pairs(
    args: &[Frame<'_>],
    num_shards: usize,
) -> Result<Vec<(usize, Vec<Bytes>)>, Vec<u8>> {
    let mut grouped = std::collections::BTreeMap::<usize, Vec<Bytes>>::new();
    for chunk in args.chunks(2) {
        let Some(key_frame) = chunk.first() else {
            continue;
        };
        let key = frame_bytes(key_frame)
            .map_err(|error| error_bytes(&error))?
            .to_vec();
        let shard_id = shard_for_key(&key, num_shards);
        let bucket = grouped.entry(shard_id).or_default();
        bucket.push(Bytes::from(key));
        if let Some(value_frame) = chunk.get(1) {
            bucket.push(
                frame_bytes(value_frame)
                    .map_err(|error| error_bytes(&error))
                    .map(Bytes::copy_from_slice)?,
            );
        }
    }
    Ok(grouped.into_iter().collect())
}

fn group_msetex_pairs(
    parsed: &ParsedCrossShardMsetex,
    num_shards: usize,
) -> Vec<(usize, Vec<Bytes>)> {
    let mut grouped = std::collections::BTreeMap::<usize, Vec<Bytes>>::new();
    for chunk in parsed.pair_bytes.chunks(2) {
        let Some(key) = chunk.first() else {
            continue;
        };
        let shard_id = shard_for_key(key.as_ref(), num_shards);
        let bucket = grouped.entry(shard_id).or_default();
        bucket.extend(chunk.iter().cloned());
    }
    for bucket in grouped.values_mut() {
        let local_numkeys = bucket.len() / 2;
        bucket.insert(0, Bytes::from(local_numkeys.to_string()));
        bucket.extend(parsed.option_bytes.iter().cloned());
    }
    grouped.into_iter().collect()
}

fn parse_cross_shard_msetex_args(args: &[Frame<'_>]) -> Result<ParsedCrossShardMsetex, Vec<u8>> {
    let Some(numkeys_frame) = args.first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'msetex' command",
        ));
    };
    let numkeys =
        std::str::from_utf8(frame_bytes(numkeys_frame).map_err(|error| error_bytes(&error))?)
            .ok()
            .and_then(|text| text.parse::<usize>().ok())
            .ok_or_else(|| error_message("ERR numkeys value is not an integer or out of range"))?;
    if numkeys == 0 {
        return Err(error_message("ERR numkeys should be greater than 0"));
    }
    let pair_args = numkeys
        .checked_mul(2)
        .ok_or_else(|| error_message("ERR syntax error"))?;
    if args.len() < 1 + pair_args {
        return Err(error_message(
            "ERR numkeys does not match number of key-value pairs",
        ));
    }
    let pair_bytes = args[1..1 + pair_args]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map_err(|error| error_bytes(&error))
                .map(Bytes::copy_from_slice)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let option_bytes = args[1 + pair_args..]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map_err(|error| error_bytes(&error))
                .map(Bytes::copy_from_slice)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut condition = CrossShardBatchCondition::Always;
    for token in &option_bytes {
        if token.eq_ignore_ascii_case(b"NX") {
            condition = CrossShardBatchCondition::Nx;
        } else if token.eq_ignore_ascii_case(b"XX") {
            condition = CrossShardBatchCondition::Xx;
        }
    }
    Ok(ParsedCrossShardMsetex {
        numkeys,
        pair_bytes,
        option_bytes,
        condition,
    })
}

async fn cross_shard_exists_count(
    args: &[Frame<'_>],
    num_shards: usize,
    meta: &ConnectionMeta,
) -> Result<i64, ConnectionControl> {
    let grouped = group_key_args(args, num_shards).map_err(ConnectionControl::Continue)?;
    let mut total = 0i64;
    for (shard_id, entries) in grouped {
        let shard_args = entries
            .iter()
            .map(|(_, bytes)| Bytes::copy_from_slice(bytes.as_slice()))
            .collect::<Vec<_>>();
        let reply = server_info::execute_store_command_on_shard(
            shard_id,
            Bytes::copy_from_slice(b"EXISTS"),
            shard_args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        total += parse_integer_response(&reply.response).map_err(ConnectionControl::Continue)?;
    }
    Ok(total)
}

async fn fetch_set_sources(
    args: &[Bytes],
    num_shards: usize,
    meta: &ConnectionMeta,
) -> Result<Vec<HashSet<Vec<u8>, RandomState>>, ConnectionControl> {
    let mut sets = Vec::with_capacity(args.len());
    for key in args {
        let reply = server_info::execute_store_command_on_shard(
            shard_for_key(key.as_ref(), num_shards),
            Bytes::copy_from_slice(b"SMEMBERS"),
            vec![key.clone()],
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        let members =
            parse_bulk_array_response(&reply.response).map_err(ConnectionControl::Continue)?;
        let mut set = HashSet::with_hasher(RandomState::default());
        for member in members {
            set.insert(member);
        }
        sets.push(set);
    }
    Ok(sets)
}

fn compute_cross_shard_sdiff(sets: &[HashSet<Vec<u8>, RandomState>]) -> Vec<Vec<u8>> {
    let Some(first) = sets.first() else {
        return Vec::new();
    };
    let mut current = first.clone();
    for other in &sets[1..] {
        current.retain(|member| !other.contains(member));
        if current.is_empty() {
            break;
        }
    }
    current.into_iter().collect()
}

fn compute_cross_shard_sinter(
    sets: &[HashSet<Vec<u8>, RandomState>],
    limit: Option<usize>,
) -> Vec<Vec<u8>> {
    let Some(first) = sets.first() else {
        return Vec::new();
    };
    let mut current = first.clone();
    for other in &sets[1..] {
        current.retain(|member| other.contains(member));
        if current.is_empty() {
            break;
        }
    }
    let mut members = current.into_iter().collect::<Vec<_>>();
    if let Some(limit) = limit {
        members.truncate(limit);
    }
    members
}

fn compute_cross_shard_sunion(sets: &[HashSet<Vec<u8>, RandomState>]) -> Vec<Vec<u8>> {
    let mut out = HashSet::with_hasher(RandomState::default());
    for set in sets {
        out.extend(set.iter().cloned());
    }
    out.into_iter().collect()
}

fn set_members_response(members: &[Vec<u8>]) -> Response {
    Response::Array(Box::new(
        members
            .iter()
            .map(|member| Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(member)))))
            .collect::<smallvec::SmallVec<[Response; 16]>>(),
    ))
}

fn parse_bulk_array_response(bytes: &[u8]) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let frame = parse_single_response_frame(bytes)?;
    if let Frame::SimpleError(error) | Frame::BlobError(error) = frame {
        return Err(error.to_vec());
    }
    let Frame::Array(values) = frame else {
        return Err(error_message("ERR shard coordination protocol error"));
    };
    values
        .iter()
        .map(|value| match value.map_err(|error| error_bytes(&error))? {
            Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes.to_vec()),
            Frame::Null => Ok(Vec::new()),
            Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
            _ => Err(error_message("ERR shard coordination protocol error")),
        })
        .collect()
}

fn parse_cross_shard_sintercard_args(
    args: &[Frame<'_>],
) -> Result<(Vec<Bytes>, Option<usize>), ConnectionControl> {
    if args.len() < 2 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'sintercard' command",
        )));
    }
    let numkeys = std::str::from_utf8(
        frame_bytes(&args[0]).map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
    )
    .ok()
    .and_then(|value| value.parse::<usize>().ok())
    .ok_or_else(|| {
        ConnectionControl::Continue(error_message("ERR numkeys should be greater than 0"))
    })?;
    if numkeys == 0 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR numkeys should be greater than 0",
        )));
    }
    if args.len() < 1 + numkeys {
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    }
    let limit = if args.len() == 1 + numkeys {
        None
    } else if args.len() == 3 + numkeys {
        let token = frame_bytes(&args[1 + numkeys])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if !token.eq_ignore_ascii_case(b"LIMIT") {
            return Err(ConnectionControl::Continue(error_message(
                "ERR syntax error",
            )));
        }
        Some(
            std::str::from_utf8(
                frame_bytes(&args[2 + numkeys])
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            )
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            .max(0) as usize,
        )
    } else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    };
    Ok((
        args[1..1 + numkeys]
            .iter()
            .map(|frame| {
                frame_bytes(frame)
                    .map(Bytes::copy_from_slice)
                    .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
            })
            .collect::<Result<Vec<_>, _>>()?,
        limit.filter(|limit| *limit > 0),
    ))
}

fn parse_cross_shard_set_store_args(
    command: &[u8],
    args: &[Frame<'_>],
) -> Result<(Bytes, Vec<Bytes>), ConnectionControl> {
    if args.len() < 2 {
        let message = if eq_ascii(command, b"SDIFFSTORE") {
            "ERR wrong number of arguments for 'sdiffstore' command"
        } else if eq_ascii(command, b"SINTERSTORE") {
            "ERR wrong number of arguments for 'sinterstore' command"
        } else {
            "ERR wrong number of arguments for 'sunionstore' command"
        };
        return Err(ConnectionControl::Continue(error_message(message)));
    }
    let destination = frame_bytes(&args[0])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let sources = args[1..]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((destination, sources))
}

async fn write_cross_shard_set_result(
    destination: &[u8],
    members: &[Vec<u8>],
    num_shards: usize,
    meta: &ConnectionMeta,
) -> Result<Option<u64>, ConnectionControl> {
    let shard_id = shard_for_key(destination, num_shards);
    let mut max_offset = None;
    let delete_reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"DEL"),
        vec![Bytes::copy_from_slice(destination)],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    max_offset = max_offset.max(delete_reply.replication_offset);
    if members.is_empty() {
        return Ok(max_offset);
    }
    let mut args = Vec::with_capacity(members.len() + 1);
    args.push(Bytes::copy_from_slice(destination));
    args.extend(members.iter().map(|member| Bytes::copy_from_slice(member)));
    let add_reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"SADD"),
        args,
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let _ = parse_integer_response(&add_reply.response).map_err(ConnectionControl::Continue)?;
    max_offset = max_offset.max(add_reply.replication_offset);
    Ok(max_offset)
}

fn parse_single_response_frame<'a>(bytes: &'a [u8]) -> Result<Frame<'a>, Vec<u8>> {
    match RESP_PARSER
        .parse(bytes)
        .map_err(|error| error_bytes(&error))?
    {
        ParseStatus::Complete(frame, used) if used == bytes.len() => Ok(frame),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn parse_optional_bytes_response(bytes: &[u8]) -> Result<Option<Vec<u8>>, Vec<u8>> {
    match parse_single_response_frame(bytes)? {
        Frame::BulkString(value) | Frame::SimpleString(value) => Ok(Some(value.to_vec())),
        Frame::Null => Ok(None),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn parse_response(bytes: &[u8]) -> Result<Response, Vec<u8>> {
    frame_to_response(parse_single_response_frame(bytes)?)
}

fn is_null_response(bytes: &[u8]) -> Result<bool, Vec<u8>> {
    match parse_single_response_frame(bytes)? {
        Frame::Null => Ok(true),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Ok(false),
    }
}

fn frame_to_response(frame: Frame<'_>) -> Result<Response, Vec<u8>> {
    match frame {
        Frame::SimpleString(value) | Frame::BulkString(value) => Ok(Response::Value(Some(
            SenkoValue::Raw(Bytes::copy_from_slice(value)),
        ))),
        Frame::Integer(value) => Ok(Response::Integer(value)),
        Frame::Null => Ok(Response::Value(None)),
        Frame::Array(values) => Ok(Response::Array(Box::new(
            values
                .iter()
                .map(|value| {
                    value
                        .map_err(|error| error_bytes(&error))
                        .and_then(frame_to_response)
                })
                .collect::<Result<smallvec::SmallVec<[Response; 16]>, _>>()?,
        ))),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn parse_integer_response(bytes: &[u8]) -> Result<i64, Vec<u8>> {
    match parse_single_response_frame(bytes)? {
        Frame::Integer(value) => Ok(value),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn parse_ok_response(bytes: &[u8]) -> Result<bool, Vec<u8>> {
    match parse_single_response_frame(bytes)? {
        Frame::SimpleString(value) => Ok(value.eq_ignore_ascii_case(b"OK")),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn parse_type_name_response(bytes: &[u8]) -> Result<Vec<u8>, Vec<u8>> {
    match parse_single_response_frame(bytes)? {
        Frame::BulkString(value) | Frame::SimpleString(value) => Ok(value.to_vec()),
        Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
        _ => Err(error_message("ERR shard coordination protocol error")),
    }
}

fn random_index(len: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    (nanos % len as u128) as usize
}

fn parse_mget_response(bytes: &[u8]) -> Result<Vec<Response>, Vec<u8>> {
    let frame = parse_single_response_frame(bytes)?;
    if let Frame::SimpleError(error) | Frame::BlobError(error) = frame {
        return Err(error.to_vec());
    }
    let Frame::Array(values) = frame else {
        return Err(error_message("ERR shard coordination protocol error"));
    };
    values
        .iter()
        .map(|value| match value.map_err(|error| error_bytes(&error))? {
            Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(Response::Value(Some(
                SenkoValue::Raw(Bytes::copy_from_slice(bytes)),
            ))),
            Frame::Null => Ok(Response::Value(None)),
            Frame::SimpleError(error) | Frame::BlobError(error) => Err(error.to_vec()),
            _ => Err(error_message("ERR shard coordination protocol error")),
        })
        .collect()
}

fn parse_cross_shard_two_keys(
    args: &[Frame<'_>],
    wrong_arity: &str,
    validate_destination_utf8: bool,
) -> Result<(Bytes, Bytes), ConnectionControl> {
    if args.len() != 2 {
        return Err(ConnectionControl::Continue(error_message(wrong_arity)));
    }
    let source = frame_bytes(&args[0])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let destination = frame_bytes(&args[1])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    if validate_destination_utf8 && std::str::from_utf8(destination.as_ref()).is_err() {
        return Err(ConnectionControl::Continue(error_message(
            "ERR invalid UTF-8 key",
        )));
    }
    Ok((source, destination))
}

fn parse_cross_shard_copy_args(
    args: &[Frame<'_>],
) -> Result<(Bytes, Bytes, bool), ConnectionControl> {
    if args.len() < 2 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR wrong number of arguments for 'copy' command",
        )));
    }
    let source = frame_bytes(&args[0])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    let destination = frame_bytes(&args[1])
        .map(Bytes::copy_from_slice)
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    if std::str::from_utf8(destination.as_ref()).is_err() {
        return Err(ConnectionControl::Continue(error_message(
            "ERR invalid UTF-8 key",
        )));
    }
    let mut replace = false;
    let mut index = 2usize;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if token.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            index += 1;
            continue;
        }
        if token.eq_ignore_ascii_case(b"DB") {
            index += 1;
            if index >= args.len() {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR syntax error",
                )));
            }
            let db = frame_bytes(&args[index])
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
            let Some(db) = std::str::from_utf8(db)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
            else {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR invalid DB index",
                )));
            };
            if db != 0 {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR invalid DB index",
                )));
            }
            index += 1;
            continue;
        }
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    }
    Ok((source, destination, replace))
}

fn parse_cross_shard_list_move_args<'a>(
    command: &[u8],
    args: &'a [Frame<'_>],
) -> Result<(Bytes, Bytes, &'static [u8], &'static [u8]), ConnectionControl> {
    let (source, destination, from, to) = if eq_ascii(command, b"RPOPLPUSH") {
        if args.len() != 2 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR wrong number of arguments for 'rpoplpush' command",
            )));
        }
        (
            frame_bytes(&args[0])
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            frame_bytes(&args[1])
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            b"RPOP".as_slice(),
            b"LPUSH".as_slice(),
        )
    } else {
        if args.len() != 4 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR wrong number of arguments for 'lmove' command",
            )));
        }
        let from = frame_bytes(&args[2])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let to = frame_bytes(&args[3])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let pop = if from.eq_ignore_ascii_case(b"LEFT") {
            b"LPOP".as_slice()
        } else if from.eq_ignore_ascii_case(b"RIGHT") {
            b"RPOP".as_slice()
        } else {
            return Err(ConnectionControl::Continue(error_message(
                "ERR syntax error",
            )));
        };
        let push = if to.eq_ignore_ascii_case(b"LEFT") {
            b"LPUSH".as_slice()
        } else if to.eq_ignore_ascii_case(b"RIGHT") {
            b"RPUSH".as_slice()
        } else {
            return Err(ConnectionControl::Continue(error_message(
                "ERR syntax error",
            )));
        };
        (
            frame_bytes(&args[0])
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            frame_bytes(&args[1])
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?,
            pop,
            push,
        )
    };
    Ok((source, destination, from, to))
}

async fn ensure_remote_type(
    shard_id: usize,
    key: &Bytes,
    expected: &[u8],
    meta: &ConnectionMeta,
) -> Result<(), ConnectionControl> {
    let reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"TYPE"),
        vec![key.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let value = parse_type_name_response(&reply.response).map_err(ConnectionControl::Continue)?;
    if value != b"none" && value != expected {
        return Err(ConnectionControl::Continue(error_message(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )));
    }
    Ok(())
}

async fn fetch_dump_payload(
    source: &Bytes,
    shard_id: usize,
    meta: &ConnectionMeta,
) -> Result<Option<(Bytes, u64)>, ConnectionControl> {
    let dump_reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"DUMP"),
        vec![source.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let Some(payload) =
        parse_optional_bytes_response(&dump_reply.response).map_err(ConnectionControl::Continue)?
    else {
        return Ok(None);
    };
    let ttl_reply = server_info::execute_store_command_on_shard(
        shard_id,
        Bytes::copy_from_slice(b"PTTL"),
        vec![source.clone()],
        meta.resp_version == 3,
        meta.no_touch,
        meta.id,
    )
    .await
    .map_err(ConnectionControl::Continue)?;
    let ttl = parse_integer_response(&ttl_reply.response).map_err(ConnectionControl::Continue)?;
    Ok(Some((Bytes::from(payload), ttl.max(0) as u64)))
}

fn restore_args(destination: &Bytes, ttl_ms: u64, payload: &Bytes, replace: bool) -> Vec<Bytes> {
    let mut args = vec![
        destination.clone(),
        Bytes::from(ttl_ms.to_string()),
        payload.clone(),
    ];
    if replace {
        args.push(Bytes::copy_from_slice(b"REPLACE"));
    }
    args
}

#[inline]
fn target_shard_for_keys(keys: &[Vec<u8>], num_shards: usize) -> Option<usize> {
    let mut iter = keys.iter();
    let first = shard_for_key(iter.next()?.as_slice(), num_shards);
    if iter.all(|key| shard_for_key(key.as_slice(), num_shards) == first) {
        Some(first)
    } else {
        None
    }
}

fn target_shard_for_byte_keys(keys: &[Bytes], num_shards: usize) -> Option<usize> {
    let mut iter = keys.iter();
    let first = shard_for_key(iter.next()?.as_ref(), num_shards);
    if iter.all(|key| shard_for_key(key.as_ref(), num_shards) == first) {
        Some(first)
    } else {
        None
    }
}

#[inline]
fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    usize::from(senko_cluster::crc16_slot(key)) % num_shards.max(1)
}

#[inline]
fn is_local_only_store_command(command: &[u8]) -> bool {
    eq_ascii(command, b"BLPOP")
        || eq_ascii(command, b"BRPOP")
        || eq_ascii(command, b"BLMOVE")
        || eq_ascii(command, b"BRPOPLPUSH")
        || eq_ascii(command, b"BLMPOP")
        || eq_ascii(command, b"BZPOPMIN")
        || eq_ascii(command, b"BZPOPMAX")
        || eq_ascii(command, b"BZMPOP")
        || eq_ascii(command, b"XREAD")
        || eq_ascii(command, b"XREADGROUP")
}

#[inline]
fn should_replicate_command(command: &[u8]) -> bool {
    command_info::is_write_command(command)
}

#[inline]
fn should_replicate_store_command(command: &[u8]) -> bool {
    command_info::is_write_command(command)
}

struct ScriptRuntimeAdapter<'a> {
    shard_id: usize,
    meta: &'a mut ConnectionMeta,
    store: &'a Rc<RefCell<Store>>,
    blocked: &'a Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &'a Rc<RefCell<WatchRegistry>>,
    connections: &'a Rc<RefCell<ConnectionMap>>,
    client_connections: &'a Rc<RefCell<ClientConnectionMap>>,
    tracking_registry: &'a Rc<RefCell<TrackingRegistry>>,
}

impl ScriptExecutionHooks for ScriptRuntimeAdapter<'_> {
    fn dispatch(&mut self, command: &[u8], args: &[Bytes]) -> Result<ScriptRespValue, LuaError> {
        let frames = bytes_to_frames(args);
        let response = {
            let mut store_ref = self.store.borrow_mut();
            let restore_no_touch = store_ref.no_touch();
            store_ref.set_no_touch(self.meta.no_touch);
            let response = dispatch::dispatch(&mut store_ref, command, &frames);
            store_ref.set_no_touch(restore_no_touch);
            response
        }
        .map_err(|error| LuaError::Message(error_message_text(&error)))?;
        {
            let mut store_ref = self.store.borrow_mut();
            let restore_no_touch = store_ref.no_touch();
            store_ref.set_no_touch(self.meta.no_touch);
            post_dispatch_notify(
                command,
                &frames,
                &response,
                &mut store_ref,
                self.blocked,
                self.watch_registry,
                self.connections,
            );
            store_ref.set_no_touch(restore_no_touch);
        }
        let keys = notification_keys(command, &frames, &response);
        client_ops::invalidate_written_keys(
            &keys,
            self.meta.id,
            self.tracking_registry,
            self.client_connections,
        );
        if should_replicate_store_command(command) {
            server_replication::record_write(self.shard_id, self.meta, command, &frames);
        }
        Ok(response_to_script_value(&response))
    }

    fn acl_check(
        &mut self,
        _username: &str,
        command: &[u8],
        args: &[Bytes],
    ) -> Result<(), LuaError> {
        let frames = bytes_to_frames(args);
        acl::check_permissions(self.meta, command, &frames, AclContext::Toplevel, 0)
            .map_err(|bytes| LuaError::Message(resp_error_to_text(&bytes)))
    }

    fn log(&mut self, level: i64, message: &str) {
        match level {
            0 => tracing::debug!(target = "senko.scripting", "{message}"),
            1 => tracing::info!(target = "senko.scripting", "{message}"),
            2 => tracing::info!(target = "senko.scripting", "{message}"),
            _ => tracing::warn!(target = "senko.scripting", "{message}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_scripting_command(
    meta: &mut ConnectionMeta,
    shard_id: usize,
    command: &[u8],
    args: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    engine: &Rc<RefCell<LuaEngine>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
    config: &SenkoConfig,
) -> Option<Result<(Vec<u8>, bool, bool, bool), Vec<u8>>> {
    if eq_ascii(command, b"EVAL")
        || eq_ascii(command, b"EVALSHA")
        || eq_ascii(command, b"EVAL_RO")
        || eq_ascii(command, b"EVALSHA_RO")
        || eq_ascii(command, b"FCALL")
        || eq_ascii(command, b"FCALL_RO")
    {
        let readonly = eq_ascii(command, b"EVAL_RO")
            || eq_ascii(command, b"EVALSHA_RO")
            || eq_ascii(command, b"FCALL_RO");
        let result = match parse_script_invocation(args) {
            Ok((head, keys, argv)) => {
                if config.cluster_enabled
                    && let Err(error) = validate_script_keys(&keys)
                {
                    return Some(Err(error));
                }
                let db_id = meta.db;
                let username = meta.username.clone();
                let mut runtime = ScriptRuntimeAdapter {
                    shard_id,
                    meta,
                    store,
                    blocked,
                    watch_registry,
                    connections,
                    client_connections,
                    tracking_registry,
                };
                let context = ScriptContext {
                    keys: &keys,
                    args: &argv,
                    readonly,
                    db_id,
                    username: username.as_str(),
                    hooks: &mut runtime,
                };
                if eq_ascii(command, b"EVAL") || eq_ascii(command, b"EVAL_RO") {
                    let sha = match server_info::script_load_all(head.clone()).await {
                        Ok(sha) => sha,
                        Err(error) => return Some(Err(error)),
                    };
                    engine.borrow_mut().evalsha(sha.as_str(), context)
                } else if eq_ascii(command, b"EVALSHA") || eq_ascii(command, b"EVALSHA_RO") {
                    let sha1 = String::from_utf8_lossy(head.as_ref()).into_owned();
                    engine.borrow_mut().evalsha(sha1.as_str(), context)
                } else {
                    let name = String::from_utf8_lossy(head.as_ref()).into_owned();
                    engine.borrow_mut().fcall(name.as_str(), context)
                }
            }
            Err(error) => Err(LuaError::Message(resp_error_to_text(&error))),
        };
        return Some(
            result
                .map(|value| {
                    (
                        serialize_script_response(&value, meta.resp_version == 3),
                        false,
                        false,
                        false,
                    )
                })
                .map_err(|error| error_message(&error.client_message())),
        );
    }

    if eq_ascii(command, b"SCRIPT") {
        return Some(dispatch_script_meta(meta, args, engine).await);
    }
    if eq_ascii(command, b"FUNCTION") {
        return Some(dispatch_function_meta(meta, args, engine).await);
    }
    None
}

async fn dispatch_script_meta(
    meta: &ConnectionMeta,
    args: &[Frame<'_>],
    engine: &Rc<RefCell<LuaEngine>>,
) -> Result<(Vec<u8>, bool, bool, bool), Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'script' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"LOAD") {
        let [script] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'script|load' command",
            ));
        };
        let script =
            Bytes::copy_from_slice(frame_bytes(script).map_err(|error| error_bytes(&error))?);
        let sha = server_info::script_load_all(script).await?;
        return Ok((bulk_string(sha.as_bytes()), false, false, false));
    }
    if eq_ascii(subcommand, b"EXISTS") {
        let sha1s = rest
            .iter()
            .map(|frame| frame_bytes(frame).map_err(|error| error_bytes(&error)))
            .collect::<Result<Vec<_>, _>>()?;
        let sha1s = sha1s
            .iter()
            .map(|sha| std::str::from_utf8(sha).unwrap_or_default())
            .collect::<Vec<_>>();
        let result = engine.borrow().script_exists(&sha1s);
        let values = ScriptRespValue::Array(
            result
                .into_iter()
                .map(|exists| ScriptRespValue::Integer(i64::from(exists)))
                .collect(),
        );
        return Ok((
            serialize_script_response(&values, meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    if eq_ascii(subcommand, b"FLUSH") {
        server_info::script_flush_all().await?;
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"DEBUG") {
        let Some(mode) = rest.first() else {
            return Err(error_message(
                "ERR wrong number of arguments for 'script|debug' command",
            ));
        };
        let mode = frame_bytes(mode).map_err(|error| error_bytes(&error))?;
        let debug_mode = if eq_ascii(mode, b"YES") {
            ScriptDebugMode::Yes
        } else if eq_ascii(mode, b"SYNC") {
            ScriptDebugMode::Sync
        } else if eq_ascii(mode, b"NO") {
            ScriptDebugMode::No
        } else {
            return Err(error_message("ERR syntax error"));
        };
        engine.borrow_mut().set_debug_mode(debug_mode);
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"KILL") {
        if !server_info::kill_running_script().await? {
            return Err(error_message("NOTBUSY No scripts in execution right now"));
        }
        return Ok((simple_string(b"OK"), false, false, false));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'script'",
    ))
}

async fn dispatch_function_meta(
    meta: &ConnectionMeta,
    args: &[Frame<'_>],
    engine: &Rc<RefCell<LuaEngine>>,
) -> Result<(Vec<u8>, bool, bool, bool), Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(
            "ERR wrong number of arguments for 'function' command",
        ));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    if eq_ascii(subcommand, b"LOAD") {
        let (replace, code_frame) = match rest {
            [code] => (false, code),
            [flag, code]
                if eq_ascii(
                    frame_bytes(flag).map_err(|error| error_bytes(&error))?,
                    b"REPLACE",
                ) =>
            {
                (true, code)
            }
            _ => {
                return Err(error_message(
                    "ERR wrong number of arguments for 'function|load' command",
                ));
            }
        };
        let code =
            Bytes::copy_from_slice(frame_bytes(code_frame).map_err(|error| error_bytes(&error))?);
        server_info::function_load_all(code, replace).await?;
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"LIST") {
        let mut pattern = None;
        let mut with_code = false;
        let mut index = 0usize;
        while index < rest.len() {
            let token = frame_bytes(&rest[index]).map_err(|error| error_bytes(&error))?;
            if eq_ascii(token, b"WITHCODE") {
                with_code = true;
                index += 1;
            } else if eq_ascii(token, b"LIBRARYNAME") && index + 1 < rest.len() {
                pattern = Some(frame_bytes(&rest[index + 1]).map_err(|error| error_bytes(&error))?);
                index += 2;
            } else {
                return Err(error_message("ERR syntax error"));
            }
        }
        let list = engine.borrow().function_list(pattern, with_code);
        let value = ScriptRespValue::Array(list.into_iter().map(library_info_to_script).collect());
        return Ok((
            serialize_script_response(&value, meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    if eq_ascii(subcommand, b"DELETE") {
        let [name] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'function|delete' command",
            ));
        };
        let name = std::str::from_utf8(frame_bytes(name).map_err(|error| error_bytes(&error))?)
            .map_err(|_| error_message("ERR library name is not valid UTF-8"))?;
        server_info::function_delete_all(name.to_owned()).await?;
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"FLUSH") {
        server_info::function_flush_all().await?;
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"STATS") {
        let value = engine.borrow().function_stats(None);
        return Ok((
            serialize_script_response(&value, meta.resp_version == 3),
            false,
            false,
            false,
        ));
    }
    if eq_ascii(subcommand, b"DUMP") {
        let bytes = engine.borrow().function_dump();
        return Ok((bulk_string(bytes.as_ref()), false, false, false));
    }
    if eq_ascii(subcommand, b"RESTORE") {
        let [payload, mode] = rest else {
            return Err(error_message(
                "ERR wrong number of arguments for 'function|restore' command",
            ));
        };
        let payload = frame_bytes(payload).map_err(|error| error_bytes(&error))?;
        let mode = frame_bytes(mode).map_err(|error| error_bytes(&error))?;
        let mode = if eq_ascii(mode, b"FLUSH") {
            senko_scripting::functions::RestoreMode::Flush
        } else if eq_ascii(mode, b"APPEND") {
            senko_scripting::functions::RestoreMode::Append
        } else if eq_ascii(mode, b"REPLACE") {
            senko_scripting::functions::RestoreMode::Replace
        } else {
            return Err(error_message("ERR syntax error"));
        };
        server_info::function_restore_all(Bytes::copy_from_slice(payload), mode).await?;
        return Ok((simple_string(b"OK"), false, false, false));
    }
    if eq_ascii(subcommand, b"KILL") {
        if !server_info::kill_running_script().await? {
            return Err(error_message("NOTBUSY No scripts in execution right now"));
        }
        return Ok((simple_string(b"OK"), false, false, false));
    }
    Err(error_message(
        "ERR unknown subcommand or wrong number of arguments for 'function'",
    ))
}

fn parse_script_invocation(args: &[Frame<'_>]) -> Result<(Bytes, Vec<Bytes>, Vec<Bytes>), Vec<u8>> {
    if args.len() < 2 {
        return Err(error_message(
            "ERR wrong number of arguments for scripting command",
        ));
    }
    let head = Bytes::copy_from_slice(frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?);
    let numkeys = std::str::from_utf8(frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?)
        .ok()
        .and_then(|text| text.parse::<usize>().ok())
        .ok_or_else(|| error_message("ERR value is not an integer or out of range"))?;
    if args.len() < 2 + numkeys {
        return Err(error_message("ERR syntax error"));
    }
    let keys = args[2..2 + numkeys]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| error_bytes(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let argv = args[2 + numkeys..]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| error_bytes(&error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((head, keys, argv))
}

fn validate_script_keys(keys: &[Bytes]) -> Result<(), Vec<u8>> {
    let Some(first) = keys.first() else {
        return Ok(());
    };
    let slot = senko_cluster::crc16_slot(first.as_ref() as &[u8]);
    for key in &keys[1..] {
        if senko_cluster::crc16_slot(key.as_ref() as &[u8]) != slot {
            return Err(error_message(
                "CROSSSLOT Keys in request don't hash to the same slot",
            ));
        }
    }
    Ok(())
}

fn bytes_to_frames<'a>(args: &'a [Bytes]) -> Vec<Frame<'a>> {
    args.iter()
        .map(|arg: &'a Bytes| Frame::BulkString(arg.as_ref()))
        .collect()
}

fn response_to_script_value(response: &Response) -> ScriptRespValue {
    match response {
        Response::Simple(value) => ScriptRespValue::Simple(Bytes::copy_from_slice(value)),
        Response::Value(Some(SenkoValue::Raw(value))) => ScriptRespValue::Bulk(Some(value.clone())),
        Response::Value(Some(SenkoValue::Int(value))) => ScriptRespValue::Integer(*value),
        Response::Value(Some(SenkoValue::Float(value))) => {
            ScriptRespValue::Integer(value.trunc() as i64)
        }
        Response::Value(Some(value)) => {
            ScriptRespValue::Bulk(Some(Bytes::copy_from_slice(value.as_bytes().as_ref())))
        }
        Response::Value(None) | Response::NullArray => ScriptRespValue::Bulk(None),
        Response::Integer(value) => ScriptRespValue::Integer(*value),
        Response::Array(values) => {
            ScriptRespValue::Array(values.iter().map(response_to_script_value).collect())
        }
        Response::Map(values) => {
            let mut pairs = Vec::new();
            let mut iter = values.iter();
            while let Some(key) = iter.next() {
                let Some(value) = iter.next() else {
                    break;
                };
                pairs.push((
                    response_to_script_value(key),
                    response_to_script_value(value),
                ));
            }
            ScriptRespValue::Map(pairs)
        }
    }
}

fn library_info_to_script(info: senko_scripting::LibraryInfo) -> ScriptRespValue {
    let mut functions = Vec::new();
    for function in info.functions {
        functions.push(ScriptRespValue::Map(vec![
            (bulk_script(b"name"), bulk_script(function.name.as_bytes())),
            (
                bulk_script(b"description"),
                match function.description {
                    Some(description) => bulk_script(description.as_bytes()),
                    None => ScriptRespValue::Bulk(None),
                },
            ),
            (
                bulk_script(b"flags"),
                ScriptRespValue::Array(
                    function
                        .flags
                        .into_iter()
                        .map(|flag| bulk_script(flag.as_bytes()))
                        .collect(),
                ),
            ),
        ]));
    }
    let mut map = vec![
        (
            bulk_script(b"library_name"),
            bulk_script(info.library_name.as_bytes()),
        ),
        (bulk_script(b"engine"), bulk_script(info.engine.as_bytes())),
        (bulk_script(b"functions"), ScriptRespValue::Array(functions)),
    ];
    if let Some(code) = info.library_code {
        map.push((
            bulk_script(b"library_code"),
            ScriptRespValue::Bulk(Some(code)),
        ));
    }
    ScriptRespValue::Map(map)
}

fn bulk_script(bytes: &[u8]) -> ScriptRespValue {
    ScriptRespValue::Bulk(Some(Bytes::copy_from_slice(bytes)))
}

fn serialize_script_response(value: &ScriptRespValue, resp3: bool) -> Vec<u8> {
    let mut out = BytesMut::new();
    write_script_response(&mut out, value, resp3);
    out.to_vec()
}

fn write_script_response(out: &mut BytesMut, value: &ScriptRespValue, resp3: bool) {
    match value {
        ScriptRespValue::Simple(value) => RespSerializer::write_simple_string(out, value),
        ScriptRespValue::Error(value) => RespSerializer::write_error(out, value),
        ScriptRespValue::Bulk(Some(value)) => RespSerializer::write_bulk_string(out, value),
        ScriptRespValue::Bulk(None) => {
            if resp3 {
                RespSerializer::write_null(out);
            } else {
                RespSerializer::write_nil_bulk(out);
            }
        }
        ScriptRespValue::Integer(value) => RespSerializer::write_integer(out, *value),
        ScriptRespValue::Array(values) => {
            RespSerializer::write_array_header(out, values.len());
            for value in values {
                write_script_response(out, value, resp3);
            }
        }
        ScriptRespValue::Map(values) => {
            if resp3 {
                RespSerializer::write_raw_map_header(out, values.len());
                for (key, value) in values {
                    write_script_response(out, key, resp3);
                    write_script_response(out, value, resp3);
                }
            } else {
                RespSerializer::write_array_header(out, values.len() * 2);
                for (key, value) in values {
                    write_script_response(out, key, resp3);
                    write_script_response(out, value, resp3);
                }
            }
        }
    }
}

fn resp_error_to_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.trim_start_matches('-')
        .trim_end_matches("\r\n")
        .to_owned()
}

fn diagnostics_command(frame: Frame<'_>) -> Option<(Vec<u8>, Vec<Frame<'_>>)> {
    let Frame::Array(aggregate) = frame else {
        return None;
    };
    if aggregate.kind() != AggregateKind::Array || aggregate.is_empty() {
        return None;
    }
    let mut frames = Vec::with_capacity(aggregate.len());
    for item in aggregate.iter() {
        frames.push(item.ok()?);
    }
    let command = command_name(&frames[0]).ok()?.to_vec();
    Some((command, frames[1..].to_vec()))
}

pub(crate) fn dispatch_key_lifecycle_command(
    command: &[u8],
    args: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    resp3: bool,
) -> Result<Option<Vec<u8>>, Vec<u8>> {
    if eq_ascii(command, b"DEL") || eq_ascii(command, b"UNLINK") {
        if args.is_empty() {
            return Err(error_message(if eq_ascii(command, b"DEL") {
                "ERR wrong number of arguments for 'del' command"
            } else {
                "ERR wrong number of arguments for 'unlink' command"
            }));
        }
        let keys = args
            .iter()
            .map(frame_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error_bytes(&error))?;
        let response = {
            let mut store_ref = store.borrow_mut();
            let outcome = generic_keys::delete_keys(&mut store_ref, &keys);
            let watched_keys: Vec<CompactString> = keys
                .iter()
                .filter_map(|key| CompactString::from_utf8(*key).ok())
                .collect();
            notify_keys_written(&watched_keys, &mut store_ref, watch_registry, connections);
            let mut registry = blocked.borrow_mut();
            for key in outcome.deleted_blocking_keys {
                let _ = registry.cancel_waiters(&key);
            }
            Response::Integer(outcome.count as i64)
        };
        return Ok(Some(serialize_response(&response, resp3)));
    }

    if eq_ascii(command, b"RENAME") {
        if args.len() != 2 {
            return Err(error_message(
                "ERR wrong number of arguments for 'rename' command",
            ));
        }
        let source = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
        let destination = parse_compact_key(&args[1])?;
        let source_key = CompactString::from_utf8(source).ok();
        let (response, outcome) = {
            let mut store_ref = store.borrow_mut();
            let outcome = generic_keys::rename_key(&mut store_ref, source, destination, true)
                .map_err(|error| error_bytes(&error))?;
            let mut watched_keys = Vec::new();
            if let Some(source_key) = &source_key {
                watched_keys.push(source_key.clone());
            }
            if let Some(key) = &outcome.destination_blocking_key {
                watched_keys.push(key.clone());
            } else if let Ok(destination_key) = parse_compact_key(&args[1]) {
                watched_keys.push(destination_key);
            }
            notify_keys_written(&watched_keys, &mut store_ref, watch_registry, connections);
            let response = Response::Simple(b"OK");
            let mut registry = blocked.borrow_mut();
            if let Some(key) = &outcome.source_blocking_key {
                let _ = registry.cancel_waiters(key);
            }
            if let Some(key) = &outcome.overwritten_blocking_key {
                let _ = registry.cancel_waiters(key);
            }
            if let Some(key) = &outcome.destination_blocking_key {
                while registry.notify(key, &mut store_ref).is_some() {}
            }
            (response, outcome)
        };
        let _ = outcome;
        return Ok(Some(serialize_response(&response, resp3)));
    }

    if eq_ascii(command, b"RENAMENX") {
        if args.len() != 2 {
            return Err(error_message(
                "ERR wrong number of arguments for 'renamenx' command",
            ));
        }
        let source = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
        let destination = parse_compact_key(&args[1])?;
        let response = {
            let mut store_ref = store.borrow_mut();
            let source_type = store_ref.type_name(source);
            let renamed = generic_keys::rename_nx_key(&mut store_ref, source, destination)
                .map_err(|error| error_bytes(&error))?;
            if renamed {
                let mut watched_keys = Vec::new();
                if let Ok(source_key) = CompactString::from_utf8(source) {
                    watched_keys.push(source_key);
                }
                if let Ok(destination_key) = parse_compact_key(&args[1]) {
                    watched_keys.push(destination_key);
                }
                notify_keys_written(&watched_keys, &mut store_ref, watch_registry, connections);
                if let Some(source_type) = source_type {
                    let mut registry = blocked.borrow_mut();
                    if source_type == b"list" || source_type == b"zset" || source_type == b"stream"
                    {
                        if let Ok(source_key) = CompactString::from_utf8(source) {
                            let _ = registry.cancel_waiters(&source_key);
                        }
                        if let Ok(destination_key) = CompactString::from_utf8(
                            frame_bytes(&args[1]).map_err(|error| error_bytes(&error))?,
                        ) {
                            while registry.notify(&destination_key, &mut store_ref).is_some() {}
                        }
                    }
                }
            }
            Response::Integer(renamed as i64)
        };
        return Ok(Some(serialize_response(&response, resp3)));
    }

    if eq_ascii(command, b"COPY") {
        if args.len() < 2 {
            return Err(error_message(
                "ERR wrong number of arguments for 'copy' command",
            ));
        }
        let source = frame_bytes(&args[0]).map_err(|error| error_bytes(&error))?;
        let destination = parse_compact_key(&args[1])?;
        let (replace, db) = parse_copy_lifecycle_options(&args[2..])?;
        let destination_key = destination.clone();
        let response = {
            let mut store_ref = store.borrow_mut();
            let outcome = generic_keys::copy_key(&mut store_ref, source, destination, replace, db)
                .map_err(|error| error_bytes(&error))?;
            if outcome.is_some() {
                notify_keys_written(
                    std::slice::from_ref(&destination_key),
                    &mut store_ref,
                    watch_registry,
                    connections,
                );
            }
            let mut registry = blocked.borrow_mut();
            if let Some(outcome) = &outcome {
                if let Some(key) = &outcome.overwritten_blocking_key {
                    let _ = registry.cancel_waiters(key);
                }
                if let Some(key) = &outcome.destination_blocking_key {
                    while registry.notify(key, &mut store_ref).is_some() {}
                }
            }
            Response::Integer(outcome.is_some() as i64)
        };
        return Ok(Some(serialize_response(&response, resp3)));
    }

    Ok(None)
}

fn parse_compact_key(frame: &Frame<'_>) -> Result<CompactString, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    CompactString::from_utf8(bytes)
        .map_err(|_| error_bytes(&SenkoError::Protocol("invalid UTF-8 key")))
}

fn parse_copy_lifecycle_options(args: &[Frame<'_>]) -> Result<(bool, u64), Vec<u8>> {
    let mut replace = false;
    let mut db = 0u64;
    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
        if eq_ascii(token, b"REPLACE") {
            replace = true;
            index += 1;
            continue;
        }
        if eq_ascii(token, b"DB") {
            index += 1;
            if index >= args.len() {
                return Err(error_message("ERR syntax error"));
            }
            let raw = frame_bytes(&args[index]).map_err(|error| error_bytes(&error))?;
            db = std::str::from_utf8(raw)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or_else(|| error_message("ERR invalid DB index"))?;
            index += 1;
            continue;
        }
        return Err(error_message("ERR syntax error"));
    }
    Ok((replace, db))
}

fn command_name<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    frame_bytes(frame)
}

pub(crate) fn frame_bytes<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) | Frame::BlobError(bytes) => {
            Ok(bytes)
        }
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::Protocol("command arguments must be strings")),
    }
}

pub(crate) fn serialize_response(response: &Response, resp3: bool) -> Vec<u8> {
    let mut out = BytesMut::new();
    write_response(&mut out, response, resp3);
    out.to_vec()
}

pub(crate) fn write_response(out: &mut BytesMut, response: &Response, resp3: bool) {
    match response {
        Response::Simple(value) => RespSerializer::write_simple_string(out, value),
        Response::Value(Some(value)) => write_value(out, value),
        Response::Value(None) => {
            if resp3 {
                RespSerializer::write_null(out);
            } else {
                RespSerializer::write_nil_bulk(out);
            }
        }
        Response::NullArray => {
            if resp3 {
                RespSerializer::write_null(out);
            } else {
                out.extend_from_slice(b"*-1\r\n");
            }
        }
        Response::Integer(value) => RespSerializer::write_integer(out, *value),
        Response::Array(items) => {
            RespSerializer::write_array_header(out, items.len());
            for item in items.iter() {
                write_response(out, item, resp3);
            }
        }
        Response::Map(items) => {
            if resp3 {
                RespSerializer::write_raw_map_header(out, items.len() / 2);
            } else {
                RespSerializer::write_array_header(out, items.len());
            }
            for item in items.iter() {
                write_response(out, item, resp3);
            }
        }
    }
}

fn write_value(out: &mut BytesMut, value: &SenkoValue) {
    match value {
        SenkoValue::Raw(raw) => RespSerializer::write_bulk_string(out, raw),
        SenkoValue::Int(value) => {
            let rendered = value.to_string();
            RespSerializer::write_bulk_string(out, rendered.as_bytes());
        }
        SenkoValue::Float(value) => {
            let rendered = value.to_string();
            RespSerializer::write_bulk_string(out, rendered.as_bytes());
        }
        SenkoValue::Hash(_) => RespSerializer::write_bulk_string(out, b"[hash]"),
        SenkoValue::List(_) => RespSerializer::write_bulk_string(out, b"[list]"),
        SenkoValue::Set(_) => RespSerializer::write_bulk_string(out, b"[set]"),
        SenkoValue::Stream(_) => RespSerializer::write_bulk_string(out, b"[stream]"),
        SenkoValue::ZSet(_) => RespSerializer::write_bulk_string(out, b"[zset]"),
        #[cfg(feature = "json")]
        SenkoValue::Json(value) => {
            let json = SenkoValue::Json(value.clone());
            let rendered = json.as_bytes();
            RespSerializer::write_bulk_string(out, rendered.as_ref());
        }
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => RespSerializer::write_bulk_string(out, b"[vectorset]"),
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => RespSerializer::write_bulk_string(out, b"[bloom]"),
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => RespSerializer::write_bulk_string(out, b"[cuckoo]"),
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => RespSerializer::write_bulk_string(out, b"[cms]"),
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => RespSerializer::write_bulk_string(out, b"[topk]"),
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => RespSerializer::write_bulk_string(out, b"[tdigest]"),
    }
}

pub(crate) fn simple_string(value: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_simple_string(&mut out, value);
    out.to_vec()
}

pub(crate) fn bulk_string(value: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_bulk_string(&mut out, value);
    out.to_vec()
}

pub(crate) fn error_message(message: &str) -> Vec<u8> {
    let mut out = BytesMut::new();
    RespSerializer::write_error(&mut out, message.as_bytes());
    out.to_vec()
}

pub(crate) fn error_bytes(error: &SenkoError) -> Vec<u8> {
    let message = error_message_text(error);
    error_message(&message)
}

pub(crate) fn error_message_text(error: &SenkoError) -> String {
    let text = match error {
        SenkoError::Protocol(message)
        | SenkoError::Storage(message)
        | SenkoError::InvalidConfig(message) => (*message).to_owned(),
        SenkoError::ProtocolMessage(message) | SenkoError::StorageMessage(message) => {
            message.to_string()
        }
        SenkoError::WrongType { .. } => {
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_owned()
        }
        _ => error.to_string(),
    };

    if text.starts_with("ERR ")
        || text.starts_with("WRONGTYPE")
        || text.starts_with("INVALIDOBJ")
        || text.starts_with("NOPROTO")
        || text.starts_with("NOAUTH")
        || text.starts_with("WRONGPASS")
    {
        text
    } else {
        format!("ERR {text}")
    }
}

fn sync_meta_flags(meta: &mut ConnectionMeta, tx_state: &TxState, state: &ConnectionState) {
    match tx_state {
        TxState::Multi { queue, .. } => {
            meta.flags.insert(ConnectionFlags::MULTI);
            meta.multi_queue_len = queue.len() as i32;
        }
        TxState::None => {
            meta.flags.remove(ConnectionFlags::MULTI);
            meta.multi_queue_len = -1;
        }
    }
    if matches!(state, ConnectionState::Blocked { .. }) {
        meta.flags.insert(ConnectionFlags::BLOCKED);
    } else {
        meta.flags.remove(ConnectionFlags::BLOCKED);
    }
}

fn should_write_response(
    meta: &mut ConnectionMeta,
    pre_reply_mode: ReplyMode,
    force_send_response: bool,
    suppress_response: bool,
) -> bool {
    if force_send_response {
        return true;
    }
    if suppress_response {
        return false;
    }
    match pre_reply_mode {
        ReplyMode::Normal => true,
        ReplyMode::Off => false,
        ReplyMode::Skip => {
            if matches!(meta.reply_mode, ReplyMode::Skip) {
                meta.reply_mode = ReplyMode::Normal;
            }
            false
        }
    }
}

pub(crate) fn current_unix_ms() -> u64 {
    senko_store::store::current_unix_ms()
}

fn current_unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or(0)
}

pub(crate) fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

const UNBALANCED_XREAD_ERROR: &str =
    "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified";

async fn dispatch_cross_shard_blocking_command(
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    shard_id: usize,
    meta: &mut ConnectionMeta,
) -> Result<Option<Response>, ConnectionControl> {
    let mut temp_store = Store::new(config.max_memory);
    if eq_ascii(command, b"BLPOP")
        || eq_ascii(command, b"BRPOP")
        || eq_ascii(command, b"BLMOVE")
        || eq_ascii(command, b"BRPOPLPUSH")
        || eq_ascii(command, b"BLMPOP")
    {
        let parsed = if eq_ascii(command, b"BLPOP") {
            blpop(&mut temp_store, args)
        } else if eq_ascii(command, b"BRPOP") {
            brpop(&mut temp_store, args)
        } else if eq_ascii(command, b"BLMOVE") {
            blmove(&mut temp_store, args)
        } else if eq_ascii(command, b"BRPOPLPUSH") {
            brpoplpush(&mut temp_store, args)
        } else {
            blmpop(&mut temp_store, args)
        }
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let BlockingCommandResult::Block(spec) = parsed else {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        };
        return poll_cross_shard_list_block(spec, config, meta)
            .await
            .map(Some);
    }
    if eq_ascii(command, b"BZPOPMIN")
        || eq_ascii(command, b"BZPOPMAX")
        || eq_ascii(command, b"BZMPOP")
    {
        let parsed = if eq_ascii(command, b"BZPOPMIN") {
            bzpopmin(&mut temp_store, args)
        } else if eq_ascii(command, b"BZPOPMAX") {
            bzpopmax(&mut temp_store, args)
        } else {
            bzmpop(&mut temp_store, args)
        }
        .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let ZBlockingCommandResult::Block(spec) = parsed else {
            return Err(ConnectionControl::Continue(error_message(
                "ERR shard coordination protocol error",
            )));
        };
        return poll_cross_shard_zset_block(spec, config, meta)
            .await
            .map(Some);
    }
    if eq_ascii(command, b"XREAD") {
        let stream_keys = collect_xread_keys(args)?;
        if let Some(target_shard) = target_shard_for_byte_keys(&stream_keys, config.num_shards) {
            if target_shard == shard_id {
                return Ok(None);
            }
            let routed_args = collect_frame_args(args)?;
            let reply = server_info::execute_store_command_on_shard(
                target_shard,
                Bytes::copy_from_slice(command),
                routed_args,
                meta.resp_version == 3,
                meta.no_touch,
                meta.id,
            )
            .await
            .map_err(ConnectionControl::Continue)?;
            return parse_response(&reply.response)
                .map(Some)
                .map_err(ConnectionControl::Continue);
        }
        let mut temp_store = hydrate_temp_store_from_keys(&stream_keys, config, meta).await?;
        let parsed = xread(&mut temp_store, args)
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        return match parsed {
            StreamBlockingCommandResult::Immediate(_) => {
                execute_grouped_xread(meta, &strip_block_option(args)?, config.num_shards)
                    .await
                    .map(Some)
            }
            StreamBlockingCommandResult::Block(spec) => {
                poll_cross_shard_xread_block(spec, config, meta)
                    .await
                    .map(Some)
            }
        };
    }
    if eq_ascii(command, b"XREADGROUP") {
        let stream_keys = collect_xreadgroup_keys(args)?;
        if let Some(target_shard) = target_shard_for_byte_keys(&stream_keys, config.num_shards) {
            if target_shard == shard_id {
                return Ok(None);
            }
            let routed_args = collect_frame_args(args)?;
            let reply = server_info::execute_store_command_on_shard(
                target_shard,
                Bytes::copy_from_slice(command),
                routed_args,
                meta.resp_version == 3,
                meta.no_touch,
                meta.id,
            )
            .await
            .map_err(ConnectionControl::Continue)?;
            if let Some(offset) = reply.replication_offset {
                meta.last_write_replication_offset = offset;
            }
            return parse_response(&reply.response)
                .map(Some)
                .map_err(ConnectionControl::Continue);
        }
        let mut temp_store = hydrate_temp_store_from_keys(&stream_keys, config, meta).await?;
        let parsed = xreadgroup(&mut temp_store, args)
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        return match parsed {
            StreamGroupBlockingCommandResult::Immediate(_) => {
                execute_grouped_xreadgroup(meta, &strip_block_option(args)?, config.num_shards)
                    .await
                    .map(Some)
            }
            StreamGroupBlockingCommandResult::Block(spec) => {
                poll_cross_shard_xreadgroup_block(spec, config, meta)
                    .await
                    .map(Some)
            }
        };
    }
    Ok(None)
}

async fn poll_cross_shard_list_block(
    spec: BlockSpec,
    config: &SenkoConfig,
    meta: &mut ConnectionMeta,
) -> Result<Response, ConnectionControl> {
    if let StoreBlockingOp::Move { dest, .. } | StoreBlockingOp::MoveDeprecated { dest } = &spec.op
    {
        ensure_remote_type(
            shard_for_key(dest.as_bytes(), config.num_shards),
            &Bytes::copy_from_slice(dest.as_bytes()),
            b"list",
            meta,
        )
        .await?;
    }
    let deadline = spec.timeout.map(|timeout| Instant::now() + timeout);
    loop {
        match &spec.op {
            StoreBlockingOp::Pop { direction } => {
                let pop_command = if *direction == BlockSpecDirection::Left {
                    b"LPOP".as_slice()
                } else {
                    b"RPOP".as_slice()
                };
                for key in &spec.keys {
                    let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                    let reply = server_info::execute_store_command_on_shard(
                        shard_for_key(key.as_bytes(), config.num_shards),
                        Bytes::copy_from_slice(pop_command),
                        vec![key_bytes.clone()],
                        meta.resp_version == 3,
                        meta.no_touch,
                        meta.id,
                    )
                    .await
                    .map_err(ConnectionControl::Continue)?;
                    let Some(value) = parse_optional_bytes_response(&reply.response)
                        .map_err(ConnectionControl::Continue)?
                    else {
                        continue;
                    };
                    if let Some(offset) = reply.replication_offset {
                        meta.last_write_replication_offset = offset;
                    }
                    return Ok(Response::Array(Box::new(smallvec::smallvec![
                        Response::Value(Some(SenkoValue::Raw(key_bytes))),
                        Response::Value(Some(SenkoValue::Raw(Bytes::from(value)))),
                    ])));
                }
            }
            StoreBlockingOp::Move {
                dest,
                src_dir,
                dst_dir,
            } => {
                let source = spec.keys.first().expect("blocking source key");
                let command_args = [
                    Frame::BulkString(source.as_bytes()),
                    Frame::BulkString(dest.as_bytes()),
                    Frame::BulkString(if *src_dir == BlockSpecDirection::Left {
                        b"LEFT"
                    } else {
                        b"RIGHT"
                    }),
                    Frame::BulkString(if *dst_dir == BlockSpecDirection::Left {
                        b"LEFT"
                    } else {
                        b"RIGHT"
                    }),
                ];
                let (bytes, _, _, _) =
                    dispatch_cross_shard_list_move(meta, b"LMOVE", &command_args, config).await?;
                let response = parse_response(&bytes).map_err(ConnectionControl::Continue)?;
                if !matches!(response, Response::Value(None)) {
                    return Ok(response);
                }
            }
            StoreBlockingOp::MoveDeprecated { dest } => {
                let source = spec.keys.first().expect("blocking source key");
                let command_args = [
                    Frame::BulkString(source.as_bytes()),
                    Frame::BulkString(dest.as_bytes()),
                ];
                let (bytes, _, _, _) =
                    dispatch_cross_shard_list_move(meta, b"RPOPLPUSH", &command_args, config)
                        .await?;
                let response = parse_response(&bytes).map_err(ConnectionControl::Continue)?;
                if !matches!(response, Response::Value(None)) {
                    return Ok(response);
                }
            }
            StoreBlockingOp::MPop { direction, count } => {
                let side = if *direction == BlockSpecDirection::Left {
                    b"LEFT".as_slice()
                } else {
                    b"RIGHT".as_slice()
                };
                for key in &spec.keys {
                    let key_bytes = Bytes::copy_from_slice(key.as_bytes());
                    let reply = server_info::execute_store_command_on_shard(
                        shard_for_key(key.as_bytes(), config.num_shards),
                        Bytes::copy_from_slice(b"LMPOP"),
                        vec![
                            Bytes::from_static(b"1"),
                            key_bytes,
                            Bytes::copy_from_slice(side),
                            Bytes::from_static(b"COUNT"),
                            Bytes::from(count.to_string()),
                        ],
                        meta.resp_version == 3,
                        meta.no_touch,
                        meta.id,
                    )
                    .await
                    .map_err(ConnectionControl::Continue)?;
                    if is_null_response(&reply.response).map_err(ConnectionControl::Continue)? {
                        continue;
                    }
                    if let Some(offset) = reply.replication_offset {
                        meta.last_write_replication_offset = offset;
                    }
                    return parse_response(&reply.response).map_err(ConnectionControl::Continue);
                }
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(match spec.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            });
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn poll_cross_shard_zset_block(
    spec: ZBlockSpec,
    config: &SenkoConfig,
    meta: &mut ConnectionMeta,
) -> Result<Response, ConnectionControl> {
    let deadline = spec.timeout.map(|timeout| Instant::now() + timeout);
    loop {
        match spec.op {
            StoreZBlockingOp::ZPop { direction } => {
                let side = if direction == ZBlockSpecDirection::Min {
                    b"MIN".as_slice()
                } else {
                    b"MAX".as_slice()
                };
                for key in &spec.keys {
                    let reply = server_info::execute_store_command_on_shard(
                        shard_for_key(key.as_bytes(), config.num_shards),
                        Bytes::copy_from_slice(b"ZMPOP"),
                        vec![
                            Bytes::from_static(b"1"),
                            Bytes::copy_from_slice(key.as_bytes()),
                            Bytes::copy_from_slice(side),
                            Bytes::from_static(b"COUNT"),
                            Bytes::from_static(b"1"),
                        ],
                        meta.resp_version == 3,
                        meta.no_touch,
                        meta.id,
                    )
                    .await
                    .map_err(ConnectionControl::Continue)?;
                    if is_null_response(&reply.response).map_err(ConnectionControl::Continue)? {
                        continue;
                    }
                    if let Some(offset) = reply.replication_offset {
                        meta.last_write_replication_offset = offset;
                    }
                    let value =
                        parse_response(&reply.response).map_err(ConnectionControl::Continue)?;
                    return flatten_zmpop_single_response(value);
                }
            }
            StoreZBlockingOp::ZMPop { direction, count } => {
                let side = if direction == ZBlockSpecDirection::Min {
                    b"MIN".as_slice()
                } else {
                    b"MAX".as_slice()
                };
                for key in &spec.keys {
                    let reply = server_info::execute_store_command_on_shard(
                        shard_for_key(key.as_bytes(), config.num_shards),
                        Bytes::copy_from_slice(b"ZMPOP"),
                        vec![
                            Bytes::from_static(b"1"),
                            Bytes::copy_from_slice(key.as_bytes()),
                            Bytes::copy_from_slice(side),
                            Bytes::from_static(b"COUNT"),
                            Bytes::from(count.to_string()),
                        ],
                        meta.resp_version == 3,
                        meta.no_touch,
                        meta.id,
                    )
                    .await
                    .map_err(ConnectionControl::Continue)?;
                    if is_null_response(&reply.response).map_err(ConnectionControl::Continue)? {
                        continue;
                    }
                    if let Some(offset) = reply.replication_offset {
                        meta.last_write_replication_offset = offset;
                    }
                    return parse_response(&reply.response).map_err(ConnectionControl::Continue);
                }
            }
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(match spec.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            });
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn poll_cross_shard_xread_block(
    spec: XReadBlockSpec,
    config: &SenkoConfig,
    meta: &mut ConnectionMeta,
) -> Result<Response, ConnectionControl> {
    let deadline = spec.timeout.map(|timeout| Instant::now() + timeout);
    let prefix = build_xread_prefix(spec.count);
    loop {
        let response = execute_grouped_stream_read_resolved(
            meta,
            b"XREAD",
            &prefix,
            &spec.streams,
            config.num_shards,
        )
        .await?;
        if !matches!(response, Response::Value(None)) {
            return Ok(response);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(match spec.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            });
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn poll_cross_shard_xreadgroup_block(
    spec: XReadGroupBlockSpec,
    config: &SenkoConfig,
    meta: &mut ConnectionMeta,
) -> Result<Response, ConnectionControl> {
    let deadline = spec.timeout.map(|timeout| Instant::now() + timeout);
    let prefix = build_xreadgroup_prefix(&spec);
    loop {
        let response = execute_grouped_stream_read_resolved(
            meta,
            b"XREADGROUP",
            &prefix,
            &spec.streams,
            config.num_shards,
        )
        .await?;
        if !matches!(response, Response::Value(None)) {
            return Ok(response);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(match spec.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            });
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn execute_grouped_xread(
    meta: &mut ConnectionMeta,
    args: &[Bytes],
    num_shards: usize,
) -> Result<Response, ConnectionControl> {
    execute_grouped_stream_read(meta, b"XREAD", args, num_shards).await
}

async fn execute_grouped_xreadgroup(
    meta: &mut ConnectionMeta,
    args: &[Bytes],
    num_shards: usize,
) -> Result<Response, ConnectionControl> {
    execute_grouped_stream_read(meta, b"XREADGROUP", args, num_shards).await
}

async fn execute_grouped_stream_read(
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Bytes],
    num_shards: usize,
) -> Result<Response, ConnectionControl> {
    let grouped = group_stream_read_args(args, num_shards)?;
    execute_grouped_stream_read_requests(meta, command, grouped).await
}

async fn execute_grouped_stream_read_resolved(
    meta: &mut ConnectionMeta,
    command: &[u8],
    prefix: &[Bytes],
    streams: &[(CompactString, senko_core::StreamId)],
    num_shards: usize,
) -> Result<Response, ConnectionControl> {
    let grouped = group_resolved_stream_read_args(prefix, streams, num_shards);
    execute_grouped_stream_read_requests(meta, command, grouped).await
}

async fn execute_grouped_stream_read_requests(
    meta: &mut ConnectionMeta,
    command: &[u8],
    grouped: Vec<GroupedStreamRead>,
) -> Result<Response, ConnectionControl> {
    let order = grouped
        .iter()
        .flat_map(|group| group.streams.iter().map(|(key, _)| key.clone()))
        .collect::<Vec<_>>();
    let mut responses = Vec::with_capacity(grouped.len());
    let mut max_offset = None;
    for group in grouped {
        let reply = server_info::execute_store_command_on_shard(
            group.shard_id,
            Bytes::copy_from_slice(command),
            group.args,
            meta.resp_version == 3,
            meta.no_touch,
            meta.id,
        )
        .await
        .map_err(ConnectionControl::Continue)?;
        responses.push(parse_response(&reply.response).map_err(ConnectionControl::Continue)?);
        max_offset = max_offset.max(reply.replication_offset);
    }
    if eq_ascii(command, b"XREADGROUP")
        && let Some(offset) = max_offset
    {
        meta.last_write_replication_offset = offset;
    }
    merge_stream_read_responses(&order, responses)
}

fn build_xread_prefix(count: Option<usize>) -> Vec<Bytes> {
    let mut prefix = Vec::new();
    if let Some(count) = count {
        prefix.push(Bytes::from_static(b"COUNT"));
        prefix.push(Bytes::from(count.to_string()));
    }
    prefix
}

fn build_xreadgroup_prefix(spec: &XReadGroupBlockSpec) -> Vec<Bytes> {
    let mut prefix = vec![
        Bytes::from_static(b"GROUP"),
        Bytes::copy_from_slice(spec.group.as_bytes()),
        Bytes::copy_from_slice(spec.consumer.as_bytes()),
    ];
    if let Some(count) = spec.count {
        prefix.push(Bytes::from_static(b"COUNT"));
        prefix.push(Bytes::from(count.to_string()));
    }
    if spec.noack {
        prefix.push(Bytes::from_static(b"NOACK"));
    }
    prefix
}

#[derive(Clone)]
struct GroupedStreamRead {
    shard_id: usize,
    args: Vec<Bytes>,
    streams: Vec<(Bytes, Bytes)>,
}

fn group_stream_read_args(
    args: &[Bytes],
    num_shards: usize,
) -> Result<Vec<GroupedStreamRead>, ConnectionControl> {
    let (prefix, streams) = split_stream_read_args(args)?;
    Ok(group_resolved_stream_read_bytes(
        prefix, streams, num_shards,
    ))
}

fn group_resolved_stream_read_args(
    prefix: &[Bytes],
    streams: &[(CompactString, senko_core::StreamId)],
    num_shards: usize,
) -> Vec<GroupedStreamRead> {
    let stream_bytes = streams
        .iter()
        .map(|(key, id)| {
            let id = id.to_string();
            (
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(id.as_bytes()),
            )
        })
        .collect::<Vec<_>>();
    group_resolved_stream_read_bytes(prefix.to_vec(), stream_bytes, num_shards)
}

fn group_resolved_stream_read_bytes(
    prefix: Vec<Bytes>,
    streams: Vec<(Bytes, Bytes)>,
    num_shards: usize,
) -> Vec<GroupedStreamRead> {
    let mut grouped = std::collections::BTreeMap::<usize, Vec<(Bytes, Bytes)>>::new();
    for (key, id) in streams {
        grouped
            .entry(shard_for_key(key.as_ref(), num_shards))
            .or_default()
            .push((key, id));
    }
    grouped
        .into_iter()
        .map(|(shard_id, streams)| {
            let mut args = prefix.clone();
            args.push(Bytes::from_static(b"STREAMS"));
            args.extend(streams.iter().map(|(key, _)| key.clone()));
            args.extend(streams.iter().map(|(_, id)| id.clone()));
            GroupedStreamRead {
                shard_id,
                args,
                streams,
            }
        })
        .collect()
}

fn split_stream_read_args(
    args: &[Bytes],
) -> Result<(Vec<Bytes>, Vec<(Bytes, Bytes)>), ConnectionControl> {
    let stream_index = args
        .iter()
        .position(|arg| arg.eq_ignore_ascii_case(b"STREAMS"))
        .ok_or_else(|| ConnectionControl::Continue(error_message("ERR syntax error")))?;
    let prefix = args[..stream_index].to_vec();
    let remaining = &args[stream_index + 1..];
    if remaining.len() < 2 || !remaining.len().is_multiple_of(2) {
        return Err(ConnectionControl::Continue(error_message(
            UNBALANCED_XREAD_ERROR,
        )));
    }
    let half = remaining.len() / 2;
    Ok((
        prefix,
        remaining[..half]
            .iter()
            .cloned()
            .zip(remaining[half..].iter().cloned())
            .collect(),
    ))
}

fn merge_stream_read_responses(
    order: &[Bytes],
    responses: Vec<Response>,
) -> Result<Response, ConnectionControl> {
    let mut merged = HashMap::<Vec<u8>, Response, RandomState>::with_hasher(RandomState::default());
    for response in responses {
        match response {
            Response::Value(None) | Response::NullArray => {}
            Response::Array(items) => {
                for item in items.into_vec() {
                    let key = stream_response_key(&item)?.to_vec();
                    merged.insert(key, item);
                }
            }
            _ => {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR shard coordination protocol error",
                )));
            }
        }
    }
    let mut out = smallvec::SmallVec::<[Response; 16]>::new();
    for key in order {
        if let Some(item) = merged.remove(key.as_ref()) {
            out.push(item);
        }
    }
    if out.is_empty() {
        Ok(Response::Value(None))
    } else {
        Ok(Response::Array(Box::new(out)))
    }
}

fn stream_response_key(response: &Response) -> Result<&[u8], ConnectionControl> {
    let Response::Array(items) = response else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    };
    let Some(Response::Value(Some(SenkoValue::Raw(key)))) = items.first() else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    };
    Ok(key.as_ref())
}

fn flatten_zmpop_single_response(response: Response) -> Result<Response, ConnectionControl> {
    let Response::Array(items) = response else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    };
    if items.len() == 3 {
        return Ok(Response::Array(items));
    }
    if items.len() != 2 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    let key = items[0].clone();
    let Response::Array(entries) = &items[1] else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    };
    let Some(Response::Array(entry)) = entries.first() else {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    };
    if entry.len() != 2 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR shard coordination protocol error",
        )));
    }
    Ok(Response::Array(Box::new(smallvec::smallvec![
        key,
        entry[0].clone(),
        entry[1].clone(),
    ])))
}

fn strip_block_option(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    let mut out = Vec::with_capacity(args.len());
    let mut index = 0usize;
    while index < args.len() {
        let bytes = frame_bytes(&args[index])
            .map(Bytes::copy_from_slice)
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if bytes.eq_ignore_ascii_case(b"BLOCK") {
            index += 2;
            continue;
        }
        out.push(bytes);
        index += 1;
    }
    Ok(out)
}

fn collect_xread_keys(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if token.eq_ignore_ascii_case(b"COUNT") || token.eq_ignore_ascii_case(b"BLOCK") {
            index += 2;
            continue;
        }
        break;
    }
    collect_stream_keys_after_token(args, index)
}

fn collect_xreadgroup_keys(args: &[Frame<'_>]) -> Result<Vec<Bytes>, ConnectionControl> {
    if args.len() < 4 {
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    }
    let mut index = 3usize;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if token.eq_ignore_ascii_case(b"COUNT")
            || token.eq_ignore_ascii_case(b"BLOCK")
            || token.eq_ignore_ascii_case(b"CLAIM")
        {
            index += 2;
            continue;
        }
        if token.eq_ignore_ascii_case(b"NOACK") {
            index += 1;
            continue;
        }
        break;
    }
    collect_stream_keys_after_token(args, index)
}

fn collect_stream_keys_after_token(
    args: &[Frame<'_>],
    token_index: usize,
) -> Result<Vec<Bytes>, ConnectionControl> {
    let token = args
        .get(token_index)
        .ok_or_else(|| ConnectionControl::Continue(error_message("ERR syntax error")))?;
    let token =
        frame_bytes(token).map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    if !token.eq_ignore_ascii_case(b"STREAMS") {
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
    }
    let remaining = &args[token_index + 1..];
    if remaining.len() < 2 || !remaining.len().is_multiple_of(2) {
        return Err(ConnectionControl::Continue(error_message(
            UNBALANCED_XREAD_ERROR,
        )));
    }
    remaining[..remaining.len() / 2]
        .iter()
        .map(|frame| {
            frame_bytes(frame)
                .map(Bytes::copy_from_slice)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))
        })
        .collect()
}

async fn dispatch_blocking_command(
    conn_id: u64,
    command: &[u8],
    args: &[Frame<'_>],
    config: &SenkoConfig,
    shard_id: usize,
    store: &Rc<RefCell<Store>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
) -> Result<Option<Response>, ConnectionControl> {
    if config.num_shards > 1
        && let Some(response) =
            dispatch_cross_shard_blocking_command(command, args, config, shard_id, meta).await?
    {
        return Ok(Some(response));
    }

    let list_blocked_result = {
        let mut store_ref = store.borrow_mut();
        if eq_ascii(command, b"BLPOP") {
            Some(blpop(&mut store_ref, args))
        } else if eq_ascii(command, b"BRPOP") {
            Some(brpop(&mut store_ref, args))
        } else if eq_ascii(command, b"BLMOVE") {
            Some(blmove(&mut store_ref, args))
        } else if eq_ascii(command, b"BRPOPLPUSH") {
            Some(brpoplpush(&mut store_ref, args))
        } else if eq_ascii(command, b"BLMPOP") {
            Some(blmpop(&mut store_ref, args))
        } else {
            None
        }
    };
    if let Some(result) = list_blocked_result {
        let result = result.map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        return match result {
            BlockingCommandResult::Immediate(response) => {
                let mut store_ref = store.borrow_mut();
                post_dispatch_notify(
                    command,
                    args,
                    &response,
                    &mut store_ref,
                    blocked,
                    watch_registry,
                    connections,
                );
                Ok(Some(response))
            }
            BlockingCommandResult::Block(spec) => {
                let response = await_blocked_list(conn_id, spec, blocked, state, meta).await;
                match response {
                    Ok(response) => Ok(Some(response)),
                    Err(error) => Err(ConnectionControl::Continue(error_bytes(&error))),
                }
            }
        };
    }

    let zset_blocked_result = {
        let mut store_ref = store.borrow_mut();
        if eq_ascii(command, b"BZPOPMIN") {
            Some(bzpopmin(&mut store_ref, args))
        } else if eq_ascii(command, b"BZPOPMAX") {
            Some(bzpopmax(&mut store_ref, args))
        } else if eq_ascii(command, b"BZMPOP") {
            Some(bzmpop(&mut store_ref, args))
        } else {
            None
        }
    };
    if let Some(result) = zset_blocked_result {
        let result = result.map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        return match result {
            ZBlockingCommandResult::Immediate(response) => Ok(Some(response)),
            ZBlockingCommandResult::Block(spec) => {
                let response = await_blocked_zset(conn_id, spec, blocked, state, meta).await;
                match response {
                    Ok(response) => Ok(Some(response)),
                    Err(error) => Err(ConnectionControl::Continue(error_bytes(&error))),
                }
            }
        };
    }

    let stream_read_result = {
        let mut store_ref = store.borrow_mut();
        if eq_ascii(command, b"XREAD") {
            Some(xread(&mut store_ref, args))
        } else {
            None
        }
    };
    if let Some(result) = stream_read_result {
        let result = result.map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        return match result {
            StreamBlockingCommandResult::Immediate(response) => Ok(Some(response)),
            StreamBlockingCommandResult::Block(spec) => {
                let response = await_blocked(
                    conn_id,
                    spec.keys,
                    spec.timeout,
                    BlockedOp::XRead {
                        streams: spec.streams,
                        count: spec.count,
                    },
                    spec.timeout_response,
                    blocked,
                    state,
                    meta,
                )
                .await;
                match response {
                    Ok(response) => Ok(Some(response)),
                    Err(error) => Err(ConnectionControl::Continue(error_bytes(&error))),
                }
            }
        };
    }

    let stream_group_result = {
        let mut store_ref = store.borrow_mut();
        if eq_ascii(command, b"XREADGROUP") {
            Some(xreadgroup(&mut store_ref, args))
        } else {
            None
        }
    };
    let Some(result) = stream_group_result else {
        return Ok(None);
    };
    let result = result.map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    match result {
        StreamGroupBlockingCommandResult::Immediate(response) => Ok(Some(response)),
        StreamGroupBlockingCommandResult::Block(spec) => {
            let response = await_blocked(
                conn_id,
                spec.keys,
                spec.timeout,
                BlockedOp::XReadGroup {
                    streams: spec.streams,
                    group: spec.group,
                    consumer: spec.consumer,
                    count: spec.count,
                    noack: spec.noack,
                },
                spec.timeout_response,
                blocked,
                state,
                meta,
            )
            .await;
            match response {
                Ok(response) => Ok(Some(response)),
                Err(error) => Err(ConnectionControl::Continue(error_bytes(&error))),
            }
        }
    }
}

async fn await_blocked_list(
    conn_id: u64,
    spec: BlockSpec,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    state: &mut ConnectionState,
    meta: &mut ConnectionMeta,
) -> Result<Response, SenkoError> {
    await_blocked(
        conn_id,
        spec.keys,
        spec.timeout,
        BlockedOp::from(spec.op),
        spec.timeout_response,
        blocked,
        state,
        meta,
    )
    .await
}

async fn await_blocked_zset(
    conn_id: u64,
    spec: ZBlockSpec,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    state: &mut ConnectionState,
    meta: &mut ConnectionMeta,
) -> Result<Response, SenkoError> {
    await_blocked(
        conn_id,
        spec.keys,
        spec.timeout,
        BlockedOp::from(spec.op),
        spec.timeout_response,
        blocked,
        state,
        meta,
    )
    .await
}

async fn await_blocked(
    conn_id: u64,
    keys: smallvec::SmallVec<[CompactString; 4]>,
    timeout: Option<Duration>,
    op: BlockedOp,
    timeout_response: senko_store::commands::list::blocking::BlockingResponseKind,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    state: &mut ConnectionState,
    meta: &mut ConnectionMeta,
) -> Result<Response, SenkoError> {
    let deadline = timeout.map(|timeout| Instant::now() + timeout);
    *state = ConnectionState::Blocked {
        keys: keys.clone(),
        deadline,
        pending_response: None,
    };
    meta.flags.insert(ConnectionFlags::BLOCKED);
    let mut registered = false;
    let response = poll_fn(|cx| {
        let mut registry = blocked.borrow_mut();
        if let Some(ready) = registry.take_ready(conn_id) {
            return std::task::Poll::Ready(ready);
        }
        if !registered {
            registry.register(BlockedClient {
                conn_id,
                keys: keys.clone(),
                deadline,
                waker: cx.waker().clone(),
                op: op.clone(),
                timeout_response: timeout_response.clone(),
            });
            registered = true;
        } else {
            registry.refresh_waker(conn_id, cx.waker());
        }
        std::task::Poll::Pending
    })
    .await?;
    *state = ConnectionState::Reading;
    meta.flags.remove(ConnectionFlags::BLOCKED);
    Ok(response)
}

async fn await_pause(conn_id: u64, pause_state: &Rc<RefCell<PauseState>>) {
    poll_fn(|cx| {
        let mut pause = pause_state.borrow_mut();
        if pause.paused_until.is_none() {
            return std::task::Poll::Ready(());
        }
        pause.register(conn_id, cx.waker());
        std::task::Poll::Pending
    })
    .await
}

pub(crate) fn post_dispatch_notify(
    command: &[u8],
    args: &[Frame<'_>],
    response: &Response,
    store: &mut Store,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
) {
    let keys = notification_keys(command, args, response);
    notify_keys_written(&keys, store, watch_registry, connections);
    let Some(key) = keys.first().cloned() else {
        return;
    };
    let mut registry = blocked.borrow_mut();
    if eq_ascii(command, b"XADD")
        && let Response::Value(Some(SenkoValue::Raw(id_bytes))) = response
        && let Ok(new_id) = senko_core::StreamId::parse(id_bytes.as_ref())
    {
        let _ = registry.notify_stream(&key, new_id, store);
        return;
    }
    while registry.notify(&key, store).is_some() {}
}

#[allow(clippy::too_many_arguments)]
fn apply_store_write_side_effects(
    shard_id: usize,
    meta: &mut ConnectionMeta,
    command: &[u8],
    args: &[Frame<'_>],
    response: &Response,
    store: &Rc<RefCell<Store>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    client_connections: &Rc<RefCell<ClientConnectionMap>>,
    tracking_registry: &Rc<RefCell<TrackingRegistry>>,
) {
    {
        let mut store_ref = store.borrow_mut();
        let restore_no_touch = store_ref.no_touch();
        store_ref.set_no_touch(meta.no_touch);
        post_dispatch_notify(
            command,
            args,
            response,
            &mut store_ref,
            blocked,
            watch_registry,
            connections,
        );
        store_ref.set_no_touch(restore_no_touch);
    }
    let keys = notification_keys(command, args, response);
    client_ops::invalidate_written_keys(&keys, meta.id, tracking_registry, client_connections);
    if should_replicate_store_command(command) {
        server_replication::record_write(shard_id, meta, command, args);
    }
}

fn notify_keys_written(
    keys: &[CompactString],
    store: &mut Store,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
) {
    if keys.is_empty() {
        return;
    }
    let mut registry = watch_registry.borrow_mut();
    let mut connections = connections.borrow_mut();
    for key in keys {
        let version = store.notify_watchers(key.as_bytes());
        registry.notify_write(key, version, &mut connections);
    }
}

pub(crate) fn notification_keys(
    command: &[u8],
    args: &[Frame<'_>],
    response: &Response,
) -> Vec<CompactString> {
    if matches!(response, Response::Integer(value) if *value <= 0) {
        return Vec::new();
    }
    if eq_ascii(command, b"ZADD")
        && matches!(response, Response::Value(None) | Response::Integer(0))
    {
        return Vec::new();
    }
    if eq_ascii(command, b"BLPOP")
        || eq_ascii(command, b"BRPOP")
        || eq_ascii(command, b"BLMPOP")
        || eq_ascii(command, b"BZPOPMIN")
        || eq_ascii(command, b"BZPOPMAX")
    {
        return blocking_response_keys(response);
    }
    let key_indexes: &[usize] = if eq_ascii(command, b"LMOVE")
        || eq_ascii(command, b"RPOPLPUSH")
        || eq_ascii(command, b"BLMOVE")
        || eq_ascii(command, b"BRPOPLPUSH")
        || eq_ascii(command, b"SMOVE")
    {
        &[0, 1]
    } else if eq_ascii(command, b"MSET")
        || eq_ascii(command, b"MSETNX")
        || eq_ascii(command, b"MSETEX")
    {
        &[]
    } else if eq_ascii(command, b"LPUSH")
        || eq_ascii(command, b"RPUSH")
        || eq_ascii(command, b"LPUSHX")
        || eq_ascii(command, b"RPUSHX")
        || eq_ascii(command, b"LPOP")
        || eq_ascii(command, b"RPOP")
        || eq_ascii(command, b"LREM")
        || eq_ascii(command, b"LTRIM")
        || eq_ascii(command, b"LSET")
        || eq_ascii(command, b"LINSERT")
        || eq_ascii(command, b"SET")
        || eq_ascii(command, b"SETEX")
        || eq_ascii(command, b"PSETEX")
        || eq_ascii(command, b"SETNX")
        || eq_ascii(command, b"APPEND")
        || eq_ascii(command, b"SETRANGE")
        || eq_ascii(command, b"GETSET")
        || eq_ascii(command, b"GETDEL")
        || eq_ascii(command, b"GETEX")
        || eq_ascii(command, b"INCR")
        || eq_ascii(command, b"INCRBY")
        || eq_ascii(command, b"INCRBYFLOAT")
        || eq_ascii(command, b"DECR")
        || eq_ascii(command, b"DECRBY")
        || eq_ascii(command, b"EXPIRE")
        || eq_ascii(command, b"PEXPIRE")
        || eq_ascii(command, b"EXPIREAT")
        || eq_ascii(command, b"PEXPIREAT")
        || eq_ascii(command, b"PERSIST")
        || eq_ascii(command, b"HSET")
        || eq_ascii(command, b"HMSET")
        || eq_ascii(command, b"HSETEX")
        || eq_ascii(command, b"HSETNX")
        || eq_ascii(command, b"HDEL")
        || eq_ascii(command, b"HINCRBY")
        || eq_ascii(command, b"HINCRBYFLOAT")
        || eq_ascii(command, b"HEXPIRE")
        || eq_ascii(command, b"HEXPIREAT")
        || eq_ascii(command, b"HPEXPIRE")
        || eq_ascii(command, b"HPEXPIREAT")
        || eq_ascii(command, b"HPERSIST")
        || eq_ascii(command, b"HGETDEL")
        || eq_ascii(command, b"HGETEX")
        || eq_ascii(command, b"SADD")
        || eq_ascii(command, b"SREM")
        || eq_ascii(command, b"SPOP")
        || eq_ascii(command, b"SDIFFSTORE")
        || eq_ascii(command, b"SINTERSTORE")
        || eq_ascii(command, b"SUNIONSTORE")
        || eq_ascii(command, b"ZADD")
        || eq_ascii(command, b"ZINCRBY")
        || eq_ascii(command, b"ZREM")
        || eq_ascii(command, b"ZREMRANGEBYLEX")
        || eq_ascii(command, b"ZREMRANGEBYRANK")
        || eq_ascii(command, b"ZREMRANGEBYSCORE")
        || eq_ascii(command, b"ZPOPMIN")
        || eq_ascii(command, b"ZPOPMAX")
        || eq_ascii(command, b"ZRANGESTORE")
        || eq_ascii(command, b"ZUNIONSTORE")
        || eq_ascii(command, b"ZINTERSTORE")
        || eq_ascii(command, b"ZDIFFSTORE")
        || eq_ascii(command, b"XADD")
        || eq_ascii(command, b"XDEL")
        || eq_ascii(command, b"XDELEX")
        || eq_ascii(command, b"XSETID")
        || eq_ascii(command, b"XTRIM")
    {
        &[0]
    } else {
        &[]
    };
    if eq_ascii(command, b"MSET") || eq_ascii(command, b"MSETNX") || eq_ascii(command, b"MSETEX") {
        return args
            .chunks(2)
            .filter_map(|chunk| chunk.first())
            .filter_map(|frame| frame_bytes(frame).ok())
            .filter_map(|bytes| CompactString::from_utf8(bytes).ok())
            .collect();
    }
    key_indexes
        .iter()
        .filter_map(|index| args.get(*index))
        .filter_map(|frame| frame_bytes(frame).ok())
        .filter_map(|bytes| CompactString::from_utf8(bytes).ok())
        .collect()
}

fn blocking_response_keys(response: &Response) -> Vec<CompactString> {
    match response {
        Response::Array(items) => items
            .first()
            .and_then(response_bulk_bytes)
            .and_then(|bytes| CompactString::from_utf8(bytes).ok())
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

fn response_bulk_bytes(response: &Response) -> Option<&[u8]> {
    match response {
        Response::Value(Some(SenkoValue::Raw(bytes))) => Some(bytes.as_ref()),
        Response::Simple(bytes) => Some(bytes),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use smallvec::smallvec;

    use super::{blocking_response_keys, notification_keys};
    use senko_core::SenkoValue;
    use senko_store::Response;

    #[test]
    fn blocking_response_keys_extracts_actual_list_key() {
        let response = Response::Array(Box::new(smallvec![
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"jobs")))),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"item-1")))),
        ]));

        let keys = blocking_response_keys(&response);

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_str(), "jobs");
    }

    #[test]
    fn notification_keys_uses_blocking_response_key_for_blpop() {
        let response = Response::Array(Box::new(smallvec![
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"queue:2")))),
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"payload")))),
        ]));

        let keys = notification_keys(b"BLPOP", &[], &response);

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_str(), "queue:2");
    }
}
