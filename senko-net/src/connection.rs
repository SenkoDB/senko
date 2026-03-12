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
        BlockSpec, BlockingCommandResult, blmove, blmpop, blpop, brpop, brpoplpush,
    },
    commands::stream::read::{
        BlockingCommandResult as StreamBlockingCommandResult,
        GroupBlockingCommandResult as StreamGroupBlockingCommandResult, xread, xreadgroup,
    },
    commands::zset::blocking::{
        BlockSpec as ZBlockSpec, BlockingCommandResult as ZBlockingCommandResult, bzmpop, bzpopmax,
        bzpopmin,
    },
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
        pubsub::cleanup_pubsub_state(self.meta.id, &mut self.pubsub, &self.shard_pubsub);
        self.meta.flags.remove(ConnectionFlags::PUBSUB);
        if let Some(subscription) = self.monitor.take() {
            command_info::unsubscribe_monitor(&subscription);
        }
        self.meta.flags.remove(ConnectionFlags::MONITOR);
        self.blocked.borrow_mut().remove_client(self.meta.id);
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
                    pubsub::cleanup_pubsub_state(meta.id, pubsub, shard_pubsub);
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
        server_replication::execute(command, args, meta.resp_version == 3, meta, config)
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

    if let Some(blocked_response) = dispatch_blocking_command(
        meta.id,
        command,
        args,
        store,
        blocked,
        watch_registry,
        connections,
        meta,
        state,
    )
    .await?
    {
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
        meta,
    )? {
        return Ok((response, false, false, false));
    }

    if let Some(module_response) = crate::modules::dispatch(
        shard_id,
        command,
        args,
        meta.resp_version == 3,
        shard_extensions,
        &mut store.borrow_mut(),
    ) {
        return match module_response {
            Ok(response) => Ok((response, false, false, false)),
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
            {
                let mut store_ref = store.borrow_mut();
                let restore_no_touch = store_ref.no_touch();
                store_ref.set_no_touch(meta.no_touch);
                post_dispatch_notify(
                    command,
                    args,
                    &response,
                    &mut store_ref,
                    blocked,
                    watch_registry,
                    connections,
                );
                store_ref.set_no_touch(restore_no_touch);
            }
            let keys = notification_keys(command, args, &response);
            client_ops::invalidate_written_keys(
                &keys,
                meta.id,
                tracking_registry,
                client_connections,
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

struct ScriptRuntimeAdapter<'a> {
    meta: &'a ConnectionMeta,
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
    meta: &ConnectionMeta,
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
                let mut runtime = ScriptRuntimeAdapter {
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
                    db_id: meta.db,
                    username: meta.username.as_str(),
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

fn dispatch_key_lifecycle_command(
    command: &[u8],
    args: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    meta: &ConnectionMeta,
) -> Result<Option<Vec<u8>>, ConnectionControl> {
    if eq_ascii(command, b"DEL") || eq_ascii(command, b"UNLINK") {
        if args.is_empty() {
            return Err(ConnectionControl::Continue(error_message(
                if eq_ascii(command, b"DEL") {
                    "ERR wrong number of arguments for 'del' command"
                } else {
                    "ERR wrong number of arguments for 'unlink' command"
                },
            )));
        }
        let keys = args
            .iter()
            .map(frame_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
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
        return Ok(Some(serialize_response(&response, meta.resp_version == 3)));
    }

    if eq_ascii(command, b"RENAME") {
        if args.len() != 2 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR wrong number of arguments for 'rename' command",
            )));
        }
        let source = frame_bytes(&args[0])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let destination = parse_compact_key(&args[1])?;
        let source_key = CompactString::from_utf8(source).ok();
        let (response, outcome) = {
            let mut store_ref = store.borrow_mut();
            let outcome = generic_keys::rename_key(&mut store_ref, source, destination, true)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
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
        return Ok(Some(serialize_response(&response, meta.resp_version == 3)));
    }

    if eq_ascii(command, b"RENAMENX") {
        if args.len() != 2 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR wrong number of arguments for 'renamenx' command",
            )));
        }
        let source = frame_bytes(&args[0])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let destination = parse_compact_key(&args[1])?;
        let response = {
            let mut store_ref = store.borrow_mut();
            let source_type = store_ref.type_name(source);
            let renamed = generic_keys::rename_nx_key(&mut store_ref, source, destination)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
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
                        if let Ok(destination_key) =
                            CompactString::from_utf8(frame_bytes(&args[1]).map_err(|error| {
                                ConnectionControl::Continue(error_bytes(&error))
                            })?)
                        {
                            while registry.notify(&destination_key, &mut store_ref).is_some() {}
                        }
                    }
                }
            }
            Response::Integer(renamed as i64)
        };
        return Ok(Some(serialize_response(&response, meta.resp_version == 3)));
    }

    if eq_ascii(command, b"COPY") {
        if args.len() < 2 {
            return Err(ConnectionControl::Continue(error_message(
                "ERR wrong number of arguments for 'copy' command",
            )));
        }
        let source = frame_bytes(&args[0])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        let destination = parse_compact_key(&args[1])?;
        let (replace, db) = parse_copy_lifecycle_options(&args[2..])?;
        let destination_key = destination.clone();
        let response = {
            let mut store_ref = store.borrow_mut();
            let outcome = generic_keys::copy_key(&mut store_ref, source, destination, replace, db)
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
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
        return Ok(Some(serialize_response(&response, meta.resp_version == 3)));
    }

    Ok(None)
}

fn parse_compact_key(frame: &Frame<'_>) -> Result<CompactString, ConnectionControl> {
    let bytes =
        frame_bytes(frame).map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
    CompactString::from_utf8(bytes).map_err(|_| {
        ConnectionControl::Continue(error_bytes(&SenkoError::Protocol("invalid UTF-8 key")))
    })
}

fn parse_copy_lifecycle_options(args: &[Frame<'_>]) -> Result<(bool, u64), ConnectionControl> {
    let mut replace = false;
    let mut db = 0u64;
    let mut index = 0usize;
    while index < args.len() {
        let token = frame_bytes(&args[index])
            .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
        if eq_ascii(token, b"REPLACE") {
            replace = true;
            index += 1;
            continue;
        }
        if eq_ascii(token, b"DB") {
            index += 1;
            if index >= args.len() {
                return Err(ConnectionControl::Continue(error_message(
                    "ERR syntax error",
                )));
            }
            let raw = frame_bytes(&args[index])
                .map_err(|error| ConnectionControl::Continue(error_bytes(&error)))?;
            db = std::str::from_utf8(raw)
                .ok()
                .and_then(|text| text.parse::<u64>().ok())
                .ok_or_else(|| {
                    ConnectionControl::Continue(error_message("ERR invalid DB index"))
                })?;
            index += 1;
            continue;
        }
        return Err(ConnectionControl::Continue(error_message(
            "ERR syntax error",
        )));
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
        SenkoValue::Int(value) => RespSerializer::write_integer(out, *value),
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

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

async fn dispatch_blocking_command(
    conn_id: u64,
    command: &[u8],
    args: &[Frame<'_>],
    store: &Rc<RefCell<Store>>,
    blocked: &Rc<RefCell<BlockedKeyRegistry>>,
    watch_registry: &Rc<RefCell<WatchRegistry>>,
    connections: &Rc<RefCell<ConnectionMap>>,
    meta: &mut ConnectionMeta,
    state: &mut ConnectionState,
) -> Result<Option<Response>, ConnectionControl> {
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

fn post_dispatch_notify(
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

fn notification_keys(
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
