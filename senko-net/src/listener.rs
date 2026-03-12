use std::{
    cell::RefCell,
    io,
    net::{SocketAddr, TcpListener as StdTcpListener},
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, OnceLock},
    time::Duration,
};

use compio::{
    net::{SocketOpts, TcpListener},
    runtime::{JoinHandle, spawn},
    time::interval,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use senko_core::{
    ModuleRegistry, SenkoConfig, SenkoError, SenkoResult, ShardExtensions, ShardState,
};
use senko_scripting::{LuaEngine, ScriptingConfig};
use senko_store::Store;
use socket2::{Domain, Protocol, Socket, Type};
use tracing::debug;

use crate::{
    acl,
    blocked::BlockedKeyRegistry,
    commands::cluster::ClusterCommandState,
    commands::connection::client_ops::{PauseState, TrackingRegistry},
    commands::server::command_info,
    commands::server::config as live_config,
    commands::server::diagnostics as server_diagnostics,
    commands::server::info as server_info,
    commands::server::replication as server_replication,
    connection::{ClientConnectionMap, Connection},
    pubsub::fanout::{CrossShardBus, ShardFanOut},
    transaction::{ConnectionMap, WatchRegistry},
};

static CROSS_SHARD_BUS: OnceLock<Arc<CrossShardBus>> = OnceLock::new();

#[derive(Debug)]
pub struct PreparedListener {
    inner: StdTcpListener,
}

impl PreparedListener {
    fn into_compio(self) -> io::Result<TcpListener> {
        TcpListener::from_std(self.inner)
    }
}

pub fn prepare_listeners(config: &SenkoConfig) -> SenkoResult<Vec<PreparedListener>> {
    let mut listeners = Vec::with_capacity(config.num_shards);
    #[cfg(windows)]
    {
        let base = bind_std_listener(config.bind_addr, config)?;
        listeners.push(PreparedListener {
            inner: base.try_clone()?,
        });
        for _ in 1..config.num_shards {
            listeners.push(PreparedListener {
                inner: base.try_clone()?,
            });
        }
    }
    #[cfg(not(windows))]
    {
        for _ in 0..config.num_shards {
            listeners.push(PreparedListener {
                inner: bind_std_listener(config.bind_addr, config)?,
            });
        }
    }
    Ok(listeners)
}

pub async fn run_shard(
    shard_index: usize,
    config: SenkoConfig,
    prepared: PreparedListener,
    module_registry: Arc<ModuleRegistry>,
) -> SenkoResult<()> {
    debug!(
        shard = shard_index,
        bind_addr = %config.bind_addr,
        "shard runtime initialized"
    );
    crate::modules::init(Arc::clone(&module_registry));
    live_config::init(&config);
    command_info::init(&config);
    acl::init(&config)?;
    server_diagnostics::init(&config);
    server_info::init(&config);
    server_replication::init(&config);
    let listener = prepared.into_compio()?;
    let store = Rc::new(RefCell::new(Store::new(config.max_memory)));
    let engine = Rc::new(RefCell::new(
        LuaEngine::new(&ScriptingConfig {
            max_depth: 1,
            time_limit_ms: config.lua_time_limit,
        })
        .map_err(|error| SenkoError::ProtocolMessage(error.client_message().into()))?,
    ));
    let shard_extensions = Arc::new(ShardExtensions::default());
    let mut shard_state = ShardState::new(shard_index, Arc::clone(&shard_extensions));
    module_registry.init_shard(&mut shard_state);
    let blocked = Rc::new(RefCell::new(BlockedKeyRegistry::default()));
    let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
        config.bind_addr,
        shard_index,
    )));
    let watch_registry = Rc::new(RefCell::new(WatchRegistry::default()));
    let connections = Rc::new(RefCell::new(ConnectionMap::default()));
    let client_connections = Rc::new(RefCell::new(ClientConnectionMap::default()));
    let pause_state = Rc::new(RefCell::new(PauseState::default()));
    let tracking_registry = Rc::new(RefCell::new(TrackingRegistry::default()));
    let shard_pubsub = Rc::new(RefCell::new(ShardFanOut::new(
        shard_index,
        Arc::clone(CROSS_SHARD_BUS.get_or_init(|| Arc::new(CrossShardBus::new(config.num_shards)))),
    )));
    let query_receiver = server_info::take_query_receiver(shard_index);
    let accept_opts = SocketOpts::new()
        .keepalive(config.tcp_keepalive > 0)
        .nodelay(config.tcp_nodelay);
    let per_shard_limit = (config.max_connections / config.num_shards.max(1)).max(1);
    let mut tasks: FuturesUnordered<JoinHandle<SenkoResult<()>>> = FuturesUnordered::new();
    let expiry_store = Rc::clone(&store);
    let blocked_registry = Rc::clone(&blocked);
    let shard_pause = Rc::clone(&pause_state);
    let shard_pubsub_tick = Rc::clone(&shard_pubsub);
    let query_store = Rc::clone(&store);
    let query_blocked = Rc::clone(&blocked);
    let query_engine = Rc::clone(&engine);
    let query_connections = Rc::clone(&client_connections);
    let query_pause = Rc::clone(&pause_state);
    let query_watch_registry = Rc::clone(&watch_registry);
    let query_watch_connections = Rc::clone(&connections);
    let query_pubsub = Rc::clone(&shard_pubsub);
    spawn(async move {
        let mut ticks = interval(Duration::from_millis(100));
        loop {
            ticks.tick().await;
            let now_ms = senko_store::store::current_unix_ms();
            let expired = expiry_store.borrow_mut().advance_expiry_wheel(now_ms);
            server_info::on_expired_keys(shard_index, expired);
            let _ = blocked_registry
                .borrow_mut()
                .check_timeouts(std::time::Instant::now());
            let _ = shard_pause
                .borrow_mut()
                .check_expired(std::time::Instant::now());
        }
    })
    .detach();
    spawn(async move {
        let mut ticks = interval(Duration::from_millis(1));
        loop {
            ticks.tick().await;
            let _ = shard_pubsub_tick.borrow_mut().drain_bus();
            let _ = server_info::drain_shard_queries(
                shard_index,
                &query_receiver,
                &query_store,
                &query_engine,
                &query_blocked,
                &query_connections,
                &query_pause,
                &query_watch_registry,
                &query_watch_connections,
                &query_pubsub,
            );
        }
    })
    .detach();
    let next_conn_id = AtomicU64::new(1);

    loop {
        while tasks.len() >= per_shard_limit {
            if let Some(result) = tasks.next().await {
                result.expect("connection task panicked")?;
            }
        }

        let (stream, peer_addr) = listener.accept_with_options(&accept_opts).await?;
        let local_addr = stream.local_addr()?;
        debug!(
            shard = shard_index,
            peer_addr = %peer_addr,
            local_addr = %local_addr,
            "accepted connection"
        );
        server_info::on_connection_open(shard_index);
        let connection = Connection::new(
            shard_index,
            next_conn_id.fetch_add(1, Ordering::Relaxed),
            stream,
            peer_addr,
            local_addr,
            Rc::clone(&store),
            Rc::clone(&engine),
            Arc::clone(&shard_extensions),
            Rc::clone(&blocked),
            Rc::clone(&cluster),
            Rc::clone(&watch_registry),
            Rc::clone(&connections),
            Rc::clone(&client_connections),
            Rc::clone(&pause_state),
            Rc::clone(&tracking_registry),
            Rc::clone(&shard_pubsub),
            &config,
        );
        let config = config.clone();
        tasks.push(spawn(async move { connection.run(&config).await }));
    }
}

fn bind_std_listener(addr: SocketAddr, config: &SenkoConfig) -> SenkoResult<StdTcpListener> {
    let domain = Domain::for_address(addr);
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(
        unix,
        not(any(target_os = "illumos", target_os = "solaris", target_os = "cygwin"))
    ))]
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(config.tcp_backlog as i32)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}
