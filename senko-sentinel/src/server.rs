use std::{
    net::{SocketAddr, TcpListener as StdTcpListener},
    rc::Rc,
    time::Duration,
};

use bytes::BytesMut;
use compio::{
    BufResult,
    io::{AsyncReadManaged, AsyncWriteExt},
    net::{SocketOpts, TcpListener, TcpStream},
    runtime::{BufferPool, spawn},
    time::interval,
};
use senko_core::SenkoResult;
use senko_proto::{ParseStatus, RespParser};
use socket2::{Domain, Protocol, Socket, Type};

use crate::{
    SharedRuntime,
    client::SentinelClientPool,
    commands::{SentinelClient, dispatch, drive_failovers},
    current_unix_ms,
    gossip::HelloMessage,
    state::TiltState,
};

const READ_CHUNK_SIZE: usize = 8 * 1024;
const INFO_REFRESH_MS: u64 = 10_000;
const HELLO_PUBLISH_MS: u64 = 2_000;

pub async fn run(runtime: SharedRuntime) -> SenkoResult<()> {
    let bind_ip = runtime
        .borrow()
        .config
        .bind_addrs()
        .first()
        .map(String::as_str)
        .unwrap_or("0.0.0.0")
        .parse()?;
    let port = runtime.borrow().config.port();
    let listener = bind_listener(bind_ip, port)?;
    let listener = TcpListener::from_std(listener)?;
    let pool = Rc::new(BufferPool::new(128, READ_CHUNK_SIZE)?);
    let monitor_runtime = runtime.clone();
    spawn(async move {
        let mut ticks = interval(Duration::from_millis(100));
        let mut tilt = TiltState::default();
        let mut upstream = SentinelClientPool::default();
        loop {
            ticks.tick().await;
            let now = current_unix_ms();
            if let Some(state) = tilt.observe(now) {
                let event = if state { "+tilt" } else { "-tilt" };
                monitor_runtime
                    .borrow_mut()
                    .notifier
                    .emit(event, "sentinel");
            }
            {
                let runtime_ref = &mut *monitor_runtime.borrow_mut();
                let down_after = |name: &str| runtime_ref.config.down_after_milliseconds(name);
                runtime_ref.monitor.sweep(
                    &runtime_ref.world,
                    now,
                    down_after,
                    &mut runtime_ref.notifier,
                    tilt.tilt_mode,
                );
            }
            let actions = collect_monitor_actions(&monitor_runtime, now);
            for action in actions {
                let _ = apply_monitor_action(&monitor_runtime, &mut upstream, action, now).await;
            }
            auto_start_failovers(&monitor_runtime, now, tilt.tilt_mode);
            drive_failovers(&monitor_runtime);
        }
    })
    .detach();

    let accept_opts = SocketOpts::new().keepalive(true).nodelay(true);
    let mut next_client_id = 1u64;
    loop {
        let (stream, _) = listener.accept_with_options(&accept_opts).await?;
        runtime
            .borrow()
            .stats
            .total_connections_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let client_id = next_client_id;
        next_client_id = next_client_id.saturating_add(1);
        let runtime = runtime.clone();
        let pool = pool.clone();
        spawn(async move {
            let _ = handle_client(stream, runtime, pool, client_id).await;
        })
        .detach();
    }
}

async fn handle_client(
    mut stream: TcpStream,
    runtime: SharedRuntime,
    pool: Rc<BufferPool>,
    client_id: u64,
) -> SenkoResult<()> {
    runtime.borrow_mut().connected_clients += 1;
    let peer_addr = stream.peer_addr()?;
    let starts_authenticated = runtime.borrow().config.requirepass().is_none();
    let mut client = SentinelClient::new(client_id, peer_addr, starts_authenticated);
    let parser = RespParser::new();
    let mut parse_buffer = BytesMut::with_capacity(READ_CHUNK_SIZE);
    loop {
        let read = stream.read_managed(pool.as_ref(), READ_CHUNK_SIZE).await?;
        if read.is_empty() {
            break;
        }
        runtime
            .borrow()
            .stats
            .total_net_input_bytes
            .fetch_add(read.len() as u64, std::sync::atomic::Ordering::Relaxed);
        parse_buffer.extend_from_slice(&read);
        let mut consumed = 0usize;
        loop {
            match parser.parse(&parse_buffer[consumed..])? {
                ParseStatus::Complete(frame, used) => {
                    consumed += used;
                    let result = dispatch(frame, &runtime, &mut client)?;
                    runtime.borrow().stats.total_net_output_bytes.fetch_add(
                        result.response.len() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let BufResult(write, _) = stream.write_all(result.response.freeze()).await;
                    write?;
                    if result.close {
                        let current = runtime.borrow().connected_clients;
                        runtime.borrow_mut().connected_clients = current.saturating_sub(1);
                        return Ok(());
                    }
                }
                ParseStatus::Incomplete(_) => break,
            }
        }
        if consumed > 0 {
            let _ = parse_buffer.split_to(consumed);
        }
    }
    let current = runtime.borrow().connected_clients;
    runtime.borrow_mut().connected_clients = current.saturating_sub(1);
    Ok(())
}

fn bind_listener(bind: std::net::IpAddr, port: u16) -> SenkoResult<StdTcpListener> {
    let addr = SocketAddr::new(bind, port);
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(all(
        unix,
        not(any(target_os = "illumos", target_os = "solaris", target_os = "cygwin"))
    ))]
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

#[derive(Clone)]
enum MonitorAction {
    PingMaster { name: String, addr: SocketAddr },
    InfoMaster { name: String, addr: SocketAddr },
    PublishHello { addr: SocketAddr, payload: String },
    PingReplica { master: String, addr: SocketAddr },
    InfoReplica { master: String, addr: SocketAddr },
}

fn collect_monitor_actions(runtime: &SharedRuntime, now: u64) -> Vec<MonitorAction> {
    let runtime_ref = runtime.borrow();
    let snapshot = runtime_ref.snapshot();
    let mut actions = Vec::new();
    for master in snapshot.masters.values() {
        actions.push(MonitorAction::PingMaster {
            name: master.name.clone(),
            addr: master.addr,
        });
        if now.saturating_sub(master.info_refresh) >= INFO_REFRESH_MS {
            actions.push(MonitorAction::InfoMaster {
                name: master.name.clone(),
                addr: master.addr,
            });
        }
        let bind_addr = runtime_ref
            .config
            .bind_addrs()
            .first()
            .map(String::as_str)
            .unwrap_or("0.0.0.0");
        if now.saturating_sub(master.last_ping_sent) >= HELLO_PUBLISH_MS {
            let hello = HelloMessage {
                sentinel_ip: runtime_ref
                    .config
                    .network
                    .announce_ip
                    .as_deref()
                    .unwrap_or(bind_addr)
                    .parse()
                    .unwrap_or(master.addr.ip()),
                sentinel_port: runtime_ref
                    .config
                    .network
                    .announce_port
                    .unwrap_or(runtime_ref.config.port()),
                sentinel_runid: snapshot.my_id.to_string(),
                current_epoch: snapshot.epoch,
                master_name: master.name.clone(),
                master_ip: master.addr.ip(),
                master_port: master.addr.port(),
                master_config_epoch: master.config_epoch,
            };
            actions.push(MonitorAction::PublishHello {
                addr: master.addr,
                payload: hello.encode(),
            });
        }
        for replica in master.replicas.values() {
            actions.push(MonitorAction::PingReplica {
                master: master.name.clone(),
                addr: replica.addr,
            });
            if now.saturating_sub(replica.info_refresh) >= INFO_REFRESH_MS {
                actions.push(MonitorAction::InfoReplica {
                    master: master.name.clone(),
                    addr: replica.addr,
                });
            }
        }
    }
    actions
}

async fn apply_monitor_action(
    runtime: &SharedRuntime,
    pool: &mut SentinelClientPool,
    action: MonitorAction,
    now: u64,
) -> SenkoResult<()> {
    match action {
        MonitorAction::PingMaster { name, addr } => match pool.ping(addr).await {
            Ok(()) => runtime.borrow_mut().monitor.on_pong(addr, now),
            Err(_) => mark_disconnected(runtime, &name, addr),
        },
        MonitorAction::InfoMaster { name, addr } => {
            let info = pool.info(addr).await?;
            {
                let runtime_ref = &mut *runtime.borrow_mut();
                runtime_ref.monitor.on_info(&name, addr, &info, now);
                let _ = crate::state::update_world(&runtime_ref.world, |snapshot| {
                    if let Some(master) = snapshot.masters.get_mut(&name) {
                        let before = master.replicas.len();
                        let _ =
                            crate::monitor::MonitorEngine::apply_info_to_master(master, &info, now);
                        if master.replicas.len() > before {
                            runtime_ref.notifier.emit("+slave", name.as_str());
                        }
                    }
                });
            }
        }
        MonitorAction::PublishHello { addr, payload } => {
            let _ = pool.publish_hello(addr, &payload).await;
        }
        MonitorAction::PingReplica { master, addr } => match pool.ping(addr).await {
            Ok(()) => {
                let _ = crate::state::update_world(&runtime.borrow().world, |snapshot| {
                    if let Some(master_state) = snapshot.masters.get_mut(&master)
                        && let Some(replica) = master_state.replicas.get_mut(&addr)
                    {
                        replica.last_ok_ping = now;
                        replica
                            .flags
                            .remove(crate::state::InstanceFlags::DISCONNECTED);
                    }
                });
            }
            Err(_) => {
                let _ = crate::state::update_world(&runtime.borrow().world, |snapshot| {
                    if let Some(master_state) = snapshot.masters.get_mut(&master)
                        && let Some(replica) = master_state.replicas.get_mut(&addr)
                    {
                        replica
                            .flags
                            .insert(crate::state::InstanceFlags::DISCONNECTED);
                    }
                });
            }
        },
        MonitorAction::InfoReplica { master, addr } => {
            let info = pool.info(addr).await?;
            let parsed = crate::monitor::parse_info_replication(&info);
            let _ = crate::state::update_world(&runtime.borrow().world, |snapshot| {
                if let Some(master_state) = snapshot.masters.get_mut(&master)
                    && let Some(replica) = master_state.replicas.get_mut(&addr)
                {
                    replica.info_refresh = now;
                    replica.role_reported = parsed.role.clone();
                    replica.master_link_status = parsed.master_link_status.clone();
                    replica.master_link_down_time = parsed.master_link_down_time;
                    replica.slave_priority = parsed.slave_priority;
                    replica.slave_repl_offset = parsed.slave_repl_offset;
                }
            });
        }
    }
    Ok(())
}

fn mark_disconnected(runtime: &SharedRuntime, master_name: &str, addr: SocketAddr) {
    let _ = crate::state::update_world(&runtime.borrow().world, |snapshot| {
        if let Some(master) = snapshot.masters.get_mut(master_name)
            && master.addr == addr
        {
            master
                .flags
                .insert(crate::state::InstanceFlags::DISCONNECTED);
        }
    });
}

fn auto_start_failovers(runtime: &SharedRuntime, now: u64, tilt_mode: bool) {
    if tilt_mode {
        return;
    }
    let mut runtime_ref = runtime.borrow_mut();
    let snapshot = runtime_ref.snapshot();
    let candidates = snapshot
        .masters
        .values()
        .filter(|master| {
            master.flags.contains(crate::state::InstanceFlags::O_DOWN)
                && !master
                    .flags
                    .contains(crate::state::InstanceFlags::FAILOVER_IN_PROGRESS)
        })
        .map(|master| master.name.clone())
        .collect::<Vec<_>>();
    drop(snapshot);
    let my_id = runtime_ref.my_id();
    for name in candidates {
        let world = runtime_ref.world.clone();
        let epoch = runtime_ref
            .elections
            .start_election(&world, &name, &my_id, now);
        let _ = crate::state::update_world(&world, |snapshot| {
            if let Some(master) = snapshot.masters.get_mut(&name) {
                crate::failover::begin_failover(master, epoch);
            }
        });
        runtime_ref.notifier.emit("+new-epoch", name.as_str());
        runtime_ref.notifier.emit("+try-failover", name.as_str());
        runtime_ref.notifier.emit("+elected-leader", name.as_str());
    }
}
