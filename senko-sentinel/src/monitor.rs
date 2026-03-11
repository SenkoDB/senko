use std::{collections::VecDeque, net::SocketAddr};

use ahash::RandomState;
use bytes::Bytes;
use hashbrown::HashMap;
use memchr::memchr;

use crate::{
    detector,
    notify::Notifier,
    state::{
        InstanceFlags, LinkStatus, MasterState, ReplicaState, Role, SentinelWorld, update_world,
    },
};

#[derive(Debug, Clone)]
pub enum SentinelCommand {
    Ping,
    Info,
    SubscribeHello,
    PublishHello(String),
    SlaveOfNoOne,
    SlaveOf(SocketAddr),
    IsMasterDownByAddr {
        ip: String,
        port: u16,
        current_epoch: u64,
        runid: String,
    },
}

#[derive(Debug, Clone)]
pub struct InstanceLink {
    pub addr: SocketAddr,
    pub pending_commands: VecDeque<SentinelCommand>,
    pub last_send_time: u64,
    pub last_recv_time: u64,
    pub last_ok_ping: u64,
    pub last_pong_time: u64,
    pub last_info_time: u64,
    pub ping_sent_time: u64,
    pub reconnect_period: u64,
    pub next_reconnect: u64,
    pub refcount: u32,
}

impl InstanceLink {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            pending_commands: VecDeque::new(),
            last_send_time: 0,
            last_recv_time: 0,
            last_ok_ping: 0,
            last_pong_time: 0,
            last_info_time: 0,
            ping_sent_time: 0,
            reconnect_period: 500,
            next_reconnect: 0,
            refcount: 1,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InfoReplication {
    pub role: Role,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub master_link_status: LinkStatus,
    pub master_link_down_time: u64,
    pub slave_priority: i32,
    pub slave_repl_offset: u64,
    pub replicas: Vec<ReplicaInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInfo {
    pub addr: SocketAddr,
    pub offset: u64,
}

pub struct MonitorEngine {
    pub links: HashMap<SocketAddr, InstanceLink, RandomState>,
    pub info_cache: HashMap<String, Bytes, RandomState>,
    pub sentinel_hz_ms: u64,
}

impl MonitorEngine {
    pub fn new(sentinel_hz_ms: u64) -> Self {
        Self {
            links: HashMap::with_hasher(RandomState::new()),
            info_cache: HashMap::with_hasher(RandomState::new()),
            sentinel_hz_ms,
        }
    }

    pub fn register_master(&mut self, name: &str, addr: SocketAddr) {
        self.links
            .entry(addr)
            .or_insert_with(|| InstanceLink::new(addr));
        self.info_cache
            .entry(name.to_owned())
            .or_default();
    }

    pub fn queue_command(&mut self, addr: SocketAddr, command: SentinelCommand) {
        self.links
            .entry(addr)
            .or_insert_with(|| InstanceLink::new(addr))
            .pending_commands
            .push_back(command);
    }

    pub fn on_pong(&mut self, addr: SocketAddr, now: u64) {
        if let Some(link) = self.links.get_mut(&addr) {
            link.last_recv_time = now;
            link.last_ok_ping = now;
            link.last_pong_time = now;
            link.ping_sent_time = 0;
        }
    }

    pub fn on_info(&mut self, master_name: &str, addr: SocketAddr, payload: &[u8], now: u64) {
        self.info_cache
            .insert(master_name.to_owned(), Bytes::copy_from_slice(payload));
        if let Some(link) = self.links.get_mut(&addr) {
            link.last_info_time = now;
            link.last_recv_time = now;
        }
    }

    pub fn sweep(
        &mut self,
        world: &SentinelWorld,
        now: u64,
        down_after_ms: impl Fn(&str) -> u64,
        notifier: &mut Notifier,
        tilt_mode: bool,
    ) {
        let _ = update_world(world, |snapshot| {
            for master in snapshot.masters.values_mut() {
                if let Some(link) = self.links.get(&master.addr) {
                    master.last_ping_sent = link.ping_sent_time;
                    master.last_ok_ping = link.last_ok_ping;
                    master.link_pending_commands = link.pending_commands.len() as u32;
                    master.link_refcount = link.refcount;
                }
                let changes =
                    detector::sweep_master(master, now, down_after_ms(&master.name), tilt_mode);
                if changes.sdown_changed == Some(true) {
                    notifier.emit("+sdown", master.name.as_str());
                } else if changes.sdown_changed == Some(false) {
                    notifier.emit("-sdown", master.name.as_str());
                }
                if changes.odown_changed == Some(true) {
                    notifier.emit("+odown", master.name.as_str());
                } else if changes.odown_changed == Some(false) {
                    notifier.emit("-odown", master.name.as_str());
                }
            }
            snapshot.timestamp = now;
        });
    }

    pub fn apply_info_to_master(
        master: &mut MasterState,
        payload: &[u8],
        now: u64,
    ) -> InfoReplication {
        let parsed = parse_info_replication(payload);
        master.role_reported = parsed.role.clone();
        master.info_refresh = now;
        master.cached_info = payload.to_vec();
        for replica in &parsed.replicas {
            master
                .replicas
                .entry(replica.addr)
                .and_modify(|state| state.slave_repl_offset = replica.offset)
                .or_insert_with(|| ReplicaState {
                    addr: replica.addr,
                    flags: InstanceFlags::SLAVE,
                    last_ok_ping: now,
                    last_ping_sent: 0,
                    down_since: None,
                    info_refresh: now,
                    master_link_down_time: 0,
                    master_link_status: LinkStatus::Ok,
                    slave_priority: 100,
                    slave_repl_offset: replica.offset,
                    replica_announced: true,
                    role_reported: Role::Slave,
                    name: replica.addr.to_string(),
                });
        }
        parsed
    }
}

pub fn parse_info_replication(payload: &[u8]) -> InfoReplication {
    let mut info = InfoReplication::default();
    let mut cursor = 0usize;
    while cursor < payload.len() {
        let end = memchr(b'\n', &payload[cursor..])
            .map(|index| cursor + index)
            .unwrap_or(payload.len());
        let line = payload[cursor..end]
            .strip_suffix(b"\r")
            .unwrap_or(&payload[cursor..end]);
        cursor = end.saturating_add(1);
        if line.is_empty() || line[0] == b'#' {
            continue;
        }
        let Some(separator) = memchr(b':', line) else {
            continue;
        };
        let key = &line[..separator];
        let value = &line[separator + 1..];
        match key {
            b"role" if value == b"master" => info.role = Role::Master,
            b"role" if value == b"slave" => info.role = Role::Slave,
            b"master_host" => info.master_host = Some(String::from_utf8_lossy(value).into_owned()),
            b"master_port" => {
                info.master_port = std::str::from_utf8(value).ok().and_then(|v| v.parse().ok())
            }
            b"master_link_status" if value == b"up" => info.master_link_status = LinkStatus::Ok,
            b"master_link_status" if value == b"down" => info.master_link_status = LinkStatus::Err,
            b"master_link_down_since_seconds" => {
                info.master_link_down_time = std::str::from_utf8(value)
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0)
                    * 1_000
            }
            b"slave_priority" => {
                info.slave_priority = std::str::from_utf8(value)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(100);
            }
            b"slave_repl_offset" => {
                info.slave_repl_offset = std::str::from_utf8(value)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
            }
            _ if key.starts_with(b"slave") => {
                if let Some(replica) = parse_replica_line(value) {
                    info.replicas.push(replica);
                }
            }
            _ => {}
        }
    }
    info
}

fn parse_replica_line(payload: &[u8]) -> Option<ReplicaInfo> {
    let raw = std::str::from_utf8(payload).ok()?;
    let mut ip = None;
    let mut port = None;
    let mut offset = 0u64;
    for segment in raw.split(',') {
        let (key, value) = segment.split_once('=')?;
        match key {
            "ip" => ip = value.parse().ok(),
            "port" => port = value.parse().ok(),
            "offset" => offset = value.parse().ok()?,
            _ => {}
        }
    }
    Some(ReplicaInfo {
        addr: SocketAddr::new(ip?, port?),
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn parses_replication_info_without_regex() {
        let info = parse_info_replication(
            br#"# Replication
role:master
connected_slaves:1
slave0:ip=127.0.0.1,port=6380,state=online,offset=99,lag=0
"#,
        );
        assert_eq!(info.role, Role::Master);
        assert_eq!(info.replicas.len(), 1);
        assert_eq!(
            info.replicas[0].addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6380)
        );
        assert_eq!(info.replicas[0].offset, 99);
    }
}
