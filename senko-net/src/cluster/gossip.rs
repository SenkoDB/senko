use std::{
    cell::RefCell,
    io,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    rc::Rc,
    time::Duration,
};

use ahash::RandomState;
use compio::io::AsyncWrite;
use compio::{
    BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    runtime::{JoinHandle, spawn},
    time::interval,
};
use hashbrown::{HashMap, HashSet};
use rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom};
use roaring::RoaringBitmap;
use senko_cluster::{ClusterState, NodeId, NodeMeta, NodeRole, NodeState};

pub const CLUSTER_MAGIC: [u8; 4] = *b"SNKU";
pub const CLUSTER_VERSION: u8 = 1;
pub const CLUSTER_HEADER_LEN: usize = 36;
pub const PING_PERIOD_MS: u64 = 100;
pub const DEFAULT_NODE_TIMEOUT_MS: u64 = 15_000;
pub const SLOT_SUMMARY_BYTES: usize = 256;
pub const SLOT_BITMAP_BYTES: usize = 16_384 / 8;
pub const GOSSIP_ENTRY_LEN: usize = 20 + 8 + 16 + 2 + 2 + 2 + SLOT_SUMMARY_BYTES + 8;
pub const UPDATE_ENTRY_LEN: usize = 20 + 8 + 16 + 2 + 2 + 2 + 2 + 20 + SLOT_BITMAP_BYTES + 8;
pub const FAILOVER_AUTH_REQ_LEN: usize = 20 + 8 + 8 + SLOT_BITMAP_BYTES;
pub const FAILOVER_AUTH_ACK_LEN: usize = 20 + 8;
pub const FAIL_MESSAGE_LEN: usize = 20;
pub const FAILOVER_DELAY_MS: u64 = 500;
pub const FAILOVER_JITTER_MS: u64 = 500;
pub const ROLE_PRIMARY_FLAG: u16 = 1 << 8;
pub const ROLE_REPLICA_FLAG: u16 = 1 << 9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Ping = 1,
    Pong = 2,
    Meet = 3,
    Update = 4,
    Fail = 5,
    FailoverAuth = 6,
    FailoverAuthAck = 7,
}

impl MessageType {
    #[inline]
    pub const fn from_u8(value: u8) -> Result<Self, GossipProtocolError> {
        match value {
            1 => Ok(Self::Ping),
            2 => Ok(Self::Pong),
            3 => Ok(Self::Meet),
            4 => Ok(Self::Update),
            5 => Ok(Self::Fail),
            6 => Ok(Self::FailoverAuth),
            7 => Ok(Self::FailoverAuthAck),
            _ => Err(GossipProtocolError::InvalidMessageType(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMessageHeader {
    pub msg_type: MessageType,
    pub sender_id: NodeId,
    pub config_epoch: u64,
    pub flags: u16,
}

impl ClusterMessageHeader {
    #[inline]
    pub fn new(msg_type: MessageType, sender_id: NodeId, config_epoch: u64) -> Self {
        Self {
            msg_type,
            sender_id,
            config_epoch,
            flags: 0,
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&CLUSTER_MAGIC);
        out.push(CLUSTER_VERSION);
        out.push(self.msg_type as u8);
        out.extend_from_slice(self.sender_id.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
        out.extend_from_slice(&self.flags.to_be_bytes());
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, GossipProtocolError> {
        let magic = cursor.take::<4>()?;
        if magic != CLUSTER_MAGIC {
            return Err(GossipProtocolError::InvalidMagic(magic));
        }

        let version = cursor.take_u8()?;
        if version != CLUSTER_VERSION {
            return Err(GossipProtocolError::UnsupportedVersion(version));
        }

        let msg_type = MessageType::from_u8(cursor.take_u8()?)?;
        let sender_id = NodeId::new(cursor.take::<20>()?);
        let config_epoch = cursor.take_u64()?;
        let flags = cursor.take_u16()?;

        Ok(Self {
            msg_type,
            sender_id,
            config_epoch,
            flags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GossipEntry {
    pub node_id: NodeId,
    pub config_epoch: u64,
    pub ip: [u8; 16],
    pub port: u16,
    pub cluster_port: u16,
    pub flags: u16,
    pub slot_summary: [u8; SLOT_SUMMARY_BYTES],
    pub pong_received: u64,
}

impl GossipEntry {
    #[inline]
    pub fn from_meta(meta: &NodeMeta) -> Self {
        let (ip, port, cluster_port) = encode_addrs(meta.addr, meta.cluster_addr);
        Self {
            node_id: meta.id,
            config_epoch: meta.config_epoch,
            ip,
            port,
            cluster_port,
            flags: meta.state.to_flags() | role_flag(meta),
            slot_summary: slot_summary(&meta.slots),
            pong_received: meta.pong_recv,
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.node_id.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
        out.extend_from_slice(&self.ip);
        out.extend_from_slice(&self.port.to_be_bytes());
        out.extend_from_slice(&self.cluster_port.to_be_bytes());
        out.extend_from_slice(&self.flags.to_be_bytes());
        out.extend_from_slice(&self.slot_summary);
        out.extend_from_slice(&self.pong_received.to_be_bytes());
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, GossipProtocolError> {
        Ok(Self {
            node_id: NodeId::new(cursor.take::<20>()?),
            config_epoch: cursor.take_u64()?,
            ip: cursor.take::<16>()?,
            port: cursor.take_u16()?,
            cluster_port: cursor.take_u16()?,
            flags: cursor.take_u16()?,
            slot_summary: cursor.take::<SLOT_SUMMARY_BYTES>()?,
            pong_received: cursor.take_u64()?,
        })
    }

    #[inline]
    pub fn addr(&self) -> Result<SocketAddr, GossipProtocolError> {
        decode_socket_addr(self.ip, self.port)
    }

    #[inline]
    pub fn cluster_addr(&self) -> Result<SocketAddr, GossipProtocolError> {
        decode_socket_addr(self.ip, self.cluster_port)
    }

    #[inline]
    pub fn node_state(&self) -> NodeState {
        NodeState::from_flags(self.flags)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateMessage {
    pub node_id: NodeId,
    pub config_epoch: u64,
    pub ip: [u8; 16],
    pub port: u16,
    pub cluster_port: u16,
    pub state_flags: u16,
    pub role_flags: u16,
    pub primary: NodeId,
    pub slot_bitmap: [u8; SLOT_BITMAP_BYTES],
    pub pong_received: u64,
}

impl UpdateMessage {
    pub fn from_meta(meta: &NodeMeta) -> Self {
        let (ip, port, cluster_port) = encode_addrs(meta.addr, meta.cluster_addr);
        let (role_flags, primary) = encode_role(&meta.role);
        Self {
            node_id: meta.id,
            config_epoch: meta.config_epoch,
            ip,
            port,
            cluster_port,
            state_flags: meta.state.to_flags(),
            role_flags,
            primary,
            slot_bitmap: slots_to_bitmap(&meta.slots),
            pong_received: meta.pong_recv,
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.node_id.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
        out.extend_from_slice(&self.ip);
        out.extend_from_slice(&self.port.to_be_bytes());
        out.extend_from_slice(&self.cluster_port.to_be_bytes());
        out.extend_from_slice(&self.state_flags.to_be_bytes());
        out.extend_from_slice(&self.role_flags.to_be_bytes());
        out.extend_from_slice(self.primary.as_bytes());
        out.extend_from_slice(&self.slot_bitmap);
        out.extend_from_slice(&self.pong_received.to_be_bytes());
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, GossipProtocolError> {
        Ok(Self {
            node_id: NodeId::new(cursor.take::<20>()?),
            config_epoch: cursor.take_u64()?,
            ip: cursor.take::<16>()?,
            port: cursor.take_u16()?,
            cluster_port: cursor.take_u16()?,
            state_flags: cursor.take_u16()?,
            role_flags: cursor.take_u16()?,
            primary: NodeId::new(cursor.take::<20>()?),
            slot_bitmap: cursor.take::<SLOT_BITMAP_BYTES>()?,
            pong_received: cursor.take_u64()?,
        })
    }

    fn to_meta(&self) -> Result<NodeMeta, GossipProtocolError> {
        Ok(NodeMeta {
            id: self.node_id,
            addr: decode_socket_addr(self.ip, self.port)?,
            cluster_addr: decode_socket_addr(self.ip, self.cluster_port)?,
            role: decode_role(self.role_flags, self.primary),
            state: NodeState::from_flags(self.state_flags),
            ping_sent: 0,
            pong_recv: self.pong_received,
            config_epoch: self.config_epoch,
            slots: bitmap_to_slots(&self.slot_bitmap),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailoverAuthRequest {
    pub requesting_node_id: NodeId,
    pub config_epoch: u64,
    pub replication_offset: u64,
    pub slot_bitmap: [u8; SLOT_BITMAP_BYTES],
}

impl FailoverAuthRequest {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.requesting_node_id.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
        out.extend_from_slice(&self.replication_offset.to_be_bytes());
        out.extend_from_slice(&self.slot_bitmap);
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, GossipProtocolError> {
        Ok(Self {
            requesting_node_id: NodeId::new(cursor.take::<20>()?),
            config_epoch: cursor.take_u64()?,
            replication_offset: cursor.take_u64()?,
            slot_bitmap: cursor.take::<SLOT_BITMAP_BYTES>()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailoverAuthAck {
    pub requesting_node_id: NodeId,
    pub config_epoch: u64,
}

impl FailoverAuthAck {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.requesting_node_id.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, GossipProtocolError> {
        Ok(Self {
            requesting_node_id: NodeId::new(cursor.take::<20>()?),
            config_epoch: cursor.take_u64()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterMessageBody {
    Empty,
    Gossip(Vec<GossipEntry>),
    Update(UpdateMessage),
    Fail { failed_node_id: NodeId },
    FailoverAuth(FailoverAuthRequest),
    FailoverAuthAck(FailoverAuthAck),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMessage {
    pub header: ClusterMessageHeader,
    pub body: ClusterMessageBody,
}

impl ClusterMessage {
    #[inline]
    pub fn ping(sender_id: NodeId, config_epoch: u64, entries: Vec<GossipEntry>) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::Ping, sender_id, config_epoch),
            body: ClusterMessageBody::Gossip(entries),
        }
    }

    #[inline]
    pub fn pong(sender_id: NodeId, config_epoch: u64, entries: Vec<GossipEntry>) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::Pong, sender_id, config_epoch),
            body: ClusterMessageBody::Gossip(entries),
        }
    }

    #[inline]
    pub fn meet(sender_id: NodeId, config_epoch: u64, entries: Vec<GossipEntry>) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::Meet, sender_id, config_epoch),
            body: ClusterMessageBody::Gossip(entries),
        }
    }

    #[inline]
    pub fn update(sender_id: NodeId, config_epoch: u64, update: UpdateMessage) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::Update, sender_id, config_epoch),
            body: ClusterMessageBody::Update(update),
        }
    }

    #[inline]
    pub fn fail(sender_id: NodeId, config_epoch: u64, failed_node_id: NodeId) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::Fail, sender_id, config_epoch),
            body: ClusterMessageBody::Fail { failed_node_id },
        }
    }

    #[inline]
    pub fn failover_auth(
        sender_id: NodeId,
        config_epoch: u64,
        request: FailoverAuthRequest,
    ) -> Self {
        Self {
            header: ClusterMessageHeader::new(MessageType::FailoverAuth, sender_id, config_epoch),
            body: ClusterMessageBody::FailoverAuth(request),
        }
    }

    #[inline]
    pub fn failover_auth_ack(sender_id: NodeId, config_epoch: u64, ack: FailoverAuthAck) -> Self {
        Self {
            header: ClusterMessageHeader::new(
                MessageType::FailoverAuthAck,
                sender_id,
                config_epoch,
            ),
            body: ClusterMessageBody::FailoverAuthAck(ack),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.header.encode_into(&mut out);
        match &self.body {
            ClusterMessageBody::Empty => {}
            ClusterMessageBody::Gossip(entries) => {
                out.extend_from_slice(&(entries.len() as u16).to_be_bytes());
                for entry in entries {
                    entry.encode_into(&mut out);
                }
            }
            ClusterMessageBody::Update(update) => update.encode_into(&mut out),
            ClusterMessageBody::Fail { failed_node_id } => {
                out.extend_from_slice(failed_node_id.as_bytes());
            }
            ClusterMessageBody::FailoverAuth(request) => request.encode_into(&mut out),
            ClusterMessageBody::FailoverAuthAck(ack) => ack.encode_into(&mut out),
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GossipProtocolError> {
        let mut cursor = Cursor::new(bytes);
        let header = ClusterMessageHeader::decode(&mut cursor)?;
        let body = match header.msg_type {
            MessageType::Ping | MessageType::Pong | MessageType::Meet => {
                let count = cursor.take_u16()? as usize;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push(GossipEntry::decode(&mut cursor)?);
                }
                ClusterMessageBody::Gossip(entries)
            }
            MessageType::Update => ClusterMessageBody::Update(UpdateMessage::decode(&mut cursor)?),
            MessageType::Fail => ClusterMessageBody::Fail {
                failed_node_id: NodeId::new(cursor.take::<20>()?),
            },
            MessageType::FailoverAuth => {
                ClusterMessageBody::FailoverAuth(FailoverAuthRequest::decode(&mut cursor)?)
            }
            MessageType::FailoverAuthAck => {
                ClusterMessageBody::FailoverAuthAck(FailoverAuthAck::decode(&mut cursor)?)
            }
        };
        cursor.finish()?;
        Ok(Self { header, body })
    }

    #[inline]
    pub fn encoded_len(&self) -> usize {
        match &self.body {
            ClusterMessageBody::Empty => CLUSTER_HEADER_LEN,
            ClusterMessageBody::Gossip(entries) => {
                CLUSTER_HEADER_LEN + 2 + (entries.len() * GOSSIP_ENTRY_LEN)
            }
            ClusterMessageBody::Update(_) => CLUSTER_HEADER_LEN + UPDATE_ENTRY_LEN,
            ClusterMessageBody::Fail { .. } => CLUSTER_HEADER_LEN + FAIL_MESSAGE_LEN,
            ClusterMessageBody::FailoverAuth(_) => CLUSTER_HEADER_LEN + FAILOVER_AUTH_REQ_LEN,
            ClusterMessageBody::FailoverAuthAck(_) => CLUSTER_HEADER_LEN + FAILOVER_AUTH_ACK_LEN,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GossipProtocolError {
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidMessageType(u8),
    Truncated,
    TrailingBytes(usize),
    InvalidIp([u8; 16]),
}

impl std::fmt::Display for GossipProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic(magic) => write!(f, "invalid cluster magic: {magic:?}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported cluster bus version {version}")
            }
            Self::InvalidMessageType(msg_type) => {
                write!(f, "invalid cluster message type {msg_type}")
            }
            Self::Truncated => write!(f, "truncated cluster message"),
            Self::TrailingBytes(count) => write!(f, "cluster message has {count} trailing bytes"),
            Self::InvalidIp(bytes) => write!(f, "invalid encoded socket ip: {bytes:?}"),
        }
    }
}

impl std::error::Error for GossipProtocolError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Udp,
    Tcp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundEnvelope {
    pub transport: Transport,
    pub addr: SocketAddr,
    pub message: ClusterMessage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergeSummary {
    pub adopted: bool,
    pub correction_sent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplicationTracker {
    replication_id: [u8; 16],
    replication_offset: u64,
    primary_repl_id: [u8; 16],
    primary_repl_offset: u64,
    rtt_ms: Option<u64>,
}

impl ReplicationTracker {
    fn new(rng: &mut SmallRng) -> Self {
        let mut replication_id = [0_u8; 16];
        rng.fill(&mut replication_id);
        Self {
            replication_id,
            replication_offset: 0,
            primary_repl_id: [0; 16],
            primary_repl_offset: 0,
            rtt_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FailoverCampaign {
    failed_primary: NodeId,
    requested_epoch: u64,
    scheduled_at_ms: u64,
    auth_sent: bool,
    votes: HashSet<NodeId, RandomState>,
}

#[derive(Clone, Debug)]
pub struct GossipState {
    cluster: ClusterState,
    node_timeout_ms: u64,
    cluster_replica_no_failover: bool,
    failure_reports: HashMap<NodeId, HashSet<NodeId, RandomState>, RandomState>,
    ping_sent: HashMap<NodeId, u64, RandomState>,
    replication: HashMap<NodeId, ReplicationTracker, RandomState>,
    voted_epoch: Option<u64>,
    failover: Option<FailoverCampaign>,
}

impl GossipState {
    pub fn new(local_meta: NodeMeta) -> Self {
        let mut rng = SmallRng::seed_from_u64(0x5EED_1234_5678_9ABC);
        let local_id = local_meta.id;
        let mut cluster = ClusterState::with_local_node(local_meta);
        if let Some(local) = cluster.get_node_mut(&local_id) {
            local.pong_recv = 0;
        }
        let mut replication = HashMap::with_hasher(RandomState::new());
        replication.insert(local_id, ReplicationTracker::new(&mut rng));
        Self {
            cluster,
            node_timeout_ms: DEFAULT_NODE_TIMEOUT_MS,
            cluster_replica_no_failover: false,
            failure_reports: HashMap::with_hasher(RandomState::new()),
            ping_sent: HashMap::with_hasher(RandomState::new()),
            replication,
            voted_epoch: None,
            failover: None,
        }
    }

    #[inline]
    pub fn cluster(&self) -> &ClusterState {
        &self.cluster
    }

    #[inline]
    pub fn cluster_mut(&mut self) -> &mut ClusterState {
        &mut self.cluster
    }

    #[inline]
    pub fn local_node_id(&self) -> NodeId {
        self.cluster.local_node_id()
    }

    #[inline]
    pub fn set_node_timeout_ms(&mut self, timeout_ms: u64) {
        self.node_timeout_ms = timeout_ms;
    }

    #[inline]
    pub fn set_cluster_replica_no_failover(&mut self, disabled: bool) {
        self.cluster_replica_no_failover = disabled;
    }

    pub fn insert_node(&mut self, node: NodeMeta) {
        let node_id = node.id;
        self.cluster.insert_node(node);
        self.ensure_replication_tracker(node_id);
    }

    pub fn set_replication_offset(&mut self, node_id: NodeId, offset: u64) {
        self.ensure_replication_tracker(node_id).replication_offset = offset;
    }

    pub fn set_primary_progress(
        &mut self,
        node_id: NodeId,
        primary_repl_id: [u8; 16],
        primary_repl_offset: u64,
    ) {
        let tracker = self.ensure_replication_tracker(node_id);
        tracker.primary_repl_id = primary_repl_id;
        tracker.primary_repl_offset = primary_repl_offset;
    }

    #[inline]
    pub fn rtt_ms(&self, node_id: &NodeId) -> Option<u64> {
        self.replication
            .get(node_id)
            .and_then(|tracker| tracker.rtt_ms)
    }

    pub fn make_meet_message(&self) -> ClusterMessage {
        ClusterMessage::meet(
            self.local_node_id(),
            self.local_config_epoch(),
            vec![GossipEntry::from_meta(self.local_meta())],
        )
    }

    pub fn handle_message(
        &mut self,
        message: ClusterMessage,
        source_addr: SocketAddr,
        now_ms: u64,
        rng: &mut SmallRng,
    ) -> Result<Vec<OutboundEnvelope>, GossipProtocolError> {
        let mut out = Vec::new();
        let sender_id = message.header.sender_id;

        match message.body {
            ClusterMessageBody::Gossip(entries) => {
                self.ensure_sender_from_entries(sender_id, source_addr, &entries)?;
                let sender_cluster_addr = self
                    .cluster
                    .get_node(&sender_id)
                    .map(|meta| meta.cluster_addr)
                    .unwrap_or(source_addr);

                if matches!(message.header.msg_type, MessageType::Pong) {
                    self.observe_pong(sender_id, now_ms);
                }

                for entry in entries {
                    self.merge_gossip_entry(sender_id, sender_cluster_addr, entry, &mut out)?;
                }

                if matches!(
                    message.header.msg_type,
                    MessageType::Ping | MessageType::Meet
                ) {
                    out.push(OutboundEnvelope {
                        transport: Transport::Udp,
                        addr: source_addr,
                        message: ClusterMessage::pong(
                            self.local_node_id(),
                            self.local_config_epoch(),
                            self.build_gossip_entries(rng),
                        ),
                    });
                }
            }
            ClusterMessageBody::Update(update) => {
                self.apply_update(sender_id, source_addr, update, &mut out)?;
            }
            ClusterMessageBody::Fail { failed_node_id } => {
                if self.mark_failed(failed_node_id) {
                    out.extend(self.handle_failed_primary(now_ms, rng));
                }
            }
            ClusterMessageBody::FailoverAuth(request) => {
                if let Some(ack) = self.process_failover_auth(sender_id, request, source_addr) {
                    out.push(ack);
                }
            }
            ClusterMessageBody::FailoverAuthAck(ack) => {
                out.extend(self.process_failover_ack(sender_id, ack, now_ms, rng));
            }
            ClusterMessageBody::Empty => {}
        }

        Ok(out)
    }

    pub fn tick(&mut self, now_ms: u64, rng: &mut SmallRng) -> Vec<OutboundEnvelope> {
        let mut out = Vec::new();
        let newly_failed = self.detect_failures(now_ms);
        for failed in newly_failed {
            out.extend(self.broadcast_fail(failed));
        }

        out.extend(self.handle_failed_primary(now_ms, rng));

        let ping_entries = self.build_gossip_entries(rng);
        for target in self.select_ping_targets(rng) {
            if let Some(cluster_addr) = self.cluster.get_node(&target).map(|meta| meta.cluster_addr)
            {
                self.ping_sent.insert(target, now_ms);
                if let Some(local) = self.cluster.get_node_mut(&target) {
                    local.ping_sent = now_ms;
                }
                out.push(OutboundEnvelope {
                    transport: Transport::Udp,
                    addr: cluster_addr,
                    message: ClusterMessage::ping(
                        self.local_node_id(),
                        self.local_config_epoch(),
                        ping_entries.clone(),
                    ),
                });
            }
        }

        if let Some(campaign) = &self.failover
            && campaign.auth_sent
            && self.can_promote_failover(campaign.failed_primary)
            && self.failover_vote_count(campaign.failed_primary, &campaign.votes)
                >= self.required_failover_votes(campaign.failed_primary)
        {
            out.extend(self.promote_local(now_ms, rng));
        }

        out
    }

    fn local_meta(&self) -> &NodeMeta {
        self.cluster
            .get_node(&self.local_node_id())
            .expect("local node metadata missing")
    }

    fn local_meta_mut(&mut self) -> &mut NodeMeta {
        let local_node_id = self.local_node_id();
        self.cluster
            .get_node_mut(&local_node_id)
            .expect("local node metadata missing")
    }

    fn local_config_epoch(&self) -> u64 {
        self.local_meta().config_epoch
    }

    fn ensure_replication_tracker(&mut self, node_id: NodeId) -> &mut ReplicationTracker {
        self.replication.entry(node_id).or_insert_with(|| {
            let mut rng = SmallRng::seed_from_u64(u64::from_be_bytes(
                node_id.as_bytes()[0..8].try_into().unwrap_or([0; 8]),
            ));
            ReplicationTracker::new(&mut rng)
        })
    }

    fn ensure_sender_from_entries(
        &mut self,
        sender_id: NodeId,
        source_addr: SocketAddr,
        entries: &[GossipEntry],
    ) -> Result<(), GossipProtocolError> {
        if self.cluster.get_node(&sender_id).is_some() {
            return Ok(());
        }

        if let Some(entry) = entries.iter().find(|entry| entry.node_id == sender_id) {
            let meta = NodeMeta {
                id: sender_id,
                addr: entry.addr()?,
                cluster_addr: entry.cluster_addr()?,
                role: NodeRole::Primary,
                state: entry.node_state(),
                ping_sent: 0,
                pong_recv: entry.pong_received,
                config_epoch: entry.config_epoch,
                slots: RoaringBitmap::new(),
            };
            self.insert_node(meta);
            return Ok(());
        }

        let placeholder = NodeMeta {
            id: sender_id,
            addr: source_addr,
            cluster_addr: source_addr,
            role: NodeRole::Primary,
            state: NodeState::Connected,
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 0,
            slots: RoaringBitmap::new(),
        };
        self.insert_node(placeholder);
        Ok(())
    }

    fn select_ping_targets(&self, rng: &mut SmallRng) -> Vec<NodeId> {
        let mut peers = self
            .cluster
            .iter()
            .filter(|node| node.id != self.local_node_id() && node.state != NodeState::Failed)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        peers.shuffle(rng);
        let cluster_size = self.cluster.nodes().len().max(1);
        let target_count = usize::max(3, integer_sqrt(cluster_size)).min(peers.len());
        peers.truncate(target_count);
        peers
    }

    fn build_gossip_entries(&self, rng: &mut SmallRng) -> Vec<GossipEntry> {
        let mut selected = HashSet::with_hasher(RandomState::new());
        let mut entries = Vec::new();
        entries.push(GossipEntry::from_meta(self.local_meta()));
        selected.insert(self.local_node_id());

        let cluster_size = self.cluster.nodes().len();
        let sample_size = usize::min(6, cluster_size / 2);
        let suspected = self
            .cluster
            .iter()
            .filter(|node| node.state == NodeState::PFail || node.state == NodeState::Failed)
            .map(|node| node.id)
            .collect::<Vec<_>>();

        for node_id in suspected {
            if selected.insert(node_id)
                && let Some(node) = self.cluster.get_node(&node_id)
            {
                entries.push(GossipEntry::from_meta(node));
            }
        }

        let mut candidates = self
            .cluster
            .iter()
            .filter(|node| node.id != self.local_node_id())
            .map(|node| node.id)
            .collect::<Vec<_>>();
        candidates.shuffle(rng);

        for node_id in candidates.into_iter().take(sample_size) {
            if selected.insert(node_id)
                && let Some(node) = self.cluster.get_node(&node_id)
            {
                entries.push(GossipEntry::from_meta(node));
            }
        }

        entries
    }

    fn merge_gossip_entry(
        &mut self,
        reporter: NodeId,
        sender_cluster_addr: SocketAddr,
        entry: GossipEntry,
        out: &mut Vec<OutboundEnvelope>,
    ) -> Result<MergeSummary, GossipProtocolError> {
        let received_state = entry.node_state();
        let mut adopted = false;
        let mut correction_sent = false;
        let new_addr = entry.addr()?;
        let new_cluster_addr = entry.cluster_addr()?;

        match self.cluster.get_node(&entry.node_id).cloned() {
            Some(local) => {
                if entry.config_epoch > local.config_epoch {
                    let node = self
                        .cluster
                        .get_node_mut(&entry.node_id)
                        .expect("node disappeared during gossip merge");
                    node.addr = new_addr;
                    node.cluster_addr = new_cluster_addr;
                    node.state = received_state;
                    node.pong_recv = entry.pong_received;
                    node.config_epoch = entry.config_epoch;
                    adopted = true;
                } else if entry.config_epoch == local.config_epoch {
                    if received_state == NodeState::Failed && local.state != NodeState::Failed {
                        self.cluster
                            .get_node_mut(&entry.node_id)
                            .expect("node disappeared during fail merge")
                            .state = NodeState::Failed;
                        adopted = true;
                    }
                } else if let Some(correct) = self.make_update_for(entry.node_id) {
                    out.push(OutboundEnvelope {
                        transport: Transport::Tcp,
                        addr: sender_cluster_addr,
                        message: correct,
                    });
                    correction_sent = true;
                }
            }
            None => {
                let meta = NodeMeta {
                    id: entry.node_id,
                    addr: new_addr,
                    cluster_addr: new_cluster_addr,
                    role: NodeRole::Primary,
                    state: received_state,
                    ping_sent: 0,
                    pong_recv: entry.pong_received,
                    config_epoch: entry.config_epoch,
                    slots: RoaringBitmap::new(),
                };
                self.insert_node(meta);
                adopted = true;
            }
        }

        if received_state.is_fail_like() {
            self.failure_reports
                .entry(entry.node_id)
                .or_insert_with(|| HashSet::with_hasher(RandomState::new()))
                .insert(reporter);
        }

        Ok(MergeSummary {
            adopted,
            correction_sent,
        })
    }

    fn apply_update(
        &mut self,
        sender_id: NodeId,
        sender_addr: SocketAddr,
        update: UpdateMessage,
        out: &mut Vec<OutboundEnvelope>,
    ) -> Result<(), GossipProtocolError> {
        let incoming = update.to_meta()?;
        let incoming_id = incoming.id;
        let incoming_epoch = incoming.config_epoch;

        if let Some(existing) = self.cluster.get_node(&incoming_id).cloned() {
            if incoming_epoch < existing.config_epoch {
                if let Some(correct) = self.make_update_for(incoming_id) {
                    out.push(OutboundEnvelope {
                        transport: Transport::Tcp,
                        addr: sender_addr,
                        message: correct,
                    });
                }
                return Ok(());
            }

            if incoming_epoch == existing.config_epoch {
                if incoming.state == NodeState::Failed && existing.state != NodeState::Failed {
                    self.cluster
                        .get_node_mut(&incoming_id)
                        .expect("node disappeared during update fail merge")
                        .state = NodeState::Failed;
                }

                let conflicts = self
                    .cluster
                    .iter()
                    .filter(|node| {
                        node.id != incoming_id
                            && node.role == NodeRole::Primary
                            && !slot_sets_disjoint(&node.slots, &incoming.slots)
                            && node.config_epoch == incoming_epoch
                    })
                    .map(|node| node.id)
                    .collect::<Vec<_>>();

                for conflict_id in conflicts {
                    if conflict_id > incoming_id {
                        if let Some(correct) = self.make_update_for(conflict_id) {
                            out.push(OutboundEnvelope {
                                transport: Transport::Tcp,
                                addr: sender_addr,
                                message: correct,
                            });
                        }
                        return Ok(());
                    }

                    if conflict_id == self.local_node_id() {
                        let new_epoch = self.local_meta().config_epoch.saturating_add(1);
                        self.local_meta_mut().config_epoch = new_epoch;
                        if let Some(correct) = self.make_update_for(self.local_node_id()) {
                            for target in self.broadcast_targets() {
                                out.push(OutboundEnvelope {
                                    transport: Transport::Tcp,
                                    addr: target,
                                    message: correct.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        if incoming.id != self.local_node_id() {
            self.cluster.insert_node(incoming.clone());
        } else if incoming.role == NodeRole::Primary && self.local_meta().role == NodeRole::Primary
        {
            let local_epoch = self.local_meta().config_epoch;
            if incoming.config_epoch > local_epoch
                && !slot_sets_disjoint(&incoming.slots, &self.local_meta().slots)
            {
                self.local_meta_mut().role = NodeRole::Replica {
                    primary: incoming.id,
                };
            }
        }

        if incoming.state == NodeState::Failed && self.mark_failed(incoming.id) {
            out.extend(self.handle_failed_primary(0, &mut SmallRng::seed_from_u64(7)));
        }

        if incoming.role == NodeRole::Primary
            && incoming.config_epoch > self.local_meta().config_epoch
            && self.local_meta().role == NodeRole::Primary
            && !slot_sets_disjoint(&incoming.slots, &self.local_meta().slots)
        {
            self.local_meta_mut().role = NodeRole::Replica {
                primary: incoming.id,
            };
        }

        if self.cluster.get_node(&sender_id).is_none() {
            let placeholder = NodeMeta {
                id: sender_id,
                addr: sender_addr,
                cluster_addr: sender_addr,
                role: NodeRole::Primary,
                state: NodeState::Connected,
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 0,
                slots: RoaringBitmap::new(),
            };
            self.insert_node(placeholder);
        }

        Ok(())
    }

    fn observe_pong(&mut self, sender_id: NodeId, now_ms: u64) {
        if let Some(node) = self.cluster.get_node_mut(&sender_id) {
            node.pong_recv = now_ms;
            if node.state == NodeState::PFail {
                node.state = NodeState::Connected;
            }
        }

        if let Some(sent_at) = self.ping_sent.remove(&sender_id) {
            let rtt = now_ms.saturating_sub(sent_at);
            self.ensure_replication_tracker(sender_id).rtt_ms = Some(rtt);
        }

        let local_node_id = self.local_node_id();
        if let Some(reporters) = self.failure_reports.get_mut(&sender_id) {
            reporters.remove(&local_node_id);
        }
    }

    fn detect_failures(&mut self, now_ms: u64) -> Vec<NodeId> {
        let local_node_id = self.local_node_id();
        let mut newly_failed = Vec::new();
        let cluster_majority = (self.cluster.nodes().len() / 2) + 1;
        let mut ids = self
            .cluster
            .iter()
            .filter(|node| node.id != local_node_id)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        ids.sort_unstable();

        for node_id in ids {
            let mut mark_pfail = false;
            if let Some(node) = self.cluster.get_node(&node_id)
                && node.state != NodeState::Failed
                && now_ms.saturating_sub(node.pong_recv) > self.node_timeout_ms
            {
                mark_pfail = true;
            }

            if mark_pfail {
                if let Some(node) = self.cluster.get_node_mut(&node_id) {
                    node.state = NodeState::PFail;
                }
                self.failure_reports
                    .entry(node_id)
                    .or_insert_with(|| HashSet::with_hasher(RandomState::new()))
                    .insert(local_node_id);
            }

            let report_count = self.failure_reports.get(&node_id).map_or(0, HashSet::len);
            if report_count >= cluster_majority && self.mark_failed(node_id) {
                newly_failed.push(node_id);
            }
        }

        newly_failed
    }

    fn mark_failed(&mut self, node_id: NodeId) -> bool {
        if let Some(node) = self.cluster.get_node_mut(&node_id) {
            if node.state == NodeState::Failed {
                return false;
            }
            node.state = NodeState::Failed;
            return true;
        }
        false
    }

    fn broadcast_fail(&self, failed_node_id: NodeId) -> Vec<OutboundEnvelope> {
        let message = ClusterMessage::fail(
            self.local_node_id(),
            self.local_config_epoch(),
            failed_node_id,
        );
        self.broadcast_targets()
            .into_iter()
            .map(|addr| OutboundEnvelope {
                transport: Transport::Udp,
                addr,
                message: message.clone(),
            })
            .collect()
    }

    fn handle_failed_primary(&mut self, now_ms: u64, rng: &mut SmallRng) -> Vec<OutboundEnvelope> {
        let mut out = Vec::new();
        let NodeRole::Replica { primary } = self.local_meta().role.clone() else {
            return out;
        };

        if self.cluster_replica_no_failover {
            return out;
        }

        let Some(primary_meta) = self.cluster.get_node(&primary) else {
            return out;
        };

        if primary_meta.state != NodeState::Failed {
            return out;
        }

        if self.failover.is_none() {
            self.schedule_failover(now_ms, rng.gen_range(0..FAILOVER_JITTER_MS));
        }

        let failover_snapshot = self.failover.as_ref().map(|campaign| {
            (
                campaign.failed_primary,
                campaign.requested_epoch,
                campaign.scheduled_at_ms,
                campaign.auth_sent,
            )
        });
        if let Some((failed_primary, requested_epoch, scheduled_at_ms, auth_sent)) =
            failover_snapshot
            && !auth_sent
            && now_ms >= scheduled_at_ms
        {
            if let Some(campaign) = self.failover.as_mut() {
                campaign.auth_sent = true;
            }
            if self.total_primary_count() <= 1 {
                out.extend(self.promote_local(now_ms, rng));
            } else {
                let local_node_id = self.local_node_id();
                let replication_offset = self
                    .replication
                    .get(&local_node_id)
                    .map_or(0, |tracker| tracker.primary_repl_offset);
                let slot_bitmap = slots_to_bitmap(&self.local_meta().slots);
                let request = FailoverAuthRequest {
                    requesting_node_id: local_node_id,
                    config_epoch: requested_epoch,
                    replication_offset,
                    slot_bitmap,
                };
                let message =
                    ClusterMessage::failover_auth(local_node_id, requested_epoch, request);
                for addr in self.primary_targets(failed_primary) {
                    out.push(OutboundEnvelope {
                        transport: Transport::Udp,
                        addr,
                        message: message.clone(),
                    });
                }
            }
        }

        out
    }

    fn process_failover_auth(
        &mut self,
        sender_id: NodeId,
        request: FailoverAuthRequest,
        source_addr: SocketAddr,
    ) -> Option<OutboundEnvelope> {
        let local = self.local_meta().clone();
        if local.role != NodeRole::Primary || local.state.is_fail_like() {
            return None;
        }

        if self.voted_epoch == Some(request.config_epoch) {
            return None;
        }

        let requester = self.cluster.get_node(&request.requesting_node_id)?;
        let NodeRole::Replica { primary } = requester.role.clone() else {
            return None;
        };
        let failed_primary = self.cluster.get_node(&primary)?;
        if failed_primary.state != NodeState::Failed {
            return None;
        }

        self.voted_epoch = Some(request.config_epoch);
        Some(OutboundEnvelope {
            transport: Transport::Udp,
            addr: source_addr,
            message: ClusterMessage::failover_auth_ack(
                self.local_node_id(),
                local.config_epoch,
                FailoverAuthAck {
                    requesting_node_id: sender_id,
                    config_epoch: request.config_epoch,
                },
            ),
        })
    }

    fn process_failover_ack(
        &mut self,
        sender_id: NodeId,
        ack: FailoverAuthAck,
        now_ms: u64,
        rng: &mut SmallRng,
    ) -> Vec<OutboundEnvelope> {
        let mut out = Vec::new();
        if ack.requesting_node_id != self.local_node_id() {
            return out;
        }

        let Some(campaign) = &mut self.failover else {
            return out;
        };

        if ack.config_epoch != campaign.requested_epoch {
            return out;
        }

        campaign.votes.insert(sender_id);
        let failed_primary = campaign.failed_primary;
        let votes = campaign.votes.clone();
        if self.failover_vote_count(failed_primary, &votes)
            >= self.required_failover_votes(failed_primary)
        {
            out.extend(self.promote_local(now_ms, rng));
        }

        out
    }

    fn schedule_failover(&mut self, now_ms: u64, jitter_ms: u64) {
        let local_id = self.local_node_id();
        let NodeRole::Replica { primary } = self.local_meta().role.clone() else {
            return;
        };
        let local_offset = self
            .replication
            .get(&local_id)
            .map_or(0, |tracker| tracker.primary_repl_offset);
        let best_offset = self
            .cluster
            .iter()
            .filter_map(|node| match node.role {
                NodeRole::Replica {
                    primary: node_primary,
                } if node_primary == primary => Some(
                    self.replication
                        .get(&node.id)
                        .map_or(0, |tracker| tracker.primary_repl_offset),
                ),
                _ => None,
            })
            .max()
            .unwrap_or(local_offset);
        let extra_delay = best_offset.saturating_sub(local_offset).min(2_000);

        self.failover = Some(FailoverCampaign {
            failed_primary: primary,
            requested_epoch: self.local_meta().config_epoch.saturating_add(1),
            scheduled_at_ms: now_ms + FAILOVER_DELAY_MS + jitter_ms + extra_delay,
            auth_sent: false,
            votes: HashSet::with_hasher(RandomState::new()),
        });
    }

    fn promote_local(&mut self, _now_ms: u64, rng: &mut SmallRng) -> Vec<OutboundEnvelope> {
        let Some(campaign) = self.failover.take() else {
            return Vec::new();
        };
        if !self.can_promote_failover(campaign.failed_primary) {
            return Vec::new();
        }

        let failed_primary = campaign.failed_primary;
        let claimed_slots = self
            .cluster
            .get_node(&failed_primary)
            .map(|node| node.slots.clone())
            .unwrap_or_default();
        let new_epoch = self.local_meta().config_epoch.saturating_add(1);
        let new_replication_id = {
            let mut id = [0_u8; 16];
            rng.fill(&mut id);
            id
        };

        {
            let local = self.local_meta_mut();
            local.role = NodeRole::Primary;
            local.config_epoch = new_epoch;
            local.state = NodeState::Connected;
            local.slots = claimed_slots.clone();
        }

        if let Some(tracker) = self.replication.get_mut(&self.local_node_id()) {
            tracker.replication_id = new_replication_id;
            tracker.primary_repl_id = [0; 16];
            tracker.primary_repl_offset = 0;
        }

        let local_node_id = self.local_node_id();
        if let Some(primary) = self.cluster.get_node_mut(&failed_primary) {
            primary.state = NodeState::Failed;
            primary.slots.clear();
            primary.role = NodeRole::Replica {
                primary: local_node_id,
            };
            primary.config_epoch = new_epoch;
        }

        let mut out = Vec::new();
        if let Some(local_update) = self.make_update_for(self.local_node_id()) {
            for addr in self.broadcast_targets() {
                out.push(OutboundEnvelope {
                    transport: Transport::Tcp,
                    addr,
                    message: local_update.clone(),
                });
            }
        }
        if let Some(primary_update) = self.make_update_for(failed_primary) {
            for addr in self.broadcast_targets() {
                out.push(OutboundEnvelope {
                    transport: Transport::Tcp,
                    addr,
                    message: primary_update.clone(),
                });
            }
        }
        out
    }

    fn can_promote_failover(&self, failed_primary: NodeId) -> bool {
        let Some(node) = self.cluster.get_node(&failed_primary) else {
            return false;
        };
        node.state == NodeState::Failed
    }

    fn total_primary_count(&self) -> usize {
        self.cluster
            .iter()
            .filter(|node| node.role == NodeRole::Primary)
            .count()
    }

    fn required_failover_votes(&self, _failed_primary: NodeId) -> usize {
        let total_primaries = self.total_primary_count();
        if total_primaries <= 1 {
            1
        } else {
            (total_primaries / 2) + 1
        }
    }

    fn failover_vote_count(
        &self,
        failed_primary: NodeId,
        votes: &HashSet<NodeId, RandomState>,
    ) -> usize {
        if self.total_primary_count() <= 1 && failed_primary != self.local_node_id() {
            1
        } else {
            votes.len()
        }
    }

    fn primary_targets(&self, failed_primary: NodeId) -> Vec<SocketAddr> {
        self.cluster
            .iter()
            .filter(|node| {
                node.id != failed_primary
                    && node.id != self.local_node_id()
                    && node.role == NodeRole::Primary
                    && node.state != NodeState::Failed
            })
            .map(|node| node.cluster_addr)
            .collect()
    }

    fn make_update_for(&self, node_id: NodeId) -> Option<ClusterMessage> {
        self.cluster.get_node(&node_id).map(|node| {
            ClusterMessage::update(
                self.local_node_id(),
                self.local_config_epoch(),
                UpdateMessage::from_meta(node),
            )
        })
    }

    fn broadcast_targets(&self) -> Vec<SocketAddr> {
        self.cluster
            .iter()
            .filter(|node| node.id != self.local_node_id() && node.state != NodeState::Failed)
            .map(|node| node.cluster_addr)
            .collect()
    }
}

pub struct GossipTask {
    state: Rc<RefCell<GossipState>>,
    udp_socket: UdpSocket,
    tcp_listener: TcpListener,
}

impl GossipTask {
    pub async fn bind(state: Rc<RefCell<GossipState>>, bind_addr: SocketAddr) -> io::Result<Self> {
        let udp_socket = UdpSocket::bind(bind_addr).await?;
        let tcp_listener = TcpListener::bind(bind_addr).await?;
        Ok(Self {
            state,
            udp_socket,
            tcp_listener,
        })
    }

    pub fn spawn(self) -> JoinHandle<io::Result<()>> {
        spawn(async move { self.run().await })
    }

    pub async fn run(self) -> io::Result<()> {
        let tick_socket = self.udp_socket.clone();
        let udp_socket = self.udp_socket.clone();
        let tcp_send_socket = self.udp_socket.clone();
        let tcp_listener = self.tcp_listener.clone();
        let tick_state = Rc::clone(&self.state);
        let udp_state = Rc::clone(&self.state);
        let tcp_state = Rc::clone(&self.state);

        futures_util::try_join!(
            async move {
                let mut ticks = interval(Duration::from_millis(PING_PERIOD_MS));
                let mut rng = SmallRng::from_entropy();
                loop {
                    ticks.tick().await;
                    let now_ms = current_unix_ms();
                    let envelopes = tick_state.borrow_mut().tick(now_ms, &mut rng);
                    send_envelopes(&tick_socket, envelopes).await?;
                }
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            },
            async move {
                let mut rng = SmallRng::from_entropy();
                loop {
                    let BufResult(result, buffer) =
                        udp_socket.recv_from(Vec::with_capacity(8192)).await;
                    let (size, source) = result?;
                    let payload = &buffer[..size];
                    let message = ClusterMessage::decode(payload)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    let now_ms = current_unix_ms();
                    let envelopes = udp_state
                        .borrow_mut()
                        .handle_message(message, source, now_ms, &mut rng)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    send_envelopes(&udp_socket, envelopes).await?;
                }
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            },
            async move {
                let mut rng = SmallRng::from_entropy();
                loop {
                    let (mut stream, source) = tcp_listener.accept().await?;
                    let BufResult(result, buffer) =
                        stream.read_to_end(Vec::with_capacity(16 * 1024)).await;
                    let _ = result?;
                    let message = ClusterMessage::decode(&buffer)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    let now_ms = current_unix_ms();
                    let envelopes = tcp_state
                        .borrow_mut()
                        .handle_message(message, source, now_ms, &mut rng)
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    send_envelopes(&tcp_send_socket, envelopes).await?;
                }
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            },
        )?;

        Ok(())
    }

    pub async fn send_meet(&self, target: SocketAddr) -> io::Result<()> {
        let message = self.state.borrow().make_meet_message().encode();
        let BufResult(result, _) = self.udp_socket.send_to(message, target).await;
        result.map(|_| ())
    }
}

async fn send_envelopes(socket: &UdpSocket, envelopes: Vec<OutboundEnvelope>) -> io::Result<()> {
    for envelope in envelopes {
        match envelope.transport {
            Transport::Udp => {
                let payload = envelope.message.encode();
                let BufResult(result, _) = socket.send_to(payload, envelope.addr).await;
                result?;
            }
            Transport::Tcp => {
                let payload = envelope.message.encode();
                let mut stream = TcpStream::connect(envelope.addr).await?;
                let BufResult(result, _) = stream.write_all(payload).await;
                result?;
                let _ = stream.shutdown().await;
            }
        }
    }
    Ok(())
}

#[inline]
pub fn current_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn role_flag(meta: &NodeMeta) -> u16 {
    match meta.role {
        NodeRole::Primary => ROLE_PRIMARY_FLAG,
        NodeRole::Replica { .. } => ROLE_REPLICA_FLAG,
    }
}

fn encode_role(role: &NodeRole) -> (u16, NodeId) {
    match role {
        NodeRole::Primary => (ROLE_PRIMARY_FLAG, NodeId::ZERO),
        NodeRole::Replica { primary } => (ROLE_REPLICA_FLAG, *primary),
    }
}

fn decode_role(flags: u16, primary: NodeId) -> NodeRole {
    if (flags & ROLE_REPLICA_FLAG) != 0 {
        NodeRole::Replica { primary }
    } else {
        NodeRole::Primary
    }
}

fn encode_addrs(addr: SocketAddr, cluster_addr: SocketAddr) -> ([u8; 16], u16, u16) {
    (encode_ip(addr.ip()), addr.port(), cluster_addr.port())
}

fn encode_ip(ip: IpAddr) -> [u8; 16] {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
        IpAddr::V6(v6) => v6.octets(),
    }
}

fn decode_socket_addr(ip: [u8; 16], port: u16) -> Result<SocketAddr, GossipProtocolError> {
    let ipv6 = Ipv6Addr::from(ip);
    if let Some(v4) = ipv6.to_ipv4_mapped() {
        return Ok(SocketAddr::new(IpAddr::V4(v4), port));
    }
    if ipv6.is_unspecified() {
        return Err(GossipProtocolError::InvalidIp(ip));
    }
    Ok(SocketAddr::new(IpAddr::V6(ipv6), port))
}

fn slot_summary(slots: &RoaringBitmap) -> [u8; SLOT_SUMMARY_BYTES] {
    let mut summary = [0_u8; SLOT_SUMMARY_BYTES];
    for slot in slots.iter() {
        let primary = (slot as usize) & ((SLOT_SUMMARY_BYTES * 8) - 1);
        let secondary = ((slot as usize) * 131) & ((SLOT_SUMMARY_BYTES * 8) - 1);
        summary[primary / 8] |= 1 << (primary % 8);
        summary[secondary / 8] |= 1 << (secondary % 8);
    }
    summary
}

fn slots_to_bitmap(slots: &RoaringBitmap) -> [u8; SLOT_BITMAP_BYTES] {
    let mut bitmap = [0_u8; SLOT_BITMAP_BYTES];
    for slot in slots.iter() {
        let index = slot as usize;
        bitmap[index / 8] |= 1 << (index % 8);
    }
    bitmap
}

fn bitmap_to_slots(bitmap: &[u8; SLOT_BITMAP_BYTES]) -> RoaringBitmap {
    let mut slots = RoaringBitmap::new();
    for (byte_index, byte) in bitmap.iter().copied().enumerate() {
        if byte == 0 {
            continue;
        }
        for bit in 0..8 {
            if (byte & (1 << bit)) != 0 {
                slots.insert((byte_index * 8 + bit) as u32);
            }
        }
    }
    slots
}

fn slot_sets_disjoint(left: &RoaringBitmap, right: &RoaringBitmap) -> bool {
    let (small, large) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    small.iter().all(|slot| !large.contains(slot))
}

fn integer_sqrt(value: usize) -> usize {
    let mut x = 0usize;
    while (x + 1) * (x + 1) <= value {
        x += 1;
    }
    x
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], GossipProtocolError> {
        if self.offset + N > self.bytes.len() {
            return Err(GossipProtocolError::Truncated);
        }
        let value = self.bytes[self.offset..self.offset + N]
            .try_into()
            .expect("slice length checked");
        self.offset += N;
        Ok(value)
    }

    fn take_u8(&mut self) -> Result<u8, GossipProtocolError> {
        Ok(self.take::<1>()?[0])
    }

    fn take_u16(&mut self) -> Result<u16, GossipProtocolError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn take_u64(&mut self) -> Result<u64, GossipProtocolError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    fn finish(&self) -> Result<(), GossipProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(GossipProtocolError::TrailingBytes(
                self.bytes.len() - self.offset,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use rand::SeedableRng;
    use roaring::RoaringBitmap;
    use senko_cluster::{NodeId, NodeMeta, NodeRole, NodeState};

    use super::{
        ClusterMessage, ClusterMessageBody, GossipEntry, GossipState, MessageType, SmallRng,
        UpdateMessage, bitmap_to_slots, current_unix_ms, slot_summary, slots_to_bitmap,
    };
    use crate::cluster::gossip::Transport;

    fn make_node(id_byte: u8, port: u16) -> NodeMeta {
        NodeMeta {
            id: NodeId::new([id_byte; 20]),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            cluster_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 10_000),
            role: NodeRole::Primary,
            state: NodeState::Connected,
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            slots: RoaringBitmap::new(),
        }
    }

    fn with_slots(mut node: NodeMeta, range: std::ops::Range<u32>) -> NodeMeta {
        node.slots = range.collect();
        node
    }

    fn seeded_rng(seed: u64) -> SmallRng {
        SmallRng::seed_from_u64(seed)
    }

    fn deliver(
        states: &mut [GossipState],
        envelopes: Vec<super::OutboundEnvelope>,
        now_ms: u64,
        seed: u64,
    ) {
        let mut queue = VecDeque::from(envelopes);
        let mut rng = seeded_rng(seed);
        while let Some(envelope) = queue.pop_front() {
            let target = states
                .iter()
                .enumerate()
                .find_map(|(index, state)| {
                    state
                        .cluster()
                        .get_node(&state.local_node_id())
                        .and_then(|node| (node.cluster_addr == envelope.addr).then_some(index))
                })
                .or_else(|| {
                    states.iter().enumerate().find_map(|(index, state)| {
                        state
                            .cluster()
                            .get_node(&state.local_node_id())
                            .and_then(|node| (node.addr == envelope.addr).then_some(index))
                    })
                })
                .expect("target node exists");
            let responses = states[target]
                .handle_message(envelope.message, envelope.addr, now_ms, &mut rng)
                .expect("valid message");
            queue.extend(responses);
        }
    }

    #[test]
    fn ping_pong_roundtrip_serializes_losslessly() {
        let mut node = with_slots(make_node(1, 7000), 0..128);
        node.pong_recv = 777;
        let entry = GossipEntry::from_meta(&node);
        let message = ClusterMessage::ping(node.id, node.config_epoch, vec![entry.clone()]);
        let encoded = message.encode();
        let decoded = ClusterMessage::decode(&encoded).expect("decode cluster message");

        assert_eq!(decoded, message);
        assert_eq!(encoded.len(), message.encoded_len());
        assert_eq!(entry.slot_summary, slot_summary(&node.slots));
    }

    #[test]
    fn higher_config_epoch_wins_on_merge() {
        let local = make_node(1, 7000);
        let remote = make_node(2, 7001);
        let mut state = GossipState::new(local);
        state.insert_node(remote.clone());
        let mut upgraded = remote.clone();
        upgraded.config_epoch = 5;
        upgraded.state = NodeState::Handshaking;

        state
            .handle_message(
                ClusterMessage::pong(
                    upgraded.id,
                    upgraded.config_epoch,
                    vec![GossipEntry::from_meta(&upgraded)],
                ),
                upgraded.cluster_addr,
                1_000,
                &mut seeded_rng(1),
            )
            .expect("merge succeeds");

        let merged = state.cluster().get_node(&upgraded.id).expect("node exists");
        assert_eq!(merged.config_epoch, 5);
        assert_eq!(merged.state, NodeState::Handshaking);
    }

    #[test]
    fn fail_is_monotonic_at_same_epoch() {
        let local = make_node(1, 7000);
        let remote = make_node(2, 7001);
        let mut state = GossipState::new(local);
        state.insert_node(remote.clone());
        let mut failed = remote.clone();
        failed.state = NodeState::Failed;

        state
            .handle_message(
                ClusterMessage::pong(
                    failed.id,
                    failed.config_epoch,
                    vec![GossipEntry::from_meta(&failed)],
                ),
                failed.cluster_addr,
                1_000,
                &mut seeded_rng(2),
            )
            .expect("merge succeeds");

        assert_eq!(
            state.cluster().get_node(&failed.id).map(|node| node.state),
            Some(NodeState::Failed)
        );
    }

    #[test]
    fn config_epoch_conflict_higher_node_id_wins_and_loser_bumps_epoch() {
        let local = with_slots(make_node(1, 7000), 0..64);
        let remote = with_slots(make_node(2, 7001), 32..96);
        let mut state = GossipState::new(local.clone());
        state.insert_node(remote.clone());
        let update = UpdateMessage::from_meta(&remote);

        let out = state
            .handle_message(
                ClusterMessage::update(remote.id, remote.config_epoch, update),
                remote.cluster_addr,
                1_000,
                &mut seeded_rng(3),
            )
            .expect("update merge succeeds");

        assert!(state.local_meta().config_epoch > local.config_epoch);
        assert!(
            out.iter()
                .any(|envelope| envelope.transport == Transport::Tcp
                    && matches!(envelope.message.header.msg_type, MessageType::Update))
        );
    }

    #[test]
    fn failure_detection_marks_pfail_after_timeout() {
        let local = make_node(1, 7000);
        let mut remote = make_node(2, 7001);
        remote.pong_recv = 10;
        let mut state = GossipState::new(local);
        state.set_node_timeout_ms(100);
        state.insert_node(remote.clone());

        let _ = state.tick(200, &mut seeded_rng(4));

        assert_eq!(
            state.cluster().get_node(&remote.id).map(|node| node.state),
            Some(NodeState::PFail)
        );
    }

    #[test]
    fn fail_message_marks_node_failed_immediately() {
        let local = make_node(1, 7000);
        let remote = make_node(2, 7001);
        let mut state = GossipState::new(local);
        state.insert_node(remote.clone());

        state
            .handle_message(
                ClusterMessage::fail(remote.id, remote.config_epoch, remote.id),
                remote.cluster_addr,
                100,
                &mut seeded_rng(5),
            )
            .expect("fail applies");

        assert_eq!(
            state.cluster().get_node(&remote.id).map(|node| node.state),
            Some(NodeState::Failed)
        );
    }

    #[test]
    fn meet_adds_both_nodes() {
        let left = make_node(1, 7000);
        let right = make_node(2, 7001);
        let mut left_state = GossipState::new(left.clone());
        let mut right_state = GossipState::new(right.clone());

        let meet = left_state.make_meet_message();
        let responses = right_state
            .handle_message(meet, left.cluster_addr, 100, &mut seeded_rng(6))
            .expect("meet handled");

        assert!(right_state.cluster().get_node(&left.id).is_some());

        for response in responses {
            let _ = left_state
                .handle_message(
                    response.message,
                    right.cluster_addr,
                    100,
                    &mut seeded_rng(7),
                )
                .expect("pong handled");
        }

        assert!(left_state.cluster().get_node(&right.id).is_some());
    }

    #[test]
    fn ten_node_cluster_converges_within_five_rounds() {
        let mut states = (0..10)
            .map(|index| {
                let local = make_node((index + 1) as u8, 7100 + index as u16);
                let mut state = GossipState::new(local.clone());
                for peer in 0..10 {
                    if peer != index {
                        state.insert_node(make_node((peer + 1) as u8, 7100 + peer as u16));
                    }
                }
                state
            })
            .collect::<Vec<_>>();

        let mut changed = states[0]
            .cluster()
            .get_node(&states[0].local_node_id())
            .cloned()
            .expect("local node");
        changed.config_epoch = 9;
        changed.state = NodeState::Handshaking;
        states[0].cluster_mut().insert_node(changed.clone());

        let mut now = 0;
        for round in 0..5 {
            now += 100;
            let mut outbound = Vec::new();
            for (index, state) in states.iter_mut().enumerate() {
                let mut rng = seeded_rng(round as u64 * 31 + index as u64);
                outbound.extend(state.tick(now, &mut rng));
            }
            deliver(&mut states, outbound, now, round as u64 + 1000);
        }

        for state in &states[1..] {
            let node = state
                .cluster()
                .get_node(&changed.id)
                .expect("changed node known");
            assert_eq!(node.config_epoch, 9);
            assert_eq!(node.state, NodeState::Handshaking);
        }
    }

    #[test]
    fn highest_offset_replica_wins_failover_and_claims_slots() {
        let primary = with_slots(make_node(1, 7200), 0..256);
        let mut replica1 = make_node(2, 7201);
        replica1.role = NodeRole::Replica {
            primary: primary.id,
        };
        let mut replica2 = make_node(3, 7202);
        replica2.role = NodeRole::Replica {
            primary: primary.id,
        };

        let mut state1 = GossipState::new(replica1.clone());
        state1.insert_node(primary.clone());
        state1.insert_node(replica2.clone());
        state1.set_primary_progress(replica1.id, [1; 16], 100);
        state1.set_primary_progress(replica2.id, [1; 16], 200);

        let mut state2 = GossipState::new(replica2.clone());
        state2.insert_node(primary.clone());
        state2.insert_node(replica1.clone());
        state2.set_primary_progress(replica1.id, [1; 16], 100);
        state2.set_primary_progress(replica2.id, [1; 16], 200);

        let _ = state1
            .handle_message(
                ClusterMessage::fail(primary.id, primary.config_epoch, primary.id),
                primary.cluster_addr,
                100,
                &mut seeded_rng(8),
            )
            .expect("fail applied");
        let _ = state2
            .handle_message(
                ClusterMessage::fail(primary.id, primary.config_epoch, primary.id),
                primary.cluster_addr,
                100,
                &mut seeded_rng(9),
            )
            .expect("fail applied");

        state1.schedule_failover(100, 0);
        state2.schedule_failover(100, 0);

        let _out1 = state1.tick(650, &mut seeded_rng(10));
        let out2 = state2.tick(650, &mut seeded_rng(11));

        assert_ne!(
            state1
                .cluster()
                .get_node(&replica1.id)
                .map(|node| node.role.clone()),
            Some(NodeRole::Primary)
        );
        assert!(
            !out2.is_empty()
                || state2
                    .cluster()
                    .get_node(&replica2.id)
                    .map(|node| node.role.clone())
                    == Some(NodeRole::Primary)
        );

        assert_eq!(
            state2
                .cluster()
                .get_node(&replica2.id)
                .map(|node| node.role.clone()),
            Some(NodeRole::Primary)
        );
        assert_eq!(
            state2
                .cluster()
                .get_node(&replica2.id)
                .map(|node| node.slots.clone()),
            Some(primary.slots.clone())
        );
    }

    #[test]
    fn old_primary_is_demoted_when_new_primary_update_arrives() {
        let mut old_primary = with_slots(make_node(1, 7300), 0..128);
        let mut new_primary = with_slots(make_node(2, 7301), 0..128);
        new_primary.config_epoch = 5;
        let mut old_state = GossipState::new(old_primary.clone());
        old_state.insert_node(new_primary.clone());

        old_state
            .handle_message(
                ClusterMessage::update(
                    new_primary.id,
                    new_primary.config_epoch,
                    UpdateMessage::from_meta(&new_primary),
                ),
                new_primary.cluster_addr,
                200,
                &mut seeded_rng(12),
            )
            .expect("update applies");

        assert_eq!(
            old_state
                .cluster()
                .get_node(&old_primary.id)
                .map(|node| node.role.clone()),
            Some(NodeRole::Replica {
                primary: new_primary.id,
            })
        );

        old_primary.config_epoch = 1;
    }

    #[test]
    fn minority_partition_cannot_complete_failover() {
        let failed_primary = with_slots(make_node(1, 7400), 0..64);
        let mut voter1 = make_node(2, 7401);
        voter1.role = NodeRole::Primary;
        let mut voter2 = make_node(3, 7402);
        voter2.role = NodeRole::Primary;
        let mut replica = make_node(4, 7403);
        replica.role = NodeRole::Replica {
            primary: failed_primary.id,
        };

        let mut replica_state = GossipState::new(replica.clone());
        replica_state.insert_node(failed_primary.clone());
        replica_state.insert_node(voter1.clone());
        replica_state.insert_node(voter2.clone());
        replica_state.set_primary_progress(replica.id, [9; 16], 500);
        let _ = replica_state
            .handle_message(
                ClusterMessage::fail(
                    failed_primary.id,
                    failed_primary.config_epoch,
                    failed_primary.id,
                ),
                failed_primary.cluster_addr,
                100,
                &mut seeded_rng(13),
            )
            .expect("fail applied");
        replica_state.schedule_failover(100, 0);

        let outbound = replica_state.tick(700, &mut seeded_rng(14));
        let auth = outbound
            .into_iter()
            .find(|envelope| envelope.addr == voter1.cluster_addr)
            .expect("auth request sent to reachable voter");

        let mut voter_state = GossipState::new(voter1.clone());
        voter_state.insert_node(failed_primary.clone());
        voter_state.insert_node(replica.clone());
        voter_state.insert_node(voter2.clone());
        let _ = voter_state
            .handle_message(
                ClusterMessage::fail(
                    failed_primary.id,
                    failed_primary.config_epoch,
                    failed_primary.id,
                ),
                failed_primary.cluster_addr,
                200,
                &mut seeded_rng(15),
            )
            .expect("fail applied on voter");
        let responses = voter_state
            .handle_message(auth.message, replica.cluster_addr, 700, &mut seeded_rng(15))
            .expect("vote handled");
        assert_eq!(responses.len(), 1);

        let _ = replica_state
            .handle_message(
                responses[0].message.clone(),
                voter1.cluster_addr,
                800,
                &mut seeded_rng(16),
            )
            .expect("ack handled");

        assert_ne!(
            replica_state
                .cluster()
                .get_node(&replica.id)
                .map(|node| node.role.clone()),
            Some(NodeRole::Primary)
        );
    }

    #[test]
    fn slot_bitmap_roundtrip_is_lossless() {
        let slots = (0..16_384_u32).step_by(17).collect::<RoaringBitmap>();
        let bitmap = slots_to_bitmap(&slots);
        let roundtrip = bitmap_to_slots(&bitmap);
        assert_eq!(roundtrip, slots);
    }

    #[test]
    fn update_serializes_full_slot_set() {
        let node = with_slots(make_node(9, 7600), 0..512);
        let update = UpdateMessage::from_meta(&node);
        let message = ClusterMessage::update(node.id, node.config_epoch, update.clone());
        let decoded = ClusterMessage::decode(&message.encode()).expect("decode update");

        match decoded.body {
            ClusterMessageBody::Update(decoded_update) => {
                assert_eq!(bitmap_to_slots(&decoded_update.slot_bitmap), node.slots);
            }
            other => panic!("expected update body, got {other:?}"),
        }
    }

    #[test]
    fn current_unix_ms_monotonic_enough_for_runtime_tick() {
        let first = current_unix_ms();
        let second = current_unix_ms();
        assert!(second >= first);
    }
}
