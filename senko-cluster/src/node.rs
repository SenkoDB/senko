use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

use ahash::RandomState;
use hashbrown::HashMap;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use roaring::RoaringBitmap;

pub const NODE_ID_LEN: usize = 20;
const NODE_ID_HEX_LEN: usize = NODE_ID_LEN * 2;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId([u8; NODE_ID_LEN]);

impl NodeId {
    pub const ZERO: Self = Self([0; NODE_ID_LEN]);

    #[inline]
    pub const fn new(bytes: [u8; NODE_ID_LEN]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub fn generate() -> Self {
        let mut rng = SmallRng::from_entropy();
        Self::generate_with(&mut rng)
    }

    #[inline]
    pub fn generate_with(rng: &mut SmallRng) -> Self {
        let mut bytes = [0_u8; NODE_ID_LEN];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    #[inline]
    pub const fn as_bytes(&self) -> &[u8; NODE_ID_LEN] {
        &self.0
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; NODE_ID_LEN] {
        self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<[u8; NODE_ID_LEN]> for NodeId {
    fn from(bytes: [u8; NODE_ID_LEN]) -> Self {
        Self(bytes)
    }
}

impl From<NodeId> for [u8; NODE_ID_LEN] {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for NodeId {
    type Err = NodeIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != NODE_ID_HEX_LEN {
            return Err(NodeIdParseError::InvalidLength { len: s.len() });
        }

        let mut bytes = [0_u8; NODE_ID_LEN];
        for (index, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_nibble(chunk[0]).ok_or(NodeIdParseError::InvalidHex {
                index: index * 2,
                byte: chunk[0],
            })?;
            let low = decode_nibble(chunk[1]).ok_or(NodeIdParseError::InvalidHex {
                index: index * 2 + 1,
                byte: chunk[1],
            })?;
            bytes[index] = (high << 4) | low;
        }

        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeIdParseError {
    InvalidLength { len: usize },
    InvalidHex { index: usize, byte: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRole {
    Primary,
    Replica { primary: NodeId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    Connected,
    Disconnected,
    Handshaking,
    PFail,
    Failed,
}

impl NodeState {
    pub const CONNECTED_BIT: u16 = 1 << 0;
    pub const DISCONNECTED_BIT: u16 = 1 << 1;
    pub const HANDSHAKING_BIT: u16 = 1 << 2;
    pub const PFAIL_BIT: u16 = 1 << 3;
    pub const FAIL_BIT: u16 = 1 << 4;

    #[inline]
    pub const fn to_flags(self) -> u16 {
        match self {
            Self::Connected => Self::CONNECTED_BIT,
            Self::Disconnected => Self::DISCONNECTED_BIT,
            Self::Handshaking => Self::HANDSHAKING_BIT,
            Self::PFail => Self::PFAIL_BIT,
            Self::Failed => Self::FAIL_BIT,
        }
    }

    #[inline]
    pub const fn from_flags(flags: u16) -> Self {
        if (flags & Self::FAIL_BIT) != 0 {
            Self::Failed
        } else if (flags & Self::PFAIL_BIT) != 0 {
            Self::PFail
        } else if (flags & Self::HANDSHAKING_BIT) != 0 {
            Self::Handshaking
        } else if (flags & Self::DISCONNECTED_BIT) != 0 {
            Self::Disconnected
        } else {
            Self::Connected
        }
    }

    #[inline]
    pub const fn is_fail_like(self) -> bool {
        matches!(self, Self::PFail | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NodeMeta {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub cluster_addr: SocketAddr,
    pub role: NodeRole,
    pub state: NodeState,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub slots: RoaringBitmap,
}

impl NodeMeta {
    #[inline]
    pub fn new(id: NodeId, addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            cluster_addr,
            role: NodeRole::Primary,
            state: NodeState::Connected,
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 0,
            slots: RoaringBitmap::new(),
        }
    }
}

pub type NodeTable = HashMap<NodeId, NodeMeta, RandomState>;

#[derive(Clone, Debug)]
pub struct ClusterState {
    local_node_id: NodeId,
    nodes: NodeTable,
    current_epoch: u64,
}

impl ClusterState {
    #[inline]
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            nodes: HashMap::with_hasher(RandomState::new()),
            current_epoch: 0,
        }
    }

    #[inline]
    pub fn with_local_node(local: NodeMeta) -> Self {
        let local_node_id = local.id;
        let mut state = Self::new(local_node_id);
        state.insert_node(local);
        state
    }

    #[inline]
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    #[inline]
    pub fn set_current_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    #[inline]
    pub fn insert_node(&mut self, node: NodeMeta) -> Option<NodeMeta> {
        self.nodes.insert(node.id, node)
    }

    #[inline]
    pub fn remove_node(&mut self, id: &NodeId) -> Option<NodeMeta> {
        self.nodes.remove(id)
    }

    #[inline]
    pub fn get_node(&self, id: &NodeId) -> Option<&NodeMeta> {
        self.nodes.get(id)
    }

    #[inline]
    pub fn get_node_mut(&mut self, id: &NodeId) -> Option<&mut NodeMeta> {
        self.nodes.get_mut(id)
    }

    #[inline]
    pub fn nodes(&self) -> &NodeTable {
        &self.nodes
    }

    #[inline]
    pub fn nodes_mut(&mut self) -> &mut NodeTable {
        &mut self.nodes
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &NodeMeta> {
        self.nodes.values()
    }
}

#[inline]
fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::NodeId;

    #[test]
    fn node_id_hex_roundtrip() {
        let id = NodeId::new([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10, 0x12, 0x34, 0x56, 0x78,
        ]);

        let encoded = id.to_string();
        let decoded: NodeId = encoded.parse().expect("valid node id");

        assert_eq!(decoded, id);
        assert_eq!(encoded.len(), 40);
    }
}
