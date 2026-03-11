use ahash::RandomState;
use hashbrown::HashMap;

use crate::node::{ClusterState, NodeId, NodeMeta};
use crate::slot::SlotTableSnapshot;

pub const MAX_CLUSTER_NODES: usize = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopologyError {
    TooManyNodes { count: usize, max: usize },
    MissingLocalNode(NodeId),
}

#[derive(Clone, Debug)]
pub struct ClusterTopology {
    state: ClusterState,
    ordered_node_ids: Vec<NodeId>,
    node_index_by_id: HashMap<NodeId, u16, RandomState>,
}

impl ClusterTopology {
    pub fn new(state: ClusterState) -> Result<Self, TopologyError> {
        let mut topology = Self {
            state,
            ordered_node_ids: Vec::new(),
            node_index_by_id: HashMap::with_hasher(RandomState::new()),
        };
        topology.rebuild_indexes()?;
        Ok(topology)
    }

    #[inline]
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    #[inline]
    pub fn state_mut(&mut self) -> &mut ClusterState {
        &mut self.state
    }

    pub fn rebuild_indexes(&mut self) -> Result<(), TopologyError> {
        let mut ordered_node_ids = self.state.nodes().keys().copied().collect::<Vec<_>>();
        ordered_node_ids.sort_unstable();

        if ordered_node_ids.len() > MAX_CLUSTER_NODES {
            return Err(TopologyError::TooManyNodes {
                count: ordered_node_ids.len(),
                max: MAX_CLUSTER_NODES,
            });
        }

        if !ordered_node_ids.contains(&self.state.local_node_id()) {
            return Err(TopologyError::MissingLocalNode(self.state.local_node_id()));
        }

        let mut node_index_by_id =
            HashMap::with_capacity_and_hasher(ordered_node_ids.len(), RandomState::new());
        for (index, id) in ordered_node_ids.iter().copied().enumerate() {
            node_index_by_id.insert(id, index as u16);
        }

        self.ordered_node_ids = ordered_node_ids;
        self.node_index_by_id = node_index_by_id;
        Ok(())
    }

    #[inline]
    pub fn ordered_node_ids(&self) -> &[NodeId] {
        &self.ordered_node_ids
    }

    #[inline]
    pub fn node_index(&self, node_id: &NodeId) -> Option<u16> {
        self.node_index_by_id.get(node_id).copied()
    }

    #[inline]
    pub fn node_by_index(&self, index: u16) -> Option<&NodeMeta> {
        self.ordered_node_ids
            .get(index as usize)
            .and_then(|node_id| self.state.get_node(node_id))
    }

    #[inline]
    pub fn local_node_index(&self) -> Option<u16> {
        self.node_index(&self.state.local_node_id())
    }

    pub fn populate_snapshot_routes(&self, snapshot: &mut SlotTableSnapshot) {
        for (index, node_id) in self.ordered_node_ids.iter().copied().enumerate() {
            let node = self
                .state
                .get_node(&node_id)
                .expect("topology index missing node metadata");
            snapshot.set_route_node(index as u16, node.id, node.addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::node::{ClusterState, NodeMeta};

    use super::ClusterTopology;

    #[test]
    fn topology_builds_deterministic_indexes() {
        let first = NodeMeta::new(
            [1; 20].into(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16379),
        );
        let second = NodeMeta::new(
            [2; 20].into(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6380),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 16380),
        );

        let mut state = ClusterState::with_local_node(first.clone());
        state.insert_node(second.clone());

        let topology = ClusterTopology::new(state).expect("valid topology");

        assert_eq!(
            topology.node_by_index(0).map(|node| node.id),
            Some(first.id)
        );
        assert_eq!(
            topology.node_by_index(1).map(|node| node.id),
            Some(second.id)
        );
    }
}
