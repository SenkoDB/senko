use std::net::SocketAddr;

use crate::node::NodeId;
use crate::slot::{FLAG_IMPORTING, FLAG_LOCAL, FLAG_MIGRATING, SlotTableSnapshot, crc16_slot};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RouteOptions {
    pub current_shard: usize,
    pub asking: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteDecision {
    LocalShard(usize),
    CrossShard(usize),
    Moved(NodeId, SocketAddr),
    Ask(NodeId, SocketAddr),
    Proxy { to: NodeId },
}

#[inline]
pub fn route(slot_table: &SlotTableSnapshot, key: &[u8], write: bool) -> RouteDecision {
    route_with_options(slot_table, key, write, RouteOptions::default())
}

#[inline]
pub fn route_with_options(
    slot_table: &SlotTableSnapshot,
    key: &[u8],
    _write: bool,
    options: RouteOptions,
) -> RouteDecision {
    let slot = crc16_slot(key);
    let entry = slot_table.entry(slot);

    if (entry.flags & FLAG_MIGRATING) != 0 && slot_table.is_key_migrated(slot, key) {
        return remote_decision(slot_table, entry.node_index, true);
    }

    if (entry.flags & FLAG_IMPORTING) != 0 && !options.asking {
        return remote_decision(slot_table, entry.node_index, false);
    }

    if (entry.flags & FLAG_LOCAL) != 0 || ((entry.flags & FLAG_IMPORTING) != 0 && options.asking) {
        return local_decision(entry.shard_index as usize, options.current_shard);
    }

    if slot_table.proxy_remote() {
        let node = slot_table
            .route_node(entry.node_index)
            .expect("route node missing for remote slot");
        return RouteDecision::Proxy { to: node.id };
    }

    remote_decision(slot_table, entry.node_index, false)
}

#[inline]
fn local_decision(target_shard: usize, current_shard: usize) -> RouteDecision {
    if target_shard == current_shard {
        RouteDecision::LocalShard(target_shard)
    } else {
        RouteDecision::CrossShard(target_shard)
    }
}

#[inline]
fn remote_decision(slot_table: &SlotTableSnapshot, node_index: u16, ask: bool) -> RouteDecision {
    let node = slot_table
        .route_node(node_index)
        .expect("route node missing for remote slot");
    if ask {
        RouteDecision::Ask(node.id, node.addr)
    } else {
        RouteDecision::Moved(node.id, node.addr)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use crate::node::NodeId;
    use crate::slot::{
        FLAG_IMPORTING, FLAG_LOCAL, FLAG_MIGRATING, SlotEntry, SlotTableSnapshot, crc16_slot,
    };

    use super::{RouteDecision, RouteOptions, route, route_with_options};

    #[test]
    fn local_slot_routes_to_local_shard() {
        let key = b"foo";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 0,
                shard_index: 2,
                flags: FLAG_LOCAL,
            },
        );

        assert_eq!(
            route_with_options(
                &snapshot,
                key,
                true,
                RouteOptions {
                    current_shard: 2,
                    asking: false,
                },
            ),
            RouteDecision::LocalShard(2)
        );
    }

    #[test]
    fn remote_slot_routes_to_moved() {
        let key = b"foo";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        let node_id = NodeId::new([3; 20]);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7002);
        snapshot.set_route_node(1, node_id, addr);
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 1,
                shard_index: 0,
                flags: 0,
            },
        );

        assert_eq!(
            route(&snapshot, key, true),
            RouteDecision::Moved(node_id, addr)
        );
    }

    #[test]
    fn migrating_known_key_routes_to_ask() {
        let key = b"{tenant}:migrated";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        let node_id = NodeId::new([4; 20]);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7003);
        snapshot.set_route_node(2, node_id, addr);
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 2,
                shard_index: 0,
                flags: FLAG_LOCAL | FLAG_MIGRATING,
            },
        );
        snapshot.insert_migrating_key(slot, key);

        assert_eq!(
            route(&snapshot, key, true),
            RouteDecision::Ask(node_id, addr)
        );
    }

    #[test]
    fn migrating_unknown_key_stays_local() {
        let key = b"{tenant}:still-local";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 2,
                shard_index: 1,
                flags: FLAG_LOCAL | FLAG_MIGRATING,
            },
        );

        assert_eq!(
            route_with_options(
                &snapshot,
                key,
                true,
                RouteOptions {
                    current_shard: 1,
                    asking: false,
                },
            ),
            RouteDecision::LocalShard(1)
        );
    }

    #[test]
    fn importing_without_asking_routes_back_to_primary() {
        let key = b"{tenant}:importing";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        let node_id = NodeId::new([8; 20]);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7004);
        snapshot.set_route_node(7, node_id, addr);
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 7,
                shard_index: 0,
                flags: FLAG_LOCAL | FLAG_IMPORTING,
            },
        );

        assert_eq!(
            route(&snapshot, key, true),
            RouteDecision::Moved(node_id, addr)
        );
    }

    #[test]
    fn importing_with_asking_stays_local() {
        let key = b"{tenant}:importing";
        let slot = crc16_slot(key);
        let mut snapshot = SlotTableSnapshot::default();
        snapshot.set_entry(
            slot,
            SlotEntry {
                node_index: 7,
                shard_index: 0,
                flags: FLAG_LOCAL | FLAG_IMPORTING,
            },
        );

        assert_eq!(
            route_with_options(
                &snapshot,
                key,
                true,
                RouteOptions {
                    current_shard: 0,
                    asking: true,
                },
            ),
            RouteDecision::LocalShard(0)
        );
    }
}
