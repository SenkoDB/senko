#![deny(unsafe_code)]

pub mod config;
pub mod node;
pub mod route;
pub mod slot;
pub mod topo;

pub use config::ClusterConfig;
pub use node::{ClusterState, NodeId, NodeMeta, NodeRole, NodeState, NodeTable};
pub use route::{RouteDecision, RouteOptions, route, route_with_options};
pub use slot::{
    FLAG_IMPORTING, FLAG_LOCAL, FLAG_MIGRATING, FLAG_REPLICA, SLOT_COUNT, SLOT_MASK,
    SeqLockSlotTable, SlotEntry, SlotTable, SlotTableSnapshot, assign_slots_to_shards, crc16_ccitt,
    crc16_slot, hash_tag,
};
pub use topo::{ClusterTopology, TopologyError};
