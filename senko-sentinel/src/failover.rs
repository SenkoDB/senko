use std::{cmp::Ordering, net::SocketAddr};

use crate::state::{FailoverState, InstanceFlags, LinkStatus, MasterState, ReplicaState, Role};

#[derive(Debug, Default, Clone)]
pub struct FailoverTransition {
    pub emitted: Vec<&'static str>,
    pub switched_master: Option<(SocketAddr, SocketAddr)>,
    pub aborted: bool,
}

pub fn begin_failover(master: &mut MasterState, epoch: u64) {
    master.failover_state = FailoverState::WaitStart;
    master.failover_epoch = epoch;
    master.flags.insert(InstanceFlags::FAILOVER_IN_PROGRESS);
}

pub fn select_best_replica(
    master: &MasterState,
    now: u64,
    down_after_ms: u64,
    odown_time: u64,
) -> Option<SocketAddr> {
    let mut candidates = master
        .replicas
        .values()
        .filter(|replica| {
            !replica.flags.intersects(
                InstanceFlags::S_DOWN | InstanceFlags::O_DOWN | InstanceFlags::DISCONNECTED,
            )
        })
        .filter(|replica| replica.slave_priority > 0)
        .filter(|replica| now.saturating_sub(replica.last_ok_ping) <= 5 * down_after_ms)
        .filter(|replica| {
            replica.master_link_down_time <= now.saturating_sub(odown_time) + 10 * down_after_ms
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| compare_replicas(left, right));
    candidates.first().map(|replica| replica.addr)
}

pub fn advance_failover(
    master: &mut MasterState,
    now: u64,
    down_after_ms: u64,
    failover_timeout: u64,
    parallel_syncs: u32,
    elected_leader: bool,
) -> FailoverTransition {
    let mut outcome = FailoverTransition::default();
    if now.saturating_sub(master.failover_epoch) > failover_timeout {
        master.failover_state = FailoverState::None;
        master.flags.remove(InstanceFlags::FAILOVER_IN_PROGRESS);
        outcome.emitted.push("-failover-abort");
        outcome.aborted = true;
        return outcome;
    }
    match master.failover_state {
        FailoverState::None => {}
        FailoverState::WaitStart => {
            if elected_leader {
                master.failover_state = FailoverState::SelectSlave;
                outcome.emitted.push("+failover-state-select-slave");
            }
        }
        FailoverState::SelectSlave => {
            let odown_time = master.down_since.unwrap_or(now);
            let selected = select_best_replica(master, now, down_after_ms, odown_time);
            match selected {
                Some(addr) => {
                    master.selected_replica = Some(addr);
                    master.failover_state = FailoverState::SendSlaveOf;
                    outcome.emitted.push("+selected-slave");
                    outcome.emitted.push("+failover-state-send-slaveof-noone");
                }
                None => {
                    master.failover_state = FailoverState::None;
                    master.flags.remove(InstanceFlags::FAILOVER_IN_PROGRESS);
                    outcome.emitted.push("-failover-abort");
                    outcome.aborted = true;
                }
            }
        }
        FailoverState::SendSlaveOf => {
            master.failover_state = FailoverState::WaitPromotion;
            outcome.emitted.push("+failover-state-wait-promotion");
        }
        FailoverState::WaitPromotion => {
            let promoted = master
                .selected_replica
                .and_then(|addr| master.replicas.get(&addr))
                .map(|replica| replica.role_reported == Role::Master)
                .unwrap_or(false);
            if promoted {
                master.failover_state = FailoverState::ReconfSlaves;
                outcome.emitted.push("+promoted-slave");
                outcome.emitted.push("+failover-state-reconf-slaves");
            }
        }
        FailoverState::ReconfSlaves => {
            let selected = master.selected_replica;
            let mut in_flight = 0u32;
            let mut done = 0usize;
            let total = master
                .replicas
                .values()
                .filter(|replica| Some(replica.addr) != selected)
                .count();
            for replica in master.replicas.values_mut() {
                if Some(replica.addr) == selected {
                    continue;
                }
                if replica.flags.contains(InstanceFlags::RECONF_DONE) {
                    done += 1;
                    continue;
                }
                if replica.master_link_status == LinkStatus::Ok {
                    replica
                        .flags
                        .remove(InstanceFlags::RECONF_SENT | InstanceFlags::RECONF_INPROG);
                    replica.flags.insert(InstanceFlags::RECONF_DONE);
                    outcome.emitted.push("+slave-reconf-done");
                    done += 1;
                    continue;
                }
                if replica.flags.contains(InstanceFlags::RECONF_SENT) {
                    replica.flags.remove(InstanceFlags::RECONF_SENT);
                    replica.flags.insert(InstanceFlags::RECONF_INPROG);
                    outcome.emitted.push("+slave-reconf-inprog");
                    in_flight += 1;
                    continue;
                }
                if in_flight < parallel_syncs {
                    replica.flags.insert(InstanceFlags::RECONF_SENT);
                    outcome.emitted.push("+slave-reconf-sent");
                    in_flight += 1;
                }
            }
            if done >= total {
                master.failover_state = FailoverState::UpdateConfig;
            }
        }
        FailoverState::UpdateConfig => {
            if let Some(selected) = master.selected_replica {
                let old = master.addr;
                master.addr = selected;
                master.config_epoch += 1;
                master.failover_state = FailoverState::None;
                master.flags.remove(InstanceFlags::FAILOVER_IN_PROGRESS);
                master
                    .flags
                    .remove(InstanceFlags::O_DOWN | InstanceFlags::S_DOWN);
                outcome.switched_master = Some((old, selected));
                outcome.emitted.push("+switch-master");
                outcome.emitted.push("+failover-end");
            }
        }
    }
    outcome
}

fn compare_replicas(left: &ReplicaState, right: &ReplicaState) -> Ordering {
    left.slave_priority
        .cmp(&right.slave_priority)
        .then_with(|| right.slave_repl_offset.cmp(&left.slave_repl_offset))
        .then_with(|| left.name.cmp(&right.name))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use ahash::RandomState;
    use hashbrown::HashMap;

    use super::*;
    use crate::state::{InstanceFlags, LinkStatus, MasterState, Role};

    fn replica(
        port: u16,
        priority: i32,
        offset: u64,
        name: &str,
        flags: InstanceFlags,
    ) -> ReplicaState {
        ReplicaState {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            flags,
            last_ok_ping: 10_000,
            last_ping_sent: 0,
            down_since: None,
            info_refresh: 0,
            master_link_down_time: 0,
            master_link_status: LinkStatus::Err,
            slave_priority: priority,
            slave_repl_offset: offset,
            replica_announced: true,
            role_reported: Role::Slave,
            name: name.to_owned(),
        }
    }

    #[test]
    fn replica_selection_skips_priority_zero_and_uses_offset_then_name() {
        let mut replicas = HashMap::with_hasher(RandomState::new());
        replicas.insert(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6380),
            replica(6380, 0, 200, "c", InstanceFlags::SLAVE),
        );
        replicas.insert(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6381),
            replica(6381, 100, 100, "b", InstanceFlags::SLAVE),
        );
        replicas.insert(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6382),
            replica(6382, 100, 120, "a", InstanceFlags::SLAVE),
        );
        let master = MasterState {
            name: "m".to_owned(),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            quorum: 2,
            flags: InstanceFlags::MASTER,
            config_epoch: 0,
            leader: None,
            leader_epoch: 0,
            replicas,
            sentinels: HashMap::with_hasher(RandomState::new()),
            last_ping_sent: 0,
            last_ok_ping: 0,
            down_since: Some(1),
            failover_state: FailoverState::SelectSlave,
            failover_epoch: 0,
            selected_replica: None,
            role_reported: Role::Master,
            info_refresh: 0,
            link_pending_commands: 0,
            link_refcount: 0,
            cached_info: Vec::new(),
        };
        let selected = select_best_replica(&master, 12_000, 1_000, 1).expect("replica");
        assert_eq!(selected.port(), 6382);
    }

    #[test]
    fn failover_walks_until_update_config() {
        let selected_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6381);
        let mut replicas = HashMap::with_hasher(RandomState::new());
        replicas.insert(
            selected_addr,
            ReplicaState {
                addr: selected_addr,
                flags: InstanceFlags::SLAVE,
                last_ok_ping: 10_000,
                last_ping_sent: 0,
                down_since: None,
                info_refresh: 0,
                master_link_down_time: 0,
                master_link_status: LinkStatus::Ok,
                slave_priority: 100,
                slave_repl_offset: 100,
                replica_announced: true,
                role_reported: Role::Master,
                name: "a".to_owned(),
            },
        );
        let mut master = MasterState {
            name: "m".to_owned(),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            quorum: 2,
            flags: InstanceFlags::MASTER,
            config_epoch: 0,
            leader: None,
            leader_epoch: 0,
            replicas,
            sentinels: HashMap::with_hasher(RandomState::new()),
            last_ping_sent: 0,
            last_ok_ping: 0,
            down_since: Some(1),
            failover_state: FailoverState::WaitStart,
            failover_epoch: 0,
            selected_replica: None,
            role_reported: Role::Master,
            info_refresh: 0,
            link_pending_commands: 0,
            link_refcount: 0,
            cached_info: Vec::new(),
        };
        begin_failover(&mut master, 1);
        let _ = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        let _ = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        let _ = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        let _ = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        let _ = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        let outcome = advance_failover(&mut master, 10, 1_000, 10_000, 1, true);
        assert_eq!(
            outcome.switched_master,
            Some((
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
                selected_addr
            ))
        );
        assert_eq!(master.addr, selected_addr);
        assert_eq!(master.failover_state, FailoverState::None);
    }
}
