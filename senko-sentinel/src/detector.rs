use crate::state::{InstanceFlags, MasterState};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DetectionChanges {
    pub sdown_changed: Option<bool>,
    pub odown_changed: Option<bool>,
}

#[inline]
pub fn detect_sdown(now: u64, last_ok_ping: u64, down_after_ms: u64, tilt_mode: bool) -> bool {
    !tilt_mode && now.saturating_sub(last_ok_ping) > down_after_ms
}

#[inline]
pub fn detect_odown(self_sdown: bool, agreeing_sentinels: usize, quorum: u32) -> bool {
    self_sdown && (agreeing_sentinels + 1) >= quorum as usize
}

pub fn sweep_master(
    master: &mut MasterState,
    now: u64,
    down_after_ms: u64,
    tilt_mode: bool,
) -> DetectionChanges {
    let mut changes = DetectionChanges::default();
    let sdown = detect_sdown(now, master.last_ok_ping, down_after_ms, tilt_mode);
    let was_sdown = master.flags.contains(InstanceFlags::S_DOWN);
    if sdown != was_sdown {
        if sdown {
            master.flags.insert(InstanceFlags::S_DOWN);
            master.down_since = Some(now);
        } else {
            master.flags.remove(InstanceFlags::S_DOWN);
            master.down_since = None;
        }
        changes.sdown_changed = Some(sdown);
    }
    let agreeing = master
        .sentinels
        .values()
        .filter(|peer| peer.flags.contains(InstanceFlags::S_DOWN))
        .count();
    let odown = detect_odown(sdown, agreeing, master.quorum);
    let was_odown = master.flags.contains(InstanceFlags::O_DOWN);
    if odown != was_odown {
        if odown {
            master.flags.insert(InstanceFlags::O_DOWN);
        } else {
            master.flags.remove(InstanceFlags::O_DOWN);
        }
        changes.odown_changed = Some(odown);
    }
    changes
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use ahash::RandomState;
    use compact_str::CompactString;
    use hashbrown::HashMap;

    use super::*;
    use crate::state::{FailoverState, InstanceFlags, MasterState, Role, SentinelPeer};

    #[test]
    fn marks_master_sdown_when_ping_is_stale() {
        let mut master = MasterState {
            name: "m".to_owned(),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            quorum: 2,
            flags: InstanceFlags::MASTER,
            config_epoch: 0,
            leader: None,
            leader_epoch: 0,
            replicas: HashMap::with_hasher(RandomState::new()),
            sentinels: HashMap::with_hasher(RandomState::new()),
            last_ping_sent: 0,
            last_ok_ping: 1_000,
            down_since: None,
            failover_state: FailoverState::None,
            failover_epoch: 0,
            selected_replica: None,
            role_reported: Role::Master,
            info_refresh: 0,
            link_pending_commands: 0,
            link_refcount: 0,
            cached_info: Vec::new(),
        };
        let changes = sweep_master(&mut master, 31_100, 30_000, false);
        assert_eq!(changes.sdown_changed, Some(true));
        assert!(master.flags.contains(InstanceFlags::S_DOWN));
    }

    #[test]
    fn marks_master_odown_when_quorum_votes_agree() {
        let mut peers = HashMap::with_hasher(RandomState::new());
        peers.insert(
            CompactString::from("peer-a"),
            SentinelPeer {
                id: CompactString::from("peer-a"),
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 26_379),
                flags: InstanceFlags::SENTINEL | InstanceFlags::S_DOWN,
                last_hello: 0,
                last_ok_ping: 0,
                down_since: None,
                voted_leader: None,
                voted_epoch: 0,
                runid: "peer-a".to_owned(),
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
            replicas: HashMap::with_hasher(RandomState::new()),
            sentinels: peers,
            last_ping_sent: 0,
            last_ok_ping: 1_000,
            down_since: None,
            failover_state: FailoverState::None,
            failover_epoch: 0,
            selected_replica: None,
            role_reported: Role::Master,
            info_refresh: 0,
            link_pending_commands: 0,
            link_refcount: 0,
            cached_info: Vec::new(),
        };
        let changes = sweep_master(&mut master, 31_100, 30_000, false);
        assert_eq!(changes.odown_changed, Some(true));
        assert!(master.flags.contains(InstanceFlags::O_DOWN));
    }
}
