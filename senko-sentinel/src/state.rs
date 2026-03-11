use std::net::SocketAddr;

use ahash::RandomState;
use arc_swap::ArcSwap;
use compact_str::CompactString;
use hashbrown::HashMap;

pub type SentinelId = CompactString;
pub type SentinelWorld = std::sync::Arc<ArcSwap<WorldSnapshot>>;

#[derive(Clone)]
pub struct WorldSnapshot {
    pub epoch: u64,
    pub my_id: SentinelId,
    pub masters: HashMap<String, MasterState, RandomState>,
    pub timestamp: u64,
}

#[derive(Clone, Debug)]
pub struct MasterState {
    pub name: String,
    pub addr: SocketAddr,
    pub quorum: u32,
    pub flags: InstanceFlags,
    pub config_epoch: u64,
    pub leader: Option<SentinelId>,
    pub leader_epoch: u64,
    pub replicas: HashMap<SocketAddr, ReplicaState, RandomState>,
    pub sentinels: HashMap<SentinelId, SentinelPeer, RandomState>,
    pub last_ping_sent: u64,
    pub last_ok_ping: u64,
    pub down_since: Option<u64>,
    pub failover_state: FailoverState,
    pub failover_epoch: u64,
    pub selected_replica: Option<SocketAddr>,
    pub role_reported: Role,
    pub info_refresh: u64,
    pub link_pending_commands: u32,
    pub link_refcount: u32,
    pub cached_info: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ReplicaState {
    pub addr: SocketAddr,
    pub flags: InstanceFlags,
    pub last_ok_ping: u64,
    pub last_ping_sent: u64,
    pub down_since: Option<u64>,
    pub info_refresh: u64,
    pub master_link_down_time: u64,
    pub master_link_status: LinkStatus,
    pub slave_priority: i32,
    pub slave_repl_offset: u64,
    pub replica_announced: bool,
    pub role_reported: Role,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct SentinelPeer {
    pub id: SentinelId,
    pub addr: SocketAddr,
    pub flags: InstanceFlags,
    pub last_hello: u64,
    pub last_ok_ping: u64,
    pub down_since: Option<u64>,
    pub voted_leader: Option<SentinelId>,
    pub voted_epoch: u64,
    pub runid: String,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InstanceFlags: u32 {
        const MASTER = 1 << 0;
        const SLAVE = 1 << 1;
        const SENTINEL = 1 << 2;
        const S_DOWN = 1 << 3;
        const O_DOWN = 1 << 4;
        const MASTER_DOWN = 1 << 5;
        const FAILOVER_IN_PROGRESS = 1 << 6;
        const PROMOTED = 1 << 7;
        const RECONF_SENT = 1 << 8;
        const RECONF_INPROG = 1 << 9;
        const RECONF_DONE = 1 << 10;
        const FORCE_FAILOVER = 1 << 11;
        const DISCONNECTED = 1 << 12;
        const NO_FAILOVER = 1 << 13;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailoverState {
    None,
    WaitStart,
    SelectSlave,
    SendSlaveOf,
    WaitPromotion,
    ReconfSlaves,
    UpdateConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
    Unknown,
}

impl Default for Role {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkStatus {
    Ok,
    Err,
}

impl Default for LinkStatus {
    fn default() -> Self {
        Self::Err
    }
}

#[derive(Clone, Debug)]
pub struct TiltState {
    pub tilt_start_time: u64,
    pub tilt_mode: bool,
    pub previous_time: u64,
}

impl Default for TiltState {
    fn default() -> Self {
        Self {
            tilt_start_time: 0,
            tilt_mode: false,
            previous_time: 0,
        }
    }
}

impl TiltState {
    pub fn observe(&mut self, now: u64) -> Option<bool> {
        let previous = self.previous_time;
        self.previous_time = now;
        if previous == 0 {
            return None;
        }
        let delta = now as i128 - previous as i128;
        if delta < 0 || delta > 2_000 {
            if !self.tilt_mode {
                self.tilt_mode = true;
                self.tilt_start_time = now;
                return Some(true);
            }
            return None;
        }
        if self.tilt_mode && now.saturating_sub(self.tilt_start_time) >= 30_000 {
            self.tilt_mode = false;
            self.tilt_start_time = 0;
            return Some(false);
        }
        None
    }
}

#[inline]
pub fn new_world(snapshot: WorldSnapshot) -> SentinelWorld {
    std::sync::Arc::new(ArcSwap::from_pointee(snapshot))
}

#[inline]
pub fn update_world<F>(world: &SentinelWorld, update: F) -> std::sync::Arc<WorldSnapshot>
where
    F: FnOnce(&mut WorldSnapshot),
{
    let current = world.load_full();
    let mut next = (*current).clone();
    update(&mut next);
    next.timestamp = next.timestamp.max(crate::current_unix_ms());
    let next = std::sync::Arc::new(next);
    world.store(next.clone());
    next
}

pub fn instance_flags_to_string(flags: InstanceFlags) -> String {
    let mut names = Vec::with_capacity(8);
    if flags.contains(InstanceFlags::MASTER) {
        names.push("master");
    }
    if flags.contains(InstanceFlags::SLAVE) {
        names.push("slave");
    }
    if flags.contains(InstanceFlags::SENTINEL) {
        names.push("sentinel");
    }
    if flags.contains(InstanceFlags::S_DOWN) {
        names.push("s_down");
    }
    if flags.contains(InstanceFlags::O_DOWN) {
        names.push("o_down");
    }
    if flags.contains(InstanceFlags::FAILOVER_IN_PROGRESS) {
        names.push("failover_in_progress");
    }
    if flags.contains(InstanceFlags::PROMOTED) {
        names.push("promoted");
    }
    if flags.contains(InstanceFlags::RECONF_SENT) {
        names.push("reconf_sent");
    }
    if flags.contains(InstanceFlags::RECONF_INPROG) {
        names.push("reconf_inprog");
    }
    if flags.contains(InstanceFlags::RECONF_DONE) {
        names.push("reconf_done");
    }
    if flags.contains(InstanceFlags::FORCE_FAILOVER) {
        names.push("force_failover");
    }
    if flags.contains(InstanceFlags::DISCONNECTED) {
        names.push("disconnected");
    }
    if flags.contains(InstanceFlags::NO_FAILOVER) {
        names.push("no_failover");
    }
    if names.is_empty() {
        names.push("none");
    }
    names.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn world_snapshot_clone_and_swap_under_reads() {
        let world = new_world(WorldSnapshot {
            epoch: 1,
            my_id: CompactString::from("0123456789012345678901234567890123456789"),
            masters: HashMap::with_hasher(RandomState::new()),
            timestamp: 1,
        });
        let mut readers = Vec::new();
        for _ in 0..8 {
            let world = world.clone();
            readers.push(thread::spawn(move || {
                for _ in 0..500 {
                    let snapshot = world.load_full();
                    assert!(snapshot.epoch >= 1);
                }
            }));
        }
        for epoch in 2..32 {
            let _ = update_world(&world, |snapshot| snapshot.epoch = epoch);
        }
        for reader in readers {
            reader.join().expect("reader panicked");
        }
        assert_eq!(world.load().epoch, 31);
    }

    #[test]
    fn tilt_detection_handles_backward_and_stalled_clock() {
        let mut tilt = TiltState::default();
        assert_eq!(tilt.observe(1_000), None);
        assert_eq!(tilt.observe(500), Some(true));
        assert!(tilt.tilt_mode);
        for now in (1_500..=30_500).step_by(1_000) {
            let changed = tilt.observe(now);
            if now < 30_500 {
                assert_eq!(changed, None);
            } else {
                assert_eq!(changed, Some(false));
            }
        }
        assert!(!tilt.tilt_mode);
    }
}
