use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::HashMap;

use crate::{
    current_unix_ms,
    state::{SentinelId, SentinelWorld, update_world},
};

#[derive(Debug, Clone)]
pub struct ElectionState {
    pub epoch: u64,
    pub votes_received: HashMap<SentinelId, u64, RandomState>,
    pub vote_granted_to: Option<(SentinelId, u64)>,
    pub election_start: u64,
}

impl Default for ElectionState {
    fn default() -> Self {
        Self {
            epoch: 0,
            votes_received: HashMap::with_hasher(RandomState::new()),
            vote_granted_to: None,
            election_start: 0,
        }
    }
}

#[derive(Default)]
pub struct ElectionBook {
    pub states: HashMap<String, ElectionState, RandomState>,
}

impl ElectionBook {
    pub fn start_election(
        &mut self,
        world: &SentinelWorld,
        master_name: &str,
        my_id: &SentinelId,
        now: u64,
    ) -> u64 {
        let next = update_world(world, |snapshot| {
            snapshot.epoch += 1;
            if let Some(master) = snapshot.masters.get_mut(master_name) {
                master.leader = Some(my_id.clone());
                master.leader_epoch = snapshot.epoch;
                master.failover_epoch = snapshot.epoch;
            }
            snapshot.timestamp = now;
        });
        let epoch = next.epoch;
        let state = self.states.entry(master_name.to_owned()).or_default();
        state.epoch = epoch;
        state.election_start = now;
        state.votes_received.clear();
        state.votes_received.insert(my_id.clone(), epoch);
        state.vote_granted_to = Some((my_id.clone(), epoch));
        epoch
    }

    pub fn process_vote_request(
        &mut self,
        master_name: &str,
        candidate: SentinelId,
        epoch: u64,
    ) -> (Option<SentinelId>, u64) {
        let state = self.states.entry(master_name.to_owned()).or_default();
        if epoch > state.epoch {
            state.epoch = epoch;
            state.vote_granted_to = None;
        }
        if state.vote_granted_to.is_none() && epoch >= state.epoch {
            state.vote_granted_to = Some((candidate.clone(), epoch));
        }
        (
            state.vote_granted_to.as_ref().map(|vote| vote.0.clone()),
            state
                .vote_granted_to
                .as_ref()
                .map(|vote| vote.1)
                .unwrap_or(epoch),
        )
    }

    pub fn process_vote_reply(
        &mut self,
        master_name: &str,
        from: SentinelId,
        leader: SentinelId,
        epoch: u64,
    ) {
        let state = self.states.entry(master_name.to_owned()).or_default();
        if state.epoch < epoch {
            state.epoch = epoch;
        }
        if leader == from || leader == CompactString::default() {
            state.votes_received.insert(from, epoch);
        } else if leader == from {
            state.votes_received.insert(leader, epoch);
        }
    }

    pub fn grant_vote(&mut self, master_name: &str, voter: SentinelId, epoch: u64) {
        let state = self.states.entry(master_name.to_owned()).or_default();
        state.votes_received.insert(voter, epoch);
    }

    pub fn count_votes(&self, master_name: &str, quorum: u32) -> Option<SentinelId> {
        let state = self.states.get(master_name)?;
        let votes = state
            .votes_received
            .values()
            .filter(|epoch| **epoch == state.epoch)
            .count();
        if votes >= quorum as usize {
            state.vote_granted_to.as_ref().map(|vote| vote.0.clone())
        } else {
            None
        }
    }

    pub fn is_leader(&self, world: &SentinelWorld, master_name: &str, my_id: &SentinelId) -> bool {
        let snapshot = world.load();
        let Some(master) = snapshot.masters.get(master_name) else {
            return false;
        };
        master.leader.as_ref() == Some(my_id)
    }

    pub fn election_timed_out(&self, master_name: &str, now: u64, timeout_ms: u64) -> bool {
        self.states
            .get(master_name)
            .map(|state| now.saturating_sub(state.election_start) > timeout_ms)
            .unwrap_or(false)
    }

    pub fn touch_now() -> u64 {
        current_unix_ms()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::RandomState;
    use hashbrown::HashMap;

    use crate::state::{WorldSnapshot, new_world};

    #[test]
    fn counts_votes_until_quorum() {
        let world = new_world(WorldSnapshot {
            epoch: 0,
            my_id: CompactString::from("self"),
            masters: HashMap::with_hasher(RandomState::new()),
            timestamp: 0,
        });
        let mut book = ElectionBook::default();
        let epoch = book.start_election(&world, "m", &CompactString::from("self"), 1_000);
        book.grant_vote("m", CompactString::from("self"), epoch);
        book.grant_vote("m", CompactString::from("peer-a"), epoch);
        assert_eq!(book.count_votes("m", 2), Some(CompactString::from("self")));
    }
}
