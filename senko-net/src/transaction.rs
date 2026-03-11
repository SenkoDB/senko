use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::HashMap;
use smallvec::SmallVec;
use std::{cell::RefCell, rc::Rc};

#[derive(Debug, Clone, PartialEq)]
pub enum TxState {
    None,
    Multi {
        queue: Vec<QueuedCommand>,
        error: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuedCommand {
    pub name: CompactString,
    pub frames: Vec<Bytes>,
    pub response_override: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatchState {
    pub watched_keys: SmallVec<[(CompactString, u64); 8]>,
    pub dirty: bool,
}

pub type ConnectionMap = HashMap<u64, Rc<RefCell<WatchState>>, RandomState>;

#[derive(Debug, Default)]
pub struct WatchRegistry {
    watchers: HashMap<CompactString, Vec<u64>, RandomState>,
}

impl WatchRegistry {
    pub fn watch(&mut self, conn_id: u64, key: CompactString, _version: u64) {
        let waiters = self.watchers.entry(key).or_default();
        if !waiters.contains(&conn_id) {
            waiters.push(conn_id);
        }
    }

    pub fn unwatch(&mut self, conn_id: u64) {
        self.watchers.retain(|_, conn_ids| {
            conn_ids.retain(|candidate| *candidate != conn_id);
            !conn_ids.is_empty()
        });
    }

    pub fn notify_write(
        &mut self,
        key: &CompactString,
        new_version: u64,
        connections: &mut ConnectionMap,
    ) {
        let Some(conn_ids) = self.watchers.get(key).cloned() else {
            return;
        };
        for conn_id in conn_ids {
            let Some(state) = connections.get(&conn_id) else {
                continue;
            };
            let mut state = state.borrow_mut();
            if state.dirty {
                continue;
            }
            if state
                .watched_keys
                .iter()
                .any(|(watched_key, watched_version)| {
                    watched_key == key && *watched_version != new_version
                })
            {
                state.dirty = true;
            }
        }
    }

    pub fn cleanup_conn(&mut self, conn_id: u64) {
        self.unwatch(conn_id);
    }

    pub fn mark_all_dirty(&mut self, connections: &mut ConnectionMap) {
        for state in connections.values() {
            state.borrow_mut().dirty = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectionMap, WatchRegistry, WatchState};
    use compact_str::CompactString;
    use std::{cell::RefCell, rc::Rc};

    fn state_with_watch(key: &str, version: u64) -> Rc<RefCell<WatchState>> {
        Rc::new(RefCell::new(WatchState {
            watched_keys: smallvec::smallvec![(CompactString::from(key), version)],
            dirty: false,
        }))
    }

    #[test]
    fn watch_registry_marks_matching_key_dirty() {
        let mut registry = WatchRegistry::default();
        let mut connections = ConnectionMap::default();
        connections.insert(1, state_with_watch("k", 0));
        registry.watch(1, CompactString::from("k"), 0);

        registry.notify_write(&CompactString::from("k"), 1, &mut connections);

        assert!(connections.get(&1).unwrap().borrow().dirty);
    }

    #[test]
    fn watch_registry_ignores_other_keys() {
        let mut registry = WatchRegistry::default();
        let mut connections = ConnectionMap::default();
        connections.insert(1, state_with_watch("k", 0));
        registry.watch(1, CompactString::from("k"), 0);

        registry.notify_write(&CompactString::from("other"), 1, &mut connections);

        assert!(!connections.get(&1).unwrap().borrow().dirty);
    }

    #[test]
    fn watch_registry_unwatch_removes_registrations() {
        let mut registry = WatchRegistry::default();
        let mut connections = ConnectionMap::default();
        connections.insert(1, state_with_watch("k", 0));
        registry.watch(1, CompactString::from("k"), 0);
        registry.unwatch(1);

        registry.notify_write(&CompactString::from("k"), 1, &mut connections);

        assert!(!connections.get(&1).unwrap().borrow().dirty);
    }

    #[test]
    fn watch_registry_marks_all_connections_dirty() {
        let mut registry = WatchRegistry::default();
        let mut connections = ConnectionMap::default();
        connections.insert(1, state_with_watch("k", 0));
        connections.insert(2, state_with_watch("k", 0));
        registry.watch(1, CompactString::from("k"), 0);
        registry.watch(2, CompactString::from("k"), 0);

        registry.notify_write(&CompactString::from("k"), 1, &mut connections);

        assert!(connections.get(&1).unwrap().borrow().dirty);
        assert!(connections.get(&2).unwrap().borrow().dirty);
    }

    #[test]
    fn watch_registry_cleanup_conn_removes_dangling_refs() {
        let mut registry = WatchRegistry::default();
        let mut connections = ConnectionMap::default();
        connections.insert(1, state_with_watch("k", 0));
        registry.watch(1, CompactString::from("k"), 0);
        registry.cleanup_conn(1);
        connections.remove(&1);

        registry.notify_write(&CompactString::from("k"), 1, &mut connections);
    }
}
