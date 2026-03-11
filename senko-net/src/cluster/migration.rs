use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::{HashMap, HashSet};
use senko_cluster::{
    FLAG_IMPORTING, FLAG_LOCAL, FLAG_MIGRATING, NodeId, RouteDecision, RouteOptions, SLOT_COUNT,
    SlotEntry, SlotTableSnapshot, crc16_slot, route_with_options,
};
use senko_core::SenkoError;
use senko_proto::Frame;
use senko_store::{
    SetOptions, Store,
    commands::generic::migrate::{dump_value, restore_value},
};

pub const DEFAULT_MAX_CONCURRENT_MIGRATIONS: usize = 8;
pub const DEFAULT_PIPELINE_WIDTH: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationAuth {
    Auth(Box<[u8]>),
    Auth2 {
        username: Box<[u8]>,
        password: Box<[u8]>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrateRequest {
    pub host: Box<[u8]>,
    pub port: u16,
    pub key: Option<Box<[u8]>>,
    pub keys: Vec<Box<[u8]>>,
    pub db: u32,
    pub timeout_ms: u64,
    pub copy: bool,
    pub replace: bool,
    pub auth: Option<MigrationAuth>,
}

impl MigrateRequest {
    pub fn parse(args: &[Frame<'_>]) -> Result<Self, SenkoError> {
        if args.len() < 5 {
            return Err(SenkoError::Protocol(
                "wrong number of arguments for 'migrate' command",
            ));
        }

        let host = frame_bytes(&args[0])?.into();
        let port = parse_u16(frame_bytes(&args[1])?)?;
        let key_arg = frame_bytes(&args[2])?;
        let db = parse_u32(frame_bytes(&args[3])?)?;
        let timeout_ms = parse_u64(frame_bytes(&args[4])?)?;
        let mut copy = false;
        let mut replace = false;
        let mut auth = None;
        let mut keys = Vec::new();
        let mut index = 5usize;

        while index < args.len() {
            let token = frame_bytes(&args[index])?;
            index += 1;

            if token.eq_ignore_ascii_case(b"COPY") {
                copy = true;
                continue;
            }

            if token.eq_ignore_ascii_case(b"REPLACE") {
                replace = true;
                continue;
            }

            if token.eq_ignore_ascii_case(b"AUTH") {
                if index >= args.len() {
                    return Err(SenkoError::Protocol("syntax error"));
                }
                auth = Some(MigrationAuth::Auth(frame_bytes(&args[index])?.into()));
                index += 1;
                continue;
            }

            if token.eq_ignore_ascii_case(b"AUTH2") {
                if index + 1 >= args.len() {
                    return Err(SenkoError::Protocol("syntax error"));
                }
                auth = Some(MigrationAuth::Auth2 {
                    username: frame_bytes(&args[index])?.into(),
                    password: frame_bytes(&args[index + 1])?.into(),
                });
                index += 2;
                continue;
            }

            if token.eq_ignore_ascii_case(b"KEYS") {
                while index < args.len() {
                    keys.push(frame_bytes(&args[index])?.into());
                    index += 1;
                }
                break;
            }

            return Err(SenkoError::Protocol("syntax error"));
        }

        let key = if key_arg.is_empty() {
            None
        } else {
            Some(Box::<[u8]>::from(key_arg))
        };

        if key.is_none() && keys.is_empty() {
            return Err(SenkoError::Protocol("ERR no keys in command"));
        }

        if key.is_some() && !keys.is_empty() {
            return Err(SenkoError::Protocol("ERR syntax error"));
        }

        if db != 0 {
            return Err(SenkoError::Protocol("ERR DB index is out of range"));
        }

        Ok(Self {
            host,
            port,
            key,
            keys,
            db,
            timeout_ms,
            copy,
            replace,
            auth,
        })
    }

    pub fn selected_keys(&self) -> Vec<&[u8]> {
        if let Some(key) = &self.key {
            vec![key.as_ref()]
        } else {
            self.keys.iter().map(Box::as_ref).collect()
        }
    }
}

#[derive(Debug)]
pub enum MigrationError {
    BusyKey(CompactString),
    KeyNotUtf8,
    MaxConcurrent { max: usize },
    NoSuchJob(u16),
    SlotMismatch { slot: u16, key: CompactString },
    Store(SenkoError),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BusyKey(key) => write!(f, "target already contains key {key}"),
            Self::KeyNotUtf8 => write!(f, "migration keys must be valid UTF-8"),
            Self::MaxConcurrent { max } => {
                write!(f, "migration concurrency limit reached ({max})")
            }
            Self::NoSuchJob(slot) => write!(f, "no active migration for slot {slot}"),
            Self::SlotMismatch { slot, key } => {
                write!(f, "key {key} does not belong to slot {slot}")
            }
            Self::Store(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for MigrationError {}

impl From<SenkoError> for MigrationError {
    fn from(error: SenkoError) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AskingState {
    armed: bool,
}

impl AskingState {
    pub fn arm(&mut self) {
        self.armed = true;
    }

    pub fn consume(&mut self) -> bool {
        let armed = self.armed;
        self.armed = false;
        armed
    }
}

#[derive(Clone, Debug)]
pub struct SlotIndex {
    slots: Vec<HashSet<CompactString, RandomState>>,
}

impl SlotIndex {
    pub fn new() -> Self {
        Self {
            slots: (0..SLOT_COUNT)
                .map(|_| HashSet::with_hasher(RandomState::new()))
                .collect(),
        }
    }

    pub fn rebuild_from_store(&mut self, store: &mut Store) {
        self.clear();
        for key in store.live_keys_snapshot() {
            self.insert_compact(key);
        }
    }

    pub fn clear(&mut self) {
        for keys in &mut self.slots {
            keys.clear();
        }
    }

    pub fn insert(&mut self, key: &[u8]) -> Result<bool, MigrationError> {
        let key = CompactString::from_utf8(key).map_err(|_| MigrationError::KeyNotUtf8)?;
        Ok(self.insert_compact(key))
    }

    pub fn insert_compact(&mut self, key: CompactString) -> bool {
        let slot = crc16_slot(key.as_bytes()) as usize;
        self.slots[slot].insert(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        match CompactString::from_utf8(key) {
            Ok(key) => self.remove_compact(&key),
            Err(_) => false,
        }
    }

    pub fn remove_compact(&mut self, key: &CompactString) -> bool {
        let slot = crc16_slot(key.as_bytes()) as usize;
        self.slots[slot].remove(key)
    }

    pub fn count_keys_in_slot(&self, slot: u16) -> usize {
        self.slots[slot as usize].len()
    }

    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<CompactString> {
        let mut keys = self.slots[slot as usize]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.truncate(count);
        keys
    }

    pub fn contains(&self, slot: u16, key: &[u8]) -> bool {
        CompactString::from_utf8(key)
            .ok()
            .is_some_and(|key| self.slots[slot as usize].contains(&key))
    }
}

impl Default for SlotIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationStep {
    pub moved_keys: Vec<CompactString>,
    pub remaining_keys: usize,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictResolution {
    pub winner: NodeId,
    pub loser: NodeId,
    pub epoch: u64,
    pub loser_must_bump_epoch: bool,
}

#[derive(Debug)]
pub struct MigrationJob {
    pub slot: u16,
    pub target: NodeId,
    pub target_addr: SocketAddr,
    pending: VecDeque<CompactString>,
    pending_set: HashSet<CompactString, RandomState>,
    moved: HashSet<CompactString, RandomState>,
    pub keys_migrated: AtomicU64,
    pub pipeline_width: usize,
    pub copy: bool,
    pub replace: bool,
}

impl MigrationJob {
    fn new(
        slot: u16,
        target: NodeId,
        target_addr: SocketAddr,
        keys: Vec<CompactString>,
        pipeline_width: usize,
        copy: bool,
        replace: bool,
    ) -> Self {
        let mut pending = VecDeque::with_capacity(keys.len());
        let mut pending_set = HashSet::with_capacity_and_hasher(keys.len(), RandomState::new());
        for key in keys {
            pending.push_back(key.clone());
            pending_set.insert(key);
        }
        Self {
            slot,
            target,
            target_addr,
            pending,
            pending_set,
            moved: HashSet::with_hasher(RandomState::new()),
            keys_migrated: AtomicU64::new(0),
            pipeline_width,
            copy,
            replace,
        }
    }

    fn pending_len(&self) -> usize {
        self.pending_set.len()
    }

    fn enqueue(&mut self, key: CompactString) {
        if self.moved.contains(&key) || self.pending_set.contains(&key) {
            return;
        }
        self.pending.push_back(key.clone());
        self.pending_set.insert(key);
    }

    fn remove_pending(&mut self, key: &CompactString) {
        if !self.pending_set.remove(key) {
            return;
        }
        self.pending.retain(|candidate| candidate != key);
    }

    fn next_batch(&mut self) -> Vec<CompactString> {
        let mut batch = Vec::with_capacity(self.pipeline_width);
        while batch.len() < self.pipeline_width {
            let Some(key) = self.pending.pop_front() else {
                break;
            };
            if self.pending_set.contains(&key) {
                batch.push(key);
            }
        }
        batch
    }

    fn mark_moved(&mut self, key: &CompactString) {
        self.pending_set.remove(key);
        self.moved.insert(key.clone());
        self.keys_migrated.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct MigrationManager {
    active: HashMap<u16, MigrationJob, RandomState>,
    importing: HashMap<u16, NodeId, RandomState>,
    pub max_concurrent: usize,
    pub pipeline_width: usize,
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CONCURRENT_MIGRATIONS, DEFAULT_PIPELINE_WIDTH)
    }
}

impl MigrationManager {
    pub fn new(max_concurrent: usize, pipeline_width: usize) -> Self {
        Self {
            active: HashMap::with_hasher(RandomState::new()),
            importing: HashMap::with_hasher(RandomState::new()),
            max_concurrent: max_concurrent.max(1),
            pipeline_width: pipeline_width.max(1),
        }
    }

    pub fn migrating_slot_count(&self) -> usize {
        self.active.len()
    }

    pub fn importing_slot_count(&self) -> usize {
        self.importing.len()
    }

    pub fn job(&self, slot: u16) -> Option<&MigrationJob> {
        self.active.get(&slot)
    }

    pub fn set_slot_migrating(
        &mut self,
        snapshot: &mut SlotTableSnapshot,
        slot_index: &SlotIndex,
        slot: u16,
        target: NodeId,
        target_node_index: u16,
        target_addr: SocketAddr,
        current_shard: usize,
    ) -> Result<usize, MigrationError> {
        if !self.active.contains_key(&slot) && self.active.len() >= self.max_concurrent {
            return Err(MigrationError::MaxConcurrent {
                max: self.max_concurrent,
            });
        }

        let keys = slot_index.get_keys_in_slot(slot, usize::MAX);
        let mut entry = snapshot.entry(slot);
        entry.node_index = target_node_index;
        entry.shard_index = current_shard as u16;
        entry.flags |= FLAG_LOCAL | FLAG_MIGRATING;
        entry.flags &= !FLAG_IMPORTING;
        snapshot.set_entry(slot, entry);
        snapshot.clear_migrating_slot(slot);

        self.active.insert(
            slot,
            MigrationJob::new(
                slot,
                target,
                target_addr,
                keys.clone(),
                self.pipeline_width,
                false,
                true,
            ),
        );
        Ok(keys.len())
    }

    pub fn set_slot_importing(
        &mut self,
        snapshot: &mut SlotTableSnapshot,
        slot: u16,
        source: NodeId,
        source_node_index: u16,
        current_shard: usize,
    ) {
        let _ = source;
        let mut entry = snapshot.entry(slot);
        entry.node_index = source_node_index;
        entry.shard_index = current_shard as u16;
        entry.flags |= FLAG_LOCAL | FLAG_IMPORTING;
        entry.flags &= !FLAG_MIGRATING;
        snapshot.set_entry(slot, entry);
        self.importing.insert(slot, source);
    }

    pub fn note_local_write(&mut self, slot: u16, key: &[u8]) -> Result<(), MigrationError> {
        let Some(job) = self.active.get_mut(&slot) else {
            return Ok(());
        };
        let key = CompactString::from_utf8(key).map_err(|_| MigrationError::KeyNotUtf8)?;
        job.enqueue(key);
        Ok(())
    }

    pub fn note_local_delete(&mut self, slot: u16, key: &[u8]) {
        let Some(job) = self.active.get_mut(&slot) else {
            return;
        };
        let Ok(key) = CompactString::from_utf8(key) else {
            return;
        };
        job.remove_pending(&key);
    }

    pub fn migrate_slot_chunk(
        &mut self,
        slot: u16,
        source_store: &mut Store,
        target_store: &mut Store,
        source_index: &mut SlotIndex,
        target_index: &mut SlotIndex,
        snapshot: &mut SlotTableSnapshot,
    ) -> Result<MigrationStep, MigrationError> {
        let Some(job) = self.active.get_mut(&slot) else {
            return Err(MigrationError::NoSuchJob(slot));
        };

        let batch = job.next_batch();
        let mut moved_keys = Vec::with_capacity(batch.len());
        for key in batch {
            if crc16_slot(key.as_bytes()) != slot {
                return Err(MigrationError::SlotMismatch { slot, key });
            }

            let Some(entry) = source_store.clone_entry(key.as_bytes()) else {
                source_index.remove_compact(&key);
                job.remove_pending(&key);
                continue;
            };

            if !job.replace && target_store.type_name(key.as_bytes()).is_some() {
                return Err(MigrationError::BusyKey(key));
            }

            let dump = dump_value(&entry.value, entry.expires_at);
            let value = restore_value(&dump)?;
            let _ = target_store.set(key.clone(), value, SetOptions::default());
            match entry.expires_at {
                Some(expires_at) => target_store.set_expiry(key.as_bytes(), expires_at),
                None => target_store.remove_expiry(key.as_bytes()),
            }
            target_index.insert_compact(key.clone());

            if !job.copy {
                let _ = source_store.delete(key.as_bytes());
                source_index.remove_compact(&key);
            }

            snapshot.insert_migrating_key(slot, key.as_bytes());
            job.mark_moved(&key);
            moved_keys.push(key);
        }

        Ok(MigrationStep {
            moved_keys,
            remaining_keys: job.pending_len(),
            complete: job.pending_len() == 0,
        })
    }

    pub fn finalize_slot(
        &mut self,
        snapshot: &mut SlotTableSnapshot,
        slot: u16,
        owner_node_index: u16,
        current_shard: usize,
        local_owner: bool,
    ) {
        let entry = SlotEntry {
            node_index: owner_node_index,
            shard_index: current_shard as u16,
            flags: if local_owner { FLAG_LOCAL } else { 0 },
        };
        snapshot.set_entry(slot, entry);
        snapshot.clear_migrating_slot(slot);
        self.active.remove(&slot);
        self.importing.remove(&slot);
    }
}

pub fn route_migration_command(
    snapshot: &SlotTableSnapshot,
    key: &[u8],
    write: bool,
    current_shard: usize,
    asking: &mut AskingState,
) -> RouteDecision {
    route_with_options(
        snapshot,
        key,
        write,
        RouteOptions {
            current_shard,
            asking: asking.consume(),
        },
    )
}

pub fn resolve_slot_owner_conflict(
    left_owner: NodeId,
    left_epoch: u64,
    right_owner: NodeId,
    right_epoch: u64,
) -> ConflictResolution {
    if left_epoch > right_epoch {
        return ConflictResolution {
            winner: left_owner,
            loser: right_owner,
            epoch: left_epoch,
            loser_must_bump_epoch: false,
        };
    }

    if right_epoch > left_epoch {
        return ConflictResolution {
            winner: right_owner,
            loser: left_owner,
            epoch: right_epoch,
            loser_must_bump_epoch: false,
        };
    }

    if left_owner >= right_owner {
        ConflictResolution {
            winner: left_owner,
            loser: right_owner,
            epoch: left_epoch,
            loser_must_bump_epoch: true,
        }
    } else {
        ConflictResolution {
            winner: right_owner,
            loser: left_owner,
            epoch: right_epoch,
            loser_must_bump_epoch: true,
        }
    }
}

fn frame_bytes<'a>(frame: &'a Frame<'_>) -> Result<&'a [u8], SenkoError> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) | Frame::BlobError(bytes) => {
            Ok(bytes)
        }
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::Protocol("command arguments must be strings")),
    }
}

fn parse_u16(raw: &[u8]) -> Result<u16, SenkoError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u16>().ok())
        .ok_or(SenkoError::Protocol("ERR invalid port"))
}

fn parse_u32(raw: &[u8]) -> Result<u32, SenkoError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u32>().ok())
        .ok_or(SenkoError::Protocol(
            "ERR value is not an integer or out of range",
        ))
}

fn parse_u64(raw: &[u8]) -> Result<u64, SenkoError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or(SenkoError::Protocol(
            "ERR value is not an integer or out of range",
        ))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_cluster::{FLAG_LOCAL, NodeId, RouteDecision, crc16_slot};
    use senko_core::SenkoValue;

    use super::{
        AskingState, MigrateRequest, MigrationManager, SlotIndex, resolve_slot_owner_conflict,
        route_migration_command,
    };
    use senko_cluster::SlotTableSnapshot;
    use senko_proto::Frame;
    use senko_store::Store;

    fn bs<'a>(bytes: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(bytes)
    }

    fn set_string(store: &mut Store, key: &str, value: &str) {
        let _ = store.set(
            CompactString::from(key),
            SenkoValue::Raw(Bytes::copy_from_slice(value.as_bytes())),
            Default::default(),
        );
    }

    fn get_string(store: &mut Store, key: &str) -> Option<Vec<u8>> {
        store
            .get(key.as_bytes())
            .map(|value| value.as_bytes().to_vec())
    }

    fn key_for_slot(slot: u16, prefix: &str, seed: usize) -> String {
        let mut attempt = seed;
        loop {
            let candidate = format!("{{{prefix}:{attempt}}}");
            if crc16_slot(candidate.as_bytes()) == slot {
                return candidate;
            }
            attempt += 1;
        }
    }

    #[test]
    fn migrate_request_parses_batch_and_auth_options() {
        let request = MigrateRequest::parse(&[
            bs(b"127.0.0.1"),
            bs(b"6380"),
            bs(b""),
            bs(b"0"),
            bs(b"1000"),
            bs(b"COPY"),
            bs(b"REPLACE"),
            bs(b"AUTH2"),
            bs(b"default"),
            bs(b"secret"),
            bs(b"KEYS"),
            bs(b"k1"),
            bs(b"k2"),
        ])
        .unwrap();

        assert_eq!(request.port, 6380);
        assert!(request.copy);
        assert!(request.replace);
        assert_eq!(request.key, None);
        assert_eq!(request.keys.len(), 2);
    }

    #[test]
    fn slot_index_counts_and_enumerates_keys_in_slot() {
        let slot = 0u16;
        let mut store = Store::new(None);
        let key_a = key_for_slot(slot, "slot-zero-a", 0);
        let key_b = key_for_slot(slot, "slot-zero-b", 1_000);
        let other = key_for_slot(1, "slot-one", 0);

        set_string(&mut store, &key_a, "a");
        set_string(&mut store, &key_b, "b");
        set_string(&mut store, &other, "c");

        let mut index = SlotIndex::new();
        index.rebuild_from_store(&mut store);

        assert_eq!(index.count_keys_in_slot(slot), 2);
        let keys = index.get_keys_in_slot(slot, 10);
        assert!(keys.iter().any(|key| key.as_str() == key_a));
        assert!(keys.iter().any(|key| key.as_str() == key_b));
        assert_eq!(index.count_keys_in_slot(1), 1);
    }

    #[test]
    fn basic_slot_migration_moves_only_target_slot_keys() {
        let migrating_slot = 0u16;
        let other_slot = 1u16;
        let source_id = NodeId::new([1; 20]);
        let target_id = NodeId::new([2; 20]);
        let target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001);

        let mut source_store = Store::new(None);
        let mut target_store = Store::new(None);
        let moved_a = key_for_slot(migrating_slot, "move-a", 0);
        let moved_b = key_for_slot(migrating_slot, "move-b", 10_000);
        let stay = key_for_slot(other_slot, "stay", 0);
        set_string(&mut source_store, &moved_a, "va");
        set_string(&mut source_store, &moved_b, "vb");
        set_string(&mut source_store, &stay, "vs");

        let mut source_index = SlotIndex::new();
        source_index.rebuild_from_store(&mut source_store);
        let mut target_index = SlotIndex::new();
        target_index.rebuild_from_store(&mut target_store);

        let mut source_snapshot = SlotTableSnapshot::default();
        source_snapshot.set_entry(
            migrating_slot,
            senko_cluster::SlotEntry {
                node_index: 0,
                shard_index: 0,
                flags: FLAG_LOCAL,
            },
        );
        let mut target_snapshot = SlotTableSnapshot::default();

        let mut manager = MigrationManager::new(8, 2);
        manager
            .set_slot_migrating(
                &mut source_snapshot,
                &source_index,
                migrating_slot,
                target_id,
                1,
                target_addr,
                0,
            )
            .unwrap();
        manager.set_slot_importing(&mut target_snapshot, migrating_slot, source_id, 0, 0);

        loop {
            let step = manager
                .migrate_slot_chunk(
                    migrating_slot,
                    &mut source_store,
                    &mut target_store,
                    &mut source_index,
                    &mut target_index,
                    &mut source_snapshot,
                )
                .unwrap();
            if step.complete {
                break;
            }
        }

        manager.finalize_slot(&mut source_snapshot, migrating_slot, 1, 0, false);
        manager.finalize_slot(&mut target_snapshot, migrating_slot, 1, 0, true);

        assert_eq!(get_string(&mut source_store, &moved_a), None);
        assert_eq!(get_string(&mut source_store, &moved_b), None);
        assert_eq!(
            get_string(&mut target_store, &moved_a),
            Some(b"va".to_vec())
        );
        assert_eq!(
            get_string(&mut target_store, &moved_b),
            Some(b"vb".to_vec())
        );
        assert_eq!(get_string(&mut source_store, &stay), Some(b"vs".to_vec()));
        assert_eq!(source_index.count_keys_in_slot(migrating_slot), 0);
        assert_eq!(target_index.count_keys_in_slot(migrating_slot), 2);
        assert_eq!(source_snapshot.entry(migrating_slot).flags, 0);
        assert_eq!(target_snapshot.entry(migrating_slot).flags, FLAG_LOCAL);
    }

    #[test]
    fn write_during_migration_is_queued_and_preserved() {
        let slot = 0u16;
        let target_id = NodeId::new([9; 20]);
        let target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7002);
        let mut source_store = Store::new(None);
        let mut target_store = Store::new(None);
        let key_a = key_for_slot(slot, "during-a", 0);
        let key_b = key_for_slot(slot, "during-b", 50_000);
        set_string(&mut source_store, &key_a, "a");

        let mut source_index = SlotIndex::new();
        source_index.rebuild_from_store(&mut source_store);
        let mut target_index = SlotIndex::new();
        let mut snapshot = SlotTableSnapshot::default();
        snapshot.set_entry(
            slot,
            senko_cluster::SlotEntry {
                node_index: 0,
                shard_index: 0,
                flags: FLAG_LOCAL,
            },
        );

        let mut manager = MigrationManager::new(8, 1);
        manager
            .set_slot_migrating(
                &mut snapshot,
                &source_index,
                slot,
                target_id,
                1,
                target_addr,
                0,
            )
            .unwrap();

        set_string(&mut source_store, &key_b, "b");
        source_index.insert(key_b.as_bytes()).unwrap();
        manager.note_local_write(slot, key_b.as_bytes()).unwrap();

        loop {
            let step = manager
                .migrate_slot_chunk(
                    slot,
                    &mut source_store,
                    &mut target_store,
                    &mut source_index,
                    &mut target_index,
                    &mut snapshot,
                )
                .unwrap();
            if step.complete {
                break;
            }
        }

        assert_eq!(get_string(&mut target_store, &key_a), Some(b"a".to_vec()));
        assert_eq!(get_string(&mut target_store, &key_b), Some(b"b".to_vec()));
        assert_eq!(get_string(&mut source_store, &key_a), None);
        assert_eq!(get_string(&mut source_store, &key_b), None);
    }

    #[test]
    fn ask_redirect_flips_after_key_is_migrated_and_asking_is_one_shot() {
        let slot = 0u16;
        let source_id = NodeId::new([1; 20]);
        let target_id = NodeId::new([2; 20]);
        let target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7003);
        let key = key_for_slot(slot, "ask", 0);

        let mut source_store = Store::new(None);
        let mut target_store = Store::new(None);
        set_string(&mut source_store, &key, "value");

        let mut source_index = SlotIndex::new();
        source_index.rebuild_from_store(&mut source_store);
        let mut target_index = SlotIndex::new();
        let mut source_snapshot = SlotTableSnapshot::default();
        source_snapshot.set_route_node(1, target_id, target_addr);
        source_snapshot.set_entry(
            slot,
            senko_cluster::SlotEntry {
                node_index: 0,
                shard_index: 0,
                flags: FLAG_LOCAL,
            },
        );
        let mut target_snapshot = SlotTableSnapshot::default();
        target_snapshot.set_route_node(
            0,
            source_id,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000),
        );

        let mut manager = MigrationManager::new(8, 1);
        manager
            .set_slot_migrating(
                &mut source_snapshot,
                &source_index,
                slot,
                target_id,
                1,
                target_addr,
                0,
            )
            .unwrap();
        manager.set_slot_importing(&mut target_snapshot, slot, source_id, 0, 0);

        let mut asking = AskingState::default();
        assert_eq!(
            route_migration_command(&source_snapshot, key.as_bytes(), true, 0, &mut asking),
            RouteDecision::LocalShard(0)
        );

        let _ = manager
            .migrate_slot_chunk(
                slot,
                &mut source_store,
                &mut target_store,
                &mut source_index,
                &mut target_index,
                &mut source_snapshot,
            )
            .unwrap();

        assert_eq!(
            route_migration_command(&source_snapshot, key.as_bytes(), true, 0, &mut asking),
            RouteDecision::Ask(target_id, target_addr)
        );

        assert_eq!(
            route_migration_command(&target_snapshot, key.as_bytes(), true, 0, &mut asking),
            RouteDecision::Moved(
                source_id,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)
            )
        );

        asking.arm();
        assert_eq!(
            route_migration_command(&target_snapshot, key.as_bytes(), true, 0, &mut asking),
            RouteDecision::LocalShard(0)
        );
        assert_eq!(
            route_migration_command(&target_snapshot, key.as_bytes(), true, 0, &mut asking),
            RouteDecision::Moved(
                source_id,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)
            )
        );
    }

    #[test]
    fn higher_node_id_wins_same_epoch_conflict() {
        let low = NodeId::new([1; 20]);
        let high = NodeId::new([2; 20]);
        let resolution = resolve_slot_owner_conflict(low, 9, high, 9);
        assert_eq!(resolution.winner, high);
        assert_eq!(resolution.loser, low);
        assert!(resolution.loser_must_bump_epoch);
    }
}
