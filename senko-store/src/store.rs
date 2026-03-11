use std::{
    cell::Cell,
    time::{SystemTime, UNIX_EPOCH},
};

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::{HashMap, HashTable};
use senko_core::{HashObject, QuickList, SenkoValue, SetObject, StreamObject, ZSetObject};

use crate::{
    eviction::{self, EVICTION_SAMPLE_SIZE, MemoryAccountant},
    expiry::{FieldExpiryWheel, TimerWheel},
};

const DEFAULT_BUCKETS: usize = 64;
const LOAD_FACTOR_NUMERATOR: usize = 3;
const LOAD_FACTOR_DENOMINATOR: usize = 4;
const REHASH_BATCH_BUCKETS: usize = 8;
const LRU_GRANULARITY_MS: u64 = 10_000;

type Slot = (CompactString, Entry);

#[repr(C)]
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub value: SenkoValue,
    pub expires_at: Option<u64>,
    pub lru_clock: Cell<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationSnapshotEntry {
    pub key: CompactString,
    pub dump: Bytes,
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCondition {
    Always,
    NX,
    XX,
    IfEq(Bytes),
    IfNe(Bytes),
    IfDeq(Bytes),
    IfDne(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetExpiry {
    None,
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
    KeepTtl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOptions {
    pub condition: SetCondition,
    pub expiry: SetExpiry,
    pub get_old: bool,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            condition: SetCondition::Always,
            expiry: SetExpiry::None,
            get_old: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetResult {
    pub applied: bool,
    pub old_value: Option<SenkoValue>,
}

#[derive(Debug)]
struct RehashState {
    table: HashTable<Slot>,
    scan_bucket: usize,
}

#[derive(Debug)]
pub struct Store {
    primary: HashTable<Slot>,
    resize: Option<RehashState>,
    global_version: u64,
    per_key_version: HashMap<CompactString, u64, RandomState>,
    expiry_wheel: Box<TimerWheel>,
    field_expiry_wheel: Box<FieldExpiryWheel>,
    hasher: RandomState,
    clock_ms: u64,
    max_memory: Option<usize>,
    memory: MemoryAccountant,
    total_commands_processed: usize,
    eviction_seed: u64,
    no_touch: bool,
}

impl Default for Store {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Store {
    pub fn new(max_memory: Option<usize>) -> Self {
        let clock_ms = current_unix_ms();
        Self {
            primary: HashTable::with_capacity(DEFAULT_BUCKETS),
            resize: None,
            global_version: 0,
            per_key_version: HashMap::with_hasher(RandomState::new()),
            expiry_wheel: Box::new(TimerWheel::new(clock_ms)),
            field_expiry_wheel: Box::new(FieldExpiryWheel::new(clock_ms)),
            hasher: RandomState::new(),
            clock_ms,
            max_memory,
            memory: MemoryAccountant::default(),
            total_commands_processed: 0,
            eviction_seed: clock_ms,
            no_touch: false,
        }
    }

    pub fn record_command(&mut self) {
        self.total_commands_processed = self.total_commands_processed.saturating_add(1);
    }

    pub fn key_version(&self, key: &[u8]) -> u64 {
        self.per_key_version
            .iter()
            .find_map(|(stored_key, version)| (stored_key.as_bytes() == key).then_some(*version))
            .unwrap_or(0)
    }

    pub fn notify_watchers(&mut self, key: &[u8]) -> u64 {
        self.global_version = self.global_version.saturating_add(1);
        let version = self.global_version;
        match CompactString::from_utf8(key) {
            Ok(key) => {
                self.per_key_version.insert(key, version);
            }
            Err(_) => {
                if let Some((stored_key, _)) = self.find_slot_ref(key) {
                    self.per_key_version.insert(stored_key.clone(), version);
                }
            }
        }
        version
    }

    pub fn total_commands_processed(&self) -> usize {
        self.total_commands_processed
    }

    pub fn set_no_touch(&mut self, no_touch: bool) {
        self.no_touch = no_touch;
    }

    pub fn no_touch(&self) -> bool {
        self.no_touch
    }

    pub fn next_random_seed(&mut self) -> u64 {
        self.eviction_seed = self
            .eviction_seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(self.clock_ms | 1);
        self.eviction_seed
    }

    pub fn used_memory(&self) -> usize {
        self.memory.get()
    }

    pub fn memory_usage(&mut self, key: &[u8]) -> Option<usize> {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return None;
        }
        self.find_slot_ref(key)
            .map(|(stored_key, entry)| crate::eviction::entry_bytes(stored_key, entry))
    }

    pub fn expiry_count(&self) -> usize {
        self.primary
            .iter()
            .filter(|(_, entry)| entry.expires_at.is_some())
            .count()
            + self
                .resize
                .as_ref()
                .map(|resize| {
                    resize
                        .table
                        .iter()
                        .filter(|(_, entry)| entry.expires_at.is_some())
                        .count()
                })
                .unwrap_or(0)
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&SenkoValue> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired
                && !self.no_touch && entry.lru_clock.get() != now_lru {
                    entry.lru_clock.set(now_lru);
                }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        self.find_slot_ref(key).map(|(_, entry)| &entry.value)
    }

    pub fn get_cloned(&mut self, key: &[u8]) -> Option<SenkoValue> {
        let clock_snapshot = self.clock_ms;
        let mut expired = false;
        let value = if let Some((_, entry)) = self.find_slot_ref(key) {
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired {
                if !self.no_touch && entry.lru_clock.get() != now_lru {
                    entry.lru_clock.set(now_lru);
                }
                Some(entry.value.clone())
            } else {
                None
            }
        } else {
            None
        };
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        value
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        Some(entry)
    }

    pub fn set(&mut self, key: CompactString, value: SenkoValue, opts: SetOptions) -> SetResult {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let (old_value, condition_applies, expires_at) = {
            let existing = self.find_slot(key.as_bytes()).map(|(_, entry)| entry);
            let old_value = if opts.get_old {
                existing.map(|entry| entry.value.clone())
            } else {
                None
            };
            let condition_applies = condition_matches(&opts.condition, existing);
            let expires_at = expiry_from_option(opts.expiry, existing, self.clock_ms);
            (old_value, condition_applies, expires_at)
        };

        if !condition_applies {
            return SetResult {
                applied: false,
                old_value,
            };
        }

        let entry = Entry {
            value,
            expires_at,
            lru_clock: Cell::new(lru_clock(self.clock_ms)),
        };
        self.insert_or_replace(key.clone(), entry);
        self.expiry_wheel.update_expiry(&key, None, expires_at);
        self.maybe_start_resize();
        self.maybe_evict();

        SetResult {
            applied: true,
            old_value,
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key, self.clock_ms);
        self.remove_entry(key).is_some()
    }

    pub fn delete_with_type(&mut self, key: &[u8]) -> Option<&'static [u8]> {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key, self.clock_ms);
        self.remove_entry(key)
            .map(|entry| value_type_name(&entry.value))
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let now_ms = current_unix_ms();
        self.find_slot_ref(key)
            .is_some_and(|(_, entry)| !is_expired(entry.expires_at, now_ms))
    }

    pub fn ttl_ms(&self, key: &[u8]) -> Option<i64> {
        let now_ms = current_unix_ms();
        let (_, entry) = self.find_slot_ref(key)?;
        match entry.expires_at {
            None => Some(-1),
            Some(deadline) if deadline <= now_ms => None,
            Some(deadline) => Some(deadline.saturating_sub(now_ms) as i64),
        }
    }

    pub fn type_name(&mut self, key: &[u8]) -> Option<&'static [u8]> {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return None;
        }
        self.find_slot_ref(key)
            .map(|(_, entry)| value_type_name(&entry.value))
    }

    pub fn clone_entry(&mut self, key: &[u8]) -> Option<Entry> {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return None;
        }
        self.find_slot_ref(key).map(|(_, entry)| entry.clone())
    }

    pub fn replication_snapshot(&mut self) -> Vec<ReplicationSnapshotEntry> {
        let now_ms = current_unix_ms();
        let keys = self
            .live_entries_snapshot(now_ms)
            .into_iter()
            .map(|(key, _, _, _)| key)
            .collect::<Vec<_>>();
        let mut snapshot = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(entry) = self.clone_entry(key.as_bytes()) {
                snapshot.push(ReplicationSnapshotEntry {
                    key,
                    dump: crate::commands::generic::migrate::dump_value(
                        &entry.value,
                        entry.expires_at,
                    ),
                    expires_at: entry.expires_at,
                });
            }
        }
        snapshot
    }

    pub fn live_keys_snapshot(&mut self) -> Vec<CompactString> {
        let now_ms = current_unix_ms();
        self.live_entries_snapshot(now_ms)
            .into_iter()
            .map(|(key, _, _, _)| key)
            .collect()
    }

    pub fn rename(
        &mut self,
        source: &[u8],
        dest: CompactString,
    ) -> Option<(&'static [u8], Option<&'static [u8]>)> {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(source, self.clock_ms) {
            return None;
        }
        let _ = self.remove_if_expired(dest.as_bytes(), self.clock_ms);
        let (_stored_source, entry) = self.remove_slot(source)?;
        let source_type = value_type_name(&entry.value);
        let overwritten_type = self
            .remove_entry(dest.as_bytes())
            .map(|existing| value_type_name(&existing.value));
        let expires_at = entry.expires_at;
        self.insert_or_replace(
            dest.clone(),
            Entry {
                value: entry.value,
                expires_at,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            },
        );
        self.expiry_wheel.update_expiry(&dest, None, expires_at);
        if let Some((_, new_entry)) = self.find_slot_ref(dest.as_bytes()) {
            let value = new_entry.value.clone();
            self.schedule_field_expiries_for_value(&dest, &value);
        }
        self.maybe_start_resize();
        Some((source_type, overwritten_type))
    }

    pub fn copy_from_entry(
        &mut self,
        dest: CompactString,
        source_entry: &Entry,
        new_expires_at: Option<u64>,
    ) -> Option<&'static [u8]> {
        self.refresh_clock();
        self.incremental_rehash_step();
        let _ = self.remove_if_expired(dest.as_bytes(), self.clock_ms);
        let overwritten = self
            .remove_entry(dest.as_bytes())
            .map(|entry| value_type_name(&entry.value));
        let new_entry = Entry {
            value: source_entry.value.clone(),
            expires_at: new_expires_at,
            lru_clock: Cell::new(lru_clock(self.clock_ms)),
        };
        self.insert_or_replace(dest.clone(), new_entry.clone());
        self.expiry_wheel.update_expiry(&dest, None, new_expires_at);
        self.schedule_field_expiries_for_value(&dest, &new_entry.value);
        self.maybe_start_resize();
        self.maybe_evict();
        overwritten
    }

    pub fn set_expiry(&mut self, key: &[u8], expires_at: u64) {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return;
        }
        let scheduled = if let Some((stored_key, entry)) = self.find_slot_mut(key) {
            let previous_expiry = entry.expires_at;
            entry.expires_at = Some(expires_at);
            Some((stored_key.clone(), previous_expiry))
        } else {
            None
        };
        if let Some((scheduled_key, previous_expiry)) = scheduled {
            self.expiry_wheel
                .update_expiry(&scheduled_key, previous_expiry, Some(expires_at));
        }
    }

    pub fn remove_expiry(&mut self, key: &[u8]) {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return;
        }
        let cleared = if let Some((stored_key, entry)) = self.find_slot_mut(key) {
            let previous_expiry = entry.expires_at;
            entry.expires_at = None;
            Some((stored_key.clone(), previous_expiry))
        } else {
            None
        };
        if let Some((stored_key, previous_expiry)) = cleared {
            self.expiry_wheel
                .update_expiry(&stored_key, previous_expiry, None);
        }
    }

    pub fn expiretime_ms(&mut self, key: &[u8]) -> Option<i64> {
        self.refresh_clock();
        self.incremental_rehash_step();
        if self.remove_if_expired(key, self.clock_ms) {
            return None;
        }
        let (_, entry) = self.find_slot_ref(key)?;
        entry
            .expires_at
            .map(|deadline| deadline as i64)
            .or(Some(-1))
    }

    pub fn touch(&mut self, key: &[u8]) -> bool {
        self.get_mut(key).is_some()
    }

    pub fn entry_count(&self) -> usize {
        self.primary.len() + self.resize.as_ref().map_or(0, |resize| resize.table.len())
    }

    pub fn clear(&mut self) {
        self.clock_ms = current_unix_ms();
        self.primary = HashTable::with_capacity(DEFAULT_BUCKETS);
        self.resize = None;
        self.global_version = 0;
        self.per_key_version.clear();
        *self.expiry_wheel = TimerWheel::new(self.clock_ms);
        *self.field_expiry_wheel = FieldExpiryWheel::new(self.clock_ms);
        self.memory = MemoryAccountant::default();
    }

    pub fn advance_expiry_wheel(&mut self, now_ms: u64) -> usize {
        self.clock_ms = self.clock_ms.max(now_ms);
        self.incremental_rehash_step();
        let expired = self.expiry_wheel.advance(self.clock_ms);
        let expired_fields = self.field_expiry_wheel.advance(self.clock_ms);
        let mut deleted = 0usize;
        for key in expired {
            if self.remove_if_expired(key.as_bytes(), self.clock_ms) {
                deleted += 1;
            }
        }
        for (key, field) in expired_fields {
            let now_ms = self.clock_ms;
            let mut should_delete_key = false;
            if let Some((_, entry)) = self.find_slot_mut(key.as_bytes())
                && let SenkoValue::Hash(hash) = &mut entry.value
            {
                let _ = hash.get_mut(field.as_bytes(), now_ms);
                should_delete_key = hash.is_empty(now_ms);
            }
            if should_delete_key && self.remove_entry(key.as_bytes()).is_some() {
                deleted += 1;
            }
        }
        let keys_with_field_ttl: Vec<CompactString> = self
            .primary
            .iter()
            .chain(
                self.resize
                    .as_ref()
                    .into_iter()
                    .flat_map(|resize| resize.table.iter()),
            )
            .filter_map(|(key, entry)| match &entry.value {
                SenkoValue::Hash(hash) if hash.has_field_expiry => Some(key.clone()),
                _ => None,
            })
            .collect();
        for key in keys_with_field_ttl {
            let now_ms = self.clock_ms;
            let mut should_delete_key = false;
            if let Some((_, entry)) = self.find_slot_mut(key.as_bytes())
                && let SenkoValue::Hash(hash) = &mut entry.value
            {
                let _ = hash.drain_expired(now_ms);
                should_delete_key = hash.is_empty(now_ms);
            }
            if should_delete_key && self.remove_entry(key.as_bytes()).is_some() {
                deleted += 1;
            }
        }
        deleted
    }

    pub fn schedule_hash_field_expiry(
        &mut self,
        key: CompactString,
        field: CompactString,
        expires_at: u64,
    ) {
        self.field_expiry_wheel
            .schedule_field(key, field, expires_at);
    }

    pub fn get_hash(&mut self, key: &[u8]) -> Option<&HashObject> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired && !self.no_touch && entry.lru_clock.get() != now_lru {
                entry.lru_clock.set(now_lru);
            }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Hash(hash) => Some(hash.is_empty(clock_snapshot)),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }
        self.find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Hash(hash) => Some(hash.as_ref()),
                _ => None,
            })
    }

    pub fn get_hash_mut(&mut self, key: &[u8]) -> Option<&mut HashObject> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Hash(hash) => Some(hash.is_empty(clock_snapshot)),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        match &mut entry.value {
            SenkoValue::Hash(hash) => Some(hash.as_mut()),
            _ => None,
        }
    }

    pub fn get_or_create_hash(&mut self, key: CompactString) -> &mut HashObject {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let exists_as_hash = self
            .find_slot_ref(key.as_bytes())
            .is_some_and(|(_, entry)| matches!(entry.value, SenkoValue::Hash(_)));

        if !exists_as_hash {
            let entry = Entry {
                value: SenkoValue::Hash(Box::new(HashObject::with_hasher(self.hasher.clone()))),
                expires_at: None,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            };
            self.insert_or_replace(key.clone(), entry);
        }

        let (_, entry) = self
            .find_slot_mut(key.as_bytes())
            .expect("hash key must exist after insertion");
        if let SenkoValue::Hash(hash) = &mut entry.value {
            return hash.as_mut();
        }
        unreachable!("non-hash value after hash insertion")
    }

    pub fn get_list(&mut self, key: &[u8]) -> Option<&QuickList> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired && !self.no_touch && entry.lru_clock.get() != now_lru {
                entry.lru_clock.set(now_lru);
            }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::List(list) => Some(list.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }
        self.find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::List(list) => Some(list.as_ref()),
                _ => None,
            })
    }

    pub fn get_list_mut(&mut self, key: &[u8]) -> Option<&mut QuickList> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::List(list) => Some(list.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        match &mut entry.value {
            SenkoValue::List(list) => Some(list.as_mut()),
            _ => None,
        }
    }

    pub fn get_or_create_list(&mut self, key: CompactString) -> &mut QuickList {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let exists_as_list = self
            .find_slot_ref(key.as_bytes())
            .is_some_and(|(_, entry)| matches!(entry.value, SenkoValue::List(_)));

        if !exists_as_list {
            let entry = Entry {
                value: SenkoValue::List(Box::default()),
                expires_at: None,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            };
            self.insert_or_replace(key.clone(), entry);
        }

        let (_, entry) = self
            .find_slot_mut(key.as_bytes())
            .expect("list key must exist after insertion");
        if let SenkoValue::List(list) = &mut entry.value {
            return list.as_mut();
        }
        unreachable!("non-list value after list insertion")
    }

    pub fn remove_list_if_empty(&mut self, key: &[u8]) {
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::List(list) => Some(list.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
        }
    }

    pub fn get_set(&mut self, key: &[u8]) -> Option<&SetObject> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired && !self.no_touch && entry.lru_clock.get() != now_lru {
                entry.lru_clock.set(now_lru);
            }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Set(set) => Some(set.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }
        self.find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Set(set) => Some(set.as_ref()),
                _ => None,
            })
    }

    pub fn get_set_mut(&mut self, key: &[u8]) -> Option<&mut SetObject> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Set(set) => Some(set.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        match &mut entry.value {
            SenkoValue::Set(set) => Some(set.as_mut()),
            _ => None,
        }
    }

    pub fn get_or_create_set(&mut self, key: CompactString) -> &mut SetObject {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let exists_as_set = self
            .find_slot_ref(key.as_bytes())
            .is_some_and(|(_, entry)| matches!(entry.value, SenkoValue::Set(_)));

        if !exists_as_set {
            let entry = Entry {
                value: SenkoValue::Set(Box::new(SetObject::with_hasher(self.hasher.clone()))),
                expires_at: None,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            };
            self.insert_or_replace(key.clone(), entry);
        }

        let (_, entry) = self
            .find_slot_mut(key.as_bytes())
            .expect("set key must exist after insertion");
        if let SenkoValue::Set(set) = &mut entry.value {
            return set.as_mut();
        }
        unreachable!("non-set value after set insertion")
    }

    pub fn remove_set_if_empty(&mut self, key: &[u8]) {
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Set(set) => Some(set.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
        }
    }

    pub fn get_stream(&mut self, key: &[u8]) -> Option<&StreamObject> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired && !self.no_touch && entry.lru_clock.get() != now_lru {
                entry.lru_clock.set(now_lru);
            }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        self.find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::Stream(stream) => Some(stream.as_ref()),
                _ => None,
            })
    }

    pub fn get_stream_mut(&mut self, key: &[u8]) -> Option<&mut StreamObject> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        match &mut entry.value {
            SenkoValue::Stream(stream) => Some(stream.as_mut()),
            _ => None,
        }
    }

    pub fn get_or_create_stream(&mut self, key: CompactString) -> &mut StreamObject {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let exists_as_stream = self
            .find_slot_ref(key.as_bytes())
            .is_some_and(|(_, entry)| matches!(entry.value, SenkoValue::Stream(_)));

        if !exists_as_stream {
            let entry = Entry {
                value: SenkoValue::Stream(Box::new(StreamObject::new())),
                expires_at: None,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            };
            self.insert_or_replace(key.clone(), entry);
        }

        let (_, entry) = self
            .find_slot_mut(key.as_bytes())
            .expect("stream key must exist after insertion");
        if let SenkoValue::Stream(stream) = &mut entry.value {
            return stream.as_mut();
        }
        unreachable!("non-stream value after stream insertion")
    }

    pub fn get_zset(&mut self, key: &[u8]) -> Option<&ZSetObject> {
        let clock_snapshot = self.clock_ms;
        let mut found = false;
        let mut expired = false;
        if let Some((_, entry)) = self.find_slot_ref(key) {
            found = true;
            let now_lru = if let Some(deadline) = entry.expires_at {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    expired = true;
                    0
                } else {
                    lru_clock(now_ms)
                }
            } else {
                lru_clock(clock_snapshot)
            };

            if !expired && !self.no_touch && entry.lru_clock.get() != now_lru {
                entry.lru_clock.set(now_lru);
            }
        }
        if expired {
            let _ = self.remove_entry(key);
            return None;
        }
        if !found {
            return None;
        }
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::ZSet(zset) => Some(zset.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }
        self.find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::ZSet(zset) => Some(zset.as_ref()),
                _ => None,
            })
    }

    pub fn get_zset_mut(&mut self, key: &[u8]) -> Option<&mut ZSetObject> {
        let clock_snapshot = self.clock_ms;
        let now_lru = match self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
        {
            Some(deadline) => {
                let now_ms = current_unix_ms();
                if deadline <= now_ms {
                    let _ = self.remove_entry(key);
                    return None;
                }
                lru_clock(now_ms)
            }
            None => lru_clock(clock_snapshot),
        };

        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::ZSet(zset) => Some(zset.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
            return None;
        }

        let no_touch = self.no_touch;
        let (_, entry) = self.find_slot_mut(key)?;
        if !no_touch && entry.lru_clock.get() != now_lru {
            entry.lru_clock.set(now_lru);
        }
        match &mut entry.value {
            SenkoValue::ZSet(zset) => Some(zset.as_mut()),
            _ => None,
        }
    }

    pub fn get_or_create_zset(&mut self, key: CompactString) -> &mut ZSetObject {
        self.refresh_clock();
        self.incremental_rehash_step();
        self.remove_if_expired(key.as_bytes(), self.clock_ms);

        let exists_as_zset = self
            .find_slot_ref(key.as_bytes())
            .is_some_and(|(_, entry)| matches!(entry.value, SenkoValue::ZSet(_)));

        if !exists_as_zset {
            let entry = Entry {
                value: SenkoValue::ZSet(Box::new(ZSetObject::with_hasher(self.hasher.clone()))),
                expires_at: None,
                lru_clock: Cell::new(lru_clock(self.clock_ms)),
            };
            self.insert_or_replace(key.clone(), entry);
        }

        let (_, entry) = self
            .find_slot_mut(key.as_bytes())
            .expect("zset key must exist after insertion");
        if let SenkoValue::ZSet(zset) = &mut entry.value {
            return zset.as_mut();
        }
        unreachable!("non-zset value after zset insertion")
    }

    pub fn remove_zset_if_empty(&mut self, key: &[u8]) {
        let should_delete = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| match &entry.value {
                SenkoValue::ZSet(zset) => Some(zset.is_empty()),
                _ => None,
            })
            .unwrap_or(false);
        if should_delete {
            let _ = self.remove_entry(key);
        }
    }

    pub fn info(&self) -> String {
        format!(
            "# Server\r\nredis_version:7.0.0-senko\r\n# Stats\r\ntotal_commands_processed:{}\r\n# Memory\r\nused_memory:{}\r\n# Keyspace\r\ndb0:keys={},expires={}\r\n",
            self.total_commands_processed(),
            self.used_memory(),
            self.entry_count(),
            self.expiry_count()
        )
    }

    fn refresh_clock(&mut self) {
        self.clock_ms = current_unix_ms();
    }

    fn insert_or_replace(&mut self, key: CompactString, entry: Entry) {
        let hash = self.hash_key(key.as_bytes());
        let hasher = self.hasher.clone();
        if let Some((stored_key, old_expires_at, old_bytes, new_bytes, had_field_expiry)) = self
            .primary
            .find_mut(hash, |(candidate, _)| {
                candidate.as_bytes() == key.as_bytes()
            })
            .map(|(stored_key, existing)| {
                let old_key = stored_key.clone();
                let old_expires_at = existing.expires_at;
                let old_bytes = eviction::entry_bytes(stored_key, existing);
                let new_bytes = eviction::entry_bytes(stored_key, &entry);
                let had_field_expiry =
                    matches!(&existing.value, SenkoValue::Hash(hash) if hash.has_field_expiry);
                *existing = entry.clone();
                (
                    old_key,
                    old_expires_at,
                    old_bytes,
                    new_bytes,
                    had_field_expiry,
                )
            })
        {
            self.expiry_wheel
                .tombstone(stored_key.as_bytes(), old_expires_at);
            if had_field_expiry {
                self.field_expiry_wheel.tombstone_key(stored_key.as_bytes());
            }
            if new_bytes >= old_bytes {
                self.memory.add(new_bytes - old_bytes);
            } else {
                self.memory.sub(old_bytes - new_bytes);
            }
            return;
        }
        if let Some(resize) = self.resize.as_mut()
            && let Ok(found) = resize.table.find_entry(hash, |(candidate, _)| {
                candidate.as_bytes() == key.as_bytes()
            })
        {
            let ((old_key, old_entry), _) = found.remove();
            self.expiry_wheel
                .tombstone(old_key.as_bytes(), old_entry.expires_at);
            self.tombstone_field_expiries_for_value(old_key.as_bytes(), &old_entry.value);
            self.memory.sub(eviction::entry_bytes(&old_key, &old_entry));
        }
        self.memory.add(eviction::entry_bytes(&key, &entry));
        self.primary
            .insert_unique(hash, (key, entry), move |(stored_key, _)| {
                hasher.hash_one(stored_key.as_bytes())
            });
    }

    fn remove_entry(&mut self, key: &[u8]) -> Option<Entry> {
        self.remove_slot(key).map(|(_, entry)| entry)
    }

    fn remove_slot(&mut self, key: &[u8]) -> Option<(CompactString, Entry)> {
        let hash = self.hash_key(key);
        if let Ok(found) = self
            .primary
            .find_entry(hash, |(candidate, _)| candidate.as_bytes() == key)
        {
            let ((stored_key, entry), _) = found.remove();
            self.expiry_wheel
                .tombstone(stored_key.as_bytes(), entry.expires_at);
            self.tombstone_field_expiries_for_value(stored_key.as_bytes(), &entry.value);
            self.memory.sub(eviction::entry_bytes(&stored_key, &entry));
            return Some((stored_key, entry));
        }
        let resize = self.resize.as_mut()?;
        let found = resize
            .table
            .find_entry(hash, |(candidate, _)| candidate.as_bytes() == key)
            .ok()?;
        let ((stored_key, entry), _) = found.remove();
        self.expiry_wheel
            .tombstone(stored_key.as_bytes(), entry.expires_at);
        self.tombstone_field_expiries_for_value(stored_key.as_bytes(), &entry.value);
        self.memory.sub(eviction::entry_bytes(&stored_key, &entry));
        Some((stored_key, entry))
    }

    fn remove_if_expired(&mut self, key: &[u8], now_ms: u64) -> bool {
        let expired = self
            .find_slot_ref(key)
            .and_then(|(_, entry)| entry.expires_at)
            .is_some_and(|deadline| deadline <= now_ms);
        if expired {
            let _ = self.remove_entry(key);
            return true;
        }
        false
    }

    fn find_slot(&self, key: &[u8]) -> Option<&Slot> {
        self.find_slot_in(&self.primary, key).or_else(|| {
            self.resize
                .as_ref()
                .and_then(|resize| self.find_slot_in(&resize.table, key))
        })
    }

    fn find_slot_ref(&self, key: &[u8]) -> Option<(&CompactString, &Entry)> {
        self.find_slot(key)
            .map(|(stored_key, entry)| (stored_key, entry))
    }

    fn find_slot_mut(&mut self, key: &[u8]) -> Option<(&CompactString, &mut Entry)> {
        let hash = self.hash_key(key);
        if let Some((stored_key, entry)) = self
            .primary
            .find_mut(hash, |(candidate, _)| candidate.as_bytes() == key)
        {
            return Some((&*stored_key, entry));
        }
        self.resize
            .as_mut()?
            .table
            .find_mut(hash, |(candidate, _)| candidate.as_bytes() == key)
            .map(|(stored_key, entry)| (&*stored_key, entry))
    }

    fn find_slot_in<'a>(&'a self, table: &'a HashTable<Slot>, key: &[u8]) -> Option<&'a Slot> {
        let hash = self.hash_key(key);
        table.find(hash, |(candidate, _)| candidate.as_bytes() == key)
    }

    fn maybe_start_resize(&mut self) {
        if self.resize.is_some() {
            return;
        }
        let buckets = self.primary.num_buckets().max(1);
        if self.primary.len() * LOAD_FACTOR_DENOMINATOR <= buckets * LOAD_FACTOR_NUMERATOR {
            return;
        }
        let new_capacity = (buckets * 2).max(DEFAULT_BUCKETS);
        let old_primary =
            std::mem::replace(&mut self.primary, HashTable::with_capacity(new_capacity));
        self.resize = Some(RehashState {
            table: old_primary,
            scan_bucket: 0,
        });
    }

    fn incremental_rehash_step(&mut self) {
        let mut migrated = 0usize;
        let hasher = self.hasher.clone();
        while migrated < REHASH_BATCH_BUCKETS {
            let Some(resize) = self.resize.as_mut() else {
                break;
            };
            if resize.table.is_empty() {
                self.resize = None;
                break;
            }
            if resize.scan_bucket >= resize.table.num_buckets() {
                resize.scan_bucket = 0;
            }

            let bucket = resize.scan_bucket;
            resize.scan_bucket += 1;
            let Some((key, _)) = resize
                .table
                .get_bucket(bucket)
                .map(|(key, entry)| (key.clone(), entry.expires_at))
            else {
                continue;
            };
            let hash = hasher.hash_one(key.as_bytes());
            if let Ok(found) = resize.table.find_entry(hash, |(candidate, _)| {
                candidate.as_bytes() == key.as_bytes()
            }) {
                let (slot, _) = found.remove();
                self.primary.insert_unique(hash, slot, |(stored_key, _)| {
                    hasher.hash_one(stored_key.as_bytes())
                });
                migrated += 1;
            }
        }
        if self
            .resize
            .as_ref()
            .is_some_and(|resize| resize.table.is_empty())
        {
            self.resize = None;
        }
    }

    fn maybe_evict(&mut self) {
        if !eviction::should_evict(self.used_memory(), self.max_memory) {
            return;
        }
        if let Some(victim) = self.sample_eviction_candidate() {
            let _ = self.remove_entry(victim.as_bytes());
        }
    }

    fn sample_eviction_candidate(&mut self) -> Option<CompactString> {
        let total = self.entry_count();
        if total == 0 {
            return None;
        }
        self.eviction_seed = self
            .eviction_seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(self.clock_ms | 1);
        let start = (self.eviction_seed as usize) % total;
        let live: Vec<(CompactString, u32)> = self
            .iter_live_slots()
            .map(|(key, entry)| (key.clone(), entry.lru_clock.get()))
            .collect();
        if live.is_empty() {
            return None;
        }
        let start = start % live.len();
        let mut oldest: Option<(CompactString, u32)> = None;
        for offset in 0..EVICTION_SAMPLE_SIZE.min(live.len()) {
            let (key, lru_clock) = &live[(start + offset) % live.len()];
            match &mut oldest {
                Some((candidate, lru)) if *lru_clock < *lru => {
                    *candidate = key.clone();
                    *lru = *lru_clock;
                }
                None => oldest = Some((key.clone(), *lru_clock)),
                _ => {}
            }
        }
        oldest.map(|(key, _)| key)
    }

    fn iter_live_slots(&self) -> impl Iterator<Item = (&CompactString, &Entry)> {
        self.primary
            .iter()
            .chain(
                self.resize
                    .as_ref()
                    .into_iter()
                    .flat_map(|resize| resize.table.iter()),
            )
            .filter_map(|(key, entry)| {
                (!is_expired(entry.expires_at, self.clock_ms)).then_some((key, entry))
            })
    }
    fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    pub(crate) fn live_entries_snapshot(
        &self,
        now_ms: u64,
    ) -> Vec<(CompactString, &'static [u8], Option<u64>, u32)> {
        self.primary
            .iter()
            .chain(
                self.resize
                    .as_ref()
                    .into_iter()
                    .flat_map(|resize| resize.table.iter()),
            )
            .map(|(key, entry)| {
                (
                    key.clone(),
                    value_type_name(&entry.value),
                    entry.expires_at,
                    entry.lru_clock.get(),
                )
            })
            .filter(|(_, _, expires_at, _)| !is_expired(*expires_at, now_ms))
            .collect()
    }

    pub(crate) fn bucket_snapshot(
        &self,
        table_index: usize,
        bucket: usize,
    ) -> Option<(CompactString, &'static [u8], Option<u64>)> {
        match table_index {
            0 => self
                .primary
                .get_bucket(bucket)
                .map(|(key, entry)| (key.clone(), value_type_name(&entry.value), entry.expires_at)),
            1 => self
                .resize
                .as_ref()
                .and_then(|resize| resize.table.get_bucket(bucket))
                .map(|(key, entry)| (key.clone(), value_type_name(&entry.value), entry.expires_at)),
            _ => None,
        }
    }

    pub(crate) fn table_bucket_counts(&self) -> [usize; 2] {
        [
            self.primary.num_buckets().max(1),
            self.resize
                .as_ref()
                .map(|resize| resize.table.num_buckets().max(1))
                .unwrap_or(0),
        ]
    }

    #[cfg(test)]
    pub(crate) fn expiry_overflow_contains_deadline(&self, deadline: u64) -> bool {
        self.expiry_wheel.overflow_contains_deadline(deadline)
    }

    fn tombstone_field_expiries_for_value(&mut self, key: &[u8], value: &SenkoValue) {
        if matches!(value, SenkoValue::Hash(hash) if hash.has_field_expiry) {
            self.field_expiry_wheel.tombstone_key(key);
        }
    }

    fn schedule_field_expiries_for_value(&mut self, key: &CompactString, value: &SenkoValue) {
        let SenkoValue::Hash(hash) = value else {
            return;
        };
        if !hash.has_field_expiry {
            return;
        }
        for (field, field_value) in hash.iter_live(self.clock_ms) {
            if let Some(expires_at) = field_value.expires_at {
                self.field_expiry_wheel
                    .schedule_field(key.clone(), field.clone(), expires_at);
            }
        }
    }
}

fn value_type_name(value: &SenkoValue) -> &'static [u8] {
    match value {
        SenkoValue::Raw(_) | SenkoValue::Int(_) | SenkoValue::Float(_) => b"string",
        SenkoValue::Hash(_) => b"hash",
        SenkoValue::List(_) => b"list",
        SenkoValue::Set(_) => b"set",
        SenkoValue::Stream(_) => b"stream",
        SenkoValue::ZSet(_) => b"zset",
        #[cfg(feature = "json")]
        SenkoValue::Json(_) => b"json",
        #[cfg(feature = "vector")]
        SenkoValue::VectorSet(_) => b"vectorset",
        #[cfg(feature = "prob")]
        SenkoValue::BloomFilter(_) => b"MBbloom--",
        #[cfg(feature = "prob")]
        SenkoValue::CuckooFilter(_) => b"cuckooFilter",
        #[cfg(feature = "prob")]
        SenkoValue::CountMinSketch(_) => b"CMSk--",
        #[cfg(feature = "prob")]
        SenkoValue::TopK(_) => b"topk",
        #[cfg(feature = "prob")]
        SenkoValue::TDigest(_) => b"TDIS-TYPE",
    }
}

fn condition_matches(condition: &SetCondition, current: Option<&Entry>) -> bool {
    match condition {
        SetCondition::Always => true,
        SetCondition::NX => current.is_none(),
        SetCondition::XX => current.is_some(),
        SetCondition::IfEq(expected) => {
            current.is_some_and(|entry| entry.value.as_bytes().as_ref() == expected.as_ref())
        }
        SetCondition::IfNe(expected) => {
            current.is_some_and(|entry| entry.value.as_bytes().as_ref() != expected.as_ref())
        }
        SetCondition::IfDeq(expected) => current
            .map(|entry| entry.value.as_bytes().as_ref() == expected.as_ref())
            .unwrap_or(true),
        SetCondition::IfDne(expected) => current
            .map(|entry| entry.value.as_bytes().as_ref() != expected.as_ref())
            .unwrap_or(true),
    }
}

fn expiry_from_option(expiry: SetExpiry, current: Option<&Entry>, now_ms: u64) -> Option<u64> {
    match expiry {
        SetExpiry::None => None,
        SetExpiry::Ex(seconds) => Some(now_ms.saturating_add(seconds.saturating_mul(1_000))),
        SetExpiry::Px(milliseconds) => Some(now_ms.saturating_add(milliseconds)),
        SetExpiry::ExAt(seconds) => Some(seconds.saturating_mul(1_000)),
        SetExpiry::PxAt(milliseconds) => Some(milliseconds),
        SetExpiry::KeepTtl => current.and_then(|entry| entry.expires_at),
    }
}

fn is_expired(expires_at: Option<u64>, now_ms: u64) -> bool {
    expires_at.is_some_and(|deadline| deadline <= now_ms)
}

fn lru_clock(now_ms: u64) -> u32 {
    (now_ms / LRU_GRANULARITY_MS) as u32
}

pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use compact_str::CompactString;
    use senko_core::SenkoValue;

    use crate::store::{SetCondition, SetExpiry, SetOptions, Store, current_unix_ms};

    #[test]
    fn set_get_delete_round_trip() {
        let mut store = Store::default();
        let result = store.set(
            CompactString::from("key"),
            SenkoValue::from(7_i64),
            SetOptions::default(),
        );
        assert!(result.applied);
        assert_eq!(store.get(b"key"), Some(&SenkoValue::Int(7)));
        assert!(store.delete(b"key"));
        assert_eq!(store.get(b"key"), None);
    }

    #[test]
    fn conditional_set_respects_existing_value() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("key"),
            SenkoValue::from(Bytes::from_static(b"one")),
            SetOptions::default(),
        );
        let blocked = store.set(
            CompactString::from("key"),
            SenkoValue::from(Bytes::from_static(b"two")),
            SetOptions {
                condition: SetCondition::IfEq(Bytes::from_static(b"other")),
                expiry: SetExpiry::None,
                get_old: true,
            },
        );
        assert!(!blocked.applied);
        assert_eq!(
            blocked.old_value,
            Some(SenkoValue::from(Bytes::from_static(b"one")))
        );
    }

    #[test]
    fn expiry_wheel_removes_due_keys() {
        let mut store = Store::default();
        let now_ms = current_unix_ms();
        let _ = store.set(
            CompactString::from("key"),
            SenkoValue::from(1_i64),
            SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::PxAt(now_ms + 200),
                get_old: false,
            },
        );
        assert_eq!(store.advance_expiry_wheel(now_ms + 500), 1);
        assert_eq!(store.get(b"key"), None);
    }

    #[test]
    fn active_expiry_expires_many_keys() {
        let mut store = Store::default();
        let now_ms = current_unix_ms();
        for index in 0..1000 {
            let _ = store.set(
                CompactString::from(format!("key:{index}")),
                SenkoValue::from(1_i64),
                SetOptions {
                    condition: SetCondition::Always,
                    expiry: SetExpiry::PxAt(now_ms + 200),
                    get_old: false,
                },
            );
        }
        assert_eq!(store.advance_expiry_wheel(now_ms + 300), 1000);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn overflow_bucket_expires_after_full_rotation() {
        let mut store = Store::default();
        let now_ms = current_unix_ms();
        let _ = store.set(
            CompactString::from("later"),
            SenkoValue::from(1_i64),
            SetOptions {
                condition: SetCondition::Always,
                expiry: SetExpiry::PxAt(now_ms + 60_000),
                get_old: false,
            },
        );
        assert_eq!(store.advance_expiry_wheel(now_ms + 60_000), 1);
        assert_eq!(store.get(b"later"), None);
    }

    #[test]
    fn memory_eviction_reduces_used_memory() {
        let mut store = Store::new(Some(512));
        let before = store.used_memory();
        for index in 0..32 {
            let _ = store.set(
                CompactString::from(format!("evict:{index}")),
                SenkoValue::from(Bytes::from(vec![b'x'; 64])),
                SetOptions::default(),
            );
        }
        assert!(store.used_memory() < 512 || store.entry_count() < 32);
        assert!(store.used_memory() >= before);
    }

    #[test]
    fn zset_accessor_round_trip() {
        let mut store = Store::default();
        let zset = store.get_or_create_zset(CompactString::from("zs"));
        let result = zset.add(1.5, CompactString::from("member"), Default::default());
        assert_eq!(result.added, 1);
        assert_eq!(
            store
                .get_zset(b"zs")
                .and_then(|value| value.score(b"member")),
            Some(1.5)
        );
        assert_eq!(
            store
                .get_zset_mut(b"zs")
                .and_then(|value| value.remove(b"member")),
            Some(1.5)
        );
        store.remove_zset_if_empty(b"zs");
        assert!(store.get_zset(b"zs").is_none());
    }

    #[test]
    fn key_version_starts_at_zero_and_increments_per_write() {
        let mut store = Store::default();

        assert_eq!(store.key_version(b"k"), 0);
        let first = store.notify_watchers(b"k");
        let second = store.notify_watchers(b"k");

        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(store.key_version(b"k"), 2);
    }

    #[test]
    fn key_versions_are_independent_per_key() {
        let mut store = Store::default();

        let first = store.notify_watchers(b"first");
        let second = store.notify_watchers(b"second");

        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(store.key_version(b"first"), 1);
        assert_eq!(store.key_version(b"second"), 2);
    }

    #[test]
    fn del_can_bump_watch_version() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("gone"),
            SenkoValue::from(Bytes::from_static(b"value")),
            SetOptions::default(),
        );
        let watched = store.key_version(b"gone");

        assert!(store.delete(b"gone"));
        let new_version = store.notify_watchers(b"gone");

        assert!(new_version > watched);
        assert_eq!(store.key_version(b"gone"), new_version);
    }

    #[test]
    fn expire_can_bump_watch_version() {
        let mut store = Store::default();
        let _ = store.set(
            CompactString::from("ttl"),
            SenkoValue::from(Bytes::from_static(b"value")),
            SetOptions::default(),
        );
        let watched = store.key_version(b"ttl");

        store.set_expiry(b"ttl", current_unix_ms() + 10_000);
        let new_version = store.notify_watchers(b"ttl");

        assert!(new_version > watched);
        assert_eq!(store.key_version(b"ttl"), new_version);
    }
}
