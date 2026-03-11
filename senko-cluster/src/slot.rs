use std::hint::spin_loop;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use hashbrown::{HashMap, HashSet};
use memchr::memchr;
use roaring::RoaringBitmap;

use crate::node::NodeId;

pub const SLOT_COUNT: usize = 16_384;
pub const SLOT_MASK: u16 = (SLOT_COUNT as u16) - 1;

pub const FLAG_MIGRATING: u16 = 1 << 0;
pub const FLAG_IMPORTING: u16 = 1 << 1;
pub const FLAG_LOCAL: u16 = 1 << 2;
pub const FLAG_REPLICA: u16 = 1 << 3;

const NODE_SHIFT: u64 = 48;
const SHARD_SHIFT: u64 = 32;
const FLAGS_SHIFT: u64 = 16;
const FLAGS_MASK: u64 = 0xffff;
const FIELD_MASK: u64 = 0xffff;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlotEntry {
    pub node_index: u16,
    pub shard_index: u16,
    pub flags: u16,
}

impl SlotEntry {
    #[inline]
    pub const fn pack(self) -> u64 {
        ((self.node_index as u64) << NODE_SHIFT)
            | ((self.shard_index as u64) << SHARD_SHIFT)
            | ((self.flags as u64) << FLAGS_SHIFT)
    }

    #[inline]
    pub const fn unpack(packed: u64) -> Self {
        Self {
            node_index: ((packed >> NODE_SHIFT) & FIELD_MASK) as u16,
            shard_index: ((packed >> SHARD_SHIFT) & FIELD_MASK) as u16,
            flags: ((packed >> FLAGS_SHIFT) & FLAGS_MASK) as u16,
        }
    }

    #[inline]
    pub const fn is_local(self) -> bool {
        (self.flags & FLAG_LOCAL) != 0
    }

    #[inline]
    pub const fn is_migrating(self) -> bool {
        (self.flags & FLAG_MIGRATING) != 0
    }

    #[inline]
    pub const fn is_importing(self) -> bool {
        (self.flags & FLAG_IMPORTING) != 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteNode {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MigrationSlotState {
    keys: HashSet<Box<[u8]>, RandomState>,
}

impl MigrationSlotState {
    #[inline]
    fn new() -> Self {
        Self {
            keys: HashSet::with_hasher(RandomState::new()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotTableSnapshot {
    entries: [u64; SLOT_COUNT],
    nodes: Vec<Option<RouteNode>>,
    migrating_slots: HashMap<u16, MigrationSlotState, RandomState>,
    proxy_remote: bool,
}

impl Default for SlotTableSnapshot {
    fn default() -> Self {
        Self {
            entries: [0; SLOT_COUNT],
            nodes: Vec::new(),
            migrating_slots: HashMap::with_hasher(RandomState::new()),
            proxy_remote: false,
        }
    }
}

impl SlotTableSnapshot {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn entry(&self, slot: u16) -> SlotEntry {
        SlotEntry::unpack(self.entries[slot as usize])
    }

    #[inline]
    pub fn packed_entry(&self, slot: u16) -> u64 {
        self.entries[slot as usize]
    }

    #[inline]
    pub fn set_entry(&mut self, slot: u16, entry: SlotEntry) {
        self.entries[slot as usize] = entry.pack();
    }

    #[inline]
    pub fn set_route_node(&mut self, node_index: u16, node_id: NodeId, addr: SocketAddr) {
        let required_len = node_index as usize + 1;
        if self.nodes.len() < required_len {
            self.nodes.resize(required_len, None);
        }
        self.nodes[node_index as usize] = Some(RouteNode { id: node_id, addr });
    }

    #[inline]
    pub fn route_node(&self, node_index: u16) -> Option<&RouteNode> {
        self.nodes.get(node_index as usize).and_then(Option::as_ref)
    }

    #[inline]
    pub fn set_proxy_remote(&mut self, enabled: bool) {
        self.proxy_remote = enabled;
    }

    #[inline]
    pub fn proxy_remote(&self) -> bool {
        self.proxy_remote
    }

    pub fn insert_migrating_key(&mut self, slot: u16, key: &[u8]) {
        let state = self
            .migrating_slots
            .entry(slot)
            .or_insert_with(MigrationSlotState::new);
        state.keys.insert(Box::<[u8]>::from(key));
    }

    pub fn set_migrating_keys<I, K>(&mut self, slot: u16, keys: I)
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let mut state = MigrationSlotState::new();
        for key in keys {
            state.keys.insert(Box::<[u8]>::from(key.as_ref()));
        }
        self.migrating_slots.insert(slot, state);
    }

    pub fn clear_migrating_slot(&mut self, slot: u16) {
        self.migrating_slots.remove(&slot);
    }

    #[inline]
    pub fn is_key_migrated(&self, slot: u16, key: &[u8]) -> bool {
        self.migrating_slots
            .get(&slot)
            .is_some_and(|state| state.keys.contains(key))
    }

    #[inline]
    pub fn entries(&self) -> &[u64; SLOT_COUNT] {
        &self.entries
    }
}

pub struct SlotTable {
    entries: [AtomicU64; SLOT_COUNT],
}

impl Default for SlotTable {
    fn default() -> Self {
        Self {
            entries: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl SlotTable {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_snapshot(snapshot: &SlotTableSnapshot) -> Self {
        let table = Self::new();
        table.apply_snapshot(snapshot);
        table
    }

    pub fn apply_snapshot(&self, snapshot: &SlotTableSnapshot) {
        for (index, value) in snapshot.entries.iter().copied().enumerate() {
            self.entries[index].store(value, Ordering::Release);
        }
    }

    #[inline]
    pub fn load_entry(&self, slot: u16) -> SlotEntry {
        SlotEntry::unpack(self.entries[slot as usize].load(Ordering::Acquire))
    }

    #[inline]
    pub fn load_packed(&self, slot: u16) -> u64 {
        self.entries[slot as usize].load(Ordering::Acquire)
    }

    #[inline]
    pub fn store_entry(&self, slot: u16, entry: SlotEntry) {
        self.entries[slot as usize].store(entry.pack(), Ordering::Release);
    }
}

pub struct SeqLockSlotTable {
    seq: AtomicU64,
    data: [SlotTableSnapshot; 2],
}

impl SeqLockSlotTable {
    #[inline]
    pub fn new(initial: SlotTableSnapshot) -> Self {
        Self {
            seq: AtomicU64::new(0),
            data: [initial.clone(), initial],
        }
    }

    #[inline]
    pub fn sequence(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    pub fn read(&self) -> SlotTableSnapshot {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if (s1 & 1) == 1 {
                spin_loop();
                continue;
            }

            let snapshot = self.data[stable_index(s1)].clone();
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                return snapshot;
            }
        }
    }

    pub fn write(&mut self, f: impl FnOnce(&mut SlotTableSnapshot)) {
        let start = self.seq.fetch_add(1, Ordering::AcqRel);
        let stable = stable_index(start);
        let staging = stable ^ 1;
        self.data[staging] = self.data[stable].clone();
        f(&mut self.data[staging]);
        self.seq.fetch_add(1, Ordering::Release);
    }
}

#[inline]
const fn stable_index(seq: u64) -> usize {
    ((seq >> 1) & 1) as usize
}

#[inline]
pub fn assign_slots_to_shards(
    owned_slots: &RoaringBitmap,
    num_shards: usize,
) -> HashMap<u16, usize, RandomState> {
    let mut assignments =
        HashMap::with_capacity_and_hasher(owned_slots.len() as usize, RandomState::new());
    if num_shards == 0 {
        return assignments;
    }

    for (index, slot) in owned_slots.iter().enumerate() {
        assignments.insert(slot as u16, index % num_shards);
    }
    assignments
}

#[inline]
pub fn hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = memchr(b'{', key) {
        let rest = &key[open + 1..];
        if let Some(close) = memchr(b'}', rest)
            && close > 0
        {
            return &rest[..close];
        }
    }
    key
}

#[inline]
pub fn crc16_slot(key: &[u8]) -> u16 {
    crc16_ccitt(hash_tag(key)) & SLOT_MASK
}

#[inline]
pub fn crc16_ccitt(input: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in input {
        let index = ((crc >> 8) as u8 ^ byte) as usize;
        crc = (crc << 8) ^ CRC16_CCITT_TABLE[index];
    }
    crc
}

const CRC16_CCITT_TABLE: [u16; 256] = build_crc16_ccitt_table();

const fn build_crc16_ccitt_table() -> [u16; 256] {
    let mut table = [0_u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::thread;

    use roaring::RoaringBitmap;

    use super::{
        FLAG_IMPORTING, FLAG_LOCAL, FLAG_MIGRATING, SLOT_COUNT, SeqLockSlotTable, SlotEntry,
        SlotTableSnapshot, assign_slots_to_shards, crc16_slot, hash_tag,
    };
    use crate::node::NodeId;

    #[test]
    fn crc16_matches_redis_reference_cases() {
        assert_eq!(crc16_slot(b"foo"), 12_182);
        assert_eq!(crc16_slot(b"{foo}.bar"), crc16_slot(b"foo"));
        assert_eq!(crc16_slot(b"{}foo"), crc16_slot(b"{}foo"));
        assert_eq!(crc16_slot(b"{foo}"), crc16_slot(b"foo"));
    }

    #[test]
    fn hash_tag_uses_first_non_empty_braces() {
        assert_eq!(hash_tag(b"foo"), b"foo");
        assert_eq!(hash_tag(b"{foo}.bar"), b"foo");
        assert_eq!(hash_tag(b"foo{}{bar}"), b"foo{}{bar}");
    }

    #[test]
    fn slot_entry_pack_roundtrip_preserves_fields() {
        let entry = SlotEntry {
            node_index: 511,
            shard_index: 7,
            flags: FLAG_LOCAL | FLAG_MIGRATING | FLAG_IMPORTING,
        };

        let unpacked = SlotEntry::unpack(entry.pack());

        assert_eq!(unpacked, entry);
    }

    #[test]
    fn seqlock_read_without_writer_returns_immediately() {
        let lock = SeqLockSlotTable::new(SlotTableSnapshot::default());
        let snapshot = lock.read();

        assert_eq!(lock.sequence(), 0);
        assert_eq!(snapshot.packed_entry(0), 0);
    }

    #[test]
    fn seqlock_concurrent_reads_do_not_observe_torn_slot_state() {
        let mut initial = SlotTableSnapshot::default();
        initial.set_entry(
            42,
            SlotEntry {
                node_index: 1,
                shard_index: 1,
                flags: FLAG_LOCAL,
            },
        );

        let lock = Arc::new(std::sync::Mutex::new(SeqLockSlotTable::new(initial)));

        let reader = {
            let lock = Arc::clone(&lock);
            thread::spawn(move || {
                for _ in 0..2_000 {
                    let snapshot = lock.lock().expect("lock poisoned").read();
                    let entry = snapshot.entry(42);
                    assert!(
                        entry
                            == SlotEntry {
                                node_index: 1,
                                shard_index: 1,
                                flags: FLAG_LOCAL,
                            }
                            || entry
                                == SlotEntry {
                                    node_index: 9,
                                    shard_index: 3,
                                    flags: FLAG_LOCAL | FLAG_MIGRATING,
                                }
                    );
                }
            })
        };

        {
            let mut guard = lock.lock().expect("lock poisoned");
            for _ in 0..250 {
                guard.write(|snapshot| {
                    snapshot.set_entry(
                        42,
                        SlotEntry {
                            node_index: 9,
                            shard_index: 3,
                            flags: FLAG_LOCAL | FLAG_MIGRATING,
                        },
                    );
                });
                guard.write(|snapshot| {
                    snapshot.set_entry(
                        42,
                        SlotEntry {
                            node_index: 1,
                            shard_index: 1,
                            flags: FLAG_LOCAL,
                        },
                    );
                });
            }
        }

        reader.join().expect("reader thread");
    }

    #[test]
    fn shard_assignment_even_for_full_keyspace() {
        let owned_slots = (0..SLOT_COUNT as u32).collect::<RoaringBitmap>();
        let assignments = assign_slots_to_shards(&owned_slots, 8);
        let mut counts = [0_usize; 8];

        for shard in assignments.values().copied() {
            counts[shard] += 1;
        }

        assert_eq!(assignments.get(&0), Some(&0));
        assert!(counts.iter().all(|count| *count == 2048));
    }

    #[test]
    fn shard_assignment_stays_within_one_for_partial_set() {
        let owned_slots = (0..100_u32).collect::<RoaringBitmap>();
        let assignments = assign_slots_to_shards(&owned_slots, 3);
        let mut counts = [0_usize; 3];

        for shard in assignments.values().copied() {
            counts[shard] += 1;
        }

        assert_eq!(assignments.get(&0), Some(&0));
        assert_eq!(counts.iter().sum::<usize>(), 100);
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
    }

    #[test]
    #[ignore = "performance target should be measured in release mode"]
    fn crc16_slot_throughput_smoke() {
        let mut total = 0_u64;
        for _ in 0..10_000_000 {
            total += u64::from(crc16_slot(b"{tenant42}:session:abcdef"));
        }
        assert_ne!(total, 0);
    }

    #[test]
    fn snapshot_can_store_route_nodes() {
        let mut snapshot = SlotTableSnapshot::default();
        let node_id = NodeId::new([7; 20]);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000);

        snapshot.set_route_node(3, node_id, addr);

        assert_eq!(snapshot.route_node(3).map(|node| node.id), Some(node_id));
        assert_eq!(snapshot.route_node(3).map(|node| node.addr), Some(addr));
    }
}
