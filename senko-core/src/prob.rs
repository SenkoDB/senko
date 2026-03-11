use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    f64::consts::{E, LN_2},
};

use ahash::AHashMap;
use bytes::Bytes;
use smallvec::SmallVec;

const CHUNK_BYTES: usize = 8 * 1024;
const XXH_PRIME_1: u64 = 0x9E37_79B1_85EB_CA87;
const XXH_PRIME_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const XXH_PRIME_3: u64 = 0x1656_67B1_9E37_79F9;
const XXH_PRIME_4: u64 = 0x85EB_CA77_C2B2_AE63;
const XXH_PRIME_5: u64 = 0x27D4_EB2F_1656_67C5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitVec(pub Vec<u64>);

impl BitVec {
    pub fn with_bits(num_bits: u64) -> Self {
        Self(vec![0; num_bits.div_ceil(64) as usize])
    }

    #[inline(always)]
    pub fn set(&mut self, idx: usize) {
        self.0[idx >> 6] |= 1u64 << (idx & 63);
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> bool {
        ((self.0[idx >> 6] >> (idx & 63)) & 1) == 1
    }

    pub fn byte_len(&self) -> usize {
        self.0.len() * std::mem::size_of::<u64>()
    }

    pub fn chunk(&self, iter: usize) -> Option<Bytes> {
        let start = iter.checked_mul(CHUNK_BYTES)?;
        let len = self.byte_len();
        if start >= len {
            return None;
        }
        let end = (start + CHUNK_BYTES).min(len);
        let mut out = Vec::with_capacity(end - start);
        for &word in &self.0 {
            out.extend_from_slice(&word.to_le_bytes());
        }
        Some(Bytes::from(out[start..end].to_vec()))
    }

    pub fn load_chunk(&mut self, iter: usize, data: &[u8]) {
        let start = iter * CHUNK_BYTES;
        let required = start + data.len();
        let words = required.div_ceil(8);
        if self.0.len() < words {
            self.0.resize(words, 0);
        }
        for (offset, byte) in data.iter().copied().enumerate() {
            let absolute = start + offset;
            let word = absolute >> 3;
            let shift = (absolute & 7) * 8;
            self.0[word] &= !(0xffu64 << shift);
            self.0[word] |= (byte as u64) << shift;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoubleHasher {
    h1: u64,
    h2: u64,
}

impl DoubleHasher {
    pub fn new(data: &[u8]) -> Self {
        let hash = xxhash3_128(data);
        Self {
            h1: hash as u64,
            h2: (hash >> 64) as u64 | 1,
        }
    }

    #[inline(always)]
    pub fn get(&self, i: u64) -> u64 {
        self.h1.wrapping_add(i.wrapping_mul(self.h2))
    }

    #[inline(always)]
    pub fn index(&self, i: u64, n: u64) -> usize {
        ((self.get(i) as u128 * n as u128) >> 64) as usize
    }
}

#[inline(always)]
fn avalanche64(mut value: u64) -> u64 {
    value ^= value >> 37;
    value = value.wrapping_mul(0x1656_6791_9E37_79F9);
    value ^= value >> 32;
    value
}

fn read_u64(input: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes[..input.len()].copy_from_slice(input);
    u64::from_le_bytes(bytes)
}

fn read_u32(input: &[u8]) -> u32 {
    let mut bytes = [0u8; 4];
    bytes[..input.len()].copy_from_slice(input);
    u32::from_le_bytes(bytes)
}

pub fn xxhash3_128(data: &[u8]) -> u128 {
    let mut acc1 = XXH_PRIME_1.wrapping_add((data.len() as u64).wrapping_mul(XXH_PRIME_2));
    let mut acc2 = XXH_PRIME_2 ^ (data.len() as u64).wrapping_mul(XXH_PRIME_3);
    let mut chunks = data.chunks_exact(16);
    for chunk in &mut chunks {
        let lane1 = read_u64(&chunk[..8]);
        let lane2 = read_u64(&chunk[8..]);
        acc1 = avalanche64(acc1 ^ lane1.wrapping_mul(XXH_PRIME_2).rotate_left(31));
        acc2 = avalanche64(acc2 ^ lane2.wrapping_mul(XXH_PRIME_1).rotate_left(27));
        acc1 = acc1.wrapping_add(acc2 ^ XXH_PRIME_4);
        acc2 = acc2.wrapping_add(acc1 ^ XXH_PRIME_5);
    }
    let rem = chunks.remainder();
    if rem.len() >= 8 {
        acc1 ^= read_u64(&rem[..8]).wrapping_mul(XXH_PRIME_2);
        acc1 = avalanche64(acc1);
        acc2 ^= read_u32(&rem[8..]) as u64 * XXH_PRIME_1;
        acc2 = avalanche64(acc2);
    } else if rem.len() >= 4 {
        acc1 ^= read_u32(&rem[..4]) as u64 * XXH_PRIME_1;
        acc1 = avalanche64(acc1);
        acc2 ^= read_u32(&rem[4..]) as u64 * XXH_PRIME_2;
        acc2 = avalanche64(acc2);
    } else if !rem.is_empty() {
        let mut tail = 0u64;
        for (index, byte) in rem.iter().copied().enumerate() {
            tail |= (byte as u64) << (index * 8);
        }
        acc1 ^= tail.wrapping_mul(XXH_PRIME_5);
        acc1 = avalanche64(acc1);
        acc2 ^= tail.rotate_left(11).wrapping_mul(XXH_PRIME_3);
        acc2 = avalanche64(acc2);
    }
    let low = avalanche64(acc1.wrapping_add(acc2.rotate_left(13)));
    let high = avalanche64(
        acc2.wrapping_add(acc1.rotate_left(7))
            .wrapping_add(XXH_PRIME_3),
    );
    ((high as u128) << 64) | low as u128
}

#[inline(always)]
pub fn optimal_bits(capacity: u64, error_rate: f64) -> u64 {
    (-(capacity as f64) * error_rate.ln() / LN_2.powi(2)).ceil() as u64
}

#[inline(always)]
pub fn optimal_hashes(bits: u64, capacity: u64) -> u32 {
    ((bits as f64 / capacity as f64) * LN_2).round().max(1.0) as u32
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubFilter {
    pub bits: BitVec,
    pub num_bits: u64,
    pub num_hashes: u32,
    pub capacity: u64,
    pub items: u64,
}

impl SubFilter {
    pub fn new(capacity: u64, error_rate: f64) -> Self {
        let num_bits = optimal_bits(capacity.max(1), error_rate);
        let num_hashes = optimal_hashes(num_bits.max(1), capacity.max(1));
        Self {
            bits: BitVec::with_bits(num_bits.max(64)),
            num_bits: num_bits.max(64),
            num_hashes,
            capacity: capacity.max(1),
            items: 0,
        }
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        let hasher = DoubleHasher::new(item);
        (0..self.num_hashes as u64).all(|i| self.bits.get(hasher.index(i, self.num_bits)))
    }

    pub fn insert(&mut self, item: &[u8]) -> bool {
        let already = self.contains(item);
        let hasher = DoubleHasher::new(item);
        for i in 0..self.num_hashes as u64 {
            self.bits.set(hasher.index(i, self.num_bits));
        }
        self.items = self.items.saturating_add(1);
        !already
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    pub filters: Vec<SubFilter>,
    pub error_rate: f64,
    pub capacity: u64,
    pub expansion: u8,
    pub total_items: u64,
    pub non_scaling: bool,
}

impl BloomFilter {
    pub fn new(capacity: u64, error_rate: f64, expansion: u8, non_scaling: bool) -> Self {
        Self {
            filters: vec![SubFilter::new(capacity, error_rate)],
            error_rate,
            capacity: capacity.max(1),
            expansion: expansion.max(1),
            total_items: 0,
            non_scaling,
        }
    }

    fn ensure_capacity(&mut self) -> bool {
        let Some(last) = self.filters.last() else {
            return false;
        };
        if last.items < (last.capacity as f64 * 0.95) as u64 {
            return true;
        }
        if self.non_scaling {
            return false;
        }
        let layer = self.filters.len() as i32;
        let capacity = last.capacity.saturating_mul(self.expansion as u64).max(1);
        let error = self.error_rate * 0.5f64.powi(layer);
        self.filters
            .push(SubFilter::new(capacity, error.max(f64::EPSILON)));
        true
    }

    #[allow(clippy::result_unit_err)]
    pub fn add(&mut self, item: &[u8]) -> Result<bool, ()> {
        if !self.ensure_capacity() {
            return Err(());
        }
        let inserted = self.filters.last_mut().expect("subfilter").insert(item);
        self.total_items = self.total_items.saturating_add(1);
        Ok(inserted)
    }

    pub fn exists(&self, item: &[u8]) -> bool {
        self.filters
            .iter()
            .rev()
            .any(|filter| filter.contains(item))
    }

    pub fn scandump(&self, iter: usize) -> Option<(usize, Bytes)> {
        let mut remaining = iter * CHUNK_BYTES;
        for filter in &self.filters {
            let filter_bytes = filter.bits.byte_len();
            if remaining < filter_bytes {
                let start_iter = remaining / CHUNK_BYTES;
                if let Some(chunk) = filter.bits.chunk(start_iter) {
                    let next = iter + 1;
                    return Some((next, chunk));
                }
            }
            remaining = remaining.saturating_sub(filter_bytes);
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub slots: SmallVec<[u16; 4]>,
}

impl Bucket {
    fn new(size: usize) -> Self {
        let mut slots = SmallVec::new();
        slots.resize(size, 0);
        Self { slots }
    }

    fn contains(&self, fp: u16) -> bool {
        self.slots.iter().copied().any(|value| value == fp)
    }

    fn insert(&mut self, fp: u16) -> bool {
        if let Some(slot) = self.slots.iter_mut().find(|slot| **slot == 0) {
            *slot = fp;
            true
        } else {
            false
        }
    }

    fn remove_one(&mut self, fp: u16) -> bool {
        if let Some(slot) = self.slots.iter_mut().find(|slot| **slot == fp) {
            *slot = 0;
            true
        } else {
            false
        }
    }

    fn count(&self, fp: u16) -> u64 {
        self.slots.iter().filter(|slot| **slot == fp).count() as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CuckooLayer {
    pub buckets: Vec<Bucket>,
    pub num_buckets: usize,
    pub bucket_size: usize,
    pub fingerprint_bits: usize,
    pub max_iterations: usize,
}

impl CuckooLayer {
    pub fn new(
        capacity: usize,
        bucket_size: usize,
        fingerprint_bits: usize,
        max_iterations: usize,
    ) -> Self {
        let num_buckets = capacity.next_power_of_two().max(2).div_ceil(bucket_size);
        Self {
            buckets: (0..num_buckets).map(|_| Bucket::new(bucket_size)).collect(),
            num_buckets,
            bucket_size,
            fingerprint_bits,
            max_iterations,
        }
    }

    fn fingerprint(&self, item: &[u8]) -> u16 {
        let width = self.fingerprint_bits.min(16);
        let hash = xxhash3_128(item) as u64;
        let mask = if width == 16 {
            u16::MAX
        } else {
            (1u16 << width) - 1
        };
        let fp = ((hash >> (64 - width)) as u16) & mask;
        fp.max(1)
    }

    fn positions(&self, item: &[u8], fp: u16) -> (usize, usize) {
        let h = DoubleHasher::new(item);
        let i1 = h.index(0, self.num_buckets as u64);
        let i2 = (i1 ^ (fp as usize).wrapping_mul(0x5bd1e995)) % self.num_buckets;
        (i1, i2)
    }

    fn alternate(&self, index: usize, fp: u16) -> usize {
        (index ^ (fp as usize).wrapping_mul(0x5bd1e995)) % self.num_buckets
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let (i1, i2) = self.positions(item, fp);
        self.buckets[i1].contains(fp) || self.buckets[i2].contains(fp)
    }

    #[allow(clippy::result_unit_err)]
    pub fn insert(&mut self, item: &[u8]) -> Result<(), ()> {
        let fp = self.fingerprint(item);
        let (i1, i2) = self.positions(item, fp);
        if self.buckets[i1].insert(fp) || self.buckets[i2].insert(fp) {
            return Ok(());
        }
        let mut idx = if fastrand::bool() { i1 } else { i2 };
        let mut victim = fp;
        for _ in 0..self.max_iterations {
            let slot = fastrand::usize(..self.bucket_size);
            std::mem::swap(&mut self.buckets[idx].slots[slot], &mut victim);
            idx = self.alternate(idx, victim);
            if self.buckets[idx].insert(victim) {
                return Ok(());
            }
        }
        Err(())
    }

    pub fn delete(&mut self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let (i1, i2) = self.positions(item, fp);
        self.buckets[i1].remove_one(fp) || self.buckets[i2].remove_one(fp)
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        let fp = self.fingerprint(item);
        let (i1, i2) = self.positions(item, fp);
        self.buckets[i1].count(fp) + self.buckets[i2].count(fp)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CuckooFilter {
    pub layers: Vec<CuckooLayer>,
    pub bucket_size: usize,
    pub fingerprint_bits: usize,
    pub max_iterations: usize,
    pub num_items: u64,
    pub num_deletes: u64,
    pub expansion: usize,
}

impl CuckooFilter {
    pub fn new(
        capacity: usize,
        bucket_size: usize,
        fingerprint_bits: usize,
        max_iterations: usize,
        expansion: usize,
    ) -> Self {
        Self {
            layers: vec![CuckooLayer::new(
                capacity,
                bucket_size.max(1),
                fingerprint_bits.max(1),
                max_iterations.max(1),
            )],
            bucket_size: bucket_size.max(1),
            fingerprint_bits: fingerprint_bits.max(1),
            max_iterations: max_iterations.max(1),
            num_items: 0,
            num_deletes: 0,
            expansion,
        }
    }

    pub fn exists(&self, item: &[u8]) -> bool {
        self.layers.iter().rev().any(|layer| layer.contains(item))
    }

    #[allow(clippy::result_unit_err)]
    pub fn add(&mut self, item: &[u8]) -> Result<(), ()> {
        match self.layers.last_mut().expect("layer").insert(item) {
            Ok(()) => {
                self.num_items = self.num_items.saturating_add(1);
                Ok(())
            }
            Err(()) if self.expansion > 0 => {
                let new_capacity =
                    self.layers.last().expect("layer").num_buckets * self.bucket_size * 2;
                self.layers.push(CuckooLayer::new(
                    new_capacity,
                    self.bucket_size,
                    self.fingerprint_bits,
                    self.max_iterations,
                ));
                self.add(item)
            }
            Err(()) => Err(()),
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn add_nx(&mut self, item: &[u8]) -> Result<bool, ()> {
        if self.exists(item) {
            return Ok(false);
        }
        self.add(item)?;
        Ok(true)
    }

    pub fn delete(&mut self, item: &[u8]) -> bool {
        for layer in self.layers.iter_mut().rev() {
            if layer.delete(item) {
                self.num_deletes = self.num_deletes.saturating_add(1);
                return true;
            }
        }
        false
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        self.layers.iter().map(|layer| layer.count(item)).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CountMinSketch {
    pub counters: Vec<u64>,
    pub width: usize,
    pub depth: usize,
    pub total_count: u64,
}

impl CountMinSketch {
    pub fn new(width: usize, depth: usize) -> Self {
        Self {
            counters: vec![0; width.max(1) * depth.max(1)],
            width: width.max(1),
            depth: depth.max(1),
            total_count: 0,
        }
    }

    pub fn width_from_error(error: f64) -> usize {
        (E / error).ceil() as usize
    }

    pub fn depth_from_confidence(delta: f64) -> usize {
        (1.0 / delta).ln().ceil() as usize
    }

    pub fn incrby(&mut self, item: &[u8], increment: u64) -> u64 {
        let hasher = DoubleHasher::new(item);
        let mut current_min = u64::MAX;
        for i in 0..self.depth {
            let idx = i * self.width + hasher.index(i as u64, self.width as u64);
            current_min = current_min.min(self.counters[idx]);
        }
        let new_val = current_min.saturating_add(increment);
        for i in 0..self.depth {
            let idx = i * self.width + hasher.index(i as u64, self.width as u64);
            if self.counters[idx] <= current_min {
                self.counters[idx] = new_val;
            }
        }
        self.total_count = self.total_count.saturating_add(increment);
        new_val
    }

    pub fn query(&self, item: &[u8]) -> u64 {
        let hasher = DoubleHasher::new(item);
        let mut out = u64::MAX;
        for i in 0..self.depth {
            let idx = i * self.width + hasher.index(i as u64, self.width as u64);
            out = out.min(self.counters[idx]);
        }
        if out == u64::MAX { 0 } else { out }
    }

    pub fn merge_from(&mut self, other: &Self, weight: u64) -> bool {
        if self.width != other.width || self.depth != other.depth {
            return false;
        }
        for (dest, src) in self.counters.iter_mut().zip(other.counters.iter().copied()) {
            *dest = (*dest).max(src.saturating_mul(weight));
        }
        self.total_count = self
            .total_count
            .max(other.total_count.saturating_mul(weight));
        true
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HkCell {
    pub fingerprint: u32,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct TopKSketch {
    pub k: usize,
    pub width: usize,
    pub depth: usize,
    pub decay: f64,
    pub buckets: Vec<HkCell>,
    pub heap: BinaryHeap<Reverse<(u64, Bytes)>>,
    pub item_counts: AHashMap<Bytes, u64>,
    pub total_items: u64,
}

impl PartialEq for TopKSketch {
    fn eq(&self, other: &Self) -> bool {
        self.k == other.k
            && self.width == other.width
            && self.depth == other.depth
            && self.decay.to_bits() == other.decay.to_bits()
            && self.buckets == other.buckets
            && self.item_counts == other.item_counts
            && self.total_items == other.total_items
    }
}

impl TopKSketch {
    pub fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self {
            k: k.max(1),
            width: width.max(1),
            depth: depth.max(1),
            decay,
            buckets: vec![HkCell::default(); width.max(1) * depth.max(1)],
            heap: BinaryHeap::new(),
            item_counts: AHashMap::new(),
            total_items: 0,
        }
    }

    fn fingerprint32(item: &[u8]) -> u32 {
        (xxhash3_128(item) >> 96) as u32
    }

    fn rebuild_heap(&mut self) {
        let mut items = self
            .item_counts
            .iter()
            .map(|(item, count)| Reverse((*count, item.clone())))
            .collect::<Vec<_>>();
        items.sort_unstable();
        self.heap = items.into_iter().collect();
    }

    fn prune_stale_topk(&mut self) {
        if self.item_counts.len() <= self.k {
            self.rebuild_heap();
            return;
        }
        let mut sorted = self
            .item_counts
            .iter()
            .map(|(item, count)| (item.clone(), *count))
            .collect::<Vec<_>>();
        sorted.sort_unstable_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.0.as_ref().cmp(right.0.as_ref()))
        });
        self.item_counts.clear();
        for (item, count) in sorted.into_iter().take(self.k) {
            self.item_counts.insert(item, count);
        }
        self.rebuild_heap();
    }

    pub fn add(&mut self, item: &[u8], increment: u64) -> Option<Bytes> {
        let fp = Self::fingerprint32(item);
        let hasher = DoubleHasher::new(item);
        let mut max_count = 0u64;
        for i in 0..self.depth {
            let idx = i * self.width + hasher.index(i as u64, self.width as u64);
            let cell = &mut self.buckets[idx];
            if cell.count == 0 {
                cell.fingerprint = fp;
                cell.count = increment.min(u32::MAX as u64) as u32;
                max_count = max_count.max(cell.count as u64);
            } else if cell.fingerprint == fp {
                cell.count = cell
                    .count
                    .saturating_add(increment.min(u32::MAX as u64) as u32);
                max_count = max_count.max(cell.count as u64);
            } else if cell.count <= 63 {
                let p = self.decay.powi(cell.count as i32);
                if fastrand::f64() < p {
                    if cell.count <= increment as u32 {
                        cell.fingerprint = fp;
                        cell.count = 1;
                        max_count = max_count.max(1);
                    } else {
                        cell.count -= increment.min(cell.count as u64) as u32;
                    }
                }
            }
        }
        self.total_items = self.total_items.saturating_add(increment);
        self.update_heap(item, max_count)
    }

    pub fn update_heap(&mut self, item: &[u8], count: u64) -> Option<Bytes> {
        let item_bytes = Bytes::copy_from_slice(item);
        self.item_counts.insert(item_bytes.clone(), count);
        let expelled = if self.item_counts.len() > self.k {
            let mut sorted = self
                .item_counts
                .iter()
                .map(|(member, member_count)| (member.clone(), *member_count))
                .collect::<Vec<_>>();
            sorted.sort_unstable_by(|left, right| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.0.as_ref().cmp(right.0.as_ref()))
            });
            let expelled = sorted.first().map(|(member, _)| member.clone());
            if let Some(expelled) = expelled.clone() {
                self.item_counts.remove(&expelled);
            }
            expelled.filter(|value| value.as_ref() != item_bytes.as_ref())
        } else {
            None
        };
        self.prune_stale_topk();
        expelled
    }

    pub fn query(&self, item: &[u8]) -> bool {
        self.item_counts.contains_key(item)
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        self.item_counts.get(item).copied().unwrap_or(0)
    }

    pub fn list(&self) -> Vec<(Bytes, u64)> {
        let mut out = self
            .item_counts
            .iter()
            .map(|(item, count)| (item.clone(), *count))
            .collect::<Vec<_>>();
        out.sort_unstable_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| left.0.as_ref().cmp(right.0.as_ref()))
        });
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TDigest {
    pub centroids: Vec<Centroid>,
    pub compression: f64,
    pub total_weight: f64,
    pub min: f64,
    pub max: f64,
    pub unmerged: Vec<(f64, f64)>,
    pub unmerged_weight: f64,
    pub merges: u64,
    pub total_compressions: u64,
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        Self {
            centroids: Vec::new(),
            compression,
            total_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            unmerged: Vec::new(),
            unmerged_weight: 0.0,
            merges: 0,
            total_compressions: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Self::new(self.compression);
    }

    #[inline(always)]
    pub fn centroid_limit(q: f64, compression: f64) -> f64 {
        4.0 * q * (1.0 - q) * compression
    }

    pub fn add(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.unmerged.push((value, 1.0));
        self.unmerged_weight += 1.0;
        if self.unmerged.len() >= (self.compression as usize).saturating_mul(5).max(1) {
            self.compress();
        }
    }

    pub fn compress(&mut self) {
        if self.unmerged.is_empty() && self.centroids.is_empty() {
            return;
        }
        let mut all = self
            .centroids
            .drain(..)
            .chain(
                self.unmerged
                    .drain(..)
                    .map(|(mean, weight)| Centroid { mean, weight }),
            )
            .collect::<Vec<_>>();
        all.sort_unstable_by(|left, right| {
            left.mean
                .partial_cmp(&right.mean)
                .unwrap_or(Ordering::Equal)
        });
        let total = all.iter().map(|centroid| centroid.weight).sum::<f64>();
        self.total_weight = total;
        self.unmerged_weight = 0.0;
        let mut result: Vec<Centroid> = Vec::with_capacity(all.len());
        let mut cumulative = 0.0;
        for centroid in all {
            let q = if total > 0.0 {
                (cumulative + centroid.weight / 2.0) / total
            } else {
                0.0
            };
            let limit = Self::centroid_limit(q, self.compression).max(1.0);
            if let Some(last) = result.last_mut()
                && last.weight + centroid.weight <= limit
            {
                let new_weight = last.weight + centroid.weight;
                last.mean =
                    (last.mean * last.weight + centroid.mean * centroid.weight) / new_weight;
                last.weight = new_weight;
                cumulative += centroid.weight;
                continue;
            }
            cumulative += centroid.weight;
            result.push(centroid);
        }
        self.centroids = result;
        self.total_compressions = self.total_compressions.saturating_add(1);
    }

    fn ensure_compressed(&mut self) {
        if !self.unmerged.is_empty() {
            self.compress();
        }
    }

    pub fn mean(&mut self) -> Option<f64> {
        self.ensure_compressed();
        if self.total_weight == 0.0 {
            return None;
        }
        let weighted = self
            .centroids
            .iter()
            .map(|centroid| centroid.mean * centroid.weight)
            .sum::<f64>();
        Some(weighted / self.total_weight)
    }

    pub fn quantile(&mut self, q: f64) -> Option<f64> {
        self.ensure_compressed();
        if self.total_weight == 0.0 {
            return None;
        }
        if q <= 0.0 {
            return Some(self.min);
        }
        if q >= 1.0 {
            return Some(self.max);
        }
        let target = q * self.total_weight;
        let mut cumulative = 0.0;
        for (index, centroid) in self.centroids.iter().enumerate() {
            let prev = cumulative;
            cumulative += centroid.weight;
            if cumulative >= target {
                let delta = (target - prev - centroid.weight / 2.0) / centroid.weight.max(1.0);
                let lower = if index > 0 {
                    self.centroids[index - 1].mean
                } else {
                    self.min
                };
                let upper = if index + 1 < self.centroids.len() {
                    self.centroids[index + 1].mean
                } else {
                    self.max
                };
                return Some(centroid.mean + delta * (upper - lower) / 2.0);
            }
        }
        Some(self.max)
    }

    pub fn cdf(&mut self, value: f64) -> f64 {
        self.ensure_compressed();
        if self.total_weight == 0.0 {
            return 0.0;
        }
        if value <= self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }
        let mut cumulative = 0.0;
        for centroid in &self.centroids {
            if value < centroid.mean {
                break;
            }
            cumulative += centroid.weight;
        }
        (cumulative / self.total_weight).clamp(0.0, 1.0)
    }

    pub fn rank(&mut self, value: f64) -> u64 {
        (self.cdf(value) * self.total_weight) as u64
    }

    pub fn rev_rank(&mut self, value: f64) -> u64 {
        self.total_weight as u64 - self.rank(value)
    }

    pub fn by_rank(&mut self, rank: u64) -> Option<f64> {
        if self.total_weight == 0.0 {
            return None;
        }
        self.quantile((rank as f64 / self.total_weight).clamp(0.0, 1.0))
    }

    pub fn by_rev_rank(&mut self, rank: u64) -> Option<f64> {
        if self.total_weight == 0.0 {
            return None;
        }
        self.quantile((1.0 - rank as f64 / self.total_weight).clamp(0.0, 1.0))
    }

    pub fn trimmed_mean(&mut self, low: f64, high: f64) -> Option<f64> {
        self.ensure_compressed();
        if self.total_weight == 0.0 || low > high {
            return None;
        }
        let low_rank = low.clamp(0.0, 1.0) * self.total_weight;
        let high_rank = high.clamp(0.0, 1.0) * self.total_weight;
        let mut cumulative = 0.0;
        let mut total_weight = 0.0;
        let mut total_sum = 0.0;
        for centroid in &self.centroids {
            let start = cumulative;
            let end = cumulative + centroid.weight;
            cumulative = end;
            let overlap = (end.min(high_rank) - start.max(low_rank)).max(0.0);
            if overlap > 0.0 {
                total_weight += overlap;
                total_sum += centroid.mean * overlap;
            }
        }
        if total_weight == 0.0 {
            None
        } else {
            Some(total_sum / total_weight)
        }
    }

    pub fn merge_from(&mut self, other: &mut TDigest) {
        other.ensure_compressed();
        self.ensure_compressed();
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.centroids.extend(other.centroids.iter().cloned());
        self.total_weight += other.total_weight;
        self.merges = self.merges.saturating_add(1);
        self.compress();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProbMergeValue {
    CountMinSketch(Box<CountMinSketch>),
    TDigest(Box<TDigest>),
}

#[cfg(test)]
mod tests {
    use super::{BloomFilter, CountMinSketch, TDigest, TopKSketch};

    #[test]
    fn bloom_round_trip_has_no_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 0.01, 2, false);
        for i in 0..1000 {
            assert!(bloom.add(format!("item-{i}").as_bytes()).is_ok());
        }
        for i in 0..1000 {
            assert!(bloom.exists(format!("item-{i}").as_bytes()));
        }
    }

    #[test]
    fn cms_conservative_update_is_monotonic() {
        let mut cms = CountMinSketch::new(128, 5);
        let first = cms.incrby(b"hot", 10);
        let second = cms.incrby(b"hot", 5);
        assert!(second >= first);
        assert!(cms.query(b"hot") >= 15);
    }

    #[test]
    fn topk_tracks_heavy_items() {
        let mut sketch = TopKSketch::new(3, 16, 5, 0.9);
        for _ in 0..100 {
            let _ = sketch.add(b"alpha", 1);
        }
        for _ in 0..50 {
            let _ = sketch.add(b"beta", 1);
        }
        assert!(sketch.query(b"alpha"));
        assert!(sketch.count(b"alpha") >= sketch.count(b"beta"));
    }

    #[test]
    fn tdigest_quantiles_track_bounds() {
        let mut digest = TDigest::new(100.0);
        for i in 0..1000 {
            digest.add(i as f64 / 1000.0);
        }
        let p50 = digest.quantile(0.5).unwrap();
        assert!((p50 - 0.5).abs() < 0.05);
        assert_eq!(digest.quantile(0.0).unwrap(), digest.min);
        assert_eq!(digest.quantile(1.0).unwrap(), digest.max);
    }
}
