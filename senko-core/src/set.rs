use std::{borrow::Cow, str};

use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::HashSet;
use memchr::memmem;
use rand::{Rng, rngs::SmallRng, seq::SliceRandom};

use crate::list::{
    ListpackIter, ListpackNode, lp_delete_at, lp_get, lp_iter, lp_len, lp_push_back,
};

const INTSET_MAX_ENTRIES: usize = 512;
const LISTPACK_MAX_ENTRIES: usize = 128;
const LISTPACK_MAX_VALUE_SIZE: usize = 64;
const SIMD_LINEAR_SEARCH_MAX: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IntSetEncoding {
    Int16,
    Int32,
    Int64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntSet {
    pub data: Vec<i64>,
    pub encoding: IntSetEncoding,
}

impl Default for IntSet {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            encoding: IntSetEncoding::Int16,
        }
    }
}

impl IntSet {
    pub fn insert(&mut self, val: i64) -> bool {
        match self.data.binary_search(&val) {
            Ok(_) => false,
            Err(index) => {
                self.data.insert(index, val);
                self.encoding = self.encoding.max(encoding_for(val));
                true
            }
        }
    }

    pub fn remove(&mut self, val: i64) -> bool {
        match self.data.binary_search(&val) {
            Ok(index) => {
                self.data.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    pub fn contains(&self, val: i64) -> bool {
        if self.data.len() <= SIMD_LINEAR_SEARCH_MAX {
            return simd_or_scalar_search(val, &self.data);
        }
        self.data.binary_search(&val).is_ok()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, i64> {
        self.data.iter()
    }

    pub fn random_index(&self, rng: &mut SmallRng) -> i64 {
        let index = rng.gen_range(0..self.data.len());
        self.data[index]
    }

    pub fn upgrade_needed(val: i64, current: IntSetEncoding) -> bool {
        encoding_for(val) > current
    }
}

#[derive(Debug)]
pub enum SetEncoding {
    Intset(IntSet),
    Listpack(ListpackNode),
    Hashtable(HashSet<CompactString, RandomState>),
}

impl Clone for SetEncoding {
    fn clone(&self) -> Self {
        match self {
            Self::Intset(intset) => Self::Intset(intset.clone()),
            Self::Listpack(node) => {
                let mut cloned = ListpackNode::default();
                for member in lp_iter(node) {
                    lp_push_back(&mut cloned, member);
                }
                Self::Listpack(cloned)
            }
            Self::Hashtable(table) => Self::Hashtable(table.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetObject {
    pub inner: SetEncoding,
    pub len: u32,
    hasher: RandomState,
}

impl Default for SetObject {
    fn default() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl PartialEq for SetObject {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().all(|member| other.contains(member.as_ref()))
    }
}

pub enum SetIter<'a> {
    Intset(std::slice::Iter<'a, i64>),
    Listpack(ListpackIter<'a>),
    Hashtable(hashbrown::hash_set::Iter<'a, CompactString>),
}

impl<'a> Iterator for SetIter<'a> {
    type Item = Cow<'a, [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Intset(iter) => iter
                .next()
                .map(|value| Cow::Owned(value.to_string().into_bytes())),
            Self::Listpack(iter) => iter.next().map(Cow::Borrowed),
            Self::Hashtable(iter) => iter.next().map(|value| Cow::Borrowed(value.as_bytes())),
        }
    }
}

impl SetObject {
    pub fn with_hasher(hasher: RandomState) -> Self {
        Self {
            inner: SetEncoding::Intset(IntSet::default()),
            len: 0,
            hasher,
        }
    }

    pub fn is_intset(&self) -> bool {
        matches!(self.inner, SetEncoding::Intset(_))
    }

    pub fn is_listpack(&self) -> bool {
        matches!(self.inner, SetEncoding::Listpack(_))
    }

    pub fn is_hashtable(&self) -> bool {
        matches!(self.inner, SetEncoding::Hashtable(_))
    }

    pub fn add(&mut self, member: &[u8]) -> bool {
        let int_value = parse_integer(member);
        self.ensure_encoding_for_insert(member, int_value);

        let inserted = match &mut self.inner {
            SetEncoding::Intset(intset) => int_value.is_some_and(|value| intset.insert(value)),
            SetEncoding::Listpack(node) => lp_set_insert(node, member),
            SetEncoding::Hashtable(table) => table.insert(compact_from_bytes(member)),
        };

        if inserted {
            self.len = self.len.saturating_add(1);
        }
        inserted
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        let removed = match &mut self.inner {
            SetEncoding::Intset(intset) => {
                parse_integer(member).is_some_and(|value| intset.remove(value))
            }
            SetEncoding::Listpack(node) => lp_set_remove(node, member),
            SetEncoding::Hashtable(table) => table.remove(String::from_utf8_lossy(member).as_ref()),
        };

        if removed {
            self.len = self.len.saturating_sub(1);
        }
        removed
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.inner {
            SetEncoding::Intset(intset) => {
                parse_integer(member).is_some_and(|value| intset.contains(value))
            }
            SetEncoding::Listpack(node) => lp_set_contains(node, member),
            SetEncoding::Hashtable(table) => {
                table.contains(String::from_utf8_lossy(member).as_ref())
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> SetIter<'_> {
        match &self.inner {
            SetEncoding::Intset(intset) => SetIter::Intset(intset.iter()),
            SetEncoding::Listpack(node) => SetIter::Listpack(lp_iter(node)),
            SetEncoding::Hashtable(table) => SetIter::Hashtable(table.iter()),
        }
    }

    pub fn pop_random(&mut self, rng: &mut SmallRng) -> Option<Vec<u8>> {
        let value = match &mut self.inner {
            SetEncoding::Intset(intset) => {
                if intset.data.is_empty() {
                    return None;
                }
                let index = rng.gen_range(0..intset.data.len());
                intset.data.remove(index).to_string().into_bytes()
            }
            SetEncoding::Listpack(node) => {
                let len = lp_len(node);
                if len == 0 {
                    return None;
                }
                let index = rng.gen_range(0..len);
                let value = lp_get(node, index)?.to_vec();
                lp_delete_at(node, index);
                value
            }
            SetEncoding::Hashtable(table) => {
                if table.is_empty() {
                    return None;
                }
                let index = rng.gen_range(0..table.len());
                let value = table.iter().nth(index)?.clone();
                table.take(value.as_str())?.to_string().into_bytes()
            }
        };

        self.len = self.len.saturating_sub(1);
        Some(value)
    }

    pub fn sample_random(&self, rng: &mut SmallRng) -> Option<Cow<'_, [u8]>> {
        match &self.inner {
            SetEncoding::Intset(intset) => (!intset.data.is_empty())
                .then(|| Cow::Owned(intset.random_index(rng).to_string().into_bytes())),
            SetEncoding::Listpack(node) => {
                let len = lp_len(node);
                if len == 0 {
                    return None;
                }
                let index = rng.gen_range(0..len);
                lp_get(node, index).map(Cow::Borrowed)
            }
            SetEncoding::Hashtable(table) => {
                if table.is_empty() {
                    return None;
                }
                let index = rng.gen_range(0..table.len());
                table
                    .iter()
                    .nth(index)
                    .map(|value| Cow::Borrowed(value.as_bytes()))
            }
        }
    }

    pub fn sample_n_distinct(&self, n: usize, rng: &mut SmallRng) -> Vec<Vec<u8>> {
        if n == 0 || self.is_empty() {
            return Vec::new();
        }
        let mut members: Vec<Vec<u8>> = self.iter().map(|member| member.into_owned()).collect();
        members.shuffle(rng);
        members.truncate(n.min(members.len()));
        members
    }

    pub fn sample_n_repeating(&self, n: usize, rng: &mut SmallRng) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let Some(sample) = self.sample_random(rng) else {
                break;
            };
            out.push(sample.into_owned());
        }
        out
    }

    fn ensure_encoding_for_insert(&mut self, member: &[u8], int_value: Option<i64>) {
        match &self.inner {
            SetEncoding::Intset(intset) => {
                let would_insert = int_value.map_or(true, |value| !intset.contains(value));
                if !would_insert {
                    return;
                }
                let new_len = intset.len() + 1;
                let member_too_large = member.len() > LISTPACK_MAX_VALUE_SIZE;
                let needs_direct_hashtable = (int_value.is_none()
                    && (new_len > LISTPACK_MAX_ENTRIES || member_too_large))
                    || new_len > INTSET_MAX_ENTRIES;
                if needs_direct_hashtable {
                    self.upgrade_intset_to_hashtable();
                } else if int_value.is_none() {
                    self.upgrade_intset_to_listpack();
                }
            }
            SetEncoding::Listpack(node) => {
                if lp_set_contains(node, member) {
                    return;
                }
                let new_len = lp_len(node) + 1;
                if new_len > LISTPACK_MAX_ENTRIES || member.len() > LISTPACK_MAX_VALUE_SIZE {
                    self.upgrade_listpack_to_hashtable();
                }
            }
            SetEncoding::Hashtable(_) => {}
        }
    }

    fn upgrade_intset_to_listpack(&mut self) {
        let SetEncoding::Intset(intset) = &self.inner else {
            return;
        };
        let mut node = ListpackNode::default();
        for value in &intset.data {
            let encoded = value.to_string();
            lp_push_back(&mut node, encoded.as_bytes());
        }
        self.inner = SetEncoding::Listpack(node);
    }

    fn upgrade_intset_to_hashtable(&mut self) {
        let SetEncoding::Intset(intset) = &self.inner else {
            return;
        };
        let mut table = HashSet::with_capacity_and_hasher(intset.len(), self.hasher.clone());
        for value in &intset.data {
            let encoded = value.to_string();
            table.insert(CompactString::from(encoded.as_str()));
        }
        self.inner = SetEncoding::Hashtable(table);
    }

    fn upgrade_listpack_to_hashtable(&mut self) {
        let SetEncoding::Listpack(node) = &self.inner else {
            return;
        };
        let mut table = HashSet::with_capacity_and_hasher(lp_len(node), self.hasher.clone());
        for member in lp_iter(node) {
            table.insert(compact_from_bytes(member));
        }
        self.inner = SetEncoding::Hashtable(table);
    }
}

pub fn lp_set_contains(node: &ListpackNode, member: &[u8]) -> bool {
    if member.is_empty() {
        return lp_iter(node).any(|value| value.is_empty());
    }

    let mut cursor = 0usize;
    while cursor < node.data.len() {
        let Some((payload, next_cursor)) = decode_entry_at(&node.data, cursor) else {
            return false;
        };
        if memmem::find(payload, member) == Some(0) && payload.len() == member.len() {
            return true;
        }
        cursor = next_cursor;
    }
    false
}

pub fn lp_set_insert(node: &mut ListpackNode, member: &[u8]) -> bool {
    if lp_set_contains(node, member) {
        return false;
    }
    lp_push_back(node, member);
    true
}

pub fn lp_set_remove(node: &mut ListpackNode, member: &[u8]) -> bool {
    let Some(index) = lp_iter(node).position(|value| value == member) else {
        return false;
    };
    lp_delete_at(node, index);
    true
}

fn compact_from_bytes(member: &[u8]) -> CompactString {
    CompactString::from(String::from_utf8_lossy(member).as_ref())
}

fn parse_integer(member: &[u8]) -> Option<i64> {
    let text = str::from_utf8(member).ok()?;
    text.parse::<i64>().ok()
}

fn encoding_for(value: i64) -> IntSetEncoding {
    if i16::try_from(value).is_ok() {
        IntSetEncoding::Int16
    } else if i32::try_from(value).is_ok() {
        IntSetEncoding::Int32
    } else {
        IntSetEncoding::Int64
    }
}

fn simd_or_scalar_search(needle: i64, data: &[i64]) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe {
                return avx2_search(needle, data);
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return neon_search(needle, data);
    }

    data.binary_search(&needle).is_ok()
}

#[cfg(target_arch = "x86_64")]
unsafe fn avx2_search(needle: i64, data: &[i64]) -> bool {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi64, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi64x,
    };

    let mut index = 0usize;
    let needle_vec = unsafe { _mm256_set1_epi64x(needle) };
    while index + 4 <= data.len() {
        let chunk = unsafe { _mm256_loadu_si256(data.as_ptr().add(index) as *const __m256i) };
        let cmp = unsafe { _mm256_cmpeq_epi64(chunk, needle_vec) };
        if unsafe { _mm256_movemask_epi8(cmp) } != 0 {
            return true;
        }
        index += 4;
    }
    data[index..].contains(&needle)
}

#[cfg(target_arch = "aarch64")]
fn neon_search(needle: i64, data: &[i64]) -> bool {
    use std::arch::aarch64::{
        vceqq_s64, vdupq_n_s64, vld1q_s64, vmaxvq_u64, vreinterpretq_u64_s64,
    };

    let mut index = 0usize;
    let needle_vec = unsafe { vdupq_n_s64(needle) };
    while index + 2 <= data.len() {
        let chunk = unsafe { vld1q_s64(data.as_ptr().add(index)) };
        let cmp = unsafe { vceqq_s64(chunk, needle_vec) };
        if unsafe { vmaxvq_u64(vreinterpretq_u64_s64(cmp)) } != 0 {
            return true;
        }
        index += 2;
    }
    data[index..].contains(&needle)
}

fn decode_entry_at(data: &[u8], offset: usize) -> Option<(&[u8], usize)> {
    if offset >= data.len() {
        return None;
    }
    let (_, prevlen_len) = read_varint(data, offset)?;
    let mut cursor = offset.checked_add(prevlen_len)?;
    let (payload_len, header_len) = read_string_header(data.get(cursor..)?)?;
    cursor += header_len;
    let payload_end = cursor.checked_add(payload_len)?;
    let payload = data.get(cursor..payload_end)?;
    let backlen = *data.get(payload_end)? as usize;
    let next_offset = offset.checked_add(backlen)?;
    Some((payload, next_offset))
}

fn read_varint(data: &[u8], offset: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    let mut index = offset;
    loop {
        let byte = *data.get(index)?;
        value |= u64::from(byte & 0x7f) << shift;
        index += 1;
        if byte & 0x80 == 0 {
            return Some((value, index - offset));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

fn read_string_header(data: &[u8]) -> Option<(usize, usize)> {
    let first = *data.first()?;
    if first & 0xc0 == 0x80 {
        return Some(((first & 0x3f) as usize, 1));
    }
    match first {
        0xe0 => {
            let bytes = data.get(1..3)?;
            Some((u16::from_le_bytes([bytes[0], bytes[1]]) as usize, 3))
        }
        0xf0 => {
            let bytes = data.get(1..5)?;
            Some((
                u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize,
                5,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet as StdHashSet;

    use rand::{SeedableRng, rngs::SmallRng};

    use super::{IntSet, SetObject};

    #[test]
    fn intset_simd_matches_scalar_for_sizes_up_to_1024() {
        for size in 1..=1024 {
            let mut intset = IntSet::default();
            for value in 0..size as i64 {
                assert!(intset.insert(value * 2));
            }
            for needle in -2..=(size as i64 * 2 + 2) {
                assert_eq!(
                    intset.contains(needle),
                    intset.data.binary_search(&needle).is_ok()
                );
            }
        }
    }

    #[test]
    fn intset_upgrades_to_listpack_for_non_integer() {
        let mut set = SetObject::default();
        assert!(set.add(b"1"));
        assert!(set.add(b"two"));
        assert!(set.is_listpack());
    }

    #[test]
    fn intset_upgrades_directly_to_hashtable_for_large_non_integer_insert() {
        let mut set = SetObject::default();
        for i in 0..512 {
            assert!(set.add(i.to_string().as_bytes()));
        }
        assert!(set.add(b"x"));
        assert!(set.is_hashtable());
    }

    #[test]
    fn listpack_upgrades_to_hashtable_at_threshold() {
        let mut set = SetObject::default();
        assert!(set.add(b"a"));
        assert!(set.is_listpack());
        for i in 0..127 {
            assert!(set.add(format!("v{i}").as_bytes()));
        }
        assert!(set.is_listpack());
        assert!(set.add(b"overflow"));
        assert!(set.is_hashtable());
    }

    #[test]
    fn no_downgrade_after_remove() {
        let mut set = SetObject::default();
        for i in 0..129 {
            assert!(set.add(format!("v{i}").as_bytes()));
        }
        assert!(set.is_hashtable());
        for i in 0..100 {
            assert!(set.remove(format!("v{i}").as_bytes()));
        }
        assert!(set.is_hashtable());
    }

    #[test]
    fn uniqueness_is_enforced() {
        let mut set = SetObject::default();
        assert!(set.add(b"dup"));
        assert!(!set.add(b"dup"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn unified_iterator_yields_all_members() {
        let mut intset = SetObject::default();
        intset.add(b"1");
        intset.add(b"2");

        let mut listpack = SetObject::default();
        listpack.add(b"a");
        listpack.add(b"b");

        let mut hashtable = SetObject::default();
        for i in 0..129 {
            hashtable.add(format!("m{i}").as_bytes());
        }

        let int_members: StdHashSet<Vec<u8>> = intset.iter().map(|v| v.into_owned()).collect();
        let lp_members: StdHashSet<Vec<u8>> = listpack.iter().map(|v| v.into_owned()).collect();
        let ht_members: StdHashSet<Vec<u8>> = hashtable.iter().map(|v| v.into_owned()).collect();

        assert_eq!(
            int_members,
            StdHashSet::from([b"1".to_vec(), b"2".to_vec()])
        );
        assert_eq!(lp_members, StdHashSet::from([b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(ht_members.len(), 129);
    }

    #[test]
    fn pop_random_drains_set() {
        let mut rng = SmallRng::seed_from_u64(7);
        let mut set = SetObject::default();
        for i in 0..64 {
            assert!(set.add(format!("v{i}").as_bytes()));
        }
        for _ in 0..64 {
            assert!(set.pop_random(&mut rng).is_some());
        }
        assert!(set.is_empty());
    }

    #[test]
    fn sample_n_distinct_is_unique_and_capped() {
        let mut rng = SmallRng::seed_from_u64(11);
        let mut set = SetObject::default();
        for i in 0..32 {
            set.add(format!("v{i}").as_bytes());
        }
        let sampled = set.sample_n_distinct(40, &mut rng);
        let uniq: StdHashSet<Vec<u8>> = sampled.iter().cloned().collect();
        assert_eq!(uniq.len(), sampled.len());
        assert!(sampled.len() <= 32);
    }

    #[test]
    fn sample_n_repeating_allows_duplicates_and_keeps_len() {
        let mut rng = SmallRng::seed_from_u64(1);
        let mut set = SetObject::default();
        set.add(b"a");
        set.add(b"b");
        let sampled = set.sample_n_repeating(16, &mut rng);
        assert_eq!(sampled.len(), 16);
    }
}
