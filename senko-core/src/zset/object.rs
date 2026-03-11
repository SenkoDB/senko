use std::{collections::VecDeque, marker::PhantomData, str};

use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::HashMap;
use rand::{Rng, rngs::SmallRng, seq::SliceRandom};

use crate::{
    ListpackNode, lp_delete_at, lp_iter, lp_len, lp_push_back, senko_hasher,
    zset::bptree::{BPTree, LexBound, ScoreBound},
};

const LISTPACK_MAX_ENTRIES: usize = 128;
const LISTPACK_MAX_MEMBER_SIZE: usize = 64;

#[derive(Debug)]
pub enum ZSetEncoding {
    Listpack(ListpackNode),
    BPTree {
        tree: BPTree,
        member_index: HashMap<CompactString, f64, RandomState>,
    },
}

impl Clone for ZSetEncoding {
    fn clone(&self) -> Self {
        match self {
            Self::Listpack(node) => {
                let mut cloned = ListpackNode::default();
                for entry in lp_iter(node) {
                    lp_push_back(&mut cloned, entry);
                }
                Self::Listpack(cloned)
            }
            Self::BPTree { tree, member_index } => Self::BPTree {
                tree: tree.clone(),
                member_index: member_index.clone(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZSetObject {
    pub inner: ZSetEncoding,
    pub len: u32,
    generation: u64,
    hasher: RandomState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZAddCond {
    Always,
    NX,
    XX,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZAddOptions {
    pub condition: ZAddCond,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

impl Default for ZAddOptions {
    fn default() -> Self {
        Self {
            condition: ZAddCond::Always,
            gt: false,
            lt: false,
            ch: false,
            incr: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZAddResult {
    pub added: u64,
    pub changed: u64,
    pub new_score: Option<f64>,
}

impl Default for ZSetObject {
    fn default() -> Self {
        Self::with_hasher(senko_hasher())
    }
}

impl PartialEq for ZSetObject {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.all_entries() == other.all_entries()
    }
}

pub struct ZSetRangeIter<'a> {
    entries: VecDeque<(f64, CompactString)>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ZSetRangeIter<'a> {
    fn new(entries: Vec<(f64, CompactString)>) -> Self {
        Self {
            entries: entries.into(),
            _marker: PhantomData,
        }
    }
}

impl<'a> Iterator for ZSetRangeIter<'a> {
    type Item = (f64, CompactString);

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.pop_front()
    }
}

impl DoubleEndedIterator for ZSetRangeIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.entries.pop_back()
    }
}

impl ZSetObject {
    pub fn with_hasher(hasher: RandomState) -> Self {
        Self {
            inner: ZSetEncoding::Listpack(ListpackNode::default()),
            len: 0,
            generation: 0,
            hasher,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn add(&mut self, score: f64, member: CompactString, opts: ZAddOptions) -> ZAddResult {
        if score.is_nan() {
            return ZAddResult {
                added: 0,
                changed: 0,
                new_score: None,
            };
        }

        let current_score = self.score(member.as_bytes());

        if matches!(opts.condition, ZAddCond::NX) && current_score.is_some() {
            return ZAddResult {
                added: 0,
                changed: 0,
                new_score: current_score,
            };
        }
        if matches!(opts.condition, ZAddCond::XX) && current_score.is_none() {
            return ZAddResult {
                added: 0,
                changed: 0,
                new_score: None,
            };
        }

        let target_score = if opts.incr {
            current_score.unwrap_or(0.0) + score
        } else {
            score
        };

        if let Some(existing) = current_score {
            if opts.gt && target_score <= existing {
                return ZAddResult {
                    added: 0,
                    changed: 0,
                    new_score: opts.incr.then_some(existing),
                };
            }
            if opts.lt && target_score >= existing {
                return ZAddResult {
                    added: 0,
                    changed: 0,
                    new_score: opts.incr.then_some(existing),
                };
            }
            if existing == target_score {
                return ZAddResult {
                    added: 0,
                    changed: 0,
                    new_score: opts.incr.then_some(existing),
                };
            }

            match &mut self.inner {
                ZSetEncoding::Listpack(node) => {
                    let _ = lp_zset_remove(node, member.as_bytes());
                    let _ = lp_zset_insert(node, target_score, member.as_bytes());
                }
                ZSetEncoding::BPTree { tree, member_index } => {
                    let _ = tree.insert(target_score, member.clone());
                    member_index.insert(member, target_score);
                }
            }
            self.bump_generation();
            return ZAddResult {
                added: 0,
                changed: u64::from(opts.ch),
                new_score: opts.incr.then_some(target_score),
            };
        }

        self.ensure_encoding_for_insert(member.as_bytes());
        match &mut self.inner {
            ZSetEncoding::Listpack(node) => {
                let inserted = lp_zset_insert(node, target_score, member.as_bytes());
                debug_assert!(inserted);
            }
            ZSetEncoding::BPTree { tree, member_index } => {
                let _ = tree.insert(target_score, member.clone());
                member_index.insert(member, target_score);
            }
        }
        self.len = self.len.saturating_add(1);
        self.bump_generation();
        ZAddResult {
            added: 1,
            changed: 1,
            new_score: opts.incr.then_some(target_score),
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let removed = match &mut self.inner {
            ZSetEncoding::Listpack(node) => lp_zset_remove(node, member),
            ZSetEncoding::BPTree { tree, member_index } => {
                let member = str::from_utf8(member).ok()?;
                let removed = tree.remove(member.as_bytes());
                if removed.is_some() {
                    member_index.remove(member);
                }
                removed
            }
        }?;
        self.len = self.len.saturating_sub(1);
        self.bump_generation();
        Some(removed)
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match &self.inner {
            ZSetEncoding::Listpack(node) => lp_zset_score(node, member),
            ZSetEncoding::BPTree { member_index, .. } => {
                let member = str::from_utf8(member).ok()?;
                member_index.get(member).copied()
            }
        }
    }

    pub fn rank(&self, member: &[u8], reverse: bool) -> Option<u64> {
        match &self.inner {
            ZSetEncoding::Listpack(node) => {
                let rank = lp_zset_rank(node, member)?;
                if reverse {
                    Some(self.len as u64 - 1 - rank)
                } else {
                    Some(rank)
                }
            }
            ZSetEncoding::BPTree { tree, member_index } => {
                let member = str::from_utf8(member).ok()?;
                let score = member_index.get(member).copied()?;
                if reverse {
                    tree.rank_of_rev(score, member.as_bytes())
                } else {
                    tree.rank_of(score, member.as_bytes())
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn range_by_rank(
        &self,
        start: i64,
        stop: i64,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> ZSetRangeIter<'_> {
        let len = self.len();
        let Some((start, stop)) = normalize_rank_bounds(len, start, stop, reverse) else {
            return ZSetRangeIter::new(Vec::new());
        };
        let mut entries: Vec<(f64, CompactString)> = match &self.inner {
            ZSetEncoding::Listpack(node) => lp_zset_range_by_rank(node, start as u64, stop as u64)
                .collect(),
            ZSetEncoding::BPTree { tree, .. } => {
                tree.range_by_rank(start as u64, stop as u64).collect()
            }
        };
        if reverse {
            entries.reverse();
        }
        ZSetRangeIter::new(apply_limit(entries, limit))
    }

    pub fn range_by_score(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> ZSetRangeIter<'_> {
        let mut entries: Vec<(f64, CompactString)> = match &self.inner {
            ZSetEncoding::Listpack(node) => lp_zset_range_by_score(node, min, max)
                .collect(),
            ZSetEncoding::BPTree { tree, .. } => tree.range_by_score(min, max).collect(),
        };
        if reverse {
            entries.reverse();
        }
        ZSetRangeIter::new(apply_limit(entries, limit))
    }

    pub fn range_by_lex(
        &self,
        min: LexBound<'_>,
        max: LexBound<'_>,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> ZSetRangeIter<'_> {
        if !self.all_scores_equal() {
            return ZSetRangeIter::new(Vec::new());
        }
        let mut entries: Vec<_> = self
            .all_entries()
            .into_iter()
            .filter(|(_, member)| lex_contains(member.as_bytes(), min, max))
            .collect();
        if reverse {
            entries.reverse();
        }
        ZSetRangeIter::new(apply_limit(entries, limit))
    }

    pub fn count_by_score(&self, min: ScoreBound, max: ScoreBound) -> u64 {
        self.range_by_score(min, max, false, None).count() as u64
    }

    pub fn count_by_lex(&self, min: LexBound<'_>, max: LexBound<'_>) -> u64 {
        self.range_by_lex(min, max, false, None).count() as u64
    }

    pub fn pop_min(&mut self, count: usize) -> Vec<(f64, CompactString)> {
        self.pop_edge(count, false)
    }

    pub fn pop_max(&mut self, count: usize) -> Vec<(f64, CompactString)> {
        self.pop_edge(count, true)
    }

    pub fn random_member(&self, rng: &mut SmallRng) -> Option<(CompactString, f64)> {
        match &self.inner {
            ZSetEncoding::Listpack(node) => {
                let len = lp_len(node) / 2;
                if len == 0 {
                    return None;
                }
                let index = rng.gen_range(0..len);
                let (_, member) = lp_zset_entries(node).into_iter().nth(index)?;
                let score = self.score(member.as_bytes())?;
                Some((member, score))
            }
            ZSetEncoding::BPTree { member_index, .. } => {
                if member_index.is_empty() {
                    return None;
                }
                let index = rng.gen_range(0..member_index.len());
                member_index
                    .iter()
                    .nth(index)
                    .map(|(member, score)| (member.clone(), *score))
            }
        }
    }

    pub fn random_members_distinct(
        &self,
        n: usize,
        rng: &mut SmallRng,
    ) -> Vec<(CompactString, f64)> {
        let mut entries: Vec<_> = self
            .all_entries()
            .into_iter()
            .map(|(score, member)| (member, score))
            .collect();
        entries.shuffle(rng);
        entries.truncate(n.min(entries.len()));
        entries
    }

    pub fn random_members_repeating(
        &self,
        n: usize,
        rng: &mut SmallRng,
    ) -> Vec<(CompactString, f64)> {
        let entries = self.all_entries();
        if entries.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let index = rng.gen_range(0..entries.len());
            let (score, member) = &entries[index];
            out.push((member.clone(), *score));
        }
        out
    }

    fn ensure_encoding_for_insert(&mut self, member: &[u8]) {
        let ZSetEncoding::Listpack(node) = &self.inner else {
            return;
        };
        if lp_zset_score(node, member).is_some() {
            return;
        }
        let new_len = self.len() + 1;
        if new_len > LISTPACK_MAX_ENTRIES || member.len() > LISTPACK_MAX_MEMBER_SIZE {
            self.upgrade_listpack_to_bptree();
        }
    }

    fn upgrade_listpack_to_bptree(&mut self) {
        let ZSetEncoding::Listpack(node) = &self.inner else {
            return;
        };
        let entries = lp_zset_entries(node);
        let mut tree = BPTree::new();
        let mut member_index =
            HashMap::with_capacity_and_hasher(entries.len(), self.hasher.clone());
        for (score, member) in entries {
            let _ = tree.insert(score, member.clone());
            member_index.insert(member, score);
        }
        self.inner = ZSetEncoding::BPTree { tree, member_index };
        self.bump_generation();
    }

    fn pop_edge(&mut self, count: usize, max: bool) -> Vec<(f64, CompactString)> {
        let mut out = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            let popped = match &mut self.inner {
                ZSetEncoding::Listpack(node) => {
                    let mut entries = lp_zset_entries(node);
                    let item = if max {
                        entries.pop()
                    } else if entries.is_empty() {
                        None
                    } else {
                        Some(entries.remove(0))
                    };
                    if item.is_some() {
                        rewrite_lp_from_entries(node, &entries);
                    }
                    item
                }
                ZSetEncoding::BPTree { tree, member_index } => {
                    let item = if max { tree.pop_max() } else { tree.pop_min() };
                    if let Some((score, member)) = &item {
                        member_index.remove(member);
                        Some((*score, member.clone()))
                    } else {
                        None
                    }
                }
            };
            let Some(item) = popped else {
                break;
            };
            self.len = self.len.saturating_sub(1);
            out.push(item);
        }
        if !out.is_empty() {
            self.bump_generation();
        }
        out
    }

    fn all_entries(&self) -> Vec<(f64, CompactString)> {
        match &self.inner {
            ZSetEncoding::Listpack(node) => lp_zset_entries(node),
            ZSetEncoding::BPTree { tree, .. } => {
                if tree.is_empty() {
                    Vec::new()
                } else {
                    tree.range_by_rank(0, tree.len() - 1).collect()
                }
            }
        }
    }

    fn all_scores_equal(&self) -> bool {
        let mut entries = self.all_entries().into_iter();
        let Some((first, _)) = entries.next() else {
            return true;
        };
        entries.all(|(score, _)| score == first)
    }

    fn bump_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }
}

pub fn lp_zset_insert(node: &mut ListpackNode, score: f64, member: &[u8]) -> bool {
    if lp_zset_score(node, member).is_some() {
        return false;
    }
    let mut entries = lp_zset_entries(node);
    let insert_at = entries.partition_point(|(existing_score, existing_member)| {
        compare_score_member(*existing_score, existing_member.as_str(), score, member)
            == std::cmp::Ordering::Less
    });
    entries.insert(insert_at, (score, compact_from_bytes(member)));
    rewrite_lp_from_entries(node, &entries);
    true
}

pub fn lp_zset_remove(node: &mut ListpackNode, member: &[u8]) -> Option<f64> {
    let mut entries = lp_zset_entries(node);
    let position = entries
        .iter()
        .position(|(_, existing_member)| existing_member.as_bytes() == member)?;
    let (score, _) = entries.remove(position);
    rewrite_lp_from_entries(node, &entries);
    Some(score)
}

pub fn lp_zset_score(node: &ListpackNode, member: &[u8]) -> Option<f64> {
    lp_zset_entries(node)
        .into_iter()
        .find(|(_, existing_member)| existing_member.as_bytes() == member)
        .map(|(score, _)| score)
}

pub fn lp_zset_rank(node: &ListpackNode, member: &[u8]) -> Option<u64> {
    lp_zset_entries(node)
        .iter()
        .position(|(_, existing_member)| existing_member.as_bytes() == member)
        .map(|index| index as u64)
}

pub fn lp_zset_iter(node: &ListpackNode) -> ZSetRangeIter<'_> {
    ZSetRangeIter::new(lp_zset_entries(node))
}

pub fn lp_zset_range_by_rank(node: &ListpackNode, start: u64, stop: u64) -> ZSetRangeIter<'_> {
    let entries = lp_zset_entries(node);
    if entries.is_empty() {
        return ZSetRangeIter::new(Vec::new());
    }
    let start = start.min(entries.len() as u64 - 1) as usize;
    let stop = stop.min(entries.len() as u64 - 1) as usize;
    if start > stop {
        return ZSetRangeIter::new(Vec::new());
    }
    ZSetRangeIter::new(entries[start..=stop].to_vec())
}

pub fn lp_zset_range_by_score(
    node: &ListpackNode,
    min: ScoreBound,
    max: ScoreBound,
) -> ZSetRangeIter<'_> {
    let entries = lp_zset_entries(node)
        .into_iter()
        .filter(|(score, _)| score_in_range(*score, min, max))
        .collect();
    ZSetRangeIter::new(entries)
}

fn lp_zset_entries(node: &ListpackNode) -> Vec<(f64, CompactString)> {
    let raw: Vec<_> = lp_iter(node).map(|entry| entry.to_vec()).collect();
    let mut entries = Vec::with_capacity(raw.len() / 2);
    let mut index = 0;
    while index + 1 < raw.len() {
        let member = compact_from_bytes(&raw[index]);
        let score = decode_lp_score(&raw[index + 1]).unwrap_or(0.0);
        entries.push((score, member));
        index += 2;
    }
    entries
}

fn rewrite_lp_from_entries(node: &mut ListpackNode, entries: &[(f64, CompactString)]) {
    while lp_len(node) > 0 {
        lp_delete_at(node, 0);
    }
    for (score, member) in entries {
        lp_push_back(node, member.as_bytes());
        lp_push_back(node, &score.to_le_bytes());
    }
}

fn compact_from_bytes(bytes: &[u8]) -> CompactString {
    CompactString::from(String::from_utf8_lossy(bytes).as_ref())
}

fn decode_lp_score(bytes: &[u8]) -> Option<f64> {
    if bytes.len() == 8 {
        let mut raw = [0_u8; 8];
        raw.copy_from_slice(bytes);
        Some(f64::from_le_bytes(raw))
    } else {
        str::from_utf8(bytes).ok()?.parse::<f64>().ok()
    }
}

fn compare_score_member(
    left_score: f64,
    left_member: &str,
    right_score: f64,
    right_member: &[u8],
) -> std::cmp::Ordering {
    if left_score < right_score {
        std::cmp::Ordering::Less
    } else if left_score > right_score {
        std::cmp::Ordering::Greater
    } else {
        left_member.as_bytes().cmp(right_member)
    }
}

fn normalize_rank_bounds(
    len: usize,
    start: i64,
    stop: i64,
    reverse: bool,
) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i64 = len as i64;
    let normalize = |index: i64| if index < 0 { len_i64 + index } else { index };
    let mut start = normalize(start);
    let mut stop = normalize(stop);
    if start < 0 {
        start = 0;
    }
    if stop < 0 {
        return None;
    }
    if start >= len_i64 {
        return None;
    }
    stop = stop.min(len_i64 - 1);
    if start > stop {
        return None;
    }
    if reverse {
        Some((
            (len_i64 - 1 - stop) as usize,
            (len_i64 - 1 - start) as usize,
        ))
    } else {
        Some((start as usize, stop as usize))
    }
}

fn apply_limit(
    entries: Vec<(f64, CompactString)>,
    limit: Option<(i64, i64)>,
) -> Vec<(f64, CompactString)> {
    let Some((offset, count)) = limit else {
        return entries;
    };
    if count <= 0 {
        return Vec::new();
    }
    entries
        .into_iter()
        .skip(offset.max(0) as usize)
        .take(count as usize)
        .collect()
}

fn score_in_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    let lower = match min {
        ScoreBound::Inclusive(v) => score >= v,
        ScoreBound::Exclusive(v) => score > v,
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => false,
    };
    let upper = match max {
        ScoreBound::Inclusive(v) => score <= v,
        ScoreBound::Exclusive(v) => score < v,
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
    };
    lower && upper
}

fn lex_contains(member: &[u8], min: LexBound<'_>, max: LexBound<'_>) -> bool {
    let lower = match min {
        LexBound::Inclusive(v) => member >= v,
        LexBound::Exclusive(v) => member > v,
        LexBound::Min => true,
        LexBound::Max => false,
    };
    let upper = match max {
        LexBound::Inclusive(v) => member <= v,
        LexBound::Exclusive(v) => member < v,
        LexBound::Min => false,
        LexBound::Max => true,
    };
    lower && upper
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::SmallRng};

    use super::*;

    fn member(index: usize) -> CompactString {
        CompactString::from(format!("m{index:04}"))
    }

    #[test]
    fn listpack_insert_rank_remove() {
        let mut zset = ZSetObject::default();
        for i in (0..10).rev() {
            let result = zset.add(i as f64, member(i), ZAddOptions::default());
            assert_eq!(result.added, 1);
        }
        let ordered: Vec<_> = zset.range_by_rank(0, 9, false, None).collect();
        assert_eq!(ordered.len(), 10);
        assert_eq!(zset.rank(b"m0000", false), Some(0));
        assert_eq!(zset.remove(b"m0005"), Some(5.0));
        assert_eq!(zset.score(b"m0005"), None);
    }

    #[test]
    fn listpack_upgrades_to_bptree_at_threshold() {
        let mut zset = ZSetObject::default();
        for i in 0..128 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        assert!(matches!(zset.inner, ZSetEncoding::Listpack(_)));
        zset.add(129.0, member(129), ZAddOptions::default());
        assert!(matches!(zset.inner, ZSetEncoding::BPTree { .. }));
    }

    #[test]
    fn zadd_conditions_and_ch() {
        let mut zset = ZSetObject::default();
        zset.add(1.0, CompactString::from("a"), ZAddOptions::default());
        let nx = zset.add(
            2.0,
            CompactString::from("a"),
            ZAddOptions {
                condition: ZAddCond::NX,
                ..Default::default()
            },
        );
        assert_eq!(nx.added, 0);
        assert_eq!(zset.score(b"a"), Some(1.0));

        let xx = zset.add(
            3.0,
            CompactString::from("b"),
            ZAddOptions {
                condition: ZAddCond::XX,
                ..Default::default()
            },
        );
        assert_eq!(xx.added, 0);
        assert_eq!(zset.score(b"b"), None);

        let gt = zset.add(
            4.0,
            CompactString::from("a"),
            ZAddOptions {
                gt: true,
                ch: true,
                ..Default::default()
            },
        );
        assert_eq!(gt.changed, 1);
        assert_eq!(zset.score(b"a"), Some(4.0));

        let lt = zset.add(
            5.0,
            CompactString::from("a"),
            ZAddOptions {
                lt: true,
                ..Default::default()
            },
        );
        assert_eq!(lt.changed, 0);
        assert_eq!(zset.score(b"a"), Some(4.0));
    }

    #[test]
    fn incr_returns_new_score() {
        let mut zset = ZSetObject::default();
        let first = zset.add(
            2.0,
            CompactString::from("inc"),
            ZAddOptions {
                incr: true,
                ..Default::default()
            },
        );
        assert_eq!(first.new_score, Some(2.0));
        let second = zset.add(
            3.0,
            CompactString::from("inc"),
            ZAddOptions {
                incr: true,
                ..Default::default()
            },
        );
        assert_eq!(second.new_score, Some(5.0));
        assert_eq!(zset.score(b"inc"), Some(5.0));
    }

    #[test]
    fn member_index_stays_in_sync() {
        let mut zset = ZSetObject::default();
        for i in 0..10_000 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        for i in (0..10_000).step_by(3) {
            let _ = zset.remove(member(i).as_bytes());
        }
        let ZSetEncoding::BPTree { tree, member_index } = &zset.inner else {
            panic!("expected bptree encoding");
        };
        let tree_entries: Vec<_> = tree.range_by_rank(0, tree.len() - 1).collect();
        assert_eq!(tree_entries.len(), member_index.len());
        for (score, member) in tree_entries {
            assert_eq!(member_index.get(&member), Some(&score));
        }
    }

    #[test]
    fn range_by_rank_limit_offset() {
        let mut zset = ZSetObject::default();
        for i in 0..20 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        let entries: Vec<_> = zset.range_by_rank(0, 19, false, Some((5, 3))).collect();
        assert_eq!(
            entries,
            vec![(5.0, member(5)), (6.0, member(6)), (7.0, member(7))]
        );
    }

    #[test]
    fn range_by_score_exclusive_bounds() {
        let mut zset = ZSetObject::default();
        for i in 0..5 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        let entries: Vec<_> = zset
            .range_by_score(
                ScoreBound::Exclusive(1.0),
                ScoreBound::Exclusive(3.0),
                false,
                None,
            )
            .collect();
        assert_eq!(entries, vec![(2.0, member(2))]);
    }

    #[test]
    fn range_by_lex_requires_equal_scores() {
        let mut zset = ZSetObject::default();
        zset.add(1.0, CompactString::from("a"), ZAddOptions::default());
        zset.add(2.0, CompactString::from("b"), ZAddOptions::default());
        assert!(
            zset.range_by_lex(LexBound::Min, LexBound::Max, false, None)
                .next()
                .is_none()
        );
    }

    #[test]
    fn pop_keeps_bptree_index_in_sync() {
        let mut zset = ZSetObject::default();
        for i in 0..256 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        let popped = zset.pop_min(3);
        assert_eq!(popped.len(), 3);
        let ZSetEncoding::BPTree { member_index, .. } = &zset.inner else {
            panic!("expected bptree");
        };
        for (_, member) in popped {
            assert!(!member_index.contains_key(&member));
        }
    }

    #[test]
    fn random_members_distinct_have_no_duplicates() {
        let mut zset = ZSetObject::default();
        for i in 0..64 {
            zset.add(i as f64, member(i), ZAddOptions::default());
        }
        let mut rng = SmallRng::seed_from_u64(42);
        let sample = zset.random_members_distinct(16, &mut rng);
        let unique: std::collections::HashSet<_> =
            sample.iter().map(|(member, _)| member.clone()).collect();
        assert_eq!(unique.len(), sample.len());
        assert!(
            sample
                .iter()
                .all(|(member, score)| zset.score(member.as_bytes()) == Some(*score))
        );
    }
}
