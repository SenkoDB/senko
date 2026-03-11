use std::array::from_fn;
use std::cmp::Ordering;

use std::collections::HashMap;

use compact_str::CompactString;
use smallvec::SmallVec;

const LEAF_CAPACITY: usize = 14;
const INNER_CAPACITY: usize = 21;
const MIN_LEAF_ITEMS: usize = LEAF_CAPACITY / 2;
const MIN_INNER_KEYS: usize = INNER_CAPACITY / 2;
const SLAB_SIZE: usize = 256;

type NodeId = usize;

#[derive(Clone, Debug, PartialEq)]
pub enum InsertResult {
    Inserted,
    Updated(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    NegInf,
    PosInf,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LexBound<'a> {
    Inclusive(&'a [u8]),
    Exclusive(&'a [u8]),
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ZSetEntry {
    pub score: f64,
    pub member: CompactString,
}

#[derive(Clone, Debug, PartialEq)]
struct EntryKey {
    score: f64,
    member: CompactString,
}

impl EntryKey {
    fn new(score: f64, member: CompactString) -> Self {
        Self { score, member }
    }

    fn from_entry(entry: &ZSetEntry) -> Self {
        Self {
            score: entry.score,
            member: entry.member.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct LeafNode {
    entries: SmallVec<[ZSetEntry; LEAF_CAPACITY]>,
    next: Option<NodeId>,
    prev: Option<NodeId>,
}

#[derive(Clone, Debug)]
struct InnerNode {
    keys: SmallVec<[EntryKey; INNER_CAPACITY]>,
    children: SmallVec<[NodeId; INNER_CAPACITY + 1]>,
    child_sizes: SmallVec<[u64; INNER_CAPACITY + 1]>,
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum Node {
    Leaf(LeafNode),
    Inner(InnerNode),
}

#[derive(Debug, Clone, Default)]
struct NodeArena {
    slabs: Vec<Box<[Option<Node>; SLAB_SIZE]>>,
    free_list: Vec<NodeId>,
    next_id: usize,
    live_nodes: usize,
}

impl NodeArena {
    fn alloc(&mut self, node: Node) -> NodeId {
        self.live_nodes += 1;
        if let Some(id) = self.free_list.pop() {
            *self.slot_mut(id) = Some(node);
            return id;
        }

        let id = self.next_id;
        if id == self.slabs.len() * SLAB_SIZE {
            self.slabs.push(Box::new(from_fn(|_| None)));
        }
        self.next_id += 1;
        *self.slot_mut(id) = Some(node);
        id
    }

    fn get(&self, id: NodeId) -> &Node {
        self.slot(id)
            .as_ref()
            .expect("node arena slot must be populated")
    }

    fn get_mut(&mut self, id: NodeId) -> &mut Node {
        self.slot_mut(id)
            .as_mut()
            .expect("node arena slot must be populated")
    }

    fn live_nodes(&self) -> usize {
        self.live_nodes
    }

    fn clear(&mut self) {
        self.slabs.clear();
        self.free_list.clear();
        self.next_id = 0;
        self.live_nodes = 0;
    }

    fn slot(&self, id: NodeId) -> &Option<Node> {
        let slab = id / SLAB_SIZE;
        let offset = id % SLAB_SIZE;
        &self.slabs[slab][offset]
    }

    fn slot_mut(&mut self, id: NodeId) -> &mut Option<Node> {
        let slab = id / SLAB_SIZE;
        let offset = id % SLAB_SIZE;
        &mut self.slabs[slab][offset]
    }
}

#[derive(Clone, Debug)]
struct SearchPath {
    leaf_id: NodeId,
    index: usize,
    found: bool,
    rank: u64,
}

#[derive(Clone, Debug)]
enum IterStop {
    None,
    Score(ScoreBound),
    Lex { max: LexBoundOwned, score: f64 },
}

#[derive(Clone, Debug)]
enum LexBoundOwned {
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
    Min,
    Max,
}

pub struct BPTreeRangeIter<'a> {
    tree: &'a BPTree,
    current_leaf: Option<NodeId>,
    index: usize,
    remaining: usize,
    reversed: bool,
    stop: IterStop,
}

#[derive(Debug, Clone, Default)]
pub struct BPTree {
    root: Option<NodeId>,
    leftmost: Option<NodeId>,
    rightmost: Option<NodeId>,
    len: u64,
    arena: NodeArena,
    member_scores: HashMap<CompactString, f64>,
}

impl BPTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn node_count(&self) -> usize {
        self.arena.live_nodes()
    }

    pub fn height(&self) -> usize {
        let Some(mut node_id) = self.root else {
            return 0;
        };
        let mut height = 1;
        loop {
            match self.arena.get(node_id) {
                Node::Leaf(_) => return height,
                Node::Inner(inner) => {
                    node_id = inner.children[0];
                    height += 1;
                }
            }
        }
    }

    pub fn clear(&mut self) {
        self.root = None;
        self.leftmost = None;
        self.rightmost = None;
        self.len = 0;
        self.member_scores.clear();
        self.arena.clear();
    }

    pub fn insert(&mut self, score: f64, member: CompactString) -> InsertResult {
        if let Some(old_score) = self.member_scores.get(&member).copied() {
            if scores_equal(old_score, score) {
                return InsertResult::Updated(old_score);
            }
            let old_key = EntryKey::new(old_score, member.clone());
            let removed = self.remove_exact(&old_key, false);
            debug_assert!(removed);
            self.member_scores.insert(member.clone(), score);
            self.insert_new_entry(ZSetEntry { score, member });
            return InsertResult::Updated(old_score);
        }

        self.member_scores.insert(member.clone(), score);
        self.insert_new_entry(ZSetEntry { score, member });
        InsertResult::Inserted
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let member = std::str::from_utf8(member).ok()?;
        let member = CompactString::from(member);
        let score = self.member_scores.get(&member).copied()?;
        let removed = self.remove_exact(&EntryKey::new(score, member.clone()), true);
        if removed { Some(score) } else { None }
    }

    pub fn get_score(&self, score: f64, member: &[u8]) -> bool {
        let Some(member) = std::str::from_utf8(member).ok() else {
            return false;
        };
        self.member_scores
            .get(member)
            .is_some_and(|stored| scores_equal(*stored, score))
    }

    pub fn rank_of(&self, score: f64, member: &[u8]) -> Option<u64> {
        let member = std::str::from_utf8(member).ok()?;
        let path = self.search_key(&EntryKey::new(score, CompactString::from(member)))?;
        path.found.then_some(path.rank)
    }

    pub fn rank_of_rev(&self, score: f64, member: &[u8]) -> Option<u64> {
        let rank = self.rank_of(score, member)?;
        Some(self.len - 1 - rank)
    }

    pub fn range_by_rank(&self, start: u64, stop: u64) -> BPTreeRangeIter<'_> {
        if start > stop || start >= self.len {
            return self.empty_iter();
        }
        let stop = stop.min(self.len.saturating_sub(1));
        let Some((leaf_id, index)) = self.locate_by_rank(start) else {
            return self.empty_iter();
        };
        BPTreeRangeIter {
            tree: self,
            current_leaf: Some(leaf_id),
            index,
            remaining: (stop - start + 1) as usize,
            reversed: false,
            stop: IterStop::None,
        }
    }

    pub fn range_by_score(&self, min: ScoreBound, max: ScoreBound) -> BPTreeRangeIter<'_> {
        let Some(hit) = self.lower_bound_score(min) else {
            return self.empty_iter();
        };
        if !self.score_within_upper_bound(hit.score(self), max) {
            return self.empty_iter();
        }
        BPTreeRangeIter {
            tree: self,
            current_leaf: Some(hit.leaf_id),
            index: hit.index,
            remaining: usize::MAX,
            reversed: false,
            stop: IterStop::Score(max),
        }
    }

    pub fn range_by_lex(
        &self,
        min: LexBound<'_>,
        max: LexBound<'_>,
        score: f64,
    ) -> BPTreeRangeIter<'_> {
        let Some(mut hit) = self.lower_bound_score(ScoreBound::Inclusive(score)) else {
            return self.empty_iter();
        };

        while let Some(entry) = self.entry_at(hit.leaf_id, hit.index) {
            if !scores_equal(entry.score, score) {
                return self.empty_iter();
            }
            if self.lex_within_lower_bound(entry.member.as_bytes(), &min) {
                break;
            }
            if !hit.advance(self) {
                return self.empty_iter();
            }
        }

        BPTreeRangeIter {
            tree: self,
            current_leaf: Some(hit.leaf_id),
            index: hit.index,
            remaining: usize::MAX,
            reversed: false,
            stop: IterStop::Lex {
                max: max.into(),
                score,
            },
        }
    }

    pub fn count_in_score_range(&self, min: ScoreBound, max: ScoreBound) -> u64 {
        self.range_by_score(min, max).count() as u64
    }

    pub fn count_in_lex_range(&self, min: LexBound<'_>, max: LexBound<'_>, score: f64) -> u64 {
        self.range_by_lex(min, max, score).count() as u64
    }

    pub fn pop_min(&mut self) -> Option<(f64, CompactString)> {
        let leaf_id = self.leftmost?;
        let first = self.leaf(leaf_id).entries.first()?.clone();
        let score = first.score;
        let member = first.member.clone();
        let removed = self.remove_exact(&EntryKey::new(score, member.clone()), true);
        if removed { Some((score, member)) } else { None }
    }

    pub fn pop_max(&mut self) -> Option<(f64, CompactString)> {
        let leaf_id = self.rightmost?;
        let last = self.leaf(leaf_id).entries.last()?.clone();
        let score = last.score;
        let member = last.member.clone();
        let removed = self.remove_exact(&EntryKey::new(score, member.clone()), true);
        if removed { Some((score, member)) } else { None }
    }

    fn insert_new_entry(&mut self, entry: ZSetEntry) {
        if self.root.is_none() {
            let node_id = self.arena.alloc(Node::Leaf(LeafNode {
                entries: smallvec::smallvec![entry],
                next: None,
                prev: None,
            }));
            self.root = Some(node_id);
            self.leftmost = Some(node_id);
            self.rightmost = Some(node_id);
            self.len = 1;
            return;
        }

        let root = self.root.expect("root checked above");
        let split = self.insert_recursive(root, &entry);
        if let Some((separator, right_id)) = split {
            let left_id = root;
            let new_root = InnerNode {
                keys: smallvec::smallvec![separator],
                children: smallvec::smallvec![left_id, right_id],
                child_sizes: smallvec::smallvec![
                    self.subtree_size(left_id),
                    self.subtree_size(right_id)
                ],
            };
            self.root = Some(self.arena.alloc(Node::Inner(new_root)));
        }
        self.len += 1;
    }

    fn remove_exact(&mut self, key: &EntryKey, remove_from_index: bool) -> bool {
        let Some(path) = self.search_key(key) else {
            return false;
        };
        if !path.found {
            return false;
        }

        let leaf_id = path.leaf_id;
        let removed = {
            let leaf = self.leaf_mut(leaf_id);
            leaf.entries.remove(path.index)
        };

        if remove_from_index {
            self.member_scores.remove(&removed.member);
        }

        self.len -= 1;
        if self.len == 0 {
            self.root = None;
            self.leftmost = None;
            self.rightmost = None;
            self.arena.clear();
            return true;
        }

        self.rebuild_tree();
        true
    }

    fn rebuild_tree(&mut self) {
        let entries = self.collect_entries();
        self.root = None;
        self.leftmost = None;
        self.rightmost = None;
        self.len = 0;
        self.arena.clear();
        for entry in entries {
            self.insert_new_entry(entry);
        }
    }

    fn collect_entries(&self) -> Vec<ZSetEntry> {
        let mut out = Vec::with_capacity(self.len as usize);
        let mut current = self.leftmost;
        while let Some(node_id) = current {
            let leaf = self.leaf(node_id);
            out.extend(leaf.entries.iter().cloned());
            current = leaf.next;
        }
        out
    }

    fn insert_recursive(
        &mut self,
        node_id: NodeId,
        entry: &ZSetEntry,
    ) -> Option<(EntryKey, NodeId)> {
        match self.arena.get(node_id) {
            Node::Leaf(_) => self.insert_into_leaf(node_id, entry),
            Node::Inner(inner) => {
                let child_index =
                    child_index_for_key(&inner.keys, entry.score, entry.member.as_str());
                let child_id = inner.children[child_index];
                let split = self.insert_recursive(child_id, entry);

                if let Some((separator, right_id)) = split {
                    let left_size = self.subtree_size(child_id);
                    let right_size = self.subtree_size(right_id);
                    let inner = self.inner_mut(node_id);
                    inner.keys.insert(child_index, separator);
                    inner.children.insert(child_index + 1, right_id);
                    inner.child_sizes[child_index] = left_size;
                    inner.child_sizes.insert(child_index + 1, right_size);
                    if inner.keys.len() > INNER_CAPACITY {
                        return self.split_inner(node_id);
                    }
                    return None;
                }

                self.inner_mut(node_id).child_sizes[child_index] += 1;
                None
            }
        }
    }

    fn insert_into_leaf(
        &mut self,
        node_id: NodeId,
        entry: &ZSetEntry,
    ) -> Option<(EntryKey, NodeId)> {
        let insert_index = {
            let leaf = self.leaf(node_id);
            lower_bound_entry(&leaf.entries, entry.score, entry.member.as_str())
        };

        {
            let leaf = self.leaf_mut(node_id);
            leaf.entries.insert(insert_index, entry.clone());
            if leaf.entries.len() <= LEAF_CAPACITY {
                return None;
            }
        }

        self.split_leaf(node_id)
    }

    fn split_leaf(&mut self, node_id: NodeId) -> Option<(EntryKey, NodeId)> {
        let (right_entries, next) = {
            let leaf = self.leaf_mut(node_id);
            let split_at = MIN_LEAF_ITEMS;
            let next = leaf.next;
            let right_entries = leaf
                .entries
                .drain(split_at..)
                .collect::<SmallVec<[ZSetEntry; LEAF_CAPACITY]>>();
            leaf.next = None;
            (right_entries, next)
        };

        let right_id = self.arena.alloc(Node::Leaf(LeafNode {
            entries: right_entries,
            next,
            prev: Some(node_id),
        }));

        {
            let leaf = self.leaf_mut(node_id);
            leaf.next = Some(right_id);
        }

        if let Some(next_id) = next {
            self.leaf_mut(next_id).prev = Some(right_id);
        } else {
            self.rightmost = Some(right_id);
        }

        let separator = {
            let right = self.leaf(right_id);
            EntryKey::from_entry(&right.entries[0])
        };
        Some((separator, right_id))
    }

    fn split_inner(&mut self, node_id: NodeId) -> Option<(EntryKey, NodeId)> {
        let (separator, right_keys, right_children, right_sizes) = {
            let inner = self.inner_mut(node_id);
            let split_at = inner.keys.len() / 2;
            let separator = inner.keys[split_at].clone();
            let right_keys = inner
                .keys
                .drain((split_at + 1)..)
                .collect::<SmallVec<[EntryKey; INNER_CAPACITY]>>();
            let right_children = inner
                .children
                .drain((split_at + 1)..)
                .collect::<SmallVec<[NodeId; INNER_CAPACITY + 1]>>();
            let right_sizes = inner
                .child_sizes
                .drain((split_at + 1)..)
                .collect::<SmallVec<[u64; INNER_CAPACITY + 1]>>();
            inner.keys.truncate(split_at);
            (separator, right_keys, right_children, right_sizes)
        };

        let right_id = self.arena.alloc(Node::Inner(InnerNode {
            keys: right_keys,
            children: right_children,
            child_sizes: right_sizes,
        }));

        if self.inner(node_id).keys.len() < MIN_INNER_KEYS && self.root != Some(node_id) {
            debug_assert!(self.inner(node_id).keys.len() < INNER_CAPACITY);
        }

        Some((separator, right_id))
    }

    fn search_key(&self, key: &EntryKey) -> Option<SearchPath> {
        let mut node_id = self.root?;
        let mut rank = 0_u64;

        loop {
            match self.arena.get(node_id) {
                Node::Leaf(leaf) => {
                    let index = lower_bound_entry(&leaf.entries, key.score, key.member.as_str());
                    let found = leaf
                        .entries
                        .get(index)
                        .is_some_and(|entry| entry_matches(entry, key.score, key.member.as_str()));
                    return Some(SearchPath {
                        leaf_id: node_id,
                        index,
                        found,
                        rank: rank + index as u64,
                    });
                }
                Node::Inner(inner) => {
                    let child_index =
                        child_index_for_key(&inner.keys, key.score, key.member.as_str());
                    rank += inner.child_sizes[..child_index]
                        .iter()
                        .copied()
                        .sum::<u64>();
                    node_id = inner.children[child_index];
                }
            }
        }
    }

    fn locate_by_rank(&self, mut rank: u64) -> Option<(NodeId, usize)> {
        let mut node_id = self.root?;
        loop {
            match self.arena.get(node_id) {
                Node::Leaf(leaf) => {
                    return (rank < leaf.entries.len() as u64).then_some((node_id, rank as usize));
                }
                Node::Inner(inner) => {
                    let mut child_index = 0;
                    while child_index < inner.child_sizes.len()
                        && rank >= inner.child_sizes[child_index]
                    {
                        rank -= inner.child_sizes[child_index];
                        child_index += 1;
                    }
                    node_id = inner.children[child_index];
                }
            }
        }
    }

    fn lower_bound_score(&self, bound: ScoreBound) -> Option<ScoreSearchHit> {
        let root = self.root?;
        let mut node_id = root;
        let mut rank = 0_u64;

        loop {
            match self.arena.get(node_id) {
                Node::Leaf(leaf) => {
                    let mut index = leaf
                        .entries
                        .partition_point(|entry| score_lower_predicate(entry.score, bound));
                    let mut current_leaf = node_id;
                    let mut current_rank = rank + index as u64;

                    loop {
                        let leaf = self.leaf(current_leaf);
                        if index < leaf.entries.len() {
                            return Some(ScoreSearchHit {
                                leaf_id: current_leaf,
                                index,
                                rank: current_rank,
                            });
                        }
                        let next = leaf.next?;
                        current_rank += (leaf.entries.len() - index) as u64;
                        current_leaf = next;
                        index = 0;
                    }
                }
                Node::Inner(inner) => {
                    let child_index = child_index_for_score_bound(&inner.keys, bound)
                        .min(inner.children.len() - 1);
                    rank += inner.child_sizes[..child_index]
                        .iter()
                        .copied()
                        .sum::<u64>();
                    node_id = inner.children[child_index];
                }
            }
        }
    }

    fn score_within_upper_bound(&self, score: f64, bound: ScoreBound) -> bool {
        match bound {
            ScoreBound::Inclusive(max) => score <= max || scores_equal(score, max),
            ScoreBound::Exclusive(max) => score < max && !scores_equal(score, max),
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
        }
    }

    fn lex_within_lower_bound(&self, member: &[u8], bound: &LexBound<'_>) -> bool {
        match bound {
            LexBound::Inclusive(min) => member >= *min,
            LexBound::Exclusive(min) => member > *min,
            LexBound::Min => true,
            LexBound::Max => false,
        }
    }

    fn entry_at(&self, leaf_id: NodeId, index: usize) -> Option<&ZSetEntry> {
        self.leaf(leaf_id).entries.get(index)
    }

    fn subtree_size(&self, node_id: NodeId) -> u64 {
        match self.arena.get(node_id) {
            Node::Leaf(leaf) => leaf.entries.len() as u64,
            Node::Inner(inner) => inner.child_sizes.iter().copied().sum(),
        }
    }

    fn empty_iter(&self) -> BPTreeRangeIter<'_> {
        BPTreeRangeIter {
            tree: self,
            current_leaf: None,
            index: 0,
            remaining: 0,
            reversed: false,
            stop: IterStop::None,
        }
    }

    fn leaf(&self, node_id: NodeId) -> &LeafNode {
        match self.arena.get(node_id) {
            Node::Leaf(leaf) => leaf,
            Node::Inner(_) => panic!("expected leaf node"),
        }
    }

    fn leaf_mut(&mut self, node_id: NodeId) -> &mut LeafNode {
        match self.arena.get_mut(node_id) {
            Node::Leaf(leaf) => leaf,
            Node::Inner(_) => panic!("expected leaf node"),
        }
    }

    fn inner(&self, node_id: NodeId) -> &InnerNode {
        match self.arena.get(node_id) {
            Node::Inner(inner) => inner,
            Node::Leaf(_) => panic!("expected inner node"),
        }
    }

    fn inner_mut(&mut self, node_id: NodeId) -> &mut InnerNode {
        match self.arena.get_mut(node_id) {
            Node::Inner(inner) => inner,
            Node::Leaf(_) => panic!("expected inner node"),
        }
    }
}

impl Iterator for BPTreeRangeIter<'_> {
    type Item = (f64, CompactString);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        loop {
            let leaf_id = self.current_leaf?;
            let leaf = self.tree.leaf(leaf_id);
            if self.index >= leaf.entries.len() {
                self.current_leaf = if self.reversed { leaf.prev } else { leaf.next };
                self.index = if self.reversed {
                    self.current_leaf
                        .map(|next_leaf| self.tree.leaf(next_leaf).entries.len().saturating_sub(1))
                        .unwrap_or(0)
                } else {
                    0
                };
                continue;
            }

            let entry = &leaf.entries[self.index];
            match &self.stop {
                IterStop::None => {}
                IterStop::Score(max) => {
                    if !self.tree.score_within_upper_bound(entry.score, *max) {
                        self.current_leaf = None;
                        return None;
                    }
                }
                IterStop::Lex { max, score } => {
                    if !scores_equal(entry.score, *score)
                        || !lex_matches_upper_bound(entry.member.as_bytes(), max)
                    {
                        self.current_leaf = None;
                        return None;
                    }
                }
            }

            let out = (entry.score, entry.member.clone());
            if self.remaining != usize::MAX {
                self.remaining -= 1;
            }

            if self.reversed {
                if self.index == 0 {
                    self.current_leaf = leaf.prev;
                    self.index = self
                        .current_leaf
                        .map(|prev_leaf| self.tree.leaf(prev_leaf).entries.len().saturating_sub(1))
                        .unwrap_or(0);
                } else {
                    self.index -= 1;
                }
            } else {
                self.index += 1;
            }
            return Some(out);
        }
    }
}

#[derive(Clone, Debug)]
struct ScoreSearchHit {
    leaf_id: NodeId,
    index: usize,
    rank: u64,
}

impl ScoreSearchHit {
    fn score(&self, tree: &BPTree) -> f64 {
        tree.leaf(self.leaf_id).entries[self.index].score
    }

    fn advance(&mut self, tree: &BPTree) -> bool {
        let leaf = tree.leaf(self.leaf_id);
        if self.index + 1 < leaf.entries.len() {
            self.index += 1;
            self.rank += 1;
            return true;
        }
        let Some(next) = leaf.next else {
            return false;
        };
        self.leaf_id = next;
        self.index = 0;
        self.rank += 1;
        true
    }
}

impl From<LexBound<'_>> for LexBoundOwned {
    fn from(bound: LexBound<'_>) -> Self {
        match bound {
            LexBound::Inclusive(v) => LexBoundOwned::Inclusive(v.to_vec()),
            LexBound::Exclusive(v) => LexBoundOwned::Exclusive(v.to_vec()),
            LexBound::Min => LexBoundOwned::Min,
            LexBound::Max => LexBoundOwned::Max,
        }
    }
}

fn scores_equal(lhs: f64, rhs: f64) -> bool {
    lhs == rhs
}

fn compare_key(score_a: f64, member_a: &str, score_b: f64, member_b: &str) -> Ordering {
    if score_a < score_b {
        Ordering::Less
    } else if score_a > score_b {
        Ordering::Greater
    } else {
        member_a.as_bytes().cmp(member_b.as_bytes())
    }
}

fn entry_matches(entry: &ZSetEntry, score: f64, member: &str) -> bool {
    scores_equal(entry.score, score) && entry.member.as_str() == member
}

fn lower_bound_entry(entries: &[ZSetEntry], score: f64, member: &str) -> usize {
    entries.partition_point(|entry| {
        compare_key(entry.score, entry.member.as_str(), score, member) == Ordering::Less
    })
}

fn child_index_for_key(keys: &[EntryKey], score: f64, member: &str) -> usize {
    keys.partition_point(|key| {
        compare_key(key.score, key.member.as_str(), score, member) != Ordering::Greater
    })
}

fn child_index_for_score_bound(keys: &[EntryKey], bound: ScoreBound) -> usize {
    match bound {
        ScoreBound::Inclusive(score) => keys.partition_point(|key| key.score < score),
        ScoreBound::Exclusive(score) => keys.partition_point(|key| key.score <= score),
        ScoreBound::NegInf => 0,
        ScoreBound::PosInf => keys.len(),
    }
}

fn score_lower_predicate(score: f64, bound: ScoreBound) -> bool {
    match bound {
        ScoreBound::Inclusive(min) => score < min,
        ScoreBound::Exclusive(min) => score <= min,
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
    }
}

fn lex_matches_upper_bound(member: &[u8], bound: &LexBoundOwned) -> bool {
    match bound {
        LexBoundOwned::Inclusive(max) => member <= max.as_slice(),
        LexBoundOwned::Exclusive(max) => member < max.as_slice(),
        LexBoundOwned::Min => false,
        LexBoundOwned::Max => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom};

    fn member(index: usize) -> CompactString {
        CompactString::from(format!("m{index:08}"))
    }

    fn sorted_reference(tree: &BPTree) -> Vec<(f64, CompactString)> {
        tree.range_by_rank(0, tree.len().saturating_sub(1))
            .collect()
    }

    #[test]
    fn duplicate_member_updates_old_score() {
        let mut tree = BPTree::new();
        assert_eq!(
            tree.insert(1.0, CompactString::from("alpha")),
            InsertResult::Inserted
        );
        assert_eq!(
            tree.insert(2.0, CompactString::from("alpha")),
            InsertResult::Updated(1.0)
        );
        assert_eq!(tree.len(), 1);
        assert_eq!(tree.rank_of(2.0, b"alpha"), Some(0));
        assert!(tree.get_score(2.0, b"alpha"));
    }

    #[test]
    fn rank_matches_sorted_position() {
        let mut tree = BPTree::new();
        for i in 0..1000 {
            let score = (i % 17) as f64;
            let _ = tree.insert(score, member(i));
        }

        let entries = sorted_reference(&tree);
        for (rank, (score, member)) in entries.iter().enumerate() {
            assert_eq!(tree.rank_of(*score, member.as_bytes()), Some(rank as u64));
            assert_eq!(
                tree.rank_of_rev(*score, member.as_bytes()),
                Some(entries.len() as u64 - 1 - rank as u64)
            );
        }
    }

    #[test]
    fn range_by_rank_matches_reference_slice() {
        let mut tree = BPTree::new();
        for i in 0..256 {
            let _ = tree.insert((255 - i) as f64, member(i));
        }
        let entries = sorted_reference(&tree);
        let slice: Vec<_> = tree.range_by_rank(25, 60).collect();
        assert_eq!(slice, entries[25..=60]);
    }

    #[test]
    fn range_by_score_respects_bounds() {
        let mut tree = BPTree::new();
        for i in 0..40 {
            let _ = tree.insert(i as f64 / 10.0, member(i));
        }

        let inclusive: Vec<_> = tree
            .range_by_score(ScoreBound::Inclusive(1.0), ScoreBound::Inclusive(1.5))
            .collect();
        assert!(
            inclusive
                .iter()
                .all(|(score, _)| *score >= 1.0 && *score <= 1.5)
        );

        let exclusive: Vec<_> = tree
            .range_by_score(ScoreBound::Exclusive(1.0), ScoreBound::Exclusive(1.5))
            .collect();
        assert!(
            exclusive
                .iter()
                .all(|(score, _)| *score > 1.0 && *score < 1.5)
        );

        let all: Vec<_> = tree
            .range_by_score(ScoreBound::NegInf, ScoreBound::PosInf)
            .collect();
        assert_eq!(all.len(), tree.len() as usize);
    }

    #[test]
    fn range_by_lex_uses_member_order_for_equal_scores() {
        let mut tree = BPTree::new();
        for value in ["aaa", "bbb", "ccc", "ddd", "eee"] {
            let _ = tree.insert(7.0, CompactString::from(value));
        }
        let only_mid: Vec<_> = tree
            .range_by_lex(
                LexBound::Inclusive(b"bbb"),
                LexBound::Exclusive(b"eee"),
                7.0,
            )
            .collect();
        let members: Vec<_> = only_mid.into_iter().map(|(_, member)| member).collect();
        assert_eq!(
            members,
            vec![
                CompactString::from("bbb"),
                CompactString::from("ccc"),
                CompactString::from("ddd")
            ]
        );
    }

    #[test]
    fn pop_min_and_max_update_leaf_edges() {
        let mut tree = BPTree::new();
        for i in 0..40 {
            let _ = tree.insert(i as f64, member(i));
        }

        assert_eq!(tree.pop_min(), Some((0.0, member(0))));
        assert_eq!(tree.pop_max(), Some((39.0, member(39))));
        assert_eq!(tree.len(), 38);
        assert!(tree.leftmost.is_some());
        assert!(tree.rightmost.is_some());
        assert_eq!(tree.range_by_rank(0, 0).next(), Some((1.0, member(1))));
        assert_eq!(
            tree.range_by_rank(tree.len().saturating_sub(1), tree.len().saturating_sub(1))
                .next(),
            Some((38.0, member(38)))
        );
        assert_eq!(tree.rank_of(1.0, member(1).as_bytes()), Some(0));
    }

    #[test]
    fn grows_above_two_levels() {
        let mut tree = BPTree::new();
        for i in 0..500 {
            let _ = tree.insert(i as f64, member(i));
        }
        assert!(tree.height() >= 3);
        assert!(tree.node_count() > 1);
    }

    #[test]
    fn delete_rebuilds_and_preserves_order() {
        let mut tree = BPTree::new();
        for i in 0..200 {
            let _ = tree.insert(i as f64, member(i));
        }
        for i in (0..200).step_by(3) {
            assert_eq!(tree.remove(member(i).as_bytes()), Some(i as f64));
        }

        let entries = sorted_reference(&tree);
        assert!(entries.windows(2).all(|window| {
            compare_key(
                window[0].0,
                window[0].1.as_str(),
                window[1].0,
                window[1].1.as_str(),
            ) == Ordering::Less
        }));
    }

    #[test]
    fn random_insert_delete_stays_consistent() {
        let mut rng = SmallRng::seed_from_u64(7);
        let mut tree = BPTree::new();
        let mut refs = std::collections::BTreeMap::<(i64, CompactString), ()>::new();
        let mut members = Vec::new();

        for i in 0..10_000 {
            let score = rng.gen_range(0..500) as i64;
            let member = member(i);
            members.push(member.clone());
            let _ = tree.insert(score as f64, member.clone());
            refs.insert((score, member), ());
        }

        members.shuffle(&mut rng);
        for member in members.iter().take(3_000) {
            let _ = tree.remove(member.as_bytes());
        }

        let tree_entries: Vec<_> = tree
            .range_by_rank(0, tree.len().saturating_sub(1))
            .map(|(score, member)| (score as i64, member))
            .collect();
        let ref_entries: Vec<_> = refs
            .into_iter()
            .filter(|((_, member), ())| tree.member_scores.contains_key(member))
            .map(|((score, member), ())| (score, member))
            .collect();
        assert_eq!(tree_entries, ref_entries);
    }

    #[test]
    fn large_random_insert_remains_sorted() {
        let mut rng = SmallRng::seed_from_u64(11);
        let mut pairs: Vec<_> = (0..100_000)
            .map(|i| (rng.gen_range(0.0..10_000.0), member(i)))
            .collect();
        pairs.shuffle(&mut rng);

        let mut tree = BPTree::new();
        for (score, member) in &pairs {
            let _ = tree.insert(*score, member.clone());
        }

        let entries = sorted_reference(&tree);
        assert_eq!(entries.len(), 100_000);
        assert!(entries.windows(2).all(|window| {
            compare_key(
                window[0].0,
                window[0].1.as_str(),
                window[1].0,
                window[1].1.as_str(),
            ) != Ordering::Greater
        }));
    }
}
