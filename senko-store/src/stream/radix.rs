use std::{collections::BTreeMap, marker::PhantomData, ptr};

use smallvec::SmallVec;

use crate::stream::{id::StreamId, macro_node::ListpackMacroNode};
use senko_core::SenkoError;

type StreamOwnedEntry = (StreamId, Vec<(Vec<u8>, Vec<u8>)>);
type StreamBorrowedEntry<'a> = (StreamId, Vec<(&'a [u8], &'a [u8])>);

pub struct NodeArena<T> {
    _phantom: PhantomData<T>,
}

impl<T> NodeArena<T> {
    pub fn clear(&mut self) {}
}

impl<T> Default for NodeArena<T> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

pub struct RadixNode {
    pub is_key: bool,
    pub is_compressed: bool,
    pub children_count: u8,
    pub key_fragment: SmallVec<[u8; 16]>,
    pub children: SmallVec<[*mut RadixNode; 4]>,
    pub value: Option<Box<ListpackMacroNode>>,
}

pub struct StreamRangeIter<'a> {
    items: Vec<StreamOwnedEntry>,
    index: usize,
    _marker: PhantomData<&'a ()>,
}

#[derive(Default)]
pub struct StreamRadixTree {
    pub root: *mut RadixNode,
    pub arena: NodeArena<RadixNode>,
    pub len: u64,
    pub total_len: u64,
    pub last_id: StreamId,
    pub first_entry_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    nodes: BTreeMap<StreamId, ListpackMacroNode>,
}

impl StreamRadixTree {
    pub fn new() -> Self {
        Self {
            root: ptr::null_mut(),
            arena: NodeArena::default(),
            len: 0,
            total_len: 0,
            last_id: StreamId::ZERO,
            first_entry_id: StreamId::ZERO,
            max_deleted_entry_id: StreamId::ZERO,
            entries_added: 0,
            nodes: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, id: StreamId, fields: &[(&[u8], &[u8])]) -> Result<(), SenkoError> {
        if self.get(id).is_some() {
            return Err(SenkoError::Protocol("ERR duplicate stream ID"));
        }

        let mut appended = false;
        if let Some((_, tail)) = self.nodes.iter_mut().next_back()
            && !tail.is_full(100, 4096)
        {
            tail.append(id, fields);
            appended = true;
        }

        if !appended {
            let node = ListpackMacroNode::new(id, fields);
            self.nodes.insert(id, node);
        }

        self.len = self.len.saturating_add(1);
        self.total_len = self.total_len.saturating_add(1);
        self.entries_added = self.entries_added.saturating_add(1);
        if self.last_id < id {
            self.last_id = id;
        }
        if self.first_entry_id == StreamId::ZERO || id < self.first_entry_id {
            self.first_entry_id = id;
        }
        Ok(())
    }

    pub fn delete(&mut self, id: StreamId) -> bool {
        let node_key = self.nodes.iter().find_map(|(k, node)| {
            if id >= node.first_id && id <= node.last_id {
                Some(*k)
            } else {
                None
            }
        });
        let Some(node_key) = node_key else {
            return false;
        };
        let Some(node) = self.nodes.get_mut(&node_key) else {
            return false;
        };

        if node.soft_delete(id) {
            self.len = self.len.saturating_sub(1);
            if id > self.max_deleted_entry_id {
                self.max_deleted_entry_id = id;
            }
            true
        } else {
            false
        }
    }

    pub fn get(&self, id: StreamId) -> Option<Vec<(&[u8], &[u8])>> {
        for node in self.nodes.values() {
            if id < node.first_id || id > node.last_id {
                continue;
            }
            if let Some(fields) = node.get(id) {
                return Some(fields);
            }
        }
        None
    }

    pub fn range(
        &self,
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> StreamRangeIter<'_> {
        let mut items = Vec::new();
        let max = count.unwrap_or(usize::MAX);
        for node in self.nodes.values() {
            if node.last_id < start || node.first_id > end {
                continue;
            }
            for (id, fields) in node.iter() {
                if id < start || id > end {
                    continue;
                }
                items.push((
                    id,
                    fields
                        .into_iter()
                        .map(|(f, v)| (f.to_vec(), v.to_vec()))
                        .collect::<Vec<_>>(),
                ));
                if items.len() >= max {
                    return StreamRangeIter {
                        items,
                        index: 0,
                        _marker: PhantomData,
                    };
                }
            }
        }
        StreamRangeIter {
            items,
            index: 0,
            _marker: PhantomData,
        }
    }

    pub fn range_rev(
        &self,
        end: StreamId,
        start: StreamId,
        count: Option<usize>,
    ) -> StreamRangeIter<'_> {
        let mut items = Vec::new();
        let max = count.unwrap_or(usize::MAX);
        for node in self.nodes.values().rev() {
            if node.last_id < start || node.first_id > end {
                continue;
            }
            let mut node_items = node.iter().collect::<Vec<_>>();
            node_items.reverse();
            for (id, fields) in node_items {
                if id < start || id > end {
                    continue;
                }
                items.push((
                    id,
                    fields
                        .into_iter()
                        .map(|(f, v)| (f.to_vec(), v.to_vec()))
                        .collect::<Vec<_>>(),
                ));
                if items.len() >= max {
                    return StreamRangeIter {
                        items,
                        index: 0,
                        _marker: PhantomData,
                    };
                }
            }
        }
        StreamRangeIter {
            items,
            index: 0,
            _marker: PhantomData,
        }
    }

    pub fn trim_by_maxlen(&mut self, maxlen: u64, approx: bool, limit: usize) {
        let mut evicted = 0usize;
        while self.total_len > maxlen {
            if limit > 0 && evicted >= limit {
                break;
            }
            let Some((&node_key, node)) = self.nodes.first_key_value() else {
                break;
            };
            let node_total = node.total_entries();
            if node_total == 0 {
                self.nodes.remove(&node_key);
                continue;
            }

            let excess = (self.total_len - maxlen) as usize;
            let room = if limit == 0 {
                usize::MAX
            } else {
                limit.saturating_sub(evicted)
            };

            if approx {
                if self.total_len.saturating_sub(node_total as u64) < maxlen {
                    break;
                }
                if room == 0 || node_total > room {
                    break;
                }
                let removed = self.remove_whole_node(node_key);
                evicted += removed;
                continue;
            }

            if node_total <= excess {
                if room == 0 {
                    break;
                }
                if node_total > room {
                    break;
                }
                let removed = self.remove_whole_node(node_key);
                evicted += removed;
                continue;
            }

            let to_trim = excess.min(room);
            if to_trim == 0 {
                break;
            }
            let removed = self.trim_from_oldest_node(node_key, to_trim);
            if removed == 0 {
                break;
            }
            evicted += removed;
        }
        self.refresh_edge_ids();
    }

    pub fn trim_by_minid(&mut self, min_id: StreamId, approx: bool, limit: usize) {
        let mut evicted = 0usize;

        loop {
            if limit > 0 && evicted >= limit {
                break;
            }
            let Some((&node_key, node)) = self.nodes.first_key_value() else {
                break;
            };
            if node.last_id >= min_id {
                break;
            }

            let room = if limit == 0 {
                usize::MAX
            } else {
                limit.saturating_sub(evicted)
            };
            let node_total = node.total_entries();
            if node_total > room {
                break;
            }

            let removed = self.remove_whole_node(node_key);
            evicted += removed;
        }

        if !approx
            && let Some((&node_key, _)) = self.nodes.first_key_value()
            && (limit == 0 || evicted < limit)
        {
            let room = if limit == 0 {
                usize::MAX
            } else {
                limit - evicted
            };
            self.trim_id_lt_from_node(node_key, min_id, room);
        }

        self.refresh_edge_ids();
    }

    pub fn first_entry(&self) -> Option<StreamBorrowedEntry<'_>> {
        for node in self.nodes.values() {
            if let Some(item) = node.iter().next() {
                return Some(item);
            }
        }
        None
    }

    pub fn last_entry(&self) -> Option<StreamBorrowedEntry<'_>> {
        for node in self.nodes.values().rev() {
            let mut last = None;
            for item in node.iter() {
                last = Some(item);
            }
            if last.is_some() {
                return last;
            }
        }
        None
    }

    fn remove_whole_node(&mut self, node_key: StreamId) -> usize {
        let Some(node) = self.nodes.remove(&node_key) else {
            return 0;
        };
        let removed_total = node.total_entries();
        self.total_len = self.total_len.saturating_sub(removed_total as u64);
        self.len = self.len.saturating_sub(node.count as u64);
        removed_total
    }

    fn trim_from_oldest_node(&mut self, node_key: StreamId, to_trim: usize) -> usize {
        let mut removed = 0usize;
        let mut remove_node = false;

        if let Some(node) = self.nodes.get_mut(&node_key) {
            removed = node.hard_trim_oldest_entries(to_trim);
            if node.total_entries() == 0 {
                remove_node = true;
            }
        }

        if remove_node {
            self.nodes.remove(&node_key);
        }

        self.total_len = self.total_len.saturating_sub(removed as u64);
        let live_removed = removed_live_estimate(removed, self.len, self.total_len);
        self.len = self.len.saturating_sub(live_removed);
        self.recompute_len_from_nodes();
        removed
    }

    fn trim_id_lt_from_node(
        &mut self,
        node_key: StreamId,
        min_id: StreamId,
        limit: usize,
    ) -> usize {
        if limit == 0 {
            return 0;
        }

        let mut removed = 0usize;
        let mut remove_node = false;

        if let Some(node) = self.nodes.get_mut(&node_key) {
            removed = node.hard_trim_while_id_lt(min_id, limit);
            if node.total_entries() == 0 {
                remove_node = true;
            }
        }

        if remove_node {
            self.nodes.remove(&node_key);
        }

        self.total_len = self.total_len.saturating_sub(removed as u64);
        self.recompute_len_from_nodes();
        removed
    }

    fn recompute_len_from_nodes(&mut self) {
        self.len = self.nodes.values().map(|n| n.count as u64).sum();
    }

    fn refresh_edge_ids(&mut self) {
        self.first_entry_id = self
            .first_entry()
            .map(|(id, _)| id)
            .unwrap_or(StreamId::ZERO);
        self.last_id = self
            .last_entry()
            .map(|(id, _)| id)
            .unwrap_or(StreamId::ZERO);
    }
}

impl Drop for StreamRadixTree {
    fn drop(&mut self) {
        self.nodes.clear();
        self.arena.clear();
        self.root = ptr::null_mut();
    }
}

impl<'a> Iterator for StreamRangeIter<'a> {
    type Item = (StreamId, Vec<(Vec<u8>, Vec<u8>)>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.items.len() {
            return None;
        }
        let item = self.items[self.index].clone();
        self.index += 1;
        Some(item)
    }
}

fn removed_live_estimate(_removed: usize, len: u64, _total_len: u64) -> u64 {
    len
}

#[cfg(test)]
mod tests {
    use crate::stream::{id::StreamId, radix::StreamRadixTree};

    fn fields(v: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![(b"f".to_vec(), v.to_string().as_bytes().to_vec())]
    }

    #[test]
    fn insert_and_range_10k_matches_sorted_reference() {
        let mut tree = StreamRadixTree::new();
        let mut expected = Vec::new();

        for i in 0..10_000u64 {
            let id = StreamId {
                ms: i / 100,
                seq: i % 100,
            };
            let payload = fields(i);
            let refs = payload
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect::<Vec<_>>();
            tree.insert(id, &refs).unwrap();
            expected.push(id);
        }

        let got = tree
            .range(StreamId::ZERO, StreamId::MAX, None)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        assert_eq!(got, expected);
    }

    #[test]
    fn trim_maxlen_exact_vs_approx() {
        let mut exact = StreamRadixTree::new();
        let mut approx = StreamRadixTree::new();

        for i in 0..250u64 {
            let id = StreamId { ms: i, seq: 0 };
            let payload = fields(i);
            let refs = payload
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect::<Vec<_>>();
            exact.insert(id, &refs).unwrap();
            approx.insert(id, &refs).unwrap();
        }

        exact.trim_by_maxlen(123, false, 0);
        approx.trim_by_maxlen(123, true, 0);

        assert_eq!(exact.total_len, 123);
        assert!(approx.total_len >= 123);
    }

    #[test]
    fn trim_minid_removes_older_entries() {
        let mut tree = StreamRadixTree::new();

        for i in 0..50u64 {
            let id = StreamId { ms: i, seq: 0 };
            let payload = fields(i);
            let refs = payload
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect::<Vec<_>>();
            tree.insert(id, &refs).unwrap();
        }

        tree.trim_by_minid(StreamId { ms: 20, seq: 0 }, false, 0);

        assert!(tree.get(StreamId { ms: 19, seq: 0 }).is_none());
        assert!(tree.get(StreamId { ms: 20, seq: 0 }).is_some());
    }

    #[test]
    fn entries_added_never_decrements() {
        let mut tree = StreamRadixTree::new();
        for i in 0..20u64 {
            let id = StreamId { ms: i, seq: 0 };
            let payload = fields(i);
            let refs = payload
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect::<Vec<_>>();
            tree.insert(id, &refs).unwrap();
        }
        let added = tree.entries_added;

        tree.delete(StreamId { ms: 1, seq: 0 });
        tree.trim_by_maxlen(5, false, 0);

        assert_eq!(tree.entries_added, added);
    }
}
