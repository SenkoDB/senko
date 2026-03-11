use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    marker::PhantomData,
    ptr,
};

use compact_str::CompactString;
use smallvec::SmallVec;

use crate::SenkoError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };
    pub const AUTO: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };
    pub const PARTIAL_AUTO_SEQ: u64 = u64::MAX;

    pub fn auto_generate(last_id: StreamId, now_ms: u64) -> StreamId {
        if now_ms > last_id.ms {
            StreamId { ms: now_ms, seq: 0 }
        } else {
            StreamId {
                ms: last_id.ms.max(now_ms),
                seq: last_id.seq.saturating_add(1),
            }
        }
    }

    pub fn parse(s: &[u8]) -> Result<StreamId, SenkoError> {
        parse_with_default_seq(s, 0)
    }

    pub fn parse_range_start(s: &[u8]) -> Result<StreamId, SenkoError> {
        if s == b"-" {
            return Ok(StreamId::ZERO);
        }
        parse_with_default_seq(s, 0)
    }

    pub fn parse_range_end(s: &[u8]) -> Result<StreamId, SenkoError> {
        if s == b"+" {
            return Ok(StreamId::MAX);
        }
        parse_with_default_seq(s, u64::MAX)
    }

    pub fn to_string(&self) -> CompactString {
        CompactString::new(format!("{}-{}", self.ms, self.seq))
    }

    pub fn as_be_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.ms.to_be_bytes());
        out[8..].copy_from_slice(&self.seq.to_be_bytes());
        out
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms
            .cmp(&other.ms)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

fn parse_with_default_seq(s: &[u8], default_seq: u64) -> Result<StreamId, SenkoError> {
    if s == b"*" {
        return Ok(StreamId::AUTO);
    }

    let text = std::str::from_utf8(s).map_err(|_| SenkoError::Protocol("invalid stream id"))?;
    if text.is_empty() {
        return Err(SenkoError::Protocol("invalid stream id"));
    }

    if let Some((ms_str, seq_str)) = text.split_once('-') {
        let ms = parse_u64(ms_str)?;
        if seq_str == "*" {
            return Ok(StreamId {
                ms,
                seq: StreamId::PARTIAL_AUTO_SEQ,
            });
        }
        Ok(StreamId {
            ms,
            seq: parse_u64(seq_str)?,
        })
    } else {
        Ok(StreamId {
            ms: parse_u64(text)?,
            seq: default_seq,
        })
    }
}

fn parse_u64(s: &str) -> Result<u64, SenkoError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SenkoError::Protocol("invalid stream id"));
    }
    s.parse::<u64>()
        .map_err(|_| SenkoError::Protocol("invalid stream id"))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StreamRefMode {
    #[default]
    KeepRef,
    DelRef,
    Acked,
}

const STREAM_ITEM_FLAG_NONE: u16 = 0;
const STREAM_ITEM_FLAG_SAMEFIELDS: u16 = 1;
const STREAM_ITEM_FLAG_DELETED: u16 = 2;
const STREAM_ITEM_FLAG_REF_DEL: u16 = 4;
const STREAM_ITEM_FLAG_REF_ACKED: u16 = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
struct MacroEntry {
    id: StreamId,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    same_fields: bool,
    deleted: bool,
    ref_mode: StreamRefMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListpackMacroNode {
    pub data: Vec<u8>,
    pub count: u32,
    pub deleted: u32,
    pub first_id: StreamId,
    pub last_id: StreamId,
    pub max_deleted_id: StreamId,
    master_fields: Vec<Vec<u8>>,
    entries: Vec<MacroEntry>,
}

pub struct MacroNodeIter<'a> {
    node: &'a ListpackMacroNode,
    index: usize,
    include_deleted: bool,
}

impl ListpackMacroNode {
    pub fn new(first_entry_id: StreamId, fields: &[(&[u8], &[u8])]) -> ListpackMacroNode {
        Self::new_with_mode(first_entry_id, fields, StreamRefMode::KeepRef)
    }

    pub fn new_with_mode(
        first_entry_id: StreamId,
        fields: &[(&[u8], &[u8])],
        ref_mode: StreamRefMode,
    ) -> ListpackMacroNode {
        let master_fields = fields
            .iter()
            .map(|(field, _)| (*field).to_vec())
            .collect::<Vec<_>>();
        let first = MacroEntry {
            id: first_entry_id,
            fields: fields
                .iter()
                .map(|(field, value)| ((*field).to_vec(), (*value).to_vec()))
                .collect(),
            same_fields: true,
            deleted: false,
            ref_mode,
        };

        let mut node = ListpackMacroNode {
            data: Vec::new(),
            count: 1,
            deleted: 0,
            first_id: first_entry_id,
            last_id: first_entry_id,
            max_deleted_id: StreamId::ZERO,
            master_fields,
            entries: vec![first],
        };
        node.rebuild_data();
        node
    }

    pub fn append(&mut self, id: StreamId, fields: &[(&[u8], &[u8])]) -> bool {
        self.append_with_mode(id, fields, StreamRefMode::KeepRef)
    }

    pub fn append_with_mode(
        &mut self,
        id: StreamId,
        fields: &[(&[u8], &[u8])],
        ref_mode: StreamRefMode,
    ) -> bool {
        let same_fields = fields.len() == self.master_fields.len()
            && fields
                .iter()
                .zip(self.master_fields.iter())
                .all(|((field, _), master_field)| *field == master_field.as_slice());

        self.entries.push(MacroEntry {
            id,
            fields: fields
                .iter()
                .map(|(field, value)| ((*field).to_vec(), (*value).to_vec()))
                .collect(),
            same_fields,
            deleted: false,
            ref_mode,
        });
        self.count = self.count.saturating_add(1);
        if id < self.first_id {
            self.first_id = id;
        }
        if id > self.last_id {
            self.last_id = id;
        }
        self.rebuild_data();
        same_fields
    }

    pub fn soft_delete(&mut self, id: StreamId) -> bool {
        self.soft_delete_with_mode(id, StreamRefMode::KeepRef)
    }

    pub fn soft_delete_with_mode(&mut self, id: StreamId, ref_mode: StreamRefMode) -> bool {
        for entry in &mut self.entries {
            if entry.id == id {
                if entry.deleted {
                    return false;
                }
                entry.deleted = true;
                entry.ref_mode = ref_mode;
                self.deleted = self.deleted.saturating_add(1);
                self.count = self.count.saturating_sub(1);
                if id > self.max_deleted_id {
                    self.max_deleted_id = id;
                }
                self.rebuild_data();
                return true;
            }
        }
        false
    }

    pub fn get(&self, id: StreamId) -> Option<Vec<(&[u8], &[u8])>> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.id == id && !entry.deleted)?;
        Some(
            entry
                .fields
                .iter()
                .map(|(field, value)| (field.as_slice(), value.as_slice()))
                .collect(),
        )
    }

    pub fn get_ref_mode(&self, id: StreamId) -> Option<StreamRefMode> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.ref_mode)
    }

    pub fn set_ref_mode(&mut self, id: StreamId, ref_mode: StreamRefMode) -> bool {
        for entry in &mut self.entries {
            if entry.id == id {
                entry.ref_mode = ref_mode;
                self.rebuild_data();
                return true;
            }
        }
        false
    }

    pub fn iter(&self) -> MacroNodeIter<'_> {
        MacroNodeIter {
            node: self,
            index: 0,
            include_deleted: false,
        }
    }

    pub fn iter_including_deleted(&self) -> MacroNodeIter<'_> {
        MacroNodeIter {
            node: self,
            index: 0,
            include_deleted: true,
        }
    }

    pub fn byte_size(&self) -> usize {
        self.data.len()
    }

    pub fn is_full(&self, max_entries: usize, max_bytes: usize) -> bool {
        let max_entries = if max_entries == 0 { 100 } else { max_entries };
        let max_bytes = if max_bytes == 0 { 4096 } else { max_bytes };
        self.count as usize + self.deleted as usize >= max_entries || self.byte_size() >= max_bytes
    }

    pub fn total_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn hard_trim_oldest_entries(&mut self, mut to_remove: usize) -> usize {
        if to_remove == 0 || self.entries.is_empty() {
            return 0;
        }
        let removed = to_remove.min(self.entries.len());
        while to_remove > 0 && !self.entries.is_empty() {
            let entry = self.entries.remove(0);
            if entry.deleted {
                self.deleted = self.deleted.saturating_sub(1);
            } else {
                self.count = self.count.saturating_sub(1);
            }
            to_remove -= 1;
        }
        self.recompute_bounds();
        self.rebuild_data();
        removed
    }

    pub fn hard_trim_while_id_lt(&mut self, threshold: StreamId, limit: usize) -> usize {
        let mut removed = 0usize;
        while !self.entries.is_empty() {
            if limit > 0 && removed >= limit {
                break;
            }
            if self.entries[0].id >= threshold {
                break;
            }
            let entry = self.entries.remove(0);
            if entry.deleted {
                self.deleted = self.deleted.saturating_sub(1);
            } else {
                self.count = self.count.saturating_sub(1);
            }
            removed += 1;
        }
        if removed > 0 {
            self.recompute_bounds();
            self.rebuild_data();
        }
        removed
    }

    fn recompute_bounds(&mut self) {
        self.first_id = self
            .entries
            .first()
            .map(|entry| entry.id)
            .unwrap_or(StreamId::ZERO);
        self.last_id = self
            .entries
            .last()
            .map(|entry| entry.id)
            .unwrap_or(StreamId::ZERO);
    }

    fn rebuild_data(&mut self) {
        let mut data = Vec::new();
        encode_u16(&mut data, STREAM_ITEM_FLAG_NONE);
        encode_u64(&mut data, self.master_fields.len() as u64);
        for field in &self.master_fields {
            encode_bytes(&mut data, field);
        }
        data.push(0);

        let master = self
            .entries
            .first()
            .map(|entry| entry.id)
            .unwrap_or(StreamId::ZERO);
        for entry in self.entries.iter().skip(1) {
            let mut flags = if entry.same_fields {
                STREAM_ITEM_FLAG_SAMEFIELDS
            } else {
                STREAM_ITEM_FLAG_NONE
            };
            if entry.deleted {
                flags |= STREAM_ITEM_FLAG_DELETED;
            }
            match entry.ref_mode {
                StreamRefMode::KeepRef => {}
                StreamRefMode::DelRef => flags |= STREAM_ITEM_FLAG_REF_DEL,
                StreamRefMode::Acked => flags |= STREAM_ITEM_FLAG_REF_ACKED,
            }
            encode_u16(&mut data, flags);
            encode_u64(&mut data, entry.id.ms.saturating_sub(master.ms));
            encode_u64(&mut data, entry.id.seq.saturating_sub(master.seq));
            if entry.same_fields {
                for (_, value) in &entry.fields {
                    encode_bytes(&mut data, value);
                }
            } else {
                encode_u64(&mut data, entry.fields.len() as u64);
                for (field, value) in &entry.fields {
                    encode_bytes(&mut data, field);
                    encode_bytes(&mut data, value);
                }
            }
            data.push(1);
        }
        self.data = data;
    }
}

impl<'a> Iterator for MacroNodeIter<'a> {
    type Item = (StreamId, Vec<(&'a [u8], &'a [u8])>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.node.entries.len() {
            let entry = &self.node.entries[self.index];
            self.index += 1;
            if !self.include_deleted && entry.deleted {
                continue;
            }
            return Some((
                entry.id,
                entry
                    .fields
                    .iter()
                    .map(|(field, value)| (field.as_slice(), value.as_slice()))
                    .collect(),
            ));
        }
        None
    }
}

fn encode_u16(dst: &mut Vec<u8>, value: u16) {
    dst.extend_from_slice(&value.to_le_bytes());
}

fn encode_u64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

fn encode_bytes(dst: &mut Vec<u8>, value: &[u8]) {
    encode_u64(dst, value.len() as u64);
    dst.extend_from_slice(value);
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug)]
pub struct RadixNode {
    pub is_key: bool,
    pub is_compressed: bool,
    pub children_count: u8,
    pub key_fragment: SmallVec<[u8; 16]>,
    pub children: SmallVec<[*mut RadixNode; 4]>,
    pub value: Option<Box<ListpackMacroNode>>,
}

pub type StreamFieldPairOwned = (Vec<u8>, Vec<u8>);
pub type StreamOwnedEntry = (StreamId, Vec<StreamFieldPairOwned>);
pub type StreamFieldPairBorrowed<'a> = (&'a [u8], &'a [u8]);
pub type StreamBorrowedEntry<'a> = (StreamId, Vec<StreamFieldPairBorrowed<'a>>);

pub struct StreamRangeIter<'a> {
    items: Vec<StreamOwnedEntry>,
    index: usize,
    _marker: PhantomData<&'a ()>,
}

#[derive(Debug, Default)]
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

impl Clone for StreamRadixTree {
    fn clone(&self) -> Self {
        Self {
            root: ptr::null_mut(),
            arena: NodeArena::default(),
            len: self.len,
            total_len: self.total_len,
            last_id: self.last_id,
            first_entry_id: self.first_entry_id,
            max_deleted_entry_id: self.max_deleted_entry_id,
            entries_added: self.entries_added,
            nodes: self.nodes.clone(),
        }
    }
}

impl PartialEq for StreamRadixTree {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
            && self.total_len == other.total_len
            && self.last_id == other.last_id
            && self.first_entry_id == other.first_entry_id
            && self.max_deleted_entry_id == other.max_deleted_entry_id
            && self.entries_added == other.entries_added
            && self.nodes == other.nodes
    }
}

impl Eq for StreamRadixTree {}

impl StreamRadixTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, id: StreamId, fields: &[(&[u8], &[u8])]) -> Result<(), SenkoError> {
        self.insert_with_mode(id, fields, StreamRefMode::KeepRef)
    }

    pub fn insert_with_mode(
        &mut self,
        id: StreamId,
        fields: &[(&[u8], &[u8])],
        ref_mode: StreamRefMode,
    ) -> Result<(), SenkoError> {
        if self.get(id).is_some() {
            return Err(SenkoError::Protocol("ERR duplicate stream ID"));
        }

        let mut appended = false;
        if let Some((_, tail)) = self.nodes.iter_mut().next_back()
            && !tail.is_full(100, 4096)
        {
            tail.append_with_mode(id, fields, ref_mode);
            appended = true;
        }

        if !appended {
            self.nodes
                .insert(id, ListpackMacroNode::new_with_mode(id, fields, ref_mode));
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
        self.delete_with_mode(id, StreamRefMode::KeepRef)
    }

    pub fn delete_with_mode(&mut self, id: StreamId, ref_mode: StreamRefMode) -> bool {
        let node_key = self.nodes.iter().find_map(|(key, node)| {
            if id >= node.first_id && id <= node.last_id {
                Some(*key)
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
        if node.soft_delete_with_mode(id, ref_mode) {
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
        self.nodes.values().find_map(|node| {
            if id < node.first_id || id > node.last_id {
                None
            } else {
                node.get(id)
            }
        })
    }

    pub fn get_ref_mode(&self, id: StreamId) -> Option<StreamRefMode> {
        self.nodes.values().find_map(|node| {
            if id < node.first_id || id > node.last_id {
                None
            } else {
                node.get_ref_mode(id)
            }
        })
    }

    pub fn set_ref_mode(&mut self, id: StreamId, ref_mode: StreamRefMode) -> bool {
        for node in self.nodes.values_mut() {
            if id < node.first_id || id > node.last_id {
                continue;
            }
            if node.set_ref_mode(id, ref_mode) {
                return true;
            }
        }
        false
    }

    pub fn macro_node_count(&self) -> usize {
        self.nodes.len()
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
                        .map(|(field, value)| (field.to_vec(), value.to_vec()))
                        .collect(),
                ));
                if items.len() >= max {
                    break;
                }
            }
            if items.len() >= max {
                break;
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
                        .map(|(field, value)| (field.to_vec(), value.to_vec()))
                        .collect(),
                ));
                if items.len() >= max {
                    break;
                }
            }
            if items.len() >= max {
                break;
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
                evicted += self.remove_whole_node(node_key);
                continue;
            }

            if node_total <= excess {
                if room == 0 || node_total > room {
                    break;
                }
                evicted += self.remove_whole_node(node_key);
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
            evicted += self.remove_whole_node(node_key);
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
        self.nodes.values().find_map(|node| node.iter().next())
    }

    pub fn last_entry(&self) -> Option<StreamBorrowedEntry<'_>> {
        for node in self.nodes.values().rev() {
            let mut last = None;
            for entry in node.iter() {
                last = Some(entry);
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
            remove_node = node.total_entries() == 0;
        }
        if remove_node {
            self.nodes.remove(&node_key);
        }
        self.total_len = self.total_len.saturating_sub(removed as u64);
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
            remove_node = node.total_entries() == 0;
        }
        if remove_node {
            self.nodes.remove(&node_key);
        }
        self.total_len = self.total_len.saturating_sub(removed as u64);
        self.recompute_len_from_nodes();
        removed
    }

    fn recompute_len_from_nodes(&mut self) {
        self.len = self.nodes.values().map(|node| node.count as u64).sum();
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PelEntry {
    pub id: StreamId,
    pub consumer: CompactString,
    pub delivery_time: u64,
    pub delivery_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerState {
    pub name: CompactString,
    pub seen_time: u64,
    pub active_time: u64,
    pub pel: BTreeMap<StreamId, PelEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroup {
    pub name: CompactString,
    pub last_delivered_id: StreamId,
    pub entries_read: u64,
    pub pel_count: u64,
    pub consumers: HashMap<CompactString, ConsumerState>,
    pub global_pel: BTreeMap<StreamId, CompactString>,
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        Self {
            name: CompactString::default(),
            last_delivered_id: StreamId::ZERO,
            entries_read: 0,
            pel_count: 0,
            consumers: HashMap::new(),
            global_pel: BTreeMap::new(),
        }
    }
}

impl ConsumerGroup {
    pub fn new(name: CompactString, last_delivered_id: StreamId, entries_read: u64) -> Self {
        Self {
            name,
            last_delivered_id,
            entries_read,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamObject {
    pub tree: StreamRadixTree,
    pub groups: HashMap<CompactString, ConsumerGroup>,
}

impl StreamObject {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry_ref_mode(&self, id: StreamId) -> Option<StreamRefMode> {
        self.tree.get_ref_mode(id)
    }
}

#[cfg(test)]
mod tests {
    use super::{ListpackMacroNode, StreamId, StreamRadixTree, StreamRefMode};

    #[test]
    fn stream_id_parsing_variants() {
        assert_eq!(StreamId::parse(b"*").unwrap(), StreamId::AUTO);
        assert_eq!(
            StreamId::parse(b"1234567890-0").unwrap(),
            StreamId {
                ms: 1234567890,
                seq: 0
            }
        );
        assert_eq!(StreamId::parse_range_start(b"-").unwrap(), StreamId::ZERO);
        assert_eq!(StreamId::parse_range_end(b"+").unwrap(), StreamId::MAX);
    }

    #[test]
    fn stream_id_ordering() {
        assert!(StreamId { ms: 1, seq: 0 } < StreamId { ms: 1, seq: 1 });
        assert!(StreamId { ms: 1, seq: 1 } < StreamId { ms: 2, seq: 0 });
    }

    #[test]
    fn auto_generation_handles_clock_skew() {
        let last = StreamId { ms: 10, seq: 7 };
        assert_eq!(
            StreamId::auto_generate(last, 12),
            StreamId { ms: 12, seq: 0 }
        );
        assert_eq!(
            StreamId::auto_generate(last, 10),
            StreamId { ms: 10, seq: 8 }
        );
        assert_eq!(
            StreamId::auto_generate(last, 9),
            StreamId { ms: 10, seq: 8 }
        );
    }

    #[test]
    fn macro_node_same_fields_and_delete() {
        let mut node = ListpackMacroNode::new(
            StreamId { ms: 1, seq: 0 },
            &[(b"f".as_slice(), b"a".as_slice())],
        );
        assert!(node.append(
            StreamId { ms: 1, seq: 1 },
            &[(b"f".as_slice(), b"b".as_slice())]
        ));
        assert!(!node.append(
            StreamId { ms: 1, seq: 2 },
            &[(b"g".as_slice(), b"c".as_slice())]
        ));
        assert!(node.soft_delete(StreamId { ms: 1, seq: 1 }));
        assert!(node.get(StreamId { ms: 1, seq: 1 }).is_none());
        let ids = node.iter().map(|(id, _)| id).collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![StreamId { ms: 1, seq: 0 }, StreamId { ms: 1, seq: 2 }]
        );
    }

    #[test]
    fn radix_range_and_ref_mode() {
        let mut tree = StreamRadixTree::new();
        for i in 0..10_000u64 {
            let id = StreamId {
                ms: i / 100,
                seq: i % 100,
            };
            let value = i.to_string().into_bytes();
            let payload = [(b"f".as_slice(), value.as_slice())];
            tree.insert_with_mode(id, &payload, StreamRefMode::Acked)
                .unwrap();
        }
        let got = tree
            .range(StreamId::ZERO, StreamId::MAX, None)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        assert_eq!(got.len(), 10_000);
        assert_eq!(
            tree.get_ref_mode(StreamId { ms: 0, seq: 0 }),
            Some(StreamRefMode::Acked)
        );
    }
}
