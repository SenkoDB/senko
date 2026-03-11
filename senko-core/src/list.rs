use std::{fmt, marker::PhantomData, ptr};

use memchr::memmem;
use smallvec::SmallVec;

const DEFAULT_FILL: i32 = 128;

#[repr(C)]
pub struct ListpackNode {
    pub data: Vec<u8>,
    pub count: u16,
    pub prev: *mut ListpackNode,
    pub next: *mut ListpackNode,
}

impl Default for ListpackNode {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            count: 0,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

impl fmt::Debug for ListpackNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListpackNode")
            .field("count", &self.count)
            .field("byte_size", &self.data.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertResult {
    Found,
    NotFound,
    KeyMissing,
}

pub struct ListpackIter<'a> {
    node: &'a ListpackNode,
    offset: usize,
    yielded: usize,
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.yielded >= self.node.count as usize {
            return None;
        }
        let (payload, next_offset) = decode_entry_at(&self.node.data, self.offset)?;
        self.offset = next_offset;
        self.yielded += 1;
        Some(payload)
    }
}

pub struct QuickListRangeIter<'a> {
    list: &'a QuickList,
    next_index: u64,
    stop_index: u64,
}

impl<'a> Iterator for QuickListRangeIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index > self.stop_index {
            return None;
        }
        let current = self.next_index;
        self.next_index = self.next_index.saturating_add(1);
        self.list.index(current as i64)
    }
}

pub struct QuickListIter<'a> {
    current: *const ListpackNode,
    current_iter: Option<ListpackIter<'a>>,
    marker: PhantomData<&'a QuickList>,
}

impl<'a> Iterator for QuickListIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.current_iter.as_mut()
                && let Some(item) = iter.next()
            {
                return Some(item);
            }
            if self.current.is_null() {
                return None;
            }
            let node = unsafe { &*self.current };
            self.current = node.next;
            self.current_iter = Some(lp_iter(node));
        }
    }
}

pub struct QuickList {
    pub head: *mut ListpackNode,
    pub tail: *mut ListpackNode,
    pub node_count: u32,
    pub len: u64,
    pub fill: i32,
}

impl Default for QuickList {
    fn default() -> Self {
        Self::new(DEFAULT_FILL)
    }
}

impl Clone for QuickList {
    fn clone(&self) -> Self {
        let mut cloned = Self::new(self.fill);
        for value in self.iter() {
            cloned.push_back(value);
        }
        cloned
    }
}

impl PartialEq for QuickList {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl fmt::Debug for QuickList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuickList")
            .field("len", &self.len)
            .field("node_count", &self.node_count)
            .field("fill", &self.fill)
            .finish()
    }
}

impl QuickList {
    pub fn new(fill: i32) -> Self {
        Self {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            node_count: 0,
            len: 0,
            fill: fill.max(1),
        }
    }

    pub fn push_front(&mut self, element: &[u8]) {
        if self.head.is_null() {
            let node = Box::into_raw(Box::new(ListpackNode::default()));
            self.head = node;
            self.tail = node;
            self.node_count = 1;
        }
        unsafe {
            lp_push_front(&mut *self.head, element);
            self.len = self.len.saturating_add(1);
            self.split_if_needed(self.head);
        }
    }

    pub fn push_back(&mut self, element: &[u8]) {
        if self.tail.is_null() {
            let node = Box::into_raw(Box::new(ListpackNode::default()));
            self.head = node;
            self.tail = node;
            self.node_count = 1;
        }
        unsafe {
            lp_push_back(&mut *self.tail, element);
            self.len = self.len.saturating_add(1);
            self.split_if_needed(self.tail);
        }
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        let head = self.head;
        if head.is_null() {
            return None;
        }
        let value = unsafe { lp_pop_front(&mut *head) }?;
        self.len = self.len.saturating_sub(1);
        unsafe {
            self.cleanup_after_mutation(head);
        }
        Some(value)
    }

    pub fn pop_back(&mut self) -> Option<Vec<u8>> {
        let tail = self.tail;
        if tail.is_null() {
            return None;
        }
        let value = unsafe { lp_pop_back(&mut *tail) }?;
        self.len = self.len.saturating_sub(1);
        unsafe {
            self.cleanup_after_mutation(tail);
        }
        Some(value)
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn index(&self, index: i64) -> Option<&[u8]> {
        let normalized = self.normalize_index(index)?;
        let mut traversed = 0u64;
        if normalized < self.len / 2 {
            let mut node = self.head;
            while !node.is_null() {
                let count = unsafe { (*node).count as u64 };
                if normalized < traversed + count {
                    return unsafe { lp_get(&*node, (normalized - traversed) as usize) };
                }
                traversed += count;
                node = unsafe { (*node).next };
            }
        } else {
            let mut node = self.tail;
            traversed = self.len;
            while !node.is_null() {
                let count = unsafe { (*node).count as u64 };
                traversed = traversed.saturating_sub(count);
                if normalized >= traversed {
                    return unsafe { lp_get(&*node, (normalized - traversed) as usize) };
                }
                node = unsafe { (*node).prev };
            }
        }
        None
    }

    pub fn set_index(&mut self, index: i64, element: &[u8]) -> bool {
        let normalized = match self.normalize_index(index) {
            Some(index) => index,
            None => return false,
        };
        let mut traversed = 0u64;
        let mut node = self.head;
        while !node.is_null() {
            let count = unsafe { (*node).count as u64 };
            if normalized < traversed + count {
                unsafe {
                    lp_set(&mut *node, (normalized - traversed) as usize, element);
                    self.split_if_needed(node);
                    self.cleanup_after_mutation(node);
                }
                return true;
            }
            traversed += count;
            node = unsafe { (*node).next };
        }
        false
    }

    pub fn insert_before(&mut self, pivot: &[u8], element: &[u8]) -> InsertResult {
        if self.head.is_null() {
            return InsertResult::KeyMissing;
        }
        let mut node = self.head;
        while !node.is_null() {
            let index = unsafe { lp_find(&*node, pivot, 0) };
            if let Some(index) = index {
                unsafe {
                    lp_insert_before(&mut *node, index, element);
                    self.len = self.len.saturating_add(1);
                    self.split_if_needed(node);
                }
                return InsertResult::Found;
            }
            node = unsafe { (*node).next };
        }
        InsertResult::NotFound
    }

    pub fn insert_after(&mut self, pivot: &[u8], element: &[u8]) -> InsertResult {
        if self.head.is_null() {
            return InsertResult::KeyMissing;
        }
        let mut node = self.head;
        while !node.is_null() {
            let index = unsafe { lp_find(&*node, pivot, 0) };
            if let Some(index) = index {
                unsafe {
                    lp_insert_after(&mut *node, index, element);
                    self.len = self.len.saturating_add(1);
                    self.split_if_needed(node);
                }
                return InsertResult::Found;
            }
            node = unsafe { (*node).next };
        }
        InsertResult::NotFound
    }

    pub fn remove(&mut self, count: i64, element: &[u8]) -> u64 {
        if self.head.is_null() {
            return 0;
        }

        let forward = count >= 0;
        let mut remaining = if count == 0 { i64::MAX } else { count.abs() };
        let mut removed = 0u64;
        let mut node = if forward { self.head } else { self.tail };

        while !node.is_null() && remaining > 0 {
            let next = unsafe { if forward { (*node).next } else { (*node).prev } };
            let mut entries = unsafe { decode_all_entries(&*node) };
            let mut removed_here = 0usize;

            if forward {
                let mut index = 0usize;
                while index < entries.len() && remaining > 0 {
                    if entries[index].as_slice() == element {
                        entries.remove(index);
                        removed_here += 1;
                        remaining -= 1;
                    } else {
                        index += 1;
                    }
                }
            } else {
                let mut index = entries.len();
                while index > 0 && remaining > 0 {
                    index -= 1;
                    if entries[index].as_slice() == element {
                        entries.remove(index);
                        removed_here += 1;
                        remaining -= 1;
                    }
                }
            }

            if removed_here > 0 {
                unsafe {
                    encode_node_entries(&mut *node, &entries);
                }
                self.len = self.len.saturating_sub(removed_here as u64);
                removed += removed_here as u64;
            }

            node = next;
        }

        if removed > 0 {
            self.compress_empty_and_merge();
        }

        removed
    }

    pub fn trim(&mut self, start: i64, stop: i64) {
        let Some((start, stop)) = self.normalize_range(start, stop) else {
            self.clear();
            return;
        };

        let mut kept = Vec::with_capacity((stop - start + 1) as usize);
        for index in start..=stop {
            if let Some(value) = self.index(index as i64) {
                kept.push(value.to_vec());
            }
        }

        self.clear();
        for value in kept {
            self.push_back(&value);
        }
    }

    pub fn range(&self, start: i64, stop: i64) -> QuickListRangeIter<'_> {
        if let Some((start, stop)) = self.normalize_range(start, stop) {
            return QuickListRangeIter {
                list: self,
                next_index: start,
                stop_index: stop,
            };
        }
        QuickListRangeIter {
            list: self,
            next_index: 1,
            stop_index: 0,
        }
    }

    pub fn pos(
        &self,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: usize,
    ) -> SmallVec<[i64; 4]> {
        let mut out = SmallVec::<[i64; 4]>::new();
        if self.is_empty() || rank == 0 {
            return out;
        }

        let target_rank = rank.unsigned_abs() as usize;
        let scan_limit = if maxlen == 0 {
            self.len as usize
        } else {
            maxlen.min(self.len as usize)
        };
        let mut matches_seen = 0usize;

        if rank > 0 {
            for (index, value) in self.iter().take(scan_limit).enumerate() {
                if value == element {
                    matches_seen += 1;
                    if matches_seen >= target_rank {
                        out.push(index as i64);
                        if count != 0 && out.len() >= count {
                            break;
                        }
                    }
                }
            }
            return out;
        }

        for scanned in 0..scan_limit {
            let index = self.len as usize - 1 - scanned;
            let Some(value) = self.index(index as i64) else {
                break;
            };
            if value == element {
                matches_seen += 1;
                if matches_seen >= target_rank {
                    out.push(index as i64);
                    if count != 0 && out.len() >= count {
                        break;
                    }
                }
            }
        }
        out
    }

    pub fn iter(&self) -> QuickListIter<'_> {
        QuickListIter {
            current: self.head,
            current_iter: None,
            marker: PhantomData,
        }
    }

    pub fn clear(&mut self) {
        let mut node = self.head;
        while !node.is_null() {
            let next = unsafe { (*node).next };
            unsafe {
                drop(Box::from_raw(node));
            }
            node = next;
        }
        self.head = ptr::null_mut();
        self.tail = ptr::null_mut();
        self.node_count = 0;
        self.len = 0;
    }

    fn normalize_index(&self, index: i64) -> Option<u64> {
        if self.len == 0 {
            return None;
        }
        let len = self.len as i64;
        let index = if index < 0 { len + index } else { index };
        (0..len).contains(&index).then_some(index as u64)
    }

    fn normalize_range(&self, start: i64, stop: i64) -> Option<(u64, u64)> {
        if self.len == 0 {
            return None;
        }
        let len = self.len as i64;
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop || start >= len {
            return None;
        }
        Some((start as u64, stop as u64))
    }

    unsafe fn split_if_needed(&mut self, node: *mut ListpackNode) {
        let count = unsafe { (*node).count as i32 };
        if node.is_null() || count <= self.fill {
            return;
        }
        let entries = unsafe { decode_all_entries(&*node) };
        let midpoint = entries.len() / 2;
        let (left, right) = entries.split_at(midpoint);
        unsafe {
            encode_node_entries(&mut *node, left);
        }

        let mut new_node = Box::new(ListpackNode::default());
        encode_node_entries(&mut new_node, right);
        new_node.prev = node;
        new_node.next = unsafe { (*node).next };
        let new_node = Box::into_raw(new_node);

        unsafe {
            if !(*new_node).next.is_null() {
                (*(*new_node).next).prev = new_node;
            } else {
                self.tail = new_node;
            }
            (*node).next = new_node;
        }
        self.node_count = self.node_count.saturating_add(1);
    }

    unsafe fn cleanup_after_mutation(&mut self, node: *mut ListpackNode) {
        if node.is_null() {
            return;
        }
        let count = unsafe { (*node).count };
        if count == 0 {
            unsafe {
                self.unlink_node(node);
            }
            return;
        }
        unsafe {
            self.try_merge_with_next(node);
            let prev = (*node).prev;
            if !prev.is_null() {
                self.try_merge_with_next(prev);
            }
        }
    }

    unsafe fn try_merge_with_next(&mut self, node: *mut ListpackNode) {
        if node.is_null() {
            return;
        }
        let next = unsafe { (*node).next };
        if next.is_null() {
            return;
        }
        let combined = unsafe { (*node).count as i32 + (*next).count as i32 };
        if combined > self.fill / 2 {
            return;
        }
        let mut merged = unsafe { decode_all_entries(&*node) };
        merged.extend(unsafe { decode_all_entries(&*next) });
        unsafe {
            encode_node_entries(&mut *node, &merged);
            self.unlink_node(next);
        }
    }

    unsafe fn unlink_node(&mut self, node: *mut ListpackNode) {
        let prev = unsafe { (*node).prev };
        let next = unsafe { (*node).next };
        unsafe {
            if !prev.is_null() {
                (*prev).next = next;
            } else {
                self.head = next;
            }
            if !next.is_null() {
                (*next).prev = prev;
            } else {
                self.tail = prev;
            }
            drop(Box::from_raw(node));
        }
        self.node_count = self.node_count.saturating_sub(1);
        if self.node_count == 0 {
            self.head = ptr::null_mut();
            self.tail = ptr::null_mut();
        }
    }

    fn compress_empty_and_merge(&mut self) {
        let mut node = self.head;
        while !node.is_null() {
            let next = unsafe { (*node).next };
            unsafe {
                self.cleanup_after_mutation(node);
            }
            node = next;
        }
    }
}

impl Drop for QuickList {
    fn drop(&mut self) {
        self.clear();
    }
}

unsafe impl Send for QuickList {}

pub fn lp_push_front(node: &mut ListpackNode, element: &[u8]) {
    let mut entries = decode_all_entries(node);
    entries.insert(0, element.to_vec());
    encode_node_entries(node, &entries);
}

pub fn lp_push_back(node: &mut ListpackNode, element: &[u8]) {
    let mut entries = decode_all_entries(node);
    entries.push(element.to_vec());
    encode_node_entries(node, &entries);
}

pub fn lp_pop_front(node: &mut ListpackNode) -> Option<Vec<u8>> {
    let mut entries = decode_all_entries(node);
    if entries.is_empty() {
        return None;
    }
    let value = entries.remove(0);
    encode_node_entries(node, &entries);
    Some(value)
}

pub fn lp_pop_back(node: &mut ListpackNode) -> Option<Vec<u8>> {
    let mut entries = decode_all_entries(node);
    let value = entries.pop()?;
    encode_node_entries(node, &entries);
    Some(value)
}

pub fn lp_get(node: &ListpackNode, index: usize) -> Option<&[u8]> {
    let mut offset = 0usize;
    let mut current = 0usize;
    while current < node.count as usize {
        let (payload, next_offset) = decode_entry_at(&node.data, offset)?;
        if current == index {
            return Some(payload);
        }
        offset = next_offset;
        current += 1;
    }
    None
}

pub fn lp_len(node: &ListpackNode) -> usize {
    node.count as usize
}

pub fn lp_iter(node: &ListpackNode) -> ListpackIter<'_> {
    ListpackIter {
        node,
        offset: 0,
        yielded: 0,
    }
}

pub fn lp_find(node: &ListpackNode, element: &[u8], skip: usize) -> Option<usize> {
    if skip == 0 && !element.is_empty() {
        for offset in memmem::find_iter(&node.data, element) {
            let mut cursor = 0usize;
            let mut index = 0usize;
            while cursor < node.data.len() {
                let Some((payload, next_cursor)) = decode_entry_at(&node.data, cursor) else {
                    break;
                };
                let payload_offset = payload.as_ptr() as usize - node.data.as_ptr() as usize;
                if payload_offset == offset && payload.len() == element.len() {
                    return Some(index);
                }
                cursor = next_cursor;
                index += 1;
            }
        }
        return None;
    }

    lp_iter(node)
        .enumerate()
        .find(|(index, value)| *index % (skip + 1) == 0 && *value == element)
        .map(|(index, _)| index)
}

pub fn lp_delete_at(node: &mut ListpackNode, index: usize) {
    let mut entries = decode_all_entries(node);
    if index >= entries.len() {
        return;
    }
    entries.remove(index);
    encode_node_entries(node, &entries);
}

pub fn lp_insert_before(node: &mut ListpackNode, index: usize, element: &[u8]) {
    let mut entries = decode_all_entries(node);
    let index = index.min(entries.len());
    entries.insert(index, element.to_vec());
    encode_node_entries(node, &entries);
}

pub fn lp_insert_after(node: &mut ListpackNode, index: usize, element: &[u8]) {
    let mut entries = decode_all_entries(node);
    let index = (index + 1).min(entries.len());
    entries.insert(index, element.to_vec());
    encode_node_entries(node, &entries);
}

pub fn lp_set(node: &mut ListpackNode, index: usize, element: &[u8]) {
    let mut entries = decode_all_entries(node);
    if index >= entries.len() {
        return;
    }
    entries[index] = element.to_vec();
    encode_node_entries(node, &entries);
}

pub fn lp_byte_size(node: &ListpackNode) -> usize {
    node.data.len()
}

fn encode_node_entries(node: &mut ListpackNode, entries: &[Vec<u8>]) {
    node.data.clear();
    node.count = entries.len() as u16;
    let mut prev_len = 0usize;
    for entry in entries {
        let encoded = encode_entry(entry, prev_len);
        prev_len = encoded.len();
        node.data.extend_from_slice(&encoded);
    }
}

fn decode_all_entries(node: &ListpackNode) -> Vec<Vec<u8>> {
    lp_iter(node).map(|value| value.to_vec()).collect()
}

fn encode_entry(element: &[u8], prev_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    write_varint(prev_len as u64, &mut out);
    write_string_header(element.len(), &mut out);
    out.extend_from_slice(element);
    let backlen = out.len().saturating_add(1);
    out.push(backlen.min(u8::MAX as usize) as u8);
    out
}

fn decode_entry_at(data: &[u8], offset: usize) -> Option<(&[u8], usize)> {
    if offset >= data.len() {
        return None;
    }
    let (_, mut cursor) = read_varint(data, offset)?;
    let (payload_len, header_len) = read_string_header(data.get(cursor..)?)?;
    cursor += header_len;
    let payload_end = cursor.checked_add(payload_len)?;
    let payload = data.get(cursor..payload_end)?;
    let backlen = *data.get(payload_end)? as usize;
    let next_offset = offset.checked_add(backlen)?;
    Some((payload, next_offset))
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_varint(data: &[u8], mut offset: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *data.get(offset)?;
        value |= ((byte & 0x7f) as u64) << shift;
        offset += 1;
        if byte & 0x80 == 0 {
            return Some((value, offset));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

fn write_string_header(len: usize, out: &mut Vec<u8>) {
    if len <= 0x3f {
        out.push(0x80 | len as u8);
    } else if u16::try_from(len).is_ok() {
        out.push(0xe0);
        out.extend_from_slice(&(len as u16).to_le_bytes());
    } else {
        out.push(0xf0);
        out.extend_from_slice(&(len as u32).to_le_bytes());
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
