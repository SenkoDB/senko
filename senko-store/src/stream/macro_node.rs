use crate::stream::id::StreamId;

const STREAM_ITEM_FLAG_NONE: u16 = 0;
const STREAM_ITEM_FLAG_SAMEFIELDS: u16 = 1;
const STREAM_ITEM_FLAG_DELETED: u16 = 2;

#[derive(Clone, Debug)]
struct MacroEntry {
    id: StreamId,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    same_fields: bool,
    deleted: bool,
}

#[derive(Clone, Debug)]
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
        let master_fields = fields
            .iter()
            .map(|(f, _)| (*f).to_vec())
            .collect::<Vec<_>>();
        let first = MacroEntry {
            id: first_entry_id,
            fields: fields
                .iter()
                .map(|(f, v)| ((*f).to_vec(), (*v).to_vec()))
                .collect(),
            same_fields: true,
            deleted: false,
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
        let same_fields = fields.len() == self.master_fields.len()
            && fields
                .iter()
                .zip(self.master_fields.iter())
                .all(|((f, _), mf)| *f == mf.as_slice());

        self.entries.push(MacroEntry {
            id,
            fields: fields
                .iter()
                .map(|(f, v)| ((*f).to_vec(), (*v).to_vec()))
                .collect(),
            same_fields,
            deleted: false,
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
        for entry in &mut self.entries {
            if entry.id == id {
                if entry.deleted {
                    return false;
                }
                entry.deleted = true;
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
        let entry = self.entries.iter().find(|e| e.id == id && !e.deleted)?;
        Some(
            entry
                .fields
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect(),
        )
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
        let entries = self.count as usize + self.deleted as usize;
        entries >= max_entries || self.byte_size() >= max_bytes
    }

    pub(crate) fn total_entries(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn hard_trim_oldest_entries(&mut self, mut to_remove: usize) -> usize {
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

    pub(crate) fn hard_trim_while_id_lt(&mut self, threshold: StreamId, limit: usize) -> usize {
        let mut removed = 0usize;
        let idx = 0usize;
        while idx < self.entries.len() {
            if limit > 0 && removed >= limit {
                break;
            }
            if self.entries[idx].id >= threshold {
                break;
            }
            let entry = self.entries.remove(idx);
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
        if let Some(first) = self.entries.first() {
            self.first_id = first.id;
        } else {
            self.first_id = StreamId::ZERO;
        }
        if let Some(last) = self.entries.last() {
            self.last_id = last.id;
        } else {
            self.last_id = StreamId::ZERO;
        }
    }

    fn rebuild_data(&mut self) {
        let mut data = Vec::new();

        encode_u16(&mut data, STREAM_ITEM_FLAG_NONE);
        encode_u64(&mut data, self.master_fields.len() as u64);
        for field in &self.master_fields {
            encode_bytes(&mut data, field);
        }
        data.push(0);

        let master = self.entries.first().map(|e| e.id).unwrap_or(StreamId::ZERO);
        for entry in self.entries.iter().skip(1) {
            let mut flags = if entry.same_fields {
                STREAM_ITEM_FLAG_SAMEFIELDS
            } else {
                STREAM_ITEM_FLAG_NONE
            };
            if entry.deleted {
                flags |= STREAM_ITEM_FLAG_DELETED;
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
            let idx = self.index;
            self.index += 1;
            let entry = &self.node.entries[idx];
            if !self.include_deleted && entry.deleted {
                continue;
            }
            let fields = entry
                .fields
                .iter()
                .map(|(f, v)| (f.as_slice(), v.as_slice()))
                .collect::<Vec<_>>();
            return Some((entry.id, fields));
        }
        None
    }
}

fn encode_u16(dst: &mut Vec<u8>, n: u16) {
    dst.extend_from_slice(&n.to_le_bytes());
}

fn encode_u64(dst: &mut Vec<u8>, n: u64) {
    dst.extend_from_slice(&n.to_le_bytes());
}

fn encode_bytes(dst: &mut Vec<u8>, bytes: &[u8]) {
    encode_u64(dst, bytes.len() as u64);
    dst.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use crate::stream::id::StreamId;

    use super::ListpackMacroNode;

    #[test]
    fn same_fields_vs_full_encoding() {
        let mut node = ListpackMacroNode::new(
            StreamId { ms: 1, seq: 0 },
            &[(b"f1".as_slice(), b"v1".as_slice())],
        );

        let same = node.append(
            StreamId { ms: 1, seq: 1 },
            &[(b"f1".as_slice(), b"v2".as_slice())],
        );
        assert!(same);

        let different = node.append(
            StreamId { ms: 1, seq: 2 },
            &[(b"f2".as_slice(), b"v3".as_slice())],
        );
        assert!(!different);
    }

    #[test]
    fn soft_delete_and_iters() {
        let mut node = ListpackMacroNode::new(
            StreamId { ms: 1, seq: 0 },
            &[(b"f".as_slice(), b"a".as_slice())],
        );
        node.append(
            StreamId { ms: 1, seq: 1 },
            &[(b"f".as_slice(), b"b".as_slice())],
        );

        assert!(node.soft_delete(StreamId { ms: 1, seq: 1 }));
        assert!(node.get(StreamId { ms: 1, seq: 1 }).is_none());

        let ids = node.iter().map(|(id, _)| id).collect::<Vec<_>>();
        assert_eq!(ids, vec![StreamId { ms: 1, seq: 0 }]);

        let all_ids = node
            .iter_including_deleted()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        assert_eq!(
            all_ids,
            vec![StreamId { ms: 1, seq: 0 }, StreamId { ms: 1, seq: 1 }]
        );
    }
}
