use ahash::RandomState;
use compact_str::CompactString;
use hashbrown::HashTable;
use memchr::memmem;

use crate::SenkoValue;

const LISTPACK_MAX_ENTRIES: u32 = 128;
const LISTPACK_MAX_ELEM_SIZE: usize = 64;
const LISTPACK_HEADER_LEN: usize = 4;

#[repr(C)]
#[derive(Debug, Clone, PartialEq)]
pub struct HashField {
    pub value: SenkoValue,
    pub expires_at: Option<u64>,
}

#[derive(Debug)]
pub struct HashObject {
    pub fields: HashTable<(CompactString, HashField)>,
    pub field_count: u32,
    pub has_field_expiry: bool,
    hasher: RandomState,
    listpack: Option<Vec<u8>>,
}

impl Default for HashObject {
    fn default() -> Self {
        Self::with_hasher(RandomState::new())
    }
}

impl Clone for HashObject {
    fn clone(&self) -> Self {
        let mut cloned = Self {
            fields: HashTable::with_capacity(self.fields.len()),
            field_count: self.field_count,
            has_field_expiry: self.has_field_expiry,
            hasher: self.hasher.clone(),
            listpack: self.listpack.clone(),
        };
        for (key, value) in self.fields.iter() {
            let hash = cloned.hash_field(key.as_bytes());
            let hasher = cloned.hasher.clone();
            cloned
                .fields
                .insert_unique(hash, (key.clone(), value.clone()), move |(k, _)| {
                    hasher.hash_one(k.as_bytes())
                });
        }
        cloned
    }
}

impl PartialEq for HashObject {
    fn eq(&self, other: &Self) -> bool {
        if self.field_count != other.field_count || self.has_field_expiry != other.has_field_expiry
        {
            return false;
        }
        self.fields.iter().all(|(key, field)| {
            let hash = other.hash_field(key.as_bytes());
            other
                .fields
                .find(hash, |(candidate, _)| {
                    candidate.as_bytes() == key.as_bytes()
                })
                .is_some_and(|(_, candidate)| candidate == field)
        })
    }
}

impl HashObject {
    pub fn with_hasher(hasher: RandomState) -> Self {
        let mut object = Self {
            fields: HashTable::new(),
            field_count: 0,
            has_field_expiry: false,
            hasher,
            listpack: Some(Vec::new()),
        };
        object.write_listpack_header();
        object
    }

    pub fn is_listpack(&self) -> bool {
        self.listpack.is_some()
    }

    pub fn upgrade_to_hashtable(&mut self) {
        self.listpack = None;
    }

    pub fn listpack_get(&self, field: &[u8]) -> Option<&[u8]> {
        if field.is_empty() || field.len() > u8::MAX as usize {
            return None;
        }
        let bytes = self.listpack.as_ref()?;
        if bytes.len() < LISTPACK_HEADER_LEN {
            return None;
        }

        let mut cursor = LISTPACK_HEADER_LEN;
        while let Some(offset) = memmem::find(&bytes[cursor..], field) {
            let field_start = cursor + offset;
            if field_start == 0 {
                return None;
            }
            if bytes[field_start - 1] as usize != field.len() {
                cursor = field_start.saturating_add(1);
                continue;
            }
            let entry_start = field_start - 1;
            if !self.listpack_entry_starts_at(bytes, entry_start) {
                cursor = field_start.saturating_add(1);
                continue;
            }

            let mut idx = field_start + field.len();
            let value_len = *bytes.get(idx)? as usize;
            idx += 1;
            let value_end = idx.checked_add(value_len)?;
            if value_end > bytes.len() {
                return None;
            }
            return Some(&bytes[idx..value_end]);
        }
        None
    }

    pub fn listpack_set(&mut self, field: &[u8], value: &[u8]) {
        if field.len() > u8::MAX as usize || value.len() > u8::MAX as usize {
            self.upgrade_to_hashtable();
            return;
        }
        let mut entries = self.decode_listpack_entries();
        if let Some((_, stored_value)) = entries.iter_mut().find(|(f, _)| f.as_slice() == field) {
            *stored_value = value.to_vec();
        } else {
            entries.push((field.to_vec(), value.to_vec()));
        }
        self.encode_listpack_entries(entries);
    }

    pub fn get(&self, field: &[u8], now_ms: u64) -> Option<&HashField> {
        let hash = self.hash_field(field);
        let (_, value) = self
            .fields
            .find(hash, |(candidate, _)| candidate.as_bytes() == field)?;
        if is_expired(value.expires_at, now_ms) {
            return None;
        }
        Some(value)
    }

    pub fn get_mut(&mut self, field: &[u8], now_ms: u64) -> Option<&mut HashField> {
        let hash = self.hash_field(field);
        let expired = self
            .fields
            .find(hash, |(candidate, _)| candidate.as_bytes() == field)
            .is_some_and(|(_, value)| is_expired(value.expires_at, now_ms));
        if expired {
            let _ = self.delete(field);
            return None;
        }
        self.fields
            .find_mut(hash, |(candidate, _)| candidate.as_bytes() == field)
            .map(|(_, value)| value)
    }

    pub fn set(
        &mut self,
        field: CompactString,
        value: SenkoValue,
        expires_at: Option<u64>,
    ) -> bool {
        let hash = self.hash_field(field.as_bytes());
        if let Some((_, stored)) = self.fields.find_mut(hash, |(candidate, _)| {
            candidate.as_bytes() == field.as_bytes()
        }) {
            stored.value = value;
            stored.expires_at = expires_at;
            self.has_field_expiry = self.has_field_expiry || expires_at.is_some();
            self.refresh_encoding();
            return false;
        }

        let hasher = self.hasher.clone();
        self.fields.insert_unique(
            hash,
            (field, HashField { value, expires_at }),
            move |(candidate, _)| hasher.hash_one(candidate.as_bytes()),
        );
        self.field_count = self.field_count.saturating_add(1);
        self.has_field_expiry = self.has_field_expiry || expires_at.is_some();
        self.refresh_encoding();
        true
    }

    pub fn delete(&mut self, field: &[u8]) -> bool {
        let hash = self.hash_field(field);
        let Ok(found) = self
            .fields
            .find_entry(hash, |(candidate, _)| candidate.as_bytes() == field)
        else {
            return false;
        };
        let ((_, removed), _) = found.remove();
        self.field_count = self.field_count.saturating_sub(1);
        if removed.expires_at.is_some() {
            self.has_field_expiry = self
                .fields
                .iter()
                .any(|(_, value)| value.expires_at.is_some());
        }
        self.refresh_encoding();
        true
    }

    pub fn exists(&self, field: &[u8], now_ms: u64) -> bool {
        self.get(field, now_ms).is_some()
    }

    pub fn len(&self, now_ms: u64) -> usize {
        if !self.has_field_expiry {
            return self.field_count as usize;
        }
        self.iter_live(now_ms).count()
    }

    pub fn is_empty(&self, now_ms: u64) -> bool {
        self.len(now_ms) == 0
    }

    pub fn iter_live(
        &self,
        now_ms: u64,
    ) -> impl Iterator<Item = (&CompactString, &HashField)> + '_ {
        self.fields
            .iter()
            .map(|(key, field)| (key, field))
            .filter(move |(_, field)| !is_expired(field.expires_at, now_ms))
    }

    pub fn drain_expired(&mut self, now_ms: u64) -> usize {
        if !self.has_field_expiry {
            return 0;
        }
        let expired: Vec<CompactString> = self
            .fields
            .iter()
            .filter_map(|(field, value)| {
                is_expired(value.expires_at, now_ms).then_some(field.clone())
            })
            .collect();
        for field in &expired {
            let _ = self.delete(field.as_bytes());
        }
        self.has_field_expiry = self
            .fields
            .iter()
            .any(|(_, value)| value.expires_at.is_some());
        expired.len()
    }

    fn hash_field(&self, field: &[u8]) -> u64 {
        self.hasher.hash_one(field)
    }

    fn can_encode_listpack(&self) -> bool {
        if self.has_field_expiry || self.field_count > LISTPACK_MAX_ENTRIES {
            return false;
        }
        self.fields.iter().all(|(field, value)| {
            let field_len_ok = field.len() <= LISTPACK_MAX_ELEM_SIZE;
            let value_len_ok = value.value.as_bytes().len() <= LISTPACK_MAX_ELEM_SIZE;
            field_len_ok && value_len_ok
        })
    }

    fn refresh_encoding(&mut self) {
        if !self.can_encode_listpack() {
            self.upgrade_to_hashtable();
            return;
        }
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .fields
            .iter()
            .map(|(field, value)| {
                (
                    field.as_bytes().to_vec(),
                    value.value.as_bytes().as_ref().to_vec(),
                )
            })
            .collect();
        self.encode_listpack_entries(entries);
    }

    fn decode_listpack_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some(bytes) = self.listpack.as_ref() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut idx = LISTPACK_HEADER_LEN;
        while idx < bytes.len() {
            let field_len = bytes[idx] as usize;
            idx += 1;
            if idx + field_len > bytes.len() {
                break;
            }
            let field = bytes[idx..idx + field_len].to_vec();
            idx += field_len;
            if idx >= bytes.len() {
                break;
            }
            let value_len = bytes[idx] as usize;
            idx += 1;
            if idx + value_len > bytes.len() {
                break;
            }
            let value = bytes[idx..idx + value_len].to_vec();
            idx += value_len;
            out.push((field, value));
        }
        out
    }

    fn encode_listpack_entries(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut bytes = Vec::with_capacity(LISTPACK_HEADER_LEN + entries.len() * 8);
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        for (field, value) in entries {
            if field.len() > u8::MAX as usize || value.len() > u8::MAX as usize {
                self.upgrade_to_hashtable();
                return;
            }
            bytes.push(field.len() as u8);
            bytes.extend_from_slice(&field);
            bytes.push(value.len() as u8);
            bytes.extend_from_slice(&value);
        }
        let total = bytes.len() as u32;
        bytes[..LISTPACK_HEADER_LEN].copy_from_slice(&total.to_le_bytes());
        self.listpack = Some(bytes);
    }

    fn listpack_entry_starts_at(&self, bytes: &[u8], target: usize) -> bool {
        let mut idx = LISTPACK_HEADER_LEN;
        while idx < bytes.len() {
            if idx == target {
                return true;
            }
            let Some(field_len) = bytes.get(idx).copied() else {
                return false;
            };
            idx = idx.saturating_add(1 + field_len as usize);
            let Some(value_len) = bytes.get(idx).copied() else {
                return false;
            };
            idx = idx.saturating_add(1 + value_len as usize);
        }
        false
    }

    fn write_listpack_header(&mut self) {
        if let Some(listpack) = self.listpack.as_mut() {
            listpack.clear();
            listpack.extend_from_slice(&(LISTPACK_HEADER_LEN as u32).to_le_bytes());
        }
    }
}

fn is_expired(expires_at: Option<u64>, now_ms: u64) -> bool {
    expires_at.is_some_and(|deadline| deadline <= now_ms)
}
