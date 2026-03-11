use std::collections::BTreeMap;

use compact_str::CompactString;
use smallvec::SmallVec;

pub const WHEEL_SLOTS: usize = 512;
pub const WHEEL_RESOLUTION_MS: u64 = 100;
pub const WHEEL_SPAN_MS: u64 = WHEEL_SLOTS as u64 * WHEEL_RESOLUTION_MS;

type WheelSlot = SmallVec<[Option<CompactString>; 8]>;
type OverflowEntries = Vec<Option<CompactString>>;
type FieldEntry = (CompactString, CompactString);
type FieldWheelSlot = SmallVec<[Option<FieldEntry>; 8]>;
type FieldOverflowEntries = Vec<Option<FieldEntry>>;

#[derive(Debug)]
pub struct TimerWheel {
    slots: [WheelSlot; WHEEL_SLOTS],
    overflow: BTreeMap<u64, OverflowEntries>,
    last_tick_ms: u64,
    rotations: u64,
}

#[derive(Debug)]
pub struct FieldExpiryWheel {
    slots: [FieldWheelSlot; WHEEL_SLOTS],
    overflow: BTreeMap<u64, FieldOverflowEntries>,
    last_tick_ms: u64,
    rotations: u64,
}

impl TimerWheel {
    pub fn new(now_ms: u64) -> Self {
        Self {
            slots: std::array::from_fn(|_| SmallVec::new()),
            overflow: BTreeMap::new(),
            last_tick_ms: align_tick(now_ms),
            rotations: 0,
        }
    }

    pub fn schedule_key(&mut self, key: CompactString, expires_at: u64) {
        let current_tick = align_tick(self.last_tick_ms);
        let scheduled_at = normalize_deadline(expires_at, current_tick);
        let entry = Some(key);
        if scheduled_at <= current_tick.saturating_add(WHEEL_SPAN_MS) {
            self.slots[slot_index(scheduled_at)].push(entry);
        } else {
            self.overflow
                .entry(align_tick(scheduled_at))
                .or_default()
                .push(entry);
        }
    }

    pub fn tombstone(&mut self, key: &[u8], expires_at: Option<u64>) {
        let Some(deadline) = expires_at else {
            return;
        };
        let aligned = align_tick(deadline);
        if aligned <= self.last_tick_ms.saturating_add(WHEEL_SPAN_MS) {
            tombstone_slot(&mut self.slots[slot_index(aligned)], key);
        } else if let Some(entries) = self.overflow.get_mut(&aligned) {
            tombstone_vec(entries, key);
            if entries.iter().all(Option::is_none) {
                self.overflow.remove(&aligned);
            }
        }
    }

    pub fn update_expiry(
        &mut self,
        key: &CompactString,
        old_expires_at: Option<u64>,
        new_expires_at: Option<u64>,
    ) {
        self.tombstone(key.as_bytes(), old_expires_at);
        if let Some(deadline) = new_expires_at {
            self.schedule_key(key.clone(), deadline);
        }
    }

    pub fn advance(&mut self, now_ms: u64) -> Vec<CompactString> {
        let mut expired = Vec::new();
        let target = align_tick(now_ms);
        while self.last_tick_ms < target {
            self.last_tick_ms = self.last_tick_ms.saturating_add(WHEEL_RESOLUTION_MS);
            let slot = slot_index(self.last_tick_ms);
            drain_slot(&mut self.slots[slot], &mut expired);
            if slot == WHEEL_SLOTS - 1 {
                self.rotations = self.rotations.saturating_add(1);
                self.promote_overflow();
            }
        }
        expired
    }

    pub fn rotations(&self) -> u64 {
        self.rotations
    }

    #[cfg(test)]
    pub fn overflow_contains_deadline(&self, deadline: u64) -> bool {
        self.overflow.contains_key(&align_tick(deadline))
    }

    fn promote_overflow(&mut self) {
        let promote_until = self.last_tick_ms.saturating_add(WHEEL_SPAN_MS);
        let deadlines: Vec<u64> = self
            .overflow
            .range(..=promote_until)
            .map(|(deadline, _)| *deadline)
            .collect();
        for deadline in deadlines {
            if let Some(mut entries) = self.overflow.remove(&deadline) {
                self.slots[slot_index(deadline)].extend(entries.drain(..));
            }
        }
    }
}

impl FieldExpiryWheel {
    pub fn new(now_ms: u64) -> Self {
        Self {
            slots: std::array::from_fn(|_| SmallVec::new()),
            overflow: BTreeMap::new(),
            last_tick_ms: align_tick(now_ms),
            rotations: 0,
        }
    }

    pub fn schedule_field(&mut self, key: CompactString, field: CompactString, expires_at: u64) {
        let current_tick = align_tick(self.last_tick_ms);
        let scheduled_at = normalize_deadline(expires_at, current_tick);
        let entry = Some((key, field));
        if scheduled_at <= current_tick.saturating_add(WHEEL_SPAN_MS) {
            self.slots[slot_index(scheduled_at)].push(entry);
        } else {
            self.overflow
                .entry(align_tick(scheduled_at))
                .or_default()
                .push(entry);
        }
    }

    pub fn advance(&mut self, now_ms: u64) -> Vec<(CompactString, CompactString)> {
        let mut expired = Vec::new();
        let target = align_tick(now_ms);
        while self.last_tick_ms < target {
            self.last_tick_ms = self.last_tick_ms.saturating_add(WHEEL_RESOLUTION_MS);
            let slot = slot_index(self.last_tick_ms);
            for entry in self.slots[slot].drain(..) {
                if let Some((key, field)) = entry {
                    expired.push((key, field));
                }
            }
            if slot == WHEEL_SLOTS - 1 {
                self.rotations = self.rotations.saturating_add(1);
                self.promote_overflow();
            }
        }
        expired
    }

    pub fn tombstone_key(&mut self, key: &[u8]) {
        for slot in &mut self.slots {
            tombstone_field_slot(slot, key);
        }
        let mut empty_deadlines = Vec::new();
        for (deadline, entries) in &mut self.overflow {
            tombstone_field_vec(entries, key);
            if entries.iter().all(Option::is_none) {
                empty_deadlines.push(*deadline);
            }
        }
        for deadline in empty_deadlines {
            let _ = self.overflow.remove(&deadline);
        }
    }

    fn promote_overflow(&mut self) {
        let promote_until = self.last_tick_ms.saturating_add(WHEEL_SPAN_MS);
        let deadlines: Vec<u64> = self
            .overflow
            .range(..=promote_until)
            .map(|(deadline, _)| *deadline)
            .collect();
        for deadline in deadlines {
            if let Some(mut entries) = self.overflow.remove(&deadline) {
                self.slots[slot_index(deadline)].extend(entries.drain(..));
            }
        }
    }
}

fn drain_slot(slot: &mut WheelSlot, expired: &mut Vec<CompactString>) {
    for entry in slot.drain(..) {
        if let Some(key) = entry {
            expired.push(key);
        }
    }
}

fn tombstone_slot(slot: &mut WheelSlot, key: &[u8]) {
    for entry in slot.iter_mut() {
        if entry
            .as_ref()
            .is_some_and(|candidate| candidate.as_bytes() == key)
        {
            *entry = None;
        }
    }
}

fn tombstone_vec(entries: &mut OverflowEntries, key: &[u8]) {
    for entry in entries.iter_mut() {
        if entry
            .as_ref()
            .is_some_and(|candidate| candidate.as_bytes() == key)
        {
            *entry = None;
        }
    }
}

fn tombstone_field_slot(slot: &mut FieldWheelSlot, key: &[u8]) {
    for entry in slot.iter_mut() {
        if entry
            .as_ref()
            .is_some_and(|(candidate, _)| candidate.as_bytes() == key)
        {
            *entry = None;
        }
    }
}

fn tombstone_field_vec(entries: &mut FieldOverflowEntries, key: &[u8]) {
    for entry in entries.iter_mut() {
        if entry
            .as_ref()
            .is_some_and(|(candidate, _)| candidate.as_bytes() == key)
        {
            *entry = None;
        }
    }
}

pub fn align_tick(ms: u64) -> u64 {
    ms - (ms % WHEEL_RESOLUTION_MS)
}

pub fn slot_index(deadline_ms: u64) -> usize {
    ((align_tick(deadline_ms) / WHEEL_RESOLUTION_MS) % WHEEL_SLOTS as u64) as usize
}

fn normalize_deadline(deadline_ms: u64, current_tick: u64) -> u64 {
    if deadline_ms <= current_tick {
        current_tick.saturating_add(WHEEL_RESOLUTION_MS)
    } else {
        deadline_ms
    }
}
