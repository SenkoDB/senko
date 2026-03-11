use std::{
    collections::BTreeMap,
    ops::Bound,
    time::{SystemTime, UNIX_EPOCH},
};

use compact_str::CompactString;
use senko_core::{
    ConsumerGroup, ConsumerState, PelEntry, SenkoError, StreamId, StreamObject, StreamRefMode,
};

pub struct PendingDetail {
    pub id: StreamId,
    pub consumer: CompactString,
    pub idle_ms: u64,
    pub delivery_count: u64,
}

pub struct PendingSummary {
    pub pel_count: u64,
    pub min_id: Option<StreamId>,
    pub max_id: Option<StreamId>,
    pub per_consumer: Vec<(CompactString, u64)>,
}

pub struct GroupInfo {
    pub name: CompactString,
    pub consumers: usize,
    pub pending: u64,
    pub last_delivered_id: StreamId,
    pub entries_read: u64,
    pub lag: u64,
}

pub struct ConsumerInfo {
    pub name: CompactString,
    pub pending: u64,
    pub idle: u64,
    pub inactive: u64,
}

pub fn create_group(
    stream: &mut StreamObject,
    name: CompactString,
    last_delivered_id: StreamId,
    entries_read: u64,
) -> Result<(), SenkoError> {
    if stream.groups.contains_key(name.as_str()) {
        return Err(SenkoError::Protocol(
            "BUSYGROUP Consumer Group name already exists",
        ));
    }
    stream.groups.insert(
        name.clone(),
        ConsumerGroup::new(name, last_delivered_id, entries_read),
    );
    Ok(())
}

pub fn create_consumer(group: &mut ConsumerGroup, consumer: CompactString, now_ms: u64) -> bool {
    if group.consumers.contains_key(consumer.as_str()) {
        return false;
    }
    group.consumers.insert(
        consumer.clone(),
        ConsumerState {
            name: consumer,
            seen_time: now_ms,
            active_time: now_ms,
            pel: BTreeMap::new(),
        },
    );
    true
}

pub fn delete_consumer(group: &mut ConsumerGroup, consumer: &[u8]) -> u64 {
    let Ok(consumer) = std::str::from_utf8(consumer) else {
        return 0;
    };
    let Some(state) = group.consumers.remove(consumer) else {
        return 0;
    };
    let removed = state.pel.len() as u64;
    for id in state.pel.keys() {
        group.global_pel.remove(id);
    }
    group.pel_count = group.pel_count.saturating_sub(removed);
    removed
}

pub fn destroy_group(stream: &mut StreamObject, group: &[u8]) -> bool {
    let Ok(group) = std::str::from_utf8(group) else {
        return false;
    };
    stream.groups.remove(group).is_some()
}

pub fn set_group_id(
    stream: &StreamObject,
    group: &mut ConsumerGroup,
    last_delivered_id: StreamId,
    entries_read: Option<u64>,
) {
    group.last_delivered_id = if last_delivered_id == StreamId::MAX {
        stream.tree.last_id
    } else {
        last_delivered_id
    };
    if let Some(entries_read) = entries_read {
        group.entries_read = entries_read;
    }
}

pub fn add_pending_entry(
    group: &mut ConsumerGroup,
    consumer: CompactString,
    id: StreamId,
    delivery_time: u64,
    delivery_count: u64,
) {
    if !group.consumers.contains_key(consumer.as_str()) {
        let _ = create_consumer(group, consumer.clone(), delivery_time);
    }

    let Some(state) = group.consumers.get_mut(consumer.as_str()) else {
        return;
    };
    state.seen_time = delivery_time;
    state.active_time = delivery_time;

    let existed = state.pel.contains_key(&id);
    state.pel.insert(
        id,
        PelEntry {
            id,
            consumer: consumer.clone(),
            delivery_time,
            delivery_count,
        },
    );
    group.global_pel.insert(id, consumer);
    if !existed {
        group.pel_count = group.pel_count.saturating_add(1);
    }
}

pub fn remove_pending_entry(group: &mut ConsumerGroup, id: StreamId) -> Option<PelEntry> {
    let owner = group.global_pel.remove(&id)?;
    let state = group.consumers.get_mut(owner.as_str())?;
    let entry = state.pel.remove(&id)?;
    group.pel_count = group.pel_count.saturating_sub(1);
    Some(entry)
}

pub fn insert_pending(group: &mut ConsumerGroup, consumer: CompactString, entry: PelEntry) {
    let id = entry.id;
    if !group.consumers.contains_key(consumer.as_str()) {
        let _ = create_consumer(group, consumer.clone(), entry.delivery_time);
    }
    let Some(state) = group.consumers.get_mut(consumer.as_str()) else {
        return;
    };
    state.seen_time = entry.delivery_time;
    state.active_time = entry.delivery_time;
    let existed = state.pel.insert(id, entry).is_some();
    group.global_pel.insert(id, consumer);
    if !existed {
        group.pel_count = group.pel_count.saturating_add(1);
    }
}

pub fn ack_id(group: &mut ConsumerGroup, id: StreamId) -> bool {
    remove_pending_entry(group, id).is_some()
}

pub fn pending_summary(group: &ConsumerGroup) -> PendingSummary {
    let per_consumer = group
        .consumers
        .values()
        .filter_map(|consumer| {
            let pending = consumer.pel.len() as u64;
            (pending > 0).then(|| (consumer.name.clone(), pending))
        })
        .collect::<Vec<_>>();
    PendingSummary {
        pel_count: group.pel_count,
        min_id: group.global_pel.first_key_value().map(|(id, _)| *id),
        max_id: group.global_pel.last_key_value().map(|(id, _)| *id),
        per_consumer,
    }
}

pub fn pending_detail(
    group: &ConsumerGroup,
    start: StreamId,
    end: StreamId,
    count: usize,
    min_idle_ms: Option<u64>,
    consumer: Option<&[u8]>,
    now_ms: u64,
) -> Vec<PendingDetail> {
    if count == 0 {
        return Vec::new();
    }

    let consumer_filter = consumer
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .map(CompactString::new);

    let mut out = Vec::new();
    for (id, owner) in group
        .global_pel
        .range((Bound::Included(start), Bound::Included(end)))
    {
        if out.len() >= count {
            break;
        }
        if consumer_filter
            .as_ref()
            .is_some_and(|expected| expected.as_str() != owner.as_str())
        {
            continue;
        }
        let Some(state) = group.consumers.get(owner.as_str()) else {
            continue;
        };
        let Some(entry) = state.pel.get(id) else {
            continue;
        };
        let idle_ms = now_ms.saturating_sub(entry.delivery_time);
        if min_idle_ms.is_some_and(|min_idle| idle_ms <= min_idle) {
            continue;
        }
        out.push(PendingDetail {
            id: *id,
            consumer: owner.clone(),
            idle_ms,
            delivery_count: entry.delivery_count,
        });
    }
    out
}

pub fn group_info(stream: &StreamObject) -> Vec<GroupInfo> {
    stream
        .groups
        .values()
        .map(|group| GroupInfo {
            name: group.name.clone(),
            consumers: group.consumers.len(),
            pending: group.pel_count,
            last_delivered_id: group.last_delivered_id,
            entries_read: group.entries_read,
            lag: stream.tree.entries_added.saturating_sub(group.entries_read),
        })
        .collect()
}

pub fn consumer_info(group: &ConsumerGroup, now_ms: u64) -> Vec<ConsumerInfo> {
    group
        .consumers
        .values()
        .map(|consumer| ConsumerInfo {
            name: consumer.name.clone(),
            pending: consumer.pel.len() as u64,
            idle: now_ms.saturating_sub(consumer.seen_time),
            inactive: now_ms.saturating_sub(consumer.active_time),
        })
        .collect()
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

pub fn xackdel_apply(
    stream: &mut StreamObject,
    group: &mut ConsumerGroup,
    id: StreamId,
    ref_mode: StreamRefMode,
) -> bool {
    if !ack_id(group, id) {
        return false;
    }
    match ref_mode {
        StreamRefMode::KeepRef => {}
        StreamRefMode::DelRef => {
            let _ = stream.tree.delete_with_mode(id, StreamRefMode::DelRef);
        }
        StreamRefMode::Acked => {
            let _ = stream.tree.set_ref_mode(id, StreamRefMode::Acked);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use compact_str::CompactString;
    use senko_core::{StreamObject, StreamRadixTree};

    use super::*;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId { ms, seq }
    }

    #[test]
    fn pel_sync_survives_random_like_sequence() {
        let mut stream = StreamObject {
            tree: StreamRadixTree::new(),
            groups: Default::default(),
        };
        create_group(&mut stream, CompactString::new("g"), StreamId::ZERO, 0).unwrap();
        let group = stream.groups.get_mut("g").unwrap();

        for i in 0..1000u64 {
            let consumer = if i % 2 == 0 { "a" } else { "b" };
            add_pending_entry(group, CompactString::new(consumer), id(i + 1, 0), i, 1);
        }
        for i in (0..1000u64).step_by(3) {
            let _ = ack_id(group, id(i + 1, 0));
        }

        let global = group.global_pel.len();
        let per_consumer = group
            .consumers
            .values()
            .map(|consumer| consumer.pel.len())
            .sum::<usize>();
        assert_eq!(global, per_consumer);
        assert_eq!(group.pel_count as usize, global);
        for (entry_id, consumer) in &group.global_pel {
            assert!(
                group
                    .consumers
                    .get(consumer.as_str())
                    .and_then(|state| state.pel.get(entry_id))
                    .is_some()
            );
        }
    }
}
