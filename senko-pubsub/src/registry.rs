use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use hashbrown::HashMap;
use smallvec::SmallVec;

use crate::{
    message::{MessageKind, PubSubMessage},
    pattern::PatternIndex,
    slot::BroadcastSlot,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PublishReport {
    pub delivered: u64,
    pub lagged_connections: SmallVec<[u64; 4]>,
}

#[derive(Debug, Clone)]
pub struct ChannelEntry {
    pub name: CompactString,
    pub slots: SmallVec<[Arc<BroadcastSlot>; 8]>,
    pub subscriber_count: u32,
}

impl ChannelEntry {
    #[inline]
    pub fn new(name: CompactString) -> Self {
        Self {
            name,
            slots: SmallVec::new(),
            subscriber_count: 0,
        }
    }

    #[inline]
    pub fn subscribe(&mut self, conn_id: u64) -> Arc<BroadcastSlot> {
        if let Some(existing) = self.slots.iter().find(|slot| slot.conn_id() == conn_id) {
            return Arc::clone(existing);
        }

        let slot = Arc::new(BroadcastSlot::new(conn_id));
        self.slots.push(Arc::clone(&slot));
        self.subscriber_count = self.slots.len() as u32;
        slot
    }

    #[inline]
    pub fn unsubscribe(&mut self, conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let index = self
            .slots
            .iter()
            .position(|slot| slot.conn_id() == conn_id)?;
        let slot = self.slots.swap_remove(index);
        self.subscriber_count = self.slots.len() as u32;
        Some(slot)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline]
    pub fn publish(&self, message: Arc<PubSubMessage>) -> PublishReport {
        let mut report = PublishReport::default();

        for slot in &self.slots {
            match slot.publish(Arc::clone(&message)) {
                Ok(()) => report.delivered += 1,
                Err(_) => report.lagged_connections.push(slot.conn_id()),
            }
        }

        report
    }
}

#[derive(Debug)]
pub struct ChannelRegistry {
    pub channels: HashMap<CompactString, ChannelEntry, RandomState>,
    pub patterns: PatternIndex,
    pub shard_channels: HashMap<CompactString, ChannelEntry, RandomState>,
    pub total_pattern_subscriptions: u32,
    pub total_messages_published: AtomicU64,
}

impl Default for ChannelRegistry {
    fn default() -> Self {
        Self {
            channels: HashMap::with_hasher(RandomState::new()),
            patterns: PatternIndex::default(),
            shard_channels: HashMap::with_hasher(RandomState::new()),
            total_pattern_subscriptions: 0,
            total_messages_published: AtomicU64::new(0),
        }
    }
}

impl ChannelRegistry {
    #[inline]
    pub fn subscribe(&mut self, channel: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let name = channel_name(channel);
        let entry = self
            .channels
            .entry(name.clone())
            .or_insert_with(|| ChannelEntry::new(name));
        entry.subscribe(conn_id)
    }

    #[inline]
    pub fn unsubscribe(&mut self, channel: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let name = channel_name(channel);
        self.unsubscribe_exact(&name, conn_id)
    }

    #[inline]
    pub fn psubscribe(&mut self, pattern: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let slot = self.patterns.subscribe(pattern, conn_id);
        self.total_pattern_subscriptions = self.patterns.total_subscriber_count();
        slot
    }

    #[inline]
    pub fn punsubscribe(&mut self, pattern: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let slot = self.patterns.unsubscribe(pattern, conn_id)?;
        self.total_pattern_subscriptions = self.patterns.total_subscriber_count();
        Some(slot)
    }

    #[inline]
    pub fn ssubscribe(&mut self, channel: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let name = channel_name(channel);
        let entry = self
            .shard_channels
            .entry(name.clone())
            .or_insert_with(|| ChannelEntry::new(name));
        entry.subscribe(conn_id)
    }

    #[inline]
    pub fn sunsubscribe(&mut self, channel: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let name = channel_name(channel);
        self.unsubscribe_shard_exact(&name, conn_id)
    }

    pub fn publish(&mut self, channel: &[u8], payload: Bytes) -> u64 {
        self.total_messages_published
            .fetch_add(1, Ordering::Relaxed);

        let exact_name = self.lookup_channel_name(&self.channels, channel);
        let has_patterns = self.patterns.has_matching_pattern(channel);
        if exact_name.is_none() && !has_patterns {
            return 0;
        }

        let channel_name = exact_name.clone().unwrap_or_else(|| channel_name(channel));
        let message = Arc::new(PubSubMessage {
            channel: channel_name.clone(),
            payload,
            kind: MessageKind::Message,
        });

        let mut delivered = 0u64;
        if let Some(name) = exact_name {
            let report = self
                .channels
                .get(name.as_str())
                .expect("exact channel exists")
                .publish(Arc::clone(&message));
            delivered += report.delivered;
            self.remove_lagged_exact(&name, &report.lagged_connections);
        }

        if has_patterns {
            delivered += self
                .patterns
                .publish_to_patterns(channel, Arc::clone(&message));
            self.total_pattern_subscriptions = self.patterns.total_subscriber_count();
        }

        delivered
    }

    pub fn publish_shard(&mut self, channel: &[u8], payload: Bytes) -> u64 {
        self.total_messages_published
            .fetch_add(1, Ordering::Relaxed);

        let Some(name) = self.lookup_channel_name(&self.shard_channels, channel) else {
            return 0;
        };

        let message = Arc::new(PubSubMessage {
            channel: name.clone(),
            payload,
            kind: MessageKind::SMessage,
        });
        let report = self
            .shard_channels
            .get(name.as_str())
            .expect("shard channel exists")
            .publish(message);
        let delivered = report.delivered;
        self.remove_lagged_shard(&name, &report.lagged_connections);
        delivered
    }

    #[inline]
    pub fn num_subscribers(&self, channel: &[u8]) -> u32 {
        self.lookup_channel(&self.channels, channel)
            .map_or(0, |entry| entry.subscriber_count)
    }

    #[inline]
    pub fn num_shard_subscribers(&self, channel: &[u8]) -> u32 {
        self.lookup_channel(&self.shard_channels, channel)
            .map_or(0, |entry| entry.subscriber_count)
    }

    #[inline]
    pub fn total_messages_published(&self) -> u64 {
        self.total_messages_published.load(Ordering::Relaxed)
    }

    fn lookup_channel<'a>(
        &'a self,
        map: &'a HashMap<CompactString, ChannelEntry, RandomState>,
        channel: &[u8],
    ) -> Option<&'a ChannelEntry> {
        if let Ok(channel) = std::str::from_utf8(channel) {
            return map.get(channel);
        }

        let channel = channel_name(channel);
        map.get(&channel)
    }

    fn lookup_channel_name(
        &self,
        map: &HashMap<CompactString, ChannelEntry, RandomState>,
        channel: &[u8],
    ) -> Option<CompactString> {
        self.lookup_channel(map, channel)
            .map(|entry| entry.name.clone())
    }

    fn remove_lagged_exact(&mut self, channel: &CompactString, conn_ids: &[u64]) {
        remove_lagged_from_map(&mut self.channels, channel, conn_ids);
    }

    fn remove_lagged_shard(&mut self, channel: &CompactString, conn_ids: &[u64]) {
        remove_lagged_from_map(&mut self.shard_channels, channel, conn_ids);
    }

    fn unsubscribe_exact(
        &mut self,
        channel: &CompactString,
        conn_id: u64,
    ) -> Option<Arc<BroadcastSlot>> {
        let mut remove_entry = false;
        let slot = self.channels.get_mut(channel)?.unsubscribe(conn_id);
        if let Some(entry) = self.channels.get(channel) {
            remove_entry = entry.is_empty();
        }
        if remove_entry {
            self.channels.remove(channel);
        }
        slot
    }

    fn unsubscribe_shard_exact(
        &mut self,
        channel: &CompactString,
        conn_id: u64,
    ) -> Option<Arc<BroadcastSlot>> {
        let mut remove_entry = false;
        let slot = self.shard_channels.get_mut(channel)?.unsubscribe(conn_id);
        if let Some(entry) = self.shard_channels.get(channel) {
            remove_entry = entry.is_empty();
        }
        if remove_entry {
            self.shard_channels.remove(channel);
        }
        slot
    }
}

#[inline]
fn channel_name(channel: &[u8]) -> CompactString {
    match std::str::from_utf8(channel) {
        Ok(channel) => CompactString::from(channel),
        Err(_) => CompactString::from_utf8_lossy(channel),
    }
}

fn remove_lagged_from_map(
    map: &mut HashMap<CompactString, ChannelEntry, RandomState>,
    channel: &CompactString,
    conn_ids: &[u64],
) {
    if conn_ids.is_empty() {
        return;
    }

    let mut remove_entry = false;
    if let Some(entry) = map.get_mut(channel) {
        for conn_id in conn_ids {
            let _ = entry.unsubscribe(*conn_id);
        }
        remove_entry = entry.is_empty();
    }
    if remove_entry {
        map.remove(channel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{ChannelEntry, ChannelRegistry};
    use crate::{
        message::{MessageKind, PubSubMessage},
        pattern::PatternSubscription,
        slot::RING_SIZE,
    };

    #[test]
    fn subscribe_creates_channel_and_returns_slot() {
        let mut registry = ChannelRegistry::default();
        let slot = registry.subscribe(b"alpha", 10);

        assert_eq!(slot.conn_id(), 10);
        assert_eq!(registry.num_subscribers(b"alpha"), 1);
        assert!(registry.channels.contains_key("alpha"));
    }

    #[test]
    fn unsubscribe_removes_slot_and_deletes_empty_channel() {
        let mut registry = ChannelRegistry::default();
        let slot = registry.subscribe(b"alpha", 10);

        let removed = registry.unsubscribe(b"alpha", 10).expect("removed slot");
        assert!(Arc::ptr_eq(&slot, &removed));
        assert_eq!(registry.num_subscribers(b"alpha"), 0);
        assert!(!registry.channels.contains_key("alpha"));
    }

    #[test]
    fn publish_to_zero_subscribers_returns_zero() {
        let mut registry = ChannelRegistry::default();
        assert_eq!(
            registry.publish(b"ghost", Bytes::from_static(b"payload")),
            0
        );
        assert_eq!(registry.total_messages_published(), 1);
    }

    #[test]
    fn publish_to_ten_thousand_subscribers_returns_ten_thousand_and_shares_message() {
        let mut registry = ChannelRegistry::default();
        let mut slots = Vec::with_capacity(10_000);
        for conn_id in 0..10_000u64 {
            slots.push(registry.subscribe(b"fanout", conn_id));
        }

        let delivered = registry.publish(b"fanout", Bytes::from_static(b"payload"));
        assert_eq!(delivered, 10_000);

        let mut first_ptr = None;
        for slot in &slots {
            let message = slot.recv().expect("published message");
            let ptr = Arc::as_ptr(&message);
            if let Some(expected) = first_ptr {
                assert_eq!(ptr, expected);
            } else {
                first_ptr = Some(ptr);
            }
        }
    }

    #[test]
    fn arc_refcount_after_publish_to_n_subscribers_is_n_plus_one() {
        let mut entry = ChannelEntry::new("fanout".into());
        for conn_id in 0..32u64 {
            let _ = entry.subscribe(conn_id);
        }

        let message = Arc::new(PubSubMessage {
            channel: "fanout".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });
        let report = entry.publish(Arc::clone(&message));

        assert_eq!(report.delivered, 32);
        assert_eq!(Arc::strong_count(&message), 33);

        for slot in &entry.slots {
            drop(slot.recv().expect("queued message"));
        }
        assert_eq!(Arc::strong_count(&message), 1);
    }

    #[test]
    fn lagged_subscribers_are_removed_after_publish() {
        let mut registry = ChannelRegistry::default();
        let lagged = registry.subscribe(b"slow", 1);
        let healthy = registry.subscribe(b"slow", 2);

        for index in 0..RING_SIZE as u64 {
            assert!(
                lagged
                    .publish(Arc::new(PubSubMessage {
                        channel: "slow".into(),
                        payload: Bytes::copy_from_slice(&index.to_le_bytes()),
                        kind: MessageKind::Message,
                    }))
                    .is_ok()
            );
        }

        let delivered = registry.publish(b"slow", Bytes::from_static(b"x"));
        assert_eq!(delivered, 1);
        assert_eq!(registry.num_subscribers(b"slow"), 1);
        assert!(healthy.recv().is_some());

        drop(registry.unsubscribe(b"slow", 2));
        drop(lagged);
    }

    #[test]
    fn pattern_subscriptions_receive_pattern_messages() {
        let mut registry = ChannelRegistry::default();
        let slot = registry.psubscribe(b"news:*", 1);

        let delivered = registry.publish(b"news:world", Bytes::from_static(b"payload"));
        assert_eq!(delivered, 1);

        let message = slot.recv().expect("pattern message");
        assert_eq!(message.channel, "news:world");
        assert_eq!(
            message.kind,
            MessageKind::PMessage {
                pattern: "news:*".into()
            }
        );
    }

    #[test]
    fn pattern_index_groups_subscribers_by_pattern() {
        let mut registry = ChannelRegistry::default();
        let first = registry.psubscribe(b"news:*", 1);
        let second = registry.psubscribe(b"news:*", 2);

        assert_eq!(registry.patterns.subscriptions.len(), 1);
        let grouped: &PatternSubscription = &registry.patterns.subscriptions[0];
        assert_eq!(grouped.subscriber_count, 2);

        let delivered = registry.publish(b"news:world", Bytes::from_static(b"payload"));
        assert_eq!(delivered, 2);
        assert!(first.recv().is_some());
        assert!(second.recv().is_some());
    }

    #[test]
    fn shard_publish_uses_shard_message_kind() {
        let mut registry = ChannelRegistry::default();
        let slot = registry.ssubscribe(b"shard:1", 5);

        let delivered = registry.publish_shard(b"shard:1", Bytes::from_static(b"payload"));
        assert_eq!(delivered, 1);

        let message = slot.recv().expect("shard message");
        assert_eq!(message.kind, MessageKind::SMessage);
    }

    #[test]
    fn threaded_publish_and_recv_stress_does_not_drop_messages() {
        let mut registry = ChannelRegistry::default();
        let slot = registry.subscribe(b"stress", 1);

        for index in 0..10_000u64 {
            let delivered =
                registry.publish(b"stress", Bytes::copy_from_slice(&index.to_le_bytes()));
            assert_eq!(delivered, 1);
            let message = slot.recv().expect("queued message");
            let value =
                u64::from_le_bytes(message.payload.as_ref().try_into().expect("payload width"));
            assert_eq!(value, index);
        }
    }
}
