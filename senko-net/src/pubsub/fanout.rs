use std::sync::{Arc, Mutex};

use ahash::RandomState;
use bytes::Bytes;
use compact_str::CompactString;
use flume::{Receiver, Sender, TrySendError};
use hashbrown::HashMap;
use senko_cluster::{SLOT_COUNT, crc16_slot};
use senko_pubsub::{BroadcastSlot, ChannelRegistry, MessageKind, PubSubMessage};

pub const COORDINATOR_SHARD_ID: usize = 0;
pub const FANOUT_QUEUE_CAPACITY: usize = 4096;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChannelTopology {
    pub shard_bitmask: u64,
    pub generation: u64,
    pub subscriber_count: u32,
}

impl ChannelTopology {
    #[inline(always)]
    pub fn contains_shard(self, shard_id: usize) -> bool {
        (self.shard_bitmask & shard_mask(shard_id)) != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanOutMsg {
    Subscribe {
        channel: CompactString,
        shard_id: usize,
    },
    Unsubscribe {
        channel: CompactString,
        shard_id: usize,
    },
    Message {
        msg: Arc<PubSubMessage>,
    },
    PatternSubscribe {
        pattern: CompactString,
        shard_id: usize,
    },
    PatternUnsubscribe {
        pattern: CompactString,
        shard_id: usize,
    },
    CacheInvalidate {
        channel: CompactString,
        topology: ChannelTopology,
    },
    PatternCacheInvalidate {
        pattern: CompactString,
        topology: ChannelTopology,
    },
    QueryChannel {
        channel: CompactString,
        requester_shard: usize,
    },
    QueryChannelResponse {
        channel: CompactString,
        topology: ChannelTopology,
    },
}

#[derive(Debug)]
pub struct CrossShardBus {
    pub senders: Box<[Sender<FanOutMsg>]>,
    receivers: Box<[Mutex<Option<Receiver<FanOutMsg>>>]>,
}

impl CrossShardBus {
    pub fn new(num_shards: usize) -> Self {
        assert!(
            num_shards <= u64::BITS as usize,
            "pub/sub fan-out supports at most 64 shards"
        );

        let mut senders = Vec::with_capacity(num_shards);
        let mut receivers = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            let (sender, receiver) = flume::bounded(FANOUT_QUEUE_CAPACITY);
            senders.push(sender);
            receivers.push(Mutex::new(Some(receiver)));
        }

        Self {
            senders: senders.into_boxed_slice(),
            receivers: receivers.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.senders.len()
    }

    pub fn take_receiver(&self, shard_id: usize) -> Receiver<FanOutMsg> {
        self.receivers[shard_id]
            .lock()
            .expect("fan-out receiver lock poisoned")
            .take()
            .expect("fan-out receiver already taken")
    }

    #[inline(always)]
    pub fn try_send(
        &self,
        shard_id: usize,
        message: FanOutMsg,
    ) -> Result<(), TrySendError<FanOutMsg>> {
        self.senders[shard_id].try_send(message)
    }

    fn send_control(&self, shard_id: usize, message: FanOutMsg) {
        self.senders[shard_id]
            .send(message)
            .expect("cross-shard control plane channel closed");
    }
}

#[derive(Debug, Default)]
pub struct LocalShardCache {
    cache: HashMap<CompactString, ChannelTopology, RandomState>,
    pattern_cache: HashMap<CompactString, ChannelTopology, RandomState>,
    generation: u64,
}

impl LocalShardCache {
    pub fn channel_topology(&self, channel: &[u8]) -> Option<ChannelTopology> {
        lookup_topology(&self.cache, channel)
    }

    pub fn pattern_topology(&self, pattern: &[u8]) -> Option<ChannelTopology> {
        lookup_topology(&self.pattern_cache, pattern)
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn update_channel(&mut self, channel: CompactString, topology: ChannelTopology) {
        self.generation = self.generation.max(topology.generation);
        if topology.subscriber_count == 0 {
            self.cache.remove(channel.as_str());
        } else {
            self.cache.insert(channel, topology);
        }
    }

    fn update_pattern(&mut self, pattern: CompactString, topology: ChannelTopology) {
        self.generation = self.generation.max(topology.generation);
        if topology.subscriber_count == 0 {
            self.pattern_cache.remove(pattern.as_str());
        } else {
            self.pattern_cache.insert(pattern, topology);
        }
    }

    fn matching_patterns(&self, channel: &[u8]) -> (u64, u32) {
        let mut shard_bitmask = 0u64;
        let mut subscriber_count = 0u32;

        for (pattern, topology) in &self.pattern_cache {
            if senko_pubsub::pattern::glob_match(pattern.as_bytes(), channel) {
                shard_bitmask |= topology.shard_bitmask;
                subscriber_count = subscriber_count.saturating_add(topology.subscriber_count);
            }
        }

        (shard_bitmask, subscriber_count)
    }

    pub fn channel_names(&self, pattern: Option<&[u8]>) -> Vec<CompactString> {
        collect_matching_names(&self.cache, pattern)
    }

    pub fn channel_subscriber_count(&self, channel: &[u8]) -> u32 {
        self.channel_topology(channel)
            .map(|topology| topology.subscriber_count)
            .unwrap_or_default()
    }

    pub fn pattern_subscriber_count(&self) -> u32 {
        self.pattern_cache
            .values()
            .map(|topology| topology.subscriber_count)
            .sum()
    }
}

#[derive(Debug, Default)]
pub struct GlobalChannelIndex {
    pub channel_shards: HashMap<CompactString, u64, RandomState>,
    pub pattern_shards: HashMap<CompactString, u64, RandomState>,
    pub channel_counts: HashMap<CompactString, u32, RandomState>,
    pub pattern_count: u32,
    generation: u64,
    channel_local_counts: HashMap<(CompactString, usize), u32, RandomState>,
    pattern_local_counts: HashMap<(CompactString, usize), u32, RandomState>,
    pattern_counts: HashMap<CompactString, u32, RandomState>,
}

impl GlobalChannelIndex {
    pub fn subscribe(&mut self, channel: CompactString, shard_id: usize) -> ChannelTopology {
        self.increment_channel(channel, shard_id)
    }

    pub fn unsubscribe(&mut self, channel: CompactString, shard_id: usize) -> ChannelTopology {
        self.decrement_channel(channel, shard_id)
    }

    pub fn pattern_subscribe(
        &mut self,
        pattern: CompactString,
        shard_id: usize,
    ) -> ChannelTopology {
        self.increment_pattern(pattern, shard_id)
    }

    pub fn pattern_unsubscribe(
        &mut self,
        pattern: CompactString,
        shard_id: usize,
    ) -> ChannelTopology {
        self.decrement_pattern(pattern, shard_id)
    }

    pub fn query_channel(&self, channel: &[u8]) -> ChannelTopology {
        let Some(name) = lookup_owned_key(&self.channel_shards, channel) else {
            return ChannelTopology {
                generation: self.generation,
                ..ChannelTopology::default()
            };
        };

        ChannelTopology {
            shard_bitmask: *self.channel_shards.get(name.as_str()).unwrap_or(&0),
            generation: self.generation,
            subscriber_count: *self.channel_counts.get(name.as_str()).unwrap_or(&0),
        }
    }

    pub fn matching_patterns(&self, channel: &[u8]) -> (u64, u32) {
        let mut shard_bitmask = 0u64;
        let mut subscriber_count = 0u32;

        for (pattern, bitmask) in &self.pattern_shards {
            if senko_pubsub::pattern::glob_match(pattern.as_bytes(), channel) {
                shard_bitmask |= *bitmask;
                subscriber_count = subscriber_count.saturating_add(
                    self.pattern_counts
                        .get(pattern.as_str())
                        .copied()
                        .unwrap_or_default(),
                );
            }
        }

        (shard_bitmask, subscriber_count)
    }

    pub fn channel_names(&self, pattern: Option<&[u8]>) -> Vec<CompactString> {
        collect_matching_names_from_counts(&self.channel_counts, pattern)
    }

    pub fn channel_subscriber_count(&self, channel: &[u8]) -> u32 {
        lookup_count(&self.channel_counts, channel)
    }

    #[inline]
    pub fn pattern_count(&self) -> u32 {
        self.pattern_count
    }

    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn increment_channel(&mut self, channel: CompactString, shard_id: usize) -> ChannelTopology {
        let mask = shard_mask(shard_id);
        let local_key = (channel.clone(), shard_id);
        let local_count = self.channel_local_counts.entry(local_key).or_default();
        *local_count = local_count.saturating_add(1);

        let total_count = {
            let total_count = self.channel_counts.entry(channel.clone()).or_default();
            *total_count = total_count.saturating_add(1);
            *total_count
        };

        let bitmask = {
            let bitmask = self.channel_shards.entry(channel).or_default();
            *bitmask |= mask;
            *bitmask
        };
        self.bump_generation(bitmask, total_count)
    }

    fn decrement_channel(&mut self, channel: CompactString, shard_id: usize) -> ChannelTopology {
        let local_key = (channel.clone(), shard_id);
        let Some(local_count) = self.channel_local_counts.get_mut(&local_key) else {
            return self.query_channel(channel.as_bytes());
        };

        *local_count = local_count.saturating_sub(1);
        let shard_went_empty = *local_count == 0;
        if shard_went_empty {
            self.channel_local_counts.remove(&local_key);
        }

        let mut total_count = 0u32;
        if let Some(count) = self.channel_counts.get_mut(channel.as_str()) {
            *count = count.saturating_sub(1);
            total_count = *count;
            if *count == 0 {
                self.channel_counts.remove(channel.as_str());
            }
        }

        let mut bitmask = self
            .channel_shards
            .get(channel.as_str())
            .copied()
            .unwrap_or(0);
        if shard_went_empty {
            bitmask &= !shard_mask(shard_id);
        }

        if total_count == 0 || bitmask == 0 {
            self.channel_shards.remove(channel.as_str());
            bitmask = 0;
        } else if let Some(entry) = self.channel_shards.get_mut(channel.as_str()) {
            *entry = bitmask;
        }

        self.bump_generation(bitmask, total_count)
    }

    fn increment_pattern(&mut self, pattern: CompactString, shard_id: usize) -> ChannelTopology {
        let mask = shard_mask(shard_id);
        let local_key = (pattern.clone(), shard_id);
        let local_count = self.pattern_local_counts.entry(local_key).or_default();
        *local_count = local_count.saturating_add(1);

        let total_count = {
            let total_count = self.pattern_counts.entry(pattern.clone()).or_default();
            *total_count = total_count.saturating_add(1);
            *total_count
        };
        self.pattern_count = self.pattern_count.saturating_add(1);

        let bitmask = {
            let bitmask = self.pattern_shards.entry(pattern).or_default();
            *bitmask |= mask;
            *bitmask
        };
        self.bump_generation(bitmask, total_count)
    }

    fn decrement_pattern(&mut self, pattern: CompactString, shard_id: usize) -> ChannelTopology {
        let local_key = (pattern.clone(), shard_id);
        let Some(local_count) = self.pattern_local_counts.get_mut(&local_key) else {
            return ChannelTopology {
                generation: self.generation,
                shard_bitmask: self
                    .pattern_shards
                    .get(pattern.as_str())
                    .copied()
                    .unwrap_or(0),
                subscriber_count: self
                    .pattern_counts
                    .get(pattern.as_str())
                    .copied()
                    .unwrap_or(0),
            };
        };

        *local_count = local_count.saturating_sub(1);
        let shard_went_empty = *local_count == 0;
        if shard_went_empty {
            self.pattern_local_counts.remove(&local_key);
        }

        let mut total_count = 0u32;
        if let Some(count) = self.pattern_counts.get_mut(pattern.as_str()) {
            *count = count.saturating_sub(1);
            total_count = *count;
            self.pattern_count = self.pattern_count.saturating_sub(1);
            if *count == 0 {
                self.pattern_counts.remove(pattern.as_str());
            }
        }

        let mut bitmask = self
            .pattern_shards
            .get(pattern.as_str())
            .copied()
            .unwrap_or(0);
        if shard_went_empty {
            bitmask &= !shard_mask(shard_id);
        }

        if total_count == 0 || bitmask == 0 {
            self.pattern_shards.remove(pattern.as_str());
            bitmask = 0;
        } else if let Some(entry) = self.pattern_shards.get_mut(pattern.as_str()) {
            *entry = bitmask;
        }

        self.bump_generation(bitmask, total_count)
    }

    fn bump_generation(&mut self, shard_bitmask: u64, subscriber_count: u32) -> ChannelTopology {
        self.generation = self.generation.saturating_add(1);
        ChannelTopology {
            shard_bitmask,
            generation: self.generation,
            subscriber_count,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishOutcome {
    pub delivered: u64,
    pub cache_miss: bool,
}

#[derive(Debug)]
pub struct ShardFanOut {
    shard_id: usize,
    bus: Arc<CrossShardBus>,
    receiver: Receiver<FanOutMsg>,
    pub registry: ChannelRegistry,
    pub cache: LocalShardCache,
    coordinator: Option<GlobalChannelIndex>,
    dropped_messages: u64,
}

impl ShardFanOut {
    pub fn new(shard_id: usize, bus: Arc<CrossShardBus>) -> Self {
        let receiver = bus.take_receiver(shard_id);
        let coordinator = if shard_id == COORDINATOR_SHARD_ID {
            Some(GlobalChannelIndex::default())
        } else {
            None
        };

        Self {
            shard_id,
            bus,
            receiver,
            registry: ChannelRegistry::default(),
            cache: LocalShardCache::default(),
            coordinator,
            dropped_messages: 0,
        }
    }

    #[inline(always)]
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    #[inline(always)]
    pub fn dropped_messages(&self) -> u64 {
        self.dropped_messages
    }

    pub fn subscribe(&mut self, channel: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let slot = self.registry.subscribe(channel, conn_id);
        let channel = channel_name(channel);
        self.handle_local_control(FanOutMsg::Subscribe {
            channel,
            shard_id: self.shard_id,
        });
        slot
    }

    pub fn unsubscribe(&mut self, channel: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let removed = self.registry.unsubscribe(channel, conn_id)?;
        let channel = channel_name(channel);
        self.handle_local_control(FanOutMsg::Unsubscribe {
            channel,
            shard_id: self.shard_id,
        });
        Some(removed)
    }

    pub fn psubscribe(&mut self, pattern: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let slot = self.registry.psubscribe(pattern, conn_id);
        let pattern = channel_name(pattern);
        self.handle_local_control(FanOutMsg::PatternSubscribe {
            pattern,
            shard_id: self.shard_id,
        });
        slot
    }

    pub fn punsubscribe(&mut self, pattern: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let removed = self.registry.punsubscribe(pattern, conn_id)?;
        let pattern = channel_name(pattern);
        self.handle_local_control(FanOutMsg::PatternUnsubscribe {
            pattern,
            shard_id: self.shard_id,
        });
        Some(removed)
    }

    pub fn publish(&mut self, channel: &[u8], payload: Bytes) -> PublishOutcome {
        let msg = Arc::new(PubSubMessage {
            channel: channel_name(channel),
            payload,
            kind: MessageKind::Message,
        });
        self.publish_arc(msg)
    }

    pub fn publish_arc(&mut self, msg: Arc<PubSubMessage>) -> PublishOutcome {
        let exact_topology = self.resolve_exact_topology(msg.channel.as_bytes());
        let (pattern_bitmask, pattern_subscribers) =
            self.cache.matching_patterns(msg.channel.as_bytes());
        let local_exact =
            deliver_exact_to_registry(&mut self.registry, &msg.channel, Arc::clone(&msg));
        let local_patterns =
            deliver_patterns_to_registry(&mut self.registry, &msg.channel, &msg.payload);

        let Some(exact_topology) = exact_topology else {
            return PublishOutcome {
                delivered: local_exact + local_patterns,
                cache_miss: true,
            };
        };

        let mut remote_mask = exact_topology.shard_bitmask | pattern_bitmask;
        remote_mask &= !shard_mask(self.shard_id);
        fan_out_to_remote_shards(
            self.bus.as_ref(),
            remote_mask,
            Arc::clone(&msg),
            &mut self.dropped_messages,
        );

        PublishOutcome {
            delivered: u64::from(exact_topology.subscriber_count) + u64::from(pattern_subscribers),
            cache_miss: false,
        }
    }

    pub fn drain_bus(&mut self) -> usize {
        let mut drained = 0usize;

        while let Ok(message) = self.receiver.try_recv() {
            drained += 1;
            match message {
                FanOutMsg::Message { msg } => {
                    let _ = deliver_exact_to_registry(
                        &mut self.registry,
                        &msg.channel,
                        Arc::clone(&msg),
                    );
                    let _ = deliver_patterns_to_registry(
                        &mut self.registry,
                        &msg.channel,
                        &msg.payload,
                    );
                }
                FanOutMsg::CacheInvalidate { channel, topology } => {
                    self.cache.update_channel(channel, topology);
                }
                FanOutMsg::PatternCacheInvalidate { pattern, topology } => {
                    self.cache.update_pattern(pattern, topology);
                }
                FanOutMsg::QueryChannel {
                    channel,
                    requester_shard,
                } => {
                    if self.shard_id != COORDINATOR_SHARD_ID {
                        continue;
                    }
                    let topology = self
                        .coordinator
                        .as_ref()
                        .expect("coordinator must exist")
                        .query_channel(channel.as_bytes());
                    self.bus.send_control(
                        requester_shard,
                        FanOutMsg::QueryChannelResponse { channel, topology },
                    );
                }
                FanOutMsg::QueryChannelResponse { channel, topology } => {
                    self.cache.update_channel(channel, topology);
                }
                FanOutMsg::Subscribe { .. }
                | FanOutMsg::Unsubscribe { .. }
                | FanOutMsg::PatternSubscribe { .. }
                | FanOutMsg::PatternUnsubscribe { .. } => {
                    if self.shard_id == COORDINATOR_SHARD_ID {
                        self.apply_control_plane_update(message);
                    }
                }
            }
        }

        drained
    }

    pub fn coordinator_index(&self) -> Option<&GlobalChannelIndex> {
        self.coordinator.as_ref()
    }

    pub fn pubsub_channels(&self, pattern: Option<&[u8]>) -> Vec<CompactString> {
        if let Some(coordinator) = self.coordinator.as_ref() {
            return coordinator.channel_names(pattern);
        }
        self.cache.channel_names(pattern)
    }

    pub fn pubsub_numsub(&self, channel: &[u8]) -> u32 {
        if let Some(coordinator) = self.coordinator.as_ref() {
            return coordinator.channel_subscriber_count(channel);
        }
        self.cache.channel_subscriber_count(channel)
    }

    pub fn pubsub_numpat(&self) -> u32 {
        if let Some(coordinator) = self.coordinator.as_ref() {
            return coordinator.pattern_count();
        }
        self.cache.pattern_subscriber_count()
    }

    pub fn subscribe_shard(
        &mut self,
        router: &ShardChannelRouter,
        channel: &[u8],
        conn_id: u64,
    ) -> Result<Arc<BroadcastSlot>, ShardRouteError> {
        match router.route(channel, self.shard_id) {
            ShardRoute::Local => Ok(self.registry.ssubscribe(channel, conn_id)),
            ShardRoute::Remote { slot, shard_id } => Err(ShardRouteError::Moved { slot, shard_id }),
        }
    }

    pub fn unsubscribe_shard(
        &mut self,
        router: &ShardChannelRouter,
        channel: &[u8],
        conn_id: u64,
    ) -> Result<Option<Arc<BroadcastSlot>>, ShardRouteError> {
        match router.route(channel, self.shard_id) {
            ShardRoute::Local => Ok(self.registry.sunsubscribe(channel, conn_id)),
            ShardRoute::Remote { slot, shard_id } => Err(ShardRouteError::Moved { slot, shard_id }),
        }
    }

    pub fn spublish(
        &mut self,
        router: &ShardChannelRouter,
        channel: &[u8],
        payload: Bytes,
    ) -> Result<u64, ShardRouteError> {
        match router.route(channel, self.shard_id) {
            ShardRoute::Local => Ok(self.registry.publish_shard(channel, payload)),
            ShardRoute::Remote { slot, shard_id } => Err(ShardRouteError::Moved { slot, shard_id }),
        }
    }

    pub fn subscribe_shard_local(&mut self, channel: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        self.registry.ssubscribe(channel, conn_id)
    }

    pub fn unsubscribe_shard_local(
        &mut self,
        channel: &[u8],
        conn_id: u64,
    ) -> Option<Arc<BroadcastSlot>> {
        self.registry.sunsubscribe(channel, conn_id)
    }

    pub fn spublish_local(&mut self, channel: &[u8], payload: Bytes) -> u64 {
        self.registry.publish_shard(channel, payload)
    }

    pub fn shard_channels(&self, pattern: Option<&[u8]>) -> Vec<CompactString> {
        collect_matching_channel_entries(&self.registry.shard_channels, pattern)
    }

    pub fn shard_numsub(&self, channel: &[u8]) -> u32 {
        self.registry.num_shard_subscribers(channel)
    }

    fn handle_local_control(&mut self, message: FanOutMsg) {
        if self.shard_id == COORDINATOR_SHARD_ID {
            self.apply_control_plane_update(message);
        } else {
            self.bus.send_control(COORDINATOR_SHARD_ID, message);
        }
    }

    fn apply_control_plane_update(&mut self, message: FanOutMsg) {
        match message {
            FanOutMsg::Subscribe { channel, shard_id } => {
                let topology = self
                    .coordinator
                    .as_mut()
                    .expect("coordinator must exist")
                    .subscribe(channel.clone(), shard_id);
                self.cache.update_channel(channel.clone(), topology);
                self.broadcast_exact_invalidation(channel, topology);
            }
            FanOutMsg::Unsubscribe { channel, shard_id } => {
                let topology = self
                    .coordinator
                    .as_mut()
                    .expect("coordinator must exist")
                    .unsubscribe(channel.clone(), shard_id);
                self.cache.update_channel(channel.clone(), topology);
                self.broadcast_exact_invalidation(channel, topology);
            }
            FanOutMsg::PatternSubscribe { pattern, shard_id } => {
                let topology = self
                    .coordinator
                    .as_mut()
                    .expect("coordinator must exist")
                    .pattern_subscribe(pattern.clone(), shard_id);
                self.cache.update_pattern(pattern.clone(), topology);
                self.broadcast_pattern_invalidation(pattern, topology);
            }
            FanOutMsg::PatternUnsubscribe { pattern, shard_id } => {
                let topology = self
                    .coordinator
                    .as_mut()
                    .expect("coordinator must exist")
                    .pattern_unsubscribe(pattern.clone(), shard_id);
                self.cache.update_pattern(pattern.clone(), topology);
                self.broadcast_pattern_invalidation(pattern, topology);
            }
            _ => {}
        }
    }

    fn broadcast_exact_invalidation(&self, channel: CompactString, topology: ChannelTopology) {
        for shard_id in 0..self.bus.shard_count() {
            if shard_id == self.shard_id {
                continue;
            }
            self.bus.send_control(
                shard_id,
                FanOutMsg::CacheInvalidate {
                    channel: channel.clone(),
                    topology,
                },
            );
        }
    }

    fn broadcast_pattern_invalidation(&self, pattern: CompactString, topology: ChannelTopology) {
        for shard_id in 0..self.bus.shard_count() {
            if shard_id == self.shard_id {
                continue;
            }
            self.bus.send_control(
                shard_id,
                FanOutMsg::PatternCacheInvalidate {
                    pattern: pattern.clone(),
                    topology,
                },
            );
        }
    }

    fn resolve_exact_topology(&mut self, channel: &[u8]) -> Option<ChannelTopology> {
        if let Some(topology) = self.cache.channel_topology(channel) {
            return Some(topology);
        }

        if let Some(coordinator) = self.coordinator.as_ref() {
            let topology = coordinator.query_channel(channel);
            self.cache.update_channel(channel_name(channel), topology);
            return Some(topology);
        }

        let channel = channel_name(channel);
        let _ = self.bus.try_send(
            COORDINATOR_SHARD_ID,
            FanOutMsg::QueryChannel {
                channel,
                requester_shard: self.shard_id,
            },
        );
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRoute {
    Local,
    Remote { slot: u16, shard_id: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRouteError {
    Moved { slot: u16, shard_id: usize },
}

#[derive(Debug, Clone)]
pub struct ShardChannelRouter {
    cluster_mode: bool,
    slot_to_shard: Box<[usize]>,
}

impl ShardChannelRouter {
    pub fn standalone() -> Self {
        Self {
            cluster_mode: false,
            slot_to_shard: vec![0; SLOT_COUNT].into_boxed_slice(),
        }
    }

    pub fn cluster_by_modulo(num_shards: usize) -> Self {
        let slot_to_shard = (0..SLOT_COUNT)
            .map(|slot| slot % num_shards.max(1))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            cluster_mode: true,
            slot_to_shard,
        }
    }

    pub fn owner_of(&self, channel: &[u8]) -> usize {
        if !self.cluster_mode {
            return 0;
        }
        let slot = usize::from(crc16_slot(channel));
        self.slot_to_shard[slot]
    }

    pub fn route(&self, channel: &[u8], local_shard: usize) -> ShardRoute {
        let slot = crc16_slot(channel);
        let owner = if self.cluster_mode {
            self.slot_to_shard[usize::from(slot)]
        } else {
            0
        };
        if owner == local_shard {
            ShardRoute::Local
        } else {
            ShardRoute::Remote {
                slot,
                shard_id: owner,
            }
        }
    }
}

#[inline]
fn deliver_exact_to_registry(
    registry: &mut ChannelRegistry,
    channel: &CompactString,
    msg: Arc<PubSubMessage>,
) -> u64 {
    let Some(entry) = registry.channels.get(channel.as_str()) else {
        return 0;
    };

    let report = entry.publish(msg);
    let delivered = report.delivered;
    for conn_id in &report.lagged_connections {
        let _ = registry.unsubscribe(channel.as_bytes(), *conn_id);
    }
    delivered
}

#[inline]
fn deliver_patterns_to_registry(
    registry: &mut ChannelRegistry,
    channel: &CompactString,
    payload: &Bytes,
) -> u64 {
    let message = Arc::new(PubSubMessage {
        channel: channel.clone(),
        payload: payload.clone(),
        kind: MessageKind::Message,
    });
    registry
        .patterns
        .publish_to_patterns(channel.as_bytes(), message)
}

#[inline]
fn fan_out_to_remote_shards(
    bus: &CrossShardBus,
    mut remote_mask: u64,
    msg: Arc<PubSubMessage>,
    dropped_messages: &mut u64,
) {
    while remote_mask != 0 {
        let shard_id = remote_mask.trailing_zeros() as usize;
        remote_mask &= remote_mask - 1;
        if bus
            .try_send(
                shard_id,
                FanOutMsg::Message {
                    msg: Arc::clone(&msg),
                },
            )
            .is_err()
        {
            *dropped_messages = dropped_messages.saturating_add(1);
        }
    }
}

#[inline(always)]
fn shard_mask(shard_id: usize) -> u64 {
    1u64 << shard_id
}

fn lookup_topology(
    map: &HashMap<CompactString, ChannelTopology, RandomState>,
    key: &[u8],
) -> Option<ChannelTopology> {
    if let Ok(key) = std::str::from_utf8(key) {
        return map.get(key).copied();
    }

    let key = channel_name(key);
    map.get(&key).copied()
}

fn lookup_owned_key<V>(
    map: &HashMap<CompactString, V, RandomState>,
    key: &[u8],
) -> Option<CompactString> {
    if let Ok(key) = std::str::from_utf8(key) {
        return map.get_key_value(key).map(|(key, _)| key.clone());
    }

    let key = channel_name(key);
    map.get_key_value(&key).map(|(key, _)| key.clone())
}

fn collect_matching_names(
    map: &HashMap<CompactString, ChannelTopology, RandomState>,
    pattern: Option<&[u8]>,
) -> Vec<CompactString> {
    let mut names = map
        .iter()
        .filter(|(_, topology)| topology.subscriber_count > 0)
        .filter(|(name, _)| {
            pattern.is_none_or(|pattern| {
                senko_pubsub::pattern::glob_match_simd(pattern, name.as_bytes())
            })
        })
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn collect_matching_names_from_counts(
    map: &HashMap<CompactString, u32, RandomState>,
    pattern: Option<&[u8]>,
) -> Vec<CompactString> {
    let mut names = map
        .iter()
        .filter(|(_, count)| **count > 0)
        .filter(|(name, _)| {
            pattern.is_none_or(|pattern| {
                senko_pubsub::pattern::glob_match_simd(pattern, name.as_bytes())
            })
        })
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn collect_matching_channel_entries(
    map: &HashMap<CompactString, senko_pubsub::ChannelEntry, RandomState>,
    pattern: Option<&[u8]>,
) -> Vec<CompactString> {
    let mut names = map
        .iter()
        .filter(|(_, entry)| entry.subscriber_count > 0)
        .filter(|(name, _)| {
            pattern.is_none_or(|pattern| {
                senko_pubsub::pattern::glob_match_simd(pattern, name.as_bytes())
            })
        })
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn lookup_count(map: &HashMap<CompactString, u32, RandomState>, key: &[u8]) -> u32 {
    if let Ok(key) = std::str::from_utf8(key) {
        return map.get(key).copied().unwrap_or_default();
    }

    let key = channel_name(key);
    map.get(&key).copied().unwrap_or_default()
}

#[inline]
fn channel_name(channel: &[u8]) -> CompactString {
    match std::str::from_utf8(channel) {
        Ok(channel) => CompactString::from(channel),
        Err(_) => CompactString::from_utf8_lossy(channel),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::{
        COORDINATOR_SHARD_ID, CrossShardBus, FANOUT_QUEUE_CAPACITY, GlobalChannelIndex,
        PublishOutcome, ShardChannelRouter, ShardFanOut, ShardRouteError,
    };
    use bytes::Bytes;
    use senko_pubsub::{MessageKind, PubSubMessage};

    fn make_shards(num_shards: usize) -> Vec<ShardFanOut> {
        let bus = Arc::new(CrossShardBus::new(num_shards));
        (0..num_shards)
            .map(|shard_id| ShardFanOut::new(shard_id, Arc::clone(&bus)))
            .collect()
    }

    fn tick_all(shards: &mut [ShardFanOut]) -> usize {
        shards.iter_mut().map(ShardFanOut::drain_bus).sum()
    }

    fn flush_bus(shards: &mut [ShardFanOut]) {
        while tick_all(shards) != 0 {}
    }

    #[test]
    fn cross_shard_publish_reaches_remote_subscriber_within_two_ticks() {
        let mut shards = make_shards(4);
        let slot = shards[3].subscribe(b"news", 1);

        assert_eq!(shards[COORDINATOR_SHARD_ID].drain_bus(), 1);
        assert!(tick_all(&mut shards) >= 1);

        let outcome = shards[1].publish(b"news", Bytes::from_static(b"hello"));
        assert_eq!(
            outcome,
            PublishOutcome {
                delivered: 1,
                cache_miss: false
            }
        );

        assert!(slot.recv().is_none());
        shards[3].drain_bus();
        let message = slot.recv().expect("remote publish delivered");
        assert_eq!(message.payload, Bytes::from_static(b"hello"));
    }

    #[test]
    fn fanout_arc_refcount_is_per_receiving_shard_not_per_subscriber() {
        let mut shards = make_shards(4);
        let slot_a = shards[2].subscribe(b"fanout", 10);
        let slot_b = shards[2].subscribe(b"fanout", 11);
        let slot_c = shards[3].subscribe(b"fanout", 12);
        flush_bus(&mut shards);

        let msg = Arc::new(PubSubMessage {
            channel: "fanout".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });

        let outcome = shards[1].publish_arc(Arc::clone(&msg));
        assert_eq!(
            outcome,
            PublishOutcome {
                delivered: 3,
                cache_miss: false
            }
        );
        assert_eq!(Arc::strong_count(&msg), 3);

        shards[2].drain_bus();
        shards[3].drain_bus();
        assert!(slot_a.recv().is_some());
        assert!(slot_b.recv().is_some());
        assert!(slot_c.recv().is_some());
    }

    #[test]
    fn bus_full_drops_messages_and_increments_counter() {
        let mut shards = make_shards(4);
        let _slot = shards[3].subscribe(b"backpressure", 1);
        flush_bus(&mut shards);

        for index in 0..(FANOUT_QUEUE_CAPACITY as u64 + 1) {
            let payload = Bytes::copy_from_slice(&index.to_le_bytes());
            let _ = shards[1].publish(b"backpressure", payload);
        }

        assert_eq!(shards[1].dropped_messages(), 1);
        assert_eq!(shards[3].drain_bus(), FANOUT_QUEUE_CAPACITY);
    }

    #[test]
    fn bus_drains_all_buffered_messages_in_order() {
        let bus = CrossShardBus::new(4);
        let receiver = bus.take_receiver(3);
        for index in 0..FANOUT_QUEUE_CAPACITY as u64 {
            let msg = Arc::new(PubSubMessage {
                channel: "ordered".into(),
                payload: Bytes::copy_from_slice(&index.to_le_bytes()),
                kind: MessageKind::Message,
            });
            bus.try_send(3, super::FanOutMsg::Message { msg })
                .expect("buffered send");
        }

        for index in 0..FANOUT_QUEUE_CAPACITY as u64 {
            let super::FanOutMsg::Message { msg } = receiver.try_recv().expect("ordered message")
            else {
                unreachable!("message variant")
            };
            let value = u64::from_le_bytes(msg.payload.as_ref().try_into().expect("payload width"));
            assert_eq!(value, index);
        }
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn global_channel_index_updates_bitmasks_and_counts() {
        let mut index = GlobalChannelIndex::default();
        let topology = index.subscribe("chan".into(), 0);
        assert_eq!(topology.shard_bitmask, 0b001);
        let topology = index.subscribe("chan".into(), 1);
        assert_eq!(topology.shard_bitmask, 0b011);
        let topology = index.subscribe("chan".into(), 2);
        assert_eq!(topology.shard_bitmask, 0b111);

        let topology = index.unsubscribe("chan".into(), 1);
        assert_eq!(topology.shard_bitmask, 0b101);
        assert_eq!(topology.subscriber_count, 2);
    }

    #[test]
    fn cache_invalidation_propagates_within_two_ticks() {
        let mut shards = make_shards(4);
        let _ = shards[2].subscribe(b"invalidate", 8);

        assert_eq!(shards[COORDINATOR_SHARD_ID].drain_bus(), 1);
        assert!(tick_all(&mut shards) >= 1);

        let topology = shards[1]
            .cache
            .channel_topology(b"invalidate")
            .expect("cached topology");
        assert!(topology.contains_shard(2));
        assert_eq!(topology.subscriber_count, 1);
    }

    #[test]
    fn spublish_routing_and_local_delivery_follow_slot_owner() {
        let mut shards = make_shards(4);
        let router = ShardChannelRouter::cluster_by_modulo(4);
        let channel = b"orders";
        let owner = router.owner_of(channel);
        let wrong = (owner + 1) % 4;

        let err = shards[wrong]
            .subscribe_shard(&router, channel, 99)
            .expect_err("wrong shard must redirect");
        assert_eq!(
            err,
            ShardRouteError::Moved {
                slot: senko_cluster::crc16_slot(channel),
                shard_id: owner
            }
        );

        let slot = shards[owner]
            .subscribe_shard(&router, channel, 100)
            .expect("local shard subscribe");
        let delivered = shards[owner]
            .spublish(&router, channel, Bytes::from_static(b"payload"))
            .expect("local shard publish");
        assert_eq!(delivered, 1);
        assert_eq!(
            slot.recv().expect("shard message").kind,
            MessageKind::SMessage
        );
    }

    #[test]
    fn end_to_end_eight_shards_hundred_channels_no_duplicates_and_ordered() {
        const SHARDS: usize = 8;
        const CHANNELS: usize = 100;
        const SUBSCRIBERS_PER_CHANNEL: usize = 10;
        const PUBLISHERS: usize = 8;
        const MESSAGES_PER_PUBLISHER: usize = 2;

        let mut shards = make_shards(SHARDS);
        let mut slots = Vec::new();

        for channel_index in 0..CHANNELS {
            let channel = format!("chan:{channel_index}");
            for subscriber_index in 0..SUBSCRIBERS_PER_CHANNEL {
                let shard_id = (channel_index + subscriber_index) % SHARDS;
                let conn_id = (channel_index * 1_000 + subscriber_index) as u64;
                let slot = shards[shard_id].subscribe(channel.as_bytes(), conn_id);
                slots.push((channel.clone(), shard_id, slot));
            }
        }
        flush_bus(&mut shards);

        for (publisher, shard) in shards.iter_mut().enumerate().take(PUBLISHERS) {
            for channel_index in 0..CHANNELS {
                let channel = format!("chan:{channel_index}");
                for sequence in 0..MESSAGES_PER_PUBLISHER {
                    let payload = format!("{publisher}:{sequence}");
                    let outcome =
                        shard.publish(channel.as_bytes(), Bytes::from(payload.into_bytes()));
                    assert_eq!(outcome.delivered, SUBSCRIBERS_PER_CHANNEL as u64);
                    assert!(!outcome.cache_miss);
                }
            }
        }
        flush_bus(&mut shards);

        for (channel, _shard_id, slot) in slots {
            let mut seen = BTreeSet::new();
            let expected = PUBLISHERS * MESSAGES_PER_PUBLISHER;
            let mut prior_by_publisher = std::collections::HashMap::<usize, usize>::new();

            for _ in 0..expected {
                let message = slot.recv().expect("expected message");
                let payload = std::str::from_utf8(message.payload.as_ref()).expect("utf8 payload");
                assert!(
                    seen.insert(payload.to_owned()),
                    "duplicate delivery for {channel}/{payload}"
                );

                let (publisher, sequence) =
                    payload.split_once(':').expect("publisher and sequence");
                let publisher = publisher.parse::<usize>().expect("publisher id");
                let sequence = sequence.parse::<usize>().expect("sequence");
                let prior = prior_by_publisher.insert(publisher, sequence);
                if let Some(previous) = prior {
                    assert_eq!(sequence, previous + 1, "publisher order violated");
                } else {
                    assert_eq!(sequence, 0, "first sequence must be zero");
                }
            }
            assert!(slot.recv().is_none());
        }
    }
}
