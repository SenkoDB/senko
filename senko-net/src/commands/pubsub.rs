use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use ahash::RandomState;
use bytes::{Bytes, BytesMut};
use compact_str::CompactString;
use hashbrown::HashMap;
use senko_core::SenkoValue;
use senko_proto::{Frame, RespSerializer};
use senko_pubsub::{BroadcastSlot, MessageKind, PubSubMessage};
use senko_store::Response;
use smallvec::SmallVec;

use crate::{
    commands::cluster::ClusterCommandState,
    commands::server::info as server_info,
    connection::{
        ConnectionFlags, ConnectionMeta, error_bytes, error_message, frame_bytes,
        serialize_response,
    },
    pubsub::fanout::ShardFanOut,
};

const PUBSUB_CONTEXT_ERROR: &str = "ERR Command not allowed inside a pub/sub context";
const UNKNOWN_PUBSUB_SUBCOMMAND: &str =
    "ERR Unknown PUBSUB subcommand or wrong number of arguments";

#[derive(Debug)]
pub(crate) struct PubSubCommandOutcome {
    pub response: Vec<u8>,
    pub close_after_write: bool,
    pub suppress_response: bool,
    pub force_send_response: bool,
}

#[derive(Debug)]
pub struct PubSubState {
    pub channel_slots: HashMap<CompactString, Arc<BroadcastSlot>, RandomState>,
    pub pattern_slots: HashMap<CompactString, Arc<BroadcastSlot>, RandomState>,
    pub shard_channel_slots: HashMap<CompactString, Arc<BroadcastSlot>, RandomState>,
    pub wake_token: Arc<AtomicBool>,
}

impl Default for PubSubState {
    fn default() -> Self {
        Self {
            channel_slots: HashMap::with_hasher(RandomState::new()),
            pattern_slots: HashMap::with_hasher(RandomState::new()),
            shard_channel_slots: HashMap::with_hasher(RandomState::new()),
            wake_token: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PubSubState {
    pub fn is_empty(&self) -> bool {
        self.channel_slots.is_empty()
            && self.pattern_slots.is_empty()
            && self.shard_channel_slots.is_empty()
    }

    pub fn total_subscriptions(&self) -> usize {
        self.channel_slots.len() + self.pattern_slots.len() + self.shard_channel_slots.len()
    }

    pub fn has_pending_messages(&self) -> bool {
        self.channel_slots.values().any(|slot| !slot.is_empty())
            || self.pattern_slots.values().any(|slot| !slot.is_empty())
            || self
                .shard_channel_slots
                .values()
                .any(|slot| !slot.is_empty())
    }

    pub fn is_lagged(&self) -> bool {
        self.channel_slots.values().any(|slot| slot.is_lagged())
            || self.pattern_slots.values().any(|slot| slot.is_lagged())
            || self
                .shard_channel_slots
                .values()
                .any(|slot| slot.is_lagged())
    }

    pub fn register_wakers(&self, cx: &Context<'_>) {
        for slot in self.channel_slots.values() {
            slot.register_waker(cx.waker());
        }
        for slot in self.pattern_slots.values() {
            slot.register_waker(cx.waker());
        }
        for slot in self.shard_channel_slots.values() {
            slot.register_waker(cx.waker());
        }
    }

    pub fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        if self.is_lagged() || self.has_pending_messages() {
            self.wake_token.store(true, Ordering::Release);
            return Poll::Ready(());
        }

        self.register_wakers(cx);
        if self.is_lagged() || self.has_pending_messages() {
            self.wake_token.store(true, Ordering::Release);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub fn take_drain_needed(&self) -> bool {
        self.wake_token.swap(false, Ordering::AcqRel)
    }
}

pub(crate) fn execute(
    command: &[u8],
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Option<Result<PubSubCommandOutcome, Vec<u8>>> {
    if eq_ascii(command, b"SUBSCRIBE") {
        return Some(handle_subscribe(args, meta, pubsub, shard_pubsub));
    }
    if eq_ascii(command, b"UNSUBSCRIBE") {
        return Some(handle_unsubscribe(args, meta, pubsub, shard_pubsub));
    }
    if eq_ascii(command, b"PSUBSCRIBE") {
        return Some(handle_psubscribe(args, meta, pubsub, shard_pubsub));
    }
    if eq_ascii(command, b"PUNSUBSCRIBE") {
        return Some(handle_punsubscribe(args, meta, pubsub, shard_pubsub));
    }
    if eq_ascii(command, b"SSUBSCRIBE") {
        return Some(handle_ssubscribe(args, meta, pubsub, shard_pubsub, cluster));
    }
    if eq_ascii(command, b"SUNSUBSCRIBE") {
        return Some(handle_sunsubscribe(
            args,
            meta,
            pubsub,
            shard_pubsub,
            cluster,
        ));
    }
    if eq_ascii(command, b"PUBLISH") {
        return Some(handle_publish(args, shard_pubsub));
    }
    if eq_ascii(command, b"SPUBLISH") {
        return Some(handle_spublish(args, shard_pubsub, cluster));
    }
    if eq_ascii(command, b"PUBSUB") {
        return Some(handle_pubsub(args, meta, shard_pubsub));
    }
    None
}

pub(crate) fn is_pubsub_context_command_allowed(command: &[u8]) -> bool {
    eq_ascii(command, b"SUBSCRIBE")
        || eq_ascii(command, b"UNSUBSCRIBE")
        || eq_ascii(command, b"PSUBSCRIBE")
        || eq_ascii(command, b"PUNSUBSCRIBE")
        || eq_ascii(command, b"SSUBSCRIBE")
        || eq_ascii(command, b"SUNSUBSCRIBE")
        || eq_ascii(command, b"PING")
        || eq_ascii(command, b"RESET")
        || eq_ascii(command, b"QUIT")
        || eq_ascii(command, b"PUBLISH")
        || eq_ascii(command, b"SPUBLISH")
}

pub(crate) fn pubsub_context_error() -> Vec<u8> {
    error_message(PUBSUB_CONTEXT_ERROR)
}

pub(crate) fn cleanup_pubsub_state(
    conn_id: u64,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) {
    let Some(state) = pubsub.take() else {
        return;
    };

    let local_shard = shard_pubsub.borrow().shard_id();
    for channel in state.channel_slots.keys() {
        let _ = shard_pubsub
            .borrow_mut()
            .unsubscribe(channel.as_bytes(), conn_id);
    }
    for pattern in state.pattern_slots.keys() {
        let _ = shard_pubsub
            .borrow_mut()
            .punsubscribe(pattern.as_bytes(), conn_id);
    }
    for channel in state.shard_channel_slots.keys() {
        let target_shard = shard_channel_owner(&cluster.borrow(), channel.as_bytes());
        if target_shard == local_shard {
            let _ = shard_pubsub
                .borrow_mut()
                .unsubscribe_shard_local(channel.as_bytes(), conn_id);
        } else {
            let _ = server_info::shard_pubsub_unsubscribe(
                target_shard,
                Bytes::copy_from_slice(channel.as_bytes()),
                conn_id,
            );
        }
    }
}

pub(crate) fn drain_pubsub_messages(state: &PubSubState, resp3: bool) -> Vec<Vec<u8>> {
    let mut outbound = Vec::new();
    drain_slot_map(&state.channel_slots, resp3, &mut outbound);
    drain_slot_map(&state.pattern_slots, resp3, &mut outbound);
    drain_slot_map(&state.shard_channel_slots, resp3, &mut outbound);
    outbound
}

pub(crate) fn lagged_disconnect_frame() -> Vec<u8> {
    error_message("ERR Client is lagged out")
}

fn handle_subscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'subscribe' command",
        ));
    }

    let state = pubsub.get_or_insert_with(PubSubState::default);
    let mut response = Vec::new();
    let mut fanout = shard_pubsub.borrow_mut();

    for arg in args {
        let channel = compact_frame(arg)?;
        let slot = fanout.subscribe(channel.as_bytes(), meta.id);
        state.channel_slots.insert(channel.clone(), slot);
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"subscribe",
            channel.as_bytes(),
            state.total_subscriptions(),
            meta.resp_version == 3,
        ));
    }

    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_unsubscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let mut response = Vec::new();
    let Some(state) = pubsub.as_mut() else {
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"unsubscribe",
            b"",
            0,
            meta.resp_version == 3,
        ));
        return Ok(outcome(response));
    };

    let channels = if args.is_empty() {
        if state.channel_slots.is_empty() {
            vec![CompactString::default()]
        } else {
            sorted_keys(&state.channel_slots)
        }
    } else {
        collect_names(args)?
    };

    let mut fanout = shard_pubsub.borrow_mut();
    for channel in channels {
        state.channel_slots.remove(channel.as_str());
        let _ = fanout.unsubscribe(channel.as_bytes(), meta.id);
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"unsubscribe",
            channel.as_bytes(),
            remaining_total(state),
            meta.resp_version == 3,
        ));
    }

    if state.is_empty() {
        *pubsub = None;
    }
    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_psubscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'psubscribe' command",
        ));
    }

    let state = pubsub.get_or_insert_with(PubSubState::default);
    let mut response = Vec::new();
    let mut fanout = shard_pubsub.borrow_mut();

    for arg in args {
        let pattern = compact_frame(arg)?;
        let slot = fanout.psubscribe(pattern.as_bytes(), meta.id);
        state.pattern_slots.insert(pattern.clone(), slot);
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"psubscribe",
            pattern.as_bytes(),
            state.total_subscriptions(),
            meta.resp_version == 3,
        ));
    }

    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_punsubscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let mut response = Vec::new();
    let Some(state) = pubsub.as_mut() else {
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"punsubscribe",
            b"",
            0,
            meta.resp_version == 3,
        ));
        return Ok(outcome(response));
    };

    let patterns = if args.is_empty() {
        if state.pattern_slots.is_empty() {
            vec![CompactString::default()]
        } else {
            sorted_keys(&state.pattern_slots)
        }
    } else {
        collect_names(args)?
    };

    let mut fanout = shard_pubsub.borrow_mut();
    for pattern in patterns {
        state.pattern_slots.remove(pattern.as_str());
        let _ = fanout.punsubscribe(pattern.as_bytes(), meta.id);
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"punsubscribe",
            pattern.as_bytes(),
            remaining_total(state),
            meta.resp_version == 3,
        ));
    }

    if state.is_empty() {
        *pubsub = None;
    }
    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_ssubscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    if args.is_empty() {
        return Err(error_message(
            "ERR wrong number of arguments for 'ssubscribe' command",
        ));
    }

    let state = pubsub.get_or_insert_with(PubSubState::default);
    let mut response = Vec::new();
    let local_shard = shard_pubsub.borrow().shard_id();
    let cluster = cluster.borrow();

    for arg in args {
        let channel = compact_frame(arg)?;
        let target_shard = shard_channel_owner(&cluster, channel.as_bytes());
        let slot = if target_shard == local_shard {
            shard_pubsub
                .borrow_mut()
                .subscribe_shard_local(channel.as_bytes(), meta.id)
        } else {
            server_info::shard_pubsub_subscribe(
                target_shard,
                Bytes::copy_from_slice(channel.as_bytes()),
                meta.id,
            )?
        };
        state.shard_channel_slots.insert(channel.clone(), slot);
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"ssubscribe",
            channel.as_bytes(),
            state.total_subscriptions(),
            meta.resp_version == 3,
        ));
    }

    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_sunsubscribe(
    args: &[Frame<'_>],
    meta: &mut ConnectionMeta,
    pubsub: &mut Option<PubSubState>,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let mut response = Vec::new();
    let Some(state) = pubsub.as_mut() else {
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"sunsubscribe",
            b"",
            0,
            meta.resp_version == 3,
        ));
        return Ok(outcome(response));
    };

    let channels = if args.is_empty() {
        if state.shard_channel_slots.is_empty() {
            vec![CompactString::default()]
        } else {
            sorted_keys(&state.shard_channel_slots)
        }
    } else {
        collect_names(args)?
    };

    let local_shard = shard_pubsub.borrow().shard_id();
    let cluster = cluster.borrow();
    for channel in channels {
        if !channel.is_empty() {
            let target_shard = shard_channel_owner(&cluster, channel.as_bytes());
            if target_shard == local_shard {
                let _ = shard_pubsub
                    .borrow_mut()
                    .unsubscribe_shard_local(channel.as_bytes(), meta.id);
            } else {
                server_info::shard_pubsub_unsubscribe(
                    target_shard,
                    Bytes::copy_from_slice(channel.as_bytes()),
                    meta.id,
                )?;
            }
            state.shard_channel_slots.remove(channel.as_str());
        }
        response.extend_from_slice(&serialize_subscription_confirmation(
            b"sunsubscribe",
            channel.as_bytes(),
            remaining_total(state),
            meta.resp_version == 3,
        ));
    }

    if state.is_empty() {
        *pubsub = None;
    }
    update_pubsub_mode(meta, pubsub);
    Ok(outcome(response))
}

fn handle_publish(
    args: &[Frame<'_>],
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let [channel, payload] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'publish' command",
        ));
    };
    let channel = frame_bytes(channel).map_err(|error| error_bytes(&error))?;
    let payload = frame_bytes(payload).map_err(|error| error_bytes(&error))?;
    let delivered = shard_pubsub
        .borrow_mut()
        .publish(channel, Bytes::copy_from_slice(payload))
        .delivered;
    Ok(outcome(integer_response(delivered as i64)))
}

fn handle_spublish(
    args: &[Frame<'_>],
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
    cluster: &Rc<RefCell<ClusterCommandState>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let [channel, payload] = args else {
        return Err(error_message(
            "ERR wrong number of arguments for 'spublish' command",
        ));
    };
    let channel = frame_bytes(channel).map_err(|error| error_bytes(&error))?;
    let payload = frame_bytes(payload).map_err(|error| error_bytes(&error))?;
    let local_shard = shard_pubsub.borrow().shard_id();
    let target_shard = shard_channel_owner(&cluster.borrow(), channel);
    let delivered = if target_shard == local_shard {
        shard_pubsub
            .borrow_mut()
            .spublish_local(channel, Bytes::copy_from_slice(payload))
    } else {
        server_info::shard_pubsub_publish(
            target_shard,
            Bytes::copy_from_slice(channel),
            Bytes::copy_from_slice(payload),
        )?
    };
    Ok(outcome(integer_response(delivered as i64)))
}

fn handle_pubsub(
    args: &[Frame<'_>],
    meta: &ConnectionMeta,
    shard_pubsub: &Rc<RefCell<ShardFanOut>>,
) -> Result<PubSubCommandOutcome, Vec<u8>> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND));
    };
    let subcommand = frame_bytes(subcommand).map_err(|error| error_bytes(&error))?;
    let fanout = shard_pubsub.borrow_mut();

    let response = if eq_ascii(subcommand, b"CHANNELS") {
        if rest.len() > 1 {
            return Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND));
        }
        let pattern = optional_bytes(rest)?;
        let channels = fanout.pubsub_channels(pattern);
        serialize_response(
            &bulk_array(channels.iter().map(|name| name.as_bytes())),
            meta.resp_version == 3,
        )
    } else if eq_ascii(subcommand, b"NUMSUB") {
        let mut items = SmallVec::new();
        for channel in rest {
            let channel = frame_bytes(channel).map_err(|error| error_bytes(&error))?;
            items.push(response_bulk(channel));
            items.push(Response::Integer(i64::from(fanout.pubsub_numsub(channel))));
        }
        serialize_response(&Response::Array(Box::new(items)), meta.resp_version == 3)
    } else if eq_ascii(subcommand, b"NUMPAT") {
        if !rest.is_empty() {
            return Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND));
        }
        integer_response(i64::from(fanout.pubsub_numpat()))
    } else if eq_ascii(subcommand, b"SHARDCHANNELS") {
        if rest.len() > 1 {
            return Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND));
        }
        let pattern = optional_bytes(rest)?;
        let channels = fanout.shard_channels(pattern);
        serialize_response(
            &bulk_array(channels.iter().map(|name| name.as_bytes())),
            meta.resp_version == 3,
        )
    } else if eq_ascii(subcommand, b"SHARDNUMSUB") {
        let mut items = SmallVec::new();
        for channel in rest {
            let channel = frame_bytes(channel).map_err(|error| error_bytes(&error))?;
            items.push(response_bulk(channel));
            items.push(Response::Integer(i64::from(fanout.shard_numsub(channel))));
        }
        serialize_response(&Response::Array(Box::new(items)), meta.resp_version == 3)
    } else {
        return Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND));
    };

    Ok(outcome(response))
}

fn drain_slot_map(
    slots: &HashMap<CompactString, Arc<BroadcastSlot>, RandomState>,
    resp3: bool,
    outbound: &mut Vec<Vec<u8>>,
) {
    let mut ordered = slots.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    for (_, slot) in ordered {
        while let Some(message) = slot.recv() {
            outbound.push(serialize_pubsub_message(&message, resp3));
        }
    }
}

fn serialize_subscription_confirmation(
    kind: &[u8],
    name: &[u8],
    total_subscriptions: usize,
    resp3: bool,
) -> Vec<u8> {
    let total = total_subscriptions as i64;
    serialize_pubsub_frame(
        &[
            PubSubItem::Bulk(kind),
            PubSubItem::Bulk(name),
            PubSubItem::Integer(total),
        ],
        resp3,
    )
}

fn serialize_pubsub_message(message: &PubSubMessage, resp3: bool) -> Vec<u8> {
    match &message.kind {
        MessageKind::Message => serialize_pubsub_frame(
            &[
                PubSubItem::Bulk(b"message"),
                PubSubItem::Bulk(message.channel.as_bytes()),
                PubSubItem::Bulk(message.payload.as_ref()),
            ],
            resp3,
        ),
        MessageKind::SMessage => serialize_pubsub_frame(
            &[
                PubSubItem::Bulk(b"smessage"),
                PubSubItem::Bulk(message.channel.as_bytes()),
                PubSubItem::Bulk(message.payload.as_ref()),
            ],
            resp3,
        ),
        MessageKind::PMessage { pattern } => serialize_pubsub_frame(
            &[
                PubSubItem::Bulk(b"pmessage"),
                PubSubItem::Bulk(pattern.as_bytes()),
                PubSubItem::Bulk(message.channel.as_bytes()),
                PubSubItem::Bulk(message.payload.as_ref()),
            ],
            resp3,
        ),
    }
}

enum PubSubItem<'a> {
    Bulk(&'a [u8]),
    Integer(i64),
}

fn serialize_pubsub_frame(items: &[PubSubItem<'_>], resp3: bool) -> Vec<u8> {
    let mut out = BytesMut::with_capacity(64);
    if resp3 {
        out.extend_from_slice(format!(">{}\r\n", items.len()).as_bytes());
    } else {
        RespSerializer::write_array_header(&mut out, items.len());
    }
    for item in items {
        match item {
            PubSubItem::Bulk(value) => RespSerializer::write_bulk_string(&mut out, value),
            PubSubItem::Integer(value) => RespSerializer::write_integer(&mut out, *value),
        }
    }
    out.to_vec()
}

fn shard_channel_owner(cluster: &ClusterCommandState, channel: &[u8]) -> usize {
    if !cluster.is_enabled() {
        return 0;
    }
    let slot = senko_cluster::crc16_slot(channel);
    usize::from(cluster.slot_table().entry(slot).shard_index)
}

fn update_pubsub_mode(meta: &mut ConnectionMeta, pubsub: &Option<PubSubState>) {
    if pubsub.as_ref().is_some_and(|state| !state.is_empty()) {
        meta.flags.insert(ConnectionFlags::PUBSUB);
    } else {
        meta.flags.remove(ConnectionFlags::PUBSUB);
    }
}

fn remaining_total(state: &PubSubState) -> usize {
    state.total_subscriptions()
}

fn optional_bytes<'a>(args: &'a [Frame<'a>]) -> Result<Option<&'a [u8]>, Vec<u8>> {
    match args {
        [] => Ok(None),
        [value] => frame_bytes(value)
            .map(Some)
            .map_err(|error| error_bytes(&error)),
        _ => Err(error_message(UNKNOWN_PUBSUB_SUBCOMMAND)),
    }
}

fn collect_names(args: &[Frame<'_>]) -> Result<Vec<CompactString>, Vec<u8>> {
    args.iter().map(compact_frame).collect()
}

fn compact_frame(frame: &Frame<'_>) -> Result<CompactString, Vec<u8>> {
    let bytes = frame_bytes(frame).map_err(|error| error_bytes(&error))?;
    Ok(match std::str::from_utf8(bytes) {
        Ok(value) => CompactString::from(value),
        Err(_) => CompactString::from_utf8_lossy(bytes),
    })
}

fn sorted_keys(
    map: &HashMap<CompactString, Arc<BroadcastSlot>, RandomState>,
) -> Vec<CompactString> {
    let mut keys = map.keys().cloned().collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

fn response_bulk(value: &[u8]) -> Response {
    Response::Value(Some(SenkoValue::Raw(Bytes::copy_from_slice(value))))
}

fn bulk_array<'a>(values: impl Iterator<Item = &'a [u8]>) -> Response {
    let mut items = SmallVec::new();
    for value in values {
        items.push(response_bulk(value));
    }
    Response::Array(Box::new(items))
}

fn integer_response(value: i64) -> Vec<u8> {
    let mut out = BytesMut::with_capacity(32);
    RespSerializer::write_integer(&mut out, value);
    out.to_vec()
}

fn outcome(response: Vec<u8>) -> PubSubCommandOutcome {
    PubSubCommandOutcome {
        response,
        close_after_write: false,
        suppress_response: false,
        force_send_response: false,
    }
}

fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::{
        PubSubState, cleanup_pubsub_state, drain_pubsub_messages, execute,
        is_pubsub_context_command_allowed, lagged_disconnect_frame, pubsub_context_error,
    };
    use crate::{
        commands::cluster::ClusterCommandState,
        connection::{ConnectionFlags, ConnectionMeta, ReplyMode},
        pubsub::fanout::{CrossShardBus, ShardFanOut},
    };
    use compact_str::CompactString;
    use senko_proto::Frame;
    use std::{
        cell::RefCell,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        rc::Rc,
        sync::Arc,
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    fn meta() -> ConnectionMeta {
        ConnectionMeta {
            id: 9,
            username: CompactString::const_new("default"),
            name: None,
            db: 0,
            flags: ConnectionFlags::empty(),
            created_at: 0,
            last_cmd: None,
            last_cmd_at: 0,
            lib_name: None,
            lib_ver: None,
            peer_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4000),
            local_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379),
            resp_version: 2,
            no_evict: false,
            no_touch: false,
            reply_mode: ReplyMode::Normal,
            watch_count: 0,
            multi_queue_len: -1,
            tracking_redirect: -1,
            tracking_optin: false,
            tracking_optout: false,
            tracking_bcast: false,
            tracking_noloop: false,
            tracking_prefixes: smallvec::SmallVec::new(),
            tracking_caching: None,
            replica_listening_port: None,
            replica_ip_address: None,
            replica_psync2: false,
            replica_eof: false,
            replica_ack_offset: 0,
            last_write_replication_offset: 0,
        }
    }

    fn shard_pubsub() -> Rc<RefCell<ShardFanOut>> {
        Rc::new(RefCell::new(ShardFanOut::new(
            0,
            Arc::new(CrossShardBus::new(1)),
        )))
    }

    #[test]
    fn subscribe_publish_unsubscribe_round_trip() {
        let mut meta = meta();
        let fanout = shard_pubsub();
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            meta.local_addr,
            0,
        )));
        let mut state = None;

        let subscribe = execute(
            b"SUBSCRIBE",
            &[bs(b"a"), bs(b"b"), bs(b"c")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            subscribe.response,
            concat!(
                "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n",
                "*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n",
                "*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:3\r\n"
            )
            .as_bytes()
        );
        assert!(meta.flags.contains(ConnectionFlags::PUBSUB));

        let publish = execute(
            b"PUBLISH",
            &[bs(b"b"), bs(b"payload")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(publish.response, b":1\r\n");

        let delivered = drain_pubsub_messages(state.as_ref().unwrap(), false);
        assert_eq!(
            delivered,
            vec![b"*3\r\n$7\r\nmessage\r\n$1\r\nb\r\n$7\r\npayload\r\n".to_vec()]
        );

        let unsubscribe = execute(
            b"UNSUBSCRIBE",
            &[bs(b"b")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            unsubscribe.response,
            b"*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:2\r\n"
        );
    }

    #[test]
    fn pattern_subscriptions_deliver_pmessages() {
        let mut meta = meta();
        let fanout = shard_pubsub();
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            meta.local_addr,
            0,
        )));
        let mut state = None;

        let _ = execute(
            b"PSUBSCRIBE",
            &[bs(b"news.*")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let _ = execute(
            b"PUBLISH",
            &[bs(b"news.sports"), bs(b"hello")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();

        let delivered = drain_pubsub_messages(state.as_ref().unwrap(), false);
        assert_eq!(
            delivered,
            vec![
                b"*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$11\r\nnews.sports\r\n$5\r\nhello\r\n"
                    .to_vec()
            ]
        );
    }

    #[test]
    fn shard_subscriptions_are_separate_from_global_publish() {
        let mut meta = meta();
        let fanout = shard_pubsub();
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            meta.local_addr,
            0,
        )));
        let mut state = None;

        let _ = execute(
            b"SSUBSCRIBE",
            &[bs(b"orders")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let publish = execute(
            b"PUBLISH",
            &[bs(b"orders"), bs(b"global")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(publish.response, b":0\r\n");
        assert!(drain_pubsub_messages(state.as_ref().unwrap(), false).is_empty());

        let spublish = execute(
            b"SPUBLISH",
            &[bs(b"orders"), bs(b"shipped")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(spublish.response, b":1\r\n");
        assert_eq!(
            drain_pubsub_messages(state.as_ref().unwrap(), false),
            vec![b"*3\r\n$8\r\nsmessage\r\n$6\r\norders\r\n$7\r\nshipped\r\n".to_vec()]
        );
    }

    #[test]
    fn pubsub_mode_forbidden_command_uses_exact_error() {
        assert!(is_pubsub_context_command_allowed(b"PUBLISH"));
        assert!(!is_pubsub_context_command_allowed(b"GET"));
        assert_eq!(
            pubsub_context_error(),
            b"-ERR Command not allowed inside a pub/sub context\r\n"
        );
    }

    #[test]
    fn pubsub_introspection_reports_global_and_shard_counts() {
        let mut meta = meta();
        let fanout = shard_pubsub();
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            meta.local_addr,
            0,
        )));
        let mut state = None;

        let _ = execute(
            b"SUBSCRIBE",
            &[bs(b"news.sports"), bs(b"news.tech")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let _ = execute(
            b"PSUBSCRIBE",
            &[bs(b"news.*")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let _ = execute(
            b"SSUBSCRIBE",
            &[bs(b"orders")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();

        let channels = execute(
            b"PUBSUB",
            &[bs(b"CHANNELS"), bs(b"news.*")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            channels.response,
            b"*2\r\n$11\r\nnews.sports\r\n$9\r\nnews.tech\r\n"
        );

        let numsub = execute(
            b"PUBSUB",
            &[bs(b"NUMSUB"), bs(b"news.sports"), bs(b"missing")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            numsub.response,
            b"*4\r\n$11\r\nnews.sports\r\n:1\r\n$7\r\nmissing\r\n:0\r\n"
        );

        let numpat = execute(
            b"PUBSUB",
            &[bs(b"NUMPAT")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(numpat.response, b":1\r\n");

        let shardchannels = execute(
            b"PUBSUB",
            &[bs(b"SHARDCHANNELS")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        assert_eq!(shardchannels.response, b"*1\r\n$6\r\norders\r\n");
    }

    #[test]
    fn cleanup_unsubscribes_everything() {
        let mut meta = meta();
        let fanout = shard_pubsub();
        let cluster = Rc::new(RefCell::new(ClusterCommandState::disabled(
            meta.local_addr,
            0,
        )));
        let mut state = None;

        let _ = execute(
            b"SUBSCRIBE",
            &[bs(b"chan")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let _ = execute(
            b"PSUBSCRIBE",
            &[bs(b"p*")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();
        let _ = execute(
            b"SSUBSCRIBE",
            &[bs(b"orders")],
            &mut meta,
            &mut state,
            &fanout,
            &cluster,
        )
        .unwrap()
        .unwrap();

        cleanup_pubsub_state(meta.id, &mut state, &fanout, &cluster);
        assert!(state.is_none());
        assert_eq!(fanout.borrow().pubsub_numsub(b"chan"), 0);
        assert_eq!(fanout.borrow().pubsub_numpat(), 0);
        assert_eq!(fanout.borrow().shard_numsub(b"orders"), 0);
    }

    #[test]
    fn lagged_state_is_visible_to_connection() {
        let mut state = PubSubState::default();
        let fanout = shard_pubsub();
        let slot = fanout.borrow_mut().subscribe(b"lag", 55);
        state
            .channel_slots
            .insert(CompactString::from("lag"), Arc::clone(&slot));
        for index in 0..=senko_pubsub::RING_SIZE {
            let payload = format!("{index}");
            let _ = fanout.borrow_mut().publish(b"lag", payload.into());
        }

        assert!(state.is_lagged());
        assert_eq!(lagged_disconnect_frame(), b"-ERR Client is lagged out\r\n");
    }
}
