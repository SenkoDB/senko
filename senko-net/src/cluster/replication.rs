use std::io;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use compact_str::CompactString;
use compio::{
    BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use rand::{RngCore, SeedableRng, rngs::SmallRng};
use senko_cluster::NodeId;
use senko_core::SenkoError;
use senko_proto::{AggregateKind, Frame, ParseStatus, RespParser};
use senko_store::{
    ReplicationSnapshotEntry, SetOptions, Store, commands::generic::migrate::restore_value,
};

use crate::dispatch;

pub const REPL_HELLO_MAGIC: [u8; 4] = *b"SNKR";
pub const REPL_VERSION: u8 = 1;
pub const DEFAULT_REPL_BACKLOG_SIZE: usize = 256 * 1024 * 1024;
pub const DEFAULT_ACK_INTERVAL_MS: u64 = 1_000;
pub const DEFAULT_ACK_BYTES: usize = 65_536;
pub const REPL_HELLO_LEN: usize = 4 + 1 + 20 + 16 + 8 + 8;
pub const REPL_HELLO_ACK_LEN: usize = 1 + 16 + 8;
pub const REPL_FRAME_HEADER_LEN: usize = 1 + 4 + 2 + 8;

#[derive(Debug)]
pub enum ReplError {
    OffsetTooOld { requested: u64, oldest: u64 },
    NoData { requested: u64, head: u64 },
    EntryTooLarge { len: usize, capacity: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidStatus(u8),
    InvalidFrameType(u8),
    InvalidShard(u16),
    InvalidShardMask(u64),
    Truncated,
    TrailingBytes(usize),
    SnapshotDecode,
    Protocol(&'static str),
    Store(SenkoError),
    Io(String),
}

impl std::fmt::Display for ReplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OffsetTooOld { requested, oldest } => {
                write!(
                    f,
                    "requested replication offset {requested} is older than backlog tail {oldest}"
                )
            }
            Self::NoData { requested, head } => {
                write!(
                    f,
                    "requested replication offset {requested} is beyond head {head}"
                )
            }
            Self::EntryTooLarge { len, capacity } => {
                write!(
                    f,
                    "replication entry of {len} bytes exceeds backlog capacity {capacity}"
                )
            }
            Self::InvalidMagic(magic) => write!(f, "invalid replication magic {magic:?}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported replication protocol version {version}")
            }
            Self::InvalidStatus(status) => {
                write!(f, "invalid replication hello ack status {status}")
            }
            Self::InvalidFrameType(frame_type) => {
                write!(f, "invalid replication frame type {frame_type}")
            }
            Self::InvalidShard(shard) => write!(f, "invalid replication shard {shard}"),
            Self::InvalidShardMask(mask) => write!(f, "invalid shard mask 0x{mask:016x}"),
            Self::Truncated => write!(f, "truncated replication payload"),
            Self::TrailingBytes(count) => write!(f, "{count} trailing replication bytes"),
            Self::SnapshotDecode => write!(f, "invalid snapshot payload"),
            Self::Protocol(message) => write!(f, "{message}"),
            Self::Store(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<SenkoError> for ReplError {
    fn from(error: SenkoError) -> Self {
        Self::Store(error)
    }
}

impl From<io::Error> for ReplError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplHelloStatus {
    Ok = 0,
    FullSync = 1,
    Wait = 2,
}

impl ReplHelloStatus {
    fn decode(value: u8) -> Result<Self, ReplError> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::FullSync),
            2 => Ok(Self::Wait),
            _ => Err(ReplError::InvalidStatus(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplHello {
    pub replica_id: NodeId,
    pub repl_id: [u8; 16],
    pub offset: u64,
    pub shard_mask: u64,
}

impl ReplHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(REPL_HELLO_LEN);
        out.extend_from_slice(&REPL_HELLO_MAGIC);
        out.push(REPL_VERSION);
        out.extend_from_slice(self.replica_id.as_bytes());
        out.extend_from_slice(&self.repl_id);
        out.extend_from_slice(&self.offset.to_be_bytes());
        out.extend_from_slice(&self.shard_mask.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplError> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor.take::<4>()?;
        if magic != REPL_HELLO_MAGIC {
            return Err(ReplError::InvalidMagic(magic));
        }
        let version = cursor.take_u8()?;
        if version != REPL_VERSION {
            return Err(ReplError::UnsupportedVersion(version));
        }
        let hello = Self {
            replica_id: NodeId::new(cursor.take::<20>()?),
            repl_id: cursor.take::<16>()?,
            offset: cursor.take_u64()?,
            shard_mask: cursor.take_u64()?,
        };
        if hello.shard_mask == 0 {
            return Err(ReplError::InvalidShardMask(hello.shard_mask));
        }
        cursor.finish()?;
        Ok(hello)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplHelloAck {
    pub status: ReplHelloStatus,
    pub repl_id: [u8; 16],
    pub offset: u64,
}

impl ReplHelloAck {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(REPL_HELLO_ACK_LEN);
        out.push(self.status as u8);
        out.extend_from_slice(&self.repl_id);
        out.extend_from_slice(&self.offset.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplError> {
        let mut cursor = Cursor::new(bytes);
        let ack = Self {
            status: ReplHelloStatus::decode(cursor.take_u8()?)?,
            repl_id: cursor.take::<16>()?,
            offset: cursor.take_u64()?,
        };
        cursor.finish()?;
        Ok(ack)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplFrameType {
    Command = 0,
    Snapshot = 1,
    SnapshotEnd = 2,
    Ack = 3,
}

impl ReplFrameType {
    fn decode(value: u8) -> Result<Self, ReplError> {
        match value {
            0 => Ok(Self::Command),
            1 => Ok(Self::Snapshot),
            2 => Ok(Self::SnapshotEnd),
            3 => Ok(Self::Ack),
            _ => Err(ReplError::InvalidFrameType(value)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplFrame {
    pub frame_type: ReplFrameType,
    pub shard: u16,
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl ReplFrame {
    pub fn command(shard: u16, offset: u64, payload: Vec<u8>) -> Self {
        Self {
            frame_type: ReplFrameType::Command,
            shard,
            offset,
            payload,
        }
    }

    pub fn snapshot(shard: u16, offset: u64, payload: Vec<u8>) -> Self {
        Self {
            frame_type: ReplFrameType::Snapshot,
            shard,
            offset,
            payload,
        }
    }

    pub fn snapshot_end(shard: u16, snap_offset: u64) -> Self {
        Self {
            frame_type: ReplFrameType::SnapshotEnd,
            shard,
            offset: snap_offset,
            payload: snap_offset.to_be_bytes().to_vec(),
        }
    }

    pub fn ack(offset: u64) -> Self {
        Self {
            frame_type: ReplFrameType::Ack,
            shard: 0,
            offset,
            payload: Vec::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(REPL_FRAME_HEADER_LEN + self.payload.len());
        out.push(self.frame_type as u8);
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.shard.to_be_bytes());
        out.extend_from_slice(&self.offset.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplError> {
        let mut cursor = Cursor::new(bytes);
        let frame_type = ReplFrameType::decode(cursor.take_u8()?)?;
        let len = cursor.take_u32()? as usize;
        let shard = cursor.take_u16()?;
        let offset = cursor.take_u64()?;
        let payload = cursor.take_vec(len)?;
        cursor.finish()?;
        Ok(Self {
            frame_type,
            shard,
            offset,
            payload,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub key: CompactString,
    pub dump: Vec<u8>,
    pub expires_at: Option<u64>,
}

impl SnapshotRecord {
    pub fn from_store_entry(entry: ReplicationSnapshotEntry) -> Self {
        Self {
            key: entry.key,
            dump: entry.dump.to_vec(),
            expires_at: entry.expires_at,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let key = self.key.as_bytes();
        let mut out = Vec::with_capacity(4 + key.len() + 1 + 8 + 4 + self.dump.len());
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.push(u8::from(self.expires_at.is_some()));
        out.extend_from_slice(&self.expires_at.unwrap_or(0).to_be_bytes());
        out.extend_from_slice(&(self.dump.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.dump);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ReplError> {
        let mut cursor = Cursor::new(bytes);
        let key_len = cursor.take_u32()? as usize;
        let key = CompactString::from_utf8(cursor.take_vec(key_len)?)
            .map_err(|_| ReplError::SnapshotDecode)?;
        let has_expiry = cursor.take_u8()? != 0;
        let expires_at = if has_expiry {
            Some(cursor.take_u64()?)
        } else {
            let _ = cursor.take_u64()?;
            None
        };
        let dump_len = cursor.take_u32()? as usize;
        let dump = cursor.take_vec(dump_len)?;
        cursor.finish()?;
        Ok(Self {
            key,
            dump,
            expires_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplRecord {
    pub start_offset: u64,
    pub end_offset: u64,
    pub payload: Vec<u8>,
}

pub struct ReplBuffer {
    data: Box<[AtomicU8]>,
    capacity: usize,
    head: AtomicU64,
    tail: AtomicU64,
    base_offset: u64,
    notify_epoch: AtomicU64,
    notify_lock: Mutex<u64>,
    notify_cv: Condvar,
}

impl std::fmt::Debug for ReplBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplBuffer")
            .field("capacity", &self.capacity)
            .field("head", &self.head.load(Ordering::Acquire))
            .field("tail", &self.tail.load(Ordering::Acquire))
            .field("base_offset", &self.base_offset)
            .finish()
    }
}

impl ReplBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(8);
        Self {
            data: std::iter::repeat_with(|| AtomicU8::new(0))
                .take(capacity)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            capacity,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            base_offset: 0,
            notify_epoch: AtomicU64::new(0),
            notify_lock: Mutex::new(0),
            notify_cv: Condvar::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn head_offset(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }

    pub fn tail_offset(&self) -> u64 {
        self.tail.load(Ordering::Acquire)
    }

    pub fn append(&self, cmd_bytes: &[u8]) -> u64 {
        self.try_append(cmd_bytes)
            .expect("replication backlog entry must fit configured backlog")
    }

    pub fn try_append(&self, cmd_bytes: &[u8]) -> Result<u64, ReplError> {
        let total_len = 4 + cmd_bytes.len();
        if total_len > self.capacity {
            return Err(ReplError::EntryTooLarge {
                len: total_len,
                capacity: self.capacity,
            });
        }

        let head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail.load(Ordering::Relaxed);
        while head + total_len as u64 > tail + self.capacity as u64 {
            let entry_len = self.entry_total_len_at(tail)?;
            tail = tail.saturating_add(entry_len as u64);
        }

        self.write_bytes(head, &(cmd_bytes.len() as u32).to_be_bytes());
        self.write_bytes(head + 4, cmd_bytes);

        let new_head = head + total_len as u64;
        self.tail.store(tail, Ordering::Release);
        self.head.store(new_head, Ordering::Release);
        self.notify_epoch.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut guard) = self.notify_lock.lock() {
            *guard = new_head;
            self.notify_cv.notify_all();
        }
        Ok(new_head)
    }

    pub fn read_from(&self, offset: u64, out: &mut Vec<u8>) -> Result<usize, ReplError> {
        let tail = self.tail.load(Ordering::Acquire);
        if offset < tail {
            return Err(ReplError::OffsetTooOld {
                requested: offset,
                oldest: tail,
            });
        }
        let head = self.head.load(Ordering::Acquire);
        if offset >= head {
            return Err(ReplError::NoData {
                requested: offset,
                head,
            });
        }
        if offset + 4 > head {
            return Err(ReplError::NoData {
                requested: offset,
                head,
            });
        }

        let payload_len = self.read_u32(offset)? as usize;
        let total_len = 4 + payload_len;
        let end = offset + total_len as u64;
        if end > head {
            return Err(ReplError::NoData {
                requested: offset,
                head,
            });
        }

        out.reserve(total_len);
        self.read_bytes(offset, total_len, out);
        Ok(total_len)
    }

    pub fn read_record(&self, offset: u64) -> Result<ReplRecord, ReplError> {
        let mut raw = Vec::new();
        self.read_from(offset, &mut raw)?;
        let payload_len = u32::from_be_bytes(raw[0..4].try_into().expect("4-byte length")) as usize;
        Ok(ReplRecord {
            start_offset: offset,
            end_offset: offset + 4 + payload_len as u64,
            payload: raw[4..].to_vec(),
        })
    }

    pub fn wait_for_data(&self, offset: u64, timeout: Duration) -> bool {
        if self.head_offset() > offset {
            return true;
        }
        let Ok(guard) = self.notify_lock.lock() else {
            return false;
        };
        let wait_result = self
            .notify_cv
            .wait_timeout_while(guard, timeout, |current| *current <= offset);
        match wait_result {
            Ok((guard, _)) => *guard > offset,
            Err(_) => false,
        }
    }

    fn entry_total_len_at(&self, offset: u64) -> Result<usize, ReplError> {
        let head = self.head.load(Ordering::Acquire);
        if offset + 4 > head {
            return Err(ReplError::NoData {
                requested: offset,
                head,
            });
        }
        Ok(4 + self.read_u32(offset)? as usize)
    }

    fn read_u32(&self, offset: u64) -> Result<u32, ReplError> {
        let mut bytes = [0_u8; 4];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = self
                .byte_at(offset + index as u64)
                .ok_or(ReplError::Truncated)?;
        }
        Ok(u32::from_be_bytes(bytes))
    }

    fn write_bytes(&self, offset: u64, bytes: &[u8]) {
        for (index, byte) in bytes.iter().copied().enumerate() {
            let slot = ((offset - self.base_offset + index as u64) % self.capacity as u64) as usize;
            self.data[slot].store(byte, Ordering::Relaxed);
        }
    }

    fn read_bytes(&self, offset: u64, len: usize, out: &mut Vec<u8>) {
        for index in 0..len {
            let slot = ((offset - self.base_offset + index as u64) % self.capacity as u64) as usize;
            out.push(self.data[slot].load(Ordering::Acquire));
        }
    }

    fn byte_at(&self, offset: u64) -> Option<u8> {
        if offset < self.base_offset {
            return None;
        }
        let slot = ((offset - self.base_offset) % self.capacity as u64) as usize;
        Some(self.data[slot].load(Ordering::Acquire))
    }
}

impl Default for ReplBuffer {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_REPL_BACKLOG_SIZE)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotBarrier {
    pub snap_offset: u64,
}

#[derive(Debug)]
pub struct ShardReplication {
    shard: u16,
    backlog: Arc<ReplBuffer>,
    snapshot_in_progress: AtomicBool,
    snapshot_delta: Mutex<Vec<ReplFrame>>,
}

impl ShardReplication {
    pub fn new(shard: u16, backlog_size: usize) -> Self {
        Self {
            shard,
            backlog: Arc::new(ReplBuffer::with_capacity(backlog_size)),
            snapshot_in_progress: AtomicBool::new(false),
            snapshot_delta: Mutex::new(Vec::new()),
        }
    }

    pub fn shard(&self) -> u16 {
        self.shard
    }

    pub fn backlog(&self) -> &Arc<ReplBuffer> {
        &self.backlog
    }

    pub fn append_command(&self, cmd_bytes: &[u8]) -> Result<u64, ReplError> {
        let offset = self.backlog.try_append(cmd_bytes)?;
        if self.snapshot_in_progress.load(Ordering::Acquire)
            && let Ok(mut delta) = self.snapshot_delta.lock()
        {
            delta.push(ReplFrame::command(self.shard, offset, cmd_bytes.to_vec()));
        }
        Ok(offset)
    }

    pub fn next_frame(&self, offset: u64) -> Result<ReplFrame, ReplError> {
        let record = self.backlog.read_record(offset)?;
        Ok(ReplFrame::command(
            self.shard,
            record.end_offset,
            record.payload,
        ))
    }

    pub fn start_snapshot(&self) -> SnapshotBarrier {
        self.snapshot_in_progress.store(true, Ordering::Release);
        if let Ok(mut delta) = self.snapshot_delta.lock() {
            delta.clear();
        }
        SnapshotBarrier {
            snap_offset: self.backlog.head_offset(),
        }
    }

    pub fn build_snapshot_frames(
        &self,
        store: &mut Store,
        barrier: SnapshotBarrier,
    ) -> Result<Vec<ReplFrame>, ReplError> {
        Ok(store
            .replication_snapshot()
            .into_iter()
            .map(|entry| {
                let payload = SnapshotRecord::from_store_entry(entry).encode();
                ReplFrame::snapshot(self.shard, barrier.snap_offset, payload)
            })
            .collect())
    }

    pub fn finish_snapshot(&self, barrier: SnapshotBarrier) -> Vec<ReplFrame> {
        self.snapshot_in_progress.store(false, Ordering::Release);
        let mut tail = self
            .snapshot_delta
            .lock()
            .expect("snapshot delta queue poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        tail.push(ReplFrame::snapshot_end(self.shard, barrier.snap_offset));
        tail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullSyncPlan {
    pub snapshot_frames: Vec<ReplFrame>,
    pub trailing_frames: Vec<ReplFrame>,
    pub snap_offset: u64,
}

#[derive(Debug)]
pub struct PrimaryReplicationState {
    repl_id: [u8; 16],
    shards: Vec<Arc<ShardReplication>>,
    ack_tracker: Arc<ReplicaAckTracker>,
}

impl PrimaryReplicationState {
    pub fn new(shards: Vec<Arc<ShardReplication>>) -> Self {
        let mut repl_id = [0_u8; 16];
        SmallRng::seed_from_u64(0xDEAD_BEEF_F00D_CAFE).fill_bytes(&mut repl_id);
        Self {
            repl_id,
            shards,
            ack_tracker: Arc::new(ReplicaAckTracker::default()),
        }
    }

    pub fn repl_id(&self) -> [u8; 16] {
        self.repl_id
    }

    pub fn ack_tracker(&self) -> &Arc<ReplicaAckTracker> {
        &self.ack_tracker
    }

    pub fn handshake(&self, hello: &ReplHello) -> Result<ReplHelloAck, ReplError> {
        let shard_indices = shard_indices_from_mask(hello.shard_mask, self.shards.len())?;
        let mut oldest = 0_u64;
        let mut newest = u64::MAX;
        for index in shard_indices {
            let shard = &self.shards[index];
            oldest = oldest.max(shard.backlog.tail_offset());
            newest = newest.min(shard.backlog.head_offset());
        }
        let status = if hello.offset < oldest {
            ReplHelloStatus::FullSync
        } else if hello.offset > newest {
            ReplHelloStatus::Wait
        } else {
            ReplHelloStatus::Ok
        };

        Ok(ReplHelloAck {
            status,
            repl_id: self.repl_id,
            offset: match status {
                ReplHelloStatus::Ok => hello.offset,
                ReplHelloStatus::FullSync => oldest,
                ReplHelloStatus::Wait => newest,
            },
        })
    }

    pub fn stream_available(
        &self,
        shard: usize,
        mut offset: u64,
        max_frames: usize,
    ) -> Result<Vec<ReplFrame>, ReplError> {
        let Some(replication) = self.shards.get(shard) else {
            return Err(ReplError::InvalidShard(shard as u16));
        };
        let mut frames = Vec::new();
        while frames.len() < max_frames {
            match replication.next_frame(offset) {
                Ok(frame) => {
                    offset = frame.offset;
                    frames.push(frame);
                }
                Err(ReplError::NoData { .. }) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(frames)
    }
}

#[derive(Debug, Default)]
pub struct ReplicaAckTracker {
    state: Mutex<ReplicaAckState>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct ReplicaAckState {
    offsets: std::collections::BTreeMap<NodeId, u64>,
}

impl ReplicaAckTracker {
    pub fn record_ack(&self, replica_id: NodeId, offset: u64) {
        if let Ok(mut state) = self.state.lock() {
            let current = state.offsets.entry(replica_id).or_insert(0);
            if offset > *current {
                *current = offset;
            }
            self.cv.notify_all();
        }
    }

    pub fn remove_replica(&self, replica_id: &NodeId) {
        if let Ok(mut state) = self.state.lock() {
            if state.offsets.remove(replica_id).is_some() {
                self.cv.notify_all();
            }
        }
    }

    pub fn acknowledged_offset(&self, replica_id: &NodeId) -> u64 {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.offsets.get(replica_id).copied())
            .unwrap_or(0)
    }

    pub fn count_acked_at_least(&self, offset: u64) -> usize {
        self.state
            .lock()
            .map(|state| {
                state
                    .offsets
                    .values()
                    .filter(|acked| **acked >= offset)
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn wait_for(&self, replicas: usize, offset: u64, timeout: Duration) -> usize {
        let deadline = Instant::now() + timeout;
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        while state
            .offsets
            .values()
            .filter(|acked| **acked >= offset)
            .count()
            < replicas
        {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait_for = deadline.saturating_duration_since(now);
            let Ok((next_state, result)) = self.cv.wait_timeout(state, wait_for) else {
                return 0;
            };
            state = next_state;
            if result.timed_out() {
                break;
            }
        }
        state
            .offsets
            .values()
            .filter(|acked| **acked >= offset)
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaApplyState {
    pub repl_id: [u8; 16],
    pub replication_offset: u64,
    pub shard_mask: u64,
    pub primary_addr: String,
    pub serve_stale_data: bool,
    bytes_since_ack: usize,
    last_ack_at_ms: u64,
}

impl ReplicaApplyState {
    pub fn new(
        repl_id: [u8; 16],
        shard_mask: u64,
        primary_addr: String,
        serve_stale_data: bool,
    ) -> Result<Self, ReplError> {
        if shard_mask == 0 {
            return Err(ReplError::InvalidShardMask(shard_mask));
        }
        Ok(Self {
            repl_id,
            replication_offset: 0,
            shard_mask,
            primary_addr,
            serve_stale_data,
            bytes_since_ack: 0,
            last_ack_at_ms: 0,
        })
    }

    pub fn apply_frame(
        &mut self,
        frame: &ReplFrame,
        stores: &mut [Store],
        now_ms: u64,
    ) -> Result<Option<ReplFrame>, ReplError> {
        match frame.frame_type {
            ReplFrameType::Command => {
                if !mask_contains_shard(self.shard_mask, frame.shard) {
                    return Err(ReplError::InvalidShard(frame.shard));
                }
                let Some(store) = stores.get_mut(frame.shard as usize) else {
                    return Err(ReplError::InvalidShard(frame.shard));
                };
                execute_wire_command(store, &frame.payload)?;
                self.note_progress(frame.offset, frame.payload.len(), now_ms)
            }
            ReplFrameType::Snapshot => {
                let record = SnapshotRecord::decode(&frame.payload)?;
                if !mask_contains_shard(self.shard_mask, frame.shard) {
                    return Err(ReplError::InvalidShard(frame.shard));
                }
                let Some(store) = stores.get_mut(frame.shard as usize) else {
                    return Err(ReplError::InvalidShard(frame.shard));
                };
                apply_snapshot_record(store, record)?;
                self.note_progress(frame.offset, frame.payload.len(), now_ms)
            }
            ReplFrameType::SnapshotEnd => {
                self.note_progress(frame.offset, frame.payload.len(), now_ms)
            }
            ReplFrameType::Ack => {
                self.replication_offset = self.replication_offset.max(frame.offset);
                Ok(None)
            }
        }
    }

    fn note_progress(
        &mut self,
        offset: u64,
        bytes: usize,
        now_ms: u64,
    ) -> Result<Option<ReplFrame>, ReplError> {
        self.replication_offset = self.replication_offset.max(offset);
        self.bytes_since_ack = self.bytes_since_ack.saturating_add(bytes);
        let elapsed = if self.last_ack_at_ms == 0 {
            now_ms
        } else {
            now_ms.saturating_sub(self.last_ack_at_ms)
        };
        if self.bytes_since_ack >= DEFAULT_ACK_BYTES || elapsed >= DEFAULT_ACK_INTERVAL_MS {
            self.bytes_since_ack = 0;
            self.last_ack_at_ms = now_ms;
            return Ok(Some(ReplFrame::ack(self.replication_offset)));
        }
        if self.last_ack_at_ms == 0 {
            self.last_ack_at_ms = now_ms;
        }
        Ok(None)
    }
}

pub async fn write_repl_hello(stream: &mut TcpStream, hello: &ReplHello) -> Result<(), ReplError> {
    let BufResult(result, _) = stream.write_all(hello.encode()).await;
    result?;
    Ok(())
}

pub async fn read_repl_hello(stream: &mut TcpStream) -> Result<ReplHello, ReplError> {
    let BufResult(result, buffer) = stream.read_exact(Vec::with_capacity(REPL_HELLO_LEN)).await;
    result?;
    ReplHello::decode(&buffer)
}

pub async fn write_repl_hello_ack(
    stream: &mut TcpStream,
    ack: &ReplHelloAck,
) -> Result<(), ReplError> {
    let BufResult(result, _) = stream.write_all(ack.encode()).await;
    result?;
    Ok(())
}

pub async fn read_repl_hello_ack(stream: &mut TcpStream) -> Result<ReplHelloAck, ReplError> {
    let BufResult(result, buffer) = stream
        .read_exact(Vec::with_capacity(REPL_HELLO_ACK_LEN))
        .await;
    result?;
    ReplHelloAck::decode(&buffer)
}

pub async fn write_repl_frame(stream: &mut TcpStream, frame: &ReplFrame) -> Result<(), ReplError> {
    let BufResult(result, _) = stream.write_all(frame.encode()).await;
    result?;
    Ok(())
}

pub async fn read_repl_frame(stream: &mut TcpStream) -> Result<ReplFrame, ReplError> {
    let BufResult(result, header) = stream
        .read_exact(Vec::with_capacity(REPL_FRAME_HEADER_LEN))
        .await;
    result?;
    let mut cursor = Cursor::new(&header);
    let frame_type = ReplFrameType::decode(cursor.take_u8()?)?;
    let payload_len = cursor.take_u32()? as usize;
    let shard = cursor.take_u16()?;
    let offset = cursor.take_u64()?;
    cursor.finish()?;
    let BufResult(result, payload) = stream.read_exact(Vec::with_capacity(payload_len)).await;
    result?;
    Ok(ReplFrame {
        frame_type,
        shard,
        offset,
        payload,
    })
}

pub async fn stream_backlog_from(
    stream: &mut TcpStream,
    shard: &ShardReplication,
    mut offset: u64,
    max_frames: usize,
) -> Result<u64, ReplError> {
    let mut sent = 0usize;
    while sent < max_frames {
        match shard.next_frame(offset) {
            Ok(frame) => {
                offset = frame.offset;
                write_repl_frame(stream, &frame).await?;
                sent += 1;
            }
            Err(ReplError::NoData { .. }) => break,
            Err(error) => return Err(error),
        }
    }
    Ok(offset)
}

fn execute_wire_command(store: &mut Store, wire: &[u8]) -> Result<(), ReplError> {
    let parser = RespParser::new();
    let ParseStatus::Complete(frame, used) = parser.parse(wire)? else {
        return Err(ReplError::Protocol("replication command frame incomplete"));
    };
    if used != wire.len() {
        return Err(ReplError::TrailingBytes(wire.len() - used));
    }
    let Frame::Array(aggregate) = frame else {
        return Err(ReplError::Protocol(
            "replication payload must be a command array",
        ));
    };
    if aggregate.kind() != AggregateKind::Array {
        return Err(ReplError::Protocol("replication payload must be an array"));
    }

    let mut frames = Vec::with_capacity(aggregate.len());
    for item in aggregate.iter() {
        frames.push(item?);
    }
    if frames.is_empty() {
        return Err(ReplError::Protocol("replication command array is empty"));
    }
    let command = command_name(&frames[0])?;
    let _ = dispatch::dispatch(store, command, &frames[1..])?;
    Ok(())
}

fn command_name<'a>(frame: &'a Frame<'a>) -> Result<&'a [u8], ReplError> {
    match frame {
        Frame::BulkString(value)
        | Frame::SimpleString(value)
        | Frame::SimpleError(value)
        | Frame::BigNumber(value) => Ok(value),
        _ => Err(ReplError::Protocol("command name must be a string frame")),
    }
}

fn apply_snapshot_record(store: &mut Store, record: SnapshotRecord) -> Result<(), ReplError> {
    let value = restore_value(&record.dump)?;
    let _ = store.set(record.key.clone(), value, SetOptions::default());
    if let Some(expires_at) = record.expires_at {
        store.set_expiry(record.key.as_bytes(), expires_at);
    } else {
        store.remove_expiry(record.key.as_bytes());
    }
    Ok(())
}

fn shard_indices_from_mask(mask: u64, shard_count: usize) -> Result<Vec<usize>, ReplError> {
    if mask == 0 {
        return Err(ReplError::InvalidShardMask(mask));
    }
    let mut shards = Vec::new();
    for shard in 0..shard_count {
        if (mask & (1_u64 << shard)) != 0 {
            shards.push(shard);
        }
    }
    if shards.is_empty() {
        return Err(ReplError::InvalidShardMask(mask));
    }
    Ok(shards)
}

fn mask_contains_shard(mask: u64, shard: u16) -> bool {
    shard < 64 && (mask & (1_u64 << shard)) != 0
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], ReplError> {
        if self.offset + N > self.bytes.len() {
            return Err(ReplError::Truncated);
        }
        let value = self.bytes[self.offset..self.offset + N]
            .try_into()
            .expect("slice length checked");
        self.offset += N;
        Ok(value)
    }

    fn take_u8(&mut self) -> Result<u8, ReplError> {
        Ok(self.take::<1>()?[0])
    }

    fn take_u16(&mut self) -> Result<u16, ReplError> {
        Ok(u16::from_be_bytes(self.take::<2>()?))
    }

    fn take_u32(&mut self) -> Result<u32, ReplError> {
        Ok(u32::from_be_bytes(self.take::<4>()?))
    }

    fn take_u64(&mut self) -> Result<u64, ReplError> {
        Ok(u64::from_be_bytes(self.take::<8>()?))
    }

    fn take_vec(&mut self, len: usize) -> Result<Vec<u8>, ReplError> {
        if self.offset + len > self.bytes.len() {
            return Err(ReplError::Truncated);
        }
        let value = self.bytes[self.offset..self.offset + len].to_vec();
        self.offset += len;
        Ok(value)
    }

    fn finish(&self) -> Result<(), ReplError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ReplError::TrailingBytes(self.bytes.len() - self.offset))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use compact_str::CompactString;
    use senko_core::SenkoValue;
    use senko_proto::RespSerializer;

    use super::*;

    fn set_wire(key: &str, value: &str) -> Vec<u8> {
        let mut out = BytesMut::new();
        RespSerializer::write_array_header(&mut out, 3);
        RespSerializer::write_bulk_string(&mut out, b"SET");
        RespSerializer::write_bulk_string(&mut out, key.as_bytes());
        RespSerializer::write_bulk_string(&mut out, value.as_bytes());
        out.to_vec()
    }

    fn get_string(store: &mut Store, key: &str) -> Option<Vec<u8>> {
        store
            .get(key.as_bytes())
            .map(SenkoValue::as_bytes)
            .map(|bytes| bytes.to_vec())
    }

    #[test]
    fn repl_hello_roundtrip() {
        let hello = ReplHello {
            replica_id: NodeId::new([7; 20]),
            repl_id: [9; 16],
            offset: 42,
            shard_mask: 0b101,
        };
        let ack = ReplHelloAck {
            status: ReplHelloStatus::Ok,
            repl_id: [3; 16],
            offset: 11,
        };

        assert_eq!(ReplHello::decode(&hello.encode()).unwrap(), hello);
        assert_eq!(ReplHelloAck::decode(&ack.encode()).unwrap(), ack);
    }

    #[test]
    fn repl_buffer_append_read_and_overwrite() {
        let buffer = ReplBuffer::with_capacity(32);
        let first = buffer.append(b"SET a 1");
        let _second = buffer.append(b"SET b 2");
        let third = buffer.append(b"SET c 3");

        let first_start = 0;
        let second_start = first;
        let mut payload = Vec::new();
        let _ = buffer.read_from(second_start, &mut payload).unwrap();
        assert_eq!(
            u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize,
            b"SET b 2".len()
        );

        assert!(matches!(
            buffer.read_from(first_start, &mut Vec::new()),
            Err(ReplError::OffsetTooOld { .. })
        ));
        assert_eq!(buffer.head_offset(), third);
    }

    #[test]
    fn repl_frame_roundtrip() {
        let frame = ReplFrame::snapshot(2, 99, vec![1, 2, 3, 4]);
        assert_eq!(ReplFrame::decode(&frame.encode()).unwrap(), frame);
    }

    #[test]
    fn snapshot_record_roundtrip() {
        let record = SnapshotRecord {
            key: CompactString::from("snap:key"),
            dump: vec![1, 2, 3, 4],
            expires_at: Some(123),
        };
        assert_eq!(SnapshotRecord::decode(&record.encode()).unwrap(), record);
    }

    #[test]
    fn partial_resync_replays_backlog_without_full_sync() {
        let shard = Arc::new(ShardReplication::new(0, 128 * 1024));
        let primary = PrimaryReplicationState::new(vec![Arc::clone(&shard)]);
        let mut primary_store = Store::new(None);
        let mut replica_store = Store::new(None);
        let mut replica =
            ReplicaApplyState::new(primary.repl_id(), 1, "127.0.0.1:6379".into(), false).unwrap();

        for index in 0..500 {
            let key = format!("k:{index}");
            let value = format!("v:{index}");
            let wire = set_wire(&key, &value);
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }

        let hello = ReplHello {
            replica_id: NodeId::new([1; 20]),
            repl_id: primary.repl_id(),
            offset: 0,
            shard_mask: 1,
        };
        assert_eq!(
            primary.handshake(&hello).unwrap().status,
            ReplHelloStatus::Ok
        );

        let mut offset = 0;
        for frame in primary.stream_available(0, offset, 500).unwrap() {
            offset = frame.offset;
            let _ = replica
                .apply_frame(&frame, std::slice::from_mut(&mut replica_store), 1_000)
                .unwrap();
        }

        for index in 500..1_000 {
            let key = format!("k:{index}");
            let value = format!("v:{index}");
            let wire = set_wire(&key, &value);
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }

        let reconnect = ReplHello {
            replica_id: NodeId::new([1; 20]),
            repl_id: primary.repl_id(),
            offset,
            shard_mask: 1,
        };
        assert_eq!(
            primary.handshake(&reconnect).unwrap().status,
            ReplHelloStatus::Ok
        );

        for frame in primary.stream_available(0, offset, 1_000).unwrap() {
            let _ = replica
                .apply_frame(&frame, std::slice::from_mut(&mut replica_store), 2_000)
                .unwrap();
        }

        for index in 0..1_000 {
            let key = format!("k:{index}");
            let value = format!("v:{index}").into_bytes();
            assert_eq!(get_string(&mut replica_store, &key), Some(value));
        }
    }

    #[test]
    fn full_resync_uses_snapshot_when_backlog_overflows() {
        let shard = Arc::new(ShardReplication::new(0, 1_024));
        let primary = PrimaryReplicationState::new(vec![Arc::clone(&shard)]);
        let mut primary_store = Store::new(None);
        let mut replica_store = Store::new(None);
        let mut replica =
            ReplicaApplyState::new(primary.repl_id(), 1, "127.0.0.1:6379".into(), false).unwrap();

        for index in 0..200 {
            let wire = set_wire(&format!("overflow:{index}"), &format!("value:{index}"));
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }

        let hello = ReplHello {
            replica_id: NodeId::new([2; 20]),
            repl_id: primary.repl_id(),
            offset: 0,
            shard_mask: 1,
        };
        assert_eq!(
            primary.handshake(&hello).unwrap().status,
            ReplHelloStatus::FullSync
        );

        let barrier = shard.start_snapshot();
        let snapshot_frames = shard
            .build_snapshot_frames(&mut primary_store, barrier)
            .unwrap();
        for index in 200..230 {
            let wire = set_wire(&format!("overflow:{index}"), &format!("value:{index}"));
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }
        let trailing = shard.finish_snapshot(barrier);

        for frame in snapshot_frames.into_iter().chain(trailing) {
            let _ = replica
                .apply_frame(&frame, std::slice::from_mut(&mut replica_store), 3_000)
                .unwrap();
        }

        for index in 0..230 {
            let key = format!("overflow:{index}");
            let value = format!("value:{index}").into_bytes();
            assert_eq!(get_string(&mut replica_store, &key), Some(value));
        }
    }

    #[test]
    fn snapshot_delta_replays_writes_after_snapshot_start() {
        let shard = ShardReplication::new(0, 8 * 1024);
        let mut primary_store = Store::new(None);
        let barrier = shard.start_snapshot();
        let snapshot_frames = shard
            .build_snapshot_frames(&mut primary_store, barrier)
            .unwrap();
        assert!(snapshot_frames.is_empty());

        let wire = set_wire("delta:key", "delta:value");
        execute_wire_command(&mut primary_store, &wire).unwrap();
        shard.append_command(&wire).unwrap();

        let trailing = shard.finish_snapshot(barrier);
        assert!(
            trailing
                .iter()
                .any(|frame| frame.frame_type == ReplFrameType::Command)
        );
        assert!(
            trailing
                .iter()
                .any(|frame| frame.frame_type == ReplFrameType::SnapshotEnd)
        );
    }

    #[test]
    fn replica_ack_tracker_waits_for_required_offsets() {
        let tracker = ReplicaAckTracker::default();
        let replica_a = NodeId::new([3; 20]);
        let replica_b = NodeId::new([4; 20]);

        tracker.record_ack(replica_a, 100);
        tracker.record_ack(replica_b, 50);

        assert_eq!(tracker.count_acked_at_least(75), 1);
        assert_eq!(tracker.wait_for(1, 75, Duration::from_millis(5)), 1);
        assert_eq!(tracker.wait_for(2, 75, Duration::from_millis(5)), 1);
    }

    #[test]
    fn replica_apply_emits_periodic_ack() {
        let mut store = Store::new(None);
        let mut replica =
            ReplicaApplyState::new([1; 16], 1, "127.0.0.1:6379".into(), false).unwrap();
        let payload = set_wire("ack:key", "ack:value");
        let frame = ReplFrame::command(0, 123, payload.clone());
        let ack = replica
            .apply_frame(
                &frame,
                std::slice::from_mut(&mut store),
                DEFAULT_ACK_INTERVAL_MS + 1,
            )
            .unwrap();

        assert_eq!(
            get_string(&mut store, "ack:key"),
            Some(b"ack:value".to_vec())
        );
        assert_eq!(ack, Some(ReplFrame::ack(123)));
    }

    #[test]
    fn full_sync_plan_snapshot_then_backlog_keeps_store_consistent() {
        let shard = Arc::new(ShardReplication::new(0, 16 * 1024));
        let mut primary_store = Store::new(None);
        for index in 0..20 {
            let wire = set_wire(&format!("sync:{index}"), &format!("before:{index}"));
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }

        let barrier = shard.start_snapshot();
        let snapshot_frames = shard
            .build_snapshot_frames(&mut primary_store, barrier)
            .unwrap();
        for index in 20..40 {
            let wire = set_wire(&format!("sync:{index}"), &format!("after:{index}"));
            execute_wire_command(&mut primary_store, &wire).unwrap();
            shard.append_command(&wire).unwrap();
        }
        let trailing = shard.finish_snapshot(barrier);

        let mut replica_store = Store::new(None);
        let mut replica =
            ReplicaApplyState::new([1; 16], 1, "127.0.0.1:6379".into(), false).unwrap();
        for frame in snapshot_frames.into_iter().chain(trailing.into_iter()) {
            let _ = replica
                .apply_frame(&frame, std::slice::from_mut(&mut replica_store), 4_000)
                .unwrap();
        }

        for index in 0..40 {
            let expected = if index < 20 {
                format!("before:{index}").into_bytes()
            } else {
                format!("after:{index}").into_bytes()
            };
            assert_eq!(
                get_string(&mut replica_store, &format!("sync:{index}")),
                Some(expected)
            );
        }
    }
}
