use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    task::Waker,
    time::Instant,
};

use compact_str::CompactString;
use senko_core::SenkoError;
use senko_store::{
    Response, Store,
    commands::list::blocking::{
        BlockingOp as StoreBlockingOp, BlockingResponseKind, Direction, move_now, mpop_now, pop_now,
    },
    commands::stream::read::{xread_now, xreadgroup_now},
    commands::zset::blocking::{
        BlockingOp as StoreZBlockingOp, zmpop_now as zset_mpop_now, zpop_now as zset_pop_now,
    },
    commands::zset::pop::ZPopDir,
};
use smallvec::SmallVec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnblockReason {
    Timeout,
    Error,
}

#[derive(Debug, Clone)]
pub enum BlockedOp {
    Pop {
        direction: Direction,
    },
    Move {
        dest: CompactString,
        src_dir: Direction,
        dst_dir: Direction,
    },
    MoveDeprecated {
        dest: CompactString,
    },
    MPop {
        direction: Direction,
        count: usize,
    },
    ZPop {
        direction: ZPopDir,
    },
    ZMPop {
        direction: ZPopDir,
        count: usize,
    },
    XRead {
        streams: SmallVec<[(CompactString, senko_core::StreamId); 4]>,
        count: Option<usize>,
    },
    XReadGroup {
        streams: SmallVec<[(CompactString, senko_core::StreamId); 4]>,
        group: CompactString,
        consumer: CompactString,
        count: Option<usize>,
        noack: bool,
    },
}

#[derive(Clone)]
pub struct BlockedClient {
    pub conn_id: u64,
    pub keys: SmallVec<[CompactString; 4]>,
    pub deadline: Option<Instant>,
    pub waker: Waker,
    pub op: BlockedOp,
    pub timeout_response: BlockingResponseKind,
}

impl core::fmt::Debug for BlockedClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlockedClient")
            .field("conn_id", &self.conn_id)
            .field("keys", &self.keys)
            .field("deadline", &self.deadline)
            .field("op", &self.op)
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct BlockedKeyRegistry {
    pub waiters: HashMap<CompactString, VecDeque<u64>>,
    clients: HashMap<u64, BlockedClient>,
    ready: HashMap<u64, Result<Response, SenkoError>>,
    timeout_heap: BinaryHeap<Reverse<(Instant, u64)>>,
}

impl From<StoreBlockingOp> for BlockedOp {
    fn from(value: StoreBlockingOp) -> Self {
        match value {
            StoreBlockingOp::Pop { direction } => Self::Pop { direction },
            StoreBlockingOp::Move {
                dest,
                src_dir,
                dst_dir,
            } => Self::Move {
                dest,
                src_dir,
                dst_dir,
            },
            StoreBlockingOp::MoveDeprecated { dest } => Self::MoveDeprecated { dest },
            StoreBlockingOp::MPop { direction, count } => Self::MPop { direction, count },
        }
    }
}

impl From<StoreZBlockingOp> for BlockedOp {
    fn from(value: StoreZBlockingOp) -> Self {
        match value {
            StoreZBlockingOp::ZPop { direction } => Self::ZPop { direction },
            StoreZBlockingOp::ZMPop { direction, count } => Self::ZMPop { direction, count },
        }
    }
}

impl BlockedKeyRegistry {
    pub fn register(&mut self, client: BlockedClient) {
        if let Some(deadline) = client.deadline {
            self.timeout_heap.push(Reverse((deadline, client.conn_id)));
        }
        for key in &client.keys {
            self.waiters
                .entry(key.clone())
                .or_default()
                .push_back(client.conn_id);
        }
        self.clients.insert(client.conn_id, client);
    }

    pub fn refresh_waker(&mut self, conn_id: u64, waker: &Waker) {
        if let Some(client) = self.clients.get_mut(&conn_id) {
            client.waker = waker.clone();
        }
    }

    pub fn notify(&mut self, key: &CompactString, store: &mut Store) -> Option<(u64, Vec<u8>)> {
        loop {
            let conn_id = {
                let queue = self.waiters.get_mut(key)?;
                queue.pop_front()?
            };
            let Some(client) = self.clients.remove(&conn_id) else {
                continue;
            };
            let response = self.execute(key, &client, store);
            let bulk = extract_bulk_bytes(&response);
            self.ready.insert(conn_id, response);
            client.waker.wake();

            if let Some(next_key) = destination_key(&client.op) {
                while self.notify(&next_key, store).is_some() {}
            }
            return Some((conn_id, bulk));
        }
    }

    pub fn notify_stream(
        &mut self,
        key: &CompactString,
        _new_id: senko_core::StreamId,
        store: &mut Store,
    ) -> Vec<(u64, Vec<u8>)> {
        let mut woke = Vec::new();
        let mut requeue = VecDeque::new();
        let mut seen_group_consumers =
            std::collections::HashSet::<(CompactString, CompactString)>::new();

        loop {
            let conn_id = {
                let Some(queue) = self.waiters.get_mut(key) else {
                    break;
                };
                let Some(conn_id) = queue.pop_front() else {
                    break;
                };
                conn_id
            };
            let Some(client) = self.clients.remove(&conn_id) else {
                continue;
            };

            if let BlockedOp::XReadGroup {
                group, consumer, ..
            } = &client.op
            {
                let tuple = (group.clone(), consumer.clone());
                if !seen_group_consumers.insert(tuple) {
                    requeue.push_back(client);
                    continue;
                }
            }

            let response = self.execute(key, &client, store);
            if matches!(response, Ok(Response::Value(None))) {
                requeue.push_back(client);
                continue;
            }
            let bulk = extract_bulk_bytes(&response);
            self.ready.insert(conn_id, response);
            client.waker.wake();
            woke.push((conn_id, bulk));
        }

        if !requeue.is_empty() {
            let queue = self.waiters.entry(key.clone()).or_default();
            for client in requeue {
                queue.push_back(client.conn_id);
                self.clients.insert(client.conn_id, client);
            }
        }

        woke
    }

    pub fn cancel_waiters(&mut self, key: &CompactString) -> Vec<u64> {
        let Some(queue) = self.waiters.remove(key) else {
            return Vec::new();
        };
        let mut cancelled = Vec::new();
        for conn_id in queue {
            let Some(client) = self.clients.remove(&conn_id) else {
                continue;
            };
            let response = match client.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            };
            self.ready.insert(conn_id, Ok(response));
            client.waker.wake();
            cancelled.push(conn_id);
        }
        cancelled
    }

    pub fn check_timeouts(&mut self, now: Instant) -> Vec<u64> {
        let mut expired = Vec::new();
        while let Some(Reverse((deadline, conn_id))) = self.timeout_heap.peek().copied() {
            if deadline > now {
                break;
            }
            let _ = self.timeout_heap.pop();
            let Some(client) = self.clients.remove(&conn_id) else {
                continue;
            };
            if client.deadline != Some(deadline) {
                continue;
            }
            let response = match client.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            };
            self.ready.insert(conn_id, Ok(response));
            client.waker.wake();
            expired.push(conn_id);
        }
        expired
    }

    pub fn take_ready(&mut self, conn_id: u64) -> Option<Result<Response, SenkoError>> {
        self.ready.remove(&conn_id)
    }

    pub fn force_unblock(&mut self, conn_id: u64, reason: UnblockReason) -> bool {
        let Some(client) = self.clients.remove(&conn_id) else {
            return false;
        };
        let response = match reason {
            UnblockReason::Timeout => match client.timeout_response {
                BlockingResponseKind::NullArray => Ok(Response::NullArray),
                BlockingResponseKind::NullBulk => Ok(Response::Value(None)),
            },
            UnblockReason::Error => Err(SenkoError::ProtocolMessage(
                "UNBLOCKED client unblocked via CLIENT UNBLOCK".into(),
            )),
        };
        self.ready.insert(conn_id, response);
        client.waker.wake();
        true
    }

    pub fn clear_all(&mut self) -> Vec<u64> {
        let clients = self
            .clients
            .drain()
            .map(|(_, client)| client)
            .collect::<Vec<_>>();
        self.waiters.clear();
        self.timeout_heap.clear();
        let mut cancelled = Vec::with_capacity(clients.len());
        for client in clients {
            let response = match client.timeout_response {
                BlockingResponseKind::NullArray => Response::NullArray,
                BlockingResponseKind::NullBulk => Response::Value(None),
            };
            self.ready.insert(client.conn_id, Ok(response));
            client.waker.wake();
            cancelled.push(client.conn_id);
        }
        cancelled
    }

    pub fn remove_client(&mut self, conn_id: u64) {
        let _ = self.clients.remove(&conn_id);
        let _ = self.ready.remove(&conn_id);
    }

    fn execute(
        &mut self,
        key: &CompactString,
        client: &BlockedClient,
        store: &mut Store,
    ) -> Result<Response, SenkoError> {
        match &client.op {
            BlockedOp::Pop { direction } => pop_ready(store, key, *direction),
            BlockedOp::Move {
                dest,
                src_dir,
                dst_dir,
            } => move_ready(store, key, dest, *src_dir, *dst_dir),
            BlockedOp::MoveDeprecated { dest } => {
                move_ready(store, key, dest, Direction::Right, Direction::Left)
            }
            BlockedOp::MPop { direction, count } => mpop_ready(store, key, *direction, *count),
            BlockedOp::ZPop { direction } => zpop_ready(store, key, *direction),
            BlockedOp::ZMPop { direction, count } => zmpop_ready(store, key, *direction, *count),
            BlockedOp::XRead { streams, count } => xread_now(store, streams, *count),
            BlockedOp::XReadGroup {
                streams,
                group,
                consumer,
                count,
                noack,
            } => xreadgroup_now(store, group, consumer, streams, *count, *noack, None),
        }
    }
}

fn pop_ready(
    store: &mut Store,
    key: &CompactString,
    direction: Direction,
) -> Result<Response, SenkoError> {
    match store.get(key.as_bytes()) {
        Some(senko_core::SenkoValue::List(_)) => pop_now(store, key.as_bytes(), direction),
        Some(value) => Err(wrong_type(value, "list")),
        None => Ok(Response::NullArray),
    }
}

fn move_ready(
    store: &mut Store,
    source: &CompactString,
    destination: &CompactString,
    src_dir: Direction,
    dst_dir: Direction,
) -> Result<Response, SenkoError> {
    match store.get(source.as_bytes()) {
        Some(senko_core::SenkoValue::List(_)) => {}
        Some(value) => return Err(wrong_type(value, "list")),
        None => return Ok(Response::Value(None)),
    }
    if let Some(value) = store.get(destination.as_bytes())
        && !matches!(value, senko_core::SenkoValue::List(_))
    {
        return Err(wrong_type(value, "list"));
    }
    move_now(
        store,
        source.as_bytes(),
        destination.as_bytes(),
        src_dir,
        dst_dir,
    )
}

fn mpop_ready(
    store: &mut Store,
    key: &CompactString,
    direction: Direction,
    count: usize,
) -> Result<Response, SenkoError> {
    match store.get(key.as_bytes()) {
        Some(senko_core::SenkoValue::List(_)) => {
            mpop_now(store, key.as_bytes(), direction, count)
        }
        Some(value) => Err(wrong_type(value, "list")),
        None => Ok(Response::NullArray),
    }
}

fn zpop_ready(
    store: &mut Store,
    key: &CompactString,
    direction: ZPopDir,
) -> Result<Response, SenkoError> {
    match store.get(key.as_bytes()) {
        Some(senko_core::SenkoValue::ZSet(_)) => zset_pop_now(store, key.as_bytes(), direction),
        Some(value) => Err(wrong_type(value, "zset")),
        None => Ok(Response::NullArray),
    }
}

fn zmpop_ready(
    store: &mut Store,
    key: &CompactString,
    direction: ZPopDir,
    count: usize,
) -> Result<Response, SenkoError> {
    match store.get(key.as_bytes()) {
        Some(senko_core::SenkoValue::ZSet(_)) => {
            zset_mpop_now(store, key.as_bytes(), direction, count)
        }
        Some(value) => Err(wrong_type(value, "zset")),
        None => Ok(Response::Value(None)),
    }
}

fn destination_key(op: &BlockedOp) -> Option<CompactString> {
    match op {
        BlockedOp::Move { dest, .. } | BlockedOp::MoveDeprecated { dest } => Some(dest.clone()),
        _ => None,
    }
}

fn extract_bulk_bytes(response: &Result<Response, SenkoError>) -> Vec<u8> {
    match response {
        Ok(Response::Value(Some(value))) => value.as_bytes().into_owned(),
        Ok(Response::Array(items)) if items.len() >= 2 => match &items[1] {
            Response::Value(Some(value)) => value.as_bytes().into_owned(),
            Response::Array(values) if !values.is_empty() => match &values[0] {
                Response::Value(Some(value)) => value.as_bytes().into_owned(),
                Response::Array(pair) if !pair.is_empty() => match &pair[0] {
                    Response::Value(Some(value)) => value.as_bytes().into_owned(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            },
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn wrong_type(value: &senko_core::SenkoValue, expected: &'static str) -> SenkoError {
    let actual = match value {
        senko_core::SenkoValue::Raw(_)
        | senko_core::SenkoValue::Int(_)
        | senko_core::SenkoValue::Float(_) => "string",
        senko_core::SenkoValue::Hash(_) => "hash",
        senko_core::SenkoValue::List(_) => "list",
        senko_core::SenkoValue::Set(_) => "set",
        senko_core::SenkoValue::Stream(_) => "stream",
        senko_core::SenkoValue::ZSet(_) => "zset",
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::BloomFilter(_) => "MBbloom--",
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::CuckooFilter(_) => "cuckooFilter",
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::CountMinSketch(_) => "CMSk--",
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::TopK(_) => "topk",
        #[cfg(feature = "prob")]
        senko_core::SenkoValue::TDigest(_) => "TDIS-TYPE",
        #[cfg(feature = "json")]
        senko_core::SenkoValue::Json(_) => "json",
        #[cfg(feature = "vector")]
        senko_core::SenkoValue::VectorSet(_) => "vectorset",
    };
    SenkoError::WrongType { expected, actual }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use compact_str::CompactString;
    use futures_util::task::noop_waker;
    use senko_core::{SenkoValue, StreamId, ZAddOptions};
    use senko_proto::Frame;
    use senko_store::{
        Response, SetOptions,
        commands::list::blocking::{BlockingResponseKind, Direction},
        commands::stream::{basic::xadd, group::xgroup},
        commands::zset::pop::ZPopDir,
    };

    use super::{BlockedClient, BlockedKeyRegistry, BlockedOp};

    fn bs(value: &'static [u8]) -> Frame<'static> {
        Frame::BulkString(value)
    }

    fn stream_ids(response: &Response) -> Vec<Vec<Vec<u8>>> {
        let Response::Array(streams) = response else {
            return Vec::new();
        };
        streams
            .iter()
            .filter_map(|stream| {
                let Response::Array(parts) = stream else {
                    return None;
                };
                let Some(Response::Array(entries)) = parts.get(1) else {
                    return None;
                };
                Some(
                    entries
                        .iter()
                        .filter_map(|entry| {
                            let Response::Array(pair) = entry else {
                                return None;
                            };
                            let Some(Response::Value(Some(SenkoValue::Raw(id)))) = pair.first()
                            else {
                                return None;
                            };
                            Some(id.to_vec())
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect()
    }

    #[test]
    fn notify_wakes_only_first_waiter() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("k");

        registry.register(BlockedClient {
            conn_id: 1,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });
        registry.register(BlockedClient {
            conn_id: 2,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        store.get_or_create_list(key.clone()).push_back(b"a");
        let woke = registry.notify(&key, &mut store);
        assert!(matches!(woke, Some((1, _))));
        assert!(registry.take_ready(1).is_some());
        assert!(registry.take_ready(2).is_none());
    }

    #[test]
    fn timeout_marks_client_ready() {
        let mut registry = BlockedKeyRegistry::default();
        registry.register(BlockedClient {
            conn_id: 9,
            keys: smallvec::smallvec![CompactString::from("k")],
            deadline: Some(Instant::now() - Duration::from_millis(1)),
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        let expired = registry.check_timeouts(Instant::now());
        assert_eq!(expired, vec![9]);
        assert!(matches!(
            registry.take_ready(9),
            Some(Ok(senko_store::Response::NullArray))
        ));
    }

    #[test]
    fn move_unblock_creates_destination() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let source = CompactString::from("src");
        let dest = CompactString::from("dst");

        registry.register(BlockedClient {
            conn_id: 3,
            keys: smallvec::smallvec![source.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Move {
                dest: dest.clone(),
                src_dir: Direction::Right,
                dst_dir: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        store.get_or_create_list(source.clone()).push_back(b"x");
        let _ = registry.notify(&source, &mut store);
        assert!(matches!(
            registry.take_ready(3),
            Some(Ok(senko_store::Response::Value(Some(SenkoValue::Raw(_)))))
        ));
        let dst = store.get_list(dest.as_bytes()).expect("destination list");
        assert_eq!(dst.index(0), Some(&b"x"[..]));
    }

    #[test]
    fn mpop_unblocks_on_middle_key() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let keys = [
            CompactString::from("k1"),
            CompactString::from("k2"),
            CompactString::from("k3"),
        ];

        registry.register(BlockedClient {
            conn_id: 5,
            keys: smallvec::smallvec![keys[0].clone(), keys[1].clone(), keys[2].clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::MPop {
                direction: Direction::Left,
                count: 3,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        let list = store.get_or_create_list(keys[1].clone());
        list.push_back(b"a");
        list.push_back(b"b");
        let _ = registry.notify(&keys[1], &mut store);
        let ready = registry.take_ready(5).expect("ready").expect("ok");
        let senko_store::Response::Array(items) = ready else {
            panic!("expected array response");
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn wrongtype_after_register_returns_error() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("k");

        registry.register(BlockedClient {
            conn_id: 7,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });
        let _ = store.set(
            key.clone(),
            SenkoValue::Raw(bytes::Bytes::from_static(b"value")),
            SetOptions::default(),
        );
        let _ = registry.notify(&key, &mut store);
        assert!(matches!(
            registry.take_ready(7),
            Some(Err(senko_core::SenkoError::WrongType { .. }))
        ));
    }

    #[test]
    fn timeout_zero_style_waiter_does_not_expire_and_wakes_on_notify() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("k");

        registry.register(BlockedClient {
            conn_id: 11,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        assert!(
            registry
                .check_timeouts(Instant::now() + Duration::from_secs(60))
                .is_empty()
        );
        store.get_or_create_list(key.clone()).push_back(b"x");
        let _ = registry.notify(&key, &mut store);
        assert!(matches!(
            registry.take_ready(11),
            Some(Ok(Response::Array(_)))
        ));
    }

    #[test]
    fn five_waiters_receive_five_notifications_fifo() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("k");

        for conn_id in 1..=5 {
            registry.register(BlockedClient {
                conn_id,
                keys: smallvec::smallvec![key.clone()],
                deadline: None,
                waker: noop_waker(),
                op: BlockedOp::Pop {
                    direction: Direction::Left,
                },
                timeout_response: BlockingResponseKind::NullArray,
            });
        }

        let list = store.get_or_create_list(key.clone());
        for value in [b"a", b"b", b"c", b"d", b"e"] {
            list.push_back(value);
        }

        let mut woke = Vec::new();
        for _ in 0..5 {
            woke.push(registry.notify(&key, &mut store).expect("must wake").0);
        }
        assert_eq!(woke, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn zpop_waiter_wakes_when_zset_becomes_non_empty() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("zs");

        registry.register(BlockedClient {
            conn_id: 21,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::ZPop {
                direction: ZPopDir::Min,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        let _ = store.get_or_create_zset(key.clone()).add(
            1.0,
            CompactString::from("a"),
            ZAddOptions::default(),
        );
        let woke = registry.notify(&key, &mut store);
        assert!(matches!(woke, Some((21, _))));
        let ready = registry.take_ready(21).expect("ready").expect("ok");
        let Response::Array(items) = ready else {
            panic!("expected array");
        };
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn zmpop_waiter_wakes_on_second_key() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let first = CompactString::from("zs1");
        let second = CompactString::from("zs2");

        registry.register(BlockedClient {
            conn_id: 22,
            keys: smallvec::smallvec![first.clone(), second.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::ZMPop {
                direction: ZPopDir::Max,
                count: 2,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        let _ = store.get_or_create_zset(second.clone()).add(
            2.0,
            CompactString::from("b"),
            ZAddOptions::default(),
        );
        let _ = registry.notify(&second, &mut store);
        let ready = registry.take_ready(22).expect("ready").expect("ok");
        let Response::Array(items) = ready else {
            panic!("expected array");
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn zset_waiters_are_woken_fifo() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("zs");

        for conn_id in 31..=33 {
            registry.register(BlockedClient {
                conn_id,
                keys: smallvec::smallvec![key.clone()],
                deadline: None,
                waker: noop_waker(),
                op: BlockedOp::ZPop {
                    direction: ZPopDir::Min,
                },
                timeout_response: BlockingResponseKind::NullArray,
            });
        }

        let zset = store.get_or_create_zset(key.clone());
        let _ = zset.add(1.0, CompactString::from("a"), ZAddOptions::default());
        let _ = zset.add(2.0, CompactString::from("b"), ZAddOptions::default());
        let _ = zset.add(3.0, CompactString::from("c"), ZAddOptions::default());

        let mut woke = Vec::new();
        for _ in 0..3 {
            woke.push(registry.notify(&key, &mut store).expect("must wake").0);
        }
        assert_eq!(woke, vec![31, 32, 33]);
    }

    #[test]
    fn zmpop_timeout_marks_client_ready_with_null_bulk() {
        let mut registry = BlockedKeyRegistry::default();
        registry.register(BlockedClient {
            conn_id: 41,
            keys: smallvec::smallvec![CompactString::from("zs")],
            deadline: Some(Instant::now() - Duration::from_millis(1)),
            waker: noop_waker(),
            op: BlockedOp::ZMPop {
                direction: ZPopDir::Min,
                count: 1,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        let expired = registry.check_timeouts(Instant::now());
        assert_eq!(expired, vec![41]);
        assert!(matches!(
            registry.take_ready(41),
            Some(Ok(Response::Value(None)))
        ));
    }

    #[test]
    fn zpop_timeout_marks_client_ready_with_null_array() {
        let mut registry = BlockedKeyRegistry::default();
        registry.register(BlockedClient {
            conn_id: 42,
            keys: smallvec::smallvec![CompactString::from("zs")],
            deadline: Some(Instant::now() - Duration::from_millis(1)),
            waker: noop_waker(),
            op: BlockedOp::ZPop {
                direction: ZPopDir::Min,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        let expired = registry.check_timeouts(Instant::now());
        assert_eq!(expired, vec![42]);
        assert!(matches!(
            registry.take_ready(42),
            Some(Ok(Response::NullArray))
        ));
    }

    #[test]
    fn xread_timeout_marks_client_ready_with_null_bulk() {
        let mut registry = BlockedKeyRegistry::default();
        registry.register(BlockedClient {
            conn_id: 51,
            keys: smallvec::smallvec![CompactString::from("s")],
            deadline: Some(Instant::now() - Duration::from_millis(1)),
            waker: noop_waker(),
            op: BlockedOp::XRead {
                streams: smallvec::smallvec![(CompactString::from("s"), StreamId::ZERO)],
                count: None,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        let expired = registry.check_timeouts(Instant::now());
        assert_eq!(expired, vec![51]);
        assert!(matches!(
            registry.take_ready(51),
            Some(Ok(Response::Value(None)))
        ));
    }

    #[test]
    fn xread_notify_stream_wakes_all_waiters() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("s");

        for conn_id in 61..=63 {
            registry.register(BlockedClient {
                conn_id,
                keys: smallvec::smallvec![key.clone()],
                deadline: None,
                waker: noop_waker(),
                op: BlockedOp::XRead {
                    streams: smallvec::smallvec![(key.clone(), StreamId::ZERO)],
                    count: None,
                },
                timeout_response: BlockingResponseKind::NullBulk,
            });
        }

        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let woke = registry.notify_stream(&key, StreamId { ms: 1, seq: 0 }, &mut store);
        assert_eq!(woke.len(), 3);
        for conn_id in 61..=63 {
            let ready = registry.take_ready(conn_id).expect("ready").expect("ok");
            assert_eq!(stream_ids(&ready), vec![vec![b"1-0".to_vec()]]);
        }
    }

    #[test]
    fn xreadgroup_notify_stream_adds_entry_to_pel() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("s");
        let _ = xgroup(
            &mut store,
            &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"0"), bs(b"MKSTREAM")],
        )
        .unwrap();

        registry.register(BlockedClient {
            conn_id: 71,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::XReadGroup {
                streams: smallvec::smallvec![(key.clone(), StreamId::MAX)],
                group: CompactString::from("g"),
                consumer: CompactString::from("c1"),
                count: None,
                noack: false,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let woke = registry.notify_stream(&key, StreamId { ms: 1, seq: 0 }, &mut store);
        assert_eq!(woke.len(), 1);
        let ready = registry.take_ready(71).expect("ready").expect("ok");
        assert_eq!(stream_ids(&ready), vec![vec![b"1-0".to_vec()]]);

        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert_eq!(group.pel_count, 1);
        assert!(
            group
                .consumers
                .get("c1")
                .unwrap()
                .pel
                .contains_key(&StreamId { ms: 1, seq: 0 })
        );
    }

    #[test]
    fn xreadgroup_same_group_different_consumers_do_not_duplicate_delivery() {
        let mut registry = BlockedKeyRegistry::default();
        let mut store = senko_store::Store::default();
        let key = CompactString::from("s");
        let _ = xadd(&mut store, &[bs(b"s"), bs(b"1-0"), bs(b"f"), bs(b"1")]).unwrap();
        let _ = xgroup(&mut store, &[bs(b"CREATE"), bs(b"s"), bs(b"g"), bs(b"$")]).unwrap();

        registry.register(BlockedClient {
            conn_id: 81,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::XReadGroup {
                streams: smallvec::smallvec![(key.clone(), StreamId::MAX)],
                group: CompactString::from("g"),
                consumer: CompactString::from("c1"),
                count: None,
                noack: false,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });
        registry.register(BlockedClient {
            conn_id: 82,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::XReadGroup {
                streams: smallvec::smallvec![(key.clone(), StreamId::MAX)],
                group: CompactString::from("g"),
                consumer: CompactString::from("c2"),
                count: None,
                noack: false,
            },
            timeout_response: BlockingResponseKind::NullBulk,
        });

        let _ = xadd(&mut store, &[bs(b"s"), bs(b"2-0"), bs(b"f"), bs(b"2")]).unwrap();
        let woke = registry.notify_stream(&key, StreamId { ms: 2, seq: 0 }, &mut store);
        assert_eq!(woke.len(), 1);
        let first = registry.take_ready(81);
        let second = registry.take_ready(82);
        assert!(first.is_some() ^ second.is_some());

        let group = store.get_stream(b"s").unwrap().groups.get("g").unwrap();
        assert_eq!(group.pel_count, 1);
    }

    #[test]
    fn cancel_waiters_marks_client_ready_with_null_response() {
        let mut registry = BlockedKeyRegistry::default();
        let key = CompactString::from("k");

        registry.register(BlockedClient {
            conn_id: 91,
            keys: smallvec::smallvec![key.clone()],
            deadline: None,
            waker: noop_waker(),
            op: BlockedOp::Pop {
                direction: Direction::Left,
            },
            timeout_response: BlockingResponseKind::NullArray,
        });

        assert_eq!(registry.cancel_waiters(&key), vec![91]);
        assert!(matches!(
            registry.take_ready(91),
            Some(Ok(senko_store::Response::NullArray))
        ));
    }
}
