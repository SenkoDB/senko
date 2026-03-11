use std::{
    error::Error,
    fmt, ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
    },
    task::Waker,
};

use futures_util::task::AtomicWaker;

use crate::message::PubSubMessage;

pub const RING_SIZE: usize = 256;
const RING_MASK: usize = RING_SIZE - 1;

#[repr(align(64))]
#[derive(Debug)]
pub struct CachePadded<T>(pub T);

impl<T> CachePadded<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lagged(pub u64);

impl fmt::Display for Lagged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "subscriber lagged by {} messages", self.0)
    }
}

impl Error for Lagged {}

#[derive(Debug)]
pub struct BroadcastSlot {
    conn_id: u64,
    ring: Box<[AtomicPtr<PubSubMessage>; RING_SIZE]>,
    pub(crate) head: CachePadded<AtomicU64>,
    pub(crate) tail: CachePadded<AtomicU64>,
    lagged: AtomicBool,
    waker: AtomicWaker,
}

impl BroadcastSlot {
    #[inline]
    pub fn new(conn_id: u64) -> Self {
        Self {
            conn_id,
            ring: Box::new(std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut()))),
            head: CachePadded::new(AtomicU64::new(0)),
            tail: CachePadded::new(AtomicU64::new(0)),
            lagged: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    #[inline(always)]
    pub fn conn_id(&self) -> u64 {
        self.conn_id
    }

    #[inline(always)]
    pub fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    #[inline(always)]
    pub fn publish(&self, message: Arc<PubSubMessage>) -> Result<(), Lagged> {
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);
        let used = head.wrapping_sub(tail);
        if used >= RING_SIZE as u64 {
            self.lagged.store(true, Ordering::Release);
            return Err(Lagged(used - RING_SIZE as u64));
        }

        let slot = &self.ring[head as usize & RING_MASK];
        let ptr = Arc::into_raw(message) as *mut PubSubMessage;
        slot.store(ptr, Ordering::Release);
        self.head.0.fetch_add(1, Ordering::Release);
        self.wake();
        Ok(())
    }

    #[inline(always)]
    pub fn recv(&self) -> Option<Arc<PubSubMessage>> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        if tail == head {
            return None;
        }

        let slot = &self.ring[tail as usize & RING_MASK];
        let ptr = slot.swap(ptr::null_mut(), Ordering::Acquire);
        self.tail.0.fetch_add(1, Ordering::Release);

        if ptr.is_null() {
            return None;
        }

        // SAFETY: `publish()` inserts only raw pointers created by `Arc::into_raw`.
        // Each slot is consumed exactly once by `recv()` or `drop`, which reconstructs
        // a single `Arc` from the stored raw pointer.
        Some(unsafe { Arc::from_raw(ptr) })
    }

    #[inline(always)]
    pub fn wake(&self) {
        self.waker.wake();
    }

    #[inline(always)]
    pub fn is_lagged(&self) -> bool {
        self.lagged.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        self.head
            .0
            .load(Ordering::Acquire)
            .wrapping_sub(self.tail.0.load(Ordering::Acquire))
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for BroadcastSlot {
    fn drop(&mut self) {
        for slot in self.ring.iter_mut() {
            let ptr = slot.swap(ptr::null_mut(), Ordering::AcqRel);
            if ptr.is_null() {
                continue;
            }

            // SAFETY: these pointers were created by `Arc::into_raw` in `publish()`.
            // Drop runs only when the `BroadcastSlot` itself is no longer shared, so
            // no concurrent consumer can also reconstruct the same `Arc`.
            unsafe {
                drop(Arc::from_raw(ptr));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
        thread,
    };

    use bytes::Bytes;

    use super::{BroadcastSlot, Lagged, RING_SIZE};
    use crate::{
        message::{MessageKind, PubSubMessage},
        slot::BroadcastSlot as Slot,
    };

    fn message(index: u64) -> Arc<PubSubMessage> {
        Arc::new(PubSubMessage {
            channel: "chan".into(),
            payload: Bytes::copy_from_slice(&index.to_le_bytes()),
            kind: MessageKind::Message,
        })
    }

    #[test]
    fn single_producer_single_consumer_receives_all_messages_in_order() {
        const TOTAL: u64 = 1_000_000;

        let slot = Arc::new(BroadcastSlot::new(7));
        let producer = Arc::clone(&slot);
        let consumer = Arc::clone(&slot);

        let producer_thread = thread::spawn(move || {
            for index in 0..TOTAL {
                loop {
                    if producer.publish(message(index)).is_ok() {
                        break;
                    }
                    thread::yield_now();
                }
            }
        });

        let consumer_thread = thread::spawn(move || {
            let mut next = 0u64;
            while next < TOTAL {
                let Some(message) = consumer.recv() else {
                    thread::yield_now();
                    continue;
                };
                let payload = message.payload.as_ref();
                let value = u64::from_le_bytes(payload.try_into().expect("payload width"));
                assert_eq!(value, next);
                next += 1;
            }
        });

        producer_thread.join().expect("producer thread");
        consumer_thread.join().expect("consumer thread");
    }

    #[test]
    fn ring_full_returns_lagged() {
        let slot = BroadcastSlot::new(1);
        for index in 0..RING_SIZE as u64 {
            assert!(slot.publish(message(index)).is_ok());
        }

        assert_eq!(slot.publish(message(999)), Err(Lagged(0)));
    }

    #[test]
    fn ring_empty_returns_none() {
        let slot = BroadcastSlot::new(1);
        assert!(slot.recv().is_none());
    }

    #[test]
    fn lagged_queue_can_be_drained_and_reused_without_leaks() {
        let slot = BroadcastSlot::new(1);
        let tracked = Arc::new(PubSubMessage {
            channel: "tracked".into(),
            payload: Bytes::from_static(b"x"),
            kind: MessageKind::Message,
        });
        let weak = Arc::downgrade(&tracked);

        for _ in 0..RING_SIZE {
            assert!(slot.publish(Arc::clone(&tracked)).is_ok());
        }
        drop(tracked);

        assert_eq!(slot.publish(message(42)), Err(Lagged(0)));

        for _ in 0..RING_SIZE {
            drop(slot.recv().expect("queued message"));
        }
        assert!(weak.upgrade().is_none());

        assert!(slot.publish(message(777)).is_ok());
        let received = slot.recv().expect("reused slot");
        assert_eq!(
            u64::from_le_bytes(received.payload.as_ref().try_into().expect("payload width")),
            777
        );
    }

    #[test]
    fn head_and_tail_are_on_different_cache_lines() {
        let head_offset = std::mem::offset_of!(Slot, head);
        let tail_offset = std::mem::offset_of!(Slot, tail);
        assert!(tail_offset >= head_offset + 64);
    }

    struct CountingWake {
        wakes: AtomicUsize,
    }

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn waker_fires_after_publish_and_empty_ring_does_not_yield_messages() {
        let slot = BroadcastSlot::new(9);
        let state = Arc::new(CountingWake {
            wakes: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&state));

        slot.register_waker(&waker);
        assert!(slot.recv().is_none());
        assert_eq!(state.wakes.load(Ordering::SeqCst), 0);

        assert!(slot.publish(message(5)).is_ok());
        assert_eq!(state.wakes.load(Ordering::SeqCst), 1);
        assert!(slot.recv().is_some());
        assert!(slot.recv().is_none());
    }
}
