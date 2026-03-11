# AGENTS.md — senko

> This file is the authoritative guide for AI coding agents (Codex, Claude, etc.)
> working on the senko codebase. Read it fully before touching any code.
> Every rule here exists for a reason. Do not skip sections.

---

## What Is senko?

senko (`閃光` — flash of light) is a **Redis-compatible, high-performance
in-memory store** written in Rust. It is architected as the libSQL of Redis:

- **Phase 1 (current):** A production-grade server that beats DragonflyDB on
  single-node throughput using a thread-per-core shared-nothing model powered
  by the `compio` async runtime.
- **Phase 2 (future):** An embeddable library (`senkodb` crate) with zero infra
  dependency, usable inside Axum, Tauri, or WASM edge workers.

The primary goal of Phase 1 is: **full Redis wire compatibility + measurably
better performance than DragonflyDB on string/hash/list/set/zset/stream
benchmarks.** Every architectural decision must serve this goal.

---

## Workspace Layout

```
senko/
├── Cargo.toml                  # workspace root
├── AGENTS.md                   # this file
├── senko-core/                # shared primitives, errors, config, value types
│   └── src/
│       ├── lib.rs
│       ├── error.rs            # senkoError — all error variants
│       ├── config.rs           # senkoConfig
│       └── value.rs            # 
Value enum (Raw/Int/Float/Hash/List/Set/ZSet/Stream)
├── senko-proto/               # RESP2/RESP3 zero-copy parser + serializer
│   └── src/
│       ├── lib.rs
│       ├── parser.rs           # incremental Frame parser
│       └── writer.rs           # BytesMut serializer
├── senko-store/               # per-shard store engine — ALL command logic lives here
│   └── src/
│       ├── lib.rs
│       ├── store.rs            # Store struct, hashbrown RawTable, expiry wheel
│       ├── listpack.rs         # shared listpack module (used by hash/list/set/zset)
│       ├── pattern.rs          # shared glob matching (used by HSCAN/SSCAN/ZSCAN/SCAN)
│       ├── arithmetic.rs       # shared integer/float arithmetic (INCR/HINCRBY etc.)
│       ├── hash/               # Hash type
│       │   ├── object.rs       # HashObject: listpack → hashtable
│       │   ├── expiry.rs       # FieldExpiryWheel
│       │   └── commands/       # HSET, HGET, HDEL, HEXPIRE, HSETEX, HSCAN ...
│       ├── list/               # List type
│       │   ├── object.rs       # QuickList of ListpackNodes
│       │   └── commands/       # LPUSH, RPUSH, LRANGE, LMOVE, BLPOP ...
│       ├── set/                # Set type
│       │   ├── object.rs       # IntSet → Listpack → Hashtable
│       │   └── commands/       # SADD, SMEMBERS, SDIFF, SINTER, SUNION, SSCAN ...
│       ├── zset/               # Sorted Set type
│       │   ├── bptree.rs       # B+ tree (replaces Redis skiplist)
│       │   ├── object.rs       # ZSetObject: listpack → BPTree + member_index
│       │   ├── bounds.rs       # ScoreBound, LexBound parsing
│       │   └── commands/       # ZADD, ZRANGE, ZPOPMIN, ZDIFF, ZSCAN ...
│       ├── stream/             # Stream type
│       │   ├── radix.rs        # StreamRadixTree of ListpackMacroNodes
│       │   ├── object.rs       # StreamObject + ConsumerGroup + PEL
│       │   └── commands/       # XADD, XREAD, XREADGROUP, XCLAIM ...
│       └── commands/
│           ├── string/         # GET, SET, INCR, APPEND, LCS, DIGEST, DELEX ...
│           └── dispatch.rs     # phf::Map command name → CommandFn
├── senko-net/                 # compio-based TCP listener, connection state machine
│   └── src/
│       ├── lib.rs
│       ├── listener.rs         # per-core TcpListener, SO_REUSEPORT
│       ├── connection.rs       # connection state machine: Reading/Parsing/Dispatching/Writing
│       ├── dispatch.rs         # command routing to store
│       └── blocked.rs          # BlockedKeyRegistry: BLPOP/BRPOP/BLMOVE/XREAD waiters
├── senko-server/              # binary entry point
│   └── src/
│       └── main.rs             # core pinning, shard spawn, CLI config
└── tests/
    └── compat/                 # Redis compatibility integration tests (redis-rs client)
        ├── string.rs
        ├── hash.rs
        ├── list.rs
        ├── set.rs
        ├── zset.rs
        └── stream.rs
```

---

## Non-Negotiable Architecture Rules

These are the core invariants of senko's design. **Breaking any of these is
a critical bug, not a style issue.**

### 1. Shared-Nothing Threading

Each OS thread owns exactly one `Store` instance. There is **no** `Arc<Store>`,
**no** `Mutex<Store>`, **no** `RwLock`. Cross-shard communication does not
exist in Phase 1.

```rust
// WRONG — never do this
let store = Arc::new(Mutex::new(Store::new()));

// CORRECT — each thread owns its store exclusively
fn shard_thread(config: senkoConfig) {
    let mut store = Store::new(&config);
    run_event_loop(store); // store never leaves this thread
}
```

### 2. Store Methods Are Sync

`senko-store` has **zero async code**. All `Store` methods are synchronous.
Async lives only in `senko-net`. This is intentional — it's what enables the
Phase 2 embedded library.

```rust
// WRONG — async in store
async fn get(&mut self, key: &[u8]) -> Option<&SenkoValue> { ... }

// CORRECT — sync in store, async wrapper in net layer
fn get(&mut self, key: &[u8]) -> Option<&SenkoValue> { ... }
```

### 3. No Cross-Crate Async Leakage

`senko-core` and `senko-store` must never import `compio`, `tokio`, or any
async runtime. Their `Cargo.toml` must not list these as dependencies.

### 4. No Dead Code, No Stubs

Every function, struct, and enum variant that exists must be fully implemented.
`todo!()`, `unimplemented!()`, and `#[allow(dead_code)]` are forbidden.
If a feature is not ready, do not add the skeleton — add it when you implement it.

### 5. No Unsafe Without Justification

`senko-core`, `senko-proto`, `senko-store`: `#[deny(unsafe_code)]` at crate
root. These crates must compile with zero unsafe.

`senko-net`, `senko-server`: unsafe is permitted only where `compio` requires
it (raw pointer operations on the slab allocator, arena nodes). Every `unsafe`
block must have a `// SAFETY:` comment explaining precisely why it is safe.

---

## Code Style & Conventions

### Naming

| Thing | Convention | Example |
|---|---|---|
| Commands | `snake_case` fn in `commands/` module | `fn cmd_zadd(...)` |
| Data structures | `PascalCase` | `QuickList`, `HashObject`, `BPTree` |
| Error variants | `PascalCase` | `senkoError::WrongType` |
| Config fields | `snake_case` | `config.num_shards` |
| RESP frame types | `PascalCase` | `Frame::BulkString` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_INLINE_SIZE` |

### Data Types — Always Use These

| Purpose | Use | Never Use |
|---|---|---|
| Short strings / keys | `compact_str::CompactString` | `std::String` on hot paths |
| Read buffers | `bytes::Bytes` / `bytes::BytesMut` | `Vec<u8>` for network buffers |
| Hash maps | `hashbrown::HashMap` with `ahash` | `std::collections::HashMap` |
| Raw hash tables | `hashbrown::RawTable` | anything else for the store |
| Small vecs | `smallvec::SmallVec<[T; N]>` | `Vec` when N ≤ 8 is typical |
| Random | `rand::rngs::SmallRng` (per-shard) | `thread_rng()` on hot paths |
| Float formatting | `ryu` crate | `format!("{}", f)` |
| Float parsing | `fast_float::parse` | `str::parse::<f64>()` on hot paths |
| Integer parsing | SWAR technique for short strings | `str::parse::<i64>()` on hot paths |

### Error Handling

All commands return `Result<Response, senkoError>`. The `senkoError` enum
covers all cases. **Never panic in command handlers.** Invalid user input is
always a `senkoError`, not a panic.

```rust
// WRONG
fn cmd_incr(store: &mut Store, frames: &[Frame]) -> Result<Response, senkoError> {
    let key = frames[1].as_bytes().unwrap(); // panics on missing arg
    ...
}

// CORRECT
fn cmd_incr(store: &mut Store, frames: &[Frame]) -> Result<Response, senkoError> {
    if frames.len() != 2 {
        return Err(senkoError::WrongArity("incr"));
    }
    let key = frames[1].as_bytes()?;
    ...
}
```

### RESP Error Strings

Every error string returned to clients **must exactly match** what Redis
returns. Use the test suite as ground truth. Wrong error strings break
compatibility with redis-cli, redis-benchmark, and client libraries.

```rust
// WRONG — invented error string
Err(senkoError::Custom("key does not exist".into()))

// CORRECT — exact Redis wording
Err(senkoError::NoKey) // serializes as "-ERR no such key\r\n"
```

### Inline Annotations

Functions on hot paths (GET, SET, HGET, ZADD, etc.) must be annotated:

```rust
#[inline(always)]
fn write_ok(buf: &mut BytesMut) { ... }

#[inline]
fn get_score(&self, member: &[u8]) -> Option<f64> { ... }
```

---

## Performance Rules

These are not suggestions. senko's entire value proposition is performance.

### SIMD

- Use `memchr::memchr` and `memchr::memmem` for all byte scanning.
- Use BLAKE3 (`blake3` crate) for DIGEST — it auto-selects AVX2/NEON/SSE4.
- For sorted integer array intersection in sets: implement AVX2/NEON SIMD paths
  with scalar fallback. Use `is_x86_feature_detected!` / `cfg(target_arch)` for
  runtime dispatch.
- For B+ tree inner node search: use AVX2 `_mm256_cmp_pd` with SSE2 fallback.
- Never use SIMD without a scalar fallback. Scalar fallback must always be
  tested independently.

### Encoding Upgrade Rules

Every data type has a compact encoding for small sizes. **Never bypass these.**

| Type | Small encoding | Threshold | Large encoding |
|---|---|---|---|
| Hash | listpack | count > 128 OR member > 64B | hashbrown::HashMap |
| List | listpack nodes in quicklist | always quicklist | — |
| Set | intset (integers only) | non-integer OR count > 512 | hashtable |
| Set | listpack | count > 128 OR member > 64B | hashtable |
| ZSet | listpack | count > 128 OR member > 64B | B+ tree + member_index |
| Stream | listpack macro-nodes | macro-node full | new macro-node in radix tree |

Encodings **never downgrade** on element removal (same as Redis).

### Memory Allocation

- The B+ tree (`senko-store/src/zset/bptree.rs`) and the stream radix tree
  (`senko-store/src/stream/radix.rs`) use their own `NodeArena` slab allocator.
  Do not call `Box::new` per node — allocate from the arena.
- Use `Bytes::make_mut` pattern for in-place string mutation to avoid copies
  when the buffer is uniquely owned.
- Pre-allocate response buffers with realistic capacity estimates.

### Zero-Copy Parsing

The RESP parser in `senko-proto` takes `&[u8]` slices borrowed from the
compio read buffer. It must **never allocate** on the happy path.
`CompactString` keys are only created when an entry is being inserted into the
store — not during parsing.

---

## Data Structure Details

### Store (senko-store/src/store.rs)

The top-level per-shard store uses `hashbrown::RawTable<(CompactString, Entry)>`
directly (not `HashMap`). The `Entry` struct:

```rust
#[repr(C)]
struct Entry {
    value: SenkoValue,
    expires_at: Option<u64>,  // unix millis
    lru_clock: u32,           // coarse 10s LRU clock
}
```

Incremental rehashing: two tables (primary + resize). On each write, migrate
8 buckets from resize → primary. Trigger at load factor > 0.75.

### TimerWheel (expiry)

512 slots × 100ms resolution = 51.2s coverage.
Overflow: `BTreeMap<u64, Vec<CompactString>>` for TTLs > 51.2s.
`advance_expiry_wheel(now_ms)` is called every 100ms by the compio event loop.

### FieldExpiryWheel (per-field hash TTLs)

Same 512-slot design as `TimerWheel`, but entries are
`(CompactString key, CompactString field)` tuples.
Only active when `HashObject::has_field_expiry == true`.

### B+ Tree (zset/bptree.rs)

Node size: 512 bytes. Leaf nodes hold 14 `(f64, CompactString)` pairs.
Inner nodes hold 21 separator keys + 22 child pointers.
Leaf nodes are linked (prev/next) for O(1) sequential range scan.
Inner nodes carry `subtree_size: u64` for O(log N) rank queries.
This replaces Redis's skiplist — Dragonfly uses the same design.

### Stream Radix Tree (stream/radix.rs)

Keys are 16-byte big-endian `StreamId` values (ensures correct byte-order sort).
Each leaf stores a `ListpackMacroNode` (up to 100 entries, up to 4KB).
XDEL soft-deletes entries (sets `STREAM_ITEM_FLAG_DELETED` flag) — entries are
not physically removed until the whole macro-node is empty.
This is identical to how both Redis and Dragonfly implement streams.

---

## Command Implementation Checklist

When implementing any Redis command, verify ALL of these:

- [ ] Argument count validated — returns exact Redis wrong-arity error string
- [ ] `WRONGTYPE` error when key holds a different type
- [ ] Lazy expiry checked before any read operation
- [ ] Auto-delete empty container after last element removed
  (empty hash/list/set/zset/stream must not remain as keys)
- [ ] Integer encoding attempted on string values where applicable
- [ ] Encoding upgrade triggered when thresholds exceeded
- [ ] TTL preserved on key updates unless explicitly changed
- [ ] Blocked waiters notified after mutations that add data
  (LPUSH/RPUSH → BLPOP waiters, ZADD → BZPOPMIN waiters, XADD → XREAD waiters)
- [ ] Compat test added in `tests/compat/`
- [ ] Criterion benchmark added if it's a hot-path command

---

## Blocked Client System (senko-net/src/blocked.rs)

The `BlockedKeyRegistry` is owned by the shard dispatcher (one per shard).
It is **not** inside the `Store`.

Key rules:
- **Lists/ZSets:** only ONE blocked client is woken per mutation (FIFO).
- **Streams (XREAD):** ALL blocked clients watching a key are woken (fan-out).
  This is a critical semantic difference — do not conflate them.
- Every command that can add data to a key must call
  `registry.notify(&key, store)` (lists/zsets) or
  `registry.notify_stream(&key, &new_id, store)` (streams) after success.
- Timeout checking runs every 100ms alongside the expiry wheel tick.

---

## Build & Test Commands

```bash
# Build everything
cargo build --workspace

# Build optimized (use for benchmarks)
RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C lto=fat -C codegen-units=1" \
  cargo build --release --workspace

# Run all unit tests
cargo test --workspace

# Run a specific crate's tests
cargo test -p senko-store

# Run compatibility tests (requires a running senko instance on :6379)
cargo test --test compat -- --test-threads=1

# Run benchmarks (specific crate)
cargo bench -p senko-store

# Run benchmarks with perf-counters feature
cargo bench -p senko-store --features perf-counters

# Check for unused dependencies
cargo +nightly udeps --workspace

# Miri (memory safety check for unsafe code)
cargo +nightly miri test -p senko-store -- bptree

# Fuzz the RESP parser
cargo +nightly fuzz run resp_parser
```

---

## Adding a New Command

1. Add the command function to the appropriate `commands/` module.
   Signature: `fn cmd_xyz(store: &mut Store, frames: &[Frame]) -> Result<Response, senkoError>`

2. Register it in `senko-net/src/dispatch.rs` in the `phf::Map`:
   ```rust
   b"XYZ" => cmd_xyz,
   ```
   Include all known aliases (e.g., `b"SUBSTR"` → `cmd_getrange`).

3. Add unit tests in the command module itself.

4. Add a compat test in `tests/compat/<type>.rs` using the `redis-rs` client.

5. Add a `criterion` benchmark in `senko-store/benches/<type>.rs` if hot-path.

### Phase 1 Command Limits

- `MIGRATE` is intentionally unsupported in Phase 1. Parse and validate its
  arguments, then return the exact error string:
  `ERR MIGRATE not supported in senko Phase 1`.
- `WAIT` and `WAITAOF` are placeholders until replication / AOF exist. They
  must validate integer arguments, then return immediate zero results.
- `MOVE` must treat DB `0` as "already there" and reject all other DB indexes
  until multi-DB support exists.

---

## What Dragonfly Does (Reference Architecture)

Understanding Dragonfly's choices helps explain senko's decisions:

| Feature | Dragonfly | senko approach |
|---|---|---|
| Keyspace | DASH table (B+ tree inspired open-addressing) | `hashbrown::RawTable` + incremental rehash |
| Sorted set | listpack → **B+ tree** (their own, since v1.9, default v1.11) | same |
| Stream | radix tree of listpack macro-nodes (same as Redis) | same |
| Hash | listpack → hashtable (same as Redis) | same |
| Set | intset → listpack → hashtable (same as Redis) | same |
| Threading | shared-nothing, thread-per-core | same |
| Async runtime | custom helio (io_uring) | compio (io_uring/IOCP/kqueue) |

senko's B+ tree for sorted sets directly mirrors Dragonfly's `bptree_set.h`.
The stream radix tree is Redis-compatible (Dragonfly did not change it).

---

## What NOT To Do

These are the most common mistakes to avoid:

```rust
// DON'T use std HashMap anywhere
use std::collections::HashMap; // ❌

// DON'T allocate in the RESP parser hot path
fn parse_frame(input: &[u8]) -> Frame {
    let s = String::from_utf8(input.to_vec()); // ❌ allocation
}

// DON'T share the store across threads
let store = Arc::new(Mutex::new(Store::new())); // ❌ entire architecture is wrong

// DON'T add async to store methods
async fn hget(&mut self, key: &[u8], field: &[u8]) -> Option<&[u8]> {} // ❌

// DON'T leave dead code
fn unused_helper() {} // ❌ will not compile (#[deny(dead_code)] is on)

// DON'T invent Redis error strings
return Err(senkoError::Custom("field not found".into())); // ❌

// DON'T use todo!() or unimplemented!()
fn cmd_lpos(...) { todo!() } // ❌

// DON'T downgrade encoding on remove
if self.len < 64 { self.upgrade_to_listpack(); } // ❌ Redis never downgrades

// DON'T forget to notify blocked waiters after mutations
store.lpush(key, value); // ❌ missing: registry.notify(&key, store)

// DON'T forget to auto-delete empty containers
store.hdel(key, field); // ❌ missing: store.remove_hash_if_empty(key)
```

---

## Compat Test Pattern

All compat tests follow this pattern:

```rust
// tests/compat/string.rs
use redis::{Client, Commands};

fn connect() -> redis::Connection {
    Client::open("redis://127.0.0.1:6379")
        .unwrap()
        .get_connection()
        .unwrap()
}

#[test]
fn test_set_get_roundtrip() {
    let mut conn = connect();
    let _: () = redis::cmd("SET").arg("foo").arg("bar").query(&mut conn).unwrap();
    let val: String = redis::cmd("GET").arg("foo").query(&mut conn).unwrap();
    assert_eq!(val, "bar");
}

#[test]
fn test_set_nx_existing_key() {
    let mut conn = connect();
    let _: () = redis::cmd("SET").arg("k").arg("v1").query(&mut conn).unwrap();
    let result: Option<String> = redis::cmd("SET")
        .arg("k").arg("v2").arg("NX")
        .query(&mut conn).unwrap();
    assert!(result.is_none()); // NX fails on existing key
    let val: String = redis::cmd("GET").arg("k").query(&mut conn).unwrap();
    assert_eq!(val, "v1"); // original value unchanged
}
```

Tests run sequentially (`--test-threads=1`) to avoid port conflicts and
key collisions between tests. Each test should clean up its keys or use
unique key names with a test-specific prefix.

---

## Phase 2 Readiness Checklist

While implementing Phase 1, keep these constraints so Phase 2 (embedded mode)
requires minimal refactoring:

- [ ] `senko-store` has zero dependency on `compio`, `tokio`, or any runtime
- [ ] `Store::new()` takes only a `&senkoConfig`, no async context
- [ ] All store methods are `&mut self` synchronous functions
- [ ] `senko-core` types (`SenkoValue`, `senkoError`, `senkoConfig`) are
      `Send + Sync` and have no runtime-specific lifetimes
- [ ] The `BlockedKeyRegistry` lives in `senko-net`, not in `senko-store`
      (blocking is a network concern, not a storage concern)

## Module System Direction

Phase 1 has no module system. `MODULE LOAD`, `MODULE LOADEX`, and
`MODULE UNLOAD` must remain explicit stubs until the feature exists.

Phase 2 design intent:

- senko will not expose Redis's C module ABI.
- The eventual module API should be Rust-first, based on `cdylib` plugins and
  a narrow FFI surface that preserves senko's threading and ownership model.
- Modules must not violate the shared-nothing shard architecture or require
  async inside `senko-store`.

---

## Performance Targets

These are the benchmark targets that define "beats DragonflyDB":

| Operation | Target (ops/sec per core) |
|---|---|
| SET | ≥ 1.5M |
| GET | ≥ 2M |
| INCR | ≥ 1.5M |
| HGET (listpack) | within 10% of GET |
| HGET (hashtable) | within 20% of GET |
| ZADD (B+ tree) | ≥ 2M |
| ZSCORE | ≥ 8M (member_index O(1)) |
| XADD (same fields) | ≥ 2M |
| LPUSH/RPUSH | within 15% of SET |

Run benchmarks with:
```bash
RUSTFLAGS="-C target-cpu=native" cargo bench -p senko-store
```

Pin to a specific core for reproducible results:
```bash
taskset -c 0 cargo bench -p senko-store
```

---

## Dependency Policy

New dependencies require justification. Prefer:

| Need | Approved crate |
|---|---|
| Hash maps | `hashbrown` |
| String type | `compact_str` |
| Byte buffers | `bytes` |
| Hashing | `ahash` |
| BLAKE3 | `blake3` |
| Float fmt | `ryu` |
| Float parse | `fast_float` |
| SIMD patterns | `memchr` |
| Random | `rand` (SmallRng only) |
| Roaring bitmaps | `roaring` |
| Perfect hash | `phf` |
| Constant-time cmp | `subtle` |
| Serde | only for config files, never on hot paths |
| Async runtime | `compio` only (in `senko-net` / `senko-server`) |

Do **not** add `tokio`, `async-std`, `actix`, or any HTTP framework.

---

*Last updated: Phase 1 — String, Hash, List, Set, ZSet, Stream commands.*
