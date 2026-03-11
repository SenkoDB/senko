# senko

senko (`閃光`, "flash of light") is a Redis-compatible in-memory data store written in Rust.
The project is organized as a workspace of focused crates: a storage engine, RESP protocol
implementation, network server, and optional feature modules such as JSON, vector search,
probabilistic data structures, search, and time-series support.

The design target is straightforward:

- keep the hot path in Rust with predictable memory layouts
- preserve Redis wire compatibility where implemented
- use a shard-per-thread execution model instead of a shared global store
- stay modular enough to grow beyond a single binary

This repository is in active development. It already contains a substantial compatibility and
benchmarking surface, but it is not yet a complete Redis replacement for every production use case.

## Current status

Senko currently includes:

- a server binary in [`senko-server`](./senko-server)
- a synchronous storage engine in [`senko-store`](./senko-store)
- a RESP parser/serializer in [`senko-proto`](./senko-proto)
- network dispatch, pub/sub, cluster plumbing, and server command handling in [`senko-net`](./senko-net)
- optional module crates:
  - [`senko-json`](./senko-json)
  - [`senko-prob`](./senko-prob)
  - [`senko-search`](./senko-search)
  - [`senko-ts`](./senko-ts)
  - [`senko-vector`](./senko-vector)
- additional support crates:
  - [`senko-core`](./senko-core)
  - [`senko-cluster`](./senko-cluster)
  - [`senko-pubsub`](./senko-pubsub)
  - [`senko-sentinel`](./senko-sentinel)

The command surface already covers much of the common Redis workflow for:

- strings
- hashes, including field-expiry behavior
- lists, including blocking list operations
- sets
- sorted sets
- streams
- transactions
- pub/sub
- a large subset of server, config, and compatibility commands

The repo also ships:

- unit and crate-level integration tests
- Redis compatibility-style tests under [`tests/compat`](./tests/compat)
- imported Redis test assets under [`tests/tests`](./tests/tests)
- Criterion benchmarks across the store and network layers

## Architecture

At a high level:

1. `senko-server` parses CLI/config and boots one or more shard runtimes.
2. `senko-net` owns sockets, connection state machines, dispatch, blocking clients, pub/sub,
   and server-side control commands.
3. `senko-store` owns synchronous command execution and the in-memory data structures.
4. `senko-core` defines shared config, errors, module interfaces, and value types.

The intended execution model is shared-nothing sharding: each worker owns its local state rather
than routing every operation through a single locked map. Optional capabilities are compiled in as
built-in modules through Cargo features instead of being loaded dynamically at runtime.

## Workspace layout

```text
senko/
├── senko-core/       Shared config, errors, values, module traits
├── senko-proto/      RESP parser and serializer
├── senko-store/      In-memory store and command logic
├── senko-net/        Listeners, connections, dispatch, pub/sub, cluster plumbing
├── senko-server/     Main server binary and CLI
├── senko-sentinel/   Sentinel-related functionality
├── senko-cluster/    Cluster routing and slot helpers
├── senko-pubsub/     Pub/sub registries and fan-out structures
├── senko-json/       Optional JSON module
├── senko-prob/       Optional probabilistic structures module
├── senko-search/     Optional search module
├── senko-ts/         Optional time-series module
├── senko-vector/     Optional vector similarity module
└── tests/            Compatibility tests and imported Redis test assets
```

## Building

Build the whole workspace:

```bash
cargo build --workspace
```

Build the server only:

```bash
cargo build -p senko-server
```

Build with optional modules:

```bash
cargo build -p senko-server --features "module-json module-prob module-search module-ts module-vector"
```

Relevant server features in [`senko-server/Cargo.toml`](./senko-server/Cargo.toml):

- `module-json`
- `module-prob`
- `module-search`
- `module-ts`
- `module-vector`
- `json`
- `simd`

## Running

Start the server with defaults:

```bash
cargo run -p senko-server
```

Use an explicit config file:

```bash
cargo run -p senko-server -- --config ./senko.toml
```

Print the generated default config:

```bash
cargo run -p senko-server -- default-config
```

Validate a config file:

```bash
cargo run -p senko-server -- check-config ./senko.toml
```

The Clap command name exposed by the binary is `senkodb`, while the compiled artifact from Cargo
is `senko-server`.

By default the server listens on port `6379`. Common overrides are also available through CLI flags
or environment variables, including:

- `--port` / `SENKO_PORT`
- `--bind` / `SENKO_BIND`
- `--io-threads` / `SENKO_IO_THREADS`
- `--requirepass` / `SENKO_REQUIREPASS`
- `--aclfile` / `SENKO_ACLFILE`
- `--maxmemory` / `SENKO_MAXMEMORY`
- `--replicaof` / `SENKO_REPLICAOF`
- `--cluster-enabled` / `SENKO_CLUSTER_ENABLED`
- `--tls-port` / `SENKO_TLS_PORT`

Example configuration files live in [`examples/`](./examples).

## Testing

Run the full workspace test suite:

```bash
cargo test --workspace
```

Run store tests only:

```bash
cargo test -p senko-store
```

Run compatibility tests that talk to a running server on `127.0.0.1:6379`:

```bash
cargo test --test compat -- --test-threads=1
```

Many compatibility tests are written against `redis-rs` and intentionally exercise Redis error
shapes and protocol behavior, not just happy-path value round-trips.

## Benchmarks

Run the storage benchmarks:

```bash
cargo bench -p senko-store
```

Run network benchmarks:

```bash
cargo bench -p senko-net
```

For more reproducible local numbers, build with CPU-specific optimizations:

```bash
RUSTFLAGS="-C target-cpu=native" cargo bench -p senko-store
```

## Modules and optional capabilities

Senko has a built-in module registry. Optional functionality is enabled at compile time and
registered during server startup. Today that includes crates for:

- JSON values and commands
- probabilistic data structures
- vector search
- search
- time-series

This is intentionally different from Redis modules loaded from shared libraries at runtime.

## Compatibility and limitations

This is the most important section if you are evaluating Senko for actual use.

### Redis compatibility is broad, not complete

Senko implements a large and growing subset of Redis behavior, and the repo contains significant
compatibility coverage, but you should not assume perfect parity with every Redis command, option,
or corner case. Validate the exact command mix your application uses.

### Known hard limitations

- Only logical DB `0` is currently meaningful. `MOVE` returns `0` for DB `0` and rejects other DBs.
- `MIGRATE` is explicitly unsupported and returns `ERR MIGRATE not supported in Senko Phase 1`.
- `WAIT` is currently a placeholder and returns `0` after argument validation.
- `WAITAOF` is currently a placeholder and returns `[0, 0]` after argument validation.
- `MODULE LOAD`, `MODULE LOADEX`, and `MODULE UNLOAD` are not supported in Phase 1.
- Optional modules are build-time features, not runtime-loaded plugins.

### Replication, failover, and persistence should be treated carefully

The workspace contains replication, cluster, sentinel, and persistence code, but some of that
surface is still compatibility-oriented or partial:

- `PSYNC` currently responds with a `FULLRESYNC ... 0` style stub rather than a complete mature
  replication implementation.
- `BGREWRITEAOF` currently exposes command compatibility but does not represent a full AOF
  rewrite pipeline on its own.
- RDB snapshot commands and related server reporting exist, but you should verify durability and
  restart behavior against your own workload before treating the system as production-ready.

### Production-readiness depends on your feature set

If you only need the implemented core data types and have verified the behavior you rely on,
Senko may already be useful for experimentation, benchmarking, and targeted workloads. If you need
full Redis operational parity, mature replication, dynamic modules, or every server-side edge case,
you should treat the project as in-progress.

## Recommended evaluation workflow

1. Build the server with only the features you need.
2. Run `cargo test --workspace`.
3. Run the relevant files under [`tests/compat`](./tests/compat).
4. Exercise your own client library and command mix against a local instance.
5. Benchmark with your expected payload sizes and concurrency instead of relying on generic claims.

## License

MIT. See the workspace manifest in [`Cargo.toml`](./Cargo.toml).
