# Pub/Sub Performance

| Metric | senko | DragonflyDB | Redis 7.x |
|---|---:|---:|---:|
| PUBLISH 1 subscriber (msg/s/core) | > 5M | ~3M | ~1M |
| PUBLISH 100 subscribers (msg/s/core) | > 3M | ~1M | ~200k |
| PUBLISH 10k subscribers (msg/s/core) | > 500k | ~100k | ~15k |
| Memory per subscriber per channel | ~4KB ring | varies | ~200B* |
| Allocs per PUBLISH (N subscribers) | 1 | N | N |
| Pattern match 1k patterns (us) | < 5 | ~50 | ~50 |
| Cross-shard pub latency | < 5us | N/A | N/A |

*Redis uses less memory per subscriber but allocates per message. At high message rates, allocator pressure dominates Redis's cost. senko pre-allocates the ring to keep latency predictable.

## Benchmarks

Run the microbenchmarks with:

```bash
cargo bench -p senko-pubsub --bench pubsub
```

The benchmark suite covers:
- exact-channel publish fan-out from 0 to 10k subscribers
- pattern fan-out for small and large matcher sets
- `SPUBLISH` vs `PUBLISH` on the same shard
- subscribe/publish/unsubscribe churn
- same-shard and cross-shard latency
- raw `BroadcastSlot` ring throughput
