# diceDB — Redis-Compatible In-Memory TCP Server

## One-line Summary
Built a Redis-compatible in-memory TCP server in Node.js supporting 20k concurrent clients, RESP2 protocol, 11 commands (incl. INFO), pipelining, AOF/RDB persistence with background rewrite, active expiry, and **allkeys-random eviction** — achieving ~9K ops/sec with zero fd leaks across 260+ tests.

## Key Achievements

### 1. High-Performance Concurrent Server Engine
Engineered a Redis-compatible in-memory TCP server (diceDB) supporting 20,000 concurrent clients with sub-millisecond latency, achieving 9,000 ops/sec on pipelined workloads — by implementing RESP2 protocol, custom diceObj type system (INT/EMBSTR/RAW encoding), and lock-free epoll-modeled event loop in Node.js (libuv).

### 2. Crash-Safe Persistence Layer with allkeys-random Eviction
Built crash-safe persistence layer with AOF (append-only file) + RDB (binary snapshot) supporting background rewrite and atomic restart recovery — reducing data loss to zero while adding only 17% throughput overhead. Implemented **allkeys-random eviction** with environment-aware thresholds (dev/test: 200 keys, 40% eviction; prod: 200K keys, 4% eviction), validated via 260+ automated tests covering torn-tail recovery, pipeline ordering, type-preserving serialization, and random eviction correctness.

## Technical Highlights
- **Protocol**: Full RESP2 implementation (decoder/encoder) with inline fallback
- **Data Model**: Redis `robj`-inspired `diceObj` with 4-bit type + 4-bit encoding packing
- **Concurrency**: 20k clients via libuv epoll/kqueue, per-fd 1MiB buffer guards, backpressure
- **Commands**: PING, SET (EX/PX), GET, TTL, DEL, EXPIRE, PEXPIRE, INCR, BGSAVE, BGREWRITEAOF, SAVE, **INFO**
- **Pipeline**: Cork/uncork coalescing for single-round-trip N-command batches
- **Expiry**: Lazy + active (20-key sampling, 0.25 fraction threshold, 3-pass max)
- **Eviction**: **allkeys-random** — Fisher-Yates shuffle on key array, env-configurable thresholds (dev/test: 200 keys/40%, prod: 200K keys/4%), `evictAllRandomKeys(count)` algorithm
- **Persistence**: AOF rewrite-buffer semantics, RDB2 format with RDB1 compat, atomic rename
- **Observability**: INFO command with server, stats, memory, keyspace, eviction sections
- **Testing**: 260+ tests across 9 suites (protocol, commands, eviction, pipeline, persistence, restart recovery, diceObj, INCR, INFO)