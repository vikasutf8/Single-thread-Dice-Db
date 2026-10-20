# diceDB — Redis-Compatible In-Memory Database (Node.js)

A Redis-compatible in-memory key-value store built from scratch in Node.js using only the standard library. It speaks RESP2 over TCP, runs a single-threaded non-blocking event loop (libuv epoll/kqueue), and implements commands, expiry, eviction, persistence, pipelining, transactions, and graceful shutdown.

**Final status: 418/418 tests green across 13 suites · 270k cmds/s in prod load · zero fd leaks.**

---

## Table of Contents

- [Highlights](#highlights)
- [Feature Matrix](#feature-matrix)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Command Reference](#command-reference)
- [Test Matrix](#test-matrix)
- [Load Test Matrix](#load-test-matrix)
- [Design Conclusions](#design-conclusions)
- [Documentation Index](#documentation-index)
- [Milestones](#milestones)
- [Known Limits](#known-limits)

---

## Highlights

- **15 Redis commands** — KV, admin, and transactions, wire-exact RESP2
- **20,000 concurrent client cap** (design); admission control, 1 MiB/fd buffer guard, backpressure
- **Transactions** — per-client `MULTI` / `EXEC` / `DISCARD` with `+QUEUED`, `*N` array replies, `EXECABORT`, full client isolation
- **Persistence** — AOF (per-write RESP frames) + RDB2 binary snapshots, crash-safe via tmp+rename, background `BGSAVE` / `BGREWRITEAOF`
- **Eviction** — `allkeys-lru` (24-bit clock + 16-entry pool) and `allkeys-random` (Fisher-Yates), env-aware thresholds
- **Expiry** — lazy on read + active cron (20-key sampling, 0.25 fraction break, 3 passes max)
- **Pipeline** — cork/uncork single flush per EPOLLIN hit, `maxBatchDepth` observability
- **Graceful shutdown** — drain, force-close timeout, AOF fsync, optional RDB, SIGINT/SIGTERM, idempotent
- **Observability** — `INFO` with server / stats / memory / keyspace / eviction sections; `stats()` in server handle
- **Testing** — 418 automated tests; 9 dated load runs; restart recovery proven over real TCP

---

## Feature Matrix

| Area | Implementation | Key details |
|------|----------------|-------------|
| Protocol | RESP2 encoder + decoder | `+`, `-`, `:`, `$`, `*`, nulls, nested arrays, streaming `n` |
| Data model | `diceObj` | `{ value, expireAt, typeEncoding, lastAccessedAt }`; type high nibble / encoding low nibble |
| Encodings | INT / EMBSTR / RAW | INT for integers, EMBSTR ≤ 44 bytes, RAW otherwise |
| TCP server | `RunAsyncTcpServer` | epoll mental model, `FdComm` registry, `connectedClients` Map |
| Concurrency | 20k clients | `maxClients`, `backlog`, reject over limit, 1 MiB leftover cap |
| Backpressure | pause/drain | `writableLength > 1 MiB` → pause → resume on `drain` |
| Pipeline | cork/uncork | 1 write → N cmds → N replies; `maxBatchDepth=200` proven |
| Transactions | per-client queue | `cqueue`, `isTxn`, `txnDirty`, `executing`; no rollback/isolation (Redis semantics) |
| Expiry | lazy + active | `DeleteExpiredKeys()` cron every 1 s (configurable) |
| Eviction | LRU + random | 24-bit LRU clock, 16-entry pool; dev/test 200 keys/40%, prod 200k keys/4% |
| Persistence | AOF + RDB | AOF replay then RDB load; effective-op logging; torn tail ignored |
| Shutdown | graceful | reject new → drain → force-close → AOF fsync → RDB → timers cleared |
| Observability | `INFO` + `stats()` | keyspace, memory, eviction, persistence flags, batch depth |

---

## Architecture

```text
                        ┌──────────────────────────────┐
  redis-cli / client ──►│ async.tcp.js                 │
                        │  net.Server (libuv epoll)    │
                        │  FdComm{fd, socket, leftover}│
                        │  connectedClients Map        │
                        │  cork → drain → uncork       │
                        └──────────────┬───────────────┘
                                       │ readCmd()
                                       ▼
                        ┌──────────────────────────────┐
                        │ eval.js                      │
                        │  evalAndRespond(cmd, client) │
                        │   ├─ txn queue → +QUEUED     │
                        │   └─ executeCommand → Buffer │
                        └──────┬───────────┬───────────┘
                               │           │
                     ┌─────────▼──┐   ┌────▼─────────┐
                     │ store.js   │   │ aof.js/rdb.js│
                     │ diceObj    │   │ persistence  │
                     │ expiry     │   └──────────────┘
                     │ eviction   │
                     └────────────┘
```

**Source files**

| File | Responsibility |
|------|----------------|
| `async.tcp.js` | Async multiplexed server, per-client state, graceful shutdown |
| `eval.js` | Command dispatch, transaction queueing/EXEC, AOF hooks |
| `store.js` | `diceObj`, lazy/active expiry, eviction (LRU + random), `INFO` data |
| `aof.js` | Append-only log, replay, background rewrite |
| `rdb.js` | RDB1/RDB2 snapshot, load, background save |
| `redis-cmd.js` | RESP → `{ Cmd, Args }` command parser |
| `resp-encoder.js` | RESP reply encoder (simple, bulk, error, integer, array) |
| `resp-decoder.js` | RESP2 decoder with byte-consumed tracking |
| `main.js` | Synchronous demo server (same eval path) |

---

## Getting Started

No dependencies. Node.js 18+.

```bash
# Start the async server (default)
node -e 'require("./async.tcp.js").RunAsyncTcpServer("127.0.0.1", 8080)'

# Try it
redis-cli -p 8080 PING
redis-cli -p 8080 SET name vikas EX 60
redis-cli -p 8080 GET name

# Sync demo server
node main.js --host 127.0.0.1 --port 8080
```

**With persistence + graceful shutdown**

```bash
node -e '
const { RunAsyncTcpServer, setupGracefulShutdown } = require("./async.tcp.js");
const srv = RunAsyncTcpServer("127.0.0.1", 8080, { aofEnabled: true, rdbLoad: true });
setupGracefulShutdown(srv, { saveRDB: true, flushAOF: true, timeoutMs: 10000 });'

# Ctrl-C -> drain clients -> fsync AOF -> save RDB -> exit
```

**Run everything**

```bash
for f in test-*.js; do node "$f"; done   # 418 passed, 0 failed (13 suites)
```

---

## Command Reference

| Command | Syntax | Reply | AOF |
|---------|--------|-------|-----|
| `PING` | `PING [message]` | `+PONG` / bulk echo | No |
| `SET` | `SET key value [EX sec] [PX ms]` | `+OK` | Yes |
| `GET` | `GET key` | bulk / `$-1` | No |
| `TTL` | `TTL key` | `:-2` / `:-1` / seconds | No |
| `DEL` | `DEL key [key ...]` | `:count` | Yes |
| `EXPIRE` | `EXPIRE key seconds` | `:1` / `:0` | Yes |
| `PEXPIRE` | `PEXPIRE key ms` | `:1` / `:0` | Yes |
| `INCR` | `INCR key` | `:new-value` | Yes |
| `BGSAVE` | `BGSAVE` | `+Background saving started` | No |
| `BGREWRITEAOF` | `BGREWRITEAOF` | `+Background append only file rewriting started` | No |
| `SAVE` | `SAVE` | `+OK` | No |
| `INFO` | `INFO` | bulk sections | No |
| `MULTI` | `MULTI` | `+OK` / nested error | No |
| `EXEC` | `EXEC` | `*N` array / `-EXECABORT` | per command |
| `DISCARD` | `DISCARD` | `+OK` / `-ERR DISCARD without MULTI` | No |

**Transaction wire example**

```text
MULTI            -> +OK
SET k 1          -> +QUEUED
GET k            -> +QUEUED
EXEC             -> *2\r\n+OK\r\n$1\r\n1\r\n
```

Queue-time errors (unknown command / wrong arity) → `-EXECABORT Transaction discarded because of previous errors.`

---

## Test Matrix

| Suite | Tests | Coverage |
|-------|------:|----------|
| `test-aof-rdb.js` | 32 | AOF encode/replay/rewrite, RDB save/load, torn tail, TCP restart recovery |
| `test-del-expire.js` | 37 | DEL counts, EXPIRE/PEXPIRE `0`/negative → DEL, active cron proof |
| `test-diceobj.js` | 42 | 4+4 bit pack/unpack, INT/EMBSTR/RAW, RDB2/RDB1, WRONGTYPE |
| `test-evict.js` | 22 | LRU pattern correctness, random eviction, env configs, pool, policy switch |
| `test-get-set-ttl.js` | 34 | SET EX/PX, GET, TTL, dispatch, single-write invariant |
| `test-graceful-shutdown.js` | 17 | Drain, reject, AOF fsync, RDB reload, signals, idempotency, timeout |
| `test-incr.js` | 21 | Missing→1, overflow, TTL preserved, WRONGTYPE, AOF replay |
| `test-load-graceful.js` | 21 | Prod load + shutdown under load (rounds A/B/C) |
| `test-load-multi.js` | 37 | Prod transaction load (rounds A–E, 108.5k cmds) |
| `test-multi.js` | 79 | MULTI/EXEC/DISCARD direct + wire + per-client isolation |
| `test-ping.js` | 20 | PING variants, errors, pipelined |
| `test-pipeline.js` | 21 | 4-in-1 batch, error mid-batch, split packet, stats depth |
| `test-resp-decoder.js` | 35 | All RESP2 types, nulls, nesting, streaming, malformed input |
| **Total** | **418** | **13 suites, 0 failures** |

---

## Load Test Matrix

All runs in `load_test.md` (append-only). No `redis-benchmark` on the box — scripted RESP-over-TCP with byte-exact reply comparison.

| Run (UTC) | Focus | Result |
|-----------|-------|--------|
| 2026-10-14 16:06 | 3 concurrent clients, PING/SET/GET/TTL | PASS, active→0, monitored→0 |
| 2026-10-14 16:27 | DEL / EXPIRE / PEXPIRE + cron | 126/126 |
| 2026-10-14 16:36 | First eviction (`maxKeys=10`) | 139/139, oldest keys evicted |
| 2026-10-15 15:09 | Pipeline, 50–200 conns | 3,200 cmds; 5.5k–8.2k cmds/s; `maxBatchDepth=200` |
| 2026-10-15 15:40 | AOF enabled | 2,000 cmds in 293 ms (6.8k/s) vs 8.2k/s baseline → ~17% AOF cost |
| 2026-10-15 16:10 | `diceObj` encodings | 2,000 cmds in 247 ms (8.1k/s), no regression |
| 2026-10-15 16:30 | INCR RMW burst | 2,100 cmds in 234 ms (9.0k/s), exact `:20` finals |
| 2026-10-19 17:23 | **Prod env** (200k keys / 4% / LRU) | A: 20k cmds, 96 ms, **208k cmds/s**; B: 250k SETs, 878 ms, **285k cmds/s**, 50k evicted (3,125 events, pool ≤16); C: shutdown under load **113 ms**, AOF 20.7 KB, RDB 600/600 |
| 2026-10-20 06:59 | **Transactions (prod)** | A 24k @ 179k/s · B 50.5k @ 282k/s · C (DISCARD) 22k @ 468k/s · D (EXECABORT) 3k @ 300k/s · E (mixed) 9k @ 281k/s; **total 108.5k cmds @ 270k cmds/s**, zero mismatches, clients→0, monitored→0 |

**Throughput journey:** 5.5k → 8.2k (pipeline) → 9.0k (INCR) → **208k–489k cmds/s** in prod-config rounds.

**Durability/cleanup invariants proven in every run:** byte-exact replies, `active → 0`, `monitored → 0` (no fd leak), zero stray AOF/RDB files.

---

## Design Conclusions

1. **Redis semantics over shortcuts** — reply shapes, error strings (`WRONGTYPE`, `EXECABORT`), termination rules, and effective-op AOF logging are wire-compatible.
2. **Pipeline ≠ transaction** — pipeline executes each parsed command immediately; a mid-batch error is one `-ERR` and execution continues. Transactions queue, then run back-to-back at `EXEC`. Neither gives ACID rollback/isolation — same as Redis.
3. **Approximate LRU is enough** — 24-bit clock (1 s resolution) + 16-entry pool + sampled access tracking evicts genuinely idle keys; verified by access-pattern tests and prod pressure (250k keys against a 200k cap).
4. **Persistence converges, not replays blindly** — `EXPIRE k 0` is logged as `DEL k`; rewrites emit TTL as `PX <remaining>`; torn AOF tails are ignored; RDB load skips expired keys. Restart recovery was proven over real TCP for both engines.
5. **Graceful shutdown loses no accepted command** — drain window + timeout + force-close, AOF fsync, optional RDB. RDB reload matched keys-at-save (600/600) during live load; shutdown completed in 113 ms.
6. **`diceObj` is future-proof** — the 4-bit type / 4-bit encoding byte leaves the full Redis type space open; adding LIST/HASH/SET/ZSET needs no migration, and the `WRONGTYPE` seam already exists.
7. **Guards matter at scale** — admission control, per-fd buffer caps, backpressure, and OOM destroys prevent one hostile client from taking down the process.
8. **AOF is the durability cost** — ~17% throughput overhead for per-write appends; `SAVE` remains the explicit durable point.

---

## Documentation Index

Every feature follows the same spec-first format: **What / Plan / Arch / Executed / Tested / Conclusion**.

| Doc | Contents |
|-----|----------|
| `async_tcp.md` | Epoll-modeled async server, 20k guards, Go→Node mapping |
| `ping-command-notes.md` | First command, RESP command loop, error handling |
| `resp-decoder-notes.md` | RESP2 decoder, streaming, 35-test log |
| `redis_cmd_impl.md` | Spec-first contract: SET/GET/TTL/DEL/EXPIRE/INCR/eviction |
| `pipeline.md` | Cork/uncork, pipeline vs transaction, batch stats |
| `diceobj.md` | redisObject port, nibble layout, RDB2, WRONGTYPE |
| `persistence.md` | AOF + RDB design, rewrite buffer, crash safety |
| `eviction.md` | allkeys-random + allkeys-lru, LRU clock, eviction pool |
| `commands.md` | 15-command reference, dispatch design, test matrix |
| `transactions.md` | MULTI/EXEC/DISCARD, per-client state, EXECABORT |
| `graceful-shutdown.md` | Drain sequence, fsync, signals, load-under-shutdown |
| `load_test.md` | Append-only benchmark history (9 runs) |
| `tcp-echo-server-notes.md` | Original Go/Node echo-server foundation |
| `resume.md` | Earlier project snapshot (pre-transactions) |

---

## Milestones

| Commit | Milestone |
|--------|-----------|
| `c86c031` | First commit |
| `072842f` | RESP decoder with major functionality |
| `7c6d44b` | PING implementation |
| `a3d06ac` | Async TCP connection |
| `05e495a` | GET / SET / TTL |
| `26cc309` | DEL and expire |
| `dc45739` | Simple eviction |
| `347dd7b` | Pipeline |
| `c9ae501` | AOF and RDB |
| `d298c04` | Redis object (`diceObj`) |
| `88413a6` | Resume point |
| `682b774` | Database statistics + allkeys-random eviction |
| `0b8c81a` | Approximate LRU |
| `6b60c33` | Graceful DB shutdown |
| `adad351` | Stateful transactions |

---

## Known Limits

- Strings only — no LIST / HASH / SET / ZSET (type nibble reserved)
- No `SCAN`, `KEYS`, `FLUSHALL` (use `clearStore()` in-process)
- No `WATCH` / optimistic locking, no pub/sub
- No replication, clustering, or partial resync
- No auth/ACL/TLS
- No `fsync` policy knob; AOF relies on OS flush (`SAVE` is the durable point)
- LRU clock resolution is 1 second (accesses within the same second tie)
- `main.js` sync demo does not wire signal handlers (async server is the production path)
