# Commands Implemented — diceDB (`eval.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Redis-compatible command set covering core KV operations + admin + transactions
- 15 commands: `PING`, `SET`, `GET`, `TTL`, `DEL`, `EXPIRE`, `PEXPIRE`, `INCR`, `BGSAVE`, `BGREWRITEAOF`, `SAVE`, `INFO`, `MULTI`, `EXEC`, `DISCARD`
- RESP2 wire protocol: each `eval*()` returns Buffer, `evalAndRespond` dispatches
- Pipeline support: cork/uncork in `async.tcp.js` coalesces N replies into single write
- Transactions: per-client queue, `+QUEUED`, EXEC array replies, DISCARD, EXECABORT
- AOF logging on successful writes only (effective-op semantics)
- Type safety: `WRONGTYPE` errors on type mismatch (strings only for now)
- Arity validation with Redis-compatible error messages

## 2. What we plan

| Command | Syntax | Reply | AOF Logged | Notes |
|---------|--------|-------|------------|-------|
| `PING` | `PING [message]` | `+PONG` / `$bulk` | No | Read-only |
| `SET` | `SET key value [EX sec] [PX ms]` | `+OK` | Yes | Auto-encodes INT/EMBSTR/RAW |
| `GET` | `GET key` | `$value` / `$-1` | No | Lazy expiry, WRONGTYPE |
| `TTL` | `TTL key` | `:-2`/`:-1`/`:sec` | No | Ceil seconds, lazy expiry |
| `DEL` | `DEL key [key...]` | `:count` | Yes | Expired=missing, `:0` ok |
| `EXPIRE` | `EXPIRE key seconds` | `:1`/`:0` | Yes | `≤0` → DEL, missing→`:0` |
| `PEXPIRE` | `PEXPIRE key ms` | `:1`/`:0` | Yes | Millisecond precision |
| `INCR` | `INCR key` | `:newval` | Yes | Missing→1, TTL preserved, re-encode |
| `BGSAVE` | `BGSAVE` | `+Background saving started` | No | Async, guard flag |
| `BGREWRITEAOF` | `BGREWRITEAOF` | `+Background append only file rewriting started` | No | Async, rewrite buffer |
| `SAVE` | `SAVE` | `+OK` | No | Blocking snapshot |
| `INFO` | `INFO` | `$bulk` (sections) | No | Server/stats/memory/keyspace/eviction |
| `MULTI` | `MULTI` | `+OK` | No | Opens per-client txn; nested → error |
| `EXEC` | `EXEC` | `*N` array | No | Runs queue in order; dirty → EXECABORT |
| `DISCARD` | `DISCARD` | `+OK` | No | Drops queue, keyspace untouched |

## 3. What we arch (design)

```text
evalAndRespond(cmd, client):
  if client.isTxn && !client.executing && verb ∉ {MULTI, EXEC, DISCARD}:
    validate arity/verb → cqueue.push(cmd) → +QUEUED
    unknown/bad arity → -ERR + client.txnDirty = true
  else:
    client.write(executeCommand(cmd, client))

executeCommand(cmd, client) -> Buffer:      # pure dispatch, no socket write
  switch cmd.Cmd.toUpperCase():
    PING → evalPING
    SET  → evalSET
    GET  → evalGET
    TTL  → evalTTL
    DEL  → evalDEL
    EXPIRE → evalEXPIRE
    PEXPIRE → evalPEXPIRE
    INCR → evalINCR
    BGSAVE → evalBGSAVE
    BGREWRITEAOF → evalBGREWRITEAOF
    SAVE → evalSAVE
    INFO → evalINFO
    MULTI → evalMULTI (start queue)
    EXEC → evalEXEC (evalMulti: *N array of queued replies)
    DISCARD → evalDISCARD (reset queue)
    default → evalDefault (-ERR unknown command)

Each eval*():
  1. Validate arity + arg types → EncodeError on failure
  2. Execute via store.js (put/get/del/setExpire/DeleteExpiredKeys)
  3. On success: appendCommand({Cmd, Args}) for write commands
  4. conn.write(Encode*) — direct calls only; executeCommand passes NULL_CONN
  5. Return Buffer (for EXEC array assembly and test assertions)

EXEC:
  dirty → reset state + -EXECABORT Transaction discarded because of previous errors.
  else  → executing=true, replies = cqueue.map(executeCommand), reset
          → EncodeArray(replies): *N\r\n + reply bytes in order

Pipeline (async.tcp.js:readFromClient):
  conn.cork()
  while leftover has complete RedisCmd:
    evalAndRespond(cmd, fdComm.client)  // queues or executes
  conn.uncork()  // single TCP flush with all replies
```

## 4. What executed

- `eval.js` (~520 lines): 15 handlers + `executeCommand` / `executeCmdToBuffer` / `evalAndRespond` / `respondError`
- Transactions: `evalMULTI` (begin), `evalEXEC`/`evalMulti` (array), `evalDISCARD`, `queueTxnCommand`, `resetTxn`, `TX_QUEUE_ARITY`
- `resp-encoder.js`: `EncodeSimpleString`, `EncodeBulkString`, `EncodeError`, `EncodeInteger`, `EncodeArray`
- `store.js`: `put`, `get`, `del`, `setExpire`, `newObj`, `deduceTypeEncoding`, `evictIfNeeded`, `configureEviction({policy})`, `getInfo()`
- `async.tcp.js`: `connectedClients` Map + `initClient()` per-connection txn state (`cqueue`, `isTxn`, `txnDirty`, `executing`, `write`)
- AOF hooks: `appendCommand` called on SET/DEL/EXPIRE/PEXPIRE/INCR success (inside EXEC too, per command)
- Background verbs: `bgEvalRDB` / `bgEvalAOF` (async, non-blocking, guard flags)
- Tests: 13 suites, 418 tests covering commands, transactions, errors, pipeline, persistence, eviction, shutdown/load

## 5. Final tested (cmds + result)

```bash
# Core commands
node test-get-set-ttl.js    # 34 passed: SET/GET/TTL + EX/PX + dispatch + wire
node test-del-expire.js     # 37 passed: DEL count, EXPIRE/PEXPIRE 0/neg→DEL, cron
node test-incr.js           # 21 passed: missing→1, 41→42→43, -1→0, non-int/overflow/TTL/WRONGTYPE/AOF replay

# Protocol + pipeline
node test-ping.js           # 20 passed: PING/PING msg/errors + pipelined
node test-resp-decoder.js   # 35 passed: all RESP2 types + errors + streaming
node test-pipeline.js       # 21 passed: 4-in-1, error mid-batch, unknown mid, split-packet, stats depth

# Data model
node test-diceobj.js        # 42 passed: typeEncoding pack/unpack, auto INT/EMBSTR/RAW, RDB2/RDB1

# Persistence
node test-aof-rdb.js        # 32 passed: AOF log/replay/rewrite, RDB save/load, TCP restart recovery

# Eviction
node test-evict.js          # 22 passed: LRU + random eviction, env configs, pool behavior, policy switching

# Transactions
node test-multi.js          # 79 passed: MULTI/EXEC/DISCARD, QUEUED, EXECABORT, runtime errors, isolation, wire
node test-load-multi.js     # 37 passed: prod load — 108.5k cmds, 270k cmds/s, byte-exact txn replies

# Shutdown + load
node test-graceful-shutdown.js  # 17 passed: drain, reject, AOF flush, RDB save, signals, idempotent
node test-load-graceful.js      # 21 passed: prod load + shutdown under load (A/B/C)

# All
# 418 passed, 0 failed (13 suites)
```

## 6. Final conclusion

- Full Redis-compatible command subset implemented with correct wire semantics
- Pipeline = transport batching (no atomicity); error mid-batch doesn't rollback/stop
- MULTI/EXEC = per-client queueing with `+QUEUED`, `*N` array replies, DISCARD, EXECABORT
- Transactions are not ACID: no rollback/isolation; queued cmds run back-to-back in one turn
- AOF logs effective ops: `EXPIRE k 0` → `DEL k`, TTL → `PX remaining-ms` at rewrite
- `INFO` provides observability (server env, memory, keyspace, eviction policy)
- Known limits: no `SCAN`, no `KEYS`, no `FLUSHALL` (use `clearStore()`), no WATCH, strings only (no LIST/HASH/SET/ZSET)

---

## Appendix: Runs log (append-only)

### Run — 2026-10-19 16:45:00 UTC

- Added `INFO` command with 5 sections (server, stats, memory, keyspace, eviction)
- All 11 commands tested across pipeline, persistence, eviction scenarios
- 260/260 tests pass, zero regressions
- Wire-compatible with `redis-cli` and `redis-benchmark` for implemented verbs

### Run — 2026-10-20 (transactions)

- Added `MULTI` / `EXEC` / `DISCARD` with per-client state (`connectedClients` registry in `async.tcp.js`)
- `evalAndRespond` now queues data commands as `+QUEUED` while a transaction is open; `EXEC` returns `*N` array; queue-time errors → `EXECABORT`
- 418/418 tests pass across 13 suites (incl. 79 transaction tests + 37 prod load tests), zero regressions
- Prod load: 108.5k transaction cmds, 270k cmds/s peak round 468k, all byte-exact, keyspace bounded