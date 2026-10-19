# Commands Implemented — diceDB (`eval.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Redis-compatible command set covering core KV operations + admin
- 11 commands: `PING`, `SET`, `GET`, `TTL`, `DEL`, `EXPIRE`, `PEXPIRE`, `INCR`, `BGSAVE`, `BGREWRITEAOF`, `SAVE`, `INFO`
- RESP2 wire protocol: each `eval*()` returns Buffer, `evalAndRespond` dispatches
- Pipeline support: cork/uncork in `async.tcp.js` coalesces N replies into single write
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

## 3. What we arch (design)

```text
evalAndRespond(cmd, conn):
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
    default → evalDefault (-ERR unknown command)

Each eval*():
  1. Validate arity + arg types → EncodeError on failure
  2. Execute via store.js (put/get/del/setExpire/DeleteExpiredKeys)
  3. On success: appendCommand({Cmd, Args}) for write commands
  4. conn.write(Encode*) — single write per command
  5. Return Buffer (for test assertions)

Pipeline (async.tcp.js:readFromClient):
  conn.cork()
  while leftover has complete RedisCmd:
    evalAndRespond(cmd, conn)  // writes to corked socket
  conn.uncork()  // single TCP flush with all replies
```

## 4. What executed

- `eval.js` (~400 lines): 12 handlers + `evalAndRespond` + `respondError`
- `resp-encoder.js`: `EncodeSimpleString`, `EncodeBulkString`, `EncodeError`, `EncodeInteger`
- `store.js`: `put`, `get`, `del`, `setExpire`, `newObj`, `deduceTypeEncoding`, `evictIfNeeded`
- AOF hooks: `appendCommand` called on SET/DEL/EXPIRE/PEXPIRE/INCR success
- Background verbs: `bgEvalRDB` / `bgEvalAOF` (async, non-blocking, guard flags)
- Tests: 9 suites, 260 tests covering all commands, errors, pipeline, persistence, eviction

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
node test-evict.js          # 18 passed: random eviction, env configs, evictAllRandomKeys

# All
# 260 passed, 0 failed
```

## 6. Final conclusion

- Full Redis-compatible command subset implemented with correct wire semantics
- Pipeline = transport batching (no MULTI/EXEC, no atomicity) — error mid-batch doesn't rollback/stop
- AOF logs effective ops: `EXPIRE k 0` → `DEL k`, TTL → `PX remaining-ms` at rewrite
- `INFO` provides observability (server env, memory, keyspace, eviction policy)
- Known limits: no `MULTI/EXEC`, no `SCAN`, no `KEYS`, no `FLUSHALL` (use `clearStore()`), strings only (no LIST/HASH/SET/ZSET)

---

## Appendix: Runs log (append-only)

### Run — 2026-10-19 16:45:00 UTC

- Added `INFO` command with 5 sections (server, stats, memory, keyspace, eviction)
- All 11 commands tested across pipeline, persistence, eviction scenarios
- 260/260 tests pass, zero regressions
- Wire-compatible with `redis-cli` and `redis-benchmark` for implemented verbs