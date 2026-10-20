# Load Test Log (`load_test.md`)

> APPEND-ONLY FILE — never edit or delete past entries.
> Every test run appends a new dated entry at the bottom.

## Prerequisites

- `redis-benchmark` installed (ships with Redis).
- Server running on `localhost:8080` — either:
  - `node main.js --host 127.0.0.1 --port 8080`, or
  - `node -e 'require("./async.tcp.js").RunAsyncTcpServer("127.0.0.1", 8080)'`

## Test command

```bash
redis-benchmark -n 10000 -t ping_mbulk -c 200 -h localhost -p 8080
```

- `-n 10000` → total requests
- `-t ping_mbulk` → PING test (bulk pipeline mode)
- `-c 200` → 200 parallel clients
- `-h / -p` → target host/port

## How to append a result (do this every run)

Run the benchmark and append its output with a date stamp — single command:

```bash
{
  echo ""
  echo "## Run — $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo ""
  echo '```'
  redis-benchmark -n 10000 -t ping_mbulk -c 200 -h localhost -p 8080
  echo '```'
} >> load_test.md
```

Verify it appended:

```bash
tail -20 load_test.md
```

## Results log (append below this line)

<!-- New runs go below. Do not edit entries above. -->

## Run — 2026-10-14 16:06:49 UTC

- Server: async (`RunAsyncTcpServer("127.0.0.1", 8081)`), 1 server × 3 concurrent scripted clients
- Client scripts (RESP over TCP):
  - client1: `PING`, `SET name vikas`, `GET name`
  - client2: `SET tmp val EX 60`, `TTL tmp`, `GET tmp`
  - client3: `GET nokey`, `TTL nokey`, `PING hello`

```text
client1: "+PONG\r\n+OK\r\n$5\r\nvikas\r\n"
client2: "+OK\r\n:60\r\n$3\r\nval\r\n"
client3: "$-1\r\n:-2\r\n$5\r\nhello\r\n"
server stats: {"active":0,"accepted":3,"rejected":0,"monitored":0}
```

- Result: PASS — all replies correct, `active` back to 0, `monitored_fds` 0 (no fd leak).
- Note: an early client2 run failed with `-ERR syntax error` because the
  test script sent `*4` for 5-element `SET tmp val EX 60`; fixed to `*5`, server behaved correctly throughout.

## Run — 2026-10-14 16:27:30 UTC

- Change under test: `DEL` / `EXPIRE` / `PEXPIRE` + active-expiry cron in async server (`cronFrequency` 1 s, `lastCronExecution` stamped per tick)
- Unit/integration suites (no `redis-benchmark` installed on this box):

```text
test-del-expire.js:    37 passed, 0 failed  (DEL counts, EXPIRE/PEXPIRE incl. 0/negative→delete+:1,
                                             DeleteExpiredKeys frac loop, live cron proof:
                                             PX 50 key vanished with zero reads, "[cron] expired 1 keys")
test-get-set-ttl.js:   34 passed, 0 failed
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
```

- Result: PASS — 126/126 across all suites, no regressions.

## Run — 2026-10-14 16:36:08 UTC

- Change under test: simple FIRST eviction (thresholds 20k keys / 10 MB; breach → drop oldest-inserted 10%). 10 MB path coded, not unit-tested.
- Unit/integration suites:

```text
test-evict.js:         13 passed, 0 failed  (byte accounting, maxKeys=10: 11th SET evicts k1,k2,
                                              newest kept, wire GET evicted -> $-1)
test-del-expire.js:    37 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
```

- Terminal-path check (`RunAsyncTcpServer("127.0.0.1",18083,{maxKeys:10})`, 11 SETs over TCP):
  - server log: `[evict] reason=keys removed=2 size=9 bytes=616`
  - client: 11× `+OK`, `GET k1` → `$-1`, `GET k11` → `$3 v11`
- Result: PASS — 139/139 across all suites, no regressions.

## Run — 2026-10-15 15:09:40 UTC

- Change under test: Redis pipeline — 1 round trip, N cmds, N replies
  (`async.tcp.js:readFromClient` wrapped in `conn.cork()`/`uncork()` so one
  EPOLLIN hit flushes all replies together; pipeline != transaction, no
  atomicity — see `pipeline.md`). No codec/eval/store changes.
- Basic + regression suites:

```text
test-pipeline.js:      21 passed, 0 failed  (4-in-1 PING+SET+GET+PING one write;
                                             error mid-batch neither rolls back
                                             nor stops; unknown FOO mid-batch;
                                             split-packet batch; stats depth 10;
                                             mixed SET-EX+EXPIRE+TTL+GET)
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-del-expire.js:    37 passed, 0 failed
test-evict.js:         13 passed, 0 failed
```

- Load test (no `redis-benchmark` on this box; scripted RESP-over-TCP
  equivalent — single-write pipelined batches, replies verified in order):

```text
Round A (mixed): 50 conns x 20-cmd batches (PING/SET alternating, ONE write
  each) + 1 x 200-PING mega-pipeline
  -> okConns=50/50 okReplies=1000 bad=0; MEGA pong=200/200
  -> TOTAL 1200 cmds in 218ms = 5505 cmds/s
  -> stats {"active":0,"accepted":51,"rejected":0,"monitored":0,
            "commands":1200,"batches":51,"maxBatchDepth":200}
Round B (burst): 200 conns x 10-PING batches, ONE write each
  -> ok=200/200 bad=0, 2000 cmds in 244ms = 8197 cmds/s
  -> stats {"active":0,"accepted":200,"rejected":0,"monitored":0,
            "commands":2000,"batches":200,"maxBatchDepth":10}
```

- Result: PASS — 160/160 (139 regression + 21 pipeline), 3200 pipelined
  cmds over 251 conns with zero failures, `active` back to 0, `monitored` 0
  (no fd leak). `maxBatchDepth=200` proves single-hit batch execution.

## Run — 2026-10-15 15:40:00 UTC

- Change under test: persistence — AOF (`aof.js`, `./AOF/dice-master.aof`)
  + RDB (`rdb.js`, `./RDB/dump.rdb`), background verbs `BGSAVE`/`BGREWRITEAOF`
  (`bgEvalRDB`/`bgEvalAOF`) + blocking `SAVE`. Effective-op AOF logging
  (see `persistence.md`). Opt-in: no files unless `aofEnabled`/`rdbLoad`.
- Basic + regression suites:

```text
test-aof-rdb.js:       32 passed, 0 failed  (encode round-trip; SET logged,
                                             bad-SET+GET not logged; replay 5
                                             cmds incl. EXPIRE-0-as-DEL; torn
                                             tail ignored; dumpAllAOF skips
                                             dead + keeps TTL as PX; bg rewrite
                                             keeps mid-rewrite tail write; RDB
                                             round-trip + expired-skip + bad
                                             magic throws; BGSAVE/BGREWRITEAOF
                                             /SAVE dispatch; TCP restart
                                             recovery via AOF and via RDB)
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-del-expire.js:    37 passed, 0 failed
test-evict.js:         13 passed, 0 failed
test-pipeline.js:      21 passed, 0 failed
```

- Load test with AOF enabled (scripted RESP-over-TCP, AOF on tmp path so the
  repo stays clean; 200 conns x 10-cmd PING/SET batches, ONE write each):

```text
AOF-ON conns=200 depth=10 ok=200/200 bad=0, 2000 cmds in 293ms = 6826 cmds/s
AOF-Frames-Appended=1000 (1000 SETs; 1000 PINGs read-only, correctly unlogged)
stats {"active":0,"accepted":200,"rejected":0,"monitored":0,
       "commands":2000,"batches":200,"maxBatchDepth":10}
```

- Baseline without AOF (previous run): 2000 cmds in 244ms = 8197 cmds/s.
  AOF append (`appendFileSync` per write) costs ~17% throughput here.
- Result: PASS — 192/192 (160 regression + 32 persistence), restart
  recovery proven over real TCP for both engines, `active` back to 0,
  `monitored` 0 (no fd leak), repo has zero stray `*.aof`/`*.rdb` files.

## Run — 2026-10-15 16:10:00 UTC

- Change under test: `diceObj` — store values grow `{value, expireAt}` →
  `{value, expireAt, typeEncoding}` (redisObject layout: type high nibble,
  encoding low nibble; `OBJ_TYPE_STRING`, `RAW`/`INT`/`EMBSTR` + `getType` /
  `getEncoding` / `assertType` / `assertEncoding` / `autoEncoding` /
  `newObjAbs`). RDB bumped to `RDB2` (one `u8 typeEncoding` per entry,
  legacy RDB1 still loads). `GET`/`TTL` answer `-WRONGTYPE` on type
  mismatch (see `diceobj.md`).
- Basic + regression suites:

```text
test-diceobj.js:       38 passed, 0 failed  (pack/unpack nibbles + masking;
                                             legacy defaults; auto INT/EMBSTR/
                                             RAW incl. 44B edge; stamping;
                                             asserts incl. WRONGTYPE verbatim;
                                             SET stamps INT, wrong-type GET/
                                             TTL -> -WRONGTYPE, conn survives;
                                             RDB2 byte-exact encodings;
                                             hand-built RDB1 loads)
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-del-expire.js:    37 passed, 0 failed
test-evict.js:         13 passed, 0 failed
test-pipeline.js:      21 passed, 0 failed
test-aof-rdb.js:       32 passed, 0 failed
```

- Load test (no persistence enabled; 200 conns x 10-cmd PING/SET batches,
  ONE write each — encoding stamp is on the SET path):

```text
BURST conns=200 depth=10 ok=200/200 bad=0, 2000 cmds in 247ms = 8097 cmds/s
stats {"active":0,"accepted":200,"rejected":0,"monitored":0,
       "commands":2000,"batches":200,"maxBatchDepth":10}
```

- Baseline before diceObj: 8197 cmds/s. No perf regression (one byte pack
  per SET is noise).
- Result: PASS — 230/230 (192 regression + 38 diceobj), `active` back to 0,
  `monitored` 0 (no fd leak).

## Run — 2026-10-15 16:30:00 UTC

- Change under test: `evalINCR` (`INCR key` → `:n`) + `deduceTypeEncoding`
  (tryObjectEncoding port) used by `SET`/`INCR` before storing. Missing/
  expired → `:1`; non-int/unsafe-int → not-an-integer; `MAX_SAFE_INTEGER`
  → overflow error; TTL preserved; encoding re-deduced on every write;
  AOF logs `INCR` verbatim (see `redis_cmd_impl.md` §13).
- Basic + regression suites:

```text
test-incr.js:          21 passed, 0 failed  (missing -> :1 + INT stamp;
                                            41 -> :42 -> :43; -1 -> :0;
                                            non-int/unsafe/overflow/arity;
                                            expired -> :1; TTL preserved;
                                            WRONGTYPE; lowercase; AOF replay
                                            SET 5 + INCR x2 -> "7")
test-diceobj.js:       42 passed, 0 failed  (+ deduceTypeEncoding trio,
                                            + SET type-stamp check)
test-ping.js:          20 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-del-expire.js:    37 passed, 0 failed
test-evict.js:         13 passed, 0 failed
test-pipeline.js:      21 passed, 0 failed
test-aof-rdb.js:       32 passed, 0 failed
```

- Load test (100 conns x pipeline of 1 SET-0 + 20 INCR, ONE write each;
  final reply per conn must be exactly `:20`):

```text
INCR-BURST conns=100 incr-per-conn=20 ok=100/100 bad=0, 2100 cmds in 234ms = 8974 cmds/s
stats {"active":0,"accepted":100,"monitored":0,"commands":2100,"maxBatchDepth":21}
```

- Result: PASS — 255/255 (234 regression + 21 incr), read-modify-write
  batches exact under concurrency, `active` back to 0, `monitored` 0.

## Run — 2026-10-19 17:23:40 UTC

- Change under test: **graceful shutdown** of the async server —
  `gracefulShutdown()` / `setupGracefulShutdown()` in `async.tcp.js`: stop
  accepting, drain accepted clients up to `timeoutMs`, force-close stragglers,
  fsync AOF, optional RDB snapshot; plus `opts.policy` wiring at startup
  (see `graceful-shutdown.md`). **Prod env**: maxKeys=200000, evictPercent=4%,
  allkeys-lru (24-bit LRU clock + 16-entry eviction pool).
- Basic + regression suites:

```text
test-graceful-shutdown.js: 17 passed, 0 failed  (basic active->0; drain active
                                            conns; reject during shutdown;
                                            AOF fsync; RDB save+reload; signal
                                            setup; idempotent double-call;
                                            timeout force-close)
test-load-graceful.js:     21 passed, 0 failed  (prod rounds A/B/C below)
test-evict.js:         22 passed, 0 failed  (LRU + random, pool, policy switch)
test-aof-rdb.js:       32 passed, 0 failed
test-diceobj.js:       42 passed, 0 failed
test-del-expire.js:    37 passed, 0 failed
test-get-set-ttl.js:   34 passed, 0 failed
test-incr.js:          21 passed, 0 failed
test-ping.js:          20 passed, 0 failed
test-pipeline.js:      21 passed, 0 failed
test-resp-decoder.js:  35 passed, 0 failed
```

- Load test at prod env (scripted RESP-over-TCP, one write per pipelined batch):

```text
Round A (prod baseline): 200 conns x 10 batches x 10 cmds (PING/SET) = 20k cmds
  -> ok=200/200 byte-exact, 0 timeouts, 96ms = 208k cmds/s
  -> keys=10000 (10k unique), evicted=0, pool=16 (below prod limit 200k)
Round B (prod eviction): 100 conns x 25 batches x 100 unique SETs = 250k cmds
  vs maxKeys=200000
  -> ok=100/100 byte-exact, 0 timeouts, 878ms = 285k cmds/s
  -> keys bounded at 200000, evicted=50000 over 3125 evict events, pool<=16
Round C (shutdown WHILE 100 clients mid-flight): AOF + RDB on, shutdown after 30ms
  -> 3 accepted conns drained fully (600/600 SETs applied, byte-exact)
  -> 97 conns still in OS accept backlog refused (listen socket closed) — expected
  -> shutdown=113ms (limit 3000ms); active=0; monitored=0 (no fd leak)
  -> AOF=20700 bytes (fsync'd); RDB reload 600/600 == keys-at-save (zero loss)
```

- Result: PASS — 302/302 across 11 suites (264 regression + 17 shutdown + 21 load);
  290k cmds executed under prod config with zero mismatches; graceful shutdown
  finishes in 113ms under live load with AOF fsync + RDB snapshot, zero data
  loss for accepted commands, `active`/`monitored` back to 0.

## Run — 2026-10-20 06:59:02 UTC

- Change under test: **transactions** — `MULTI` / `EXEC` / `DISCARD` in
  `eval.js` (queue phase → `+QUEUED`, EXEC → `*N` array, queue-time errors →
  `EXECABORT`, DISCARD drops queue) + per-client state in `async.tcp.js`
  (`connectedClients` Map, `initClient()` → `{cqueue, isTxn, txnDirty,
  executing, write}`) + `EncodeArray` in `resp-encoder.js`
  (see `transactions.md`). **Prod env**: maxKeys=200000, evictPercent=4%,
  allkeys-lru.
- Basic + regression suites:

```text
test-multi.js:             79 passed, 0 failed  (MULTI/EXEC/DISCARD direct +
                                            wire; +QUEUED before EXEC; queue
                                            not applied early; *N reply order;
                                            empty txn *0; nested MULTI keeps
                                            txn; unknown/bad-arity → EXECABORT
                                            applies nothing; runtime error stays
                                            an array element; DISCARD untouched
                                            keyspace; 2-socket isolation;
                                            connectedClients registry drain)
test-load-multi.js:        37 passed, 0 failed  (prod rounds A/B/C/D/E below)
test-graceful-shutdown.js: 17 passed, 0 failed
test-load-graceful.js:     21 passed, 0 failed
test-evict.js:             22 passed, 0 failed
test-aof-rdb.js:           32 passed, 0 failed
test-diceobj.js:           42 passed, 0 failed
test-del-expire.js:        37 passed, 0 failed
test-get-set-ttl.js:       34 passed, 0 failed
test-incr.js:              21 passed, 0 failed
test-ping.js:              20 passed, 0 failed
test-pipeline.js:          21 passed, 0 failed
test-resp-decoder.js:      35 passed, 0 failed
```

- Transaction load test at prod env (scripted RESP-over-TCP, one write per
  transaction batch; every reply byte-compared, incl. all `+QUEUED` and the
  EXEC `*N` array):

```text
Round A (MULTI + 10 SETs + EXEC): 100 conns x 20 batches = 24k cmds
  -> ok=100/100 byte-exact, 0 timeouts, 134ms = 179k cmds/s
  -> keys=20000 written by EXEC; evicted=0 (below prod limit); pool=16
Round B (deep queue: MULTI + 200 SETs + EXEC): 50 conns x 5 batches = 50.5k cmds
  -> ok=50/50 byte-exact, 0 timeouts, 179ms = 282k cmds/s
  -> keys=50000; evicted=0
Round C (MULTI + 20 SETs + DISCARD): 100 conns x 10 batches = 22k cmds
  -> ok=100/100 byte-exact, 0 timeouts, 47ms = 468k cmds/s
  -> keys=0 (all discarded, keyspace untouched)
Round D (MULTI + bad arity + EXECABORT): 50 conns x 5 batches = 3k cmds
  -> ok=50/50 byte-exact (-ERR then -EXECABORT), 10ms = 300k cmds/s
  -> keys=0 (aborted transaction wrote nothing)
Round E (mixed SET + MULTI + 5 SETs + EXEC + PING): 100 conns x 10 batches
  -> ok=100/100 byte-exact, 0 timeouts, 32ms = 281k cmds/s
  -> keys=6000 (1 direct + 5 txn per batch)
Total: 108.5k cmds in 402ms = 270k cmds/s, zero mismatches, zero timeouts
Drain: clients=0, monitored=0 (no fd/txn-state leak), commands counter=108500
```

```text
=== LOAD SUMMARY (prod env, transactions) ===
{
  "config": {
    "maxKeys": 200000,
    "evictPercent": 0.04,
    "policy": "allkeys-lru"
  },
  "rounds": {
    "A": {
      "shape": "MULTI+10SET+EXEC",
      "ok": 100,
      "mismatch": 0,
      "timeouts": 0,
      "partial": 0,
      "cmds": 24000,
      "ms": 130,
      "kcmds": 185
    },
    "B": {
      "shape": "MULTI+200SET+EXEC",
      "ok": 50,
      "mismatch": 0,
      "timeouts": 0,
      "partial": 0,
      "cmds": 50500,
      "ms": 174,
      "kcmds": 290
    },
    "C": {
      "shape": "MULTI+20SET+DISCARD",
      "ok": 100,
      "mismatch": 0,
      "timeouts": 0,
      "partial": 0,
      "cmds": 22000,
      "ms": 45,
      "kcmds": 489
    },
    "D": {
      "shape": "MULTI+badArity+EXECABORT",
      "ok": 50,
      "mismatch": 0,
      "timeouts": 0,
      "partial": 0,
      "cmds": 3000,
      "ms": 10,
      "kcmds": 300
    },
    "E": {
      "shape": "SET+MULTI+5SET+EXEC+PING",
      "ok": 100,
      "mismatch": 0,
      "timeouts": 0,
      "partial": 0,
      "cmds": 9000,
      "ms": 31,
      "kcmds": 290
    }
  },
  "total": {
    "cmds": 108500,
    "ms": 390,
    "kcmds": 278
  },
  "keyspace": {
    "keys": 6000,
    "evicted": 0,
    "poolSize": 16,
    "maxKeys": 200000,
    "evictPercent": 0.04,
    "policy": "allkeys-lru"
  },
  "stats": {
    "clients": 0,
    "monitored": 0,
    "commands": 108500
  }
}

```

- Result: PASS — 418/418 across 13 suites (302 prior + 79 transaction + 37
  transaction load); queue/EXEC/DISCARD wire-exact under prod config,
  byte-exact `*N` arrays under 100 concurrent clients, DISCARD and EXECABORT
  confirmed non-writing, per-client isolation holds over real sockets,
  `active`/`monitored` back to 0 (transaction state freed with the connection).


