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
