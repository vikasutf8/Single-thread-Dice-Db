# Redis Pipeline (`async.tcp.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- One request, one round trip: client sends **N commands in a single TCP
  write**, server executes them **one by one in order** and returns **all N
  replies back-to-back**, so the client reads everything in one response.
- Input/output shape: `in = cmd1 + cmd2 + … + cmdN` (concatenated RESP
  frames) → `out = reply1 + reply2 + … + replyN` (concatenated RESP replies).
- Pipeline **!=** transaction:
  - No `MULTI` / `EXEC` / queueing, no atomicity, no rollback.
  - Cmd #2 failing does NOT undo cmd #1 and does NOT stop cmd #3.
  - Each failure is just one `-ERR` reply sitting in its position in the batch.
- No new server verb: real Redis has no `PIPELINE` command either — batching
  is purely a transport shape the client chooses. The server's only job is to
  drain every complete frame already buffered and flush all replies together.

## 2. What we plan

- Keep RESP / command logic untouched (`redis-cmd.js:readCmd`,
  `eval.js:evalAndRespond`, `store.js`) — pipeline is a transport behavior,
  not a new command.
- Reuse the existing inner drain loop in `async.tcp.js:readFromClient`
  (`while (leftover) { readCmd → evalAndRespond → slice(n) }`), which already
  executes N buffered cmds sequentially and breaks correctly on `incomplete` /
  `EXIT` / disconnect.
- Add the two things that turn "N sequential replies" into "1 round trip":
  1. `conn.cork()` before the drain + `conn.uncork()` in `finally` after it,
     so N `conn.write()` calls coalesce into a single TCP flush
     ("return all in once").
  2. Pipeline observability in `stats()`: `commands` (total executed),
     `batches` (EPOLLIN hits carrying ≥1 cmd), `maxBatchDepth` (largest
     cmds-per-hit seen — the pipeline-depth proof).
- Cover with `test-pipeline.js` (single-write batches, error-mid-batch,
  split-packet batch, stats depth, mixed types) + full regression suites +
  concurrent load test appended to `load_test.md`.
- Do NOT touch `main.js` (sync server already pipelines via the same drain
  shape, minus `cork`; async server is the 10k path under test).

## 3. What we arch (design)

```text
client: ONE write (1 round trip)
  batch = RESP(cmd1) + RESP(cmd2) + ... + RESP(cmdN)
  |
  v
server EPOLLIN on client fd (conn.on("data", chunk))
  fdComm.leftover += chunk
  |
  v
readFromClient(fdComm):                      // per-hit pipeline executor
  conn.cork()                                // buffer kernel writes
  try {
    while (leftover.length > 0):             // in-order, one by one
      { cmd, n, err } = readCmd(leftover)
        err=incomplete?  break (wait for next EPOLLIN; partial batch kept)
        err=empty line?  consume, continue
        err=protocol?    respondError -> ONE -ERR reply, drop bytes, CONTINUE
        verb=EXIT/QUIT?  conn.end(), break
        else             evalAndRespond(cmd, conn)  // executes NOW, no queue
                         totalCommands++, batchDepth++
                         leftover = leftover.slice(n)
    totalBatches++ if batchDepth > 0
    maxBatchDepth = max(maxBatchDepth, batchDepth)
    leftover > 1MiB? respondError + destroy (OOM guard, unchanged)
  } finally {
    conn.uncork()                            // ONE flush: N replies together
  }
  |
  v
wire: reply1 + reply2 + ... + replyN         // client Decode()s N times via n
```

Pipeline vs transaction, side by side:

| Pipeline (this file) | Transaction (`MULTI`/`EXEC`, NOT built) |
|---|---|
| executes each cmd immediately, in order | queues cmds, executes on `EXEC` |
| error → one `-ERR` reply, rest still run | error → abort / `EXECABORT` semantics |
| no atomicity, no isolation | atomic (all-or-nothing execution) |
| no server verb; client-side batching | server verbs `MULTI`, `EXEC`, `DISCARD` |

## 4. What executed

- `async.tcp.js` only (no codec / eval / store changes):
  - `readFromClient` wrapped in `cork()` / `uncork()` (`finally`, so EXIT and
    OOM-destroy paths still flush) + comment block stating pipeline semantics.
  - Protocol-error path annotated: `-ERR` is one reply in the batch, loop
    continues (this is the pipeline!=transaction guarantee).
  - New module counters: `totalCommands`, `totalBatches`, `maxBatchDepth`;
    exposed via `stats()` as `commands` / `batches` / `maxBatchDepth`.
  - Fake `conn` objects in unit tests lack `cork` — guarded with
    `typeof conn.cork === "function"` so they keep passing.
- New `test-pipeline.js` (21 asserts, §5).
- This `pipeline.md` doc + load-test entry appended to `load_test.md`.

## 5. Final tested (cmds + result)

```bash
# 1. syntax + pipeline suite
node --check async.tcp.js && node test-pipeline.js
# 21 passed, 0 failed:
#   4-in-1 (PING+SET+GET+PING, ONE write) -> +PONG +OK $3 pv1 $5 hello, in order
#   not-txn: SET OK, PING-a-b -ERR, GET tv (error neither rolls back nor stops)
#   unknown FOO mid-batch -> single -ERR, surrounding PONGs intact
#   split-packet batch (half now, half 50ms later) -> +OK $2 sv :-1
#   stats: 10×PING in one write -> commands +10, maxBatchDepth 10
#   mixed SET-EX-60 + EXPIRE + TTL + GET -> +OK :1 :50 $2 mv

# 2. regression (no changes to command behavior)
node test-ping.js          # 20 passed, 0 failed
node test-resp-decoder.js  # 35 passed, 0 failed
node test-get-set-ttl.js   # 34 passed, 0 failed
node test-del-expire.js    # 37 passed, 0 failed
node test-evict.js         # 13 passed, 0 failed
# total: 139/139 regression + 21/21 pipeline = 160/160 green

# 3. concurrent pipeline load (see load_test.md run 2026-10-15)
node load-pipeline.js  # 50 conns x 20-cmd batches = 1000 cmds, all replies OK
```

Result: **21/21 pipeline, 139/139 regression, 50/50 concurrent pipelined
clients with zero fd leak** (`active` back to 0, `monitored` 0).

## 6. Final conclusion

- Pipeline is done and it is deliberately boring: no new command, no queue —
  the per-hit `readCmd` drain loop already had the right shape, and `cork` /
  `uncork` makes the "one request → one flush" guarantee explicit at the TCP
  layer (`maxBatchDepth` in `stats()` proves batches arrive and execute as one).
- Pipeline != transaction is enforced by construction: `evalAndRespond` runs
  each cmd the moment it is parsed, and the error path writes one `-ERR` and
  continues — there is no buffer to roll back and no `EXEC` to abort.
- Limits carried over unchanged: 1 MiB per-fd leftover cap, 1 MiB
  backpressure `pause()`/`drain`, `maxClients` admission control — a hostile
  100 MB "pipeline" still gets destroyed, not buffered.
- Next (not today): `MULTI`/`EXEC` transaction support would be a separate
  per-client queue in `FdComm` + new `eval` verbs — explicitly out of scope.
