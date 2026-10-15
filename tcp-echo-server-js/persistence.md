# Persistence — AOF + RDB (`aof.js` / `rdb.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Survive restarts: two mechanisms, same as real Redis.
  - **AOF** (`./AOF/dice-master.aof`): append every successful write command
    as its RESP frame. Restart = replay the frames in order.
  - **RDB** (`./RDB/dump.rdb`): point-in-time binary snapshot of the whole
    `store` Map. Restart = load snapshot, then replay the AOF tail on top.
- Background verbs that never stall the EPOLLIN loop:
  - `bgEvalRDB` ← `BGSAVE` (snapshot now, reply `+Background saving started`).
  - `bgEvalAOF` ← `BGREWRITEAOF` (compact the log, reply immediately).
  - `SAVE` ← blocking snapshot for terminal use.
- Crash-safe files: write tmp + atomic `rename`, never overwrite in place.
- Opt-in: all existing tests stay file-free (no AOF/RDB unless the caller
  passes `aofEnabled` / `rdbLoad`).

## 2. What we plan

- New `aof.js`: `encodeCommand` (Cmd+Args → RESP frame), `appendCommand`
  (sync O(1) append, never throws into the client path), `dumpAllAOF`
  (compact full dump: live keys only, TTL → `SET … PX <remain-ms>`),
  `bgEvalAOF` (async rewrite + rewrite-tail buffer for mid-rewrite writes),
  `replayAOF` (torn tail ignored, corruption stops replay).
- New `rdb.js`: custom `RDB1` binary format
  (`magic + u32 count + [u32 klen, key, u32 vlen, val, i64 expireAt]`),
  `dumpRDBSync` (`SAVE`), `bgEvalRDB` (`BGSAVE`, guard flag), `loadRDB`
  (bad magic throws, expired keys skipped, missing file = clean first boot).
- Wire `eval.js`: log the *effective* op on success only —
  `SET`→verbatim, `DEL`→verbatim, `EXPIRE/PEXPIRE`→verbatim on `:1`,
  `EXPIRE k 0`→`DEL k` (state-convergent replay), missing-key `:0`→no log,
  reads never log. New switch cases `BGSAVE` / `BGREWRITEAOF` / `SAVE`.
- Wire `async.tcp.js:RunAsyncTcpServer(host, port, { aofEnabled, aofPath,
  rdbLoad, rdbPath })`: RDB load → AOF replay → enable append, all before
  `listen`. Replay uses a null conn so nothing is re-logged or answered.
- `store.js`: add `clearStore()` (map + byte accounting reset) for restart
  simulation. `main.js` untouched (async server is the persistence path).

## 3. What we arch (design)

```text
write path (per command, inside evalAndRespond):
  evalSET/DEL/EXPIRE/PEXPIRE success -> appendCommand({Cmd, Args})
    -> fs.appendFileSync(AOF/dice-master.aof, RESP frame)
    -> if rewriting: also push frame to rewriteTail

bgEvalAOF (BGREWRITEAOF):
  rewriting=true, tail=[]
  buildAOFFrames()            // live store -> SET [+PX] frames, ms-scale
  await writeFile(tmp, buf)   // async: event loop keeps serving
  renameSync(tmp, AOF)        // atomic switch
  appendFileSync(AOF, tail)   // commands that arrived mid-rewrite
  rewriting=false

bgEvalRDB (BGSAVE):
  saving=true; snapshot=buildSnapshot()   // skips expired
  await writeFile(tmp, snapshot); renameSync(tmp, RDB); saving=false
  overlap -> {ok:false} -> client gets -ERR already in progress

startup (RunAsyncTcpServer, before listen):
  rdbLoad? loadRDB(RDB) -> put() live keys only
  aofEnabled? replayAOF(AOF, cmd => evalAndRespond(cmd, NOREPLY)) -> setEnabled(true)
  torn tail (crash mid-append) -> incomplete last frame ignored, prefix replays
```

AOF vs RDB, side by side:

| AOF (`dice-master.aof`) | RDB (`dump.rdb`) |
|---|---|
| RESP text, one frame per write | custom binary snapshot |
| durable per command, bigger file | compact, fast load, loses post-snapshot writes |
| replay converges (effective-op logging) | load skips keys that died while down |
| `BGREWRITEAOF` compacts via `dumpAllAOF` | `BGSAVE`/`SAVE` re-snapshots |

## 4. What executed

- New `aof.js` (~200 lines), `rdb.js` (~150 lines), `test-aof-rdb.js` (32 asserts).
- `eval.js`: 3 append hooks + `evalBGSAVE` / `evalBGREWRITEAOF` / `evalSAVE`
  + 3 switch cases (reads and failed writes log nothing).
- `async.tcp.js`: persistence opts + RDB-then-AOF load before `listen`,
  `persist` block in `stats()` (paths, flags, `lastSave`).
- `store.js`: `clearStore()` export. `.gitignore`: `AOF/ RDB/ *.aof *.rdb`.
- Did NOT change RESP codec, pipeline drain, expiry, or eviction logic.

## 5. Final tested (cmds + result)

```bash
node test-aof-rdb.js
# 32 passed, 0 failed:
#   encode round-trip; SET logged verbatim, bad-SET + GET not logged
#   replay 5 cmds (SET/SET/DEL/SET/EXPIRE-0) -> r1 kept, r2/r3 gone
#   torn tail: prefix replays, tail ignored
#   dumpAllAOF: 2/3 keys (dead skipped), TTL preserved as PX (~60s)
#   bgEvalAOF: mid-rewrite SET survives (tail=1), both keys replay
#   RDB: magic RDB1, load 2/2, expired skipped, bad magic throws, no-file ok
#   BGSAVE/BGREWRITEAOF/SAVE replies + files land on disk
#   TCP restart x2: SET -> kill (clearStore) -> reboot -> GET recovers (AOF + RDB)

node test-ping.js && node test-resp-decoder.js && node test-get-set-ttl.js \
  && node test-del-expire.js && node test-evict.js && node test-pipeline.js
# 20 + 35 + 34 + 37 + 13 + 21 = 160/160, no regressions

# load with AOF on (see load_test.md run 2026-10-15): 200 conns x 10 cmds
# -> 200/200, ~6.9k cmds/s vs ~8.2k without AOF (appendFileSync cost per write)
```

Result: **32/32 persistence, 160/160 regression, restart recovery proven
over real TCP for both engines, zero stray files in the repo.**

## 6. Final conclusion

- Persistence is done in the Redis shape: AOF for durability per command,
  RDB for compact snapshots, both crash-safe via tmp+rename, both
  non-blocking via `bgEval*` + guard flags (Node has no `fork()`, so "bg"
  means async I/O + immediate reply — the event loop never stalls).
- Effective-op logging is the subtle correctness point: `EXPIRE k 0` is
  stored as `DEL k`, and TTLs are re-emitted as `PX <remaining>` at rewrite —
  so replay converges to the same state instead of re-running stale timers.
- Known limits (not today): no `fsync` policy knob (append relies on OS
  flush; `SAVE` is the durable point), no partial resync / replication, RDB
  has no checksum (trailing-byte check only), `main.js` has no persistence.
