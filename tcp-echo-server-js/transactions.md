# Transactions (MULTI / EXEC / DISCARD) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Redis **MULTI / EXEC / DISCARD** transaction support over the existing RESP2 server
- **Per-client stateful execution**: every connected client owns its own transaction
  state (`isTxn`, command queue `cqueue`, `txnDirty`, `executing`) — one client's
  MULTI must never affect another client
- **Queue phase**: after `MULTI`, every data command replies `+QUEUED` and is NOT
  executed until `EXEC`
- **EXEC** returns a RESP array of the individual command replies, in order
- **DISCARD** drops the queue and leaves the keyspace untouched
- Queue-time errors (unknown command / wrong arity) mark the transaction dirty;
  `EXEC` then returns `-EXECABORT Transaction discarded because of previous errors.`
- Runtime errors inside `EXEC` (e.g. WRONGTYPE) appear as error elements in the
  response array and do NOT abort the remaining commands (Redis semantics)
- Full backward compatibility: `evalAndRespond(cmd, plainConn)` without client
  state keeps executing immediately (tests, AOF replay, sync server)
- Transactions are **not atomic** in the ACID sense (no rollback, no isolation);
  MULTI/EXEC only guarantees the queued commands run back-to-back in one event
  loop turn (Redis semantics)

## 2. What we plan

- `resp-encoder.js`: add `EncodeArray(items)` — `*N\r\n` + concatenated items
- `eval.js`: full restructure
  - `respOk()` — `+OK` helper
  - `executeCommand(cmd, client)` — pure dispatch, returns reply **bytes** (Buffer),
    never writes; used by both `evalAndRespond` and `evalMULTI`
  - `executeCmdToBuffer(cmd, client)` — explicit alias for the bytes contract
  - `evalAndRespond(cmd, client)` — if `client.isTxn && !client.executing` and the
    verb is not MULTI/EXEC/DISCARD → validate, queue, write `+QUEUED`; otherwise
    `client.write(executeCommand(cmd, client))`
  - `evalMULTI(client)` / `evalEXEC(client)` — EXEC array response
  - `evalDISCARD(client)`
  - `TX_QUEUE_ARITY` table — arity validation at queue time
- `async.tcp.js`: per-client object + registry
  - `connectedClients: Map<fd, client>`
  - `initClient(fdComm)` — creates `{ fd, socket, addr, cqueue, isTxn, txnDirty,
    executing, write() }`, registers in the map (called on every new connection)
  - `readFromClient` passes the **client object** to `evalAndRespond`
  - `closeClient` deletes the client from `connectedClients`
  - `stats().clients` exposes the live client count
- Tests: `test-multi.js` — direct + wire-level + per-client isolation
- Load: `test-load-multi.js` — prod env, MULTI/EXEC pipelined batches
- Docs: this file + append a run to `load_test.md`

## 3. What we arch (design)

```text
client object (created on accept, stored in connectedClients Map):
  {
    fd,               // emulated fd (logging + map key)
    socket,           // io.ReadWriter (net.Socket)
    addr,
    cqueue: [],       // RedisCmd[] queued while isTxn
    isTxn: false,     // MULTI in progress
    txnDirty: false,  // queue-time error → EXECABORT
    executing: false, // inside EXEC (never re-queue)
    write(b)          // delegates to socket.write — eval handlers write here
  }

evalAndRespond(cmd, client):
  verb = cmd.Cmd.toUpperCase()
  if client.isTxn && !client.executing && verb ∉ {MULTI, EXEC, DISCARD}:
      if verb unknown            -> write -ERR unknown command; txnDirty = true
      else if arity invalid      -> write -ERR wrong number of arguments; txnDirty = true
      else                       -> cqueue.push(cmd); write +QUEUED
      return
  out = executeCommand(cmd, client)   // Buffer
  client.write(out)

executeCommand(cmd, client) -> Buffer:        // pure: no socket writes
  switch verb:
    PING/SET/GET/TTL/DEL/EXPIRE/PEXPIRE/INCR/BGSAVE/BGREWRITEAOF/SAVE/INFO
        -> existing evalX(args, NULL_CONN)  // replies captured as Buffer
    MULTI   -> evalMULTI_BEGIN(client)
    EXEC    -> evalMULTI(client)
    DISCARD -> evalDISCARD(client)
    default -> -ERR unknown command

MULTI:
  if client.isTxn -> -ERR MULTI calls can not be nested   (transaction continues)
  else isTxn=true, cqueue=[], txnDirty=false -> +OK

EXEC (evalMULTI):
  if !client.isTxn          -> -ERR EXEC without MULTI
  if client.txnDirty        -> reset state; -EXECABORT Transaction discarded because of previous errors.
  else:
    client.executing = true
    parts = cqueue.map(cmd => executeCommand(cmd, client))
    reset txn state (isTxn=false, cqueue=[], txnDirty=false, executing=false)
    -> *<N>\r\n + parts.join("")

DISCARD:
  if !client.isTxn -> -ERR DISCARD without MULTI
  else reset state -> +OK

Queue-time arity (TX_QUEUE_ARITY):
  PING [0,1]  SET [2,∞]  GET/TTL/INCR [1,1]  DEL [1,∞]
  EXPIRE/PEXPIRE [2,2]  BGSAVE/BGREWRITEAOF/SAVE/INFO [0,0]

Wire example:
  -> MULTI                 <- +OK
  -> SET k 1               <- +QUEUED
  -> GET k                 <- +QUEUED
  -> EXEC                  <- *2\r\n+OK\r\n$1\r\n1\r\n
```

## 4. What executed

- `resp-encoder.js`: `EncodeArray(items)` — `*N\r\n` + concatenated item bytes
- `eval.js`: `executeCommand` (pure Buffer dispatch, handlers called with
  `NULL_CONN`), `executeCmdToBuffer` alias, `respOk`, `evalMULTI` (begin),
  `evalEXEC` + `evalMulti` alias (array), `evalDISCARD`, `queueTxnCommand`,
  `resetTxn`, `TX_QUEUE_ARITY`; `evalAndRespond` gained the queue-or-execute
  branch
- `async.tcp.js`: `connectedClients` Map, `initClient(fdComm)` creating
  `{ fd, socket, addr, cqueue, isTxn, txnDirty, executing, write() }` on every
  accept, `readFromClient` passes the client object to `evalAndRespond`,
  `closeClient` deletes it, `stats().clients` exposed (exports `initClient`,
  `connectedClients`)
- `test-multi.js`: 79 tests (48 direct in-memory + 31 wire over TCP)
- `test-load-multi.js`: 37 assertions over 5 prod-env load rounds
- `load_test.md`: new append-only run entry (2026-10-20 06:59:02 UTC)

## 5. Final tested (cmds + result)

```bash
node test-multi.js        # 79 passed, 0 failed
node test-load-multi.js   # 37 passed, 0 failed
for f in test-*.js; do node "$f"; done   # 418 passed, 0 failed (13 suites)
```

Direct (in-memory client) — 48 checks:
- `MULTI` → `+OK`, `isTxn=true`; queue SET/GET → `+QUEUED`, keyspace untouched
- `EXEC` → `*2\r\n+OK\r\n$2\r\nv2\r\n` (order preserved, applied, state reset)
- Empty txn → `*0\r\n`; `EXEC`/`DISCARD` without MULTI → correct errors
- Nested `MULTI` → `-ERR MULTI calls can not be nested`, txn survives, EXEC applies
- Unknown command / bad arity at queue time → error + `txnDirty` →
  `-EXECABORT Transaction discarded because of previous errors.`, nothing applied
- Runtime error (INCR on "abc") stays an array element; siblings still applied
- Mixed replies `*4\r\n+OK\r\n:6\r\n:-1\r\n$1\r\n6\r\n`; INFO queued runs at EXEC
- Per-client isolation: B executes immediately while A queues; A's EXEC
  applies only A's queue; B state untouched
- `executeCommand`/`executeCmdToBuffer` return bytes, never write
- Reuse after EXEC / DISCARD / dirty+DISCARD; case-insensitive verbs

Wire (TCP) — 31 checks:
- One round trip `MULTI SET GET EXEC` → `OK, QUEUED, QUEUED, ["OK","v1"]`
- Pipeline around txn (`PING MULTI PING EXEC PING`) → 5 replies in order
- DISCARD over wire → key absent; unknown/arity → EXECABORT on the wire
- Nested MULTI keeps txn; two sockets fully isolated (B cannot see A's queue)
- `connectedClients` counts real sockets and drops on close/abandon

Prod load (`test-load-multi.js`, maxKeys=200000, evictPercent=4%, allkeys-lru):
- A: MULTI+10 SETs+EXEC, 100 conns × 20 = 24k cmds, 134ms, 179k cmds/s,
  20k keys, byte-exact
- B: MULTI+200 SETs+EXEC, 50 conns × 5 = 50.5k cmds, 179ms, 282k cmds/s,
  50k keys, byte-exact
- C: MULTI+20 SETs+DISCARD, 100 conns × 10 = 22k cmds, 47ms, 468k cmds/s,
  keys=0 (nothing written)
- D: bad arity → EXECABORT, 50 conns × 5 = 3k cmds, 10ms, keys=0
- E: mixed SET+MULTI+5 SETs+EXEC+PING, 100 conns × 10 = 9k cmds, 32ms, 6k keys
- Total 108.5k cmds in 402ms = 270k cmds/s; clients=0, monitored=0 after drain

## 6. Final conclusion

- Per-client transaction state keeps 100+ concurrent clients isolated; state is
  freed with the connection (`connectedClients` back to 0)
- Queue/EXEC/DISCARD match Redis reply shapes: `+QUEUED`, `*N` arrays,
  `-EXECABORT`, nested-MULTI error, `-ERR EXEC without MULTI`
- Backward compatible: plain-connection callers (handlers, fakeConn in tests,
  AOF replay `NOREPLY`, `main.js`) execute immediately — no state needed
- EXEC runs queued commands back-to-back in one event-loop turn with AOF
  logging per effective write; no rollback/isolation — same contract as Redis
- Known limit: no `WATCH`/optimistic locking (same as scope of this task)

---

## Appendix: Runs log (append-only)

### Run — 2026-10-20 06:59:02 UTC

- Implemented MULTI/EXEC/DISCARD + per-client state; docs, tests, load run
- `test-multi.js` 79/79, `test-load-multi.js` 37/37, full suite 418/418 (13 suites)
- Prod load: 108.5k txn cmds, 270k cmds/s (peak round 468k), all byte-exact,
  DISCARD/EXECABORT wrote nothing, drain clients=0 monitored=0
- Appended matching entry to `load_test.md` (append-only)
