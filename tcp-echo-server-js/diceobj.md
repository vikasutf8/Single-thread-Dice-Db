# diceObj — redisObject port (`store.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Grow the store value from `{ value, expireAt }` into a Redis-`robj`-shaped
  `diceObj`: `{ value, expireAt, typeEncoding }`, where `typeEncoding` is one
  `uint8` packing **type in the high 4 bits, encoding in the low 4 bits**.
- Codes: `OBJ_TYPE_STRING` (0); `OBJ_ENCODING_RAW` (0), `OBJ_ENCODING_INT`
  (1), `OBJ_ENCODING_EMBSTR` (2) — same numeric values as Redis.
- Helpers: `packTypeEncoding`, `getType` (shift right 4, top bits),
  `getEncoding` (mask `0x0F`, top bits set to zero), `assertType`
  (throws verbatim `WRONGTYPE …`), `assertEncoding` (internal guard),
  `autoEncoding` (int → INT, ≤44 B → EMBSTR, else RAW),
  `deduceTypeEncoding(value)` → `{ otype, oEnc }` (tryObjectEncoding port,
  the single place writers ask "how should this value be stored").
- `newObj(value, durationMs, otype?, oEnc?)` + absolute-time twin
  `newObjAbs(value, expireAt, otype?, oEnc?)`; encoding auto-detects unless
  passed explicitly. Every existing call site updated; legacy raw objs and
  RDB1 files keep working (default STRING/RAW).

## 2. What we plan

- `store.js`: constants + pack/unpack + asserts + `autoEncoding`; extend
  `newObj`, add `newObjAbs`; export all. `get`/`put`/`del`/expiry/eviction
  untouched (they only read `value`/`expireAt`).
- `eval.js`: `SET` deduces `(otype, oEnc)` via `deduceTypeEncoding(value)`
  (the `tryObjectEncoding` step) and stores both fields; `INCR` re-deduces
  after `+1` (TTL untouched); `GET`/`TTL`/`INCR` `assertType` under
  try/catch → `-WRONGTYPE` (proves the seam for future LIST/HASH types).
- `rdb.js`: bump snapshot to **RDB2** (one `u8 typeEncoding` per entry, kept
  verbatim so encodings survive restarts); `loadRDB` still reads legacy RDB1
  (auto-encode on the way in).
- `aof.js`: no format change — `SET … [PX]` frames re-derive encoding on
  replay (verified by test).
- Tests: new `test-diceobj.js` (38 asserts) + migrate all raw
  `{value, expireAt}` literals to `newObjAbs`; update `redis_cmd_impl.md`
  contract + load run appended to `load_test.md`.

## 3. What we arch (design)

```text
diceObj = { value: string, expireAt: epoch-ms | -1, typeEncoding: uint8 }

typeEncoding byte:  HIGH nibble = type      LOW nibble = encoding
                    [ t t t t | e e e e ]   e.g. STRING+EMBSTR = 0x02

newObj("42", -1)            -> { value:"42", expireAt:-1, typeEncoding:0x01 }
newObj("hi", 60000)         -> { value:"hi", expireAt:now+60s, typeEncoding:0x02 }
newObj("a"*50, -1)          -> typeEncoding 0x00 (RAW)
newObjAbs(v, absMs, t?, e?) -> same shape, absolute expiry (RDB/tests)

getType(o)     = (o.typeEncoding >> 4) & 0x0F   (missing byte -> STRING)
getEncoding(o) =  o.typeEncoding & 0x0F         (missing byte -> RAW)
assertType(o, T)     -> throw WRONGTYPE...  (caught in eval -> -WRONGTYPE wire)
assertEncoding(o, E) -> throw encoding mismatch... (internal use)

RDB2 entry: u32 klen,key | u32 vlen,val | u8 typeEncoding | i64 expireAt
RDB1 entry: (no type byte) -> loads via newObjAbs(v, exp) => auto-encoding
```

## 4. What executed

- `store.js`: +70 lines (codes, pack/unpack, asserts, auto, `newObjAbs`);
  zero changes to get/put/expiry/eviction paths.
- `eval.js`: `SET` stamps type explicitly; `GET`/`TTL` assert under
  try/catch → `-WRONGTYPE` (proves the seam for future LIST/HASH types).
- `rdb.js`: RDB2 writer + dual-magic loader; legacy raw objs snapshot as
  STRING/RAW via `??` fallback.
- Tests migrated: `test-del-expire.js` (6 literals), `test-get-set-ttl.js`
  (2), `test-aof-rdb.js` (2 + magic `RDB1`→`RDB2`); new `test-diceobj.js`.

## 5. Final tested (cmds + result)

```bash
node test-diceobj.js
# 38 passed, 0 failed:
#   pack 0x02/0x01, nibble extraction + masking, legacy defaults
#   auto INT/EMBSTR(44B edge)/RAW, unsafe-int + float -> EMBSTR
#   newObj/newObjAbs stamping, asserts pass/throw (WRONGTYPE verbatim)
#   SET stamps INT, GET serves "7", tampered type -> -WRONGTYPE, conn survives
#   RDB2 keeps INT/EMBSTR/RAW byte-exact; hand-built RDB1 loads + auto-encodes

# regression: 20+35+34+37+13+21+32 = 192/192 green (total 230 with diceobj)
# load burst 200x10 (see load_test.md): ~8.0k cmds/s, no regression vs 8.2k
```

Result: **38/38 diceobj, 192/192 regression — 230/230 total, no fd leak,
no stray artifacts.**

## 6. Final conclusion

- `diceObj` is in place with the exact Redis bit layout, so every future
  type (`LIST`, `SET`, …) slots into the high nibble and every future
  encoding into the low nibble with no migration: `pack`/`getType`/
  `getEncoding` already speak the full 4+4-bit space.
- Encoding today is metadata + assertion, not a separate storage path
  (value stays a JS string) — the honest minimal step; real int-sharing or
  embstr allocation would be a later optimization with the asserts already
  guarding it.
- Wire-visible change is one: a wrong-typed key now answers `-WRONGTYPE`
  instead of its raw bytes — exactly the seam object commands will need.
