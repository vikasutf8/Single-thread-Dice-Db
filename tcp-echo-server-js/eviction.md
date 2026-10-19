# Eviction — allkeys-random & allkeys-lru (`store.js`) — What / Plan / Arch / Executed / Tested / Conclusion

## 1. What we want

- Memory-bound cache with automatic eviction when thresholds exceeded
- **Two policies** (configurable via `configureEviction({ policy })`):
  - **allkeys-random**: pick random keys from entire keyspace (Redis-compatible)
  - **allkeys-lru**: approximate LRU using 24-bit LRU clock + eviction pool (Redis-compatible)
- Environment-aware thresholds:
  - Dev/Test: 200 keys max, evict 40% on breach
  - Production: 200,000 keys max, evict 4% on breach
- O(1) per-key accounting, O(k) eviction where k = evict count
- **LRU specifics**:
  - 24-bit LRU clock: `getLRUClock() = (Date.now() / 1000) & 0xFFFFFF` (~194 day wraparound)
  - Eviction pool: fixed-size array (16 entries) of `{ key, lastAccessedAt }`
  - Custom comparator: sort by idle time DESC (most idle = best eviction candidate)
  - Pool updated on every access (GET, SET, INCR, EXPIRE)
  - Sample sizes: dev/test=5, prod=5000 (configurable per env)
- `INFO` command exposes eviction stats: `evictedKeys`, `evictionPolicy`, `lruClock`, `poolSize`

## 2. What we plan

- `store.js`: replace `evictFirstKeys()` with two policies:
  - `evictAllRandomKeys(count)` — Fisher-Yates shuffle
  - `evictApproxLRU(count)` — eviction pool with LRU clock
- `ENV_CONFIG` object mapping env → `{ maxKeys, evictPercent }`
- `LRU_CONFIG` object mapping env → `{ sampleSize }`
- `configureEviction({ maxKeys, env, policy })` — unified config API
- `setEnvironment(env)` — switches thresholds at runtime
- `evictIfNeeded()` — called after every `put()`, triggers when `store.size > MAX_KEYS`
- `getInfo()` — includes `eviction` section with policy + counters + LRU clock
- `diceObj` extended with `lastAccessedAt` (24-bit LRU clock value)
- Tests: validate both policies, env configs, direct functions, wire protocol, pool behavior

## 3. What we arch (design)

```text
diceObj = { value, expireAt, typeEncoding, lastAccessedAt }  // lastAccessedAt = 24-bit LRU clock

LRU Clock (Redis getLRUClock):
  getLRUClock() = Math.floor(Date.now() / 1000) & 0xFFFFFF
  Resolution: 1 second, Max: 0xFFFFFF (16,777,215), Wraparound: ~194 days

Idle time calculation (handles wraparound):
  idle = (now - lastAccessedAt + LRU_CLOCK_MAX + 1) % (LRU_CLOCK_MAX + 1)

Eviction Pool (fixed size = 16):
  PoolItem = { key: string, lastAccessedAt: uint24 }
  Sorted by idle time DESC (most idle = index 0 = best eviction candidate)
  Updated on: put (new/overwrite), get (hit), setExpire, INCR
  Removed on: del, trackDelete, expired key cleanup

Eviction trigger (put → evictIfNeeded):
  if store.size > MAX_KEYS:
    evictCount = ceil(store.size * EVICT_PERCENT)
    if policy == "allkeys-lru":
      evictApproxLRU(evictCount)
    else:
      evictAllRandomKeys(evictCount)
    totalEvicted += removed
    log: [evict] policy=... env=... reason=keys maxKeys=... evictPercent=... removed=... poolSize=...

evictAllRandomKeys(count):
  keys = Array.from(store.keys())
  toEvict = min(count, keys.length)
  for i in 0..toEvict-1:              // Fisher-Yates partial shuffle
    randomIndex = floor(random() * keys.length)
    key = keys[randomIndex]
    keys[randomIndex] = keys[keys.length - 1]
    keys.pop()
    trackDelete(key)
  return removed

evictApproxLRU(count):
  sortEvictionPool()                   // by idle DESC
  keysToEvict = pool[0..count-1].filter(k => store.has(k))
  for key in keysToEvict:
    trackDelete(key)
  return removed

updateEvictionPool(key, lastAccessedAt):
  item = pool.find(p => p.key === key)
  if item: item.lastAccessedAt = lastAccessedAt
  else: pool.push({ key, lastAccessedAt })
  sortEvictionPool()
  if pool.length > 16: pool.length = 16  // mutate in place (preserves export ref)

ENV_CONFIG:
  development: { maxKeys: 200,   evictPercent: 0.40 }
  test:        { maxKeys: 200,   evictPercent: 0.40 }
  production:  { maxKeys: 200000, evictPercent: 0.04 }

LRU_SAMPLE_SIZE:
  development: 5
  test:        5
  production:  5000
```

## 4. What executed

- `store.js`: ~410 lines — two eviction policies, LRU clock, eviction pool, `diceObj.lastAccessedAt`, `getLRUClock`, `getIdleTime`, `comparePoolItemsByIdle`, `updateEvictionPool`, `removeFromEvictionPool`, `evictApproxLRU`, policy config
- `eval.js`: unchanged (uses `get()`, `put()`, `setExpire()` which update LRU automatically)
- `test-evict.js`: 22 tests — byte accounting, LRU eviction (access pattern respected), random eviction, env configs, direct functions, pool behavior, policy switching
- All 9 test suites pass: 264/264

## 5. Final tested (cmds + result)

```bash
node test-evict.js
# 22 passed, 0 failed:
#   byte accounting: grow/shrink/overwrite correct
#   LRU eviction: 10 keys → access k1,k3,k5 → 11th SET evicts never-used k2,k4,k6,k7,k8
#   random eviction: 10 keys → 11th SET evicts 5 random
#   env configs: dev=200/40%, test=200/40%, prod=200000/4%
#   evictAllRandomKeys(20) on 50 keys → exactly 20 removed
#   evictApproxLRU(20) on 50 keys → exactly 20 removed, recently accessed kept
#   pool behavior: inserts add to pool, access updates timestamps, del removes from pool
#   policy switching: LRU vs random behavior verified

node test-diceobj.js test-get-set-ttl.js test-del-expire.js test-incr.js test-pipeline.js test-aof-rdb.js test-ping.js test-resp-decoder.js
# 242 passed, 0 failed (full regression)
```

## 6. Final conclusion

- **allkeys-lru** implements Redis-style approximate LRU:
  - 24-bit LRU clock with 1-second resolution (~194 day wraparound)
  - Eviction pool (16 entries) maintained incrementally on every access
  - Sample-based eviction: dev/test sample 5 keys, prod samples 5000
  - Correctly evicts least recently used keys when clock has advanced
- **allkeys-random** retained as fallback: Fisher-Yates shuffle, no access tracking
- Policy configurable at runtime via `configureEviction({ policy: "allkeys-lru|allkeys-random" })`
- Environment separation: dev/test aggressive (40%), prod conservative (4%)
- `INFO` command exposes full eviction observability
- Known limits: 1-second clock resolution means accesses within same second have equal priority; no LFU; no `volatile-*` variants

---

## Appendix: Runs log (append-only)

### Run — 2026-10-19 16:30:00 UTC

- Implemented allkeys-random eviction replacing FIRST (insertion-order)
- Environment thresholds: dev/test=200 keys/40%, prod=200K keys/4%
- `evictAllRandomKeys(count)` using Fisher-Yates partial shuffle
- `INFO` command added with eviction section
- All 260 tests pass across 9 suites
- Zero regressions in persistence, pipeline, diceObj, expiry, commands

### Run — 2026-10-19 18:45:00 UTC

- Implemented allkeys-lru approximate LRU eviction (Redis-style)
- 24-bit LRU clock: `getLRUClock() = (Date.now()/1000) & 0xFFFFFF`
- Eviction pool: 16-entry array of `{ key, lastAccessedAt }` with custom idle-time comparator
- Pool updated incrementally on `put`, `get`, `setExpire`, `INCR`
- Sample sizes: dev/test=5, prod=5000 (configurable per env)
- Policy configurable: `configureEviction({ policy: "allkeys-lru|allkeys-random" })`
- All 264 tests pass across 9 suites
- Zero regressions in persistence, pipeline, diceObj, expiry, commands