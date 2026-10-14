"use strict";

// In-memory key/value store — Go study ported to Node.js.
// Go: var store map[string]*Obj (init in init())
//     type Obj struct { Value any; ExpireAt int64 }  // ExpireAt = epoch-ms, -1 = persistent
// Here: store = Map<string, Obj>, Obj = { value, expireAt }.
// Expiry is lazy: enforced on get(), no background sweeper.

const NO_EXPIRE = -1;

// init(): store is ready at module load.
const store = new Map();

// ---------------------------------------------------------------------------
// Eviction config — simple FIRST eviction (no LRU/LFU). Either threshold
// fires eviction. Defaults are production-ish; tests / terminal demos call
// configureEviction() to shrink them (e.g. maxKeys: 10).
// NOTE: the 10 MB size path is coded + commented but has NO automated test
// (filling 10 MB in unit tests is slow/flaky); it shares evictFirstKeys()
// with the tested count path.
// ---------------------------------------------------------------------------
const DEFAULT_MAX_KEYS = 20000;
const DEFAULT_MAX_BYTES = 10 * 1024 * 1024; // 10 MB cache capacity
const ENTRY_OVERHEAD = 64; // approx per-entry Map/object overhead (approximation, not exact)

let MAX_KEYS = DEFAULT_MAX_KEYS;
let MAX_BYTES = DEFAULT_MAX_BYTES;
let currentBytes = 0; // incremental approx of cache size in bytes
let totalEvicted = 0;

function configureEviction({ maxKeys, maxBytes } = {}) {
    if (maxKeys !== undefined) MAX_KEYS = Number(maxKeys);
    if (maxBytes !== undefined) MAX_BYTES = Number(maxBytes);
}

function entrySize(key, obj) {
    return (
        Buffer.byteLength(String(key)) +
        Buffer.byteLength(String(obj.value)) +
        ENTRY_OVERHEAD
    );
}

// trackDelete(key): the ONE delete path — removes + uncounts bytes.
// Returns true when a key was actually removed.
function trackDelete(key) {
    const obj = store.get(key);
    if (obj === undefined) return false;
    currentBytes -= entrySize(key, obj);
    store.delete(key);
    return true;
}

// newObj(value, durationMs): durationMs > 0 → dies at now+ms, else persistent.
function newObj(value, durationMs) {
    const ms = Number(durationMs);
    return {
        value,
        expireAt: Number.isFinite(ms) && ms > 0 ? Date.now() + ms : NO_EXPIRE,
    };
}

function put(key, obj) {
    if (store.has(key)) currentBytes -= entrySize(key, store.get(key)); // overwrite: uncount old
    store.set(key, obj);
    currentBytes += entrySize(key, obj);
    evictIfNeeded(); // threshold breach → drop oldest-inserted first
}

// get(key): missing → null; expired → lazy delete + null; else the Obj.
function get(key) {
    const obj = store.get(key);
    if (obj === undefined) return null;
    if (obj.expireAt !== NO_EXPIRE && Date.now() > obj.expireAt) {
        trackDelete(key);
        return null;
    }
    return obj;
}

function del(key) {
    return trackDelete(key);
}

// setExpire(key, durationMs): keep the value, (re)arm the TTL.
// get() first so an already-expired key is lazy-deleted → false (missing).
function setExpire(key, durationMs) {
    const obj = get(key);
    if (obj === null) return false;
    obj.expireAt = Date.now() + Number(durationMs);
    return true;
}

// ---------------------------------------------------------------------------
// Active expiry (Redis activeExpireCycle, study scale):
// sample up to 20 TTL-bearing keys per pass so each tick stays O(1) on the
// single-threaded event loop; the caller repeats passes while frac >= 0.25.
// ---------------------------------------------------------------------------
const EXPIRE_SAMPLE_SIZE = 20;
const EXPIRE_FRAC_THRESHOLD = 0.25;
const DELETE_EXPIRED_MAX_PASSES = 3; // bound per-tick block time

// expireSample(): one pass over the unordered Map.
// Returns { sampled, expired }; expired keys are deleted inline.
function expireSample() {
    let sampled = 0;
    let expired = 0;
    const now = Date.now();
    for (const [key, obj] of store) {
        if (obj.expireAt === NO_EXPIRE) continue; // persistent: not expirable
        sampled++;
        if (now > obj.expireAt) {
            trackDelete(key);
            expired++;
        }
        if (sampled >= EXPIRE_SAMPLE_SIZE) break;
    }
    return { sampled, expired };
}

// DeleteExpiredKeys(): core sweep run by the server cron each tick.
// Repeats passes while the expired fraction stays high (many keys dying),
// stops early when frac < 0.25. Returns total keys deleted.
function DeleteExpiredKeys() {
    let deleted = 0;
    for (let pass = 0; pass < DELETE_EXPIRED_MAX_PASSES; pass++) {
        const { sampled, expired } = expireSample();
        if (sampled === 0) break; // no TTL-bearing keys at all
        deleted += expired;
        const frac = expired / sampled;
        console.log(
            `[expire] pass=${pass} sampled=${sampled} expired=${expired} frac=${frac.toFixed(2)}`
        );
        if (frac < EXPIRE_FRAC_THRESHOLD) break;
    }
    return deleted;
}

// ---------------------------------------------------------------------------
// Simple FIRST eviction: Map iterates in insertion order, so "first" keys
// are the oldest-inserted. No LRU/LFU bookkeeping.
// ---------------------------------------------------------------------------

// evictFirstKeys(n): delete up to n oldest-inserted keys. Returns removed.
function evictFirstKeys(n) {
    let removed = 0;
    for (const key of store.keys()) {
        trackDelete(key);
        if (++removed >= n) break;
    }
    return removed;
}

// evictIfNeeded(): called at the end of every put(). Either threshold fires.
function evictIfNeeded() {
    let removed = 0;
    let reason = null;
    if (store.size > MAX_KEYS) {
        // Count trigger: drop first 10% of keys.
        reason = "keys";
        removed = evictFirstKeys(Math.ceil(store.size * 0.1));
    } else if (currentBytes > MAX_BYTES) {
        // Size trigger (coded + commented, NOT unit-tested — see header):
        // drop from the front until ≥1% of capacity is freed (or 10% scanned).
        reason = "bytes";
        const target = Math.ceil(MAX_BYTES * 0.01);
        const cap = Math.ceil(store.size * 0.1);
        let freed = 0;
        let scanned = 0;
        const before = currentBytes;
        for (const key of store.keys()) {
            if (trackDelete(key)) removed++;
            scanned++;
            if (before - currentBytes >= target || scanned >= cap) break;
        }
        freed = before - currentBytes;
        totalEvicted += removed;
        console.log(
            `[evict] reason=${reason} removed=${removed} freed=${freed}B size=${store.size} bytes=${currentBytes}`
        );
        return removed;
    }
    if (reason !== null) {
        totalEvicted += removed;
        console.log(
            `[evict] reason=${reason} removed=${removed} size=${store.size} bytes=${currentBytes}`
        );
    }
    return removed;
}

function cacheStats() {
    return { keys: store.size, bytes: currentBytes, evicted: totalEvicted };
}

module.exports = {
    store,
    newObj,
    put,
    get,
    del,
    setExpire,
    expireSample,
    DeleteExpiredKeys,
    configureEviction,
    evictIfNeeded,
    evictFirstKeys,
    cacheStats,
    NO_EXPIRE,
    EXPIRE_SAMPLE_SIZE,
    DEFAULT_MAX_KEYS,
    DEFAULT_MAX_BYTES,
};
