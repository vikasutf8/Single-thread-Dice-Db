"use strict";

// In-memory key/value store — Go study ported to Node.js.
// Go: var store map[string]*Obj (init in init())
//     type Obj struct { Value any; ExpireAt int64 }  // ExpireAt = epoch-ms, -1 = persistent
// Here: store = Map<string, diceObj>, diceObj = { value, expireAt, typeEncoding }.
// Expiry is lazy: enforced on get(), no background sweeper.
//
// diceObj mirrors Redis's redisObject (robj): one uint8 packs type + encoding:
//   typeEncoding = [ type: high 4 bits | encoding: low 4 bits ]
//   getType()     -> shift right 4 (top bits)
//   getEncoding() -> mask 0x0F (top bits set to zero)

const NO_EXPIRE = -1;

// --- diceObj type codes (robj->type, high 4 bits) ---
const OBJ_TYPE_STRING = 0;

// --- diceObj encoding codes (robj->encoding, low 4 bits) ---
const OBJ_ENCODING_RAW = 0; // string value, heap object
const OBJ_ENCODING_INT = 1; // integer-representable string (compact numeric)
const OBJ_ENCODING_EMBSTR = 2; // short string (<= EMBSTR_MAX_LEN bytes)

// Redis embstr limit: strings this short live inside the object itself.
const EMBSTR_MAX_LEN = 44;

// packTypeEncoding(otype, oEnc): one uint8, type on top, encoding below.
function packTypeEncoding(otype, oEnc) {
    return (((Number(otype) || 0) & 0x0f) << 4) | ((Number(oEnc) || 0) & 0x0f);
}

// getType(o): first (most significant) 4 bits. Legacy objs without
// typeEncoding (raw {value, expireAt} literals, RDB1 loads) read as STRING.
function getType(o) {
    if (!o || o.typeEncoding === undefined) return OBJ_TYPE_STRING;
    return (o.typeEncoding >> 4) & 0x0f;
}

// getEncoding(o): last (least significant) 4 bits — top bits masked to zero.
// Legacy objs without typeEncoding read as RAW.
function getEncoding(o) {
    if (!o || o.typeEncoding === undefined) return OBJ_ENCODING_RAW;
    return o.typeEncoding & 0x0f;
}

// assertType(o, expected): throws WRONGTYPE (Redis verbatim) on mismatch.
// Callers (evalGET/TTL/...) catch this and reply -WRONGTYPE on the wire.
function assertType(o, expected) {
    if (getType(o) !== expected) {
        throw new Error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}

// assertEncoding(o, expected): internal guard (encoding is a storage hint,
// never visible as a wrong-type error to clients).
function assertEncoding(o, expected) {
    if (getEncoding(o) !== expected) {
        throw new Error(
            `encoding mismatch: expected ${expected}, got ${getEncoding(o)}`
        );
    }
}

// deduceTypeEncoding(value): Redis tryObjectEncoding port — decide the
// (otype, oEnc) pair for a fresh value before it is stored. SET only ever
// produces strings, so otype is always OBJ_TYPE_STRING today; the encoding
// attempt is where the saving happens (INT < EMBSTR < RAW). Future types
// (LIST/HASH/...) extend the otype branch here, callers stay unchanged.
function deduceTypeEncoding(value) {
    return { otype: OBJ_TYPE_STRING, oEnc: autoEncoding(value) };
}

// autoEncoding(value): Redis string-encoding rule — integers first, then
// embstr for short strings, raw for the rest.
function autoEncoding(value) {
    const s = String(value);
    if (/^-?\d+$/.test(s)) {
        const n = Number(s);
        if (Number.isSafeInteger(n)) return OBJ_ENCODING_INT;
    }
    return Buffer.byteLength(s) <= EMBSTR_MAX_LEN ? OBJ_ENCODING_EMBSTR : OBJ_ENCODING_RAW;
}

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

// newObj(value, durationMs, otype?, oEnc?): durationMs > 0 → dies at now+ms,
// else persistent. otype defaults to STRING, oEnc auto-detects (INT/EMBSTR/RAW).
function newObj(value, durationMs, otype = OBJ_TYPE_STRING, oEnc) {
    const ms = Number(durationMs);
    return {
        value,
        expireAt: Number.isFinite(ms) && ms > 0 ? Date.now() + ms : NO_EXPIRE,
        typeEncoding: packTypeEncoding(otype, oEnc === undefined ? autoEncoding(value) : oEnc),
    };
}

// newObjAbs(value, expireAt, otype?, oEnc?): absolute-expiry twin for loaders
// (RDB) and tests — expireAt is epoch-ms, -1 = persistent.
function newObjAbs(value, expireAt, otype = OBJ_TYPE_STRING, oEnc) {
    return {
        value,
        expireAt: Number(expireAt),
        typeEncoding: packTypeEncoding(otype, oEnc === undefined ? autoEncoding(value) : oEnc),
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

// clearStore(): drop everything (restart simulation in tests, FLUSHALL path).
// Resets byte accounting so the cache stays consistent afterwards.
function clearStore() {
    store.clear();
    currentBytes = 0;
}

module.exports = {
    store,
    newObj,
    newObjAbs,
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
    clearStore,
    OBJ_TYPE_STRING,
    OBJ_ENCODING_RAW,
    OBJ_ENCODING_INT,
    OBJ_ENCODING_EMBSTR,
    EMBSTR_MAX_LEN,
    packTypeEncoding,
    getType,
    getEncoding,
    assertType,
    assertEncoding,
    autoEncoding,
    deduceTypeEncoding,
    NO_EXPIRE,
    EXPIRE_SAMPLE_SIZE,
    DEFAULT_MAX_KEYS,
    DEFAULT_MAX_BYTES,
};
