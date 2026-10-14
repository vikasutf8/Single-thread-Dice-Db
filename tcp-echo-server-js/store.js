"use strict";

// In-memory key/value store — Go study ported to Node.js.
// Go: var store map[string]*Obj (init in init())
//     type Obj struct { Value any; ExpireAt int64 }  // ExpireAt = epoch-ms, -1 = persistent
// Here: store = Map<string, Obj>, Obj = { value, expireAt }.
// Expiry is lazy: enforced on get(), no background sweeper.

const NO_EXPIRE = -1;

// init(): store is ready at module load.
const store = new Map();

// newObj(value, durationMs): durationMs > 0 → dies at now+ms, else persistent.
function newObj(value, durationMs) {
    const ms = Number(durationMs);
    return {
        value,
        expireAt: Number.isFinite(ms) && ms > 0 ? Date.now() + ms : NO_EXPIRE,
    };
}

function put(key, obj) {
    store.set(key, obj);
}

// get(key): missing → null; expired → lazy delete + null; else the Obj.
function get(key) {
    const obj = store.get(key);
    if (obj === undefined) return null;
    if (obj.expireAt !== NO_EXPIRE && Date.now() > obj.expireAt) {
        store.delete(key);
        return null;
    }
    return obj;
}

function del(key) {
    return store.delete(key);
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
            store.delete(key);
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

module.exports = {
    store,
    newObj,
    put,
    get,
    del,
    setExpire,
    expireSample,
    DeleteExpiredKeys,
    NO_EXPIRE,
    EXPIRE_SAMPLE_SIZE,
};
