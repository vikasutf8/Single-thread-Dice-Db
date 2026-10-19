"use strict";

const NO_EXPIRE = -1;

const OBJ_TYPE_STRING = 0;

const OBJ_ENCODING_RAW = 0;
const OBJ_ENCODING_INT = 1;
const OBJ_ENCODING_EMBSTR = 2;

const EMBSTR_MAX_LEN = 44;

function packTypeEncoding(otype, oEnc) {
    return (((Number(otype) || 0) & 0x0f) << 4) | ((Number(oEnc) || 0) & 0x0f);
}

function getType(o) {
    if (!o || o.typeEncoding === undefined) return OBJ_TYPE_STRING;
    return (o.typeEncoding >> 4) & 0x0f;
}

function getEncoding(o) {
    if (!o || o.typeEncoding === undefined) return OBJ_ENCODING_RAW;
    return o.typeEncoding & 0x0f;
}

function assertType(o, expected) {
    if (getType(o) !== expected) {
        throw new Error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}

function assertEncoding(o, expected) {
    if (getEncoding(o) !== expected) {
        throw new Error(
            `encoding mismatch: expected ${expected}, got ${getEncoding(o)}`
        );
    }
}

function deduceTypeEncoding(value) {
    return { otype: OBJ_TYPE_STRING, oEnc: autoEncoding(value) };
}

function autoEncoding(value) {
    const s = String(value);
    if (/^-?\d+$/.test(s)) {
        const n = Number(s);
        if (Number.isSafeInteger(n)) return OBJ_ENCODING_INT;
    }
    return Buffer.byteLength(s) <= EMBSTR_MAX_LEN ? OBJ_ENCODING_EMBSTR : OBJ_ENCODING_RAW;
}

const store = new Map();

const ENV_DEV = "development";
const ENV_TEST = "test";
const ENV_PROD = "production";

let currentEnv = ENV_DEV;

const ENV_CONFIG = {
    [ENV_DEV]: { maxKeys: 200, evictPercent: 0.40 },
    [ENV_TEST]: { maxKeys: 200, evictPercent: 0.40 },
    [ENV_PROD]: { maxKeys: 200000, evictPercent: 0.04 },
};

let MAX_KEYS = ENV_CONFIG[ENV_DEV].maxKeys;
let EVICT_PERCENT = ENV_CONFIG[ENV_DEV].evictPercent;

const ENTRY_OVERHEAD = 64;
let currentBytes = 0;
let totalEvicted = 0;

function setEnvironment(env) {
    const normalized = String(env).toLowerCase();
    if (normalized === "prod" || normalized === "production") {
        currentEnv = ENV_PROD;
    } else if (normalized === "test") {
        currentEnv = ENV_TEST;
    } else {
        currentEnv = ENV_DEV;
    }
    MAX_KEYS = ENV_CONFIG[currentEnv].maxKeys;
    EVICT_PERCENT = ENV_CONFIG[currentEnv].evictPercent;
}

function configureEviction({ maxKeys, maxBytes, env } = {}) {
    if (env !== undefined) setEnvironment(env);
    if (maxKeys !== undefined) MAX_KEYS = Number(maxKeys);
}

function entrySize(key, obj) {
    return (
        Buffer.byteLength(String(key)) +
        Buffer.byteLength(String(obj.value)) +
        ENTRY_OVERHEAD
    );
}

function trackDelete(key) {
    const obj = store.get(key);
    if (obj === undefined) return false;
    currentBytes -= entrySize(key, obj);
    store.delete(key);
    return true;
}

function newObj(value, durationMs, otype = OBJ_TYPE_STRING, oEnc) {
    const ms = Number(durationMs);
    return {
        value,
        expireAt: Number.isFinite(ms) && ms > 0 ? Date.now() + ms : NO_EXPIRE,
        typeEncoding: packTypeEncoding(otype, oEnc === undefined ? autoEncoding(value) : oEnc),
    };
}

function newObjAbs(value, expireAt, otype = OBJ_TYPE_STRING, oEnc) {
    return {
        value,
        expireAt: Number(expireAt),
        typeEncoding: packTypeEncoding(otype, oEnc === undefined ? autoEncoding(value) : oEnc),
    };
}

function put(key, obj) {
    if (store.has(key)) currentBytes -= entrySize(key, store.get(key));
    store.set(key, obj);
    currentBytes += entrySize(key, obj);
    evictIfNeeded();
}

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

function setExpire(key, durationMs) {
    const obj = get(key);
    if (obj === null) return false;
    obj.expireAt = Date.now() + Number(durationMs);
    return true;
}

const EXPIRE_SAMPLE_SIZE = 20;
const EXPIRE_FRAC_THRESHOLD = 0.25;
const DELETE_EXPIRED_MAX_PASSES = 3;

function expireSample() {
    let sampled = 0;
    let expired = 0;
    const now = Date.now();
    for (const [key, obj] of store) {
        if (obj.expireAt === NO_EXPIRE) continue;
        sampled++;
        if (now > obj.expireAt) {
            trackDelete(key);
            expired++;
        }
        if (sampled >= EXPIRE_SAMPLE_SIZE) break;
    }
    return { sampled, expired };
}

function DeleteExpiredKeys() {
    let deleted = 0;
    for (let pass = 0; pass < DELETE_EXPIRED_MAX_PASSES; pass++) {
        const { sampled, expired } = expireSample();
        if (sampled === 0) break;
        deleted += expired;
        const frac = expired / sampled;
        console.log(
            `[expire] pass=${pass} sampled=${sampled} expired=${expired} frac=${frac.toFixed(2)}`
        );
        if (frac < EXPIRE_FRAC_THRESHOLD) break;
    }
    return deleted;
}

function evictAllRandomKeys(count) {
    if (count <= 0 || store.size === 0) return 0;
    const keys = Array.from(store.keys());
    const toEvict = Math.min(count, keys.length);
    let removed = 0;
    for (let i = 0; i < toEvict; i++) {
        const randomIndex = Math.floor(Math.random() * keys.length);
        const key = keys[randomIndex];
        keys[randomIndex] = keys[keys.length - 1];
        keys.pop();
        if (trackDelete(key)) removed++;
    }
    return removed;
}

function evictIfNeeded() {
    if (store.size <= MAX_KEYS) return 0;
    const evictCount = Math.ceil(store.size * EVICT_PERCENT);
    const removed = evictAllRandomKeys(evictCount);
    totalEvicted += removed;
    console.log(
        `[evict] env=${currentEnv} reason=keys maxKeys=${MAX_KEYS} evictPercent=${EVICT_PERCENT} removed=${removed} size=${store.size} bytes=${currentBytes}`
    );
    return removed;
}

function cacheStats() {
    return { keys: store.size, bytes: currentBytes, evicted: totalEvicted };
}

function clearStore() {
    store.clear();
    currentBytes = 0;
}

function getInfo() {
    const now = Date.now();
    let expiredCount = 0;
    let ttlKeys = 0;
    let totalTtl = 0;
    for (const [, obj] of store) {
        if (obj.expireAt !== NO_EXPIRE) {
            ttlKeys++;
            if (now > obj.expireAt) {
                expiredCount++;
            } else {
                totalTtl += obj.expireAt - now;
            }
        }
    }
    return {
        server: {
            env: currentEnv,
            maxKeys: MAX_KEYS,
            evictPercent: EVICT_PERCENT,
        },
        stats: {
            connectedClients: 0,
            totalCommandsProcessed: 0,
            keyspaceHits: 0,
            keyspaceMisses: 0,
        },
        memory: {
            usedMemory: currentBytes,
            usedMemoryHuman: formatBytes(currentBytes),
            maxMemory: MAX_KEYS * 1024,
        },
        keyspace: {
            keys: store.size,
            expires: ttlKeys,
            avgTtl: ttlKeys > 0 ? Math.round(totalTtl / ttlKeys) : 0,
            expiredKeys: expiredCount,
        },
        eviction: {
            evictedKeys: totalEvicted,
            evictionPolicy: "allkeys-random",
        },
    };
}

function formatBytes(bytes) {
    if (bytes === 0) return "0B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + sizes[i];
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
    evictAllRandomKeys,
    cacheStats,
    clearStore,
    setEnvironment,
    getInfo,
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
    DEFAULT_MAX_KEYS: ENV_CONFIG[ENV_DEV].maxKeys,
    DEFAULT_MAX_BYTES: 10 * 1024 * 1024,
};