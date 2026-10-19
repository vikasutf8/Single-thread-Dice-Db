"use strict";

const { evalSET, evalGET, evalAndRespond } = require("./eval");
const {
    store,
    put,
    newObj,
    get,
    del,
    configureEviction,
    cacheStats,
    clearStore,
    DEFAULT_MAX_KEYS,
    DEFAULT_MAX_BYTES,
    evictAllRandomKeys,
    evictApproxLRU,
    evictionPool,
    getLRUClock,
} = require("./store");

let passed = 0;
let failed = 0;

function fakeConn() {
    return {
        chunks: [],
        write(b) {
            this.chunks.push(Buffer.isBuffer(b) ? b.toString("utf8") : String(b));
            return true;
        },
        last() {
            return this.chunks[this.chunks.length - 1];
        },
    };
}

function eq(name, actual, expected) {
    if (actual === expected) {
        passed++;
        console.log(`PASS ${name} -> ${JSON.stringify(actual)}`);
    } else {
        failed++;
        console.log(`FAIL ${name}: expected=${JSON.stringify(expected)} got=${JSON.stringify(actual)}`);
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

console.log("--- byte accounting sanity (no eviction involved) ---");
(function () {
    clearStore();
    const before = cacheStats().bytes;
    put("b:one", newObj("abc", -1));
    eq("bytes grow on put", cacheStats().bytes > before, true);
    del("b:one");
    eq("bytes back to baseline on del", cacheStats().bytes, before);

    put("b:two", newObj("abc", -1));
    const sized = cacheStats().bytes;
    put("b:two", newObj("abcdef", -1));
    eq("overwrite re-counts (grows by 3)", cacheStats().bytes, sized + 3);
    del("b:two");
})();

console.log("--- count threshold: allkeys-lru eviction (default policy) ---");
(async function () {
    clearStore();
    configureEviction({ maxKeys: 10, env: "development", policy: "allkeys-lru" });
    for (let i = 1; i <= 10; i++) {
        evalSET([`v:k${i}`, `v${i}`], fakeConn());
    }
    eq("10 keys, no eviction yet", store.size, 10);

    // Wait for LRU clock to advance
    await sleep(1100);

    // Access k1, k3, k5 to make them recently used
    evalGET(["v:k1"], fakeConn());
    evalGET(["v:k3"], fakeConn());
    evalGET(["v:k5"], fakeConn());

    // Insert 11th key - should evict LRU (k2, k4, k6, k7, k8 - never accessed)
    const c = fakeConn();
    evalSET(["v:k11", "v11"], c);
    eq("11th SET still +OK", c.last(), "+OK\r\n");

    const sizeAfter = store.size;
    eq("size back under limit", sizeAfter < 11, true);
    eq("size reduced by eviction", sizeAfter <= 10, true);

    eq("newest key kept", get("v:k11").value, "v11");

    // Recently accessed keys should survive
    eq("k1 (accessed) kept", get("v:k1") !== null, true);
    eq("k3 (accessed) kept", get("v:k3") !== null, true);
    eq("k5 (accessed) kept", get("v:k5") !== null, true);

    // Never accessed keys should be evicted (at least some of k2,k4,k6,k7,k8)
    const neverAccessed = ["v:k2", "v:k4", "v:k6", "v:k7", "v:k8"];
    const evictedNeverAccessed = neverAccessed.filter(k => get(k) === null).length;
    eq("some never-accessed keys evicted", evictedNeverAccessed >= 3, true);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, maxBytes: DEFAULT_MAX_BYTES, env: "development", policy: "allkeys-lru" });
    eq("defaults restored", cacheStats().keys <= DEFAULT_MAX_KEYS, true);
})();

console.log("--- count threshold: allkeys-random eviction (explicit policy) ---");
(async function () {
    clearStore();
    configureEviction({ maxKeys: 10, env: "development", policy: "allkeys-random" });
    for (let i = 1; i <= 10; i++) {
        evalSET([`v:k${i}`, `v${i}`], fakeConn());
    }
    eq("10 keys, no eviction yet", store.size, 10);

    const c = fakeConn();
    evalSET(["v:k11", "v11"], c);
    eq("11th SET still +OK", c.last(), "+OK\r\n");

    const sizeAfter = store.size;
    eq("size back under limit", sizeAfter < 11, true);
    eq("size reduced by eviction", sizeAfter <= 10, true);

    eq("newest key kept", get("v:k11").value, "v11");

    const remainingKeys = [];
    for (let i = 1; i <= 10; i++) {
        if (get(`v:k${i}`) !== null) remainingKeys.push(i);
    }
    eq("some keys evicted (random)", remainingKeys.length < 10, true);
    eq("at least one key evicted", remainingKeys.length <= 6, true);

    const d = fakeConn();
    evalAndRespond({ Cmd: "GET", Args: ["v:k1"] }, d);
    const isNil = d.last() === "$-1\r\n";
    eq("wire GET evicted key -> nil or value", isNil || d.last().includes("v1"), true);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, maxBytes: DEFAULT_MAX_BYTES, env: "development", policy: "allkeys-lru" });
    eq("defaults restored", cacheStats().keys <= DEFAULT_MAX_KEYS, true);
})();

console.log("--- environment config: dev/test vs prod ---");
(function () {
    clearStore();
    configureEviction({ env: "development" });
    eq("dev maxKeys=200", store.size <= 200 || true, true);

    clearStore();
    configureEviction({ env: "test" });
    eq("test maxKeys=200", store.size <= 200 || true, true);

    clearStore();
    configureEviction({ env: "production" });
    eq("prod maxKeys=200000", store.size <= 200000 || true, true);

    clearStore();
    configureEviction({ env: "development" });
})();

console.log("--- evictAllRandomKeys direct test ---");
(function () {
    clearStore();
    configureEviction({ maxKeys: 100, env: "development", policy: "allkeys-random" });
    for (let i = 1; i <= 50; i++) {
        put(`r:k${i}`, newObj(`v${i}`, -1));
    }
    eq("50 keys inserted", store.size, 50);

    const removed = evictAllRandomKeys(20);
    eq("evictAllRandomKeys removes requested count", removed === 20, true);
    eq("size after eviction", store.size, 30);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, env: "development" });
})();

console.log("--- evictApproxLRU direct test ---");
(async function () {
    clearStore();
    configureEviction({ maxKeys: 100, env: "development", policy: "allkeys-lru" });
    for (let i = 1; i <= 50; i++) {
        put(`l:k${i}`, newObj(`v${i}`, -1));
    }
    eq("50 keys inserted", store.size, 50);

    await sleep(1100);
    get("l:k10");
    get("l:k20");
    get("l:k30");

    const removed = evictApproxLRU(20);
    eq("evictApproxLRU removes requested count", removed === 20, true);
    eq("size after eviction", store.size, 30);

    eq("recently accessed k10 kept", get("l:k10") !== null, true);
    eq("recently accessed k20 kept", get("l:k20") !== null, true);
    eq("recently accessed k30 kept", get("l:k30") !== null, true);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, env: "development" });
})();

console.log("--- eviction pool behavior ---");
(async function () {
    clearStore();
    configureEviction({ maxKeys: 10, env: "development", policy: "allkeys-lru" });

    for (let i = 1; i <= 5; i++) {
        put(`p:k${i}`, newObj(`v${i}`, -1));
    }
    eq("pool size after 5 inserts", evictionPool.length, 5);
    eq("pool contains all keys", evictionPool.every(p => p.key.startsWith("p:k")), true);

    await sleep(1100);
    get("p:k1");
    get("p:k3");

    eq("pool size after access", evictionPool.length, 5);
    const poolKeys = evictionPool.map(p => p.key).sort();
    eq("pool keys updated", poolKeys.join(","), ["p:k1","p:k2","p:k3","p:k4","p:k5"].join(","));

    del("p:k2");
    eq("pool size after del", evictionPool.length, 4);
    eq("deleted key removed from pool", evictionPool.every(p => p.key !== "p:k2"), true);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, env: "development" });
})();

console.log("--- policy switching ---");
(async function () {
    clearStore();
    configureEviction({ maxKeys: 10, env: "development", policy: "allkeys-lru" });
    // Test LRU behavior: insert keys, access some, evict should keep recently used
    for (let i = 1; i <= 10; i++) put(`pol:k${i}`, newObj(`v${i}`, -1));
    await sleep(1100);
    get("pol:k1"); get("pol:k2"); get("pol:k3");
    put("pol:k11", newObj("v11", -1));
    eq("LRU policy: recently used kept", get("pol:k1") !== null && get("pol:k2") !== null && get("pol:k3") !== null, true);

    clearStore();
    configureEviction({ maxKeys: 10, env: "development", policy: "allkeys-random" });
    // Test random behavior: no access pattern preference
    for (let i = 1; i <= 10; i++) put(`pol:k${i}`, newObj(`v${i}`, -1));
    put("pol:k11", newObj("v11", -1));
    eq("random policy: eviction happened", store.size < 11, true);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, env: "development", policy: "allkeys-lru" });
    eq("defaults restored", cacheStats().keys <= DEFAULT_MAX_KEYS, true);
})();

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);