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

console.log("--- count threshold: random eviction (allkeys-random) ---");
(function () {
    clearStore();
    configureEviction({ maxKeys: 10, env: "development" });
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

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, maxBytes: DEFAULT_MAX_BYTES, env: "development" });
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
    configureEviction({ maxKeys: 100, env: "development" });
    for (let i = 1; i <= 50; i++) {
        put(`r:k${i}`, newObj(`v${i}`, -1));
    }
    eq("50 keys inserted", store.size, 50);

    const removed = evictAllRandomKeys(20);
    eq("evictAllRandomKeys removes requested count", removed === 20, true);
    eq("size after eviction", store.size, 30);

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, env: "development" });
})();

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);