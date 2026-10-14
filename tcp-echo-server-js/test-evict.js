"use strict";

// Test FIRST eviction (count path only). The 10 MB size path is coded +
// commented in store.js but intentionally NOT unit-tested here.
const { evalSET, evalGET, evalAndRespond } = require("./eval");
const {
    store,
    put,
    newObj,
    get,
    del,
    configureEviction,
    cacheStats,
    DEFAULT_MAX_KEYS,
    DEFAULT_MAX_BYTES,
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
    const before = cacheStats().bytes;
    put("b:one", newObj("abc", -1));
    eq("bytes grow on put", cacheStats().bytes > before, true);
    del("b:one");
    eq("bytes back to baseline on del", cacheStats().bytes, before);

    put("b:two", newObj("abc", -1));
    const sized = cacheStats().bytes;
    put("b:two", newObj("abcdef", -1)); // overwrite: old size uncounted once
    eq("overwrite re-counts (grows by 3)", cacheStats().bytes, sized + 3);
    del("b:two");
})();

console.log("--- count threshold: maxKeys=10, 11th SET evicts first 10% ---");
(function () {
    configureEviction({ maxKeys: 10 });
    for (let i = 1; i <= 10; i++) {
        evalSET([`v:k${i}`, `v${i}`], fakeConn());
    }
    eq("10 keys, no eviction yet", store.size, 10);
    eq("oldest present before breach", get("v:k1").value, "v1");

    const c = fakeConn();
    evalSET(["v:k11", "v11"], c);
    eq("11th SET still +OK", c.last(), "+OK\r\n");
    eq("size back under limit (11-2)", store.size, 9);
    eq("first key evicted", get("v:k1"), null);
    eq("second key evicted", get("v:k2"), null);
    eq("third key kept", get("v:k3").value, "v3");
    eq("newest kept", get("v:k11").value, "v11");

    // Over the wire: evicted key reads as nil.
    const d = fakeConn();
    evalAndRespond({ Cmd: "GET", Args: ["v:k1"] }, d);
    eq("wire GET evicted -> nil", d.last(), "$-1\r\n");

    configureEviction({ maxKeys: DEFAULT_MAX_KEYS, maxBytes: DEFAULT_MAX_BYTES });
    eq("defaults restored", cacheStats().keys <= DEFAULT_MAX_KEYS, true);
})();

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
