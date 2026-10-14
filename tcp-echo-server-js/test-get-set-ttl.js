"use strict";

// Test SET/GET/TTL: store + eval fns + evalAndRespond dispatch. No deps.
const { readCmd } = require("./redis-cmd");
const { evalSET, evalGET, evalTTL, evalAndRespond } = require("./eval");
const { put, get } = require("./store");

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

// Sync sleep (Atomics.wait) — only for the one live-expiry check.
function sleepMs(ms) {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

console.log("--- evalSET happy paths ---");
(function () {
    let c = fakeConn();
    evalSET(["s:name", "vikas"], c);
    eq("SET plain -> OK", c.last(), "+OK\r\n");

    c = fakeConn();
    evalSET(["s:ex", "v", "EX", "100"], c);
    eq("SET EX 100 -> OK", c.last(), "+OK\r\n");

    c = fakeConn();
    evalSET(["s:px", "v", "PX", "60000"], c);
    eq("SET PX 60000 -> OK", c.last(), "+OK\r\n");

    c = fakeConn();
    evalSET(["s:low", "v", "ex", "50"], c);
    eq("SET lowercase ex -> OK", c.last(), "+OK\r\n");

    c = fakeConn();
    evalAndRespond({ Cmd: "set", Args: ["s:via", "dispatch"] }, c);
    eq("dispatch lowercase set", c.last(), "+OK\r\n");
    eq("dispatch set single write", c.chunks.length, 1);
})();

console.log("--- evalSET errors ---");
(function () {
    let c = fakeConn();
    evalSET([], c);
    eq("SET no args", c.last(), "-ERR wrong number of arguments for 'set' command\r\n");

    c = fakeConn();
    evalSET(["onlykey"], c);
    eq("SET 1 arg", c.last(), "-ERR wrong number of arguments for 'set' command\r\n");

    c = fakeConn();
    evalSET(["k", "v", "EX"], c);
    eq("SET EX w/o value -> syntax", c.last(), "-ERR syntax error\r\n");

    c = fakeConn();
    evalSET(["k", "v", "EX", "abc"], c);
    eq("SET EX abc -> not integer", c.last(), "-ERR value is not an integer or out of range\r\n");

    c = fakeConn();
    evalSET(["k", "v", "EX", "0"], c);
    eq("SET EX 0 -> invalid expire", c.last(), "-ERR invalid expire time in 'set' command\r\n");

    c = fakeConn();
    evalSET(["k", "v", "EX", "-5"], c);
    eq("SET EX -5 -> invalid expire", c.last(), "-ERR invalid expire time in 'set' command\r\n");

    c = fakeConn();
    evalSET(["k", "v", "FOO"], c);
    eq("SET junk token -> syntax", c.last(), "-ERR syntax error\r\n");
})();

console.log("--- evalGET ---");
(function () {
    let c = fakeConn();
    evalGET([], c);
    eq("GET no args", c.last(), "-ERR wrong number of arguments for 'get' command\r\n");

    c = fakeConn();
    evalGET(["s:name"], c);
    eq("GET round-trip", c.last(), "$5\r\nvikas\r\n");

    c = fakeConn();
    evalGET(["s:missing-nope"], c);
    eq("GET missing -> nil", c.last(), "$-1\r\n");

    // Deterministic expired key: Obj with expireAt already in the past.
    put("s:old", { value: "stale", expireAt: Date.now() - 1000 });
    c = fakeConn();
    evalGET(["s:old"], c);
    eq("GET expired -> nil", c.last(), "$-1\r\n");
    eq("expired key lazily deleted", get("s:old"), null);

    c = fakeConn();
    evalAndRespond({ Cmd: "GET", Args: ["s:name"] }, c);
    eq("dispatch GET single write", c.chunks.length, 1);
    eq("dispatch GET value", c.last(), "$5\r\nvikas\r\n");
})();

console.log("--- evalTTL ---");
(function () {
    let c = fakeConn();
    evalTTL([], c);
    eq("TTL no args", c.last(), "-ERR wrong number of arguments for 'ttl' command\r\n");

    c = fakeConn();
    evalTTL(["t:missing-nope"], c);
    eq("TTL missing -> -2", c.last(), ":-2\r\n");

    evalSET(["t:persist", "v"], fakeConn());
    c = fakeConn();
    evalTTL(["t:persist"], c);
    eq("TTL persistent -> -1", c.last(), ":-1\r\n");

    evalSET(["t:exp", "v", "EX", "100"], fakeConn());
    c = fakeConn();
    const n = Number(evalTTL(["t:exp"], c).toString().slice(1));
    eq("TTL EX 100 in (99..100]", n <= 100 && n >= 99, true);

    put("t:old", { value: "v", expireAt: Date.now() - 1000 });
    c = fakeConn();
    evalTTL(["t:old"], c);
    eq("TTL expired -> -2", c.last(), ":-2\r\n");

    c = fakeConn();
    evalAndRespond({ Cmd: "ttl", Args: ["t:persist"] }, c);
    eq("dispatch lowercase ttl single write", c.chunks.length, 1);
    eq("dispatch ttl value", c.last(), ":-1\r\n");
})();

console.log("--- live expiry (PX 50ms) ---");
(function () {
    evalSET(["t:live", "v", "PX", "50"], fakeConn());
    let c = fakeConn();
    evalGET(["t:live"], c);
    eq("GET before expiry", c.last(), "$1\r\nv\r\n");
    sleepMs(150);
    c = fakeConn();
    evalGET(["t:live"], c);
    eq("GET after expiry -> nil", c.last(), "$-1\r\n");
    c = fakeConn();
    evalTTL(["t:live"], c);
    eq("TTL after expiry -> -2", c.last(), ":-2\r\n");
})();

console.log("--- RESP wire: SET/GET/TTL through readCmd ---");
(function () {
    const wire = Buffer.concat([
        Buffer.from("*3\r\n$3\r\nSET\r\n$5\r\nw:key\r\n$5\r\nhello\r\n", "utf8"),
        Buffer.from("*2\r\n$3\r\nGET\r\n$5\r\nw:key\r\n", "utf8"),
        Buffer.from("*2\r\n$3\r\nTTL\r\n$5\r\nw:key\r\n", "utf8"),
    ]);
    const c = fakeConn();
    let rest = wire;
    const seen = [];
    while (rest.length > 0) {
        const r = readCmd(rest);
        if (r.err) break;
        evalAndRespond(r.cmd, c);
        seen.push(c.last());
        rest = rest.slice(r.n);
    }
    eq("wire count", seen.length, 3);
    eq("wire SET", seen[0], "+OK\r\n");
    eq("wire GET", seen[1], "$5\r\nhello\r\n");
    eq("wire TTL persist", seen[2], ":-1\r\n");
})();

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
