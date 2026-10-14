"use strict";

// Test DEL/EXPIRE/PEXPIRE + active expiry (DeleteExpiredKeys + server cron). No deps.
const { readCmd } = require("./redis-cmd");
const { evalDEL, evalEXPIRE, evalPEXPIRE, evalSET, evalTTL, evalAndRespond } = require("./eval");
const { store, put, get, DeleteExpiredKeys } = require("./store");

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

console.log("--- evalDEL ---");
(function () {
    let c = fakeConn();
    evalDEL([], c);
    eq("DEL no args", c.last(), "-ERR wrong number of arguments for 'del' command\r\n");

    evalSET(["d:a", "1"], fakeConn());
    evalSET(["d:b", "2"], fakeConn());
    c = fakeConn();
    evalDEL(["d:a", "d:b", "d:missing"], c);
    eq("DEL 2 of 3 -> :2", c.last(), ":2\r\n");
    eq("deleted keys gone", get("d:a"), null);

    c = fakeConn();
    evalDEL(["d:missing"], c);
    eq("DEL all missing -> :0", c.last(), ":0\r\n");

    // Expired key counts as absent (lazy get), not as deleted.
    put("d:old", { value: "v", expireAt: Date.now() - 1000 });
    c = fakeConn();
    evalDEL(["d:old"], c);
    eq("DEL expired -> :0", c.last(), ":0\r\n");

    evalSET(["d:x", "1"], fakeConn());
    c = fakeConn();
    evalAndRespond({ Cmd: "del", Args: ["d:x"] }, c);
    eq("dispatch lowercase del single write", c.chunks.length, 1);
    eq("dispatch del value", c.last(), ":1\r\n");
})();

console.log("--- evalEXPIRE / evalPEXPIRE ---");
(function () {
    let c = fakeConn();
    evalEXPIRE([], c);
    eq("EXPIRE no args", c.last(), "-ERR wrong number of arguments for 'expire' command\r\n");

    c = fakeConn();
    evalEXPIRE(["k"], c);
    eq("EXPIRE 1 arg", c.last(), "-ERR wrong number of arguments for 'expire' command\r\n");

    c = fakeConn();
    evalEXPIRE(["k", "10", "extra"], c);
    eq("EXPIRE 3 args", c.last(), "-ERR wrong number of arguments for 'expire' command\r\n");

    c = fakeConn();
    evalPEXPIRE(["k"], c);
    eq("PEXPIRE 1 arg names pexpire", c.last(), "-ERR wrong number of arguments for 'pexpire' command\r\n");

    c = fakeConn();
    evalEXPIRE(["e:missing-nope", "10"], c);
    eq("EXPIRE missing -> :0", c.last(), ":0\r\n");

    c = fakeConn();
    evalEXPIRE(["e:missing-nope", "abc"], c);
    eq("EXPIRE abc -> not integer", c.last(), "-ERR value is not an integer or out of range\r\n");

    evalSET(["e:k", "v"], fakeConn());
    c = fakeConn();
    evalEXPIRE(["e:k", "100"], c);
    eq("EXPIRE 100 -> :1", c.last(), ":1\r\n");
    c = fakeConn();
    const secs = Number(evalTTL(["e:k"], c).toString().slice(1));
    eq("TTL reflects EXPIRE (99..100]", secs <= 100 && secs >= 99, true);

    evalSET(["e:ms", "v"], fakeConn());
    c = fakeConn();
    evalPEXPIRE(["e:ms", "60000"], c);
    eq("PEXPIRE 60000 -> :1", c.last(), ":1\r\n");

    // seconds <= 0 deletes immediately, returns :1 (real Redis).
    evalSET(["e:zero", "v"], fakeConn());
    c = fakeConn();
    evalEXPIRE(["e:zero", "0"], c);
    eq("EXPIRE 0 -> :1", c.last(), ":1\r\n");
    eq("EXPIRE 0 key gone", get("e:zero"), null);

    evalSET(["e:neg", "v"], fakeConn());
    c = fakeConn();
    evalEXPIRE(["e:neg", "-5"], c);
    eq("EXPIRE -5 -> :1", c.last(), ":1\r\n");
    eq("EXPIRE -5 key gone", get("e:neg"), null);

    // EXPIRE on an already-expired key → :0.
    put("e:old", { value: "v", expireAt: Date.now() - 1000 });
    c = fakeConn();
    evalEXPIRE(["e:old", "10"], c);
    eq("EXPIRE expired -> :0", c.last(), ":0\r\n");

    // Value survives EXPIRE.
    evalSET(["e:keep", "stay"], fakeConn());
    evalEXPIRE(["e:keep", "100"], fakeConn());
    eq("value kept after EXPIRE", get("e:keep").value, "stay");

    c = fakeConn();
    evalAndRespond({ Cmd: "EXPIRE", Args: ["e:keep", "50"] }, c);
    eq("dispatch EXPIRE single write", c.chunks.length, 1);
    eq("dispatch EXPIRE value", c.last(), ":1\r\n");
})();

console.log("--- DeleteExpiredKeys (direct sweep) ---");
(function () {
    put("s:live1", { value: "a", expireAt: Date.now() + 60000 });
    put("s:live2", { value: "b", expireAt: -1 });
    put("s:dead1", { value: "x", expireAt: Date.now() - 1000 });
    put("s:dead2", { value: "y", expireAt: Date.now() - 5000 });
    const n = DeleteExpiredKeys();
    eq("sweep deletes >= 2", n >= 2, true);
    eq("dead1 gone", get("s:dead1"), null);
    eq("live1 kept", get("s:live1").value, "a");
    eq("live2 kept", get("s:live2").value, "b");

    // All-live store: single pass, frac 0 < 0.25 → break, returns 0.
    const m = DeleteExpiredKeys();
    eq("all-live sweep -> 0", m, 0);
})();

console.log("--- RESP wire: DEL/EXPIRE through readCmd ---");
(function () {
    const wire = Buffer.concat([
        Buffer.from("*3\r\n$3\r\nSET\r\n$5\r\nw:del\r\n$1\r\nv\r\n", "utf8"),
        Buffer.from("*2\r\n$3\r\nDEL\r\n$5\r\nw:del\r\n", "utf8"),
        Buffer.from("*3\r\n$3\r\nSET\r\n$5\r\nw:exp\r\n$1\r\nv\r\n", "utf8"),
        Buffer.from("*3\r\n$6\r\nEXPIRE\r\n$5\r\nw:exp\r\n$2\r\n60\r\n", "utf8"),
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
    eq("wire count", seen.length, 4);
    eq("wire SET", seen[0], "+OK\r\n");
    eq("wire DEL", seen[1], ":1\r\n");
    eq("wire SET2", seen[2], "+OK\r\n");
    eq("wire EXPIRE", seen[3], ":1\r\n");
})();

console.log("--- active cron proof: key dies with zero reads ---");
// NOTE: must await with real timers — Atomics.wait blocks the event loop
// and would starve the very cron timer under test.
(async () => {
    const { RunAsyncTcpServer } = require("./async.tcp.js");
    const srv = RunAsyncTcpServer("127.0.0.1", 18082, { cronFrequency: 100 });
    // SET via eval (no socket read involved), then NEVER touch the key.
    evalSET(["c:cron", "v", "PX", "50"], fakeConn());
    eq("cron key present before tick", store.has("c:cron"), true);
    await new Promise((r) => setTimeout(r, 350)); // ~3 cron ticks, no reads
    eq("cron key gone without any read", store.has("c:cron"), false);
    eq("cron ran (lastCronExecution set)", srv.stats().lastCronExecution > 0, true);
    srv.close(() => {
        console.log(`\n${passed} passed, ${failed} failed`);
        process.exit(failed === 0 ? 0 : 1);
    });
})();
