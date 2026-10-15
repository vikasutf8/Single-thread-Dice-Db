"use strict";

// test-pipeline.js — Redis pipeline: 1 round trip, N cmds, N replies, in order.
// Pipeline != transaction: no atomicity, no queueing, failure mid-batch
// does NOT roll back earlier cmds and does NOT stop later cmds.

const net = require("node:net");
const assert = require("node:assert");
const { RunAsyncTcpServer } = require("./async.tcp.js");
const { Decode } = require("./resp-decoder.js");

let passed = 0;
let failed = 0;

function ok(name, cond, extra = "") {
    if (cond) {
        passed++;
        console.log(`PASS ${name}${extra ? " — " + extra : ""}`);
    } else {
        failed++;
        console.error(`FAIL ${name}${extra ? " — " + extra : ""}`);
    }
}

// Split a raw RESP stream into its N reply values using Decode's n.
function splitReplies(buf) {
    const out = [];
    let rest = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
    while (rest.length > 0) {
        const { data, n, err } = Decode(rest);
        if (err) return { replies: out, err, rest };
        out.push(data);
        rest = rest.slice(n);
    }
    return { replies: out, err: null, rest };
}

// RESP builders (what redis-cli / benchmarks actually send).
const R = {
    ping: () => "*1\r\n$4\r\nPING\r\n",
    pingMsg: (m) => `*2\r\n$4\r\nPING\r\n$${Buffer.byteLength(m)}\r\n${m}\r\n`,
    set: (k, v) => `*3\r\n$3\r\nSET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n$${Buffer.byteLength(v)}\r\n${v}\r\n`,
    get: (k) => `*2\r\n$3\r\nGET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n`,
    ttl: (k) => `*2\r\n$3\r\nTTL\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n`,
};

function withServer(fn) {
    return new Promise((resolve, reject) => {
        const h = RunAsyncTcpServer("127.0.0.1", 0, { maxClients: 100 });
        h.server.on("listening", async () => {
            const port = h.server.address().port;
            try {
                await fn(port, h);
                h.close(() => resolve());
            } catch (e) {
                h.close(() => reject(e));
            }
        });
        h.server.on("error", reject);
    });
}

// One TCP connection, ONE write (one round trip), read until N replies seen.
function oneRoundTrip(port, batch, expectReplies, timeoutMs = 2000) {
    return new Promise((resolve, reject) => {
        const c = net.connect(port, "127.0.0.1", () => c.write(batch));
        let buf = Buffer.alloc(0);
        const timer = setTimeout(() => {
            c.destroy();
            reject(new Error(`timeout waiting for ${expectReplies} replies, got ${JSON.stringify(buf.toString())}`));
        }, timeoutMs);
        c.on("data", (d) => {
            buf = Buffer.concat([buf, d]);
            const { replies, err } = splitReplies(buf);
            if (!err && replies.length >= expectReplies) {
                clearTimeout(timer);
                c.end();
                resolve({ raw: buf.toString(), replies });
            }
        });
        c.on("error", (e) => { clearTimeout(timer); reject(e); });
    });
}

async function main() {
    // 1. Basic: PING + SET + GET + PING in ONE write -> 4 replies in order.
    await withServer(async (port) => {
        const batch = R.ping() + R.set("pk1", "pv1") + R.get("pk1") + R.pingMsg("hello");
        const { raw, replies } = await oneRoundTrip(port, batch, 4);
        ok("pipeline 4-in-1 count", replies.length === 4, JSON.stringify(raw));
        ok("pipeline reply[0] PONG", replies[0] === "PONG");
        ok("pipeline reply[1] OK", replies[1] === "OK");
        ok("pipeline reply[2] GET value", replies[2] === "pv1");
        ok("pipeline reply[3] PING echo", replies[3] === "hello");
    });

    // 2. Pipeline != transaction: bad cmd in the middle does NOT stop the rest.
    await withServer(async (port) => {
        // PING a b -> error reply; surrounding cmds must still succeed.
        const badPing = "*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n";
        const batch = R.set("tk", "tv") + badPing + R.get("tk");
        const { replies } = await oneRoundTrip(port, batch, 3);
        ok("not-txn reply[0] OK (applied before error)", replies[0] === "OK");
        ok("not-txn reply[1] is Error (no rollback, just this reply)",
            replies[1] instanceof Error, String(replies[1] && replies[1].message));
        ok("not-txn reply[2] GET still runs after error", replies[2] === "tv");
    });

    // 3. Unknown command mid-pipeline is one -ERR, rest unaffected.
    await withServer(async (port) => {
        const batch = R.ping() + "*1\r\n$3\r\nFOO\r\n" + R.ping();
        const { replies } = await oneRoundTrip(port, batch, 3);
        ok("unknown-mid reply[0] PONG", replies[0] === "PONG");
        ok("unknown-mid reply[1] is Error", replies[1] instanceof Error);
        ok("unknown-mid reply[2] PONG", replies[2] === "PONG");
    });

    // 4. Split-packet pipeline: half the batch now, half later -> still all replies.
    await withServer(async (port) => {
        const batch = R.set("sk", "sv") + R.get("sk") + R.ttl("sk");
        const half = Math.floor(batch.length / 2);
        const r = await new Promise((resolve, reject) => {
            const c = net.connect(port, "127.0.0.1", () => {
                c.write(batch.slice(0, half));
                setTimeout(() => c.write(batch.slice(half)), 50);
            });
            let buf = Buffer.alloc(0);
            const timer = setTimeout(() => { c.destroy(); reject(new Error("split timeout: " + JSON.stringify(buf.toString()))); }, 2000);
            c.on("data", (d) => {
                buf = Buffer.concat([buf, d]);
                const { replies, err } = splitReplies(buf);
                if (!err && replies.length >= 3) { clearTimeout(timer); c.end(); resolve({ replies, raw: buf.toString() }); }
            });
            c.on("error", (e) => { clearTimeout(timer); reject(e); });
        });
        ok("split-packet count", r.replies.length === 3, JSON.stringify(r.raw));
        ok("split-packet SET OK", r.replies[0] === "OK");
        ok("split-packet GET value", r.replies[1] === "sv");
        ok("split-packet TTL persistent -1", r.replies[2] === -1);
    });

    // 5. Stats prove batching: one EPOLLIN hit executed N commands.
    await withServer(async (port, h) => {
        const before = h.stats();
        const N = 10;
        let batch = "";
        for (let i = 0; i < N; i++) batch += R.ping();
        await oneRoundTrip(port, batch, N);
        await new Promise((r) => setTimeout(r, 50));
        const after = h.stats();
        ok("stats commands += N", after.commands - before.commands === N,
            `+${after.commands - before.commands}`);
        ok("stats maxBatchDepth >= N (single-hit depth)", after.maxBatchDepth >= N,
            `maxBatchDepth=${after.maxBatchDepth}`);
    });

    // 6. Mixed types incl. DEL/EXPIRE in one round trip.
    await withServer(async (port) => {
        const setK = (k, v) => `*5\r\n$3\r\nSET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n$${Buffer.byteLength(v)}\r\n${v}\r\n$2\r\nEX\r\n$2\r\n60\r\n`;
        const batch =
            setK("mk", "mv") +
            `*3\r\n$6\r\nEXPIRE\r\n$2\r\nmk\r\n$2\r\n50\r\n` +
            R.ttl("mk") + R.get("mk");
        const { replies } = await oneRoundTrip(port, batch, 4);
        ok("mixed SET EX OK", replies[0] === "OK");
        ok("mixed EXPIRE 1", replies[1] === 1);
        ok("mixed TTL ~50", replies[2] === 50, `ttl=${replies[2]}`);
        ok("mixed GET value", replies[3] === "mv");
    });

    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error("FATAL", e); process.exit(1); });
