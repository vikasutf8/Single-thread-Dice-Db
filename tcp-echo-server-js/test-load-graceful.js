"use strict";

// test-load-graceful.js — Production-env load test + graceful shutdown under load.
// Scripted RESP-over-TCP (no redis-benchmark on this box), one write per batch.
//
// Rounds:
//   A. prod config bulk load (200 conns x 10x10 PING/SET) — throughput baseline
//   B. prod eviction pressure (250k unique SETs vs maxKeys=200000, 4% evict)
//   C. graceful shutdown WHILE clients are mid-flight (AOF + RDB on)
//
// Verify: byte-exact replies, bounded keyspace, active->0, AOF exists,
// RDB reloads, shutdown completes within timeout.

const net = require("net");
const fs = require("fs");
const path = require("path");
const { RunAsyncTcpServer } = require("./async.tcp.js");
const { clearStore, configureEviction, getInfo, store } = require("./store");
const rdb = require("./rdb");

let passed = 0;
let failed = 0;

function eq(name, actual, expected) {
    if (actual === expected) {
        passed++;
        console.log(`PASS ${name}`);
    } else {
        failed++;
        console.log(`FAIL ${name}: expected=${JSON.stringify(expected)} got=${JSON.stringify(actual)}`);
    }
}

// Silence per-connection / eviction / stats spam; count evictions instead.
let evictEvents = 0;
const realLog = console.log.bind(console);
console.log = (...args) => {
    const s = String(args[0] ?? "");
    if (s.startsWith("[evict]")) { evictEvents++; return; }
    if (s.startsWith("Accepted fd=") || s.startsWith("Client gone")) return;
    if (s.startsWith("[stats]") || s.startsWith("[cron]")) return;
    realLog(...args);
};

function encodeCmd(args) {
    let s = `*${args.length}\r\n`;
    for (const a of args) {
        const b = Buffer.from(String(a));
        s += `$${b.length}\r\n${a}\r\n`;
    }
    return s;
}

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

// Pipeline client: sends `batches` batches of `depth` cmds, one write each.
// Resolves when all expected bytes arrive, or the socket closes (partial).
function runConn(port, { batches, depth, cmdAt, connId, timeoutMs = 30000 }) {
    return new Promise((resolve) => {
        const sock = net.connect(port, "127.0.0.1");
        let expected = "";
        let received = "";
        let sent = 0;
        let settled = false;
        const t0 = process.hrtime.bigint();
        const settle = (kind) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            const ms = Number(process.hrtime.bigint() - t0) / 1e6;
            try { sock.destroy(); } catch { /* ignore */ }
            resolve({ kind, ms, received, full: received === expected, sent, batches });
        };
        const timer = setTimeout(() => settle("timeout"), timeoutMs);

        sock.on("connect", () => {
            const sendNext = () => {
                if (sent >= batches) return;
                let batch = "";
                for (let d = 0; d < depth; d++) {
                    const { args, expect } = cmdAt(sent, d, connId);
                    batch += encodeCmd(args);
                    expected += expect;
                }
                sock.write(batch);
                sent++;
                if (sent < batches) sendNext();
            };
            sendNext();
        });
        sock.on("data", (chunk) => {
            received += chunk.toString();
            if (received.length >= expected.length) {
                settle(received === expected ? "ok" : "mismatch");
            }
        });
        sock.on("close", () => settle(received.length > 0 ? "partial" : "closed"));
        sock.on("error", () => settle("error"));
    });
}

async function runRound(port, { conns, batches, depth, cmdAt }) {
    const t0 = process.hrtime.bigint();
    const results = await Promise.all(
        Array.from({ length: conns }, (_, i) => runConn(port, { batches, depth, cmdAt, connId: i }))
    );
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    const ok = results.filter((r) => r.kind === "ok").length;
    const timeouts = results.filter((r) => r.kind === "timeout").length;
    const partial = results.filter((r) => r.kind === "partial" || r.kind === "closed" || r.kind === "error").length;
    return {
        conns, batches, depth,
        ok, partial, timeouts, mismatch: conns - ok - partial - timeouts,
        cmds: conns * batches * depth,
        ms: Math.round(ms),
        kcmds: Math.round((conns * batches * depth) / ms),
    };
}

function snap() {
    const i = getInfo();
    return {
        keys: i.keyspace.keys,
        evicted: i.eviction.evictedKeys,
        poolSize: i.eviction.poolSize,
        maxKeys: i.server.maxKeys,
        evictPercent: i.server.evictPercent,
        policy: i.eviction.evictionPolicy,
    };
}

async function roundA(port) {
    console.log("\n--- Round A: prod bulk load (200 conns x 10x10 PING/SET = 20k cmds) ---");
    clearStore();
    configureEviction({ env: "production", policy: "allkeys-lru" });
    const s = snap();
    eq("prod maxKeys=200000", s.maxKeys, 200000);
    eq("prod evictPercent=4%", s.evictPercent, 0.04);
    eq("prod policy=allkeys-lru", s.policy, "allkeys-lru");

    const r = await runRound(port, {
        conns: 200, batches: 10, depth: 10,
        cmdAt: (b, d, i) =>
            d % 2 === 0
                ? { args: ["PING"], expect: "+PONG\r\n" }
                : { args: ["SET", `a:${i}:${b}:${d}`, `v${b}${d}`], expect: "+OK\r\n" },
    });
    const after = snap();
    eq("all 200 conns byte-exact ok", r.ok, 200);
    eq("no timeouts", r.timeouts, 0);
    eq("10k unique keys stored", after.keys, 10000);
    eq("no eviction below prod limit", after.evicted, 0);
    console.log(`[load] A: ${r.cmds} cmds in ${r.ms}ms = ${r.kcmds}k cmds/s, keys=${after.keys}, pool=${after.poolSize}`);
    return { ...r, keys: after.keys, evicted: 0 };
}

async function roundB(port) {
    console.log("\n--- Round B: prod eviction pressure (250k unique SETs vs 200k max) ---");
    clearStore();
    configureEviction({ env: "production", policy: "allkeys-lru" });
    const before = snap();
    const evBefore = evictEvents;

    const r = await runRound(port, {
        conns: 100, batches: 25, depth: 100,
        cmdAt: (b, d, i) => ({ args: ["SET", `b:${i}:${b}:${d}`, `v${d}`], expect: "+OK\r\n" }),
    });
    const after = snap();
    const evs = evictEvents - evBefore;
    eq("all 100 conns byte-exact ok", r.ok, 100);
    eq("no timeouts", r.timeouts, 0);
    eq("keyspace bounded at prod max", after.keys <= 200001, true);
    eq("eviction occurred", after.evicted > 0, true);
    eq("pool bounded to 16", after.poolSize <= 16, true);
    eq("final policy unchanged", after.policy, "allkeys-lru");
    console.log(
        `[load] B: ${r.cmds} cmds in ${r.ms}ms = ${r.kcmds}k cmds/s, keys=${after.keys}, ` +
        `evicted=${after.evicted - before.evicted}, evictEvents=${evs}, pool=${after.poolSize}`
    );
    return { ...r, keys: after.keys, evicted: after.evicted - before.evicted, evictEvents: evs, poolSize: after.poolSize };
}

async function roundC(port, aofPath, rdbPath) {
    console.log("\n--- Round C: graceful shutdown WHILE 100 clients are mid-flight ---");
    console.log("[load] C: starting server with AOF+RDB...");
    clearStore();
    const srv = RunAsyncTcpServer("127.0.0.1", port, {
        maxClients: 1000,
        aofEnabled: true,
        aofPath,
        rdbPath,
    });
    configureEviction({ env: "production", policy: "allkeys-lru" });
    await sleep(150);

    const totalSetCmds = 100 * 20 * 10;
    const loadPromise = runRound(port, {
        conns: 100, batches: 20, depth: 10,
        cmdAt: (b, d, i) => ({ args: ["SET", `c:${i}:${b}:${d}`, `v${d}`], expect: "+OK\r\n" }),
    });

    await sleep(30); // let the load get in flight
    console.log("[load] C: initiating graceful shutdown under load...");
    const t0 = process.hrtime.bigint();
    await srv.gracefulShutdown({ saveRDB: true, flushAOF: true, timeoutMs: 3000 });
    const shutdownMs = Math.round(Number(process.hrtime.bigint() - t0) / 1e6);

    const load = await loadPromise;
    const stats = srv.stats();
    const keysAtSave = getInfo().keyspace.keys;

    eq("shutdown finished within timeout+slack", shutdownMs < 5000, true);
    eq("no client timeouts after shutdown", load.timeouts, 0);
    eq("active clients zero", stats.active, 0);
    eq("monitored fds zero (no fd leak)", stats.monitored, 0);
    eq("AOF file exists", fs.existsSync(aofPath), true);
    eq("RDB file exists", fs.existsSync(rdbPath), true);

    const loaded = rdb.loadRDB(rdbPath);
    eq("RDB reload keys match keys-at-save", loaded.loaded, keysAtSave);
    eq("keys-at-save <= total SET cmds", keysAtSave <= totalSetCmds, true);

    console.log(
        `[load] C: load ok=${load.ok}/100 partial=${load.partial} timeouts=${load.timeouts} ` +
        `cmds=${load.cmds} in-flight-ms=${load.ms}; shutdown=${shutdownMs}ms; ` +
        `keys=${keysAtSave}; rdb-loaded=${loaded.loaded}; aof-bytes=${fs.statSync(aofPath).size}`
    );
    return { ...load, shutdownMs, keysAtSave, rdbLoaded: loaded.loaded, aofBytes: fs.statSync(aofPath).size };
}

(async () => {
    const aofPath = path.join(__dirname, "AOF", "load-shutdown.aof");
    const rdbPath = path.join(__dirname, "RDB", "load-shutdown.rdb");
    try { fs.unlinkSync(aofPath); } catch { /* ignore */ }
    try { fs.unlinkSync(rdbPath); } catch { /* ignore */ }

    const srv = RunAsyncTcpServer("127.0.0.1", 18095, { maxClients: 1000 });
    await sleep(150);

    try {
        const A = await roundA(18095);
        const B = await roundB(18095);

        await new Promise((r) => srv.close(r));

        const C = await roundC(18096, aofPath, rdbPath);

        realLog("\n=== LOAD SUMMARY (prod env) ===");
        realLog(JSON.stringify({ roundA: A, roundB: B, roundC: C }, null, 2));
        realLog(`\n=== ${passed} passed, ${failed} failed ===`);

        try { fs.unlinkSync(aofPath); } catch { /* ignore */ }
        try { fs.unlinkSync(rdbPath); } catch { /* ignore */ }
        process.exit(failed === 0 ? 0 : 1);
    } catch (err) {
        realLog(`test suite error: ${err && err.stack ? err.stack : err}`);
        try { fs.unlinkSync(aofPath); } catch { /* ignore */ }
        try { fs.unlinkSync(rdbPath); } catch { /* ignore */ }
        process.exit(1);
    }
})();
