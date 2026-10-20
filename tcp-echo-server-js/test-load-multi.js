"use strict";

// test-load-multi.js — Production-env load test for MULTI / EXEC / DISCARD.
// Scripted RESP-over-TCP (no redis-benchmark on this box), one write per batch.
//
// Rounds (prod config: maxKeys=200000, evictPercent=4%, allkeys-lru):
//   A. MULTI + 10 SETs + EXEC      — 100 conns x 20 batches (24k cmds, 20k writes)
//   B. Deep queue: MULTI + 200 SETs + EXEC — 50 conns x 5 batches (50.5k cmds)
//   C. MULTI + 20 SETs + DISCARD   — 100 conns x 10 batches (22k cmds, 0 writes)
//   D. MULTI + bad-arity + SETs + EXECABORT — 50 conns x 5 batches (3k cmds)
//   E. Mixed: SET + MULTI + 5 SETs + EXEC + PING — 100 conns x 10 batches
//
// Verify: byte-exact replies, keyspace counts, no eviction, clients drained.

const net = require("net");
const { RunAsyncTcpServer } = require("./async.tcp.js");
const { clearStore, configureEviction, getInfo } = require("./store");

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

function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}

function encodeCmd(args) {
    let s = `*${args.length}\r\n`;
    for (const a of args) {
        const b = Buffer.from(String(a));
        s += `$${b.length}\r\n${a}\r\n`;
    }
    return s;
}

// One connection, `batches` transaction batches, one write per batch.
// mode: "exec" | "discard" | "abort".
function runTxnConn(port, { batches, perBatch, mode, connId, prefix, timeoutMs = 30000 }) {
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
                let req = encodeCmd(["MULTI"]);
                let exp = "+OK\r\n";
                for (let d = 0; d < perBatch; d++) {
                    const key = `${prefix}:${connId}:${sent}:${d}`;
                    if (mode === "abort" && d === 0) {
                        req += encodeCmd(["SET", key]); // wrong arity at queue time
                        exp += "-ERR wrong number of arguments for 'set' command\r\n";
                    } else {
                        req += encodeCmd(["SET", key, "v"]);
                        exp += "+QUEUED\r\n";
                    }
                }
                if (mode === "discard") {
                    req += encodeCmd(["DISCARD"]);
                    exp += "+OK\r\n";
                } else if (mode === "abort") {
                    req += encodeCmd(["EXEC"]);
                    exp += "-EXECABORT Transaction discarded because of previous errors.\r\n";
                } else {
                    req += encodeCmd(["EXEC"]);
                    exp += `*${perBatch}\r\n` + "+OK\r\n".repeat(perBatch);
                }
                expected += exp;
                sock.write(req);
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

// Mixed round: normal SET, MULTI, n SETs, EXEC, PING — all in one write.
function runMixedConn(port, { batches, perBatch, connId, prefix, timeoutMs = 30000 }) {
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
                let req = encodeCmd(["SET", `${prefix}:n:${connId}:${sent}`, "n"]);
                let exp = "+OK\r\n";
                req += encodeCmd(["MULTI"]);
                exp += "+OK\r\n";
                for (let d = 0; d < perBatch; d++) {
                    req += encodeCmd(["SET", `${prefix}:t:${connId}:${sent}:${d}`, "t"]);
                    exp += "+QUEUED\r\n";
                }
                req += encodeCmd(["EXEC"]);
                exp += `*${perBatch}\r\n` + "+OK\r\n".repeat(perBatch);
                req += encodeCmd(["PING"]);
                exp += "+PONG\r\n";
                expected += exp;
                sock.write(req);
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

async function runRound(port, runner, opts) {
    const t0 = process.hrtime.bigint();
    const results = await Promise.all(
        Array.from({ length: opts.conns }, (_, i) => runner(port, { ...opts, connId: i }))
    );
    const ms = Math.round(Number(process.hrtime.bigint() - t0) / 1e6);
    const ok = results.filter((r) => r.kind === "ok").length;
    const timeouts = results.filter((r) => r.kind === "timeout").length;
    const bad = results.filter((r) => r.kind === "mismatch");
    return {
        ok,
        mismatch: bad.length,
        timeouts,
        partial: results.filter((r) => r.kind === "partial" || r.kind === "closed" || r.kind === "error").length,
        cmds: opts.cmdsPerConn * opts.conns,
        ms,
        kcmds: Math.round((opts.cmdsPerConn * opts.conns) / Math.max(ms, 1)),
        sample: bad.length > 0 ? bad[0].received.slice(0, 200) : undefined,
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

function prodEnv() {
    clearStore();
    configureEviction({ env: "production", policy: "allkeys-lru" });
    const s = snap();
    eq("prod maxKeys=200000", s.maxKeys, 200000);
    eq("prod evictPercent=4%", s.evictPercent, 0.04);
    eq("prod policy=allkeys-lru", s.policy, "allkeys-lru");
}

(async () => {
    const server = RunAsyncTcpServer("127.0.0.1", 18097, { maxClients: 1000 });
    await sleep(150);
    const port = 18097;

    console.log("\n--- Round A: MULTI + 10 SETs + EXEC (100 conns x 20 batches = 24k cmds) ---");
    prodEnv();
    const A = await runRound(port, runTxnConn, {
        conns: 100, batches: 20, perBatch: 10, mode: "exec", prefix: "m:a",
        cmdsPerConn: 20 * 12,
    });
    const a = snap();
    eq("A all conns byte-exact", A.ok, 100);
    eq("A no timeouts", A.timeouts, 0);
    eq("A 20k keys written by EXEC", a.keys, 20000);
    eq("A no eviction below prod limit", a.evicted, 0);
    console.log(`[load] A: ${A.cmds} cmds in ${A.ms}ms = ${A.kcmds}k cmds/s, keys=${a.keys}, pool=${a.poolSize}`);

    console.log("\n--- Round B: MULTI + 200 SETs + EXEC (50 conns x 5 batches = 50.5k cmds) ---");
    prodEnv();
    const B = await runRound(port, runTxnConn, {
        conns: 50, batches: 5, perBatch: 200, mode: "exec", prefix: "m:b",
        cmdsPerConn: 5 * 202,
    });
    const b = snap();
    eq("B all conns byte-exact", B.ok, 50);
    eq("B no timeouts", B.timeouts, 0);
    eq("B 50k keys written by EXEC", b.keys, 50000);
    eq("B no eviction", b.evicted, 0);
    console.log(`[load] B: ${B.cmds} cmds in ${B.ms}ms = ${B.kcmds}k cmds/s, keys=${b.keys}, pool=${b.poolSize}`);

    console.log("\n--- Round C: MULTI + 20 SETs + DISCARD (100 conns x 10 batches = 22k cmds) ---");
    prodEnv();
    const C = await runRound(port, runTxnConn, {
        conns: 100, batches: 10, perBatch: 20, mode: "discard", prefix: "m:c",
        cmdsPerConn: 10 * 22,
    });
    const c = snap();
    eq("C all conns byte-exact", C.ok, 100);
    eq("C no timeouts", C.timeouts, 0);
    eq("C DISCARD wrote nothing", c.keys, 0);
    eq("C no eviction", c.evicted, 0);
    console.log(`[load] C: ${C.cmds} cmds in ${C.ms}ms = ${C.kcmds}k cmds/s, keys=${c.keys} (discarded)`);

    console.log("\n--- Round D: MULTI + bad arity + EXECABORT (50 conns x 5 batches = 3k cmds) ---");
    prodEnv();
    const D = await runRound(port, runTxnConn, {
        conns: 50, batches: 5, perBatch: 10, mode: "abort", prefix: "m:d",
        cmdsPerConn: 5 * 12,
    });
    const d = snap();
    eq("D all conns byte-exact", D.ok, 50);
    eq("D no timeouts", D.timeouts, 0);
    eq("D EXECABORT wrote nothing", d.keys, 0);
    eq("D no eviction", d.evicted, 0);
    console.log(`[load] D: ${D.cmds} cmds in ${D.ms}ms = ${D.kcmds}k cmds/s, keys=${d.keys} (aborted)`);

    console.log("\n--- Round E: mixed SET + MULTI/5 SETs/EXEC + PING (100 conns x 10 batches) ---");
    prodEnv();
    const E = await runRound(port, runMixedConn, {
        conns: 100, batches: 10, perBatch: 5, prefix: "m:e",
        cmdsPerConn: 10 * 9,
    });
    const e = snap();
    eq("E all conns byte-exact", E.ok, 100);
    eq("E no timeouts", E.timeouts, 0);
    eq("E 6k keys (1 direct + 5 txn per batch)", e.keys, 6000);
    eq("E no eviction", e.evicted, 0);
    console.log(`[load] E: ${E.cmds} cmds in ${E.ms}ms = ${E.kcmds}k cmds/s, keys=${e.keys}`);

    await sleep(100);
    const stats = server.stats();
    eq("all clients drained", stats.clients, 0);
    eq("all fds released", stats.monitored, 0);

    const totalCmds = A.cmds + B.cmds + C.cmds + D.cmds + E.cmds;
    const totalMs = A.ms + B.ms + C.ms + D.ms + E.ms;
    console.log(`\n=== LOAD SUMMARY (prod env, transactions) ===`);
    console.log(JSON.stringify({
        config: { maxKeys: 200000, evictPercent: 0.04, policy: "allkeys-lru" },
        rounds: {
            A: { shape: "MULTI+10SET+EXEC", ...A },
            B: { shape: "MULTI+200SET+EXEC", ...B },
            C: { shape: "MULTI+20SET+DISCARD", ...C },
            D: { shape: "MULTI+badArity+EXECABORT", ...D },
            E: { shape: "SET+MULTI+5SET+EXEC+PING", ...E },
        },
        total: { cmds: totalCmds, ms: totalMs, kcmds: Math.round(totalCmds / Math.max(totalMs, 1)) },
        keyspace: snap(),
        stats: { clients: stats.clients, monitored: stats.monitored, commands: stats.commands },
    }, null, 2));
    console.log(`\n=== ${passed} passed, ${failed} failed ===`);

    await new Promise((r) => server.close(r));
    process.exit(failed === 0 ? 0 : 1);
})().catch((e) => { console.error("FATAL", e); process.exit(1); });
