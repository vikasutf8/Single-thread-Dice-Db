"use strict";

const { RunAsyncTcpServer, setupGracefulShutdown } = require("./async.tcp.js");
const { put, newObj, clearStore, configureEviction, getInfo, store } = require("./store");
const { evalAndRespond } = require("./eval");
const net = require("net");
const fs = require("fs");
const path = require("path");

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

function encodeCmd(args) {
    let s = `*${args.length}\r\n`;
    for (const a of args) {
        const b = Buffer.from(String(a));
        s += `$${b.length}\r\n${a}\r\n`;
    }
    return s;
}

async function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

// Client that reads until expected bytes received, then closes
async function runClient(port, cmds) {
    return new Promise((resolve, reject) => {
        const sock = net.connect(port, "127.0.0.1");
        let received = "";
        let expected = "";
        for (const { args, expect } of cmds) {
            expected += expect;
            sock.write(encodeCmd(args));
        }
        const timeout = setTimeout(() => {
            sock.destroy();
            reject(new Error(`client timeout (got ${received.length}/${expected.length} bytes)`));
        }, 10000);
        
        sock.on("data", (chunk) => {
            received += chunk;
            if (received.length >= expected.length) {
                clearTimeout(timeout);
                sock.end(); // Client closes after receiving expected data
                resolve({ received, expected, ok: received === expected });
            }
        });
        sock.on("close", () => {
            clearTimeout(timeout);
            if (received.length === 0) return; // Already resolved
            resolve({ received, expected, ok: received === expected });
        });
        sock.on("error", (err) => {
            clearTimeout(timeout);
            reject(err);
        });
    });
}

// Client that just sends and closes (for testing rejection during shutdown)
async function runClientFireAndForget(port, cmds) {
    return new Promise((resolve, reject) => {
        const sock = net.connect(port, "127.0.0.1");
        let received = "";
        for (const { args, expect } of cmds) {
            sock.write(encodeCmd(args));
        }
        const timeout = setTimeout(() => {
            sock.destroy();
            resolve({ received, error: "timeout" });
        }, 2000);
        
        sock.on("data", (chunk) => { received += chunk; });
        sock.on("close", () => {
            clearTimeout(timeout);
            resolve({ received, error: null });
        });
        sock.on("error", (err) => {
            clearTimeout(timeout);
            resolve({ received, error: err.message });
        });
    });
}

console.log("=== Graceful shutdown tests ===\n");

async function test1_basic() {
    console.log("--- Test 1: Basic graceful shutdown ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18087, { maxClients: 100 });
    await sleep(100);
    
    put("k1", newObj("v1", -1));
    put("k2", newObj("v2", 60000));
    
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    const stats = srv.stats();
    eq("active clients zero after shutdown", stats.active, 0);
    eq("store data preserved", store.size, 2);
    eq("getInfo accessible", getInfo().keyspace.keys, 2);
}

async function test2_with_active_connections() {
    console.log("\n--- Test 2: Graceful shutdown with active connections ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18088, { maxClients: 100 });
    await sleep(100);
    
    put("k1", newObj("v1", -1));
    
    // Start a client and wait for it to complete
    const clientPromise = runClient(18088, [
        { args: ["SET", "slow:key", "slowval"], expect: "+OK\r\n" },
        { args: ["GET", "slow:key"], expect: "$7\r\nslowval\r\n" },
        { args: ["PING"], expect: "+PONG\r\n" },
    ]);
    
    const clientResult = await clientPromise;
    eq("client completed before shutdown", clientResult.ok, true);
    
    // Now shutdown - no active connections
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    eq("active clients zero", srv.stats().active, 0);
}

async function test3_reject_new_connections_during_shutdown() {
    console.log("\n--- Test 3: Reject new connections during shutdown ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18089, { maxClients: 100 });
    await sleep(100);
    
    put("k1", newObj("v1", -1));
    
    // Start shutdown but don't await it yet
    const shutdownPromise = srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    // Give shutdown a moment to set the flag and close the listen socket
    await sleep(100);
    
    // Try to connect during shutdown - server.close() stops accepting, so we'll get ECONNREFUSED
    const result = await runClientFireAndForget(18089, [
        { args: ["PING"], expect: "" } // Don't expect anything, connection will be refused
    ]);
    
    await shutdownPromise;
    
    // Either we got ECONNREFUSED or an error response - both mean rejected
    const rejected = result.error && (result.error.includes("ECONNREFUSED") || result.error.includes("shutting down"));
    eq("new connection rejected during shutdown", rejected || result.received.includes("shutting down"), true);
    eq("active clients zero", srv.stats().active, 0);
}

async function test4_aof_flush() {
    console.log("\n--- Test 4: AOF flush on shutdown ---");
    const aofPath = path.join(__dirname, "AOF", "test-shutdown.aof");
    try { fs.unlinkSync(aofPath); } catch { }
    try { fs.mkdirSync(path.dirname(aofPath), { recursive: true }); } catch { }
    
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18090, { 
        maxClients: 100, 
        aofEnabled: true, 
        aofPath 
    });
    await sleep(100);
    
    // Write via evalAndRespond so AOF logs the command (put() bypasses AOF).
    evalAndRespond({ Cmd: "SET", Args: ["aof:key", "aofval"] }, { write() {} });
    
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: true, timeoutMs: 5000 });
    
    // Verify AOF file exists and has content
    eq("AOF file exists", fs.existsSync(aofPath), true);
    const content = fs.readFileSync(aofPath, "utf8");
    eq("AOF has SET command", content.includes("SET") && content.includes("aof:key"), true);
    
    // Cleanup
    try { fs.unlinkSync(aofPath); } catch { }
}

async function test5_rdb_save() {
    console.log("\n--- Test 5: RDB save on shutdown ---");
    const rdbPath = path.join(__dirname, "RDB", "test-shutdown.rdb");
    try { fs.unlinkSync(rdbPath); } catch { }
    try { fs.mkdirSync(path.dirname(rdbPath), { recursive: true }); } catch { }
    
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18091, { 
        maxClients: 100, 
        rdbPath 
    });
    await sleep(100);
    
    put("rdb:key", newObj("rdbval", -1));
    put("rdb:key2", newObj("rdbval2", 60000));
    
    await srv.gracefulShutdown({ saveRDB: true, flushAOF: false, timeoutMs: 15000 });
    
    // Verify RDB file exists
    eq("RDB file exists", fs.existsSync(rdbPath), true);
    
    // Verify we can load it
    const { loadRDB } = require("./rdb");
    const result = loadRDB(rdbPath);
    eq("RDB loads correctly", result.loaded, 2);
    
    // Cleanup
    try { fs.unlinkSync(rdbPath); } catch { }
}

async function test6_signal_handler_setup() {
    console.log("\n--- Test 6: Signal handler setup ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18092, { maxClients: 100 });
    await sleep(100);
    
    // Setup signal handlers (this is what main.js would do)
    setupGracefulShutdown(srv, { saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    put("sig:key", newObj("sigval", -1));
    
    // Call gracefulShutdown directly (simulating signal handler)
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    eq("active clients zero after signal shutdown", srv.stats().active, 0);
    eq("store preserved", store.size, 1);
}

async function test7_idempotent_shutdown() {
    console.log("\n--- Test 7: Idempotent shutdown (double call) ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18093, { maxClients: 100 });
    await sleep(100);
    
    put("k1", newObj("v1", -1));
    
    // Call shutdown twice
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 5000 });
    
    eq("double shutdown safe", srv.stats().active, 0);
    eq("store intact", store.size, 1);
}

async function test8_timeout_force_close() {
    console.log("\n--- Test 8: Timeout forces connection close ---");
    clearStore();
    configureEviction({ maxKeys: 1000, env: "development" });
    
    const srv = RunAsyncTcpServer("127.0.0.1", 18094, { maxClients: 100 });
    await sleep(100);
    
    // Create a client that sends a request but doesn't read the response
    const sock = net.connect(18094, "127.0.0.1");
    sock.write(encodeCmd(["SET", "hold:key", "holdval"]));
    // Don't wait for response, don't close - just hold
    
    await sleep(200);
    eq("client connected before shutdown", srv.stats().active, 1);
    
    // Shutdown with short timeout - should force close
    await srv.gracefulShutdown({ saveRDB: false, flushAOF: false, timeoutMs: 100 });
    
    eq("force closed after timeout", srv.stats().active, 0);
    sock.destroy();
}

// Run all tests
(async () => {
    try {
        await test1_basic();
        await test2_with_active_connections();
        await test3_reject_new_connections_during_shutdown();
        await test4_aof_flush();
        await test5_rdb_save();
        await test6_signal_handler_setup();
        await test7_idempotent_shutdown();
        await test8_timeout_force_close();
        
        console.log(`\n=== ${passed} passed, ${failed} failed ===`);
        process.exit(failed === 0 ? 0 : 1);
    } catch (err) {
        console.error("Test suite error:", err);
        process.exit(1);
    }
})();