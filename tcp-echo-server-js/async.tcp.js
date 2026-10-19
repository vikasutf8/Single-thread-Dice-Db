"use strict";

// async.tcp.js — Async multiplexed TCP server (epoll mental model, Node port).
//
// Go study you described:
//   RunAsyncTcpServer(maxClients=20k, events []EpollEvent)
//     1. Socket(SOCK_STREAM|NONBLOCK) -> serverFd
//     2. SetNonBlock(serverFd)        -> SetNonBlock
//     3. parseIP + Bind(serverFd, ip:4bytes, port)
//     4. Listen(serverFd, backlog=maxClients)
//     5. EpollCreate1 -> epollFd
//     6. socketServerEvent = EpollEvent{Events: EPOLLIN, Fd: serverFd}
//     7. EpollCtl(epollFd, EPOLL_CTL_ADD, serverFd, &socketServerEvent)
//     8. for { n = EpollWait(epollFd, events); for i<n {
//            if events[i].Fd == serverFd { cfd = Accept(serverFd);
//              concurrent++; SetNonBlock(cfd); EpollCtl(ADD, cfd) }
//            else { FdComm{fd}; readCmd(); on err Close(); else evalAndRespond(PING) }
//        }}
//
// IMPORTANT Node reality:
//   Pure JS cannot call Linux epoll syscalls directly (EpollCreate1 /
//   EpollCtl / EpollWait). Node's libuv already does exactly that for you
//   under the hood (epoll on Linux, kqueue on macOS — which is why this
//   file runs on your darwin machine too). `net.createServer` + `data`
//   events IS the EpollWait dispatch loop: single-threaded, non-blocking,
//   multiplexed over all fds.
//
//   So this file keeps your RESP/PING logic identical (readCmd +
//   evalAndRespond) but restructures main.js into an explicit async
//   multiplexor with:
//     - one accept loop (EpollCtl ADD serverFd)
//     - per-client fd registry (Map, mirrors epoll interest list)
//     - per-client inner read-drain loop (while leftover has full cmd)
//       that breaks on incomplete / disconnect / EXIT
//     - admission control for 10k-20k concurrent clients
//     - backpressure + oversize-buffer guards (mandatory at 10k scale)

const net = require("node:net");
const fs = require("node:fs");
const { readCmd } = require("./redis-cmd");
const { evalAndRespond, respondError } = require("./eval");
const { DeleteExpiredKeys, configureEviction, clearStore } = require("./store");
const aof = require("./aof");
const rdb = require("./rdb");

// --- epoll constants (mirrored for teaching; used as flags in registry) ---
const EPOLLIN = 0x001;
const EPOLL_CTL_ADD = 1;
const EPOLL_CTL_DEL = 2;

const DEFAULT_MAX_CLIENTS = 20000; // your spec: 20k cap
const DEFAULT_MAX_BUFFER_PER_CLIENT = 1 << 20; // 1 MiB leftover cap per fd
const DEFAULT_CRON_FREQUENCY_MS = 1000; // active-expiry tick: 1 s, 1 run (real Redis: 10 Hz)

let concurrentClients = 0;
let totalAccepted = 0;
let totalRejected = 0;
let totalCommands = 0; // all cmds executed, incl. pipelined
let totalBatches = 0; // EPOLLIN hits that carried >=1 complete cmd
let maxBatchDepth = 0; // largest cmds-per-hit seen (pipeline depth proof)
let lastCronExecution = 0; // epoch-ms of the last expiry sweep
let nextFd = 100; // fake fd numbers for logging (real fd hidden inside libuv)

// fd registry == epoll interest list: fd -> FdComm { fd, socket, leftover, addr }
const fdRegistry = new Map();

// ---------------------------------------------------------------------------
// EpollCtl(epollFd, op, fd, event) — emulated over the Map.
// In C/Go this registers kernel interest; here it tracks which sockets we
// are multiplexing. Kept as a function so the Go mapping stays 1:1.
// ---------------------------------------------------------------------------
function EpollCtl(op, fdComm) {
    if (op === EPOLL_CTL_ADD) {
        fdRegistry.set(fdComm.fd, fdComm);
    } else if (op === EPOLL_CTL_DEL) {
        fdRegistry.delete(fdComm.fd);
    }
}

// ---------------------------------------------------------------------------
// SetNonBlock(socket) — Go: syscall.SetNonblock(fd, true).
// Node sockets are ALWAYS non-blocking (libuv). We just apply the options
// that matter at 10k scale: no Nagle delay, keep-alive to detect dead peers.
// ---------------------------------------------------------------------------
function SetNonBlock(socket) {
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 60_000);
}

// parseIP(host) — Go: net.ParseIP -> 4 bytes for Bind. Node does DNS/bind
// inside server.listen; we only validate here so Bind step is visible.
function parseIP(host) {
    if (typeof host !== "string" || host.length === 0) {
        throw new Error(`parseIP: invalid host ${JSON.stringify(host)}`);
    }
    return host;
}

// ---------------------------------------------------------------------------
// readFromClient(fdComm) — inner infinite read loop from your spec:
//   for { cmd = readCmd(buf); on incomplete break&wait-more;
//         on EXIT/quit break&close; on disconnect break&close;
//         else evalAndRespond(PING) }
//
// PIPELINE (not a transaction):
//   N complete RedisCmds already buffered in ONE EPOLLIN hit are executed
//   strictly one-by-one, in order, with NO atomicity / rollback / queueing.
//   If cmd #2 fails, #1 stays applied and #3 still runs. Replies are
//   concatenated back-to-back on the wire so the client gets them in a
//   single round trip: 1 request (N cmds, 1 write) -> 1 response (N replies).
//   No PIPELINE/MULTI/EXEC verb exists on the server — batching is purely
//   a transport shape the client chooses.
//
//   conn.cork()/uncork() wraps the drain so the N conn.write() calls made
//   by evalAndRespond coalesce into one TCP flush ("return all in once").
//   Called on every EPOLLIN hit (socket 'data' event) with whatever bytes
//   arrived; drains every complete RedisCmd already buffered.
// ---------------------------------------------------------------------------
function readFromClient(fdComm) {
    const conn = fdComm.socket;
    let batchDepth = 0;

    // Cork: buffer kernel writes until uncork -> single flush per hit.
    if (typeof conn.cork === "function") conn.cork();
    try {
        while (fdComm.leftover.length > 0) {
            const { cmd, n, err } = readCmd(fdComm.leftover);

            if (err) {
                // Incomplete RESP frame: keep bytes, break, wait for next EPOLLIN.
                if (err.message.startsWith("incomplete")) break;
                // Bare Enter from nc: consume line, keep connection open.
                if (err.message === "empty line") {
                    fdComm.leftover = fdComm.leftover.slice(n);
                    if (fdComm.leftover.length === 0) break;
                    continue;
                }
                // Protocol error: reply -ERR, drop bad bytes, keep connection.
                // Pipeline keeps going: error is one reply in the batch.
                respondError(err, conn);
                fdComm.leftover = n > 0 ? fdComm.leftover.slice(n) : Buffer.alloc(0);
                if (fdComm.leftover.length === 0) break;
                continue;
            }

            // EXIT / QUIT breaks the inner loop and closes this fd (your spec).
            const verb = String(cmd.Cmd).toUpperCase();
            if (verb === "EXIT" || verb === "QUIT") {
                conn.end();
                break;
            }

            // Pipeline step: execute now, in order, no queueing.
            evalAndRespond(cmd, conn);
            totalCommands++;
            batchDepth++;
            fdComm.leftover = fdComm.leftover.slice(n);
        }

        if (batchDepth > 0) {
            totalBatches++;
            if (batchDepth > maxBatchDepth) maxBatchDepth = batchDepth;
        }

        // Per-fd memory guard: at 10k clients one 100MB leftover kills the box.
        if (fdComm.leftover.length > fdComm.maxBuffer) {
            respondError(new Error("too much pipelined data, closing"), conn);
            conn.destroy();
        }
    } finally {
        // Single flush: all N replies from this hit go out together.
        if (typeof conn.uncork === "function") conn.uncork();
    }
}

function closeClient(fdComm, reason) {
    if (!fdRegistry.has(fdComm.fd)) return; // already removed (double close)
    EpollCtl(EPOLL_CTL_DEL, fdComm);
    concurrentClients--;
    try { fdComm.socket.destroy(); } catch { /* already gone */ }
    console.log(
        `Client gone (fd=${fdComm.fd} ${reason}) | Active: ${concurrentClients}`
    );
}

// ---------------------------------------------------------------------------
// RunAsyncTcpServer(host, port, opts) — your requested entry point.
// Mirrors: Socket + Bind + Listen + EpollCreate1 + EpollWait dispatch loop.
// ---------------------------------------------------------------------------
function RunAsyncTcpServer(host, port, opts = {}) {
    const maxClients = opts.maxClients ?? DEFAULT_MAX_CLIENTS;
    const maxBuffer = opts.maxBufferPerClient ?? DEFAULT_MAX_BUFFER_PER_CLIENT;
    const backlog = opts.backlog ?? maxClients; // Listen(fd, backlog=maxClients)
    const cronFrequency = opts.cronFrequency ?? DEFAULT_CRON_FREQUENCY_MS;

    // Eviction thresholds: defaults 20k keys / 10 MB; terminal demos and
    // tests shrink them, e.g. { maxKeys: 10 }. policy: "allkeys-lru" | "allkeys-random".
    if (opts.maxKeys !== undefined || opts.maxCacheBytes !== undefined || opts.policy !== undefined) {
        configureEviction({ maxKeys: opts.maxKeys, maxBytes: opts.maxCacheBytes, policy: opts.policy });
    }

    // --- Persistence (AOF + RDB), opt-in ---------------------------------
    // RDB snapshot first, then AOF replay on top (AOF holds the newer tail).
    // AOF append stays OFF until recovery is done so replayed cmds are not
    // re-logged. Defaults keep every existing test file-free (no AOF/RDB
    // unless the caller passes aofEnabled / rdbLoad).
    const aofPath = opts.aofPath ?? aof.DEFAULT_AOF_PATH; // ./AOF/dice-master.aof
    const rdbPath = opts.rdbPath ?? rdb.DEFAULT_RDB_PATH; // ./RDB/dump.rdb
    const aofOn = opts.aofEnabled ?? false;
    const NOREPLY = { write() {} }; // replay sink: no socket to answer
    aof.configure({ path: aofPath, enabled: false });
    rdb.configure({ path: rdbPath });
    if (opts.rdbLoad ?? false) {
        try {
            const r = rdb.loadRDB(rdbPath);
            console.log(`[persist] rdb loaded=${r.loaded} skipped=${r.skipped ?? 0}`);
        } catch (err) {
            console.error(`[persist] rdb load failed: ${err.message} (starting empty)`);
            clearStore();
        }
    }
    if (aofOn) {
        aof.replayAOF(aofPath, (cmd) => evalAndRespond(cmd, NOREPLY));
        aof.setEnabled(true);
        console.log(`[persist] aof enabled file=${aofPath}`);
    }

    parseIP(host); // Bind step validation (real bind happens in listen)

    // Socket(SOCK_STREAM | NONBLOCK) — one listening socket for all clients.
    const server = net.createServer({ keepAlive: true });

    // Track shutdown state
    let shuttingDown = false;
    let shutdownPromise = null;
    let shutdownResolve = null;

    // EpollCtl(ADD, serverFd, EPOLLIN): libuv registers the listen fd with
    // epoll/kqueue internally. Our visible part: cap concurrent accepts.
    server.maxConnections = maxClients;
    server.on("error", (err) => console.error("Server error:", err.message));

    // EpollWait dispatch: this callback fires once per ready accept (== one
    // EPOLLIN hit on serverFd). Accept() + SetNonBlock + EpollCtl(ADD, cfd).
    server.on("connection", (conn) => {
        // If shutting down, reject new connections immediately
        if (shuttingDown) {
            try { conn.write("-ERR server shutting down\r\n"); } catch { /* ignore */ }
            conn.destroy();
            return;
        }

        // Admission control — beyond maxClients, reject immediately so one
        // flood cannot OOM the other 19,999 connections.
        if (concurrentClients >= maxClients) {
            totalRejected++;
            try { conn.write("-ERR max clients reached\r\n"); } catch { /* ignore */ }
            conn.destroy();
            return;
        }

        SetNonBlock(conn); // == SetNonblock(cfd, true)

        const fdComm = {
            fd: nextFd++,                       // emulated fd number (logging)
            socket: conn,                       // io.ReadWriter for this fd
            leftover: Buffer.alloc(0),          // incomplete RESP bytes
            addr: `${conn.remoteAddress}:${conn.remotePort}`,
            events: EPOLLIN,
            maxBuffer,
        };
        EpollCtl(EPOLL_CTL_ADD, fdComm); // monitor this fd for EPOLLIN
        concurrentClients++;
        totalAccepted++;

        console.log(
            `Accepted fd=${fdComm.fd} from ${fdComm.addr} | Active: ${concurrentClients}`
        );

        // EPOLLIN on client fd -> append bytes, run inner read-drain loop.
        // Backpressure: pause reading while kernel send buffer is full.
        conn.on("data", (chunk) => {
            fdComm.leftover = Buffer.concat([fdComm.leftover, chunk]);
            readFromClient(fdComm);
            // Backpressure: pause reading while the kernel send buffer is full.
            if (conn.writableLength > 1 << 20) {
                conn.pause();
                conn.once("drain", () => conn.resume());
            }
        });

        conn.on("close", () => closeClient(fdComm, "disconnect"));
        conn.on("error", (err) => {
            console.error(`fd=${fdComm.fd} socket error:`, err.message);
            closeClient(fdComm, "error");
        });
    });

    // Bind(ip:4bytes, port) + Listen(serverFd, backlog).
    server.listen({ host, port, backlog }, () => {
        console.log(`Async TCP server listening on ${host}:${port}`);
        console.log(`Multiplexing up to ${maxClients} concurrent clients (backlog=${backlog})`);
        console.log("Epoll model: one event loop, non-blocking fds, EPOLLIN dispatch.");
    });

    // Stats line so you can watch the 10k ramp.
    const statTimer = setInterval(() => {
        console.log(
            `[stats] active=${concurrentClients} accepted=${totalAccepted} ` +
            `rejected=${totalRejected} monitored_fds=${fdRegistry.size}`
        );
    }, 30_000);
    statTimer.unref();

    // Active-expiry cron (Go: serverCron → activeExpireCycle).
    // Same single thread as the EPOLLIN dispatch: each tick sweeps expired
    // keys via core DeleteExpiredKeys(), then stamps lastCronExecution.
    const cronTimer = setInterval(() => {
        const deleted = DeleteExpiredKeys();
        lastCronExecution = Date.now();
        if (deleted > 0) console.log(`[cron] expired ${deleted} keys`);
    }, cronFrequency);
    cronTimer.unref();

    // Graceful shutdown function
    async function gracefulShutdown({ saveRDB = false, flushAOF = true, timeoutMs = 10000 } = {}) {
        if (shutdownPromise) return shutdownPromise;

        shuttingDown = true;
        console.log("[shutdown] initiating graceful shutdown...");

        shutdownPromise = new Promise(async (resolve) => {
            shutdownResolve = resolve;

            // 1. Stop accepting new connections
            console.log("[shutdown] stopping new connections...");
            server.close();

            // 2. Wait for existing connections to finish or timeout
            if (fdRegistry.size > 0) {
                console.log(`[shutdown] waiting for ${fdRegistry.size} active connections (timeout ${timeoutMs}ms)...`);
                
                const waitStart = Date.now();
                while (fdRegistry.size > 0 && Date.now() - waitStart < timeoutMs) {
                    // Give clients time to finish their current requests
                    await new Promise(r => setTimeout(r, 100));
                }

                // Force close remaining connections
                if (fdRegistry.size > 0) {
                    console.log(`[shutdown] force closing ${fdRegistry.size} remaining connections...`);
                    for (const fdComm of fdRegistry.values()) {
                        try { 
                            fdComm.socket.write("-ERR server shutting down\r\n"); 
                            fdComm.socket.end();
                        } catch { /* ignore */ }
                        closeClient(fdComm, "shutdown");
                    }
                }
            }

            // 3. Flush AOF if enabled
            if (flushAOF && aofOn) {
                console.log("[shutdown] flushing AOF...");
                try {
                    if (fs.existsSync(aofPath)) {
                        const fd = fs.openSync(aofPath, "r");
                        fs.fsyncSync(fd); // force pending appends to disk
                        fs.closeSync(fd);
                        console.log("[shutdown] AOF flushed");
                    } else {
                        console.log("[shutdown] AOF file absent (nothing appended)");
                    }
                } catch (err) {
                    console.error("[shutdown] AOF flush failed:", err.message);
                }
            }

            // 4. Save RDB if requested
            if (saveRDB) {
                console.log("[shutdown] saving RDB snapshot...");
                try {
                    const result = await rdb.bgEvalRDB(rdbPath);
                    if (result.ok) {
                        console.log(`[shutdown] RDB saved: keys=${result.keys}`);
                    } else {
                        console.error("[shutdown] RDB save failed:", result.reason);
                    }
                } catch (err) {
                    console.error("[shutdown] RDB save error:", err.message);
                }
            }

            // 5. Clean up timers
            clearInterval(cronTimer);
            clearInterval(statTimer);

            console.log("[shutdown] graceful shutdown complete");
            resolve();
        });

        return shutdownPromise;
    }

    return {
        server,
        stats: () => ({
            active: concurrentClients,
            accepted: totalAccepted,
            rejected: totalRejected,
            monitored: fdRegistry.size,
            commands: totalCommands,
            batches: totalBatches,
            maxBatchDepth,
            persist: {
                aof: aofPath,
                aofEnabled: aofOn,
                aofRewriting: aof.isRewriting(),
                rdb: rdbPath,
                rdbSaving: rdb.isSaving(),
                lastSave: rdb.lastSave(),
            },
            lastCronExecution,
        }),
        close: (cb) => {
            clearInterval(cronTimer);
            clearInterval(statTimer);
            server.close(cb);
        },
        gracefulShutdown,
    };
}

// Setup signal handlers for graceful shutdown (optional, call from main.js)
// Returns the server handle so you can await gracefulShutdown() on signal
function setupGracefulShutdown(serverHandle, opts = {}) {
    const { saveRDB = false, flushAOF = true, timeoutMs = 10000 } = opts;
    
    const handleSignal = async (signal) => {
        console.log(`[shutdown] received ${signal}, starting graceful shutdown...`);
        await serverHandle.gracefulShutdown({ saveRDB, flushAOF, timeoutMs });
        console.log("[shutdown] exiting");
        process.exit(0);
    };

    process.on("SIGINT", () => handleSignal("SIGINT"));
    process.on("SIGTERM", () => handleSignal("SIGTERM"));
    
    // Handle uncaught exceptions gracefully
    process.on("uncaughtException", async (err) => {
        console.error("[shutdown] uncaught exception:", err);
        await serverHandle.gracefulShutdown({ saveRDB, flushAOF, timeoutMs });
        process.exit(1);
    });
    
    return serverHandle;
}

module.exports = {
    RunAsyncTcpServer,
    setupGracefulShutdown,
    EPOLLIN,
    EPOLL_CTL_ADD,
    EPOLL_CTL_DEL,
    DEFAULT_MAX_CLIENTS,
    DEFAULT_CRON_FREQUENCY_MS,
};
