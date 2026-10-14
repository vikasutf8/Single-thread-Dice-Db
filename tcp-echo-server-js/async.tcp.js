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
const { readCmd } = require("./redis-cmd");
const { evalAndRespond, respondError } = require("./eval");
const { DeleteExpiredKeys, configureEviction } = require("./store");

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
// Called on every EPOLLIN hit (socket 'data' event) with whatever bytes
// arrived; drains every complete RedisCmd already buffered.
// ---------------------------------------------------------------------------
function readFromClient(fdComm) {
    const conn = fdComm.socket;

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

        // Same respond path as sync server: PING -> evalPING else evalDefault.
        evalAndRespond(cmd, conn);
        fdComm.leftover = fdComm.leftover.slice(n);
    }

    // Per-fd memory guard: at 10k clients one 100MB leftover kills the box.
    if (fdComm.leftover.length > fdComm.maxBuffer) {
        respondError(new Error("too much pipelined data, closing"), conn);
        conn.destroy();
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
    // tests shrink them, e.g. { maxKeys: 10 }.
    if (opts.maxKeys !== undefined || opts.maxCacheBytes !== undefined) {
        configureEviction({ maxKeys: opts.maxKeys, maxBytes: opts.maxCacheBytes });
    }

    parseIP(host); // Bind step validation (real bind happens in listen)

    // Socket(SOCK_STREAM | NONBLOCK) — one listening socket for all clients.
    const server = net.createServer({ keepAlive: true });

    // EpollCtl(ADD, serverFd, EPOLLIN): libuv registers the listen fd with
    // epoll/kqueue internally. Our visible part: cap concurrent accepts.
    server.maxConnections = maxClients;
    server.on("error", (err) => console.error("Server error:", err.message));

    // EpollWait dispatch: this callback fires once per ready accept (== one
    // EPOLLIN hit on serverFd). Accept() + SetNonBlock + EpollCtl(ADD, cfd).
    server.on("connection", (conn) => {
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

    return {
        server,
        stats: () => ({
            active: concurrentClients,
            accepted: totalAccepted,
            rejected: totalRejected,
            monitored: fdRegistry.size,
            lastCronExecution,
        }),
        close: (cb) => {
            clearInterval(cronTimer);
            clearInterval(statTimer);
            server.close(cb);
        },
    };
}

module.exports = {
    RunAsyncTcpServer,
    EPOLLIN,
    EPOLL_CTL_ADD,
    EPOLL_CTL_DEL,
    DEFAULT_MAX_CLIENTS,
    DEFAULT_CRON_FREQUENCY_MS,
};
