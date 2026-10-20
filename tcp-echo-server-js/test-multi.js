"use strict";

// test-multi.js — Redis transactions: MULTI / EXEC / DISCARD.
// Covers per-client state (queue, isTxn, txnDirty), queueing, EXEC reply arrays,
// DISCARD, EXECABORT, runtime errors inside EXEC, client isolation, and the
// wire shape over TCP.

const net = require("node:net");
const { RunAsyncTcpServer } = require("./async.tcp.js");
const { Decode } = require("./resp-decoder.js");
const { evalAndRespond, executeCommand, executeCmdToBuffer } = require("./eval.js");
const { clearStore, get } = require("./store.js");

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

function eq(a, b) {
    return JSON.stringify(a) === JSON.stringify(b);
}

// In-memory client with the same state shape async.tcp.js creates.
function newClient() {
    const writes = [];
    return {
        writes,
        cqueue: [],
        isTxn: false,
        txnDirty: false,
        executing: false,
        write(bytes) {
            writes.push(Buffer.isBuffer(bytes) ? bytes.toString() : String(bytes));
            return true;
        },
        last() { return this.writes[this.writes.length - 1]; },
        all() { return this.writes.join(""); },
        reset() { this.writes.length = 0; },
    };
}

const C = {
    multi: () => ({ Cmd: "MULTI", Args: [] }),
    exec: () => ({ Cmd: "EXEC", Args: [] }),
    discard: () => ({ Cmd: "DISCARD", Args: [] }),
    set: (k, v) => ({ Cmd: "SET", Args: [k, v] }),
    get: (k) => ({ Cmd: "GET", Args: [k] }),
    ttl: (k) => ({ Cmd: "TTL", Args: [k] }),
    del: (...keys) => ({ Cmd: "DEL", Args: keys }),
    incr: (k) => ({ Cmd: "INCR", Args: [k] }),
    expire: (k, s) => ({ Cmd: "EXPIRE", Args: [k, String(s)] }),
    ping: (msg) => ({ Cmd: "PING", Args: msg === undefined ? [] : [msg] }),
    info: () => ({ Cmd: "INFO", Args: [] }),
};

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

// ---------------------------------------------------------------------------
// Direct tests: evalAndRespond against the in-memory client (no socket).
// ---------------------------------------------------------------------------
function directTests() {
    // 1. MULTI starts the transaction.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        ok("multi +OK", c.last() === "+OK\r\n");
        ok("multi sets isTxn", c.isTxn === true);
        ok("multi queue empty", c.cqueue.length === 0);
    }

    // 2. Commands are queued, not executed.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("k1", "v1"), c);
        evalAndRespond(C.get("k1"), c);
        ok("queue SET +QUEUED", c.writes[1] === "+QUEUED\r\n");
        ok("queue GET +QUEUED", c.writes[2] === "+QUEUED\r\n");
        ok("queue holds 2 cmds", c.cqueue.length === 2, `cqueue=${c.cqueue.length}`);
        ok("no write before EXEC", get("k1") === null);
    }

    // 3. EXEC returns *N array in order and applies everything.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("k2", "v2"), c);
        evalAndRespond(C.get("k2"), c);
        evalAndRespond(C.exec(), c);
        ok("exec array shape", c.last() === "*2\r\n+OK\r\n$2\r\nv2\r\n", JSON.stringify(c.last()));
        ok("exec applied SET", get("k2") !== null && get("k2").value === "v2");
        ok("exec resets isTxn", c.isTxn === false && c.cqueue.length === 0);
    }

    // 4. Empty transaction -> *0.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.exec(), c);
        ok("empty exec *0", c.last() === "*0\r\n", JSON.stringify(c.last()));
    }

    // 5. DISCARD drops the queue, keyspace untouched.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("dk", "dv"), c);
        evalAndRespond(C.discard(), c);
        ok("discard +OK", c.last() === "+OK\r\n");
        ok("discard clears queue", c.cqueue.length === 0 && c.isTxn === false);
        ok("discard leaves keyspace", get("dk") === null);
        evalAndRespond(C.exec(), c);
        ok("exec after discard errors", c.last() === "-ERR EXEC without MULTI\r\n", JSON.stringify(c.last()));
    }

    // 6. EXEC / DISCARD without MULTI.
    {
        const c = newClient();
        evalAndRespond(C.exec(), c);
        ok("exec without multi", c.last() === "-ERR EXEC without MULTI\r\n");
        evalAndRespond(C.discard(), c);
        ok("discard without multi", c.last() === "-ERR DISCARD without MULTI\r\n");
    }

    // 7. Nested MULTI errors but does not abort the transaction.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("nk", "nv"), c);
        evalAndRespond(C.multi(), c);
        ok("nested multi error", c.last() === "-ERR MULTI calls can not be nested\r\n", JSON.stringify(c.last()));
        ok("nested multi keeps txn", c.isTxn === true && c.cqueue.length === 1);
        evalAndRespond(C.exec(), c);
        ok("nested multi exec applies", get("nk") !== null && get("nk").value === "nv");
    }

    // 8. Unknown command at queue time -> dirty -> EXECABORT, nothing applied.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("ab", "1"), c);
        evalAndRespond({ Cmd: "NOPE", Args: [] }, c);
        ok("unknown queue error", c.last().startsWith("-ERR unknown command"), JSON.stringify(c.last()));
        ok("unknown marks dirty", c.txnDirty === true);
        evalAndRespond(C.exec(), c);
        ok("dirty exec abort reply", c.last() === "-EXECABORT Transaction discarded because of previous errors.\r\n", JSON.stringify(c.last()));
        ok("dirty exec applies nothing", get("ab") === null);
        ok("dirty exec resets state", c.isTxn === false && c.cqueue.length === 0 && c.txnDirty === false);
    }

    // 9. Wrong arity at queue time -> dirty -> EXECABORT.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond({ Cmd: "SET", Args: ["only-key"] }, c);
        ok("arity queue error", c.last() === "-ERR wrong number of arguments for 'set' command\r\n", JSON.stringify(c.last()));
        evalAndRespond(C.exec(), c);
        ok("arity exec abort", c.last().startsWith("-EXECABORT"));
    }

    // 10. Runtime error is an array element; siblings still run.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.set("txt", "abc"), c);
        c.reset();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("runok", "1"), c);
        evalAndRespond(C.incr("txt"), c); // WRONGTYPE-ish: not an integer
        evalAndRespond(C.get("runok"), c);
        evalAndRespond(C.exec(), c);
        const raw = c.last();
        ok("runtime error in array", raw.includes("-ERR value is not an integer"), JSON.stringify(raw));
        ok("runtime error keeps siblings applied", get("runok") !== null && get("runok").value === "1");
        ok("runtime error leaves bad key", get("txt") !== null && get("txt").value === "abc");
    }

    // 11. Mixed reply types keep order inside EXEC.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("ord", "5"), c);
        evalAndRespond(C.incr("ord"), c);
        evalAndRespond(C.ttl("ord"), c);
        evalAndRespond(C.get("ord"), c);
        evalAndRespond(C.exec(), c);
        ok("exec mixed replies", c.last() === "*4\r\n+OK\r\n:6\r\n:-1\r\n$1\r\n6\r\n", JSON.stringify(c.last()));
    }

    // 12. INFO can be queued and runs at EXEC.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.info(), c);
        evalAndRespond(C.exec(), c);
        ok("exec INFO array", c.last().startsWith("*1\r\n$"), JSON.stringify(c.last().slice(0, 20)));
    }

    // 13. Per-client isolation: one client's MULTI affects nobody else.
    {
        clearStore();
        const a = newClient();
        const b = newClient();
        evalAndRespond(C.multi(), a);
        evalAndRespond(C.set("iso-a", "a"), a);
        evalAndRespond(C.set("iso-b", "b"), b); // executes immediately
        ok("isolation B executed", get("iso-b") !== null && get("iso-b").value === "b");
        ok("isolation A still queued", get("iso-a") === null);
        ok("isolation B not in txn", b.isTxn === false && b.writes[0] === "+OK\r\n");
        evalAndRespond(C.get("iso-b"), b);
        ok("isolation B reads own value", b.last() === "$1\r\nb\r\n");
        evalAndRespond(C.exec(), a);
        ok("isolation A applies at exec", get("iso-a") !== null && get("iso-a").value === "a");
        ok("isolation B untouched by A exec", b.isTxn === false && b.cqueue.length === 0);
    }

    // 14. executeCommand returns bytes without writing anything.
    {
        const c = newClient();
        const out = executeCommand(C.ping(), c);
        ok("executeCommand returns Buffer", Buffer.isBuffer(out) && out.toString() === "+PONG\r\n", JSON.stringify(out.toString()));
        ok("executeCommand no write", c.writes.length === 0);
        const out2 = executeCmdToBuffer(C.ping("hi"), c);
        ok("executeCmdToBuffer alias", out2.toString() === "$2\r\nhi\r\n");
    }

    // 15. Transaction is reusable after EXEC and after DISCARD; dirty clears.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("r1", "1"), c);
        evalAndRespond(C.exec(), c);
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("r2", "2"), c);
        evalAndRespond(C.discard(), c);
        evalAndRespond(C.multi(), c);
        evalAndRespond({ Cmd: "NOPE", Args: [] }, c); // dirty again
        evalAndRespond(C.discard(), c);               // discard resets dirty
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("r3", "3"), c);
        evalAndRespond(C.exec(), c);
        ok("reuse after exec", get("r1") !== null);
        ok("reuse after discard", get("r2") === null);
        ok("reuse after dirty+discard", get("r3") !== null && c.last() === "*1\r\n+OK\r\n");
    }

    // 16. Case-insensitive verbs and queued lowercase commands.
    {
        clearStore();
        const c = newClient();
        evalAndRespond({ Cmd: "multi", Args: [] }, c);
        evalAndRespond({ Cmd: "set", Args: ["ci", "1"] }, c);
        evalAndRespond({ Cmd: "exec", Args: [] }, c);
        ok("case-insensitive txn", get("ci") !== null && c.last() === "*1\r\n+OK\r\n");
    }

    // 17. DEL / EXPIRE queue-time arity validated.
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond({ Cmd: "DEL", Args: [] }, c);
        ok("del arity error", c.last() === "-ERR wrong number of arguments for 'del' command\r\n");
        evalAndRespond(C.exec(), c);
        ok("del arity abort", c.last().startsWith("-EXECABORT"));
    }

    // 18. QUEUED reply does not leak command output (SET returns no OK yet).
    {
        clearStore();
        const c = newClient();
        evalAndRespond(C.multi(), c);
        evalAndRespond(C.set("leak", "x"), c);
        ok("queued single reply", c.all() === "+OK\r\n+QUEUED\r\n", JSON.stringify(c.all()));
    }
}

// ---------------------------------------------------------------------------
// Wire tests over TCP.
// ---------------------------------------------------------------------------
const R = {
    multi: () => "*1\r\n$5\r\nMULTI\r\n",
    exec: () => "*1\r\n$4\r\nEXEC\r\n",
    discard: () => "*1\r\n$7\r\nDISCARD\r\n",
    ping: () => "*1\r\n$4\r\nPING\r\n",
    set: (k, v) => `*3\r\n$3\r\nSET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n$${Buffer.byteLength(v)}\r\n${v}\r\n`,
    get: (k) => `*2\r\n$3\r\nGET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n`,
    unknown: (name) => `*1\r\n$${Buffer.byteLength(name)}\r\n${name}\r\n`,
    badSet: (k) => `*2\r\n$3\r\nSET\r\n$${Buffer.byteLength(k)}\r\n${k}\r\n`,
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

// Persistent TCP client: accumulates bytes, consumes exactly N decoded replies
// per send (safe for multiple sequential sends on one connection).
function makeConn(port) {
    const socket = net.connect(port, "127.0.0.1");
    let buf = Buffer.alloc(0);
    socket.on("data", (d) => { buf = Buffer.concat([buf, d]); });
    const ready = new Promise((res, rej) => {
        socket.once("connect", res);
        socket.once("error", rej);
    });
    async function send(batch, n, timeoutMs = 2000) {
        await ready;
        socket.write(batch);
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
            let cur = buf;
            const out = [];
            while (out.length < n) {
                const { data, n: used, err } = Decode(cur);
                if (err) break;
                out.push(data);
                cur = cur.slice(used);
            }
            if (out.length === n) {
                buf = cur;
                return out;
            }
            await new Promise((r) => setTimeout(r, 5));
        }
        throw new Error(`timeout waiting ${n} replies, buffered=${JSON.stringify(buf.toString())}`);
    }
    return {
        send,
        end: () => socket.end(),
        destroy: () => socket.destroy(),
        buffered: () => buf.toString(),
    };
}

async function wireTests() {
    // 19. One round trip: MULTI SET GET EXEC.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.multi() + R.set("wk1", "wv1") + R.get("wk1") + R.exec(), 4);
        ok("wire multi reply", replies[0] === "OK");
        ok("wire queued SET", replies[1] === "QUEUED");
        ok("wire queued GET", replies[2] === "QUEUED");
        ok("wire exec array", eq(replies[3], ["OK", "wv1"]), JSON.stringify(replies[3]));
        c.end();
    });

    // 20. Pipeline around transaction: PING MULTI PING EXEC PING.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.ping() + R.multi() + R.ping() + R.exec() + R.ping(), 5);
        ok("wire around[0] PONG", replies[0] === "PONG");
        ok("wire around[1] OK", replies[1] === "OK");
        ok("wire around[2] QUEUED", replies[2] === "QUEUED");
        ok("wire around[3] exec PONG array", eq(replies[3], ["PONG"]), JSON.stringify(replies[3]));
        ok("wire around[4] PONG", replies[4] === "PONG");
        c.end();
    });

    // 21. DISCARD over the wire: queued SET never applies.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.multi() + R.set("wk2", "x") + R.discard() + R.get("wk2"), 4);
        ok("wire discard OK", replies[0] === "OK");
        ok("wire discard queued", replies[1] === "QUEUED");
        ok("wire discard reply", replies[2] === "OK");
        ok("wire discard key absent", replies[3] === null, JSON.stringify(replies[3]));
        c.end();
    });

    // 22. Queue-time error over the wire -> EXECABORT.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.multi() + R.set("wk3", "x") + R.unknown("BOGUS") + R.exec(), 4);
        ok("wire unknown error", replies[2] instanceof Error, String(replies[2]));
        ok("wire exec abort", replies[3] instanceof Error && String(replies[3].message).startsWith("EXECABORT"), String(replies[3]));
        const after = await c.send(R.get("wk3"), 1);
        ok("wire abort applied nothing", after[0] === null);
        c.end();
    });

    // 23. Bad arity at queue time over the wire -> EXECABORT.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.multi() + R.badSet("wk4") + R.exec(), 3);
        ok("wire arity error", replies[1] instanceof Error && String(replies[1].message).includes("wrong number of arguments"), String(replies[1]));
        ok("wire arity abort", replies[2] instanceof Error && String(replies[2].message).startsWith("EXECABORT"));
        c.end();
    });

    // 24. Nested MULTI over the wire keeps the transaction alive.
    await withServer(async (port) => {
        const c = makeConn(port);
        const replies = await c.send(R.multi() + R.set("wk5", "v") + R.multi() + R.exec(), 4);
        ok("wire nested error", replies[2] instanceof Error && String(replies[2].message).includes("can not be nested"), String(replies[2]));
        ok("wire nested exec applies", eq(replies[3], ["OK"]), JSON.stringify(replies[3]));
        const after = await c.send(R.get("wk5"), 1);
        ok("wire nested value set", after[0] === "v");
        c.end();
    });

    // 25. Two real connections stay isolated.
    await withServer(async (port) => {
        const a = makeConn(port);
        const b = makeConn(port);
        let r = await a.send(R.multi() + R.set("iso-wa", "1"), 2);
        ok("wire iso A queued", r[0] === "OK" && r[1] === "QUEUED");
        r = await b.send(R.set("iso-wb", "2") + R.get("iso-wa"), 2);
        ok("wire iso B executes now", r[0] === "OK");
        ok("wire iso B cannot see A queue", r[1] === null);
        r = await a.send(R.get("iso-wb") + R.exec(), 2);
        ok("wire iso A queued GET", r[0] === "QUEUED");
        ok("wire iso A exec sees B value", eq(r[1], ["OK", "2"]), JSON.stringify(r[1]));
        a.end();
        b.end();
    });

    // 26. Client registry: connectedClients tracks real sockets.
    await withServer(async (port, h) => {
        const c = makeConn(port);
        await c.send(R.ping(), 1);
        ok("registry counts client", h.stats().clients === 1, `clients=${h.stats().clients}`);
        c.end();
        await new Promise((r) => setTimeout(r, 100));
        ok("registry drops client on close", h.stats().clients === 0, `clients=${h.stats().clients}`);
    });

    // 27. Transaction memory freed with the client (no cross-connection leak).
    await withServer(async (port, h) => {
        const doomed = makeConn(port);
        await doomed.send(R.multi() + R.set("gone", "1"), 2);
        doomed.destroy();
        await new Promise((r) => setTimeout(r, 100));
        ok("registry after abandon", h.stats().clients === 0, `clients=${h.stats().clients}`);
        const c = makeConn(port);
        const replies = await c.send(R.set("gone", "2") + R.exec(), 2);
        ok("new client not in txn", replies[0] === "OK");
        ok("new client exec errors", replies[1] instanceof Error && String(replies[1].message).includes("EXEC without MULTI"), String(replies[1]));
        c.end();
    });
}

async function main() {
    directTests();
    await wireTests();
    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error("FATAL", e); process.exit(1); });
