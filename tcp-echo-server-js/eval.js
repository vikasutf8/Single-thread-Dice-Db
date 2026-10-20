"use strict";

// Command evaluation — Go study ported to Node.js.
// Go: func respond(cmd *RedisCmd, conn)
//         └── evalAndRespond(cmd, conn)
//               ├── switch cmd.Cmd: "PING" → evalPING, "SET" → evalSET,
//               │                   "GET" → evalGET, "TTL" → evalTTL,
//               │                   "DEL" → evalDEL, "EXPIRE" → evalEXPIRE,
//               │                   "PEXPIRE" → evalPEXPIRE, "INCR" → evalINCR,
//               │                   "BGSAVE" → evalBGSAVE, "BGREWRITEAOF" → evalBGREWRITEAOF,
//               │                   "SAVE" → evalSAVE, default → evalDefault
//               └── write cmds log to AOF via appendCommand (aof.js);
//                   SET/DEL/EXPIRE/PEXPIRE/INCR log the effective op.
//                   persistence verbs run bgEvalRDB / bgEvalAOF (never block).
//
// Each eval*() writes its own reply (conn.write) and returns the Buffer.
// evalAndRespond only dispatches — it must NOT write again (double-write
// would corrupt pipelined streams, e.g. redis-benchmark).

const { EncodeSimpleString, EncodeBulkString, EncodeError, EncodeInteger, EncodeArray } = require("./resp-encoder");
const { newObj, put, get, del, setExpire, OBJ_TYPE_STRING, assertType, deduceTypeEncoding, packTypeEncoding, getType, getInfo } = require("./store");
const { appendCommand, bgEvalAOF, isRewriting } = require("./aof");
const { bgEvalRDB, dumpRDBSync, isSaving } = require("./rdb");

// Any failure is written back as a RESP error so the connection stays open.
function respondError(err, conn) {
    const msg = err instanceof Error ? err.message : String(err);
    conn.write(EncodeError(msg));
}

// PING → PONG | PING hello → hello | PING a b → error
// String-only methodologies (pure, no socket):
//   1. bare PING returns a Simple String:  "PONG" -> +PONG\r\n
//   2. PING <msg> returns a Bulk String:   "hello" -> $5\r\nhello\r\n
// Keeping eval pure (return Buffer) separates "what to reply"
// from "how to write it" (evalAndRespond does conn.write).
function evalPING(args, conn) {
    let out;
    if (args.length === 0) {
        out = EncodeSimpleString("PONG"); // methodology 1: simple string
    } else if (args.length === 1) {
        out = EncodeBulkString(args[0]); // methodology 2: bulk string echo
    } else {
        out = EncodeError("wrong number of arguments for 'ping' command");
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

function evalDefault(redisCmd, conn) {
    const out = EncodeError(`unknown command '${String(redisCmd.Cmd).toLowerCase()}'`);
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// SET key value [EX seconds] [PX ms] → +OK
// args[0] = key, args[1] = value, expiryMs defaults to -1 (persistent).
function evalSET(args, conn) {
    let out;
    if (!Array.isArray(args) || args.length < 2) {
        out = EncodeError("wrong number of arguments for 'set' command");
    } else {
        const key = args[0];
        const value = args[1];
        let expiryMs = -1;
        let ok = true;
        for (let i = 2; i < args.length && ok; i++) {
            const token = String(args[i]).toUpperCase();
            if (token === "EX" || token === "PX") {
                const raw = args[i + 1];
                if (raw === undefined) {
                    out = EncodeError("syntax error");
                    ok = false;
                    break;
                }
                if (!/^-?\d+$/.test(String(raw))) {
                    out = EncodeError("value is not an integer or out of range");
                    ok = false;
                    break;
                }
                const n = Number(raw);
                if (!Number.isSafeInteger(n) || n <= 0) {
                    out = EncodeError("invalid expire time in 'set' command");
                    ok = false;
                    break;
                }
                expiryMs = token === "EX" ? n * 1000 : n;
                i++; // consume the numeric argument
            } else {
                out = EncodeError("syntax error");
                ok = false;
            }
        }
        if (ok) {
            // tryObjectEncoding step: deduce (otype, oEnc) then store them.
            const { otype, oEnc } = deduceTypeEncoding(value);
            put(key, newObj(value, expiryMs, otype, oEnc));
            out = EncodeSimpleString("OK");
            appendCommand({ Cmd: "SET", Args: args.slice() }); // AOF: log effective write
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// GET key → bulk value | $-1 (missing or expired → lazy-deleted in store.get)
function evalGET(args, conn) {
    let out;
    if (!Array.isArray(args) || args.length !== 1) {
        out = EncodeError("wrong number of arguments for 'get' command");
    } else {
        const obj = get(args[0]);
        try {
            if (obj !== null) assertType(obj, OBJ_TYPE_STRING);
            out = obj === null ? EncodeBulkString(null) : EncodeBulkString(String(obj.value));
        } catch (err) {
            out = EncodeError(err.message); // -WRONGTYPE, connection stays open
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// TTL key → -2 (missing) | -1 (persistent) | seconds (ceil, so fresh EX 10 → 10)
function evalTTL(args, conn) {
    let out;
    if (!Array.isArray(args) || args.length !== 1) {
        out = EncodeError("wrong number of arguments for 'ttl' command");
    } else {
        const obj = get(args[0]);
        try {
            if (obj !== null) assertType(obj, OBJ_TYPE_STRING);
            if (obj === null) {
                out = EncodeInteger(-2);
            } else if (obj.expireAt === -1) {
                out = EncodeInteger(-1);
            } else {
                const secs = Math.ceil((obj.expireAt - Date.now()) / 1000);
                out = EncodeInteger(secs <= 0 ? -2 : secs);
            }
        } catch (err) {
            out = EncodeError(err.message); // -WRONGTYPE, connection stays open
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// DEL key [key ...] → :deleted-count (:0 when nothing deleted, not an error)
function evalDEL(args, conn) {
    let out;
    if (!Array.isArray(args) || args.length < 1) {
        out = EncodeError("wrong number of arguments for 'del' command");
    } else {
        let count = 0;
        for (const key of args) {
            if (get(key) !== null && del(key)) count++; // lazy get: expired counts as absent
        }
        out = EncodeInteger(count);
        appendCommand({ Cmd: "DEL", Args: args.slice() }); // AOF: replay converges (:0 harmless)
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// Shared EXPIRE/PEXPIRE parser. isMillis=false → seconds × 1000.
// Missing/expired key → :0; value <= 0 → delete now, :1 (real Redis).
function evalExpirelike(args, conn, cmdName, isMillis) {
    const lname = cmdName.toLowerCase();
    let out;
    if (!Array.isArray(args) || args.length !== 2) {
        out = EncodeError(`wrong number of arguments for '${lname}' command`);
    } else if (!/^-?\d+$/.test(String(args[1]))) {
        out = EncodeError("value is not an integer or out of range");
    } else {
        const n = Number(args[1]);
        if (!Number.isSafeInteger(n)) {
            out = EncodeError("value is not an integer or out of range");
        } else if (get(args[0]) === null) {
            out = EncodeInteger(0); // missing: no AOF record (no state change)
        } else if (n <= 0) {
            del(args[0]);
            out = EncodeInteger(1);
            appendCommand({ Cmd: "DEL", Args: [args[0]] }); // AOF: log the effective delete
        } else {
            setExpire(args[0], isMillis ? n : n * 1000);
            out = EncodeInteger(1);
            appendCommand({ Cmd: cmdName.toUpperCase(), Args: args.slice() }); // AOF verbatim
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// EXPIRE key seconds → :1 timeout set / :0 key missing
function evalEXPIRE(args, conn) {
    return evalExpirelike(args, conn, "EXPIRE", false);
}

// PEXPIRE key milliseconds → :1 timeout set / :0 key missing
function evalPEXPIRE(args, conn) {
    return evalExpirelike(args, conn, "PEXPIRE", true);
}

// INCR key → value+1. Missing (or expired) key starts at 0, so INCR new = 1.
// Non-integer value → not-an-integer error; overflow past safe integer → error.
// TTL is preserved (only value + encoding change). AOF logs INCR verbatim:
// replay re-increments from the same base state, so it converges.
function evalINCR(args, conn) {
    let out;
    if (!Array.isArray(args) || args.length !== 1) {
        out = EncodeError("wrong number of arguments for 'incr' command");
    } else {
        const key = args[0];
        const obj = get(key); // lazy get: expired counts as missing
        try {
            if (obj !== null) assertType(obj, OBJ_TYPE_STRING);
            let next;
            if (obj === null) {
                next = 1;
                const { otype, oEnc } = deduceTypeEncoding(String(next));
                put(key, newObj(String(next), -1, otype, oEnc));
            } else {
                const s = String(obj.value);
                if (!/^-?\d+$/.test(s)) {
                    out = EncodeError("value is not an integer or out of range");
                } else {
                    const n = Number(s);
                    if (!Number.isSafeInteger(n)) {
                        out = EncodeError("value is not an integer or out of range");
                    } else if (!Number.isSafeInteger(n + 1)) {
                        out = EncodeError("increment or decrement would overflow");
                    } else {
                        next = n + 1;
                        obj.value = String(next); // TTL untouched
                        const { oEnc } = deduceTypeEncoding(obj.value);
                        obj.typeEncoding = packTypeEncoding(getType(obj), oEnc);
                    }
                }
            }
            if (out === undefined) {
                out = EncodeInteger(next);
                appendCommand({ Cmd: "INCR", Args: args.slice() }); // AOF verbatim
            }
        } catch (err) {
            out = EncodeError(err.message); // -WRONGTYPE, connection stays open
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// BGSAVE -> bgEvalRDB: snapshot now, reply immediately, write lands async.
function evalBGSAVE(args, conn) {
    let out;
    if (Array.isArray(args) && args.length > 0) {
        out = EncodeError("wrong number of arguments for 'bgsave' command");
    } else if (isSaving()) {
        out = EncodeError("rdb save already in progress");
    } else {
        bgEvalRDB().then((r) => {
            if (!r.ok) console.error(`[bgsave] failed: ${r.reason}`);
        });
        out = EncodeSimpleString("Background saving started");
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// BGREWRITEAOF -> bgEvalAOF: compact the log, reply immediately.
function evalBGREWRITEAOF(args, conn) {
    let out;
    if (Array.isArray(args) && args.length > 0) {
        out = EncodeError("wrong number of arguments for 'bgrewriteaof' command");
    } else if (isRewriting()) {
        out = EncodeError("aof rewrite already in progress");
    } else {
        bgEvalAOF().then((r) => {
            if (!r.ok) console.error(`[bgrewriteaof] failed: ${r.reason}`);
        });
        out = EncodeSimpleString("Background append only file rewriting started");
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// SAVE -> blocking RDB snapshot (terminal use; stalls the loop briefly).
function evalSAVE(args, conn) {
    let out;
    if (Array.isArray(args) && args.length > 0) {
        out = EncodeError("wrong number of arguments for 'save' command");
    } else {
        try {
            dumpRDBSync();
            out = EncodeSimpleString("OK");
        } catch (err) {
            out = EncodeError(err.message);
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// INFO -> server statistics
function evalINFO(args, conn) {
    let out;
    if (Array.isArray(args) && args.length > 0) {
        out = EncodeError("wrong number of arguments for 'info' command");
    } else {
        const info = getInfo();
        const lines = [];
        for (const [section, data] of Object.entries(info)) {
            lines.push(`# ${section}`);
            for (const [key, value] of Object.entries(data)) {
                lines.push(`${key}:${value}`);
            }
            lines.push("");
        }
        out = EncodeBulkString(lines.join("\r\n"));
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// ---------------------------------------------------------------------------
// Transactions: per-client state lives on the client object created in
// async.tcp.js: { cqueue, isTxn, txnDirty, executing, write() }.
// evalAndRespond queues data commands while isTxn; EXEC runs them back-to-back.
// ---------------------------------------------------------------------------

// Sink used by executeCommand so handlers never write directly: the caller
// (evalAndRespond / EXEC) owns the single write of the returned bytes.
const NULL_CONN = { write() { } };

// Commands that control the transaction rather than being queued by it.
const TX_CONTROL = new Set(["MULTI", "EXEC", "DISCARD"]);

// Queue-time arity validation — Redis rejects these at queue time and marks
// the transaction dirty, so EXEC returns EXECABORT. max = -1 means unbounded.
const TX_QUEUE_ARITY = {
    PING: [0, 1],
    SET: [2, -1],
    GET: [1, 1],
    TTL: [1, 1],
    DEL: [1, -1],
    EXPIRE: [2, 2],
    PEXPIRE: [2, 2],
    INCR: [1, 1],
    BGSAVE: [0, 0],
    BGREWRITEAOF: [0, 0],
    SAVE: [0, 0],
    INFO: [0, 0],
};

function respOk() {
    return EncodeSimpleString("OK");
}

function resetTxn(client) {
    if (!client || typeof client !== "object") return;
    client.isTxn = false;
    client.cqueue = [];
    client.txnDirty = false;
    client.executing = false;
}

// Queue a command inside MULTI. Returns the reply bytes (+QUEUED or -ERR).
// Unknown commands and bad arity set txnDirty so EXEC aborts the transaction.
function queueTxnCommand(redisCmd, client) {
    const verb = redisCmd.Cmd.toUpperCase();
    const args = redisCmd.Args || [];
    const arity = TX_QUEUE_ARITY[verb];
    if (!arity) {
        client.txnDirty = true;
        return EncodeError(`unknown command '${String(redisCmd.Cmd).toLowerCase()}'`);
    }
    const [min, max] = arity;
    if (args.length < min || (max >= 0 && args.length > max)) {
        client.txnDirty = true;
        return EncodeError(`wrong number of arguments for '${verb.toLowerCase()}' command`);
    }
    client.cqueue.push({ Cmd: verb, Args: args.slice() });
    return EncodeSimpleString("QUEUED");
}

// MULTI → +OK; nested MULTI → error (transaction keeps going, not dirty).
function evalMULTI(client) {
    if (!client || typeof client !== "object") {
        return EncodeError("MULTI calls can not be nested");
    }
    if (client.isTxn) {
        return EncodeError("MULTI calls can not be nested");
    }
    client.isTxn = true;
    client.cqueue = [];
    client.txnDirty = false;
    client.executing = false;
    return respOk();
}

// EXEC → *N array of per-command replies, in queue order.
// Queue-time errors → EXECABORT (nothing runs, state is reset).
function evalEXEC(client) {
    if (!client || !client.isTxn) {
        return EncodeError("EXEC without MULTI");
    }
    if (client.txnDirty) {
        resetTxn(client);
        return EncodeError("EXECABORT Transaction discarded because of previous errors.");
    }
    const queue = client.cqueue.slice();
    client.executing = true; // commands run directly, never re-queued
    const replies = [];
    for (const queued of queue) {
        replies.push(executeCommand(queued, client));
    }
    resetTxn(client);
    return EncodeArray(replies);
}

// User-facing alias: evalMulti is the EXEC array responder.
function evalMulti(client) {
    return evalEXEC(client);
}

// DISCARD → drop the queue, keyspace untouched, +OK.
function evalDISCARD(client) {
    if (!client || !client.isTxn) {
        return EncodeError("DISCARD without MULTI");
    }
    resetTxn(client);
    return respOk();
}

// executeCommand(cmd, client) → reply bytes (Buffer). Pure dispatch: never
// writes to the socket. Handlers get NULL_CONN to suppress their own write.
function executeCommand(redisCmd, client) {
    if (!redisCmd || typeof redisCmd.Cmd !== "string") {
        return EncodeError("empty command");
    }
    const args = redisCmd.Args || [];
    switch (redisCmd.Cmd.toUpperCase()) {
        case "PING":
            return evalPING(args, NULL_CONN);
        case "SET":
            return evalSET(args, NULL_CONN);
        case "GET":
            return evalGET(args, NULL_CONN);
        case "TTL":
            return evalTTL(args, NULL_CONN);
        case "DEL":
            return evalDEL(args, NULL_CONN);
        case "EXPIRE":
            return evalEXPIRE(args, NULL_CONN);
        case "PEXPIRE":
            return evalPEXPIRE(args, NULL_CONN);
        case "INCR":
            return evalINCR(args, NULL_CONN);
        case "BGSAVE":
            return evalBGSAVE(args, NULL_CONN);
        case "BGREWRITEAOF":
            return evalBGREWRITEAOF(args, NULL_CONN);
        case "SAVE":
            return evalSAVE(args, NULL_CONN);
        case "INFO":
            return evalINFO(args, NULL_CONN);
        case "MULTI":
            return evalMULTI(client);
        case "EXEC":
            return evalEXEC(client);
        case "DISCARD":
            return evalDISCARD(client);
        default:
            return evalDefault(redisCmd, NULL_CONN);
    }
}

// Explicit bytes-contract alias (execute command → response buffer).
function executeCmdToBuffer(redisCmd, client) {
    return executeCommand(redisCmd, client);
}

// Entry: dispatch on upper-cased Cmd. Never throws.
// If the client is inside MULTI (and not already executing EXEC), data commands
// are validated, queued, and answered +QUEUED; the single write happens here.
function evalAndRespond(redisCmd, client) {
    if (!redisCmd || typeof redisCmd.Cmd !== "string") {
        respondError(new Error("empty command"), client);
        return;
    }
    const verb = redisCmd.Cmd.toUpperCase();
    if (client && client.isTxn && !client.executing && !TX_CONTROL.has(verb)) {
        const queuedReply = queueTxnCommand(redisCmd, client);
        if (typeof client.write === "function") client.write(queuedReply);
        return;
    }
    const out = executeCommand(redisCmd, client);
    if (client && typeof client.write === "function") client.write(out);
}

// Old echo-server shape kept for compatibility: respond(conn, bytes).
// Prefer evalAndRespond going forward.
function respond(conn, bytes) {
    conn.write(bytes);
}

module.exports = {
    evalPING,
    evalSET,
    evalGET,
    evalTTL,
    evalDEL,
    evalEXPIRE,
    evalPEXPIRE,
    evalINCR,
    evalBGSAVE,
    evalBGREWRITEAOF,
    evalSAVE,
    evalINFO,
    evalDefault,
    evalMULTI,
    evalEXEC,
    evalMulti,
    evalDISCARD,
    executeCommand,
    executeCmdToBuffer,
    queueTxnCommand,
    resetTxn,
    respOk,
    evalAndRespond,
    respondError,
    respond,
};
