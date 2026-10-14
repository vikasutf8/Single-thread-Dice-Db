"use strict";

// Command evaluation — Go study ported to Node.js.
// Go: func respond(cmd *RedisCmd, conn)
//         └── evalAndRespond(cmd, conn)
//               ├── switch cmd.Cmd: "PING" → evalPING, "SET" → evalSET,
//               │                   "GET" → evalGET, "TTL" → evalTTL,
//               │                   default → evalDefault
//
// Each eval*() writes its own reply (conn.write) and returns the Buffer.
// evalAndRespond only dispatches — it must NOT write again (double-write
// would corrupt pipelined streams, e.g. redis-benchmark).

const { EncodeSimpleString, EncodeBulkString, EncodeError, EncodeInteger } = require("./resp-encoder");
const { newObj, put, get } = require("./store");

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
            put(key, newObj(value, expiryMs));
            out = EncodeSimpleString("OK");
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
        out = obj === null ? EncodeBulkString(null) : EncodeBulkString(String(obj.value));
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
        if (obj === null) {
            out = EncodeInteger(-2);
        } else if (obj.expireAt === -1) {
            out = EncodeInteger(-1);
        } else {
            const secs = Math.ceil((obj.expireAt - Date.now()) / 1000);
            out = EncodeInteger(secs <= 0 ? -2 : secs);
        }
    }
    if (conn && typeof conn.write === "function") conn.write(out);
    return out;
}

// Entry: dispatch on upper-cased Cmd. Never throws.
// Each eval*() already did the single conn.write — no write here.
function evalAndRespond(redisCmd, conn) {
    if (!redisCmd || typeof redisCmd.Cmd !== "string") {
        respondError(new Error("empty command"), conn);
        return;
    }
    const args = redisCmd.Args || [];
    switch (redisCmd.Cmd.toUpperCase()) {
        case "PING":
            evalPING(args, conn);
            break;
        case "SET":
            evalSET(args, conn);
            break;
        case "GET":
            evalGET(args, conn);
            break;
        case "TTL":
            evalTTL(args, conn);
            break;
        default:
            evalDefault(redisCmd, conn);
            break;
    }
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
    evalDefault,
    evalAndRespond,
    respondError,
    respond,
};
