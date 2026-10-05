"use strict";

// Command evaluation — Go study ported to Node.js.
// Go: func respond(cmd *RedisCmd, conn)
//         └── evalAndRespond(cmd, conn)
//               ├── switch cmd.Cmd: "PING" → evalPING, default → evalDefault

const { EncodeSimpleString, EncodeBulkString, EncodeError } = require("./resp-encoder");

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

// Entry: dispatch on upper-cased Cmd. Never throws.
// eval*() builds the reply bytes (string methodologies above),
// here we do the single conn.write.
function evalAndRespond(redisCmd, conn) {
    if (!redisCmd || typeof redisCmd.Cmd !== "string") {
        respondError(new Error("empty command"), conn);
        return;
    }
    let out;
    switch (redisCmd.Cmd.toUpperCase()) {
        case "PING":
            out = evalPING(redisCmd.Args || []);
            break;
        default:
            out = evalDefault(redisCmd);
            break;
    }
    conn.write(out);
}

// Old echo-server shape kept for compatibility: respond(conn, bytes).
// Prefer evalAndRespond going forward.
function respond(conn, bytes) {
    conn.write(bytes);
}

module.exports = {
    evalPING,
    evalDefault,
    evalAndRespond,
    respondError,
    respond,
};
