"use strict";

// RedisCmd parsing — Go study ported to Node.js.
// Go: type RedisCmd struct { Cmd string; Args []string }
//     func readCmd(conn) (*RedisCmd, error)
//         └── decodeArray(buf) using Decode() → []string

const { Decode } = require("./resp-decoder");

// Validate decoded RESP value is an array of strings.
// redis-cli always sends commands as arrays of bulk strings,
// e.g. *1\r\n$4\r\nPING\r\n
function decodeArrayValue(data) {
    if (data === null || data === undefined) {
        return { arr: undefined, err: new Error("null command") };
    }
    if (data instanceof Error) {
        return { arr: undefined, err: new Error(`command is an error: ${data.message}`) };
    }
    if (!Array.isArray(data)) {
        return { arr: undefined, err: new Error("expected array command") };
    }
    if (data.length === 0) {
        return { arr: undefined, err: new Error("empty command") };
    }
    for (let i = 0; i < data.length; i++) {
        if (typeof data[i] !== "string") {
            return { arr: undefined, err: new Error("command array must contain only strings") };
        }
    }
    return { arr: data, err: null };
}

// Inline fallback (what `nc` sends): "PING\n", "PING hello\r\n".
// Real Redis accepts these alongside RESP arrays. Split on whitespace,
// first token is Cmd (upper-cased), rest are Args.
function readInline(buf) {
    const nl = buf.indexOf("\n");
    if (nl === -1) {
        return { cmd: undefined, n: 0, err: new Error("incomplete: waiting for newline") };
    }
    const n = nl + 1;
    const line = buf.slice(0, n).toString("utf8").trim();
    if (line === "") {
        return { cmd: undefined, n, err: new Error("empty line") };
    }
    const parts = line.split(/\s+/);
    const Cmd = String(parts[0]).toUpperCase();
    const Args = parts.slice(1);
    return { cmd: { Cmd, Args }, n, err: null };
}

// Read one command from the front of buf.
// Returns { cmd: { Cmd, Args } | undefined, n, err }.
// n = bytes consumed (0 when incomplete so caller keeps the buffer).
// err with message starting "incomplete" means: wait for more bytes.
function readCmd(input) {
    const buf = Buffer.isBuffer(input) ? input : Buffer.from(String(input), "utf8");
    if (buf.length === 0) {
        return { cmd: undefined, n: 0, err: new Error("incomplete: empty input") };
    }
    const { data, n, err } = Decode(buf);
    if (!err) {
        const { arr, err: arrErr } = decodeArrayValue(data);
        if (arrErr) return { cmd: undefined, n, err: arrErr };
        const Cmd = String(arr[0]).toUpperCase();
        const Args = arr.slice(1);
        return { cmd: { Cmd, Args }, n, err: null };
    }
    // Decode failed. If the first byte is not a RESP type byte, the client
    // probably typed a plain inline command via `nc` ("PING\n").
    // Fall back to line parsing instead of "unknown type byte".
    if (err.message.startsWith("unknown type byte")) {
        return readInline(buf);
    }
    return { cmd: undefined, n: 0, err };
}

module.exports = { readCmd, decodeArrayValue };
