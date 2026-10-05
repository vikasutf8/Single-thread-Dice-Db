"use strict";

// RESP2 decoder — Go study ported to Node.js.
// Go shape: Decode(data []byte) (any, int, error)
//           decodeOne(data []byte) (any, int, error)
// Here:     Decode(buf) -> { data, n, err }
//           decodeOne(buf, pos) -> { data, pos: nextPos, err }

const CRLF = "\r\n";
const CR = 0x0d;
const LF = 0x0a;

function toBuffer(input) {
    if (Buffer.isBuffer(input)) return input;
    if (typeof input === "string") return Buffer.from(input, "utf8");
    if (input instanceof Uint8Array) return Buffer.from(input);
    throw new TypeError("Decode expects Buffer | string | Uint8Array");
}

function ok(data, pos) {
    return { data, pos, err: null };
}

function fail(errMsg, pos) {
    const err = errMsg instanceof Error ? errMsg : new Error(errMsg);
    return { data: undefined, pos, err };
}

// Find CRLF starting at pos. Returns line (utf8) + offset after CRLF.
function readLine(buf, pos) {
    const idx = buf.indexOf("\r\n", pos);
    if (idx === -1) {
        return { line: undefined, nextPos: pos, err: new Error("incomplete: missing CRLF") };
    }
    const line = buf.slice(pos, idx).toString("utf8");
    return { line, nextPos: idx + 2, err: null };
}

// +OK\r\n -> "OK"
function readSimpleString(buf, pos) {
    // pos points just after '+'
    const r = readLine(buf, pos);
    if (r.err) return fail(r.err, pos);
    return ok(r.line, r.nextPos);
}

// -ERR bad\r\n -> Error("ERR bad")
function readError(buf, pos) {
    // pos points just after '-'
    const r = readLine(buf, pos);
    if (r.err) return fail(r.err, pos);
    return ok(new Error(r.line), r.nextPos);
}

// :123\r\n -> 123
function readInt64(buf, pos) {
    // pos points just after ':'
    const r = readLine(buf, pos);
    if (r.err) return fail(r.err, pos);
    const s = r.line;
    if (!/^[+-]?\d+$/.test(s)) {
        return fail(`invalid integer: ${JSON.stringify(s)}`, pos);
    }
    const n = Number(s);
    if (!Number.isSafeInteger(n)) {
        return fail(`integer out of safe range: ${JSON.stringify(s)}`, pos);
    }
    return ok(n, r.nextPos);
}

// $5\r\nhello\r\n -> "hello" | $-1\r\n -> null
function readBulkString(buf, pos) {
    // pos points just after '$'
    const r = readLine(buf, pos);
    if (r.err) return fail(r.err, pos);
    if (!/^[+-]?\d+$/.test(r.line)) {
        return fail(`invalid bulk length: ${JSON.stringify(r.line)}`, pos);
    }
    const len = Number(r.line);
    if (len === -1) return ok(null, r.nextPos); // null bulk string
    if (len < -1) return fail(`invalid bulk length: ${r.line}`, pos);
    if (!Number.isSafeInteger(len)) {
        return fail(`bulk length out of range: ${r.line}`, pos);
    }
    const start = r.nextPos;
    // Need len bytes + trailing CRLF.
    if (buf.length < start + len + 2) {
        return fail("incomplete: bulk string bytes missing", pos);
    }
    if (buf[start + len] !== CR || buf[start + len + 1] !== LF) {
        return fail("protocol error: bulk string must end with CRLF", pos);
    }
    const data = buf.slice(start, start + len).toString("utf8");
    return ok(data, start + len + 2);
}

// *<n>\r\n<elem>... | *-1\r\n -> null
function readArray(buf, pos) {
    // pos points just after '*'
    const r = readLine(buf, pos);
    if (r.err) return fail(r.err, pos);
    if (!/^[+-]?\d+$/.test(r.line)) {
        return fail(`invalid array length: ${JSON.stringify(r.line)}`, pos);
    }
    const count = Number(r.line);
    if (count === -1) return ok(null, r.nextPos); // null array
    if (count < -1) return fail(`invalid array length: ${r.line}`, pos);
    if (!Number.isSafeInteger(count)) {
        return fail(`array length out of range: ${r.line}`, pos);
    }
    let cur = r.nextPos;
    const arr = [];
    for (let i = 0; i < count; i++) {
        const one = decodeOne(buf, cur);
        if (one.err) return one; // bubble up incomplete / protocol error
        arr.push(one.data);
        cur = one.pos;
    }
    return ok(arr, cur);
}

// Dispatch on leading type byte. pos points AT the type byte.
function decodeOne(buf, pos) {
    if (pos >= buf.length) {
        return fail("incomplete: empty input", pos);
    }
    const t = String.fromCharCode(buf[pos]);
    const next = pos + 1;
    switch (t) {
        case "+":
            return readSimpleString(buf, next);
        case "-":
            return readError(buf, next);
        case ":":
            return readInt64(buf, next);
        case "$":
            return readBulkString(buf, next);
        case "*":
            return readArray(buf, next);
        default:
            return fail(`unknown type byte: ${JSON.stringify(t)}`, pos);
    }
}

// Entry point — mirrors Go: Decode(data []byte) (any, int, error)
// Returns { data, n, err } where n = bytes consumed.
function Decode(input) {
    let buf;
    try {
        buf = toBuffer(input);
    } catch (e) {
        return { data: undefined, n: 0, err: e };
    }
    if (buf.length === 0) {
        return { data: undefined, n: 0, err: new Error("incomplete: empty input") };
    }
    const r = decodeOne(buf, 0);
    if (r.err) return { data: undefined, n: 0, err: r.err };
    return { data: r.data, n: r.pos, err: null };
}

module.exports = {
    Decode,
    decodeOne,
    readSimpleString,
    readError,
    readInt64,
    readBulkString,
    readArray,
    readLine,
};
