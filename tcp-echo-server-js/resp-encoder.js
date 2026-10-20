"use strict";

// RESP encoder — mirrors what Go study needs for replies.
// All functions return Buffer (wire bytes).

function EncodeSimpleString(s) {
    return Buffer.from(`+${s}\r\n`, "utf8");
}

function EncodeBulkString(s) {
    if (s === null || s === undefined) {
        return Buffer.from("$-1\r\n", "utf8");
    }
    const str = String(s);
    const byteLen = Buffer.byteLength(str, "utf8");
    return Buffer.from(`$${byteLen}\r\n${str}\r\n`, "utf8");
}

function EncodeError(msg) {
    const text = msg instanceof Error ? msg.message : String(msg);
    // Redis convention: errors start with "ERR " unless already prefixed.
    const body = /^(ERR|WRONGTYPE|EXECABORT|NOSCRIPT|LOADING|BUSY|NOSUB|NOPERM)/.test(text)
        ? text
        : `ERR ${text}`;
    return Buffer.from(`-${body}\r\n`, "utf8");
}

function EncodeInteger(n) {
    return Buffer.from(`:${Number(n)}\r\n`, "utf8");
}

// EncodeArray(items) — RESP2 array: *N\r\n followed by each pre-encoded item.
// Used by EXEC to return the queued commands' replies as one array.
function EncodeArray(items) {
    const parts = [Buffer.from(`*${items.length}\r\n`, "utf8")];
    for (const item of items) {
        parts.push(Buffer.isBuffer(item) ? item : Buffer.from(String(item), "utf8"));
    }
    return Buffer.concat(parts);
}

module.exports = {
    EncodeSimpleString,
    EncodeBulkString,
    EncodeError,
    EncodeInteger,
    EncodeArray,
};
