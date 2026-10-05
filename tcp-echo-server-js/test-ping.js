"use strict";

// Test PING path: readCmd + evalPING + evalAndRespond. No deps.
const { readCmd } = require("./redis-cmd");
const { evalPING, evalAndRespond, respondError } = require("./eval");

let passed = 0;
let failed = 0;

// Fake socket capturing writes as strings.
function fakeConn() {
    return {
        chunks: [],
        write(b) {
            this.chunks.push(Buffer.isBuffer(b) ? b.toString("utf8") : String(b));
            return true;
        },
        last() {
            return this.chunks[this.chunks.length - 1];
        },
    };
}

function eq(name, actual, expected) {
    if (actual === expected) {
        passed++;
        console.log(`PASS ${name} -> ${JSON.stringify(actual)}`);
    } else {
        failed++;
        console.log(`FAIL ${name}: expected=${JSON.stringify(expected)} got=${JSON.stringify(actual)}`);
    }
}

console.log("--- readCmd: Cmd upper-cased, rest Args ---");
(function () {
    let r = readCmd(Buffer.from("*1\r\n$4\r\nPING\r\n", "utf8"));
    eq("readCmd PING Cmd", r.cmd && r.cmd.Cmd, "PING");
    eq("readCmd PING Args", JSON.stringify(r.cmd && r.cmd.Args), "[]");

    r = readCmd(Buffer.from("*2\r\n$4\r\nping\r\n$5\r\nhello\r\n", "utf8"));
    eq("readCmd lowercase ping Cmd", r.cmd && r.cmd.Cmd, "PING");
    eq("readCmd ping hello Args", JSON.stringify(r.cmd && r.cmd.Args), `["hello"]`);

    r = readCmd(Buffer.from("", "utf8"));
    eq("readCmd empty incomplete", r.err && r.err.message.includes("incomplete"), true);

    r = readCmd(Buffer.from("*0\r\n", "utf8"));
    eq("readCmd empty array err", r.err && r.err.message, "empty command");

    r = readCmd(Buffer.from("+OK\r\n", "utf8"));
    eq("readCmd non-array err", r.err && r.err.message, "expected array command");
})();

console.log("--- evalPING wire replies ---");
(function () {
    let c = fakeConn();
    evalPING([], c);
    eq("PING -> PONG", c.last(), "+PONG\r\n");

    c = fakeConn();
    evalPING(["hello"], c);
    eq("PING hello -> bulk", c.last(), "$5\r\nhello\r\n");

    c = fakeConn();
    evalPING(["hello", "vikas"], c);
    eq("PING a b -> error", c.last(), "-ERR wrong number of arguments for 'ping' command\r\n");
})();

console.log("--- evalAndRespond dispatch ---");
(function () {
    let c = fakeConn();
    evalAndRespond({ Cmd: "PING", Args: [] }, c);
    eq("dispatch PING", c.last(), "+PONG\r\n");

    c = fakeConn();
    evalAndRespond({ Cmd: "ping", Args: ["hi"] }, c);
    eq("dispatch lowercase ping", c.last(), "$2\r\nhi\r\n");

    c = fakeConn();
    evalAndRespond({ Cmd: "PING", Args: ["a", "b"] }, c);
    eq("dispatch PING too many", c.last(), "-ERR wrong number of arguments for 'ping' command\r\n");

    c = fakeConn();
    evalAndRespond({ Cmd: "FOO", Args: [] }, c);
    eq("dispatch unknown", c.last(), "-ERR unknown command 'foo'\r\n");

    c = fakeConn();
    respondError(new Error("boom"), c);
    eq("respondError", c.last(), "-ERR boom\r\n");
})();

console.log("--- streaming: split + pipelined ---");
(function () {
    // Split PING across two chunks (leftover buffer case).
    const full = Buffer.from("*1\r\n$4\r\nPING\r\n", "utf8");
    const part1 = full.slice(0, 6);
    const part2 = full.slice(6);
    let leftover = Buffer.alloc(0);
    leftover = Buffer.concat([leftover, part1]);
    let r1 = readCmd(leftover);
    eq("split part1 incomplete", r1.err && r1.err.message.includes("incomplete"), true);
    leftover = Buffer.concat([leftover, part2]);
    let r2 = readCmd(leftover);
    eq("split part2 Cmd", r2.cmd && r2.cmd.Cmd, "PING");

    // Two PINGs in one chunk: use n to drain.
    const two = Buffer.concat([
        Buffer.from("*1\r\n$4\r\nPING\r\n", "utf8"),
        Buffer.from("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n", "utf8"),
    ]);
    const c = fakeConn();
    let rest = two;
    let seen = [];
    while (rest.length > 0) {
        const r = readCmd(rest);
        if (r.err) break;
        evalAndRespond(r.cmd, c);
        seen.push(c.last());
        rest = rest.slice(r.n);
    }
    eq("pipelined count", seen.length, 2);
    eq("pipelined[0]", seen[0], "+PONG\r\n");
    eq("pipelined[1]", seen[1], "$5\r\nhello\r\n");
})();

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
