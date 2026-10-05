"use strict";

// Test every RESP decode path. No dependencies: `node test-resp-decoder.js`.
const {
    Decode,
    decodeOne,
    readSimpleString,
    readError,
    readInt64,
    readBulkString,
    readArray,
} = require("./resp-decoder");

let passed = 0;
let failed = 0;

function show(v) {
    if (v instanceof Error) return `Error(${JSON.stringify(v.message)})`;
    return JSON.stringify(v);
}

function check(name, wire, expected) {
    const buf = Buffer.from(wire, "utf8");
    const { data, n, err } = Decode(buf);
    const isErrExpected = expected instanceof Error;

    if (isErrExpected) {
        // Error case: either Decode err, or decoded Error value.
        const gotErrMsg = err ? err.message : data instanceof Error ? data.message : null;
        if (gotErrMsg === expected.message) {
            passed++;
            console.log(`PASS ${name}: wire=${JSON.stringify(wire)} -> ${err ? "err" : "Error"}=${JSON.stringify(gotErrMsg)} n=${n}`);
        } else {
            failed++;
            console.log(`FAIL ${name}: wire=${JSON.stringify(wire)} expected Error(${JSON.stringify(expected.message)}) got data=${show(data)} err=${err ? err.message : null}`);
        }
        return;
    }

    if (err) {
        failed++;
        console.log(`FAIL ${name}: wire=${JSON.stringify(wire)} unexpected err=${err.message}`);
        return;
    }
    const same = JSON.stringify(data) === JSON.stringify(expected);
    // n must equal full wire length for these single-value cases.
    const nOk = n === buf.length;
    if (same && nOk) {
        passed++;
        console.log(`PASS ${name}: wire=${JSON.stringify(wire)} -> ${show(data)} n=${n}`);
    } else {
        failed++;
        console.log(`FAIL ${name}: wire=${JSON.stringify(wire)} expected=${show(expected)} got=${show(data)} n=${n}`);
    }
}

function checkDecodeError(name, wire, errContains) {
    const { data, n, err } = Decode(Buffer.from(wire, "utf8"));
    if (err && err.message.includes(errContains)) {
        passed++;
        console.log(`PASS ${name}: wire=${JSON.stringify(wire)} -> err=${JSON.stringify(err.message)}`);
    } else {
        failed++;
        console.log(`FAIL ${name}: wire=${JSON.stringify(wire)} expected err containing ${JSON.stringify(errContains)} got data=${show(data)} n=${n} err=${err ? err.message : null}`);
    }
}

console.log("--- Simple String (+) ---");
check("simple OK", "+OK\r\n", "OK");
check("simple hello", "+hello world\r\n", "hello world");
check("simple empty", "+\r\n", "");

console.log("--- Error (-) ---");
check("error value", "-ERR bad command\r\n", new Error("ERR bad command"));
check("error empty msg", "-\r\n", new Error(""));

console.log("--- Integer (:) ---");
check("int zero", ":0\r\n", 0);
check("int positive", ":1000\r\n", 1000);
check("int negative", ":-42\r\n", -42);
check("int plus sign", ":+12\r\n", 12);

console.log("--- Bulk String ($) ---");
check("bulk hello", "$5\r\nhello\r\n", "hello");
check("bulk empty", "$0\r\n\r\n", "");
check("bulk null", "$-1\r\n", null);
check("bulk with spaces", "$11\r\nhello world\r\n", "hello world");

console.log("--- Array (*) ---");
check("array empty", "*0\r\n", []);
check("array null", "*-1\r\n", null);
check("array 2 bulks", "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", ["hello", "world"]);
check("array mixed", "*3\r\n+OK\r\n:42\r\n$5\r\nhello\r\n", ["OK", 42, "hello"]);
check(
    "array nested",
    "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n+hi\r\n",
    [[1, 2], ["hi"]]
);
check(
    "array with nulls",
    "*3\r\n$-1\r\n*-1\r\n:7\r\n",
    [null, null, 7]
);
check(
    "GET command shape",
    "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
    ["GET", "key"]
);

console.log("--- Direct reader functions (pos after type byte) ---");
(function direct() {
    const buf = Buffer.from("+OK\r\n", "utf8");
    const r = readSimpleString(buf, 1);
    if (!r.err && r.data === "OK" && r.pos === 5) { passed++; console.log("PASS direct readSimpleString"); }
    else { failed++; console.log(`FAIL direct readSimpleString got=${JSON.stringify(r)}`); }

    const eb = Buffer.from("-oops\r\n", "utf8");
    const re = readError(eb, 1);
    if (!re.err && re.data instanceof Error && re.data.message === "oops") { passed++; console.log("PASS direct readError"); }
    else { failed++; console.log(`FAIL direct readError got=${JSON.stringify(re)}`); }

    const ib = Buffer.from(":99\r\n", "utf8");
    const ri = readInt64(ib, 1);
    if (!ri.err && ri.data === 99) { passed++; console.log("PASS direct readInt64"); }
    else { failed++; console.log(`FAIL direct readInt64 got=${JSON.stringify(ri)}`); }

    const bb = Buffer.from("$2\r\nhi\r\n", "utf8");
    const rb = readBulkString(bb, 1);
    if (!rb.err && rb.data === "hi") { passed++; console.log("PASS direct readBulkString"); }
    else { failed++; console.log(`FAIL direct readBulkString got=${JSON.stringify(rb)}`); }

    const ab = Buffer.from("*1\r\n:5\r\n", "utf8");
    const ra = readArray(ab, 1);
    if (!ra.err && JSON.stringify(ra.data) === "[5]") { passed++; console.log("PASS direct readArray"); }
    else { failed++; console.log(`FAIL direct readArray got=${JSON.stringify(ra)}`); }

    const o = decodeOne(Buffer.from(":7\r\n", "utf8"), 0);
    if (!o.err && o.data === 7 && o.pos === 4) { passed++; console.log("PASS direct decodeOne"); }
    else { failed++; console.log(`FAIL direct decodeOne got=${JSON.stringify(o)}`); }
})();

console.log("--- Trailing bytes / streaming (n) ---");
(function trailing() {
    const buf = Buffer.concat([Buffer.from("+OK\r\n", "utf8"), Buffer.from(":1\r\n", "utf8")]);
    const first = Decode(buf);
    const second = decodeOne(buf, first.n);
    if (!first.err && first.data === "OK" && first.n === 5 && !second.err && second.data === 1) {
        passed++;
        console.log(`PASS trailing bytes: first=${show(first.data)} n=${first.n}, second=${show(second.data)}`);
    } else {
        failed++;
        console.log(`FAIL trailing bytes got first=${JSON.stringify(first)} second=${JSON.stringify(second)}`);
    }
})();

console.log("--- Error / incomplete cases ---");
checkDecodeError("unknown prefix", "?xx\r\n", "unknown type byte");
checkDecodeError("empty input", "", "empty input");
checkDecodeError("missing CRLF", "+OK", "missing CRLF");
checkDecodeError("bad int", ":12x\r\n", "invalid integer");
checkDecodeError("short bulk", "$5\r\nhi\r\n", "incomplete");
checkDecodeError("bulk missing CRLF", "$5\r\nhelloXX", "CRLF");
checkDecodeError("short array", "*2\r\n:1\r\n", "incomplete");
checkDecodeError("negative bulk other than -1", "$-2\r\n", "invalid bulk length");

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
