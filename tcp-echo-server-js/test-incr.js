"use strict";

// test-incr.js — INCR: missing/new, ints, errors, TTL, encoding, AOF replay.

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { evalINCR, evalAndRespond } = require("./eval");
const { put, get, clearStore, newObj, newObjAbs, packTypeEncoding, getEncoding, OBJ_ENCODING_INT } = require("./store");
const aof = require("./aof");

let passed = 0;
let failed = 0;

function ok(name, cond, extra = "") {
    if (cond) { passed++; console.log(`PASS ${name}${extra ? " — " + extra : ""}`); }
    else { failed++; console.error(`FAIL ${name}${extra ? " — " + extra : ""}`); }
}
function fakeConn() {
    return { writes: [], write(b) { this.writes.push(Buffer.from(b).toString()); } };
}
function lastText(c) { return c.writes.length ? c.writes[c.writes.length - 1] : "<no write>"; }

function main() {
    // 1. Missing key starts at 1.
    {
        clearStore();
        const c = fakeConn();
        evalINCR(["n:new"], c);
        ok("incr missing -> :1", lastText(c) === ":1\r\n", JSON.stringify(lastText(c)));
        ok("stored as string", get("n:new").value === "1");
        ok("new key encoded INT", getEncoding(get("n:new")) === OBJ_ENCODING_INT);
    }

    // 2. Increment path: positive, zero, negative.
    {
        clearStore();
        put("n:a", newObj("41", -1));
        const c = fakeConn();
        evalINCR(["n:a"], c);
        ok("incr 41 -> :42", lastText(c) === ":42\r\n");
        evalINCR(["n:a"], c);
        ok("incr again -> :43", lastText(c) === ":43\r\n");
        put("n:neg", newObj("-1", -1));
        evalINCR(["n:neg"], c);
        ok("incr -1 -> :0", lastText(c) === ":0\r\n");
        ok("encoding stays INT", getEncoding(get("n:a")) === OBJ_ENCODING_INT);
    }

    // 3. Errors: non-integer, unsafe integer, overflow, arity.
    {
        clearStore();
        const c = fakeConn();
        put("n:str", newObj("hello", -1));
        evalINCR(["n:str"], c);
        ok("non-int errors", lastText(c).startsWith("-ERR value is not an integer"), JSON.stringify(lastText(c)));
        ok("failed incr keeps value", get("n:str").value === "hello");
        put("n:big", newObj("9007199254740993", -1)); // 2^53+1, not safe
        evalINCR(["n:big"], c);
        ok("unsafe int errors", lastText(c).startsWith("-ERR value is not an integer"));
        put("n:max", newObj(String(Number.MAX_SAFE_INTEGER), -1));
        evalINCR(["n:max"], c);
        ok("overflow errors", lastText(c).startsWith("-ERR increment or decrement would overflow"), JSON.stringify(lastText(c)));
        evalINCR([], c);
        ok("0 args arity", lastText(c).includes("wrong number of arguments for 'incr'"));
        evalINCR(["a", "b"], c);
        ok("2 args arity", lastText(c).includes("wrong number of arguments for 'incr'"));
    }

    // 4. Expired key behaves as missing; TTL survives INCR.
    {
        clearStore();
        put("n:old", newObjAbs("5", Date.now() - 1000));
        const c = fakeConn();
        evalINCR(["n:old"], c);
        ok("expired -> :1", lastText(c) === ":1\r\n");
        put("n:ttl", newObj("10", 60000));
        evalINCR(["n:ttl"], c);
        ok("incr keeps value+1", lastText(c) === ":11\r\n" && get("n:ttl").value === "11");
        const ttlC = fakeConn();
        const { evalTTL } = require("./eval");
        evalTTL(["n:ttl"], ttlC);
        const ttl = Number(ttlC.writes[0].toString().slice(1));
        ok("incr preserves TTL", ttl >= 55 && ttl <= 60, `ttl=${ttl}`);
    }

    // 5. Wrong type + dispatch (incl. lowercase).
    {
        clearStore();
        put("n:lst", { value: "x", expireAt: -1, typeEncoding: packTypeEncoding(1, 0) });
        const c = fakeConn();
        evalINCR(["n:lst"], c);
        ok("wrong type -> WRONGTYPE", lastText(c).startsWith("-WRONGTYPE"));
        const d = fakeConn();
        evalAndRespond({ Cmd: "incr", Args: ["n:d"] }, d);
        ok("lowercase dispatch -> :1", lastText(d) === ":1\r\n");
    }

    // 6. AOF logs INCR verbatim; replay converges.
    {
        const d = fs.mkdtempSync(path.join(os.tmpdir(), "redis-incr-"));
        const p = path.join(d, "dice-master.aof");
        clearStore();
        aof.configure({ path: p, enabled: true });
        const c = fakeConn();
        evalAndRespond({ Cmd: "SET", Args: ["n:r", "5"] }, c);
        evalAndRespond({ Cmd: "INCR", Args: ["n:r"] }, c);
        evalAndRespond({ Cmd: "INCR", Args: ["n:r"] }, c);
        ok("live value 7", get("n:r").value === "7");
        const raw = fs.readFileSync(p, "utf8");
        ok("aof has verbatim INCR", raw.includes("*2\r\n$4\r\nINCR\r\n"), JSON.stringify(raw));
        aof.setEnabled(false);
        clearStore();
        const r = aof.replayAOF(p, (cmd) => evalAndRespond(cmd, fakeConn()));
        ok("replay converges to 7", r.replayed === 3 && get("n:r").value === "7", `replayed=${r.replayed}`);
    }

    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main();
