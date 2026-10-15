"use strict";

// test-diceobj.js — diceObj (redisObject port): typeEncoding pack/unpack,
// auto-encoding, asserts, RDB2 preservation, RDB1 back-compat, WRONGTYPE wire.

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { evalAndRespond } = require("./eval");
const {
    put, get, clearStore, newObj, newObjAbs,
    OBJ_TYPE_STRING, OBJ_ENCODING_RAW, OBJ_ENCODING_INT, OBJ_ENCODING_EMBSTR,
    packTypeEncoding, getType, getEncoding,
    assertType, assertEncoding, autoEncoding, deduceTypeEncoding,
} = require("./store");
const rdb = require("./rdb");

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
    // 1. pack/unpack: type rides the high 4 bits, encoding the low 4.
    {
        ok("pack STRING+EMBSTR", packTypeEncoding(OBJ_TYPE_STRING, OBJ_ENCODING_EMBSTR) === 0x02);
        ok("pack STRING+INT", packTypeEncoding(0, 1) === 0x01);
        const te = packTypeEncoding(0, 2);
        ok("getType top bits", getType({ typeEncoding: te }) === 0);
        ok("getEncoding low bits", getEncoding({ typeEncoding: te }) === 2);
        ok("getType tampered high nibble", getType({ typeEncoding: 0x12 }) === 1);
        ok("getEncoding masks top to zero", getEncoding({ typeEncoding: 0xf2 }) === 2);
    }

    // 2. legacy objs (no typeEncoding) read as STRING/RAW.
    {
        const legacy = { value: "v", expireAt: -1 };
        ok("legacy type defaults STRING", getType(legacy) === OBJ_TYPE_STRING);
        ok("legacy encoding defaults RAW", getEncoding(legacy) === OBJ_ENCODING_RAW);
        ok("null-safe getType", getType(null) === OBJ_TYPE_STRING);
        ok("null-safe getEncoding", getEncoding(undefined) === OBJ_ENCODING_RAW);
    }

    // 3. auto-encoding rules.
    {
        ok("int string -> INT", autoEncoding("123") === OBJ_ENCODING_INT);
        ok("negative int -> INT", autoEncoding("-42") === OBJ_ENCODING_INT);
        ok("short string -> EMBSTR", autoEncoding("hello") === OBJ_ENCODING_EMBSTR);
        ok("44 bytes -> EMBSTR", autoEncoding("a".repeat(44)) === OBJ_ENCODING_EMBSTR);
        ok("45 bytes -> RAW", autoEncoding("a".repeat(45)) === OBJ_ENCODING_RAW);
        ok("unsafe integer -> EMBSTR (short)", autoEncoding("1".repeat(25)) === OBJ_ENCODING_EMBSTR);
        ok("float-looking -> EMBSTR", autoEncoding("3.14") === OBJ_ENCODING_EMBSTR);
    }

    // 4. newObj / newObjAbs stamp the byte.
    {
        const o = newObj("42", -1);
        ok("newObj type STRING", getType(o) === OBJ_TYPE_STRING);
        ok("newObj int auto-enc", getEncoding(o) === OBJ_ENCODING_INT);
        ok("newObj persistent", o.expireAt === -1 && o.value === "42");
        const t = newObj("x", 60000);
        ok("newObj TTL armed", t.expireAt - Date.now() > 30000);
        const e = newObj("v", -1, OBJ_TYPE_STRING, OBJ_ENCODING_RAW);
        ok("explicit encoding honored", getEncoding(e) === OBJ_ENCODING_RAW);
        const a = newObjAbs("v", 123456789, OBJ_TYPE_STRING, OBJ_ENCODING_EMBSTR);
        ok("newObjAbs absolute time", a.expireAt === 123456789 && getEncoding(a) === OBJ_ENCODING_EMBSTR);
    }

    // 5. asserts: pass silently, throw on mismatch.
    {
        const o = newObj("hi", -1);
        let threw = false;
        try { assertType(o, OBJ_TYPE_STRING); assertEncoding(o, OBJ_ENCODING_EMBSTR); }
        catch { threw = true; }
        ok("matching asserts pass", !threw);
        let typeErr = null;
        try { assertType({ typeEncoding: packTypeEncoding(1, 0) }, OBJ_TYPE_STRING); }
        catch (e) { typeErr = e; }
        ok("assertType throws WRONGTYPE", typeErr !== null && typeErr.message.startsWith("WRONGTYPE"));
        let encErr = null;
        try { assertEncoding(o, OBJ_ENCODING_RAW); } catch (e) { encErr = e; }
        ok("assertEncoding throws", encErr !== null);
    }

    // 6. deduceTypeEncoding (tryObjectEncoding): SET's (otype, oEnc) source.
    {
        const a = deduceTypeEncoding("123");
        ok("deduce int", a.otype === OBJ_TYPE_STRING && a.oEnc === OBJ_ENCODING_INT);
        const b = deduceTypeEncoding("hi");
        ok("deduce embstr", b.otype === OBJ_TYPE_STRING && b.oEnc === OBJ_ENCODING_EMBSTR);
        const c = deduceTypeEncoding("a".repeat(50));
        ok("deduce raw", c.otype === OBJ_TYPE_STRING && c.oEnc === OBJ_ENCODING_RAW);
    }

    // 7. wire: SET stores deduced fields, GET/TTL serve, wrong type -> -WRONGTYPE.
    {
        clearStore();
        const c = fakeConn();
        evalAndRespond({ Cmd: "SET", Args: ["num", "7"] }, c);
        ok("SET stamps INT", getEncoding(get("num")) === OBJ_ENCODING_INT);
        ok("SET stamps STRING type", getType(get("num")) === OBJ_TYPE_STRING);
        evalAndRespond({ Cmd: "GET", Args: ["num"] }, c);
        ok("GET int value", lastText(c) === "$1\r\n7\r\n", JSON.stringify(lastText(c)));
        put("fake-list", { value: "x", expireAt: -1, typeEncoding: packTypeEncoding(1, 0) });
        evalAndRespond({ Cmd: "GET", Args: ["fake-list"] }, c);
        ok("GET wrong type -> WRONGTYPE", lastText(c).startsWith("-WRONGTYPE"), JSON.stringify(lastText(c)));
        evalAndRespond({ Cmd: "TTL", Args: ["fake-list"] }, c);
        ok("TTL wrong type -> WRONGTYPE", lastText(c).startsWith("-WRONGTYPE"));
        evalAndRespond({ Cmd: "GET", Args: ["num"] }, c); // conn still open
        ok("conn survives WRONGTYPE", lastText(c) === "$1\r\n7\r\n");
    }

    // 8. RDB2 preserves the exact encoding byte across save/load.
    {
        const d = fs.mkdtempSync(path.join(os.tmpdir(), "redis-diceobj-"));
        const p = path.join(d, "dump.rdb");
        clearStore();
        put("i", newObj("99", -1)); // INT
        put("s", newObj("hi", -1)); // EMBSTR
        put("r", newObj("a".repeat(50), -1)); // RAW
        rdb.dumpRDBSync(p);
        clearStore();
        rdb.loadRDB(p);
        ok("rdb keeps INT", getEncoding(get("i")) === OBJ_ENCODING_INT);
        ok("rdb keeps EMBSTR", getEncoding(get("s")) === OBJ_ENCODING_EMBSTR);
        ok("rdb keeps RAW", getEncoding(get("r")) === OBJ_ENCODING_RAW);
        ok("rdb values intact", get("i").value === "99" && get("r").value === "a".repeat(50));
    }

    // 9. RDB1 (legacy, no type byte) still loads with auto encoding.
    {
        const d = fs.mkdtempSync(path.join(os.tmpdir(), "redis-diceobj-v1-"));
        const p = path.join(d, "legacy.rdb");
        const kb = Buffer.from("lk");
        const vb = Buffer.from("123");
        const h = Buffer.alloc(4 + 4 + 4 + kb.length + 4 + vb.length + 8);
        let o = 0;
        Buffer.from("RDB1").copy(h, o); o += 4;
        h.writeUInt32BE(1, o); o += 4;
        h.writeUInt32BE(kb.length, o); o += 4;
        kb.copy(h, o); o += kb.length;
        h.writeUInt32BE(vb.length, o); o += 4;
        vb.copy(h, o); o += vb.length;
        h.writeBigInt64BE(-1n, o);
        fs.writeFileSync(p, h);
        clearStore();
        const l = rdb.loadRDB(p);
        ok("rdb1 loads", l.loaded === 1 && get("lk").value === "123");
        ok("rdb1 auto-encodes INT", getEncoding(get("lk")) === OBJ_ENCODING_INT);
        ok("rdb1 type STRING", getType(get("lk")) === OBJ_TYPE_STRING);
    }

    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main();
