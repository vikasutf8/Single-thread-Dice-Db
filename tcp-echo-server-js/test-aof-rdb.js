"use strict";

// test-aof-rdb.js — AOF + RDB persistence: append, replay, rewrite, snapshot.
 // All files live under os.tmpdir() — the repo is never polluted.

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const net = require("node:net");
const assert = require("node:assert");
const { readCmd } = require("./redis-cmd");
const { Decode } = require("./resp-decoder");
const { evalAndRespond } = require("./eval");
const { put, get, newObj, clearStore } = require("./store");
const aof = require("./aof");
const rdb = require("./rdb");

let passed = 0;
let failed = 0;

function ok(name, cond, extra = "") {
    if (cond) { passed++; console.log(`PASS ${name}${extra ? " — " + extra : ""}`); }
    else { failed++; console.error(`FAIL ${name}${extra ? " — " + extra : ""}`); }
}

function fakeConn() {
    return { writes: [], write(b) { this.writes.push(Buffer.from(b)); } };
}
function lastText(c) {
    return c.writes.length ? c.writes[c.writes.length - 1].toString() : "<no write>";
}
function tmpDir(tag) {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), `redis-${tag}-`));
    return d;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
    // 1. encodeCommand round-trips through readCmd.
    {
        const f = aof.encodeCommand({ Cmd: "SET", Args: ["k", "v with spaces"] });
        const { cmd, err } = readCmd(f);
        ok("encode round-trip", !err && cmd.Cmd === "SET" && cmd.Args[1] === "v with spaces");
    }

    // 2. Successful writes append; failed writes do not.
    {
        const d = tmpDir("aof-append");
        const p = path.join(d, "dice-master.aof");
        clearStore();
        aof.configure({ path: p, enabled: true });
        const c = fakeConn();
        evalAndRespond({ Cmd: "SET", Args: ["ak", "av"] }, c);
        evalAndRespond({ Cmd: "SET", Args: ["onlykey"] }, c); // arity error: no log
        evalAndRespond({ Cmd: "GET", Args: ["ak"] }, c); // read-only: no log
        const raw = fs.readFileSync(p, "utf8");
        ok("aof logs SET", raw === "*3\r\n$3\r\nSET\r\n$2\r\nak\r\n$2\r\nav\r\n", JSON.stringify(raw));
        aof.setEnabled(false);
    }

    // 3. Replay restores state incl. DEL and EXPIRE-as-DEL.
    {
        const d = tmpDir("aof-replay");
        const p = path.join(d, "dice-master.aof");
        clearStore();
        aof.configure({ path: p, enabled: true });
        const c = fakeConn();
        evalAndRespond({ Cmd: "SET", Args: ["r1", "v1"] }, c);
        evalAndRespond({ Cmd: "SET", Args: ["r2", "v2"] }, c);
        evalAndRespond({ Cmd: "DEL", Args: ["r2"] }, c);
        evalAndRespond({ Cmd: "SET", Args: ["r3", "v3"] }, c);
        evalAndRespond({ Cmd: "EXPIRE", Args: ["r3", "0"] }, c); // effective DEL
        aof.setEnabled(false);
        clearStore();
        const r = aof.replayAOF(p, (cmd) => evalAndRespond(cmd, fakeConn()));
        ok("replay count", r.replayed === 5, `replayed=${r.replayed}`);
        ok("replay r1 kept", get("r1") !== null && get("r1").value === "v1");
        ok("replay r2 deleted", get("r2") === null);
        ok("replay EXPIRE-0 deleted r3", get("r3") === null);
    }

    // 4. Torn tail (crash mid-write) is ignored, earlier cmds replay.
    {
        const d = tmpDir("aof-torn");
        const p = path.join(d, "dice-master.aof");
        fs.writeFileSync(p, "*3\r\n$3\r\nSET\r\n$2\r\nt1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nt2\r\n$2");
        clearStore();
        const r = aof.replayAOF(p, (cmd) => evalAndRespond(cmd, fakeConn()));
        ok("torn tail replays prefix", r.replayed === 1 && get("t1") !== null && get("t1").value === "v1");
        ok("torn tail noted", String(r.stoppedOn).startsWith("torn tail"));
    }

    // 5. dumpAllAOF compacts + preserves TTL as PX.
    {
        const d = tmpDir("aof-dump");
        const p = path.join(d, "dice-master.aof");
        clearStore();
        put("plain", newObj("a", -1));
        put("ttl", newObj("b", 60000));
        put("gone", { value: "x", expireAt: Date.now() - 1000 }); // already dead
        const r = aof.dumpAllAOF(p);
        ok("dump skips expired", r.keys === 2, `keys=${r.keys}`);
        clearStore();
        aof.replayAOF(p, (cmd) => evalAndRespond(cmd, fakeConn()));
        ok("dump plain kept", get("plain") !== null);
        const t = get("ttl");
        ok("dump ttl kept with expiry", t !== null && t.expireAt !== -1 && t.expireAt - Date.now() > 30000);
        ok("dump dead gone", get("gone") === null);
    }

    // 6. bgEvalAOF rewrite keeps mid-rewrite appends (rewrite buffer).
    {
        const d = tmpDir("aof-bg");
        const p = path.join(d, "dice-master.aof");
        clearStore();
        aof.configure({ path: p, enabled: true });
        put("w1", newObj("a", -1));
        aof.appendCommand({ Cmd: "SET", Args: ["w1", "a"] });
        const pr = aof.bgEvalAOF(p); // starts rewrite (async write lands later)
        aof.appendCommand({ Cmd: "SET", Args: ["w2", "b"] }); // same tick: mid-rewrite
        put("w2", newObj("b", -1));
        const r = await pr;
        ok("bg rewrite ok", r.ok === true && r.tail === 1, JSON.stringify(r));
        clearStore();
        aof.setEnabled(false);
        aof.replayAOF(p, (cmd) => evalAndRespond(cmd, fakeConn()));
        ok("rewrite kept pre-image", get("w1") !== null);
        ok("rewrite kept tail write", get("w2") !== null && get("w2").value === "b");
    }

    // 7. RDB round-trip incl. expired-skip; bad magic throws; missing file ok.
    {
        const d = tmpDir("rdb");
        const p = path.join(d, "dump.rdb");
        clearStore();
        put("rk", newObj("rv", -1));
        put("rt", newObj("rv2", 60000));
        const s = rdb.dumpRDBSync(p);
        ok("rdb save", s.ok && s.keys === 2, JSON.stringify(s));
        const magic = fs.readFileSync(p).slice(0, 4).toString();
        ok("rdb magic", magic === "RDB1");
        clearStore();
        const l = rdb.loadRDB(p);
        ok("rdb load", l.loaded === 2 && get("rk").value === "rv" && get("rt").value === "rv2");
        // expired at save time never enters the snapshot
        put("old", { value: "z", expireAt: Date.now() - 10 });
        rdb.dumpRDBSync(p);
        clearStore();
        const l2 = rdb.loadRDB(p);
        ok("rdb skips expired", l2.loaded === 2 && get("old") === null);
        // short-TTL key dies between snapshot and load -> skipped at load
        clearStore();
        put("fast", newObj("f", 60));
        rdb.dumpRDBSync(p);
        await sleep(120);
        clearStore();
        const l3 = rdb.loadRDB(p);
        ok("rdb drops key that died after snapshot", l3.skipped === 1 && get("fast") === null);
        fs.writeFileSync(path.join(d, "bad.rdb"), "NOPE");
        let threw = false;
        try { rdb.loadRDB(path.join(d, "bad.rdb")); } catch { threw = true; }
        ok("rdb bad magic throws", threw);
        ok("rdb missing file", rdb.loadRDB(path.join(d, "nope.rdb")).noFile === true);
    }

    // 8. BGSAVE / BGREWRITEAOF / SAVE dispatch.
    {
        const d = tmpDir("verbs");
        rdb.configure({ path: path.join(d, "dump.rdb") });
        aof.configure({ path: path.join(d, "dice-master.aof"), enabled: true });
        clearStore();
        put("vk", newObj("vv", -1));
        let c = fakeConn();
        evalAndRespond({ Cmd: "BGSAVE" }, c);
        ok("bgsave reply", lastText(c) === "+Background saving started\r\n", JSON.stringify(lastText(c)));
        await sleep(150); // let the async write land
        ok("bgsave file exists", fs.existsSync(path.join(d, "dump.rdb")));
        c = fakeConn();
        evalAndRespond({ Cmd: "BGREWRITEAOF" }, c);
        ok("bgrewriteaof reply", lastText(c).startsWith("+Background append only"), JSON.stringify(lastText(c)));
        await sleep(150);
        ok("bgrewriteaof file exists", fs.existsSync(path.join(d, "dice-master.aof")));
        c = fakeConn();
        evalAndRespond({ Cmd: "SAVE" }, c);
        ok("save reply", lastText(c) === "+OK\r\n");
        c = fakeConn();
        evalAndRespond({ Cmd: "BGSAVE", Args: ["x"] }, c);
        ok("bgsave arity error", lastText(c).startsWith("-ERR"));
        aof.setEnabled(false);
    }

    // 9. Server restart over TCP: AOF recovery.
    {
        const d = tmpDir("srv-aof");
        const ap = path.join(d, "dice-master.aof");
        const rp = path.join(d, "dump.rdb");
        const { RunAsyncTcpServer } = require("./async.tcp.js");
        const talk = (port, frame) => new Promise((res, rej) => {
            const c = net.connect(port, "127.0.0.1", () => c.write(frame));
            let buf = Buffer.alloc(0);
            const t = setTimeout(() => { c.destroy(); rej(new Error("timeout")); }, 2000);
            c.on("data", (x) => { buf = Buffer.concat([buf, x]); });
            c.on("error", (e) => { clearTimeout(t); rej(e); });
            setTimeout(() => { clearTimeout(t); c.end(); res(buf.toString()); }, 300);
        });
        clearStore();
        const s1 = RunAsyncTcpServer("127.0.0.1", 0, { aofEnabled: true, aofPath: ap, rdbPath: rp });
        const p1 = await new Promise((r) => s1.server.on("listening", () => r(s1.server.address().port)));
        const setFrame = "*3\r\n$3\r\nSET\r\n$7\r\nsurvive\r\n$5\r\nhello\r\n";
        await talk(p1, setFrame);
        await new Promise((r) => s1.close(r));
        ok("aof file written by server", fs.existsSync(ap));
        clearStore(); // simulate process death: memory gone, disk remains
        const s2 = RunAsyncTcpServer("127.0.0.1", 0, { aofEnabled: true, aofPath: ap, rdbPath: rp });
        const p2 = await new Promise((r) => s2.server.on("listening", () => r(s2.server.address().port)));
        const got = await talk(p2, "*2\r\n$3\r\nGET\r\n$7\r\nsurvive\r\n");
        ok("restart recovers key via AOF", got.includes("hello"), JSON.stringify(got));
        await new Promise((r) => s2.close(r));
    }

    // 10. Server restart over TCP: RDB recovery (SAVE then rdbLoad).
    {
        const d = tmpDir("srv-rdb");
        const ap = path.join(d, "dice-master.aof");
        const rp = path.join(d, "dump.rdb");
        const { RunAsyncTcpServer } = require("./async.tcp.js");
        const talk = (port, frame) => new Promise((res, rej) => {
            const c = net.connect(port, "127.0.0.1", () => c.write(frame));
            let buf = Buffer.alloc(0);
            const t = setTimeout(() => { c.destroy(); rej(new Error("timeout")); }, 2000);
            c.on("data", (x) => { buf = Buffer.concat([buf, x]); });
            c.on("error", (e) => { clearTimeout(t); rej(e); });
            setTimeout(() => { clearTimeout(t); c.end(); res(buf.toString()); }, 300);
        });
        clearStore();
        const s1 = RunAsyncTcpServer("127.0.0.1", 0, { aofEnabled: false, rdbPath: rp });
        const p1 = await new Promise((r) => s1.server.on("listening", () => r(s1.server.address().port)));
        await talk(p1, "*3\r\n$3\r\nSET\r\n$6\r\nsnap-k\r\n$6\r\nsnap-v\r\n");
        await talk(p1, "*1\r\n$4\r\nSAVE\r\n");
        await sleep(100);
        await new Promise((r) => s1.close(r));
        ok("rdb file written by SAVE", fs.existsSync(rp));
        clearStore();
        const s2 = RunAsyncTcpServer("127.0.0.1", 0, { rdbLoad: true, rdbPath: rp });
        const p2 = await new Promise((r) => s2.server.on("listening", () => r(s2.server.address().port)));
        const got = await talk(p2, "*2\r\n$3\r\nGET\r\n$6\r\nsnap-k\r\n");
        ok("restart recovers key via RDB", got.includes("snap-v"), JSON.stringify(got));
        await new Promise((r) => s2.close(r));
        void ap;
    }

    aof.setEnabled(false);
    console.log(`\n${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => { console.error("FATAL", e); process.exit(1); });
