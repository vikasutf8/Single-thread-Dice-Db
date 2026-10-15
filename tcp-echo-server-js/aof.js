"use strict";

// aof.js — Append-Only File persistence (Redis AOF port).
//
// AOF = every successful WRITE command appended as its RESP frame.
// Restart = replay the frames in order. Nothing new on the wire: the file
// is just concatenated `*n\r\n$len\r\n...` frames, one per command.
//
// File layout: ./AOF/dice-master.aof  (tmp rewrite: <path>.tmp)
//   SET k v        -> *3 $3 SET $k k $v v
//   SET k v PX ms  -> same + $2 PX $<n> <ms>   (only in dumpAll/rewrite output)
//   DEL / EXPIRE / PEXPIRE logged verbatim (see eval.js for effective-op rule).
//
// Node "background": single thread, so bg = async fs I/O (never block the
// EPOLLIN dispatch) + a guard flag. Append path itself is a sync O(1) write
// so ordering is exact and crash-safe per command.

const fs = require("node:fs");
const path = require("node:path");
const { readCmd } = require("./redis-cmd");
const { store, NO_EXPIRE } = require("./store");

const DEFAULT_AOF_PATH = path.join(__dirname, "AOF", "dice-master.aof");

let aofPath = DEFAULT_AOF_PATH;
let enabled = false; // appendCommand is a no-op until the server enables it
let rewriting = false; // bgEvalAOF in flight
let rewriteTail = []; // frames appended DURING a rewrite (Redis rewrite buffer)

function configure({ path: p, enabled: en } = {}) {
    if (p !== undefined) aofPath = String(p);
    if (en !== undefined) enabled = Boolean(en);
    if (enabled) fs.mkdirSync(path.dirname(aofPath), { recursive: true });
}

function setEnabled(en) {
    enabled = Boolean(en);
    if (enabled) fs.mkdirSync(path.dirname(aofPath), { recursive: true });
}

function isRewriting() {
    return rewriting;
}

// encodeCommand(cmd) — {Cmd, Args} -> RESP array frame (what redis-cli sends).
function encodeCommand(cmd) {
    const parts = [String(cmd.Cmd), ...(cmd.Args || []).map(String)];
    let out = `*${parts.length}\r\n`;
    for (const p of parts) {
        out += `$${Buffer.byteLength(p)}\r\n${p}\r\n`;
    }
    return Buffer.from(out, "utf8");
}

// appendCommand(cmd) — log one successful write. Never throws: a disk error
// must not break the client connection (logged to console instead).
function appendCommand(cmd) {
    if (!enabled || !cmd || typeof cmd.Cmd !== "string") return;
    const frame = encodeCommand(cmd);
    try {
        fs.appendFileSync(aofPath, frame);
        if (rewriting) rewriteTail.push(frame); // also goes to the new file later
    } catch (err) {
        console.error(`[aof] append failed: ${err.message}`);
    }
}

// buildAOFFrames() — live store -> array of SET frames (rewrite body).
// Expired keys are skipped; TTL keys become SET ... PX <remaining-ms>.
function buildAOFFrames() {
    const now = Date.now();
    const frames = [];
    let keys = 0;
    for (const [key, obj] of store) {
        if (obj.expireAt !== NO_EXPIRE && now > obj.expireAt) continue; // dead: skip
        const cmd = { Cmd: "SET", Args: [key, String(obj.value)] };
        if (obj.expireAt !== NO_EXPIRE) {
            const remain = obj.expireAt - now;
            if (remain <= 0) continue;
            cmd.Args.push("PX", String(remain));
        }
        frames.push(encodeCommand(cmd));
        keys++;
    }
    return { frames, keys };
}

// dumpAllAOF(targetPath) — compact full dump of live store, blocking.
// Used by tests and as the sync core of the background rewrite.
function dumpAllAOF(targetPath) {
    const { frames, keys } = buildAOFFrames();
    const buf = Buffer.concat(frames);
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.writeFileSync(targetPath, buf);
    return { keys, bytes: buf.length };
}

// bgEvalAOF(targetPath?) — BGREWRITEAOF: compact the log without blocking.
// Frame build is ms-scale; the file write is async so EPOLLIN dispatch never
// stalls. Tmp + atomic rename => crash-safe. Commands arriving mid-rewrite
// are appended after the switch (Redis rewrite-buffer behavior).
// Returns a Promise; concurrent calls get {ok:false}.
async function bgEvalAOF(targetPath = aofPath) {
    if (rewriting) return { ok: false, reason: "aof rewrite already in progress" };
    rewriting = true;
    rewriteTail = [];
    const tmp = `${targetPath}.tmp.${process.pid}`;
    try {
        const { frames, keys } = buildAOFFrames();
        fs.mkdirSync(path.dirname(targetPath), { recursive: true });
        await fs.promises.writeFile(tmp, Buffer.concat(frames));
        fs.renameSync(tmp, targetPath); // atomic switch
        if (rewriteTail.length > 0) {
            fs.appendFileSync(targetPath, Buffer.concat(rewriteTail));
        }
        const tail = rewriteTail.length;
        rewriting = false;
        rewriteTail = [];
        console.log(`[aof] rewrite done keys=${keys} tail=${tail} file=${targetPath}`);
        return { ok: true, keys, tail };
    } catch (err) {
        rewriting = false;
        rewriteTail = [];
        try { fs.unlinkSync(tmp); } catch { /* ignore */ }
        console.error(`[aof] rewrite failed: ${err.message}`);
        return { ok: false, reason: err.message };
    }
}

// replayAOF(filePath, applyFn) — startup recovery. Torn tail (incomplete last
// frame from a crash) is ignored; hard corruption stops the replay. applyFn
// receives each parsed {Cmd, Args} (server passes evalAndRespond + null conn).
function replayAOF(filePath, applyFn) {
    let buf;
    try {
        buf = fs.readFileSync(filePath);
    } catch (err) {
        if (err.code === "ENOENT") return { replayed: 0, stoppedOn: "no file" };
        throw err;
    }
    let rest = buf;
    let replayed = 0;
    let stoppedOn = "eof";
    while (rest.length > 0) {
        const { cmd, n, err } = readCmd(rest);
        if (err) {
            if (err.message.startsWith("incomplete")) stoppedOn = "torn tail (ignored)";
            else if (err.message === "empty line") { rest = rest.slice(n); continue; }
            else stoppedOn = `corrupt frame: ${err.message}`;
            break;
        }
        const verb = String(cmd.Cmd).toUpperCase();
        if (verb !== "EXIT" && verb !== "QUIT") {
            applyFn(cmd);
            replayed++;
        }
        rest = rest.slice(n);
    }
    console.log(`[aof] replayed ${replayed} cmds from ${filePath} (${stoppedOn})`);
    return { replayed, stoppedOn };
}

module.exports = {
    configure,
    setEnabled,
    isRewriting,
    encodeCommand,
    appendCommand,
    buildAOFFrames,
    dumpAllAOF,
    bgEvalAOF,
    replayAOF,
    DEFAULT_AOF_PATH,
};
