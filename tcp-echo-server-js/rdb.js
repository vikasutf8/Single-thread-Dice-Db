"use strict";

// rdb.js — RDB snapshot persistence (Redis RDB port, minimal custom format).
//
// RDB = point-in-time binary dump of the whole store Map. Small + fast to
// load; AOF replays on top of it for the commands after the snapshot.
//
// Format (all integers big-endian):
//   magic      4 bytes  "RDB2" (v1 "RDB1" still loads: entries without type byte)
//   count      u32      number of entries
//   per entry: u32 klen + key bytes + u32 vlen + val bytes +
//              u8 typeEncoding + i64 expireAtMs (expireAtMs = -1 => persistent)
// No checksum in v1 — trailing bytes after `count` entries = corrupt file.
//
// Node "background": snapshot build over 20k keys is a few ms; the file
// write goes through fs.promises so the EPOLLIN loop never blocks, and a
// guard flag rejects overlapping saves (same as Redis "already in progress").

const fs = require("node:fs");
const path = require("node:path");
const { put, newObjAbs, packTypeEncoding, OBJ_TYPE_STRING, OBJ_ENCODING_RAW, NO_EXPIRE } = require("./store");

const DEFAULT_RDB_PATH = path.join(__dirname, "RDB", "dump.rdb");
const MAGIC = Buffer.from("RDB2", "utf8");
const MAGIC_V1 = Buffer.from("RDB1", "utf8");

let rdbPath = DEFAULT_RDB_PATH;
let saving = false; // bgEvalRDB in flight
let lastSaveAt = 0; // epoch-ms of last successful save
let lastSaveKeys = 0;

function configure({ path: p } = {}) {
    if (p !== undefined) rdbPath = String(p);
}

function isSaving() {
    return saving;
}

function lastSave() {
    return { at: lastSaveAt, keys: lastSaveKeys };
}

// buildSnapshot() — store -> one Buffer. Skips already-expired keys.
// Legacy raw objs (no typeEncoding) snapshot as STRING/RAW.
function buildSnapshot(entries) {
    const now = Date.now();
    const parts = [MAGIC, Buffer.alloc(4)];
    let count = 0;
    for (const [key, obj] of entries) {
        if (obj.expireAt !== NO_EXPIRE && now > obj.expireAt) continue;
        const kb = Buffer.from(String(key), "utf8");
        const vb = Buffer.from(String(obj.value), "utf8");
        const te = obj.typeEncoding ?? packTypeEncoding(OBJ_TYPE_STRING, OBJ_ENCODING_RAW);
        const h = Buffer.alloc(4 + kb.length + 4 + vb.length + 1 + 8);
        let o = 0;
        h.writeUInt32BE(kb.length, o); o += 4;
        kb.copy(h, o); o += kb.length;
        h.writeUInt32BE(vb.length, o); o += 4;
        vb.copy(h, o); o += vb.length;
        h.writeUInt8(te, o); o += 1;
        h.writeBigInt64BE(BigInt(obj.expireAt), o);
        parts.push(h);
        count++;
    }
    parts[1].writeUInt32BE(count, 0);
    return { buf: Buffer.concat(parts), keys: count };
}

// dumpRDBSync(targetPath?) — SAVE: blocking snapshot (terminal use, tests).
function dumpRDBSync(targetPath = rdbPath, entries) {
    const { store } = require("./store");
    const { buf, keys } = buildSnapshot(entries ?? store);
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    const tmp = `${targetPath}.tmp.${process.pid}`;
    fs.writeFileSync(tmp, buf);
    fs.renameSync(tmp, targetPath); // atomic switch
    lastSaveAt = Date.now();
    lastSaveKeys = keys;
    console.log(`[rdb] saved keys=${keys} bytes=${buf.length} file=${targetPath}`);
    return { ok: true, keys, bytes: buf.length };
}

// bgEvalRDB(targetPath?) — BGSAVE: snapshot without stalling clients.
// Replies +OK immediately; the write lands async. Overlap -> {ok:false}.
async function bgEvalRDB(targetPath = rdbPath, entries) {
    if (saving) return { ok: false, reason: "rdb save already in progress" };
    saving = true;
    const { store } = require("./store");
    const { buf, keys } = buildSnapshot(entries ?? store);
    const tmp = `${targetPath}.tmp.${process.pid}`;
    try {
        fs.mkdirSync(path.dirname(targetPath), { recursive: true });
        await fs.promises.writeFile(tmp, buf);
        fs.renameSync(tmp, targetPath); // atomic switch
        lastSaveAt = Date.now();
        lastSaveKeys = keys;
        console.log(`[rdb] background saved keys=${keys} bytes=${buf.length} file=${targetPath}`);
        return { ok: true, keys, bytes: buf.length };
    } catch (err) {
        try { fs.unlinkSync(tmp); } catch { /* ignore */ }
        console.error(`[rdb] background save failed: ${err.message}`);
        return { ok: false, reason: err.message };
    } finally {
        saving = false;
    }
}

// loadRDB(filePath, putFn?) — startup recovery. Throws on corrupt magic;
// expired keys are skipped (they died while we were down). Missing file
// returns {loaded:0} so first boot with no snapshot is normal.
function loadRDB(filePath, putFn = put) {
    let buf;
    try {
        buf = fs.readFileSync(filePath);
    } catch (err) {
        if (err.code === "ENOENT") return { loaded: 0, skipped: 0, noFile: true };
        throw err;
    }
    if (buf.length < 8 || (!buf.slice(0, 4).equals(MAGIC) && !buf.slice(0, 4).equals(MAGIC_V1))) {
        throw new Error(`bad rdb magic in ${filePath}`);
    }
    const v1 = buf.slice(0, 4).equals(MAGIC_V1); // legacy entries carry no type byte
    const count = buf.readUInt32BE(4);
    let o = 8;
    let loaded = 0;
    let skipped = 0;
    const now = Date.now();
    for (let i = 0; i < count; i++) {
        if (o + 4 > buf.length) throw new Error(`truncated rdb key len (entry ${i})`);
        const klen = buf.readUInt32BE(o); o += 4;
        if (o + klen + 4 > buf.length) throw new Error(`truncated rdb key (entry ${i})`);
        const key = buf.slice(o, o + klen).toString("utf8"); o += klen;
        const vlen = buf.readUInt32BE(o); o += 4;
        const tailNeed = v1 ? 8 : 1 + 8; // v2 stores u8 typeEncoding before expireAt
        if (o + vlen + tailNeed > buf.length) throw new Error(`truncated rdb val (entry ${i})`);
        const value = buf.slice(o, o + vlen).toString("utf8"); o += vlen;
        let otype; // undefined => legacy RDB1 entry => newObjAbs auto-detects
        let oEnc;
        if (!v1) {
            const te = buf.readUInt8(o); o += 1;
            otype = (te >> 4) & 0x0f;
            oEnc = te & 0x0f;
        }
        const expireAt = Number(buf.readBigInt64BE(o)); o += 8;
        if (expireAt !== NO_EXPIRE && now > expireAt) { skipped++; continue; }
        putFn(key, newObjAbs(value, expireAt, otype, oEnc));
        loaded++;
    }
    if (o !== buf.length) throw new Error(`trailing bytes in ${filePath}`);
    console.log(`[rdb] loaded ${loaded} keys (skipped ${skipped} expired) from ${filePath}`);
    return { loaded, skipped };
}

module.exports = {
    configure,
    isSaving,
    lastSave,
    buildSnapshot,
    dumpRDBSync,
    bgEvalRDB,
    loadRDB,
    DEFAULT_RDB_PATH,
    MAGIC,
    MAGIC_V1,
};
