"use strict";

// In-memory key/value store — Go study ported to Node.js.
// Go: var store map[string]*Obj (init in init())
//     type Obj struct { Value any; ExpireAt int64 }  // ExpireAt = epoch-ms, -1 = persistent
// Here: store = Map<string, Obj>, Obj = { value, expireAt }.
// Expiry is lazy: enforced on get(), no background sweeper.

const NO_EXPIRE = -1;

// init(): store is ready at module load.
const store = new Map();

// newObj(value, durationMs): durationMs > 0 → dies at now+ms, else persistent.
function newObj(value, durationMs) {
    const ms = Number(durationMs);
    return {
        value,
        expireAt: Number.isFinite(ms) && ms > 0 ? Date.now() + ms : NO_EXPIRE,
    };
}

function put(key, obj) {
    store.set(key, obj);
}

// get(key): missing → null; expired → lazy delete + null; else the Obj.
function get(key) {
    const obj = store.get(key);
    if (obj === undefined) return null;
    if (obj.expireAt !== NO_EXPIRE && Date.now() > obj.expireAt) {
        store.delete(key);
        return null;
    }
    return obj;
}

function del(key) {
    return store.delete(key);
}

module.exports = { store, newObj, put, get, del, NO_EXPIRE };
