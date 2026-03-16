// opfs_helpers.js — Synchronous OPFS bridge for WASM Web Workers
//
// Architecture:
//   The MAIN THREAD creates a helper worker and SharedArrayBuffers, then
//   passes the buffers to both the helper worker and the WASM worker.
//   The WASM worker calls these exported sync functions which write commands
//   to SharedArrayBuffer, block via Atomics.wait, and read results back.
//   The helper worker processes commands asynchronously and signals
//   completion via Atomics.store + Atomics.notify.
//
// Setup flow:
//   1. Main thread creates helper worker + SharedArrayBuffers
//   2. Main thread sends buffers to helper (postMessage)
//   3. Main thread sends buffers to WASM worker (postMessage)
//   4. WASM worker calls opfs_attach_buffers() before any OPFS operations
//
// This avoids the nested-worker deadlock where a parent worker creates
// a child worker and immediately blocks with Atomics.wait, preventing
// the child from ever receiving its init message.

// ── Shared state (set by opfs_attach_buffers) ───────────────────────

const CMD_SLOTS = 16;
const S_IDLE = 0, S_CMD = 1, S_OK = 2, S_ERR = 3;
const R_VOID = 0, R_FALSE = 1, R_TRUE = 2, R_HANDLE = 3, R_DATA = 4, R_ARRAY = 5;

// Opcodes
const OP_DIR_EXISTS = 1, OP_CREATE_DIR = 2, OP_GET_DIR = 3;
const OP_FILE_EXISTS = 4, OP_READ_FILE = 5, OP_WRITE_FILE = 6;
const OP_LIST_ENTRIES = 7, OP_REMOVE_ENTRY = 8, OP_GET_OPFS_ROOT = 9;

let cmdI32 = null;
let strU8 = null;
let dataBuffer = null;
let dataU8 = null;
let errU8 = null;
let initialized = false;

// ── Initialization ──────────────────────────────────────────────────

/**
 * Attach pre-created SharedArrayBuffers. Must be called before any
 * opfs_* function. The buffers are created by the main thread and
 * shared with both this worker and the helper worker.
 */
export function opfs_attach_buffers(cmd, str, data, err) {
    cmdI32 = new Int32Array(cmd);
    strU8 = new Uint8Array(str);
    dataBuffer = data;
    dataU8 = new Uint8Array(data);
    errU8 = new Uint8Array(err);
    initialized = true;
}

// ── Synchronous call bridge ─────────────────────────────────────────

function writeStr(s, offset) {
    // TextEncoder.encodeInto doesn't work with SharedArrayBuffer views,
    // so we encode to a regular buffer first, then copy.
    const encoded = new TextEncoder().encode(s);
    strU8.set(encoded, offset);
    return encoded.byteLength;
}

function callHelper(opcode, { handleId = 0, str1 = "", str2 = "", bytes = null } = {}) {
    if (!initialized) {
        throw new Error("opfs_helpers: not initialized. Call opfs_attach_buffers() first.");
    }

    // Write string args to shared buffer
    const s1Len = str1 ? writeStr(str1, 0) : 0;
    const s2Len = str2 ? writeStr(str2, s1Len) : 0;

    // Write byte data to shared buffer
    let byteLen = 0;
    if (bytes) {
        dataU8.set(bytes);
        byteLen = bytes.byteLength;
    }

    // Write command fields
    Atomics.store(cmdI32, 1, opcode);
    Atomics.store(cmdI32, 2, handleId);
    Atomics.store(cmdI32, 3, 0);
    Atomics.store(cmdI32, 4, 0);
    Atomics.store(cmdI32, 5, 0);
    Atomics.store(cmdI32, 6, s1Len);
    Atomics.store(cmdI32, 7, s2Len);
    Atomics.store(cmdI32, 8, byteLen);

    // Signal command ready and wake helper
    Atomics.store(cmdI32, 0, S_CMD);
    Atomics.notify(cmdI32, 0);

    // Block until helper signals completion
    while (Atomics.load(cmdI32, 0) === S_CMD) {
        Atomics.wait(cmdI32, 0, S_CMD);
    }

    const status = Atomics.load(cmdI32, 0);
    const rtype = Atomics.load(cmdI32, 4);
    const rval = Atomics.load(cmdI32, 5);

    // Reset to idle for next command
    Atomics.store(cmdI32, 0, S_IDLE);

    if (status === S_ERR) {
        // Copy error bytes out of SharedArrayBuffer before decoding
        const errCopy = new Uint8Array(rval);
        errCopy.set(errU8.slice(0, rval));
        const msg = new TextDecoder().decode(errCopy);
        throw new Error(msg);
    }

    switch (rtype) {
        case R_VOID: return undefined;
        case R_FALSE: return false;
        case R_TRUE: return true;
        case R_HANDLE: return { __hid: rval };
        case R_DATA: {
            // Copy data out of SharedArrayBuffer
            const result = new Uint8Array(rval);
            result.set(new Uint8Array(dataBuffer, 0, rval));
            return result;
        }
        case R_ARRAY: {
            // Copy JSON bytes out of SharedArrayBuffer before decoding
            const jsonCopy = new Uint8Array(rval);
            jsonCopy.set(new Uint8Array(dataBuffer, 0, rval));
            const json = new TextDecoder().decode(jsonCopy);
            return JSON.parse(json);
        }
        default: throw new Error("Unknown result type: " + rtype);
    }
}

function hid(val) {
    if (val && typeof val === "object" && val.__hid !== undefined) return val.__hid;
    throw new Error("Expected handle, got: " + typeof val);
}

// ── Exported synchronous functions (called from Rust via wasm_bindgen) ──

export function opfs_dir_exists(parent, name) {
    return callHelper(OP_DIR_EXISTS, { handleId: hid(parent), str1: name });
}

export function opfs_create_dir(parent, name) {
    return callHelper(OP_CREATE_DIR, { handleId: hid(parent), str1: name });
}

export function opfs_get_dir(parent, name) {
    return callHelper(OP_GET_DIR, { handleId: hid(parent), str1: name });
}

export function opfs_file_exists(dir, name) {
    return callHelper(OP_FILE_EXISTS, { handleId: hid(dir), str1: name });
}

export function opfs_read_file(dir, name) {
    return callHelper(OP_READ_FILE, { handleId: hid(dir), str1: name });
}

export function opfs_write_file(dir, name, data) {
    return callHelper(OP_WRITE_FILE, {
        handleId: hid(dir),
        str1: name,
        bytes: data instanceof Uint8Array ? data : new Uint8Array(data),
    });
}

export function opfs_list_entries(dir) {
    return callHelper(OP_LIST_ENTRIES, { handleId: hid(dir) });
}

export function opfs_remove_entry(dir, name, recursive) {
    return callHelper(OP_REMOVE_ENTRY, {
        handleId: hid(dir),
        str1: name,
        str2: recursive ? "1" : "",
    });
}

/**
 * Synchronously obtain the OPFS root directory handle.
 * The helper worker calls navigator.storage.getDirectory() and
 * stores the result in its handle registry.
 * @param {*} _promise - Ignored (kept for wasm_bindgen signature compat)
 * @returns {{ __hid: number }} Opaque handle reference
 */
export function opfs_block_on_promise(_promise) {
    return callHelper(OP_GET_OPFS_ROOT);
}
