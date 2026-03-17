// helper.js — Async OPFS worker for the Atomics bridge.
// Runs as a sibling worker to the WASM worker.
// Communication is via SharedArrayBuffer + Atomics.
"use strict";

let cmdI32, strU8, dataU8, errU8;
const handles = new Map();
let nextId = 1;
function storeHandle(obj) { const id = nextId++; handles.set(id, obj); return id; }

const OP_DIR_EXISTS=1,OP_CREATE_DIR=2,OP_GET_DIR=3,OP_FILE_EXISTS=4;
const OP_READ_FILE=5,OP_WRITE_FILE=6,OP_LIST_ENTRIES=7,OP_REMOVE_ENTRY=8,OP_GET_OPFS_ROOT=9;
const S_IDLE=0,S_CMD=1,S_OK=2,S_ERR=3;
const R_VOID=0,R_FALSE=1,R_TRUE=2,R_HANDLE=3,R_DATA=4,R_ARRAY=5;

self.onmessage = (e) => {
    if (e.data.type === "init") {
        cmdI32 = new Int32Array(e.data.cmd);
        strU8 = new Uint8Array(e.data.str);
        dataU8 = new Uint8Array(e.data.data);
        errU8 = new Uint8Array(e.data.err);
        waitForCommand();
    }
};

async function waitForCommand() {
    while (true) {
        const r = Atomics.waitAsync(cmdI32, 0, S_IDLE);
        if (r.async) await r.value;
        if (Atomics.load(cmdI32, 0) !== S_CMD) continue;
        try { await processCommand(); }
        catch (err) {
            const msg = err.message || String(err);
            const enc = new TextEncoder().encode(msg);
            const len = Math.min(enc.byteLength, errU8.byteLength);
            errU8.set(enc.subarray(0, len));
            Atomics.store(cmdI32, 5, len);
            Atomics.store(cmdI32, 0, S_ERR);
            Atomics.notify(cmdI32, 0);
        }
    }
}

function readStr(off, len) { return new TextDecoder().decode(strU8.slice(off, off+len)); }

async function processCommand() {
    const op=Atomics.load(cmdI32,1), hid1=Atomics.load(cmdI32,2);
    const s1L=Atomics.load(cmdI32,6), s2L=Atomics.load(cmdI32,7), bL=Atomics.load(cmdI32,8);
    const h1=hid1?handles.get(hid1):null;
    const s1=s1L>0?readStr(0,s1L):"", s2=s2L>0?readStr(s1L,s2L):"";
    const bytes=bL>0?new Uint8Array(dataU8.slice(0,bL)):null;
    let result;
    switch(op){
        case OP_DIR_EXISTS:{try{await h1.getDirectoryHandle(s1);result=true;}catch(e){if(e.name==="NotFoundError"){result=false;}else throw e;}break;}
        case OP_CREATE_DIR:result=await h1.getDirectoryHandle(s1,{create:true});break;
        case OP_GET_DIR:result=await h1.getDirectoryHandle(s1);break;
        case OP_FILE_EXISTS:{try{await h1.getFileHandle(s1);result=true;}catch(e){if(e.name==="NotFoundError"){result=false;}else throw e;}break;}
        case OP_READ_FILE:{const fh=await h1.getFileHandle(s1);const sh=await fh.createSyncAccessHandle();try{const sz=sh.getSize();const buf=new Uint8Array(sz);sh.read(buf,{at:0});result=buf;}finally{sh.close();}break;}
        case OP_WRITE_FILE:{const fh=await h1.getFileHandle(s1,{create:true});const sh=await fh.createSyncAccessHandle();try{sh.truncate(0);sh.write(bytes,{at:0});sh.flush();}finally{sh.close();}result=undefined;break;}
        case OP_LIST_ENTRIES:{const entries=[];for await(const[name,handle]of h1.entries()){entries.push([name,handle.kind]);}result=entries;break;}
        case OP_REMOVE_ENTRY:await h1.removeEntry(s1,{recursive:s2==="1"});result=undefined;break;
        case OP_GET_OPFS_ROOT:result=await navigator.storage.getDirectory();break;
        default:throw new Error("Unknown op: "+op);
    }
    if(result===undefined||result===null){Atomics.store(cmdI32,4,R_VOID);}
    else if(result===true){Atomics.store(cmdI32,4,R_TRUE);}
    else if(result===false){Atomics.store(cmdI32,4,R_FALSE);}
    else if(result instanceof Uint8Array){dataU8.set(result);Atomics.store(cmdI32,5,result.byteLength);Atomics.store(cmdI32,4,R_DATA);}
    else if(Array.isArray(result)){const j=JSON.stringify(result);const e=new TextEncoder().encode(j);dataU8.set(e);Atomics.store(cmdI32,5,e.byteLength);Atomics.store(cmdI32,4,R_ARRAY);}
    else{const id=storeHandle(result);Atomics.store(cmdI32,5,id);Atomics.store(cmdI32,4,R_HANDLE);}
    Atomics.store(cmdI32,0,S_OK);
    Atomics.notify(cmdI32,0);
}
