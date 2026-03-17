# terminus-store-wasm, a synchronous data store for triple data

[![Build Status](https://github.com/likelion/terminus-store-wasm/workflows/Build%20and%20test/badge.svg)](https://github.com/likelion/terminus-store-wasm/actions)
[![codecov](https://codecov.io/github/likelion/terminus-store-wasm/branch/main/graph/badge.svg)](https://app.codecov.io/github/likelion/terminus-store-wasm)

## Overview
This library implements a way to store triple data - data that
consists of a subject, predicate and an object, where object can
either be some value, or a node (a string that can appear both in
subject and object position).

An example of triple data is:
````
cow says value(moo).
duck says value(quack).
cow likes node(duck).
duck hates node(cow).
````
In `cow says value(moo)`, `cow` is the subject, `says` is the
predicate, and `value(moo)` is the object.

In `cow likes node(duck)`, `cow` is the subject, `likes` is the
predicate, and `node(duck)` is the object.

terminus-store-wasm allows you to store a lot of such facts, and
search through them efficiently.

This is a synchronous, WASM-compatible fork of
[terminus-store](https://github.com/terminusdb/terminusdb-store). It
replaces the tokio async runtime with a fully synchronous API, making
it suitable for compilation to WebAssembly (both browser and Node.js
targets) as well as native use.

### Storage backends
- **Memory** — in-process, no persistence
- **Filesystem** — native targets via `std::fs`
- **OPFS** — browser targets via the Origin Private File System API

Layer data is binary-compatible with the original terminus-store, so
databases created by either version can be read by the other.

## Usage
Add the dependency pointing at this repository:

```toml
[dependencies]
terminus-store-wasm = { git = "https://github.com/likelion/terminus-store-wasm" }
```

Open a memory-backed store:
```rust
use terminus_store_wasm::open_memory_store;

let store = open_memory_store();
```

Or a directory-backed store (native only):
```rust
use terminus_store_wasm::open_directory_store;

let store = open_directory_store("/path/to/store").unwrap();
```

### OPFS (browser)

The OPFS backend persists data to the browser's
[Origin Private File System](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system).
It must run inside a Web Worker, and the serving page must set
Cross-Origin Isolation headers:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

From the main page, create shared buffers and spawn two workers — a
helper worker for async OPFS operations and a WASM worker that runs
the store:

```html
<script type="module">
  // Shared buffers for the Atomics bridge between workers
  const cmdBuf  = new SharedArrayBuffer(16 * 4);
  const strBuf  = new SharedArrayBuffer(64 * 1024);
  const dataBuf = new SharedArrayBuffer(4 * 1024 * 1024);
  const errBuf  = new SharedArrayBuffer(4 * 1024);

  // Start the helper worker (ships as tests/opfs/helper.js)
  const helper = new Worker("helper.js");
  helper.postMessage({ type: "init", cmd: cmdBuf, str: strBuf, data: dataBuf, err: errBuf });

  // Start your WASM worker
  const wasm = new Worker("worker.js", { type: "module" });
  wasm.postMessage({ type: "init", cmd: cmdBuf, str: strBuf, data: dataBuf, err: errBuf });
</script>
```

In your WASM worker, attach the buffers before initialising the
module. `opfs_attach_buffers` is provided automatically by `wasm-pack
build` in `pkg/snippets/`:

```js
// worker.js
import init, * as wasm from "./pkg/terminus_store_wasm.js";
import { opfs_attach_buffers } from "./pkg/snippets/<crate-hash>/src/opfs_helpers.js";

self.onmessage = async (e) => {
  if (e.data.type !== "init") return;
  opfs_attach_buffers(e.data.cmd, e.data.str, e.data.data, e.data.err);
  await init();

  // Store API is Rust-side — export your own #[wasm_bindgen] functions
  // that use OpfsPersistence + open_persistence_store, then call them here.
  // See tests/opfs/worker.js for an example.
};
```

The store itself is a Rust API. To use it from JavaScript, write
`#[wasm_bindgen]`-exported Rust functions that create and query the
store, then call those from your worker. For example:

```rust
use terminus_store_wasm::storage::OpfsPersistence;
use terminus_store_wasm::store::open_persistence_store;
use terminus_store_wasm::layer::{Layer, ValueTriple};

#[wasm_bindgen]
pub fn my_store_operation() -> Result<(), JsValue> {
    let root = get_opfs_root()?;
    let store = open_persistence_store(OpfsPersistence::new(root));

    let db = store.create("main").map_err(io_err)?;
    let builder = store.create_base_layer().map_err(io_err)?;
    builder.add_value_triple(ValueTriple::new_node("alice", "knows", "bob"))
           .map_err(io_err)?;
    let layer = builder.commit().map_err(io_err)?;
    db.set_head(&layer).map_err(io_err)?;
    Ok(())
}
```

Then from the worker: `wasm.my_store_operation()`.

A complete working example lives in [`tests/opfs/`](tests/opfs/).
Run it with `make test-opfs`.

## License
terminus-store is licensed under Apache 2.0.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)

## See also
- The original async terminus-store: [GitHub](https://github.com/terminusdb/terminusdb-store)
- The Terminus database: [Website](https://terminusdb.com) — [GitHub](https://github.com/terminusdb/)
- The HDT format, which the layer format is based on: [Website](http://www.rdfhdt.org/)
