# OPFS Integration Tests

Browser-based integration tests for the `OpfsPersistence` backend, which uses the [Origin Private File System](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system) API for persistent storage in WASM.

## Why not `wasm-pack test`?

The OPFS backend requires `Atomics.wait` and `FileSystemSyncAccessHandle`, both of which are only available inside a **Web Worker**. Standard `wasm-pack test` / `wasm-bindgen-test` runs on the main thread, so these APIs are unavailable there.

Instead, the tests run inside a dedicated Web Worker in a headless Chrome instance.

## Architecture

```
index.html (main thread)
  ├── helper worker (Blob) — executes async OPFS operations
  │     communicates via SharedArrayBuffer + Atomics
  └── worker.js (WASM worker) — loads the WASM module, runs test_opfs_* functions
        calls opfs_helpers.js sync bridge → blocks on Atomics.wait
```

The main thread creates both workers as siblings and passes `SharedArrayBuffer` references to each. This avoids the nested-worker deadlock where a parent worker that blocks with `Atomics.wait` prevents its child from ever receiving messages.

## Prerequisites

- `wasm-pack` (`cargo install wasm-pack`)
- Node.js
- Puppeteer (`npm install` from the project root — it's a dev dependency)
- Google Chrome (Puppeteer downloads its own, or uses the system install)

## Running

From the `terminus-store-wasm` directory:

```bash
# Build WASM + run tests
./tests/opfs/run.sh

# Skip the build, just run tests (if you already built)
./tests/opfs/run.sh --no-build
```

Or manually:

```bash
# Terminal 1: start the test server
node tests/opfs/server.js

# Terminal 2: run headless browser tests
node tests/opfs/run_tests.js
```

To watch tests in a real browser, start the server and open `http://localhost:8787/`.

## Cross-Origin Isolation

`SharedArrayBuffer` requires [Cross-Origin Isolation](https://web.dev/articles/cross-origin-isolation-guide). The test server (`server.js`) sets the required headers on all responses:

- `Cross-Origin-Opener-Policy: same-origin`
- `Cross-Origin-Embedder-Policy: require-corp`

## Test inventory (14 tests)

| Test | What it covers |
|------|---------------|
| `test_opfs_smoke` | Create persistence, verify empty layer list |
| `test_opfs_layer_create_and_exists` | Create layer dir, check existence |
| `test_opfs_layer_write_read_roundtrip` | Write file to layer, read back, assert byte equality |
| `test_opfs_file_exists_check` | `file_exists` before/after write |
| `test_opfs_list_layers` | Multiple layers listed, label files skipped |
| `test_opfs_delete_layer` | Create then delete layer |
| `test_opfs_create_duplicate_layer` | Duplicate creation returns `AlreadyExists` |
| `test_opfs_label_crud` | Create, get, set, delete labels |
| `test_opfs_label_optimistic_concurrency` | Stale version returns `None`, correct version succeeds |
| `test_opfs_label_plaintext_roundtrip` | Raw `.label` file matches `<version>\n<hex>\n` format |
| `test_opfs_cross_backend_equivalence` | Same operations on OPFS and Memory produce identical results |
| `test_opfs_e2e_store_workflow` | Full store: base layer → child layer → delta chain queries |
| `test_opfs_e2e_rollup` | Rollup preserves triples correctly |
| `test_opfs_e2e_export_import` | Export from OPFS, import into Memory, verify identical triples |

## Files

- `run.sh` — one-command test runner (build + serve + headless test)
- `server.js` — static file server with COOP/COEP headers
- `index.html` — test harness, creates helper + WASM workers
- `worker.js` — WASM worker, discovers and runs `test_opfs_*` functions
- `run_tests.js` — Puppeteer script for headless Chrome execution
- `../../src/opfs_helpers.js` — Atomics-based sync bridge (SharedArrayBuffer protocol)
- `../../src/storage/opfs_tests.rs` — Rust test functions (`#[wasm_bindgen]` exports)
