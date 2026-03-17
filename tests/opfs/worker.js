// Web Worker that loads the WASM module and runs all test_opfs_* functions.
// Results are reported back to the main thread via postMessage.
//
// The main thread (index.html) creates a sibling helper worker and sends
// SharedArrayBuffer references to this worker. We must call opfs_attach_buffers()
// before initializing WASM so the Atomics bridge is ready.

import init, * as wasm from "../../pkg/terminus_store_wasm.js";
import { opfs_attach_buffers } from "../../pkg/snippets/terminus-store-wasm-059824aac4d229a3/src/opfs_helpers.js";

// Wait for shared buffers from main thread, then run tests
self.onmessage = async (e) => {
  if (e.data.type !== "init") return;

  const { cmd, str, data, err } = e.data;

  try {
    // Attach shared buffers to the OPFS helpers module
    opfs_attach_buffers(cmd, str, data, err);

    self.postMessage({ type: "log", msg: "Buffers attached, initializing WASM..." });

    // Initialize the WASM module
    await init();

    self.postMessage({ type: "log", msg: "WASM initialized, discovering tests..." });

    // Discover all test_opfs_* functions
    const testFns = Object.keys(wasm)
      .filter((name) => name.startsWith("test_opfs_"))
      .sort();

    self.postMessage({ type: "log", msg: `Found ${testFns.length} tests` });

    if (testFns.length === 0) {
      self.postMessage({ type: "done", results: [] });
      return;
    }

    const results = [];

    for (const name of testFns) {
      let passed = false;
      let error = null;

      try {
        wasm[name]();
        passed = true;
      } catch (e) {
        error = e instanceof Error ? e.message : String(e);
      }

      results.push({ name, passed, error });
      self.postMessage({ type: "test_result", name, error });
    }

    self.postMessage({ type: "done", results });
  } catch (e) {
    self.postMessage({
      type: "done",
      results: [{
        name: "initialization",
        passed: false,
        error: e instanceof Error ? e.message : String(e),
      }],
    });
  }
};
