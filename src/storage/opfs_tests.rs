#![cfg(target_arch = "wasm32")]

//! OPFS integration tests for the OpfsPersistence backend.
//!
//! These tests are exported via `#[wasm_bindgen]` and run inside a dedicated
//! Web Worker via the headless browser test harness in `tests/opfs/`.
//!
//! Standard `wasm-pack test` / `wasm-bindgen-test` cannot be used because
//! those run on the main thread, where `Atomics.wait` and
//! `FileSystemSyncAccessHandle` are unavailable.
//!
//! Each test function:
//! - Is gated with `#[cfg(target_arch = "wasm32")]`
//! - Has the signature `pub fn test_opfs_*() -> Result<(), JsValue>`
//! - Calls `cleanup_opfs_root()` before running for isolation

use js_sys::Array;
use wasm_bindgen::prelude::*;

use super::layer::name_to_string;
use super::memory_persistence::MemoryPersistence;
use super::opfs_persistence::OpfsPersistence;
use super::persistence::{LabelPersistence, LayerPersistence};

// ── JS helper imports (reuse from opfs_helpers.js) ──────────────────

#[wasm_bindgen(module = "/src/opfs_helpers.js")]
extern "C" {
    #[wasm_bindgen(catch)]
    fn opfs_list_entries(
        dir: &web_sys::FileSystemDirectoryHandle,
    ) -> Result<Array, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_remove_entry(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
        recursive: bool,
    ) -> Result<(), JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_read_file(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<js_sys::Uint8Array, JsValue>;
}

// ── OPFS root acquisition ───────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// Get the OPFS root directory handle via `navigator.storage.getDirectory()`.
    /// This is an async API, but we call it from the worker init path.
    #[wasm_bindgen(js_namespace = ["navigator", "storage"], js_name = "getDirectory")]
    fn get_opfs_root() -> js_sys::Promise;
}

/// Synchronously obtain the OPFS root directory handle.
/// Uses `Atomics.wait`-based blocking since we're in a Worker.
fn get_root() -> Result<web_sys::FileSystemDirectoryHandle, JsValue> {
    // We use wasm_bindgen_futures isn't available in sync context,
    // so we use the JS helper's blocking pattern instead.
    // For test setup, we use a simpler approach: call the JS global directly.
    let promise = get_opfs_root();
    // Block on the promise using a spin + Atomics approach
    // Actually, we need to use the same Atomics bridge. Let's import a helper.
    block_on_promise(promise)
}

#[wasm_bindgen(module = "/src/opfs_helpers.js")]
extern "C" {
    /// Block on a JS Promise synchronously using Atomics.wait.
    /// This must be called from a Worker thread.
    #[wasm_bindgen(catch, js_name = "opfs_block_on_promise")]
    fn block_on_promise(promise: js_sys::Promise) -> Result<web_sys::FileSystemDirectoryHandle, JsValue>;
}

// ── Cleanup helper ──────────────────────────────────────────────────

/// Delete all entries in the OPFS root for test isolation.
fn cleanup_opfs_root() -> Result<(), JsValue> {
    let root = get_root()?;
    let entries = opfs_list_entries(&root)?;
    for i in 0..entries.length() {
        let entry = entries.get(i);
        let pair = Array::from(&entry);
        let name: String = pair.get(0).as_string().unwrap_or_default();
        let kind: String = pair.get(1).as_string().unwrap_or_default();
        let recursive = kind == "directory";
        opfs_remove_entry(&root, &name, recursive)?;
    }
    Ok(())
}

/// Create a fresh OpfsPersistence instance with a clean OPFS root.
fn fresh_persistence() -> Result<OpfsPersistence, JsValue> {
    cleanup_opfs_root()?;
    let root = get_root()?;
    Ok(OpfsPersistence::new(root))
}

// ── Smoke test ──────────────────────────────────────────────────────

/// Basic smoke test: create an OpfsPersistence, verify list_layers returns empty.
#[wasm_bindgen]
pub fn test_opfs_smoke() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let layers = persistence.list_layers().map_err(|e| JsValue::from_str(&e.to_string()))?;
    if !layers.is_empty() {
        return Err(JsValue::from_str(&format!(
            "expected empty layer list, got {} layers",
            layers.len()
        )));
    }
    Ok(())
}

// ── LayerPersistence integration tests (task 12.6) ──────────────────

/// Create a layer directory and verify `layer_exists` returns true.
#[wasm_bindgen]
pub fn test_opfs_layer_create_and_exists() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let id: [u32; 5] = [1, 2, 3, 4, 5];

    // Before creation, layer should not exist
    let exists_before = persistence
        .layer_exists(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if exists_before {
        return Err(JsValue::from_str("layer should not exist before creation"));
    }

    // Create the layer directory
    persistence
        .create_layer_dir(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // After creation, layer should exist
    let exists_after = persistence
        .layer_exists(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if !exists_after {
        return Err(JsValue::from_str(
            "layer should exist after create_layer_dir",
        ));
    }

    Ok(())
}

/// Write a file to a layer, read it back, and assert byte equality.
#[wasm_bindgen]
pub fn test_opfs_layer_write_read_roundtrip() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let id: [u32; 5] = [0xdeadbeef, 0, 0, 0, 1];
    let file_name = "test_data.bin";
    let data: Vec<u8> = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0xFF, 0x42];

    persistence
        .create_layer_dir(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    persistence
        .write_file(id, file_name, &data)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let read_back = persistence
        .read_file(id, file_name)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if read_back.as_ref() != data.as_slice() {
        return Err(JsValue::from_str(&format!(
            "data mismatch: wrote {} bytes, read {} bytes",
            data.len(),
            read_back.len()
        )));
    }

    Ok(())
}

/// Verify `file_exists` returns false before write and true after.
#[wasm_bindgen]
pub fn test_opfs_file_exists_check() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let id: [u32; 5] = [1, 2, 3, 4, 5];
    let file_name = "some_file.dat";

    persistence
        .create_layer_dir(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Before writing, file should not exist
    let exists_before = persistence
        .file_exists(id, file_name)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if exists_before {
        return Err(JsValue::from_str("file should not exist before write"));
    }

    // Write the file
    persistence
        .write_file(id, file_name, b"hello")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // After writing, file should exist
    let exists_after = persistence
        .file_exists(id, file_name)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if !exists_after {
        return Err(JsValue::from_str("file should exist after write"));
    }

    Ok(())
}

/// Create multiple layers, verify `list_layers` returns all of them and skips `.label` files.
#[wasm_bindgen]
pub fn test_opfs_list_layers() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;

    let id1: [u32; 5] = [1, 0, 0, 0, 0];
    let id2: [u32; 5] = [2, 0, 0, 0, 0];
    let id3: [u32; 5] = [3, 0, 0, 0, 0];

    persistence
        .create_layer_dir(id1)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    persistence
        .create_layer_dir(id2)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    persistence
        .create_layer_dir(id3)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Create a label file to verify it's skipped by list_layers
    persistence
        .create_label("testlabel")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let mut layers = persistence
        .list_layers()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    layers.sort();

    if layers.len() != 3 {
        return Err(JsValue::from_str(&format!(
            "expected 3 layers, got {}",
            layers.len()
        )));
    }

    let mut expected = vec![id1, id2, id3];
    expected.sort();

    if layers != expected {
        return Err(JsValue::from_str("listed layers do not match expected IDs"));
    }

    Ok(())
}

/// Create a layer, delete it, and verify `layer_exists` returns false.
#[wasm_bindgen]
pub fn test_opfs_delete_layer() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let id: [u32; 5] = [0xdeadbeef, 0, 0, 0, 1];

    persistence
        .create_layer_dir(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Verify it exists
    let exists = persistence
        .layer_exists(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if !exists {
        return Err(JsValue::from_str("layer should exist after creation"));
    }

    // Delete it
    persistence
        .delete_layer(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Verify it no longer exists
    let exists_after = persistence
        .layer_exists(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if exists_after {
        return Err(JsValue::from_str(
            "layer should not exist after delete_layer",
        ));
    }

    Ok(())
}

/// Create a layer, attempt to create it again, and verify `AlreadyExists` error.
#[wasm_bindgen]
pub fn test_opfs_create_duplicate_layer() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;
    let id: [u32; 5] = [1, 2, 3, 4, 5];

    // First creation should succeed
    persistence
        .create_layer_dir(id)
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Second creation should fail with AlreadyExists
    match persistence.create_layer_dir(id) {
        Ok(()) => {
            return Err(JsValue::from_str(
                "expected AlreadyExists error on duplicate create_layer_dir",
            ));
        }
        Err(e) => {
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(JsValue::from_str(&format!(
                    "expected AlreadyExists error kind, got {:?}: {}",
                    e.kind(),
                    e
                )));
            }
        }
    }

    Ok(())
}

// ── LabelPersistence integration tests (task 12.7) ──────────────────

/// Create, get, set (with layer), and delete labels — verify each step.
#[wasm_bindgen]
pub fn test_opfs_label_crud() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;

    // Create a label
    let label = persistence
        .create_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if label.name != "mydb" {
        return Err(JsValue::from_str("label name mismatch after create"));
    }
    if label.layer.is_some() {
        return Err(JsValue::from_str("new label should have no layer"));
    }
    if label.version != 0 {
        return Err(JsValue::from_str("new label should have version 0"));
    }

    // Get the label back
    let fetched = persistence
        .get_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    match fetched {
        None => return Err(JsValue::from_str("get_label returned None for existing label")),
        Some(ref f) => {
            if f.name != label.name || f.version != label.version || f.layer != label.layer {
                return Err(JsValue::from_str("get_label returned different label"));
            }
        }
    }

    // Set the label to point to a layer
    let layer_id: [u32; 5] = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    let updated = persistence
        .set_label(&label, Some(layer_id))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    match updated {
        None => return Err(JsValue::from_str("set_label returned None with correct version")),
        Some(ref u) => {
            if u.version != 1 {
                return Err(JsValue::from_str(&format!(
                    "expected version 1 after set, got {}",
                    u.version
                )));
            }
            if u.layer != Some(layer_id) {
                return Err(JsValue::from_str("set_label did not store the layer id"));
            }
        }
    }

    // Verify get returns the updated label
    let fetched2 = persistence
        .get_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("get_label returned None after set"))?;
    if fetched2.version != 1 || fetched2.layer != Some(layer_id) {
        return Err(JsValue::from_str("get_label after set returned stale data"));
    }

    // Delete the label
    let deleted = persistence
        .delete_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if !deleted {
        return Err(JsValue::from_str("delete_label returned false for existing label"));
    }

    // Verify get returns None after delete
    let fetched3 = persistence
        .get_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if fetched3.is_some() {
        return Err(JsValue::from_str("get_label returned Some after delete"));
    }

    // Delete again should return false
    let deleted2 = persistence
        .delete_label("mydb")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if deleted2 {
        return Err(JsValue::from_str("delete_label returned true for non-existent label"));
    }

    Ok(())
}

/// Create label, set with correct version (succeeds), set with stale version (returns None).
#[wasm_bindgen]
pub fn test_opfs_label_optimistic_concurrency() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;

    let label = persistence
        .create_label("branch")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let layer_id: [u32; 5] = [1, 2, 3, 4, 5];

    // Set with correct version 0 — should succeed
    let updated = persistence
        .set_label(&label, Some(layer_id))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let updated = match updated {
        None => return Err(JsValue::from_str("set_label with correct version returned None")),
        Some(u) => u,
    };
    if updated.version != 1 {
        return Err(JsValue::from_str(&format!(
            "expected version 1, got {}",
            updated.version
        )));
    }

    // Try to set with the original (stale) label at version 0 — should return None
    let stale_result = persistence
        .set_label(&label, Some([9, 9, 9, 9, 9]))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    if stale_result.is_some() {
        return Err(JsValue::from_str(
            "set_label with stale version should return None",
        ));
    }

    // Set with the updated label (version 1) — should succeed
    let layer_id2: [u32; 5] = [10, 20, 30, 40, 50];
    let updated2 = persistence
        .set_label(&updated, Some(layer_id2))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    match updated2 {
        None => return Err(JsValue::from_str("set_label with version 1 returned None")),
        Some(ref u) => {
            if u.version != 2 {
                return Err(JsValue::from_str(&format!(
                    "expected version 2, got {}",
                    u.version
                )));
            }
            if u.layer != Some(layer_id2) {
                return Err(JsValue::from_str("set_label did not store layer_id2"));
            }
        }
    }

    Ok(())
}

/// Create label, set layer, read back raw `.label` file bytes,
/// verify two-line plaintext format matches `<version>\n<layer_hex>\n`.
#[wasm_bindgen]
pub fn test_opfs_label_plaintext_roundtrip() -> Result<(), JsValue> {
    let persistence = fresh_persistence()?;

    let label = persistence
        .create_label("testlabel")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let layer_id: [u32; 5] = [0xdeadbeef, 0xcafebabe, 0x12345678, 0x9abcdef0, 0x00112233];
    let updated = persistence
        .set_label(&label, Some(layer_id))
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("set_label returned None"))?;

    // Read the raw .label file bytes from the OPFS root
    let root = get_root()?;
    let raw_bytes = opfs_read_file(&root, "testlabel.label")?;
    let content = String::from_utf8(raw_bytes.to_vec())
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    // Expected format: "<version>\n<layer_hex>\n"
    let expected_hex = name_to_string(layer_id);
    let expected = format!("{}\n{}\n", updated.version, expected_hex);

    if content != expected {
        return Err(JsValue::from_str(&format!(
            "plaintext mismatch:\n  expected: {:?}\n  got:      {:?}",
            expected, content
        )));
    }

    // Also verify the two-line structure explicitly
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() != 2 {
        return Err(JsValue::from_str(&format!(
            "expected 2 lines, got {}",
            lines.len()
        )));
    }
    let parsed_version: u64 = lines[0]
        .parse()
        .map_err(|_| JsValue::from_str("first line is not a valid u64"))?;
    if parsed_version != updated.version {
        return Err(JsValue::from_str("parsed version does not match"));
    }
    if lines[1] != expected_hex {
        return Err(JsValue::from_str("parsed layer hex does not match"));
    }

    Ok(())
}

/// Perform same label operations on MemoryPersistence and OpfsPersistence,
/// verify identical results.
#[wasm_bindgen]
pub fn test_opfs_cross_backend_equivalence() -> Result<(), JsValue> {
    let opfs = fresh_persistence()?;
    let mem = MemoryPersistence::new();

    // Create labels on both backends
    let opfs_label = opfs
        .create_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let mem_label = mem
        .create_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if opfs_label.name != mem_label.name
        || opfs_label.version != mem_label.version
        || opfs_label.layer != mem_label.layer
    {
        return Err(JsValue::from_str("create_label results differ"));
    }

    // Set label on both
    let layer_id: [u32; 5] = [0x11, 0x22, 0x33, 0x44, 0x55];
    let opfs_updated = opfs
        .set_label(&opfs_label, Some(layer_id))
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("opfs set_label returned None"))?;
    let mem_updated = mem
        .set_label(&mem_label, Some(layer_id))
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("mem set_label returned None"))?;

    if opfs_updated.name != mem_updated.name
        || opfs_updated.version != mem_updated.version
        || opfs_updated.layer != mem_updated.layer
    {
        return Err(JsValue::from_str("set_label results differ"));
    }

    // Get label from both
    let opfs_got = opfs
        .get_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("opfs get_label returned None"))?;
    let mem_got = mem
        .get_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .ok_or_else(|| JsValue::from_str("mem get_label returned None"))?;

    if opfs_got.name != mem_got.name
        || opfs_got.version != mem_got.version
        || opfs_got.layer != mem_got.layer
    {
        return Err(JsValue::from_str("get_label results differ"));
    }

    // Stale set on both — should both return None
    let opfs_stale = opfs
        .set_label(&opfs_label, Some([9, 9, 9, 9, 9]))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let mem_stale = mem
        .set_label(&mem_label, Some([9, 9, 9, 9, 9]))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if opfs_stale.is_some() || mem_stale.is_some() {
        return Err(JsValue::from_str("stale set_label should return None on both"));
    }

    // List labels on both
    let opfs_labels = opfs
        .labels()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let mem_labels = mem
        .labels()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if opfs_labels.len() != mem_labels.len() {
        return Err(JsValue::from_str(&format!(
            "labels() count differs: opfs={}, mem={}",
            opfs_labels.len(),
            mem_labels.len()
        )));
    }

    // Delete on both
    let opfs_del = opfs
        .delete_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let mem_del = mem
        .delete_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if opfs_del != mem_del {
        return Err(JsValue::from_str("delete_label results differ"));
    }

    // Get after delete on both — should both be None
    let opfs_after = opfs
        .get_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let mem_after = mem
        .get_label("equiv")
        .map_err(|e| JsValue::from_str(&e.to_string()))?;

    if opfs_after.is_some() || mem_after.is_some() {
        return Err(JsValue::from_str("get_label after delete should be None on both"));
    }

    Ok(())
}

// ── End-to-end store tests (task 12.8) ──────────────────────────────

/// Helper to convert io::Error to JsValue for test return types.
fn io_err(e: std::io::Error) -> JsValue {
    JsValue::from_str(&e.to_string())
}

/// End-to-end store workflow via `open_persistence_store(OpfsPersistence)`:
/// create base layer, add triples, commit, query, create child layer,
/// add/remove triples, commit, query through delta chain.
///
/// Validates: Requirements 10.1, 10.4, 22.1
#[wasm_bindgen]
pub fn test_opfs_e2e_store_workflow() -> Result<(), JsValue> {
    use crate::layer::{Layer, ValueTriple};
    use crate::store::open_persistence_store;

    let persistence = fresh_persistence()?;
    let store = open_persistence_store(persistence);

    // 1. Create a named graph (label)
    let db = store.create("main").map_err(io_err)?;

    // 2. Create a base layer with triples
    let base_builder = store.create_base_layer().map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_string_value("alice", "name", "Alice"))
        .map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_node("alice", "knows", "bob"))
        .map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_string_value("bob", "name", "Bob"))
        .map_err(io_err)?;
    let base_layer = base_builder.commit().map_err(io_err)?;

    // 3. Set the label to point to the base layer
    let set_ok = db.set_head(&base_layer).map_err(io_err)?;
    if !set_ok {
        return Err(JsValue::from_str("set_head for base layer returned false"));
    }

    // 4. Query the base layer triples
    let head = db
        .head()
        .map_err(io_err)?
        .ok_or_else(|| JsValue::from_str("head returned None after set_head"))?;

    if !head.value_triple_exists(&ValueTriple::new_string_value("alice", "name", "Alice")) {
        return Err(JsValue::from_str("base layer missing triple: alice name Alice"));
    }
    if !head.value_triple_exists(&ValueTriple::new_node("alice", "knows", "bob")) {
        return Err(JsValue::from_str("base layer missing triple: alice knows bob"));
    }
    if !head.value_triple_exists(&ValueTriple::new_string_value("bob", "name", "Bob")) {
        return Err(JsValue::from_str("base layer missing triple: bob name Bob"));
    }

    // 5. Create a child layer: add a triple, remove a triple
    let child_builder = head.open_write().map_err(io_err)?;
    child_builder
        .add_value_triple(ValueTriple::new_node("bob", "knows", "charlie"))
        .map_err(io_err)?;
    child_builder
        .remove_value_triple(ValueTriple::new_node("alice", "knows", "bob"))
        .map_err(io_err)?;
    let child_layer = child_builder.commit().map_err(io_err)?;

    // 6. Update the label
    let set_ok2 = db.set_head(&child_layer).map_err(io_err)?;
    if !set_ok2 {
        return Err(JsValue::from_str("set_head for child layer returned false"));
    }

    // 7. Query through the delta chain
    let head2 = db
        .head()
        .map_err(io_err)?
        .ok_or_else(|| JsValue::from_str("head returned None after child set_head"))?;

    // alice name Alice — still present (not removed)
    if !head2.value_triple_exists(&ValueTriple::new_string_value("alice", "name", "Alice")) {
        return Err(JsValue::from_str("child layer missing: alice name Alice"));
    }
    // bob name Bob — still present
    if !head2.value_triple_exists(&ValueTriple::new_string_value("bob", "name", "Bob")) {
        return Err(JsValue::from_str("child layer missing: bob name Bob"));
    }
    // bob knows charlie — added in child
    if !head2.value_triple_exists(&ValueTriple::new_node("bob", "knows", "charlie")) {
        return Err(JsValue::from_str("child layer missing: bob knows charlie"));
    }
    // alice knows bob — removed in child
    if head2.value_triple_exists(&ValueTriple::new_node("alice", "knows", "bob")) {
        return Err(JsValue::from_str(
            "child layer should NOT contain removed triple: alice knows bob",
        ));
    }

    // 8. Verify total triple count
    let all_triples: Vec<_> = head2.triples().collect();
    if all_triples.len() != 3 {
        return Err(JsValue::from_str(&format!(
            "expected 3 triples in child layer, got {}",
            all_triples.len()
        )));
    }

    Ok(())
}

/// Verify rollup works with the OPFS backend: create a base + child layer,
/// rollup the child, and verify the rolled-up layer has the same triples.
///
/// Validates: Requirements 10.1, 10.4
#[wasm_bindgen]
pub fn test_opfs_e2e_rollup() -> Result<(), JsValue> {
    use crate::layer::{Layer, ValueTriple};
    use crate::store::open_persistence_store;

    let persistence = fresh_persistence()?;
    let store = open_persistence_store(persistence);

    // Create base layer with triples
    let base_builder = store.create_base_layer().map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
        .map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_node("duck", "hates", "cow"))
        .map_err(io_err)?;
    let base_layer = base_builder.commit().map_err(io_err)?;

    // Create child layer with additions and removals
    let child_builder = base_layer.open_write().map_err(io_err)?;
    child_builder
        .remove_value_triple(ValueTriple::new_node("duck", "hates", "cow"))
        .map_err(io_err)?;
    child_builder
        .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
        .map_err(io_err)?;
    let child_layer = child_builder.commit().map_err(io_err)?;

    // Collect triples before rollup
    let triples_before: Vec<ValueTriple> = child_layer
        .triples()
        .filter_map(|t| child_layer.id_triple_to_string(&t))
        .collect();

    // Rollup the child layer
    child_layer.rollup().map_err(io_err)?;

    // Re-fetch the layer (rollup creates a new internal layer that the cache picks up)
    let refetched = store
        .get_layer_from_id(child_layer.name())
        .map_err(io_err)?
        .ok_or_else(|| JsValue::from_str("layer not found after rollup"))?;

    // Collect triples after rollup
    let mut triples_after: Vec<ValueTriple> = refetched
        .triples()
        .filter_map(|t| refetched.id_triple_to_string(&t))
        .collect();

    // Sort both for comparison (order may differ)
    let mut triples_before_sorted = triples_before.clone();
    triples_before_sorted.sort();
    triples_after.sort();

    if triples_before_sorted != triples_after {
        return Err(JsValue::from_str(&format!(
            "rollup triples mismatch: before={}, after={}",
            triples_before_sorted.len(),
            triples_after.len()
        )));
    }

    // Verify specific triples
    if !refetched.value_triple_exists(&ValueTriple::new_node("cow", "likes", "duck")) {
        return Err(JsValue::from_str("rollup missing: cow likes duck"));
    }
    if !refetched.value_triple_exists(&ValueTriple::new_node("duck", "likes", "cow")) {
        return Err(JsValue::from_str("rollup missing: duck likes cow"));
    }
    if refetched.value_triple_exists(&ValueTriple::new_node("duck", "hates", "cow")) {
        return Err(JsValue::from_str(
            "rollup should NOT contain removed triple: duck hates cow",
        ));
    }

    Ok(())
}

/// Export layers from an OPFS-backed store and import into a MemoryPersistence
/// store, verifying identical query results (cross-backend portability).
///
/// Validates: Requirements 10.1, 10.4, 10.7, 22.1
#[wasm_bindgen]
pub fn test_opfs_e2e_export_import() -> Result<(), JsValue> {
    use crate::layer::{Layer, ValueTriple};
    use crate::store::open_persistence_store;

    // ── Build layers in OPFS store ──
    let persistence = fresh_persistence()?;
    let opfs_store = open_persistence_store(persistence);

    let base_builder = opfs_store.create_base_layer().map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_string_value("alice", "name", "Alice"))
        .map_err(io_err)?;
    base_builder
        .add_value_triple(ValueTriple::new_node("alice", "knows", "bob"))
        .map_err(io_err)?;
    let base_layer = base_builder.commit().map_err(io_err)?;
    let base_name = base_layer.name();

    let child_builder = base_layer.open_write().map_err(io_err)?;
    child_builder
        .add_value_triple(ValueTriple::new_string_value("bob", "name", "Bob"))
        .map_err(io_err)?;
    child_builder
        .add_value_triple(ValueTriple::new_node("bob", "knows", "charlie"))
        .map_err(io_err)?;
    let child_layer = child_builder.commit().map_err(io_err)?;
    let child_name = child_layer.name();

    // Collect triples from OPFS child layer (includes parent triples via delta chain)
    let mut opfs_triples: Vec<ValueTriple> = child_layer
        .triples()
        .filter_map(|t| child_layer.id_triple_to_string(&t))
        .collect();
    opfs_triples.sort();

    // ── Export from OPFS store ──
    let pack = opfs_store
        .export_layers(Box::new(vec![base_name, child_name].into_iter()))
        .map_err(io_err)?;

    if pack.is_empty() {
        return Err(JsValue::from_str("exported pack is empty"));
    }

    // ── Import into MemoryPersistence store ──
    let mem_persistence = MemoryPersistence::new();
    let mem_store = open_persistence_store(mem_persistence);

    mem_store
        .import_layers(&pack, Box::new(vec![base_name, child_name].into_iter()))
        .map_err(io_err)?;

    // ── Query imported layers and verify identical results ──
    let imported_child = mem_store
        .get_layer_from_id(child_name)
        .map_err(io_err)?
        .ok_or_else(|| JsValue::from_str("imported child layer not found"))?;

    let mut mem_triples: Vec<ValueTriple> = imported_child
        .triples()
        .filter_map(|t| imported_child.id_triple_to_string(&t))
        .collect();
    mem_triples.sort();

    if opfs_triples != mem_triples {
        return Err(JsValue::from_str(&format!(
            "cross-backend triple mismatch: opfs={} triples, mem={} triples",
            opfs_triples.len(),
            mem_triples.len()
        )));
    }

    // Verify specific triples in the imported store
    if !imported_child.value_triple_exists(&ValueTriple::new_string_value("alice", "name", "Alice"))
    {
        return Err(JsValue::from_str("imported missing: alice name Alice"));
    }
    if !imported_child.value_triple_exists(&ValueTriple::new_node("alice", "knows", "bob")) {
        return Err(JsValue::from_str("imported missing: alice knows bob"));
    }
    if !imported_child.value_triple_exists(&ValueTriple::new_string_value("bob", "name", "Bob")) {
        return Err(JsValue::from_str("imported missing: bob name Bob"));
    }
    if !imported_child.value_triple_exists(&ValueTriple::new_node("bob", "knows", "charlie")) {
        return Err(JsValue::from_str("imported missing: bob knows charlie"));
    }

    // Also verify the base layer was imported correctly
    let imported_base = mem_store
        .get_layer_from_id(base_name)
        .map_err(io_err)?
        .ok_or_else(|| JsValue::from_str("imported base layer not found"))?;

    let base_triple_count = imported_base.triples().count();
    if base_triple_count != 2 {
        return Err(JsValue::from_str(&format!(
            "imported base layer should have 2 triples, got {}",
            base_triple_count
        )));
    }

    Ok(())
}
