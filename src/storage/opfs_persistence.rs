#![cfg(target_arch = "wasm32")]

//! OPFS persistence backend for browser WebWorker environments.
//!
//! Uses the Origin Private File System (OPFS) via `wasm-bindgen` and `web-sys`
//! for persistent storage in browser WASM deployments.
//!
//! # Requirements
//! - Must run in a Web Worker (not the main thread) for `Atomics.wait`
//! - Requires OPFS support (Chrome 102+, Firefox 111+, Safari 15.2+)
//! - Requires Cross-Origin-Isolation headers (COOP + COEP) for `SharedArrayBuffer`
//!
//! # Compatibility
//! Uses the same on-disk layout as the original tokio-based
//! `DirectoryLabelStore` / `DirectoryLayerStore` from terminus-store.
//! A store directory can be copied between backends without conversion.

use bytes::Bytes;
use js_sys::{Array, Uint8Array};
use std::io;
use wasm_bindgen::prelude::*;

use super::label::Label;
use super::layer::{name_to_string, string_to_name};
use super::persistence::{LabelPersistence, LayerId, LayerPersistence};

// ── JS helper function imports (opfs_helpers.js) ────────────────────
// Block synchronously via Atomics.wait inside a Web Worker.

#[wasm_bindgen(module = "/src/opfs_helpers.js")]
extern "C" {
    #[wasm_bindgen(catch)]
    fn opfs_dir_exists(
        parent: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<bool, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_create_dir(
        parent: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<web_sys::FileSystemDirectoryHandle, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_get_dir(
        parent: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<web_sys::FileSystemDirectoryHandle, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_file_exists(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<bool, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_read_file(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<Uint8Array, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_write_file(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
        data: &[u8],
    ) -> Result<(), JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_list_entries(dir: &web_sys::FileSystemDirectoryHandle) -> Result<Array, JsValue>;

    #[wasm_bindgen(catch)]
    fn opfs_remove_entry(
        dir: &web_sys::FileSystemDirectoryHandle,
        name: &str,
        recursive: bool,
    ) -> Result<(), JsValue>;
}

/// Convert a JsValue error into an io::Error.
fn js_err(e: JsValue) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("OPFS error: {:?}", e))
}

/// Serialize a label to two-line plaintext: `<version>\n<layer_hex_or_empty>\n`
fn serialize_label(label: &Label) -> Vec<u8> {
    let layer_str = match label.layer {
        Some(id) => name_to_string(id),
        None => String::new(),
    };
    format!("{}\n{}\n", label.version, layer_str).into_bytes()
}

/// Deserialize a label from the two-line plaintext format.
fn deserialize_label(name: &str, data: &[u8]) -> io::Result<Label> {
    let s = String::from_utf8_lossy(data);
    let lines: Vec<&str> = s.lines().collect();
    if lines.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected label file to have two lines. contents were ({:?})",
                lines
            ),
        ));
    }
    let version: u64 = lines[0].parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected first line to be a number but was {}", lines[0]),
        )
    })?;
    let layer = if lines[1].is_empty() {
        None
    } else {
        Some(string_to_name(lines[1])?)
    };
    Ok(Label {
        name: name.to_string(),
        layer,
        version,
    })
}

/// OPFS persistence backend for browser WebWorker environments.
///
/// Directory layout (compatible with tokio-based terminus-store):
/// - `<layer_hex>/` subdirectories for layers (40-char hex names)
/// - `<name>.label` files flat in root for labels
///
/// `Clone` is derived because `FileSystemDirectoryHandle` (a JS object
/// reference) is `Clone`, and on `wasm32-unknown-unknown` all types are
/// automatically `Send + Sync` (single-threaded target). This allows
/// `OpfsPersistence` to satisfy the `Clone + Send + Sync + 'static`
/// bounds required by `open_persistence_store`.
#[derive(Clone)]
pub struct OpfsPersistence {
    root: web_sys::FileSystemDirectoryHandle,
}

impl OpfsPersistence {
    pub fn new(root: web_sys::FileSystemDirectoryHandle) -> Self {
        Self { root }
    }

    /// Get the directory handle for a layer by its hex-encoded ID.
    fn layer_dir(&self, id: LayerId) -> io::Result<web_sys::FileSystemDirectoryHandle> {
        let hex = name_to_string(id);
        opfs_get_dir(&self.root, &hex).map_err(js_err)
    }
}

impl LayerPersistence for OpfsPersistence {
    fn layer_exists(&self, id: LayerId) -> io::Result<bool> {
        let hex = name_to_string(id);
        opfs_dir_exists(&self.root, &hex).map_err(js_err)
    }

    fn create_layer_dir(&self, id: LayerId) -> io::Result<()> {
        let hex = name_to_string(id);
        if opfs_dir_exists(&self.root, &hex).map_err(js_err)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("layer directory already exists: {}", hex),
            ));
        }
        opfs_create_dir(&self.root, &hex).map_err(js_err)?;
        Ok(())
    }

    fn file_exists(&self, layer: LayerId, file: &str) -> io::Result<bool> {
        let dir = self.layer_dir(layer)?;
        opfs_file_exists(&dir, file).map_err(js_err)
    }

    fn read_file(&self, layer: LayerId, file: &str) -> io::Result<Bytes> {
        let dir = self.layer_dir(layer)?;
        let data = opfs_read_file(&dir, file).map_err(js_err)?;
        Ok(Bytes::from(data.to_vec()))
    }

    fn write_file(&self, layer: LayerId, file: &str, data: &[u8]) -> io::Result<()> {
        let dir = self.layer_dir(layer)?;
        opfs_write_file(&dir, file, data).map_err(js_err)
    }

    fn list_layers(&self) -> io::Result<Vec<LayerId>> {
        let entries = opfs_list_entries(&self.root).map_err(js_err)?;
        let mut layers = Vec::new();
        for i in 0..entries.length() {
            let entry = entries.get(i);
            let pair = Array::from(&entry);
            let name: String = pair.get(0).as_string().unwrap_or_default();
            let kind: String = pair.get(1).as_string().unwrap_or_default();
            if kind == "directory" && name.len() == 40 {
                if let Ok(id) = string_to_name(&name) {
                    layers.push(id);
                }
            }
        }
        Ok(layers)
    }

    fn delete_layer(&self, id: LayerId) -> io::Result<()> {
        let hex = name_to_string(id);
        if opfs_dir_exists(&self.root, &hex).map_err(js_err)? {
            opfs_remove_entry(&self.root, &hex, true).map_err(js_err)?;
        }
        Ok(())
    }
}

impl OpfsPersistence {
    /// Get the filename for a label: `<name>.label`
    fn label_filename(name: &str) -> String {
        format!("{}.label", name)
    }

    /// Write a label to the root OPFS directory as `<name>.label`.
    fn write_label_file(&self, label: &Label) -> io::Result<()> {
        let filename = Self::label_filename(&label.name);
        let data = serialize_label(label);
        opfs_write_file(&self.root, &filename, &data).map_err(js_err)
    }

    /// Read a label from the root OPFS directory. Returns `Ok(None)` if the file doesn't exist.
    fn read_label_file(&self, name: &str) -> io::Result<Option<Label>> {
        let filename = Self::label_filename(name);
        if !opfs_file_exists(&self.root, &filename).map_err(js_err)? {
            return Ok(None);
        }
        let data = opfs_read_file(&self.root, &filename).map_err(js_err)?;
        let label = deserialize_label(name, &data.to_vec())?;
        Ok(Some(label))
    }
}

impl LabelPersistence for OpfsPersistence {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let entries = opfs_list_entries(&self.root).map_err(js_err)?;
        let mut labels = Vec::new();
        for i in 0..entries.length() {
            let entry = entries.get(i);
            let pair = Array::from(&entry);
            let name: String = pair.get(0).as_string().unwrap_or_default();
            if let Some(label_name) = name.strip_suffix(".label") {
                if let Some(label) = self.read_label_file(label_name)? {
                    labels.push(label);
                }
            }
        }
        Ok(labels)
    }

    fn create_label(&self, name: &str) -> io::Result<Label> {
        let filename = Self::label_filename(name);
        if opfs_file_exists(&self.root, &filename).map_err(js_err)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("label already exists: {}", name),
            ));
        }
        let label = Label::new_empty(name);
        self.write_label_file(&label)?;
        Ok(label)
    }

    fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        self.read_label_file(name)
    }

    fn set_label(&self, label: &Label, layer: Option<LayerId>) -> io::Result<Option<Label>> {
        let stored = self.read_label_file(&label.name)?;
        match stored {
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("label not found: {}", label.name),
            )),
            Some(stored_label) => {
                if stored_label.version != label.version {
                    return Ok(None);
                }
                let updated = label.with_updated_layer(layer);
                self.write_label_file(&updated)?;
                Ok(Some(updated))
            }
        }
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let filename = Self::label_filename(name);
        if !opfs_file_exists(&self.root, &filename).map_err(js_err)? {
            return Ok(false);
        }
        opfs_remove_entry(&self.root, &filename, false).map_err(js_err)?;
        Ok(true)
    }
}
