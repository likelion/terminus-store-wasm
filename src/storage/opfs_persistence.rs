#![cfg(target_arch = "wasm32")]

//! OPFS persistence backend for browser WebWorker environments.
//!
//! Uses the Origin Private File System (OPFS) via `wasm-bindgen` and `web-sys`
//! for persistent storage in browser WASM deployments.
//!
//! # Requirements
//! - Must run in a Web Worker (not the main thread) for `FileSystemSyncAccessHandle`
//! - Requires OPFS support in the browser (Chrome 102+, Firefox 111+, Safari 15.2+)
//!
//! # Compatibility
//! This backend uses the same on-disk layout as the original tokio-based
//! `DirectoryLabelStore` / `DirectoryLayerStore` from terminus-store:
//!
//! - Layers are stored as subdirectories named by their 40-char hex ID
//! - Layer files (`.tfc`, `.logarray`, `.bitarray`, `.hex`) are stored directly
//!   inside each layer subdirectory with identical names and binary formats
//! - Labels are stored as `<name>.label` files in the root directory (flat,
//!   alongside layer subdirectories) using the two-line plaintext format:
//!   ```text
//!   <version>\n
//!   <layer_hex_or_empty>\n
//!   ```
//!
//! This means a store directory can be copied between the native filesystem
//! backend, the tokio-based async backend, and OPFS without any conversion.
//!
//! # Current Status
//! This is a stub implementation that returns `io::ErrorKind::Unsupported` for all
//! operations. The full implementation requires browser-specific testing with
//! `FileSystemSyncAccessHandle` which is only available in Web Worker contexts.

use bytes::Bytes;
use std::io;
use wasm_bindgen::prelude::*;

use super::label::Label;
#[allow(unused_imports)]
use super::layer::{name_to_string, string_to_name};
use super::persistence::{LabelPersistence, LayerId, LayerPersistence};

/// Convert a JsValue error into an io::Error.
#[allow(dead_code)]
fn js_err_to_io(e: JsValue) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("OPFS error: {:?}", e))
}

/// Serialize a label to the two-line plaintext format used by the original
/// tokio-based `DirectoryLabelStore`:
/// ```text
/// <version>\n
/// <layer_hex_or_empty>\n
/// ```
#[allow(dead_code)]
fn serialize_label(label: &Label) -> Vec<u8> {
    let layer_str = match label.layer {
        Some(id) => name_to_string(id),
        None => String::new(),
    };
    format!("{}\n{}\n", label.version, layer_str).into_bytes()
}

/// Deserialize a label from the two-line plaintext format used by the original
/// tokio-based `DirectoryLabelStore`.
#[allow(dead_code)]
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

    let version_str = lines[0];
    let layer_str = lines[1];

    let version: u64 = version_str.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected first line of label file to be a number but it was {}",
                version_str
            ),
        )
    })?;

    let layer = if layer_str.is_empty() {
        None
    } else {
        Some(string_to_name(layer_str)?)
    };

    Ok(Label {
        name: name.to_string(),
        layer,
        version,
    })
}

/// OPFS persistence backend for browser WebWorker environments.
///
/// Uses `FileSystemSyncAccessHandle` for synchronous byte-level I/O.
/// The root handle should be obtained via `navigator.storage.getDirectory()`
/// from JavaScript, then passed into Rust via `wasm-bindgen`.
///
/// # Directory Layout (compatible with tokio-based terminus-store)
/// ```text
/// <root>/
///   <layer_hex>/                        # one subdirectory per layer (40-char hex name)
///     node_dictionary_blocks.tfc
///     node_dictionary_offsets.logarray
///     predicate_dictionary_blocks.tfc
///     ...
///     parent.hex                        # (child layers only)
///     rollup.hex                        # (optional)
///   <label_name>.label                  # flat alongside layer dirs, two-line format:
///                                       #   <version>\n<layer_hex_or_empty>\n
/// ```
///
/// # Web Worker Requirement
/// `FileSystemSyncAccessHandle` is only available in dedicated Web Workers.
/// Attempting to use this from the main thread will result in errors.
pub struct OpfsPersistence {
    root: web_sys::FileSystemDirectoryHandle,
}

impl OpfsPersistence {
    /// Create a new OpfsPersistence from an OPFS directory handle.
    ///
    /// The handle should be obtained via `navigator.storage.getDirectory()` in JavaScript:
    /// ```js
    /// const root = await navigator.storage.getDirectory();
    /// const persistence = OpfsPersistence.new(root);
    /// ```
    pub fn new(root: web_sys::FileSystemDirectoryHandle) -> Self {
        Self { root }
    }
}

impl LayerPersistence for OpfsPersistence {
    /// Check if a layer directory exists in OPFS.
    ///
    /// TODO: Use `root.getDirectoryHandle(name_to_string(id), { create: false })`
    /// and check for NotFoundError to determine existence.
    fn layer_exists(&self, _id: LayerId) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::layer_exists not yet implemented — requires Web Worker context",
        ))
    }

    /// Create a new layer subdirectory in OPFS.
    ///
    /// TODO: Use `root.getDirectoryHandle(name_to_string(id), { create: true })`.
    /// Return AlreadyExists if the directory already exists.
    fn create_layer_dir(&self, _id: LayerId) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::create_layer_dir not yet implemented — requires Web Worker context",
        ))
    }

    /// Check if a specific file exists within a layer's OPFS subdirectory.
    ///
    /// TODO: Get the layer directory handle via
    /// `root.getDirectoryHandle(name_to_string(layer), { create: false })`,
    /// then call `getFileHandle(file, { create: false })` and check for NotFoundError.
    fn file_exists(&self, _layer: LayerId, _file: &str) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::file_exists not yet implemented — requires Web Worker context",
        ))
    }

    /// Read an entire file from a layer's OPFS subdirectory.
    ///
    /// TODO: Get the layer directory handle, get the file handle, call
    /// `createSyncAccessHandle()`, read all bytes, close the handle.
    /// File names are the standard terminus-store names (e.g.,
    /// "node_dictionary_blocks.tfc", "base_s_p_adjacency_list_bits.bitarray").
    fn read_file(&self, _layer: LayerId, _file: &str) -> io::Result<Bytes> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::read_file not yet implemented — requires Web Worker context",
        ))
    }

    /// Write data to a file within a layer's OPFS subdirectory.
    ///
    /// TODO: Get the layer directory handle, get or create the file handle,
    /// call `createSyncAccessHandle()`, truncate, write all bytes, flush, close.
    fn write_file(&self, _layer: LayerId, _file: &str, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::write_file not yet implemented — requires Web Worker context",
        ))
    }

    /// List all layer IDs stored in OPFS.
    ///
    /// TODO: Iterate over entries in the root directory, filter for subdirectories
    /// whose names are valid 40-char hex strings, parse via `string_to_name()`.
    /// Skip entries that are files (e.g., `.label` files).
    fn list_layers(&self) -> io::Result<Vec<LayerId>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::list_layers not yet implemented — requires Web Worker context",
        ))
    }

    /// Delete a layer and all its files from OPFS.
    ///
    /// TODO: Use `root.removeEntry(name_to_string(id), { recursive: true })`.
    fn delete_layer(&self, _id: LayerId) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::delete_layer not yet implemented — requires Web Worker context",
        ))
    }
}

impl LabelPersistence for OpfsPersistence {
    /// List all labels stored in the root OPFS directory.
    ///
    /// TODO: Iterate over entries in the root directory, filter for files
    /// ending in `.label`, read each with `createSyncAccessHandle()`,
    /// parse with `deserialize_label()`.
    fn labels(&self) -> io::Result<Vec<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::labels not yet implemented — requires Web Worker context",
        ))
    }

    /// Create a new label with no associated layer and version 0.
    ///
    /// TODO: Check that `<name>.label` does not already exist in the root
    /// directory (return AlreadyExists if it does). Create the file and write
    /// `serialize_label(&Label { name, layer: None, version: 0 })`.
    fn create_label(&self, _name: &str) -> io::Result<Label> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::create_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Get a label by name from the root OPFS directory.
    ///
    /// TODO: Try to get `<name>.label` file handle. Return Ok(None) if
    /// NotFoundError. Otherwise read contents and parse with
    /// `deserialize_label(name, &data)`.
    fn get_label(&self, _name: &str) -> io::Result<Option<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::get_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Set a label to point to a layer with optimistic concurrency control.
    ///
    /// TODO: Read the current `<name>.label` file, parse with
    /// `deserialize_label()`, check version matches `label.version`.
    /// If version matches, write updated label with `serialize_label()`.
    /// Return Ok(None) on version mismatch.
    fn set_label(&self, _label: &Label, _layer: Option<LayerId>) -> io::Result<Option<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::set_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Delete a label from the root OPFS directory.
    ///
    /// TODO: Use `root.removeEntry("<name>.label")`. Return true if it
    /// existed, false if NotFoundError.
    fn delete_label(&self, _name: &str) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::delete_label not yet implemented — requires Web Worker context",
        ))
    }
}
