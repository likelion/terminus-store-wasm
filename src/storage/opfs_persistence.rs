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

/// OPFS persistence backend for browser WebWorker environments.
///
/// Uses `FileSystemSyncAccessHandle` for synchronous byte-level I/O.
/// The root handle should be obtained via `navigator.storage.getDirectory()`
/// from JavaScript, then passed into Rust via `wasm-bindgen`.
///
/// # Directory Layout
/// ```text
/// <root>/
///   <layer_hex>/           # one subdirectory per layer (40-char hex name)
///     node_dictionary_blocks
///     predicate_dictionary_blocks
///     ...
///   labels/
///     <label_name>.json    # one JSON file per label
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
    /// TODO: Use `root.getDirectoryHandle(hex, { create: false })` and check
    /// for NotFoundError to determine existence.
    fn layer_exists(&self, _id: LayerId) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::layer_exists not yet implemented — requires Web Worker context",
        ))
    }

    /// Create a new layer subdirectory in OPFS.
    ///
    /// TODO: Use `root.getDirectoryHandle(hex, { create: true })` to create
    /// the subdirectory for the layer.
    fn create_layer_dir(&self, _id: LayerId) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::create_layer_dir not yet implemented — requires Web Worker context",
        ))
    }

    /// Check if a specific file exists within a layer's OPFS subdirectory.
    ///
    /// TODO: Get the layer directory handle, then call `getFileHandle(file, { create: false })`
    /// and check for NotFoundError.
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
    /// whose names are valid 40-char hex strings, parse into LayerIds.
    fn list_layers(&self) -> io::Result<Vec<LayerId>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::list_layers not yet implemented — requires Web Worker context",
        ))
    }

    /// Delete a layer and all its files from OPFS.
    ///
    /// TODO: Use `root.removeEntry(hex, { recursive: true })` to remove
    /// the layer's subdirectory and all contained files.
    fn delete_layer(&self, _id: LayerId) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LayerPersistence::delete_layer not yet implemented — requires Web Worker context",
        ))
    }
}

impl LabelPersistence for OpfsPersistence {
    /// List all labels stored in the OPFS labels subdirectory.
    ///
    /// TODO: Get or create a "labels" subdirectory, iterate over .json files,
    /// parse each as a Label.
    fn labels(&self) -> io::Result<Vec<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::labels not yet implemented — requires Web Worker context",
        ))
    }

    /// Create a new label with no associated layer and version 0.
    ///
    /// TODO: Create a JSON file in the "labels" subdirectory with the label data.
    /// Return an error if a label with the same name already exists.
    fn create_label(&self, _name: &str) -> io::Result<Label> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::create_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Get a label by name from the OPFS labels subdirectory.
    ///
    /// TODO: Try to read the JSON file for the label. Return Ok(None) if not found.
    fn get_label(&self, _name: &str) -> io::Result<Option<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::get_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Set a label to point to a layer with optimistic concurrency control.
    ///
    /// TODO: Read the current label, check version matches, update layer pointer
    /// and increment version, write back. Return Ok(None) on version mismatch.
    fn set_label(&self, _label: &Label, _layer: Option<LayerId>) -> io::Result<Option<Label>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::set_label not yet implemented — requires Web Worker context",
        ))
    }

    /// Delete a label from the OPFS labels subdirectory.
    ///
    /// TODO: Remove the label's JSON file. Return true if it existed, false otherwise.
    fn delete_label(&self, _name: &str) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "OPFS LabelPersistence::delete_label not yet implemented — requires Web Worker context",
        ))
    }
}
