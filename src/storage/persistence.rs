use bytes::Bytes;
use std::io;

use super::label::Label;

/// Layer identifier — 20 bytes, matching terminus-store's [u32; 5] convention.
pub type LayerId = [u32; 5];

/// Abstraction over layer file storage.
/// Each layer is a collection of named binary files (e.g., "node_dictionary_blocks").
pub trait LayerPersistence: Send + Sync {
    /// Check if a layer directory exists.
    fn layer_exists(&self, id: LayerId) -> io::Result<bool>;

    /// Create a new layer directory.
    fn create_layer_dir(&self, id: LayerId) -> io::Result<()>;

    /// Check if a specific file exists within a layer.
    fn file_exists(&self, layer: LayerId, file: &str) -> io::Result<bool>;

    /// Read an entire file from a layer into memory.
    fn read_file(&self, layer: LayerId, file: &str) -> io::Result<Bytes>;

    /// Write data to a file within a layer.
    fn write_file(&self, layer: LayerId, file: &str, data: &[u8]) -> io::Result<()>;

    /// List all layer IDs in the store.
    fn list_layers(&self) -> io::Result<Vec<LayerId>>;

    /// Delete a layer and all its files.
    fn delete_layer(&self, id: LayerId) -> io::Result<()>;
}

/// Abstraction over label (named branch pointer) storage.
pub trait LabelPersistence: Send + Sync {
    /// List all labels.
    fn labels(&self) -> io::Result<Vec<Label>>;

    /// Create a new label with no associated layer.
    fn create_label(&self, name: &str) -> io::Result<Label>;

    /// Get a label by name.
    fn get_label(&self, name: &str) -> io::Result<Option<Label>>;

    /// Set a label to point to a layer. Returns the updated label,
    /// or None if the label's version has changed (optimistic concurrency).
    fn set_label(&self, label: &Label, layer: Option<LayerId>) -> io::Result<Option<Label>>;

    /// Delete a label by name.
    fn delete_label(&self, name: &str) -> io::Result<bool>;
}
