//! Logic for dealing with various storage backends.
//!
//! Storage in terminus-store is set up in a generic way. Many data
//! structures simply rely on something that implements `FileLoad` and
//! `FileStore`, leaving the details of retrieval and storage to the
//! implementer.
//!
//! Two mechanisms are provided in this library:
//! - a memory backend
//! - a file backend
//!
//! Terminus-store stores databases as part of 2 data structures: a
//! layer store and a label store.
//!
//! A layer store is a set of directories (though it is up to a
//! specific implementation whether or not this is actually a
//! directory on a filesystem or some other mechanism). Each directory
//! is given a unique name of 20 bytes in hexadecimal format, and
//! stores the layer's primitive data structures as files inside that
//! directory.
//!
//! A label store is a set of files. The file name is of the format
//! `foo.label`, for database `foo`. This file contains the name of
//! the layer this label is pointing at.
mod cache;
pub mod consts;
pub mod directory;
mod file;
mod label;
#[macro_use]
mod layer;
// Archive module temporarily disabled during async stripping - needs deep conversion
// pub mod archive;
#[cfg(feature = "archive")]
pub mod archive;
mod copy;
pub mod delta;
mod locking;
pub mod memory;
pub mod memory_persistence;
#[cfg(not(target_arch = "wasm32"))]
pub mod fs_persistence;
#[cfg(target_arch = "wasm32")]
pub mod opfs_persistence;
pub mod pack;
pub mod persistence;
pub mod persistence_store;

pub use cache::*;
pub use delta::*;
pub use file::*;
pub use label::*;
pub use layer::*;
pub use memory_persistence::*;
#[cfg(not(target_arch = "wasm32"))]
pub use fs_persistence::*;
#[cfg(target_arch = "wasm32")]
pub use opfs_persistence::*;
pub use pack::*;
pub use persistence::*;
pub use persistence_store::*;
