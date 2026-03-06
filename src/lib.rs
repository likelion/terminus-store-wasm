#![type_length_limit = "10628160"]

//! Synchronous data store for triple data
//!
//! This library implements a way to store triple data - data that
//! consists of a subject, predicate and an object, where object can
//! either be some value, or a node (a string that can appear both in
//! subject and object position).
//!
//! This library is intended as a common base for anyone who wishes to
//! build a database containing triple data. It makes very few
//! assumptions on what valid data is, only focusing on the actual
//! storage aspect.
//!
//! Most users will probably only need to use the types and functions
//! in the `store` module. This module provides a high-level API which
//! should be sufficient for creating and querying databases.
//!
//! The `layer` and `storage` modules expose the inner workings of
//! terminus-store-wasm. They are useful for implementing new storage
//! backends, or writing analysis and recovery tools.
pub mod layer;
#[macro_use]
pub(crate) mod logging;
pub mod storage;
pub mod store;

pub use layer::{IdTriple, Layer, ObjectType, ValueTriple};
pub use store::{open_directory_store, open_memory_store};
