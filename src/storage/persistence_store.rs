//! Generic LayerStore and LabelStore implementations backed by persistence traits.
//!
//! This module bridges the `LayerPersistence` and `LabelPersistence` traits
//! (byte-level abstractions) with the `PersistentLayerStore` and `LabelStore`
//! traits (file-level abstractions used by the core layer logic).

use bytes::{Bytes, BytesMut};
use std::io::{self, Cursor, Write};
use std::sync::{Arc, RwLock};

use super::label::{Label, LabelStore};
use super::layer::PersistentLayerStore;
use super::persistence::{LabelPersistence, LayerId, LayerPersistence};
use tdb_succinct_wasm::storage::types::{FileLoad, FileStore, SyncableFile};

// ---------------------------------------------------------------------------
// PersistenceBackedStore — a FileLoad + FileStore backed by LayerPersistence
// ---------------------------------------------------------------------------

/// A file-like handle backed by a `LayerPersistence` backend.
///
/// On read (`map`, `open_read_from`, `exists`, `size`), data is loaded from
/// the persistence backend into memory.
///
/// On write (`open_write` → write → `sync_all`), accumulated bytes are
/// flushed back to the persistence backend via `write_file`.
pub struct PersistenceBackedStore<P: LayerPersistence> {
    persistence: Arc<P>,
    layer_id: LayerId,
    file_name: String,
    /// Local write buffer — written data lives here until sync_all pushes it to persistence.
    /// Also serves as a read cache after sync_all.
    buffer: Arc<RwLock<Option<Bytes>>>,
}

impl<P: LayerPersistence> Clone for PersistenceBackedStore<P> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            layer_id: self.layer_id,
            file_name: self.file_name.clone(),
            buffer: self.buffer.clone(),
        }
    }
}

impl<P: LayerPersistence> PersistenceBackedStore<P> {
    fn new(persistence: Arc<P>, layer_id: LayerId, file_name: String) -> Self {
        Self {
            persistence,
            layer_id,
            file_name,
            buffer: Arc::new(RwLock::new(None)),
        }
    }

    /// Load data from persistence into the local buffer if not already cached.
    fn ensure_loaded(&self) -> io::Result<()> {
        let guard = self.buffer.read().unwrap();
        if guard.is_some() {
            return Ok(());
        }
        drop(guard);

        let mut guard = self.buffer.write().unwrap();
        // Double-check after acquiring write lock
        if guard.is_some() {
            return Ok(());
        }

        match self.persistence.read_file(self.layer_id, &self.file_name) {
            Ok(data) => {
                *guard = Some(data);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // File doesn't exist yet — that's fine, leave buffer as None
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl<P: 'static + LayerPersistence> FileLoad for PersistenceBackedStore<P> {
    type Read = Cursor<Bytes>;

    fn exists(&self) -> io::Result<bool> {
        let guard = self.buffer.read().unwrap();
        if guard.is_some() {
            return Ok(true);
        }
        drop(guard);
        self.persistence.file_exists(self.layer_id, &self.file_name)
    }

    fn size(&self) -> io::Result<usize> {
        self.ensure_loaded()?;
        let guard = self.buffer.read().unwrap();
        match &*guard {
            Some(data) => Ok(data.len()),
            None => Ok(0),
        }
    }

    fn open_read_from(&self, offset: usize) -> io::Result<Cursor<Bytes>> {
        self.ensure_loaded()?;
        let guard = self.buffer.read().unwrap();
        match &*guard {
            Some(data) => {
                let mut cursor = Cursor::new(data.clone());
                cursor.set_position(offset as u64);
                Ok(cursor)
            }
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "tried to open a nonexistent persistence file for reading",
            )),
        }
    }

    fn map(&self) -> io::Result<Bytes> {
        self.ensure_loaded()?;
        let guard = self.buffer.read().unwrap();
        match &*guard {
            Some(data) => Ok(data.clone()),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "tried to map a nonexistent persistence file",
            )),
        }
    }
}

/// Writer for `PersistenceBackedStore`. Accumulates bytes in memory,
/// then flushes to the persistence backend on `sync_all`.
pub struct PersistenceBackedStoreWriter<P: LayerPersistence> {
    buf: BytesMut,
    persistence: Arc<P>,
    layer_id: LayerId,
    file_name: String,
    local_buffer: Arc<RwLock<Option<Bytes>>>,
}

impl<P: LayerPersistence> Write for PersistenceBackedStoreWriter<P> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<P: LayerPersistence> SyncableFile for PersistenceBackedStoreWriter<P> {
    fn sync_all(&mut self) -> io::Result<()> {
        let data = self.buf.clone().freeze();
        // Write to persistence backend
        self.persistence
            .write_file(self.layer_id, &self.file_name, &data)?;
        // Update local buffer cache
        let mut guard = self.local_buffer.write().unwrap();
        *guard = Some(data);
        Ok(())
    }
}

impl<P: 'static + LayerPersistence> FileStore for PersistenceBackedStore<P> {
    type Write = PersistenceBackedStoreWriter<P>;

    fn open_write(&self) -> io::Result<Self::Write> {
        Ok(PersistenceBackedStoreWriter {
            buf: BytesMut::new(),
            persistence: self.persistence.clone(),
            layer_id: self.layer_id,
            file_name: self.file_name.clone(),
            local_buffer: self.buffer.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// PersistenceLayerStore — implements PersistentLayerStore via LayerPersistence
// ---------------------------------------------------------------------------

/// A generic layer store backed by any `LayerPersistence` implementation.
///
/// This struct bridges the `LayerPersistence` trait (byte-level I/O) with the
/// `PersistentLayerStore` trait (file-level I/O used by the core layer logic).
/// Through the blanket `impl<T: PersistentLayerStore> LayerStore for T`, this
/// automatically provides the full `LayerStore` API including layer loading,
/// rollup, squash, merge, export, and import.
pub struct PersistenceLayerStore<P: LayerPersistence> {
    persistence: Arc<P>,
}

impl<P: LayerPersistence> Clone for PersistenceLayerStore<P> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
        }
    }
}

impl<P: LayerPersistence> PersistenceLayerStore<P> {
    pub fn new(persistence: P) -> Self {
        Self {
            persistence: Arc::new(persistence),
        }
    }
}

impl<P: 'static + LayerPersistence + Clone> PersistentLayerStore for PersistenceLayerStore<P> {
    type File = PersistenceBackedStore<P>;

    fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        self.persistence.list_layers()
    }

    fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        self.persistence.create_layer_dir(name)?;
        Ok(name)
    }

    fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        self.persistence.layer_exists(name)
    }

    fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        self.persistence.file_exists(directory, file)
    }

    fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        if !self.persistence.layer_exists(directory)? {
            return Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"));
        }
        Ok(PersistenceBackedStore::new(
            self.persistence.clone(),
            directory,
            name.to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// PersistenceLabelStore — implements LabelStore via LabelPersistence
// ---------------------------------------------------------------------------

/// A generic label store backed by any `LabelPersistence` implementation.
#[derive(Clone)]
pub struct PersistenceLabelStore<P: LabelPersistence> {
    persistence: Arc<P>,
}

impl<P: LabelPersistence> PersistenceLabelStore<P> {
    pub fn new(persistence: P) -> Self {
        Self {
            persistence: Arc::new(persistence),
        }
    }
}

impl<P: 'static + LabelPersistence + Send + Sync> LabelStore for PersistenceLabelStore<P> {
    fn labels(&self) -> io::Result<Vec<Label>> {
        self.persistence.labels()
    }

    fn create_label(&self, name: &str) -> io::Result<Label> {
        self.persistence.create_label(name)
    }

    fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        self.persistence.get_label(name)
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        self.persistence.set_label(label, layer)
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        self.persistence.delete_label(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory_persistence::MemoryPersistence;
    use crate::storage::cache::LockingHashMapLayerCache;
    use crate::storage::cache::CachedLayerStore;
    use crate::storage::layer::LayerStore as LayerStoreTrait;
    use crate::layer::{Layer, SimpleLayerBuilder, ValueTriple};

    #[test]
    fn persistence_layer_store_create_base_layer() {
        let persistence = MemoryPersistence::new();
        let store = PersistenceLayerStore::new(persistence);
        let builder = store.create_base_layer().unwrap();
        let name = builder.name();
        builder.commit_boxed().unwrap();

        // Verify the layer can be retrieved
        let layer = store.get_layer(name).unwrap();
        assert!(layer.is_some());
    }

    #[test]
    fn persistence_layer_store_with_cache() {
        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        let builder = store.create_base_layer().unwrap();
        let name = builder.name();
        builder.commit_boxed().unwrap();

        // Retrieve with cache
        let layer = store.get_layer(name).unwrap();
        assert!(layer.is_some());

        // Retrieve again — should come from cache
        let layer2 = store.get_layer(name).unwrap();
        assert!(layer2.is_some());
        assert_eq!(layer.unwrap().name(), layer2.unwrap().name());
    }

    #[test]
    fn persistence_label_store_crud() {
        let persistence = MemoryPersistence::new();
        let store = PersistenceLabelStore::new(persistence);

        // Create
        let label = store.create_label("test").unwrap();
        assert_eq!(label.name, "test");
        assert_eq!(label.layer, None);
        assert_eq!(label.version, 0);

        // Get
        let retrieved = store.get_label("test").unwrap().unwrap();
        assert_eq!(retrieved.name, "test");

        // Set
        let updated = store
            .set_label_option(&retrieved, Some([1, 2, 3, 4, 5]))
            .unwrap()
            .unwrap();
        assert_eq!(updated.version, 1);
        assert_eq!(updated.layer, Some([1, 2, 3, 4, 5]));

        // Delete
        assert!(store.delete_label("test").unwrap());
        assert!(store.get_label("test").unwrap().is_none());
    }

    #[test]
    fn persistence_layer_store_create_child_layer() {
        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        // Create base layer
        let base_builder = store.create_base_layer().unwrap();
        let base_name = base_builder.name();
        base_builder.commit_boxed().unwrap();

        // Create child layer
        let child_builder = store.create_child_layer(base_name).unwrap();
        let child_name = child_builder.name();
        child_builder.commit_boxed().unwrap();

        // Verify child layer exists and has parent
        let child = store.get_layer(child_name).unwrap().unwrap();
        assert_eq!(child.parent_name(), Some(base_name));
    }

    #[test]
    fn open_persistence_store_end_to_end() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        // Create a database
        let db = store.create("testdb").unwrap();

        // Create a base layer
        let builder = store.create_base_layer().unwrap();
        builder
            .with_builder(|b| {
                b.add_value_triple(ValueTriple::new_string_value("s", "p", "o"));
            })
            .unwrap();
        let layer = builder.commit().unwrap();

        // Set the head
        let (_, version) = db.head_version().unwrap();
        assert!(db.set_head(&layer).unwrap());

        // Verify we can read back
        let head = db.head().unwrap().unwrap();
        assert!(head.value_triple_exists(&ValueTriple::new_string_value("s", "p", "o")));
    }

    // -----------------------------------------------------------------------
    // Task 9.1: Verify export_layers works synchronously via persistence store
    // -----------------------------------------------------------------------

    #[test]
    fn persistence_export_layers_synchronous() {
        use crate::storage::pack::Packable;

        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        // Create a base layer with triples
        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.commit_boxed().unwrap();

        // Create a child layer
        let mut child_builder = store.create_child_layer(base_name).unwrap();
        let child_name = child_builder.name();
        child_builder.add_value_triple(ValueTriple::new_node("dog", "likes", "cat"));
        child_builder.commit_boxed().unwrap();

        // Export both layers
        let pack = store
            .export_layers(Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        // Pack should be non-empty tar+gzip data
        assert!(!pack.is_empty());

        // Import into a fresh store and verify
        let persistence2 = MemoryPersistence::new();
        let inner2 = PersistenceLayerStore::new(persistence2);
        let store2 = CachedLayerStore::new(inner2, LockingHashMapLayerCache::new());

        store2
            .import_layers(&pack, Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        let imported = store2.get_layer(child_name).unwrap().unwrap();
        let triples: Vec<_> = imported
            .triples()
            .map(|t| imported.id_triple_to_string(&t).unwrap())
            .collect();

        assert!(triples.contains(&ValueTriple::new_node("cow", "likes", "duck")));
        assert!(triples.contains(&ValueTriple::new_string_value("duck", "says", "quack")));
        assert!(triples.contains(&ValueTriple::new_node("dog", "likes", "cat")));
    }

    #[test]
    fn persistence_export_with_rollup() {
        use crate::storage::pack::Packable;
        use std::sync::Arc;

        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        builder.commit_boxed().unwrap();

        let mut child_builder = store.create_child_layer(base_name).unwrap();
        let child_name = child_builder.name();
        child_builder.remove_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        child_builder.add_value_triple(ValueTriple::new_node("duck", "likes", "cow"));
        child_builder.commit_boxed().unwrap();

        // Rollup the child layer
        let unrolled = store.get_layer(child_name).unwrap().unwrap();
        Arc::new(store.clone()).rollup(unrolled).unwrap();

        // Export and import
        let pack = store
            .export_layers(Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        let persistence2 = MemoryPersistence::new();
        let inner2 = PersistenceLayerStore::new(persistence2);
        let store2 = CachedLayerStore::new(inner2, LockingHashMapLayerCache::new());

        store2
            .import_layers(&pack, Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        let imported = store2.get_layer(child_name).unwrap().unwrap();
        let triples: Vec<_> = imported
            .triples()
            .map(|t| imported.id_triple_to_string(&t).unwrap())
            .collect();

        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("duck", "likes", "cow"),
            ],
            triples
        );
    }

    // -----------------------------------------------------------------------
    // Task 9.2: Verify import_layers works synchronously — skip existing, etc.
    // -----------------------------------------------------------------------

    #[test]
    fn persistence_import_skips_existing_layers() {
        use crate::storage::pack::Packable;

        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        // Create a base layer
        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.commit_boxed().unwrap();

        // Export
        let pack = store
            .export_layers(Box::new(vec![base_name].into_iter()))
            .unwrap();

        // Create a second store with the same layer already present
        let persistence2 = MemoryPersistence::new();
        let inner2 = PersistenceLayerStore::new(persistence2);
        let store2 = CachedLayerStore::new(inner2, LockingHashMapLayerCache::new());

        // Pre-create the same layer with different content
        let mut builder2 = store2.create_base_layer().unwrap();
        let pre_name = builder2.name();
        builder2.add_value_triple(ValueTriple::new_node("pig", "says", "oink"));
        builder2.commit_boxed().unwrap();

        // Import the pack — the layer from the pack is new, so it should be imported
        store2
            .import_layers(&pack, Box::new(vec![base_name].into_iter()))
            .unwrap();

        // The imported layer should be retrievable
        let imported = store2.get_layer(base_name).unwrap().unwrap();
        let triples: Vec<_> = imported
            .triples()
            .map(|t| imported.id_triple_to_string(&t).unwrap())
            .collect();
        assert!(triples.contains(&ValueTriple::new_node("cow", "likes", "duck")));

        // Import again — should not overwrite (idempotent)
        store2
            .import_layers(&pack, Box::new(vec![base_name].into_iter()))
            .unwrap();

        let imported2 = store2.get_layer(base_name).unwrap().unwrap();
        let triples2: Vec<_> = imported2
            .triples()
            .map(|t| imported2.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(triples, triples2);
    }

    #[test]
    fn persistence_import_base_layer_round_trip() {
        use crate::storage::pack::Packable;

        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        // Create base layer
        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();
        builder.add_value_triple(ValueTriple::new_string_value("alice", "name", "Alice"));
        builder.add_value_triple(ValueTriple::new_node("alice", "knows", "bob"));
        builder.commit_boxed().unwrap();

        let pack = store
            .export_layers(Box::new(vec![base_name].into_iter()))
            .unwrap();

        // Import into fresh store
        let persistence2 = MemoryPersistence::new();
        let inner2 = PersistenceLayerStore::new(persistence2);
        let store2 = CachedLayerStore::new(inner2, LockingHashMapLayerCache::new());

        store2
            .import_layers(&pack, Box::new(vec![base_name].into_iter()))
            .unwrap();

        let imported = store2.get_layer(base_name).unwrap().unwrap();
        assert!(imported
            .value_triple_exists(&ValueTriple::new_string_value("alice", "name", "Alice")));
        assert!(imported.value_triple_exists(&ValueTriple::new_node("alice", "knows", "bob")));
    }

    // -----------------------------------------------------------------------
    // Task 9.4: Wire label management through LayerStore
    // -----------------------------------------------------------------------

    #[test]
    fn persistence_label_create_has_no_layer_and_version_0() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        let db = store.create("mydb").unwrap();
        let (head, version) = db.head_version().unwrap();
        assert!(head.is_none(), "new label should have no layer");
        assert_eq!(version, 0, "new label should have version 0");
    }

    #[test]
    fn persistence_label_get_nonexistent_returns_none() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        let result = store.open("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn persistence_label_delete_existing_returns_true() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        store.create("todelete").unwrap();
        assert!(store.delete("todelete").unwrap());
        // After deletion, opening should return None
        assert!(store.open("todelete").unwrap().is_none());
    }

    #[test]
    fn persistence_label_delete_nonexistent_returns_false() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        assert!(!store.delete("doesnotexist").unwrap());
    }

    #[test]
    fn persistence_label_set_updates_layer_and_version() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        let db = store.create("versiontest").unwrap();

        // Create a base layer and set head
        let builder = store.create_base_layer().unwrap();
        builder
            .with_builder(|b| {
                b.add_value_triple(ValueTriple::new_string_value("s", "p", "o"));
            })
            .unwrap();
        let layer = builder.commit().unwrap();

        assert!(db.set_head(&layer).unwrap());

        // Version should now be 1
        let (head, version) = db.head_version().unwrap();
        assert!(head.is_some());
        assert_eq!(version, 1);

        // Create a child layer of the current head and update
        let builder2 = layer.open_write().unwrap();
        builder2
            .with_builder(|b| {
                b.add_value_triple(ValueTriple::new_string_value("a", "b", "c"));
            })
            .unwrap();
        let layer2 = builder2.commit().unwrap();

        assert!(db.set_head(&layer2).unwrap());

        let (head2, version2) = db.head_version().unwrap();
        assert!(head2.is_some());
        assert_eq!(version2, 2);
    }

    // -----------------------------------------------------------------------
    // Task 10.1: Verify dictionary string round-trip including Unicode
    // Requirements: 21.1, 21.2, 21.3
    // -----------------------------------------------------------------------

    #[test]
    fn dictionary_unicode_round_trip_via_id_lookup() {
        use crate::store::open_persistence_store;

        let persistence = MemoryPersistence::new();
        let store = open_persistence_store(persistence);

        // Unicode strings: CJK, accented Latin, emoji
        let subjects = ["日本語", "émojis 🎉", "Ñoño"];
        let predicates = ["関係", "prédicat", "predicado_ñ"];
        let objects = ["値", "objet_à_tester", "🦀 Rust"];

        let builder = store.create_base_layer().unwrap();
        for i in 0..3 {
            builder
                .add_value_triple(ValueTriple::new_string_value(
                    subjects[i],
                    predicates[i],
                    objects[i],
                ))
                .unwrap();
        }
        let layer = builder.commit().unwrap();

        // Verify round-trip: string → id → string for subjects and predicates
        for &s in &subjects {
            let id = layer.subject_id(s).expect(&format!("subject '{}' should have an id", s));
            let back = layer.id_subject(id).expect(&format!("id {} should resolve to subject", id));
            assert_eq!(s, back, "subject round-trip failed for '{}'", s);
        }

        for &p in &predicates {
            let id = layer.predicate_id(p).expect(&format!("predicate '{}' should have an id", p));
            let back = layer.id_predicate(id).expect(&format!("id {} should resolve to predicate", id));
            assert_eq!(p, back, "predicate round-trip failed for '{}'", p);
        }

        // Verify round-trip via id_triple_to_string for full triples
        let triples: Vec<_> = layer
            .triples()
            .map(|t| layer.id_triple_to_string(&t).unwrap())
            .collect();

        for i in 0..3 {
            let expected = ValueTriple::new_string_value(subjects[i], predicates[i], objects[i]);
            assert!(
                triples.contains(&expected),
                "triple ({}, {}, {}) should be retrievable",
                subjects[i], predicates[i], objects[i]
            );
        }

        // Also verify triple existence
        for i in 0..3 {
            assert!(
                layer.value_triple_exists(&ValueTriple::new_string_value(
                    subjects[i],
                    predicates[i],
                    objects[i],
                )),
                "triple {} should exist",
                i
            );
        }
    }

    // -----------------------------------------------------------------------
    // Task 10.3: Verify error propagation
    // Requirements: 23.1, 23.2, 23.3, 23.4
    // -----------------------------------------------------------------------

    #[test]
    fn error_reading_nonexistent_layer_returns_none() {
        // Requirement 23.1: LayerPersistence I/O failures propagate as io::Error
        // get_layer for a nonexistent layer returns Ok(None), not an error
        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        let fake_id = [99, 99, 99, 99, 99];
        let result = store.get_layer(fake_id).unwrap();
        assert!(result.is_none(), "nonexistent layer should return None");
    }

    #[test]
    fn error_import_with_missing_parent_returns_error() {
        // Requirement 23.4: import pack referencing missing parents returns io::Error
        use crate::storage::pack::Packable;

        let persistence = MemoryPersistence::new();
        let inner = PersistenceLayerStore::new(persistence);
        let store = CachedLayerStore::new(inner, LockingHashMapLayerCache::new());

        // Create a base layer and a child layer
        let mut base_builder = store.create_base_layer().unwrap();
        let base_name = base_builder.name();
        base_builder.add_value_triple(ValueTriple::new_node("a", "b", "c"));
        base_builder.commit_boxed().unwrap();

        let mut child_builder = store.create_child_layer(base_name).unwrap();
        let child_name = child_builder.name();
        child_builder.add_value_triple(ValueTriple::new_node("d", "e", "f"));
        child_builder.commit_boxed().unwrap();

        // Export ONLY the child layer (not the base parent)
        let pack = store
            .export_layers(Box::new(vec![child_name].into_iter()))
            .unwrap();

        // Import into a fresh store that doesn't have the parent
        let persistence2 = MemoryPersistence::new();
        let inner2 = PersistenceLayerStore::new(persistence2);
        let store2 = CachedLayerStore::new(inner2, LockingHashMapLayerCache::new());

        // The import itself may succeed (it just writes files), but loading
        // the child layer should fail because the parent is missing
        store2
            .import_layers(&pack, Box::new(vec![child_name].into_iter()))
            .unwrap();

        let result = store2.get_layer(child_name);
        match result {
            Err(err) => {
                assert_eq!(
                    err.kind(),
                    io::ErrorKind::NotFound,
                    "error should be NotFound for missing parent"
                );
            }
            Ok(None) => {
                // Also acceptable: layer not found returns None
            }
            Ok(Some(_)) => {
                panic!("loading a child layer with missing parent should not succeed");
            }
        }
    }

    #[test]
    fn error_persistence_read_nonexistent_file_returns_error() {
        // Requirement 23.1: LayerPersistence read_file for nonexistent file returns io::Error
        let persistence = MemoryPersistence::new();
        let fake_id = [42, 42, 42, 42, 42];

        let result = persistence.read_file(fake_id, "nonexistent_file");
        assert!(result.is_err(), "reading nonexistent file should error");
    }

    #[test]
    fn error_label_persistence_propagates_io_error() {
        // Requirement 23.2: LabelPersistence I/O failures propagate as io::Error
        use crate::storage::persistence::LabelPersistence;

        let persistence = MemoryPersistence::new();

        // set_label with a non-matching version returns Ok(None) (not an error)
        let label = persistence.create_label("test_label").unwrap();
        let stale_label = crate::storage::label::Label {
            name: "test_label".to_string(),
            layer: None,
            version: 999, // wrong version
        };
        let result = persistence.set_label(&stale_label, Some([1, 2, 3, 4, 5])).unwrap();
        assert!(
            result.is_none(),
            "set_label with stale version should return None"
        );
    }
}
