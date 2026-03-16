//! Synchronous store API
//!
//! Since the entire API is now synchronous, this module simply
//! re-exports the main store types with Sync prefixes for backward
//! compatibility. The `task_sync` trampoline is no longer needed.

use std::io;
use std::path::{Path, PathBuf};

use crate::layer::{IdTriple, Layer, LayerBuilder, LayerCounts, ObjectType, ValueTriple};
use crate::store::{
    open_directory_store, open_memory_store, NamedGraph, Store, StoreLayer, StoreLayerBuilder,
};
use tdb_succinct_wasm::TypedDictEntry;

/// A wrapper over a StoreLayerBuilder, providing backward-compatible sync API.
#[derive(Clone)]
pub struct SyncStoreLayerBuilder {
    inner: StoreLayerBuilder,
}

impl SyncStoreLayerBuilder {
    fn wrap(inner: StoreLayerBuilder) -> Self {
        SyncStoreLayerBuilder { inner }
    }

    pub fn with_builder<R, F: FnOnce(&mut Box<dyn LayerBuilder>) -> R>(
        &self,
        f: F,
    ) -> Result<R, io::Error> {
        self.inner.with_builder(f)
    }

    pub fn name(&self) -> [u32; 5] {
        self.inner.name()
    }

    pub fn add_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.inner.add_value_triple(triple)
    }

    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.inner.add_id_triple(triple)
    }

    pub fn remove_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.inner.remove_value_triple(triple)
    }

    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.inner.remove_id_triple(triple)
    }

    pub fn committed(&self) -> bool {
        self.inner.committed()
    }

    pub fn commit_no_load(&self) -> Result<(), io::Error> {
        self.inner.commit_no_load()
    }

    pub fn commit(&self) -> Result<SyncStoreLayer, io::Error> {
        self.inner.commit().map(SyncStoreLayer::wrap)
    }

    pub fn apply_delta(&self, delta: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.apply_delta(&delta.inner)
    }

    pub fn apply_diff(&self, other: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.apply_diff(&other.inner)
    }

    pub fn apply_merge(
        &self,
        others: Vec<&SyncStoreLayer>,
        merge_base: Option<&SyncStoreLayer>,
    ) -> Result<(), io::Error> {
        let others_inner: Vec<&StoreLayer> = others.iter().map(|x| &x.inner).collect();
        self.inner
            .apply_merge(others_inner, merge_base.and_then(|x| Some(&x.inner)))
    }
}

#[derive(Clone)]
pub struct SyncStoreLayer {
    inner: StoreLayer,
}

impl SyncStoreLayer {
    fn wrap(inner: StoreLayer) -> Self {
        Self { inner }
    }

    pub fn open_write(&self) -> Result<SyncStoreLayerBuilder, io::Error> {
        self.inner.open_write().map(SyncStoreLayerBuilder::wrap)
    }

    pub fn parent(&self) -> Result<Option<SyncStoreLayer>, io::Error> {
        self.inner.parent().map(|p| p.map(SyncStoreLayer::wrap))
    }

    pub fn squash_upto(&self, upto: &SyncStoreLayer) -> Result<SyncStoreLayer, io::Error> {
        self.inner
            .squash_upto(&upto.inner)
            .map(SyncStoreLayer::wrap)
    }

    pub fn squash(&self) -> Result<SyncStoreLayer, io::Error> {
        self.inner.squash().map(SyncStoreLayer::wrap)
    }

    pub fn rollup(&self) -> Result<(), io::Error> {
        self.inner.rollup()
    }

    pub fn rollup_upto(&self, upto: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.rollup_upto(&upto.inner)
    }

    pub fn imprecise_rollup_upto(&self, upto: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.imprecise_rollup_upto(&upto.inner)
    }

    pub fn triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.inner
            .triple_addition_exists(subject, predicate, object)
    }

    pub fn triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.inner.triple_removal_exists(subject, predicate, object)
    }

    pub fn triple_additions(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_additions()
    }

    pub fn triple_removals(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_removals()
    }

    pub fn triple_additions_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_additions_s(subject)
    }

    pub fn triple_removals_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_removals_s(subject)
    }

    pub fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_additions_sp(subject, predicate)
    }

    pub fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_removals_sp(subject, predicate)
    }

    pub fn triple_additions_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_additions_p(predicate)
    }

    pub fn triple_removals_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_removals_p(predicate)
    }

    pub fn triple_additions_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_additions_o(object)
    }

    pub fn triple_removals_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.inner.triple_removals_o(object)
    }

    pub fn triple_layer_addition_count(&self) -> io::Result<usize> {
        self.inner.triple_layer_addition_count()
    }

    pub fn triple_layer_removal_count(&self) -> io::Result<usize> {
        self.inner.triple_layer_removal_count()
    }

    pub fn retrieve_layer_stack_names(&self) -> io::Result<Vec<[u32; 5]>> {
        self.inner.retrieve_layer_stack_names()
    }
}

impl PartialEq for SyncStoreLayer {
    fn eq(&self, other: &SyncStoreLayer) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for SyncStoreLayer {}

impl Layer for SyncStoreLayer {
    fn name(&self) -> [u32; 5] {
        self.inner.name()
    }
    fn parent_name(&self) -> Option<[u32; 5]> {
        self.inner.parent_name()
    }
    fn node_and_value_count(&self) -> usize {
        self.inner.node_and_value_count()
    }
    fn predicate_count(&self) -> usize {
        self.inner.predicate_count()
    }
    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.inner.subject_id(subject)
    }
    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.inner.predicate_id(predicate)
    }
    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.inner.object_node_id(object)
    }
    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64> {
        self.inner.object_value_id(object)
    }
    fn id_subject(&self, id: u64) -> Option<String> {
        self.inner.id_subject(id)
    }
    fn id_predicate(&self, id: u64) -> Option<String> {
        self.inner.id_predicate(id)
    }
    fn id_object(&self, id: u64) -> Option<ObjectType> {
        self.inner.id_object(id)
    }
    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        self.inner.id_object_is_node(id)
    }
    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.inner.triple_exists(subject, predicate, object)
    }
    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples()
    }
    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_s(subject)
    }
    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_sp(subject, predicate)
    }
    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_p(predicate)
    }
    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.inner.triples_o(object)
    }
    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }
    fn triple_addition_count(&self) -> usize {
        self.inner.triple_addition_count()
    }
    fn triple_removal_count(&self) -> usize {
        self.inner.triple_removal_count()
    }
    fn all_counts(&self) -> LayerCounts {
        self.inner.all_counts()
    }
    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        self.inner.single_triple_sp(subject, predicate)
    }
}

#[derive(Clone)]
pub struct SyncNamedGraph {
    inner: NamedGraph,
}

impl SyncNamedGraph {
    fn wrap(inner: NamedGraph) -> Self {
        Self { inner }
    }
    pub fn name(&self) -> &str {
        self.inner.name()
    }
    pub fn head_version(&self) -> io::Result<(Option<SyncStoreLayer>, u64)> {
        self.inner
            .head_version()
            .map(|(layer, version)| (layer.map(SyncStoreLayer::wrap), version))
    }
    pub fn head(&self) -> io::Result<Option<SyncStoreLayer>> {
        self.inner.head().map(|i| i.map(SyncStoreLayer::wrap))
    }
    pub fn set_head(&self, layer: &SyncStoreLayer) -> Result<bool, io::Error> {
        self.inner.set_head(&layer.inner)
    }
    pub fn force_set_head(&self, layer: &SyncStoreLayer) -> Result<(), io::Error> {
        self.inner.force_set_head(&layer.inner)
    }
    pub fn force_set_head_version(&self, layer: &SyncStoreLayer, version: u64) -> io::Result<bool> {
        self.inner.force_set_head_version(&layer.inner, version)
    }
    pub fn delete(&self) -> io::Result<()> {
        self.inner.delete()
    }
}

#[derive(Clone)]
pub struct SyncStore {
    inner: Store,
}

impl SyncStore {
    pub fn wrap(inner: Store) -> Self {
        Self { inner }
    }
    pub fn create(&self, label: &str) -> Result<SyncNamedGraph, io::Error> {
        self.inner.create(label).map(SyncNamedGraph::wrap)
    }
    pub fn open(&self, label: &str) -> Result<Option<SyncNamedGraph>, io::Error> {
        self.inner.open(label).map(|i| i.map(SyncNamedGraph::wrap))
    }
    pub fn delete(&self, label: &str) -> io::Result<bool> {
        self.inner.delete(label)
    }
    pub fn labels(&self) -> Result<Vec<String>, io::Error> {
        self.inner.labels()
    }
    pub fn get_layer_from_id(&self, layer: [u32; 5]) -> Result<Option<SyncStoreLayer>, io::Error> {
        self.inner
            .get_layer_from_id(layer)
            .map(|l| l.map(SyncStoreLayer::wrap))
    }
    pub fn create_base_layer(&self) -> Result<SyncStoreLayerBuilder, io::Error> {
        self.inner
            .create_base_layer()
            .map(SyncStoreLayerBuilder::wrap)
    }
    pub fn merge_base_layers(
        &self,
        layers: &[[u32; 5]],
        temp_dir: &Path,
    ) -> Result<[u32; 5], io::Error> {
        self.inner.merge_base_layers(layers, temp_dir)
    }
    pub fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.inner.layer_store.export_layers(layer_ids)
    }
    pub fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.inner.layer_store.import_layers(pack, layer_ids)
    }
}

pub fn open_sync_memory_store() -> SyncStore {
    SyncStore::wrap(open_memory_store())
}

pub fn open_sync_directory_store<P: Into<PathBuf>>(path: P) -> SyncStore {
    SyncStore::wrap(open_directory_store(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn create_and_manipulate_sync_memory_database() {
        let store = open_sync_memory_store();
        let database = store.create("foodb").unwrap();
        let head = database.head().unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))
            .unwrap();
        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();
        assert_eq!(layer2_name, layer.name());
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
    }

    #[test]
    fn create_and_manipulate_sync_directory_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_sync_directory_store(dir.path());
        let database = store.create("foodb").unwrap();
        let head = database.head().unwrap();
        assert!(head.is_none());

        let mut builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let layer = builder.commit().unwrap();
        assert!(database.set_head(&layer).unwrap());

        builder = layer.open_write().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))
            .unwrap();
        let layer2 = builder.commit().unwrap();
        assert!(database.set_head(&layer2).unwrap());
        let layer2_name = layer2.name();

        let layer = database.head().unwrap().unwrap();
        assert_eq!(layer2_name, layer.name());
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
    }

    #[test]
    fn create_sync_layer_and_retrieve_it_by_id() {
        let store = open_sync_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let layer = builder.commit().unwrap();
        let id = layer.name();
        let layer2 = store.get_layer_from_id(id).unwrap().unwrap();
        assert!(layer2.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
    }

    #[test]
    fn commit_builder_makes_builder_committed() {
        let store = open_sync_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        assert!(!builder.committed());
        let _layer = builder.commit().unwrap();
        assert!(builder.committed());
    }
}
