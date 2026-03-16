//! High-level API for working with terminus-store.
//!
//! It is expected that most users of this library will work exclusively with the types contained in this module.
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::layer::{IdTriple, Layer, LayerBuilder, LayerCounts, ObjectType, ValueTriple};
use crate::storage::directory::{DirectoryLabelStore, DirectoryLayerStore};
use crate::storage::memory::{MemoryLabelStore, MemoryLayerStore};
use crate::storage::persistence::{LabelPersistence, LayerPersistence};
use crate::storage::persistence_store::{PersistenceLabelStore, PersistenceLayerStore};
use crate::storage::{CachedLayerStore, LabelStore, LayerStore, LockingHashMapLayerCache};
use tdb_succinct_wasm::TypedDictEntry;

use std::io;

/// A store, storing a set of layers and database labels pointing to these layers.
#[derive(Clone)]
pub struct Store {
    label_store: Arc<dyn LabelStore>,
    layer_store: Arc<dyn LayerStore>,
}

/// A wrapper over a SimpleLayerBuilder, providing a thread-safe sharable interface.
///
/// The SimpleLayerBuilder requires one to have a mutable reference to
/// the underlying LayerBuilder, and on commit it will be
/// consumed. This builder only requires an immutable reference, and
/// uses a std::sync::RwLock to synchronize access to it
/// between threads. Also, rather than consuming itself on commit,
/// this wrapper will simply mark itself as having committed,
/// returning errors on further calls.
#[derive(Clone)]
pub struct StoreLayerBuilder {
    parent: Option<Arc<dyn Layer>>,
    builder: Arc<RwLock<Option<Box<dyn LayerBuilder>>>>,
    name: [u32; 5],
    store: Store,
}

impl StoreLayerBuilder {
    fn new(store: Store) -> io::Result<Self> {
        let builder = store.layer_store.create_base_layer()?;

        Ok(Self {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
            store,
        })
    }

    fn wrap(builder: Box<dyn LayerBuilder>, store: Store) -> Self {
        StoreLayerBuilder {
            parent: builder.parent(),
            name: builder.name(),
            builder: Arc::new(RwLock::new(Some(builder))),
            store,
        }
    }

    pub fn with_builder<R, F: FnOnce(&mut Box<dyn LayerBuilder>) -> R>(
        &self,
        f: F,
    ) -> Result<R, io::Error> {
        let mut builder = self
            .builder
            .write()
            .expect("rwlock write should always succeed");
        match (*builder).as_mut() {
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "builder has already been committed",
            )),
            Some(builder) => Ok(f(builder)),
        }
    }

    /// Returns the name of the layer being built.
    pub fn name(&self) -> [u32; 5] {
        self.name
    }

    /// Returns the parent layer this builder is building on top of, if any.
    ///
    /// If there's no parent, this returns None.
    pub fn parent(&self) -> Option<Arc<dyn Layer>> {
        self.parent.clone()
    }

    /// Add a string triple.
    pub fn add_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_value_triple(triple))
    }

    /// Add an id triple.
    pub fn add_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.add_id_triple(triple))
    }

    /// Remove a string triple.
    pub fn remove_value_triple(&self, triple: ValueTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_value_triple(triple))
    }

    /// Remove an id triple.
    pub fn remove_id_triple(&self, triple: IdTriple) -> Result<(), io::Error> {
        self.with_builder(move |b| b.remove_id_triple(triple))
    }

    /// Returns true if this layer has been committed, and false otherwise.
    pub fn committed(&self) -> bool {
        self.builder
            .read()
            .expect("rwlock write should always succeed")
            .is_none()
    }

    /// Commit the layer to storage without loading the resulting layer.
    pub fn commit_no_load(&self) -> io::Result<()> {
        let mut builder = None;
        {
            let mut guard = self
                .builder
                .write()
                .expect("rwlock write should always succeed");

            // Setting the builder to None ensures that committed() detects we already committed (or tried to do so anyway)
            std::mem::swap(&mut builder, &mut guard);
        }

        match builder {
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "builder has already been committed",
                ))
            }
            Some(builder) => {
                let id = builder.name();
                builder.commit_boxed()?;
                self.store.layer_store.finalize_layer(id)
            }
        }
    }

    /// Commit the layer to storage.
    pub fn commit(&self) -> io::Result<StoreLayer> {
        let name = self.name;
        self.commit_no_load()?;

        let layer = self.store.layer_store.get_layer(name)?;
        Ok(StoreLayer::wrap(
            layer.expect("layer that was just created was not found in store"),
            self.store.clone(),
        ))
    }

    /// Apply all triples added and removed by a layer to this builder.
    ///
    /// This is a way to 'cherry-pick' a layer on top of another
    /// layer, without caring about its history.
    pub fn apply_delta(&self, delta: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        let triple_additions = delta.triple_additions()?;
        let triple_removals = delta.triple_removals()?;
        triple_additions.for_each(|t| {
            delta
                .id_triple_to_string(&t)
                .map(|st| self.add_value_triple(st));
        });
        triple_removals.for_each(|t| {
            delta
                .id_triple_to_string(&t)
                .map(|st| self.remove_value_triple(st));
        });

        Ok(())
    }

    /// Apply the changes required to change our parent layer into the given layer.
    pub fn apply_diff(&self, other: &StoreLayer) -> Result<(), io::Error> {
        // create a child builder and use it directly
        // first check what dictionary entries we don't know about, add those
        if let Some(this) = self.parent() {
            this.triples().for_each(|t| {
                if let Some(st) = this.id_triple_to_string(&t) {
                    if !other.value_triple_exists(&st) {
                        self.remove_value_triple(st).unwrap()
                    }
                }
            });
        }
        other.triples().for_each(|t| {
            if let Some(st) = other.id_triple_to_string(&t) {
                if let Some(this) = self.parent() {
                    if !this.value_triple_exists(&st) {
                        self.add_value_triple(st).unwrap()
                    }
                } else {
                    self.add_value_triple(st).unwrap()
                };
            }
        });

        Ok(())
    }

    // Apply changes required to change our parent layer into after merge.
    // This is a three-way merge with other layers relative to the merge base if given.
    // Requires at least two layers in `others` to perform a meaningful merge.
    pub fn apply_merge(
        &self,
        others: Vec<&StoreLayer>,
        merge_base: Option<&StoreLayer>,
    ) -> Result<(), io::Error> {
        if others.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "merge requires at least two layers",
            ));
        }
        match merge_base {
            Some(base) => {
                base.triples().for_each(|b| {
                    if let Some(t) = base.id_triple_to_string(&b) {
                        if others.iter().any(|o| !o.value_triple_exists(&t)) {
                            self.remove_value_triple(t).unwrap();
                        }
                    }
                });
            }
            None => {}
        }
        others.iter().for_each(|os| {
            os.triples().for_each(|o| {
                if let Some(t) = os.id_triple_to_string(&o) {
                    match merge_base {
                        Some(base) => {
                            if !base.value_triple_exists(&t) {
                                self.add_value_triple(t).unwrap();
                            }
                        }
                        None => {
                            self.add_value_triple(t).unwrap();
                        }
                    }
                }
            })
        });
        Ok(())
    }
}

/// A layer that keeps track of the store it came out of, allowing the creation of a layer builder on top of this layer.
///
/// This type of layer supports querying what was added and what was
/// removed in this layer. This can not be done in general, because
/// the layer that has been loaded may not be the layer that was
/// originally built. This happens whenever a rollup is done. A rollup
/// will create a new layer that bundles the changes of various
/// layers. It allows for more efficient querying, but loses the
/// ability to do these delta queries directly. In order to support
/// them anyway, the StoreLayer will dynamically load in the relevant
/// files to perform the requested addition or removal query method.
#[derive(Clone)]
pub struct StoreLayer {
    // TODO this Arc here is not great
    layer: Arc<dyn Layer>,
    store: Store,
}

impl StoreLayer {
    fn wrap(layer: Arc<dyn Layer>, store: Store) -> Self {
        StoreLayer { layer, store }
    }

    /// Create a layer builder based on this layer.
    pub fn open_write(&self) -> io::Result<StoreLayerBuilder> {
        let layer = self
            .store
            .layer_store
            .create_child_layer(self.layer.name())?;

        Ok(StoreLayerBuilder::wrap(layer, self.store.clone()))
    }

    /// Returns the parent of this layer, if any, or None if this layer has no parent.
    pub fn parent(&self) -> io::Result<Option<StoreLayer>> {
        let parent_name = self.layer.parent_name();

        match parent_name {
            None => Ok(None),
            Some(parent_name) => match self.store.layer_store.get_layer(parent_name)? {
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "parent layer not found even though it should exist",
                )),
                Some(layer) => Ok(Some(StoreLayer::wrap(layer, self.store.clone()))),
            },
        }
    }

    pub fn squash_upto(&self, upto: &StoreLayer) -> io::Result<StoreLayer> {
        let layer_opt = self.store.layer_store.get_layer(self.name())?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let name = self.store.layer_store.squash_upto(layer, upto.name())?;
        Ok(self
            .store
            .get_layer_from_id(name)?
            .expect("layer that was just created doesn't exist"))
    }

    /// Create a new base layer consisting of all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Squash is one approach of
    /// accomplishing this. Rollup is another. Squash is the better
    /// option if you do not care for history, as it throws away all
    /// data that you no longer need.
    pub fn squash(&self) -> io::Result<StoreLayer> {
        let layer_opt = self.store.layer_store.get_layer(self.name())?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let name = self.store.layer_store.squash(layer)?;
        Ok(self
            .store
            .get_layer_from_id(name)?
            .expect("layer that was just created doesn't exist"))
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all its ancestors.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub fn rollup(&self) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name())?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "layer not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup(layer)?;
        Ok(())
    }

    /// Create a new rollup layer which rolls up all triples in this layer, as well as all ancestors up to (but not including) the given ancestor.
    ///
    /// It is a good idea to keep layer stacks small, meaning, to only
    /// have a handful of ancestors for a layer. The more layers there
    /// are, the longer queries take. Rollup is one approach of
    /// accomplishing this. Squash is another. Rollup is the better
    /// option if you need to retain history.
    pub fn rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name())?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.rollup_upto(layer, upto.name())?;
        Ok(())
    }

    /// Like rollup_upto, rolls up upto the given layer. However, if
    /// this layer is a rollup layer, this will roll up upto that
    /// rollup.
    pub fn imprecise_rollup_upto(&self, upto: &StoreLayer) -> io::Result<()> {
        let store1 = self.store.layer_store.clone();
        // TODO: This is awkward, we should have a way to get the internal layer
        let layer_opt = store1.get_layer(self.name())?;
        let layer =
            layer_opt.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "label not found"))?;
        let store2 = self.store.layer_store.clone();
        store2.imprecise_rollup_upto(layer, upto.name())?;
        Ok(())
    }

    /// Returns a future that yields true if this triple has been added in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.store
            .layer_store
            .triple_addition_exists(self.layer.name(), subject, predicate, object)
    }

    /// Returns a future that yields true if this triple has been removed in this layer, or false if it doesn't.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        self.store
            .layer_store
            .triple_removal_exists(self.layer.name(), subject, predicate, object)
    }

    /// Returns a future that yields an iterator over all layer additions.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let result = self.store.layer_store.triple_additions(self.layer.name())?;

        Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
    }

    /// Returns a future that yields an iterator over all layer removals.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals(&self) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        let result = self.store.layer_store.triple_removals(self.layer.name())?;

        Ok(Box::new(result) as Box<dyn Iterator<Item = _> + Send>)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_s(self.layer.name(), subject)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_s(
        &self,
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_s(self.layer.name(), subject)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_sp(self.layer.name(), subject, predicate)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular subject and predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_sp(self.layer.name(), subject, predicate)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_p(self.layer.name(), predicate)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular predicate.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_p(
        &self,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_p(self.layer.name(), predicate)
    }

    /// Returns a future that yields an iterator over all layer additions that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_additions_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_additions_o(self.layer.name(), object)
    }

    /// Returns a future that yields an iterator over all layer removals that share a particular object.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_removals_o(
        &self,
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        self.store
            .layer_store
            .triple_removals_o(self.layer.name(), object)
    }

    /// Returns a future that yields the amount of triples that this layer adds.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_addition_count(&self) -> io::Result<usize> {
        self.store
            .layer_store
            .triple_layer_addition_count(self.layer.name())
    }

    /// Returns a future that yields the amount of triples that this layer removes.
    ///
    /// Since this operation will involve io when this layer is a
    /// rollup layer, io errors may occur.
    pub fn triple_layer_removal_count(&self) -> io::Result<usize> {
        self.store
            .layer_store
            .triple_layer_removal_count(self.layer.name())
    }

    /// Returns a future that yields a vector of layer stack names describing the history of this layer, starting from the base layer up to and including the name of this layer itself.
    pub fn retrieve_layer_stack_names(&self) -> io::Result<Vec<[u32; 5]>> {
        self.store
            .layer_store
            .retrieve_layer_stack_names(self.name())
    }
}

impl PartialEq for StoreLayer {
    #[allow(clippy::vtable_address_comparisons)]
    fn eq(&self, other: &StoreLayer) -> bool {
        Arc::ptr_eq(&self.layer, &other.layer)
    }
}

impl Eq for StoreLayer {}

impl Layer for StoreLayer {
    fn name(&self) -> [u32; 5] {
        self.layer.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.layer.parent_name()
    }

    fn node_and_value_count(&self) -> usize {
        self.layer.node_and_value_count()
    }

    fn predicate_count(&self) -> usize {
        self.layer.predicate_count()
    }

    fn subject_id(&self, subject: &str) -> Option<u64> {
        self.layer.subject_id(subject)
    }

    fn predicate_id(&self, predicate: &str) -> Option<u64> {
        self.layer.predicate_id(predicate)
    }

    fn object_node_id(&self, object: &str) -> Option<u64> {
        self.layer.object_node_id(object)
    }

    fn object_value_id(&self, object: &TypedDictEntry) -> Option<u64> {
        self.layer.object_value_id(object)
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        self.layer.id_subject(id)
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        self.layer.id_predicate(id)
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        self.layer.id_object(id)
    }

    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        self.layer.id_object_is_node(id)
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        self.layer.triple_exists(subject, predicate, object)
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples()
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_s(subject)
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_sp(subject, predicate)
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_p(predicate)
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        self.layer.triples_o(object)
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_addition_count(&self) -> usize {
        self.layer.triple_addition_count()
    }

    fn triple_removal_count(&self) -> usize {
        self.layer.triple_removal_count()
    }

    fn all_counts(&self) -> LayerCounts {
        self.layer.all_counts()
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        self.layer.single_triple_sp(subject, predicate)
    }
}

/// A named graph in terminus-store.
///
/// Named graphs in terminus-store are basically just a label pointing
/// to a layer. Opening a read transaction to a named graph is just
/// getting hold of the layer it points at, as layers are
/// read-only. Writing to a named graph is just making it point to a
/// new layer.
#[derive(Clone)]
pub struct NamedGraph {
    label: String,
    store: Store,
}

impl NamedGraph {
    fn new(label: String, store: Store) -> Self {
        NamedGraph { label, store }
    }

    /// Returns the label name itself.
    pub fn name(&self) -> &str {
        &self.label
    }

    /// Returns the layer this database points at, as well as the label version.
    pub fn head_version(&self) -> io::Result<(Option<StoreLayer>, u64)> {
        let new_label = self.store.label_store.get_label(&self.label)?;

        match new_label {
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "database not found",
            )),
            Some(new_label) => {
                let layer = match new_label.layer {
                    None => None,
                    Some(layer) => {
                        let layer = self.store.layer_store.get_layer(layer)?;
                        match layer {
                            None => {
                                return Err(io::Error::new(
                                    io::ErrorKind::NotFound,
                                    "layer not found even though it is pointed at by a label",
                                ))
                            }
                            Some(layer) => Some(StoreLayer::wrap(layer, self.store.clone())),
                        }
                    }
                };
                Ok((layer, new_label.version))
            }
        }
    }

    /// Returns the layer this database points at.
    pub fn head(&self) -> io::Result<Option<StoreLayer>> {
        Ok(self.head_version()?.0)
    }

    /// Set the database label to the given layer if it is a valid ancestor, returning false otherwise.
    pub fn set_head(&self, layer: &StoreLayer) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label)?;
        if label.is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "label not found"));
        }
        let label = label.unwrap();

        let set_is_ok = match label.layer {
            None => true,
            Some(retrieved_layer_name) => self
                .store
                .layer_store
                .layer_is_ancestor_of(layer_name, retrieved_layer_name)?,
        };

        if set_is_ok {
            Ok(self
                .store
                .label_store
                .set_label(&label, layer_name)?
                .is_some())
        } else {
            Ok(false)
        }
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor.
    pub fn force_set_head(&self, layer: &StoreLayer) -> io::Result<()> {
        let layer_name = layer.name();

        // We are stomping on the label but `set_label` expects us to
        // know about the current label, which may have been updated
        // concurrently.
        // So keep looping until an update was succesful or an error
        // was encountered.
        loop {
            let label = self.store.label_store.get_label(&self.label)?;
            match label {
                None => return Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
                Some(label) => {
                    if self
                        .store
                        .label_store
                        .set_label(&label, layer_name)?
                        .is_some()
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Set the database label to the given layer, even if it is not a valid ancestor. Also checks given version, and if it doesn't match, the update won't happen and false will be returned.
    pub fn force_set_head_version(&self, layer: &StoreLayer, version: u64) -> io::Result<bool> {
        let layer_name = layer.name();
        let label = self.store.label_store.get_label(&self.label)?;
        match label {
            None => Err(io::Error::new(io::ErrorKind::NotFound, "label not found")),
            Some(label) => {
                if label.version != version {
                    Ok(false)
                } else {
                    Ok(self
                        .store
                        .label_store
                        .set_label(&label, layer_name)?
                        .is_some())
                }
            }
        }
    }

    pub fn delete(&self) -> io::Result<()> {
        self.store.delete(&self.label).map(|_| ())
    }
}

impl Store {
    /// Create a new store from the given label and layer store.
    pub fn new<Labels: 'static + LabelStore, Layers: 'static + LayerStore>(
        label_store: Labels,
        layer_store: Layers,
    ) -> Store {
        Store {
            label_store: Arc::new(label_store),
            layer_store: Arc::new(layer_store),
        }
    }

    /// Create a new database with the given name.
    ///
    /// If the database already exists, this will return an error.
    pub fn create(&self, label: &str) -> io::Result<NamedGraph> {
        let label = self.label_store.create_label(label)?;
        Ok(NamedGraph::new(label.name, self.clone()))
    }

    /// Open an existing database with the given name, or None if it does not exist.
    pub fn open(&self, label: &str) -> io::Result<Option<NamedGraph>> {
        let label = self.label_store.get_label(label)?;
        Ok(label.map(|label| NamedGraph::new(label.name, self.clone())))
    }

    /// Delete an existing database with the given name. Returns true if this database was deleted
    /// and false otherwise.
    pub fn delete(&self, label: &str) -> io::Result<bool> {
        self.label_store.delete_label(label)
    }

    /// Return list of names of all existing databases.
    pub fn labels(&self) -> io::Result<Vec<String>> {
        let labels = self.label_store.labels()?;
        Ok(labels.iter().map(|label| label.name.to_string()).collect())
    }

    /// Retrieve a layer with the given name from the layer store this Store was initialized with.
    pub fn get_layer_from_id(&self, layer: [u32; 5]) -> io::Result<Option<StoreLayer>> {
        let layer = self.layer_store.get_layer(layer)?;
        Ok(layer.map(|layer| StoreLayer::wrap(layer, self.clone())))
    }

    /// Create a base layer builder, unattached to any database label.
    ///
    /// After having committed it, use `set_head` on a `NamedGraph` to attach it.
    pub fn create_base_layer(&self) -> io::Result<StoreLayerBuilder> {
        StoreLayerBuilder::new(self.clone())
    }

    pub fn merge_base_layers(&self, layers: &[[u32; 5]], temp_dir: &Path) -> io::Result<[u32; 5]> {
        self.layer_store.merge_base_layer(layers, temp_dir)
    }

    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    pub fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.layer_store.export_layers(layer_ids)
    }

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    pub fn import_layers<'a>(
        &'a self,
        pack: &'a [u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.layer_store.import_layers(pack, layer_ids)
    }
}

/// Open a store that is entirely in memory.
///
/// This is useful for testing purposes, or if the database is only going to be used for caching purposes.
pub fn open_memory_store() -> Store {
    Store::new(
        MemoryLabelStore::new(),
        CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new()),
    )
}

/// Open a store that stores its data in the given directory.
pub fn open_directory_store<P: Into<PathBuf>>(path: P) -> Store {
    let p = path.into();
    Store::new(
        DirectoryLabelStore::new(p.clone()),
        CachedLayerStore::new(DirectoryLayerStore::new(p), LockingHashMapLayerCache::new()),
    )
}

/// Open a store backed by a persistence backend that implements both
/// `LayerPersistence` and `LabelPersistence`.
///
/// This is the generic entry point for creating stores from any persistence
/// backend (MemoryPersistence, FsPersistence, OpfsPersistence, etc.).
///
/// The persistence backend is wrapped in a `PersistenceLayerStore` (which
/// bridges `LayerPersistence` to the `PersistentLayerStore` trait) and a
/// `PersistenceLabelStore` (which bridges `LabelPersistence` to `LabelStore`),
/// with a `CachedLayerStore` providing in-memory layer caching.
pub fn open_persistence_store<P>(persistence: P) -> Store
where
    P: 'static + LayerPersistence + LabelPersistence + Clone + Send + Sync,
{
    Store::new(
        PersistenceLabelStore::new(persistence.clone()),
        CachedLayerStore::new(
            PersistenceLayerStore::new(persistence),
            LockingHashMapLayerCache::new(),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_and_manipulate_database(store: Store) {
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
    fn create_and_manipulate_memory_database() {
        let store = open_memory_store();

        create_and_manipulate_database(store);
    }

    #[test]
    fn create_and_manipulate_directory_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());

        create_and_manipulate_database(store);
    }

    #[test]
    fn create_layer_and_retrieve_it_by_id() {
        let store = open_memory_store();
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
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        assert!(!builder.committed());

        builder.commit_no_load().unwrap();

        assert!(builder.committed());
    }

    #[test]
    fn hard_reset() {
        let store = open_memory_store();
        let database = store.create("foodb").unwrap();

        let builder1 = store.create_base_layer().unwrap();
        builder1
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer1 = builder1.commit().unwrap();

        assert!(database.set_head(&layer1).unwrap());

        let builder2 = store.create_base_layer().unwrap();
        builder2
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();

        let layer2 = builder2.commit().unwrap();

        database.force_set_head(&layer2).unwrap();

        let new_layer = database.head().unwrap().unwrap();

        assert!(
            new_layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack"))
        );
        assert!(
            !new_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[test]
    fn create_two_layers_and_squash() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let builder2 = layer.open_write().unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();

        builder2
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        builder2
            .remove_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();

        builder2
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();

        let layer2 = builder2.commit().unwrap();

        let new = layer2.squash().unwrap();
        let triples: Vec<_> = new
            .triples()
            .map(|t| new.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("dog", "says", "woof"),
            ],
            triples
        );

        assert!(new.parent().unwrap().is_none());
    }

    #[test]
    fn create_three_layers_and_squash_last_two() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().unwrap();

        let builder = base_layer.open_write().unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().unwrap();
        let builder = intermediate_layer.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().unwrap();

        let squashed_layer = final_layer.squash_upto(&base_layer).unwrap();
        assert_eq!(squashed_layer.parent_name().unwrap(), base_layer.name());
        let additions: Vec<_> = squashed_layer
            .triple_additions()
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            additions
        );
        let removals: Vec<_> = squashed_layer
            .triple_removals()
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "hates", "duck"),
                ValueTriple::new_string_value("cow", "says", "quack"),
            ],
            removals
        );

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            all_triples
        );
    }

    #[test]
    fn create_three_layers_and_squash_all_after_rollup() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().unwrap();

        let builder = base_layer.open_write().unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().unwrap();
        let builder = intermediate_layer.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().unwrap();
        final_layer.rollup_upto(&base_layer).unwrap();
        let final_rolled_layer = store
            .get_layer_from_id(final_layer.name())
            .unwrap()
            .unwrap();

        let squashed_layer = final_rolled_layer.squash().unwrap();
        assert!(squashed_layer.parent_name().is_none());

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
            ],
            all_triples
        );
    }

    #[test]
    fn create_three_layers_and_squash_last_two_after_rollup() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "horse"))
            .unwrap();

        let base_layer = builder.commit().unwrap();

        let builder = base_layer.open_write().unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("bunny", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "quack"))
            .unwrap();

        let intermediate_layer = builder.commit().unwrap();
        let builder = intermediate_layer.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("cow", "hates", "duck"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("bunny", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("bunny", "says", "sniff"))
            .unwrap();
        let final_layer = builder.commit().unwrap();
        final_layer.rollup_upto(&base_layer).unwrap();
        let final_rolled_layer = store
            .get_layer_from_id(final_layer.name())
            .unwrap()
            .unwrap();

        let squashed_layer = final_rolled_layer.squash_upto(&base_layer).unwrap();
        assert_eq!(squashed_layer.parent_name().unwrap(), base_layer.name());
        let additions: Vec<_> = squashed_layer
            .triple_additions()
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            additions
        );
        let removals: Vec<_> = squashed_layer
            .triple_removals()
            .unwrap()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "hates", "duck"),
                ValueTriple::new_string_value("cow", "says", "quack"),
            ],
            removals
        );

        let all_triples: Vec<_> = squashed_layer
            .triples()
            .map(|t| squashed_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("cow", "likes", "horse"),
                ValueTriple::new_string_value("cow", "says", "moo"),
                ValueTriple::new_node("duck", "likes", "cow"),
                ValueTriple::new_string_value("duck", "says", "quack"),
                ValueTriple::new_node("bunny", "likes", "cow"),
                ValueTriple::new_string_value("bunny", "says", "sniff"),
            ],
            all_triples
        );
    }

    #[test]
    fn squash_and_forget_dict_entries() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "b", "anode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "b", "astring"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();

        let base_layer = builder.commit().unwrap();

        let builder = base_layer.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();
        let child_layer = builder.commit().unwrap();

        let squashed = child_layer.squash().unwrap();
        // annoyingly we need to get the internal layer version, so lets re-retrieve
        let squashed = store
            .layer_store
            .get_layer(squashed.name())
            .unwrap()
            .unwrap();
        let nodes: Vec<_> = squashed
            .node_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"a" as &[u8], b"anode"], nodes);
        let preds: Vec<_> = squashed
            .predicate_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"b" as &[u8]], preds);
        let vals: Vec<_> = squashed
            .value_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"astring" as &[u8]], vals);

        let all_triples: Vec<_> = squashed
            .triples()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("a", "b", "anode"),
                ValueTriple::new_string_value("a", "b", "astring"),
            ],
            all_triples
        );
    }

    #[test]
    fn squash_upto_and_forget_dict_entries() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("foo", "bar", "baz"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("baz", "bar", "quux"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("foo", "baz", "hai"))
            .unwrap();
        let base_layer = builder.commit().unwrap();
        let builder = base_layer.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("foo", "baz", "hai"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "b", "anode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "b", "astring"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();

        let child_layer1 = builder.commit().unwrap();

        let builder = child_layer1.open_write().unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("foo", "bar", "baz"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_node("a", "c", "anothernode"))
            .unwrap();
        builder
            .remove_value_triple(ValueTriple::new_string_value("a", "c", "anotherstring"))
            .unwrap();
        let child_layer2 = builder.commit().unwrap();

        let squashed = child_layer2.squash_upto(&base_layer).unwrap();
        // annoyingly we need to get the internal layer version, so lets re-retrieve
        let squashed = store
            .layer_store
            .get_layer(squashed.name())
            .unwrap()
            .unwrap();
        let nodes: Vec<_> = squashed
            .node_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"a" as &[u8], b"anode"], nodes);
        let preds: Vec<_> = squashed
            .predicate_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"b" as &[u8]], preds);
        let vals: Vec<_> = squashed
            .value_dictionary()
            .iter()
            .map(|b| b.to_bytes())
            .collect();
        assert_eq!(vec![b"astring" as &[u8]], vals);

        let all_triple_additions: Vec<_> = squashed
            .internal_triple_additions()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        let all_triple_removals: Vec<_> = squashed
            .internal_triple_removals()
            .map(|t| squashed.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("a", "b", "anode"),
                ValueTriple::new_string_value("a", "b", "astring"),
            ],
            all_triple_additions
        );
        assert_eq!(
            vec![
                ValueTriple::new_node("foo", "bar", "baz"),
                ValueTriple::new_string_value("foo", "baz", "hai"),
            ],
            all_triple_removals
        );
    }

    #[test]
    fn apply_a_base_delta() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let builder2 = layer.open_write().unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = builder2.commit().unwrap();

        let delta_builder_1 = store.create_base_layer().unwrap();

        delta_builder_1
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        delta_builder_1
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();

        let delta_1 = delta_builder_1.commit().unwrap();

        let delta_builder_2 = delta_1.open_write().unwrap();

        delta_builder_2
            .add_value_triple(ValueTriple::new_string_value("crow", "says", "caw"))
            .unwrap();
        delta_builder_2
            .remove_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();

        let delta = delta_builder_2.commit().unwrap();

        let rebase_builder = layer2.open_write().unwrap();

        let _ = rebase_builder.apply_delta(&delta).unwrap();

        let rebase_layer = rebase_builder.commit().unwrap();

        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("crow", "says", "caw"))
        );
        assert!(
            rebase_layer.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof"))
        );
        assert!(!rebase_layer
            .value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
    }

    #[test]
    fn apply_a_merge() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();

        let merge_base = builder.commit().unwrap();

        let builder2 = merge_base.open_write().unwrap();

        builder2
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();

        let layer2 = builder2.commit().unwrap();

        let builder3 = merge_base.open_write().unwrap();

        builder3
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();

        let layer3 = builder3.commit().unwrap();

        let builder4 = merge_base.open_write().unwrap();

        builder4
            .add_value_triple(ValueTriple::new_string_value("bird", "says", "twe"))
            .unwrap();

        let layer4 = builder4.commit().unwrap();

        let merge_builder = layer4.open_write().unwrap();

        let _ = merge_builder.apply_merge(vec![&layer2, &layer3], Some(&merge_base));

        let merged_layer = merge_builder.commit().unwrap();

        assert!(
            merged_layer.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow"))
        );
        assert!(
            merged_layer.value_triple_exists(&ValueTriple::new_string_value("bird", "says", "twe"))
        );
        assert!(
            merged_layer.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof"))
        );
        assert!(
            !merged_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
    }

    #[test]
    fn apply_diff_rebases_parent_to_target() {
        let store = open_memory_store();

        // Create a base layer with shared triples
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        let base = builder.commit().unwrap();

        // Create a divergent target layer: removes cow, adds dog
        let target_builder = base.open_write().unwrap();
        target_builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        target_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        let target = target_builder.commit().unwrap();

        // Create a child of base and apply_diff to rebase onto target
        let diff_builder = base.open_write().unwrap();
        diff_builder.apply_diff(&target).unwrap();
        let diffed = diff_builder.commit().unwrap();

        // Result should match target: cat + dog, no cow
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(!diffed.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
    }

    #[test]
    fn apply_diff_no_parent_adds_all_target_triples() {
        let store = open_memory_store();

        // Create a target layer with some triples
        let target_builder = store.create_base_layer().unwrap();
        target_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        target_builder
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        let target = target_builder.commit().unwrap();

        // Create a base layer builder (no parent) and apply_diff
        let diff_builder = store.create_base_layer().unwrap();
        diff_builder.apply_diff(&target).unwrap();
        let diffed = diff_builder.commit().unwrap();

        // All target triples should be added
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
    }

    #[test]
    fn apply_merge_without_merge_base_adds_all_triples() {
        let store = open_memory_store();

        // Create two independent layers
        let builder1 = store.create_base_layer().unwrap();
        builder1
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        let layer1 = builder1.commit().unwrap();

        let builder2 = store.create_base_layer().unwrap();
        builder2
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        let layer2 = builder2.commit().unwrap();

        // Merge without merge base (None) — should add all triples from all others
        let merge_builder = store.create_base_layer().unwrap();
        merge_builder
            .apply_merge(vec![&layer1, &layer2], None)
            .unwrap();
        let merged = merge_builder.commit().unwrap();

        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
    }

    #[test]
    fn merge_union_of_additions_from_all_branches() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("base", "has", "triple"))
            .unwrap();
        let merge_base = builder.commit().unwrap();

        // Branch A adds "dog says woof"
        let branch_a_builder = merge_base.open_write().unwrap();
        branch_a_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        let branch_a = branch_a_builder.commit().unwrap();

        // Branch B adds "bird says tweet"
        let branch_b_builder = merge_base.open_write().unwrap();
        branch_b_builder
            .add_value_triple(ValueTriple::new_string_value("bird", "says", "tweet"))
            .unwrap();
        let branch_b = branch_b_builder.commit().unwrap();

        // Merge on top of branch_a
        let merge_builder = branch_a.open_write().unwrap();
        merge_builder
            .apply_merge(vec![&branch_a, &branch_b], Some(&merge_base))
            .unwrap();
        let merged = merge_builder.commit().unwrap();

        // Union: both additions present
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("bird", "says", "tweet")));
        // Base triple still present (not removed by any branch)
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("base", "has", "triple")));
    }

    #[test]
    fn merge_removal_wins_conflict_resolution() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        let merge_base = builder.commit().unwrap();

        // Branch A: keeps both triples, adds one
        let branch_a_builder = merge_base.open_write().unwrap();
        branch_a_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        let branch_a = branch_a_builder.commit().unwrap();

        // Branch B: removes "cow says moo"
        let branch_b_builder = merge_base.open_write().unwrap();
        branch_b_builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let branch_b = branch_b_builder.commit().unwrap();

        let merge_builder = branch_a.open_write().unwrap();
        merge_builder
            .apply_merge(vec![&branch_a, &branch_b], Some(&merge_base))
            .unwrap();
        let merged = merge_builder.commit().unwrap();

        // Removal wins: cow removed by branch B, so absent in merged
        assert!(!merged.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        // cat still present (not removed by any branch)
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
        // dog added by branch A
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
    }

    #[test]
    fn merge_error_if_fewer_than_two_layers() {
        let store = open_memory_store();
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        let base = builder.commit().unwrap();

        let child_builder = base.open_write().unwrap();
        child_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        let child = child_builder.commit().unwrap();

        // Try merge with only one layer — should error
        let merge_builder = child.open_write().unwrap();
        let result = merge_builder.apply_merge(vec![&child], Some(&base));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        // Try merge with zero layers — should error
        let merge_builder2 = child.open_write().unwrap();
        let result2 = merge_builder2.apply_merge(vec![], Some(&base));
        assert!(result2.is_err());
        assert_eq!(result2.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn apply_diff_divergent_branches_converge_to_target() {
        let store = open_memory_store();

        // Base layer: cow, cat, horse
        let builder = store.create_base_layer().unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("horse", "likes", "grass"))
            .unwrap();
        let base = builder.commit().unwrap();

        // Divergent target: removes cow and horse, keeps cat, adds dog and bird
        let target_builder = base.open_write().unwrap();
        target_builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        target_builder
            .remove_value_triple(ValueTriple::new_node("horse", "likes", "grass"))
            .unwrap();
        target_builder
            .add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        target_builder
            .add_value_triple(ValueTriple::new_node("bird", "likes", "sky"))
            .unwrap();
        let target = target_builder.commit().unwrap();

        // Collect target triples for comparison
        let target_triples: std::collections::HashSet<_> = target
            .triples()
            .filter_map(|t| target.id_triple_to_string(&t))
            .collect();

        // Create a child of base and apply_diff to rebase onto target
        let diff_builder = base.open_write().unwrap();
        diff_builder.apply_diff(&target).unwrap();
        let diffed = diff_builder.commit().unwrap();

        // Collect diffed triples
        let diffed_triples: std::collections::HashSet<_> = diffed
            .triples()
            .filter_map(|t| diffed.id_triple_to_string(&t))
            .collect();

        // The diffed layer should contain exactly the same triples as the target
        assert_eq!(target_triples, diffed_triples);

        // Explicit checks
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
        assert!(diffed.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(diffed.value_triple_exists(&ValueTriple::new_node("bird", "likes", "sky")));
        assert!(!diffed.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(!diffed.value_triple_exists(&ValueTriple::new_node("horse", "likes", "grass")));
    }

    #[test]
    fn apply_merge_no_base_three_layers_adds_all() {
        let store = open_memory_store();

        // Three independent layers with distinct triples (including node triples)
        let b1 = store.create_base_layer().unwrap();
        b1.add_value_triple(ValueTriple::new_string_value("dog", "says", "woof"))
            .unwrap();
        b1.add_value_triple(ValueTriple::new_node("dog", "likes", "bone"))
            .unwrap();
        let layer1 = b1.commit().unwrap();

        let b2 = store.create_base_layer().unwrap();
        b2.add_value_triple(ValueTriple::new_string_value("cat", "says", "meow"))
            .unwrap();
        b2.add_value_triple(ValueTriple::new_node("cat", "likes", "fish"))
            .unwrap();
        let layer2 = b2.commit().unwrap();

        let b3 = store.create_base_layer().unwrap();
        b3.add_value_triple(ValueTriple::new_string_value("bird", "says", "tweet"))
            .unwrap();
        b3.add_value_triple(ValueTriple::new_node("bird", "likes", "sky"))
            .unwrap();
        let layer3 = b3.commit().unwrap();

        // Merge all three without merge base
        let merge_builder = store.create_base_layer().unwrap();
        merge_builder
            .apply_merge(vec![&layer1, &layer2, &layer3], None)
            .unwrap();
        let merged = merge_builder.commit().unwrap();

        // All triples from all three layers should be present
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("dog", "says", "woof")));
        assert!(merged.value_triple_exists(&ValueTriple::new_node("dog", "likes", "bone")));
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("cat", "says", "meow")));
        assert!(merged.value_triple_exists(&ValueTriple::new_node("cat", "likes", "fish")));
        assert!(merged.value_triple_exists(&ValueTriple::new_string_value("bird", "says", "tweet")));
        assert!(merged.value_triple_exists(&ValueTriple::new_node("bird", "likes", "sky")));

        // Verify total count matches (6 distinct triples)
        let all_triples: Vec<_> = merged
            .triples()
            .filter_map(|t| merged.id_triple_to_string(&t))
            .collect();
        assert_eq!(6, all_triples.len());
    }

    fn cached_layer_name_does_not_change_after_rollup(store: Store) {
        let builder = store.create_base_layer().unwrap();
        let base_name = builder.name();
        let x = builder.commit().unwrap();
        let builder = x.open_write().unwrap();
        let child_name = builder.name();
        builder.commit().unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name).unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name, unrolled_name);
        assert_eq!(base_name, unrolled_parent_name);

        unrolled_layer.rollup().unwrap();
        let rolled_layer = store.get_layer_from_id(child_name).unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name, rolled_name);
        assert_eq!(base_name, rolled_parent_name);

        rolled_layer.rollup().unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name).unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name, rolled_name2);
        assert_eq!(base_name, rolled_parent_name2);
    }

    #[test]
    fn mem_cached_layer_name_does_not_change_after_rollup() {
        let store = open_memory_store();

        cached_layer_name_does_not_change_after_rollup(store)
    }

    #[test]
    fn dir_cached_layer_name_does_not_change_after_rollup() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());

        cached_layer_name_does_not_change_after_rollup(store)
    }

    fn cached_layer_name_does_not_change_after_rollup_upto(store: Store) {
        let builder = store.create_base_layer().unwrap();
        let _base_name = builder.name();
        let base_layer = builder.commit().unwrap();
        let builder = base_layer.open_write().unwrap();
        let child_name = builder.name();
        let x = builder.commit().unwrap();
        let builder = x.open_write().unwrap();
        let child_name2 = builder.name();
        builder.commit().unwrap();

        let unrolled_layer = store.get_layer_from_id(child_name2).unwrap().unwrap();
        let unrolled_name = unrolled_layer.name();
        let unrolled_parent_name = unrolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, unrolled_name);
        assert_eq!(child_name, unrolled_parent_name);

        unrolled_layer.rollup_upto(&base_layer).unwrap();
        let rolled_layer = store.get_layer_from_id(child_name2).unwrap().unwrap();
        let rolled_name = rolled_layer.name();
        let rolled_parent_name = rolled_layer.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name);
        assert_eq!(child_name, rolled_parent_name);

        rolled_layer.rollup_upto(&base_layer).unwrap();
        let rolled_layer2 = store.get_layer_from_id(child_name2).unwrap().unwrap();
        let rolled_name2 = rolled_layer2.name();
        let rolled_parent_name2 = rolled_layer2.parent_name().unwrap();
        assert_eq!(child_name2, rolled_name2);
        assert_eq!(child_name, rolled_parent_name2);
    }

    #[test]
    fn mem_cached_layer_name_does_not_change_after_rollup_upto() {
        let store = open_memory_store();
        cached_layer_name_does_not_change_after_rollup_upto(store)
    }

    #[test]
    fn dir_cached_layer_name_does_not_change_after_rollup_upto() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        cached_layer_name_does_not_change_after_rollup_upto(store)
    }

    #[test]
    fn force_update_with_matching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();
        let (layer, version) = graph.head_version().unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().unwrap();
        let layer = builder.commit().unwrap();

        assert!(graph.force_set_head_version(&layer, 0).unwrap());
    }

    #[test]
    fn force_update_with_mismatching_0_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();
        let (layer, version) = graph.head_version().unwrap();
        assert!(layer.is_none());
        assert_eq!(0, version);

        let builder = store.create_base_layer().unwrap();
        let layer = builder.commit().unwrap();

        assert!(!graph.force_set_head_version(&layer, 3).unwrap());
    }

    #[test]
    fn force_update_with_matching_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();

        let builder = store.create_base_layer().unwrap();
        let layer = builder.commit().unwrap();
        assert!(graph.set_head(&layer).unwrap());

        let (_, version) = graph.head_version().unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().unwrap();
        let layer2 = builder2.commit().unwrap();

        assert!(graph.force_set_head_version(&layer2, 1).unwrap());
    }

    #[test]
    fn force_update_with_mismatched_version_succeeds() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();

        let builder = store.create_base_layer().unwrap();
        let layer = builder.commit().unwrap();
        assert!(graph.set_head(&layer).unwrap());

        let (_, version) = graph.head_version().unwrap();
        assert_eq!(1, version);

        let builder2 = store.create_base_layer().unwrap();
        let layer2 = builder2.commit().unwrap();

        assert!(!graph.force_set_head_version(&layer2, 0).unwrap());
    }

    #[test]
    fn delete_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let _ = store.create("foo").unwrap();
        assert!(store.delete("foo").unwrap());
        assert!(store.open("foo").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_database() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        assert!(!store.delete("foo").unwrap());
    }

    #[test]
    fn delete_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();
        assert!(store.open("foo").unwrap().is_some());
        graph.delete().unwrap();
        assert!(store.open("foo").unwrap().is_none());
    }

    #[test]
    fn recreate_graph() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let graph = store.create("foo").unwrap();
        let builder = store.create_base_layer().unwrap();
        let layer = builder.commit().unwrap();
        graph.set_head(&layer).unwrap();
        assert!(graph.head().unwrap().is_some());
        graph.delete().unwrap();
        store.create("foo").unwrap();
        assert!(graph.head().unwrap().is_none());
    }

    #[test]
    fn list_databases() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        assert!(store.labels().unwrap().is_empty());
        let _ = store.create("foo").unwrap();
        let one = vec!["foo".to_string()];
        assert_eq!(store.labels().unwrap(), one);
        let _ = store.create("bar").unwrap();
        let two = vec!["bar".to_string(), "foo".to_string()];
        let mut left = store.labels().unwrap();
        left.sort();
        assert_eq!(left, two);
    }

    mod prop_tests {
        use super::*;
        use crate::storage::memory_persistence::MemoryPersistence;
        use proptest::collection::vec as prop_vec;
        use proptest::prelude::*;

        /// Strategy to generate a non-empty string suitable for triple components.
        /// Uses a restricted alphabet to keep generation fast and avoid empty strings.
        fn triple_component() -> impl Strategy<Value = String> {
            "[a-z][a-z0-9]{0,9}".prop_map(|s| s)
        }

        /// Strategy to generate a (subject, predicate, object) string triple.
        fn triple_strategy() -> impl Strategy<Value = (String, String, String)> {
            (triple_component(), triple_component(), triple_component())
        }

        // **Validates: Requirements 11.4, 13.1**
        //
        // Property 6: Commit Produces Retrievable Layer
        //
        // For any set of valid triples added to a LayerBuilder (base or child),
        // committing the builder SHALL return a LayerId such that `get_layer`
        // with that LayerId returns the committed layer.
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(20))]

            #[test]
            fn prop_commit_produces_retrievable_base_layer(
                triples in prop_vec(triple_strategy(), 1..10)
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer
                let builder = store.create_base_layer().unwrap();
                for (s, p, o) in &triples {
                    builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let layer = builder.commit().unwrap();
                let layer_id = layer.name();

                // Retrieve the layer by its ID
                let retrieved = store.get_layer_from_id(layer_id).unwrap();
                prop_assert!(retrieved.is_some(), "get_layer_from_id returned None for committed base layer");
                let retrieved = retrieved.unwrap();
                prop_assert_eq!(retrieved.name(), layer_id);
            }

            #[test]
            fn prop_commit_produces_retrievable_child_layer(
                base_triples in prop_vec(triple_strategy(), 1..5),
                child_triples in prop_vec(triple_strategy(), 1..5),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let base_layer = base_builder.commit().unwrap();

                // Build and commit a child layer
                let child_builder = base_layer.open_write().unwrap();
                for (s, p, o) in &child_triples {
                    child_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let child_layer = child_builder.commit().unwrap();
                let child_id = child_layer.name();

                // Retrieve the child layer by its ID
                let retrieved = store.get_layer_from_id(child_id).unwrap();
                prop_assert!(retrieved.is_some(), "get_layer_from_id returned None for committed child layer");
                let retrieved = retrieved.unwrap();
                prop_assert_eq!(retrieved.name(), child_id);
            }

            // **Validates: Requirement 13.3**
            //
            // Property 8: Base Layer Triple Existence
            //
            // For any set of triples added to a base layer builder and committed,
            // `value_triple_exists` SHALL return true for every added triple and
            // false for any triple not in the added set.
            #[test]
            fn prop_base_layer_triple_existence(
                triples in prop_vec(triple_strategy(), 1..10),
                absent_triples in prop_vec(triple_strategy(), 1..5),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer with the generated triples
                let builder = store.create_base_layer().unwrap();
                for (s, p, o) in &triples {
                    builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let layer = builder.commit().unwrap();

                // Collect the added triples into a set for membership checks
                let added_set: std::collections::HashSet<(String, String, String)> =
                    triples.iter().cloned().collect();

                // Every added triple must exist in the committed layer
                for (s, p, o) in &triples {
                    prop_assert!(
                        layer.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                        "Added triple ({}, {}, {}) not found in base layer",
                        s, p, o
                    );
                }

                // Triples not in the added set must not exist
                for (s, p, o) in &absent_triples {
                    if !added_set.contains(&(s.clone(), p.clone(), o.clone())) {
                        prop_assert!(
                            !layer.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                            "Triple ({}, {}, {}) should not exist in base layer",
                            s, p, o
                        );
                    }
                }
            }

            // **Validates: Requirement 13.4**
            //
            // Property 9: Delta Layer Correctness
            //
            // For any child layer C with parent P, and any triple T:
            // `C.value_triple_exists(T)` SHALL be true if and only if T is in C's
            // additions, or T exists in P and T is not in C's removals.
            #[test]
            fn prop_delta_layer_correctness(
                parent_triples in prop_vec(triple_strategy(), 2..8),
                child_additions in prop_vec(triple_strategy(), 1..5),
                removal_indices in prop_vec(0..100usize, 0..3),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit the parent (base) layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &parent_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let parent_layer = base_builder.commit().unwrap();

                // Determine which parent triples to remove in the child
                let parent_set: Vec<(String, String, String)> =
                    parent_triples.iter().cloned().collect();
                let removals: std::collections::HashSet<(String, String, String)> =
                    removal_indices
                        .iter()
                        .filter_map(|&i| parent_set.get(i % parent_set.len()).cloned())
                        .collect();

                // Build and commit the child (delta) layer
                let child_builder = parent_layer.open_write().unwrap();
                for (s, p, o) in &child_additions {
                    child_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                for (s, p, o) in &removals {
                    child_builder
                        .remove_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let child_layer = child_builder.commit().unwrap();

                // Compute expected visible set: parent triples minus removals, plus additions
                let parent_visible: std::collections::HashSet<(String, String, String)> =
                    parent_set.iter().cloned().collect();
                let addition_set: std::collections::HashSet<(String, String, String)> =
                    child_additions.iter().cloned().collect();
                let expected: std::collections::HashSet<(String, String, String)> =
                    parent_visible
                        .difference(&removals)
                        .cloned()
                        .chain(addition_set.iter().cloned())
                        .collect();

                // Verify: every expected triple exists
                for (s, p, o) in &expected {
                    prop_assert!(
                        child_layer.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                        "Expected triple ({}, {}, {}) not found in child layer",
                        s, p, o
                    );
                }

                // Verify: removed parent triples that were NOT re-added should be absent
                for (s, p, o) in &removals {
                    if !addition_set.contains(&(s.clone(), p.clone(), o.clone())) {
                        prop_assert!(
                            !child_layer.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                            "Removed triple ({}, {}, {}) should not exist in child layer",
                            s, p, o
                        );
                    }
                }
            }

            // **Validates: Requirements 14.1, 14.2, 14.3**
            //
            // Property 10: Rollup Equivalence
            //
            // For any layer L at the end of a delta chain, `rollup(L)` SHALL
            // produce a new base layer R such that: (a) for all triples T,
            // `L.triple_exists(T) ⟺ R.triple_exists(T)`, (b) R has no parent
            // (via the rollup mechanism), and (c) the original layer L and its
            // delta chain are unmodified.
            #[test]
            fn prop_rollup_equivalence(
                base_triples in prop_vec(triple_strategy(), 1..8),
                delta_layers in prop_vec(
                    (prop_vec(triple_strategy(), 0..5), prop_vec(0..100usize, 0..3)),
                    1..4
                ),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let mut current_layer = base_builder.commit().unwrap();

                // Build a chain of delta layers
                // Track all triples that have been added so far for removal candidates
                let mut all_added: Vec<(String, String, String)> = base_triples.clone();

                for (additions, removal_indices) in &delta_layers {
                    let child_builder = current_layer.open_write().unwrap();

                    // Add new triples
                    for (s, p, o) in additions {
                        child_builder
                            .add_value_triple(ValueTriple::new_string_value(s, p, o))
                            .unwrap();
                    }

                    // Remove some existing triples (using indices into all_added)
                    if !all_added.is_empty() {
                        let mut removed = std::collections::HashSet::new();
                        for &idx in removal_indices {
                            let triple = &all_added[idx % all_added.len()];
                            if removed.insert(triple.clone()) {
                                child_builder
                                    .remove_value_triple(ValueTriple::new_string_value(
                                        &triple.0, &triple.1, &triple.2,
                                    ))
                                    .unwrap();
                            }
                        }
                    }

                    current_layer = child_builder.commit().unwrap();
                    all_added.extend(additions.iter().cloned());
                }

                let final_layer_name = current_layer.name();

                // Collect all triples from the original (pre-rollup) layer
                let original_triples: std::collections::HashSet<_> = current_layer
                    .triples()
                    .filter_map(|t| current_layer.id_triple_to_string(&t))
                    .collect();

                // Perform rollup
                current_layer.rollup().unwrap();

                // Re-fetch the layer after rollup
                let rolled_layer = store.get_layer_from_id(final_layer_name).unwrap().unwrap();

                // Collect all triples from the rolled-up layer
                let rolled_triples: std::collections::HashSet<_> = rolled_layer
                    .triples()
                    .filter_map(|t| rolled_layer.id_triple_to_string(&t))
                    .collect();

                // (a) All triples must match between original and rolled-up layer
                prop_assert_eq!(
                    &original_triples,
                    &rolled_triples,
                    "Triples differ after rollup"
                );

                // (b) Verify every original triple exists in rolled-up layer via value_triple_exists
                for vt in &original_triples {
                    prop_assert!(
                        rolled_layer.value_triple_exists(vt),
                        "Original triple {:?} not found in rolled-up layer",
                        vt
                    );
                }

                // (c) Verify every rolled-up triple exists in original layer
                for vt in &rolled_triples {
                    prop_assert!(
                        current_layer.value_triple_exists(vt),
                        "Rolled-up triple {:?} not found in original layer",
                        vt
                    );
                }
            }

            // **Validates: Requirement 14.4**
            //
            // Property 11: Rollup-Upto Correctness
            //
            // For any layer L and any ancestor A in L's delta chain,
            // `rollup_upto(L, A)` SHALL produce a layer that contains the same
            // triples as L.
            #[test]
            fn prop_rollup_upto_correctness(
                base_triples in prop_vec(triple_strategy(), 1..6),
                mid_additions in prop_vec(triple_strategy(), 1..5),
                top_additions in prop_vec(triple_strategy(), 1..5),
                mid_removal_indices in prop_vec(0..100usize, 0..2),
                top_removal_indices in prop_vec(0..100usize, 0..2),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build base layer (this will be the "upto" ancestor)
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let base_layer = base_builder.commit().unwrap();

                // Build middle delta layer
                let mid_builder = base_layer.open_write().unwrap();
                for (s, p, o) in &mid_additions {
                    mid_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                // Remove some base triples
                if !base_triples.is_empty() {
                    let mut removed = std::collections::HashSet::new();
                    for &idx in &mid_removal_indices {
                        let triple = &base_triples[idx % base_triples.len()];
                        if removed.insert(triple.clone()) {
                            mid_builder
                                .remove_value_triple(ValueTriple::new_string_value(
                                    &triple.0, &triple.1, &triple.2,
                                ))
                                .unwrap();
                        }
                    }
                }
                let mid_layer = mid_builder.commit().unwrap();

                // Build top delta layer
                let top_builder = mid_layer.open_write().unwrap();
                for (s, p, o) in &top_additions {
                    top_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                // Remove some mid-layer triples
                let all_so_far: Vec<_> = base_triples.iter().chain(mid_additions.iter()).cloned().collect();
                if !all_so_far.is_empty() {
                    let mut removed = std::collections::HashSet::new();
                    for &idx in &top_removal_indices {
                        let triple = &all_so_far[idx % all_so_far.len()];
                        if removed.insert(triple.clone()) {
                            top_builder
                                .remove_value_triple(ValueTriple::new_string_value(
                                    &triple.0, &triple.1, &triple.2,
                                ))
                                .unwrap();
                        }
                    }
                }
                let top_layer = top_builder.commit().unwrap();
                let top_name = top_layer.name();

                // Collect all triples from the original top layer before rollup_upto
                let original_triples: std::collections::HashSet<_> = top_layer
                    .triples()
                    .filter_map(|t| top_layer.id_triple_to_string(&t))
                    .collect();

                // Perform rollup_upto the base layer
                top_layer.rollup_upto(&base_layer).unwrap();

                // Re-fetch the layer after rollup_upto
                let rolled_layer = store.get_layer_from_id(top_name).unwrap().unwrap();

                // Collect all triples from the rolled-up layer
                let rolled_triples: std::collections::HashSet<_> = rolled_layer
                    .triples()
                    .filter_map(|t| rolled_layer.id_triple_to_string(&t))
                    .collect();

                // All triples must match
                prop_assert_eq!(
                    &original_triples,
                    &rolled_triples,
                    "Triples differ after rollup_upto"
                );

                // Verify every original triple exists in rolled-up layer
                for vt in &original_triples {
                    prop_assert!(
                        rolled_layer.value_triple_exists(vt),
                        "Original triple {:?} not found after rollup_upto",
                        vt
                    );
                }
            }

            // **Validates: Requirements 15.1, 15.2**
            //
            // Property 12: Squash Equivalence
            //
            // For any child layer C, `squash(C)` SHALL produce a new layer S
            // such that for all triples T, `C.triple_exists(T) ⟺ S.triple_exists(T)`.
            #[test]
            fn prop_squash_equivalence(
                parent_triples in prop_vec(triple_strategy(), 1..8),
                child_additions in prop_vec(triple_strategy(), 1..5),
                removal_indices in prop_vec(0..100usize, 0..3),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a parent (base) layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &parent_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let parent_layer = base_builder.commit().unwrap();

                // Build and commit a child (delta) layer with additions and removals
                let child_builder = parent_layer.open_write().unwrap();
                for (s, p, o) in &child_additions {
                    child_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                // Remove some parent triples
                if !parent_triples.is_empty() {
                    let mut removed = std::collections::HashSet::new();
                    for &idx in &removal_indices {
                        let triple = &parent_triples[idx % parent_triples.len()];
                        if removed.insert(triple.clone()) {
                            child_builder
                                .remove_value_triple(ValueTriple::new_string_value(
                                    &triple.0, &triple.1, &triple.2,
                                ))
                                .unwrap();
                        }
                    }
                }
                let child_layer = child_builder.commit().unwrap();

                // Collect all triples from the original child layer before squash
                let original_triples: std::collections::HashSet<_> = child_layer
                    .triples()
                    .filter_map(|t| child_layer.id_triple_to_string(&t))
                    .collect();

                // Perform squash
                let squashed_layer = child_layer.squash().unwrap();

                // Collect all triples from the squashed layer
                let squashed_triples: std::collections::HashSet<_> = squashed_layer
                    .triples()
                    .filter_map(|t| squashed_layer.id_triple_to_string(&t))
                    .collect();

                // (a) Triple sets must be identical
                prop_assert_eq!(
                    &original_triples,
                    &squashed_triples,
                    "Triples differ after squash"
                );

                // (b) Every original triple exists in squashed layer
                for vt in &original_triples {
                    prop_assert!(
                        squashed_layer.value_triple_exists(vt),
                        "Original triple {:?} not found in squashed layer",
                        vt
                    );
                }

                // (c) Every squashed triple exists in original child layer
                for vt in &squashed_triples {
                    prop_assert!(
                        child_layer.value_triple_exists(vt),
                        "Squashed triple {:?} not found in original child layer",
                        vt
                    );
                }
            }

            // **Validates: Requirement 16.1**
            //
            // Property 13: Merge Union of Additions
            //
            // For any two divergent branches from a common ancestor, merging
            // them SHALL produce a layer whose additions are the union of all
            // additions from all input branches.
            #[test]
            fn prop_merge_union_of_additions(
                base_triples in prop_vec(triple_strategy(), 1..6),
                branch_a_additions in prop_vec(triple_strategy(), 1..5),
                branch_b_additions in prop_vec(triple_strategy(), 1..5),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a common ancestor (base) layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let merge_base = base_builder.commit().unwrap();

                // Branch A: add some triples
                let branch_a_builder = merge_base.open_write().unwrap();
                for (s, p, o) in &branch_a_additions {
                    branch_a_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let branch_a = branch_a_builder.commit().unwrap();

                // Branch B: add some triples
                let branch_b_builder = merge_base.open_write().unwrap();
                for (s, p, o) in &branch_b_additions {
                    branch_b_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let branch_b = branch_b_builder.commit().unwrap();

                // Merge on top of branch_a
                let merge_builder = branch_a.open_write().unwrap();
                merge_builder
                    .apply_merge(vec![&branch_a, &branch_b], Some(&merge_base))
                    .unwrap();
                let merged = merge_builder.commit().unwrap();

                // Compute expected: base triples + branch_a additions + branch_b additions
                let base_set: std::collections::HashSet<(String, String, String)> =
                    base_triples.iter().cloned().collect();
                let a_set: std::collections::HashSet<(String, String, String)> =
                    branch_a_additions.iter().cloned().collect();
                let b_set: std::collections::HashSet<(String, String, String)> =
                    branch_b_additions.iter().cloned().collect();
                let expected: std::collections::HashSet<(String, String, String)> =
                    base_set.iter().chain(a_set.iter()).chain(b_set.iter()).cloned().collect();

                // Every expected triple must exist in the merged layer
                for (s, p, o) in &expected {
                    prop_assert!(
                        merged.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                        "Expected triple ({}, {}, {}) not found in merged layer",
                        s, p, o
                    );
                }

                // Every triple in the merged layer should be in the expected set
                let merged_triples: std::collections::HashSet<_> = merged
                    .triples()
                    .filter_map(|t| merged.id_triple_to_string(&t))
                    .collect();
                for vt in &merged_triples {
                    let tuple = (vt.subject.clone(), vt.predicate.clone(), vt.object.clone());
                    // Check that the triple is one we expect: either from base, branch_a, or branch_b
                    let is_expected = base_set.iter().chain(a_set.iter()).chain(b_set.iter())
                        .any(|(s, p, o)| {
                            *vt == ValueTriple::new_string_value(s, p, o)
                        });
                    prop_assert!(
                        is_expected,
                        "Unexpected triple {:?} found in merged layer",
                        tuple
                    );
                }
            }

            // **Validates: Requirement 16.2**
            //
            // Property 14: Merge Removal Wins
            //
            // For any merge of branches where at least one branch removes a
            // triple that exists in the common ancestor, the merged layer SHALL
            // not contain that triple.
            #[test]
            fn prop_merge_removal_wins(
                base_triples in prop_vec(triple_strategy(), 2..8),
                branch_a_additions in prop_vec(triple_strategy(), 0..4),
                removal_indices in prop_vec(0..100usize, 1..3),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a common ancestor (base) layer
                let base_builder = store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let merge_base = base_builder.commit().unwrap();

                // Branch A: add some triples (no removals)
                let branch_a_builder = merge_base.open_write().unwrap();
                for (s, p, o) in &branch_a_additions {
                    branch_a_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let branch_a = branch_a_builder.commit().unwrap();

                // Branch B: remove some base triples
                let branch_b_builder = merge_base.open_write().unwrap();
                let mut removals = std::collections::HashSet::new();
                for &idx in &removal_indices {
                    let triple = &base_triples[idx % base_triples.len()];
                    if removals.insert(triple.clone()) {
                        branch_b_builder
                            .remove_value_triple(ValueTriple::new_string_value(
                                &triple.0, &triple.1, &triple.2,
                            ))
                            .unwrap();
                    }
                }
                let branch_b = branch_b_builder.commit().unwrap();

                // Merge on top of branch_a
                let merge_builder = branch_a.open_write().unwrap();
                merge_builder
                    .apply_merge(vec![&branch_a, &branch_b], Some(&merge_base))
                    .unwrap();
                let merged = merge_builder.commit().unwrap();

                // Removed triples (that were NOT re-added by branch_a) must be absent
                let a_set: std::collections::HashSet<(String, String, String)> =
                    branch_a_additions.iter().cloned().collect();
                for (s, p, o) in &removals {
                    if !a_set.contains(&(s.clone(), p.clone(), o.clone())) {
                        prop_assert!(
                            !merged.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                            "Removed triple ({}, {}, {}) should not exist in merged layer",
                            s, p, o
                        );
                    }
                }

                // Non-removed base triples must still be present
                let base_set: std::collections::HashSet<(String, String, String)> =
                    base_triples.iter().cloned().collect();
                for (s, p, o) in &base_set {
                    if !removals.contains(&(s.clone(), p.clone(), o.clone())) {
                        prop_assert!(
                            merged.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                            "Non-removed base triple ({}, {}, {}) should exist in merged layer",
                            s, p, o
                        );
                    }
                }

                // Branch A additions must be present
                for (s, p, o) in &branch_a_additions {
                    prop_assert!(
                        merged.value_triple_exists(&ValueTriple::new_string_value(s, p, o)),
                        "Branch A addition ({}, {}, {}) should exist in merged layer",
                        s, p, o
                    );
                }
            }

            // **Validates: Requirements 17.1, 17.2, 17.4**
            //
            // Property 15: Export/Import Round-Trip
            //
            // For any set of layers in a store, exporting them to a tar+gzip
            // pack and importing the pack into a fresh store SHALL produce
            // layers with identical triple query results to the originals.
            #[test]
            fn prop_export_import_round_trip(
                base_triples in prop_vec(triple_strategy(), 1..8),
                child_additions in prop_vec(triple_strategy(), 1..5),
                removal_indices in prop_vec(0..100usize, 0..3),
            ) {
                let source_store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer
                let base_builder = source_store.create_base_layer().unwrap();
                for (s, p, o) in &base_triples {
                    base_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let base_layer = base_builder.commit().unwrap();
                let base_id = base_layer.name();

                // Build and commit a child layer with additions and removals
                let child_builder = base_layer.open_write().unwrap();
                for (s, p, o) in &child_additions {
                    child_builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                if !base_triples.is_empty() {
                    let mut removed = std::collections::HashSet::new();
                    for &idx in &removal_indices {
                        let triple = &base_triples[idx % base_triples.len()];
                        if removed.insert(triple.clone()) {
                            child_builder
                                .remove_value_triple(ValueTriple::new_string_value(
                                    &triple.0, &triple.1, &triple.2,
                                ))
                                .unwrap();
                        }
                    }
                }
                let child_layer = child_builder.commit().unwrap();
                let child_id = child_layer.name();

                // Collect triples from both layers before export
                let base_triples_set: std::collections::HashSet<_> = base_layer
                    .triples()
                    .filter_map(|t| base_layer.id_triple_to_string(&t))
                    .collect();
                let child_triples_set: std::collections::HashSet<_> = child_layer
                    .triples()
                    .filter_map(|t| child_layer.id_triple_to_string(&t))
                    .collect();

                // Export both layers
                let layer_ids: Vec<[u32; 5]> = vec![base_id, child_id];
                let pack = source_store
                    .export_layers(Box::new(layer_ids.clone().into_iter()))
                    .unwrap();

                // Import into a fresh store
                let target_store = open_persistence_store(MemoryPersistence::new());
                target_store
                    .import_layers(&pack, Box::new(layer_ids.clone().into_iter()))
                    .unwrap();

                // Retrieve imported layers and verify identical triples
                let imported_base = target_store.get_layer_from_id(base_id).unwrap();
                prop_assert!(imported_base.is_some(), "Imported base layer not found");
                let imported_base = imported_base.unwrap();

                let imported_base_triples: std::collections::HashSet<_> = imported_base
                    .triples()
                    .filter_map(|t| imported_base.id_triple_to_string(&t))
                    .collect();
                prop_assert_eq!(
                    &base_triples_set,
                    &imported_base_triples,
                    "Base layer triples differ after export/import"
                );

                let imported_child = target_store.get_layer_from_id(child_id).unwrap();
                prop_assert!(imported_child.is_some(), "Imported child layer not found");
                let imported_child = imported_child.unwrap();

                let imported_child_triples: std::collections::HashSet<_> = imported_child
                    .triples()
                    .filter_map(|t| imported_child.id_triple_to_string(&t))
                    .collect();
                prop_assert_eq!(
                    &child_triples_set,
                    &imported_child_triples,
                    "Child layer triples differ after export/import"
                );

                // Also verify via value_triple_exists
                for vt in &base_triples_set {
                    prop_assert!(
                        imported_base.value_triple_exists(vt),
                        "Base triple {:?} not found in imported store",
                        vt
                    );
                }
                for vt in &child_triples_set {
                    prop_assert!(
                        imported_child.value_triple_exists(vt),
                        "Child triple {:?} not found in imported store",
                        vt
                    );
                }
            }

            // **Validates: Requirement 17.3**
            //
            // Property 16: Import Idempotence
            //
            // For any valid pack, importing it into a store that already
            // contains the same layers SHALL leave the existing layers
            // unchanged (no overwrite).
            #[test]
            fn prop_import_idempotence(
                triples in prop_vec(triple_strategy(), 1..8),
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer
                let builder = store.create_base_layer().unwrap();
                for (s, p, o) in &triples {
                    builder
                        .add_value_triple(ValueTriple::new_string_value(s, p, o))
                        .unwrap();
                }
                let layer = builder.commit().unwrap();
                let layer_id = layer.name();

                // Collect original triples
                let original_triples: std::collections::HashSet<_> = layer
                    .triples()
                    .filter_map(|t| layer.id_triple_to_string(&t))
                    .collect();

                // Export the layer
                let layer_ids: Vec<[u32; 5]> = vec![layer_id];
                let pack = store
                    .export_layers(Box::new(layer_ids.clone().into_iter()))
                    .unwrap();

                // Import the pack twice into the same store
                store
                    .import_layers(&pack, Box::new(layer_ids.clone().into_iter()))
                    .unwrap();
                store
                    .import_layers(&pack, Box::new(layer_ids.clone().into_iter()))
                    .unwrap();

                // Verify the layer is still retrievable with identical triples
                let retrieved = store.get_layer_from_id(layer_id).unwrap();
                prop_assert!(retrieved.is_some(), "Layer not found after double import");
                let retrieved = retrieved.unwrap();

                let retrieved_triples: std::collections::HashSet<_> = retrieved
                    .triples()
                    .filter_map(|t| retrieved.id_triple_to_string(&t))
                    .collect();
                prop_assert_eq!(
                    &original_triples,
                    &retrieved_triples,
                    "Triples changed after importing pack twice"
                );

                // Verify via value_triple_exists
                for vt in &original_triples {
                    prop_assert!(
                        retrieved.value_triple_exists(vt),
                        "Triple {:?} not found after double import",
                        vt
                    );
                }
            }

            // **Validates: Requirements 18.1, 18.2**
            //
            // Property 17: Label Create/Get Round-Trip
            //
            // For any valid label name, creating a label and then getting it
            // SHALL return a label with the same name, no associated layer
            // (None), and version 0.
            #[test]
            fn prop_label_create_get_round_trip(
                label_name in "[a-z][a-z0-9]{0,9}"
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Create a label via the Store API
                let named_graph = store.create(&label_name).unwrap();
                prop_assert_eq!(named_graph.name(), label_name.as_str());

                // Get the label back via open
                let opened = store.open(&label_name).unwrap();
                prop_assert!(opened.is_some(), "open returned None for just-created label");
                let opened = opened.unwrap();
                prop_assert_eq!(opened.name(), label_name.as_str());

                // Verify head is None (no layer) and version is 0
                let (head_layer, version) = opened.head_version().unwrap();
                prop_assert!(head_layer.is_none(), "Newly created label should have no layer");
                prop_assert_eq!(version, 0u64, "Newly created label should have version 0");
            }

            // **Validates: Requirement 18.6**
            //
            // Property 18: Label Delete
            //
            // For any valid label name, creating a label and then deleting it
            // SHALL return true from delete, and subsequent get_label SHALL
            // return None.
            #[test]
            fn prop_label_delete(
                label_name in "[a-z][a-z0-9]{0,9}"
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Create a label
                store.create(&label_name).unwrap();

                // Delete it — should return true
                let deleted = store.delete(&label_name).unwrap();
                prop_assert!(deleted, "delete should return true for existing label");

                // Open should now return None
                let opened = store.open(&label_name).unwrap();
                prop_assert!(opened.is_none(), "open should return None after deletion");
            }

            // **Validates: Requirements 21.1, 21.2**
            //
            // Property 21: Dictionary String Round-Trip
            //
            // For any string (including Unicode), adding it to a layer's
            // dictionary during construction, committing the layer, and then
            // looking up the assigned numeric ID SHALL return the original
            // string exactly.
            #[test]
            fn prop_dictionary_string_round_trip(
                subject in "\\PC{1,20}",
                predicate in "\\PC{1,20}",
                object in "\\PC{1,20}",
            ) {
                let store = open_persistence_store(MemoryPersistence::new());

                // Build and commit a base layer with a single triple
                let builder = store.create_base_layer().unwrap();
                builder
                    .add_value_triple(ValueTriple::new_string_value(&subject, &predicate, &object))
                    .unwrap();
                let layer = builder.commit().unwrap();

                // Look up subject by string -> ID -> string
                let subj_id = layer.subject_id(&subject);
                prop_assert!(subj_id.is_some(), "subject_id returned None for {:?}", subject);
                let subj_id = subj_id.unwrap();
                let subj_back = layer.id_subject(subj_id);
                prop_assert!(subj_back.is_some(), "id_subject returned None for id {}", subj_id);
                prop_assert_eq!(&subj_back.unwrap(), &subject, "subject round-trip mismatch");

                // Look up predicate by string -> ID -> string
                let pred_id = layer.predicate_id(&predicate);
                prop_assert!(pred_id.is_some(), "predicate_id returned None for {:?}", predicate);
                let pred_id = pred_id.unwrap();
                let pred_back = layer.id_predicate(pred_id);
                prop_assert!(pred_back.is_some(), "id_predicate returned None for id {}", pred_id);
                prop_assert_eq!(&pred_back.unwrap(), &predicate, "predicate round-trip mismatch");

                // Look up object (string value) by string -> ID -> string
                // Objects use the value dictionary; verify via id_triple_to_string
                let triples: Vec<_> = layer.triples().collect();
                prop_assert_eq!(triples.len(), 1, "Expected exactly 1 triple in layer");
                let vt = layer.id_triple_to_string(&triples[0]);
                prop_assert!(vt.is_some(), "id_triple_to_string returned None");
                let vt = vt.unwrap();
                prop_assert_eq!(&vt.subject, &subject, "triple subject mismatch");
                prop_assert_eq!(&vt.predicate, &predicate, "triple predicate mismatch");
                // Verify the full triple round-trips via value_triple_exists
                prop_assert!(
                    layer.value_triple_exists(&ValueTriple::new_string_value(&subject, &predicate, &object)),
                    "value_triple_exists returned false for the added triple"
                );
            }
        }
    }
}
