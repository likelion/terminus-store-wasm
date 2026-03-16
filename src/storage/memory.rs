//! In-memory implementation of storage traits.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use super::file::*;
use super::label::*;
use super::layer::*;

pub use tdb_succinct_wasm::storage::memory::*;

#[derive(Clone, Default)]
pub struct MemoryLayerStore {
    layers: Arc<RwLock<HashMap<[u32; 5], HashMap<String, MemoryBackedStore>>>>,
}

impl MemoryLayerStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PersistentLayerStore for MemoryLayerStore {
    type File = MemoryBackedStore;

    fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let guard = self.layers.read().unwrap();
        Ok(guard.keys().cloned().collect())
    }

    fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        let mut guard = self.layers.write().unwrap();
        guard.insert(name, HashMap::new());

        Ok(name)
    }

    fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        let guard = self.layers.read().unwrap();
        Ok(guard.contains_key(&name))
    }

    fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let guard = self.layers.read().unwrap();
        if let Some(files) = guard.get(&directory) {
            if let Some(file) = files.get(file) {
                file.exists()
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let guard = self.layers.read().unwrap();
        if let Some(files) = guard.get(&directory) {
            if let Some(file) = files.get(name) {
                Ok(file.clone())
            } else {
                std::mem::drop(guard); // release read lock cause it is time to write
                let mut guard = self.layers.write().unwrap();
                let files = guard.get_mut(&directory).unwrap();
                let file = MemoryBackedStore::new();
                let result = file.clone();
                files.insert(name.to_string(), file);
                Ok(result)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }
}

#[derive(Clone, Default)]
pub struct MemoryLabelStore {
    labels: Arc<RwLock<HashMap<String, Label>>>,
}

impl MemoryLabelStore {
    pub fn new() -> MemoryLabelStore {
        Default::default()
    }
}

impl LabelStore for MemoryLabelStore {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().unwrap();
        Ok(labels.values().cloned().collect())
    }

    fn create_label(&self, name: &str) -> io::Result<Label> {
        let label = Label::new_empty(name);

        let mut labels = self.labels.write().unwrap();
        if labels.get(&label.name).is_some() {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "label already exists",
            ))
        } else {
            labels.insert(label.name.clone(), label.clone());
            Ok(label)
        }
    }

    fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        let name = name.to_owned();
        let labels = self.labels.read().unwrap();
        Ok(labels.get(&name).cloned())
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);

        let mut labels = self.labels.write().unwrap();

        match labels.get(&new_label.name) {
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "label does not exist",
            )),
            Some(old_label) => {
                if old_label.version + 1 != new_label.version {
                    Ok(None)
                } else {
                    labels.insert(new_label.name.clone(), new_label.clone());

                    Ok(Some(new_label))
                }
            }
        }
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut labels = self.labels.write().unwrap();

        Ok(labels.remove(name).is_some())
    }
}

#[cfg(test)]
pub fn base_layer_memory_files() -> BaseLayerFiles<MemoryBackedStore> {
    BaseLayerFiles {
        node_dictionary_files: DictionaryFiles {
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },
        predicate_dictionary_files: DictionaryFiles {
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },
        value_dictionary_files: TypedDictionaryFiles {
            types_present_file: MemoryBackedStore::new(),
            type_offsets_file: MemoryBackedStore::new(),
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },

        id_map_files: IdMapFiles {
            node_value_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            predicate_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
        },

        subjects_file: MemoryBackedStore::new(),
        objects_file: MemoryBackedStore::new(),

        s_p_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        sp_o_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        o_ps_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        predicate_wavelet_tree_files: BitIndexFiles {
            bits_file: MemoryBackedStore::new(),
            blocks_file: MemoryBackedStore::new(),
            sblocks_file: MemoryBackedStore::new(),
        },
    }
}

#[cfg(test)]
pub fn child_layer_memory_files() -> ChildLayerFiles<MemoryBackedStore> {
    ChildLayerFiles {
        node_dictionary_files: DictionaryFiles {
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },
        predicate_dictionary_files: DictionaryFiles {
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },
        value_dictionary_files: TypedDictionaryFiles {
            types_present_file: MemoryBackedStore::new(),
            type_offsets_file: MemoryBackedStore::new(),
            blocks_file: MemoryBackedStore::new(),
            offsets_file: MemoryBackedStore::new(),
        },

        id_map_files: IdMapFiles {
            node_value_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            predicate_idmap_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
        },

        pos_subjects_file: MemoryBackedStore::new(),
        pos_objects_file: MemoryBackedStore::new(),
        neg_subjects_file: MemoryBackedStore::new(),
        neg_objects_file: MemoryBackedStore::new(),

        pos_s_p_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        pos_sp_o_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        pos_o_ps_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        neg_s_p_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        neg_sp_o_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        neg_o_ps_adjacency_list_files: AdjacencyListFiles {
            bitindex_files: BitIndexFiles {
                bits_file: MemoryBackedStore::new(),
                blocks_file: MemoryBackedStore::new(),
                sblocks_file: MemoryBackedStore::new(),
            },
            nums_file: MemoryBackedStore::new(),
        },
        pos_predicate_wavelet_tree_files: BitIndexFiles {
            bits_file: MemoryBackedStore::new(),
            blocks_file: MemoryBackedStore::new(),
            sblocks_file: MemoryBackedStore::new(),
        },
        neg_predicate_wavelet_tree_files: BitIndexFiles {
            bits_file: MemoryBackedStore::new(),
            blocks_file: MemoryBackedStore::new(),
            sblocks_file: MemoryBackedStore::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use std::io::{Read, Write};

    #[test]
    fn write_and_read_memory_backed() {
        let file = MemoryBackedStore::new();

        let mut w = file.open_write().unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        w.sync_all().unwrap();
        let mut buf = Vec::new();
        file.open_read().unwrap().read_to_end(&mut buf).unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[test]
    fn write_and_map_memory_backed() {
        let file = MemoryBackedStore::new();

        let mut w = file.open_write().unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        w.sync_all().unwrap();
        let map = file.map().unwrap();

        assert_eq!(vec![1, 2, 3], map.as_ref());
    }

    #[test]
    fn create_layers_from_memory_store() {
        let store = MemoryLayerStore::new();
        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().unwrap();
        builder = store.create_child_layer(base_name).unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().unwrap();
        let layer = store.get_layer(child_name).unwrap().unwrap();

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[test]
    fn memory_create_and_retrieve_equal_label() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").unwrap();
        assert_eq!(foo, store.get_label("foo").unwrap().unwrap());
    }

    #[test]
    fn memory_update_label_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").unwrap();

        assert_eq!(
            1,
            store
                .set_label(&foo, [6, 7, 8, 9, 10])
                .unwrap()
                .unwrap()
                .version
        );

        assert_eq!(1, store.get_label("foo").unwrap().unwrap().version);
    }

    #[test]
    fn memory_update_label_twice_from_same_label_object_fails() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").unwrap();

        assert!(store.set_label(&foo, [6, 7, 8, 9, 10]).unwrap().is_some());
        assert!(store.set_label(&foo, [1, 1, 1, 1, 1]).unwrap().is_none());
    }

    #[test]
    fn memory_update_label_twice_from_updated_label_object_succeeds() {
        let store = MemoryLabelStore::new();
        let foo = store.create_label("foo").unwrap();

        let foo2 = store.set_label(&foo, [6, 7, 8, 9, 10]).unwrap().unwrap();
        assert!(store.set_label(&foo2, [1, 1, 1, 1, 1]).unwrap().is_some());
    }

    #[test]
    fn create_and_delete_label() {
        let store = MemoryLabelStore::new();

        store.create_label("foo").unwrap();
        assert!(store.get_label("foo").unwrap().is_some());
        assert!(store.delete_label("foo").unwrap());
        assert!(store.get_label("foo").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_label() {
        let store = MemoryLabelStore::new();

        assert!(!store.delete_label("foo").unwrap());
    }
}
