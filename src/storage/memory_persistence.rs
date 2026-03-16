use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use super::label::Label;
use super::persistence::{LabelPersistence, LayerId, LayerPersistence};

/// In-memory persistence backend.
/// Available on all targets (no #[cfg] gate).
#[derive(Clone)]
pub struct MemoryPersistence {
    layers: Arc<RwLock<HashMap<LayerId, HashMap<String, Bytes>>>>,
    labels: Arc<RwLock<HashMap<String, Label>>>,
}

impl MemoryPersistence {
    pub fn new() -> Self {
        Self {
            layers: Arc::new(RwLock::new(HashMap::new())),
            labels: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryPersistence {
    fn default() -> Self {
        Self::new()
    }
}

impl LayerPersistence for MemoryPersistence {
    fn layer_exists(&self, id: LayerId) -> io::Result<bool> {
        let layers = self.layers.read().unwrap();
        Ok(layers.contains_key(&id))
    }

    fn create_layer_dir(&self, id: LayerId) -> io::Result<()> {
        let mut layers = self.layers.write().unwrap();
        if layers.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("layer {:?} already exists", id),
            ));
        }
        layers.insert(id, HashMap::new());
        Ok(())
    }

    fn file_exists(&self, layer: LayerId, file: &str) -> io::Result<bool> {
        let layers = self.layers.read().unwrap();
        Ok(layers
            .get(&layer)
            .map(|files| files.contains_key(file))
            .unwrap_or(false))
    }

    fn read_file(&self, layer: LayerId, file: &str) -> io::Result<Bytes> {
        let layers = self.layers.read().unwrap();
        let files = layers.get(&layer).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("layer {:?} not found", layer),
            )
        })?;
        files.get(file).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("file {:?} not found in layer {:?}", file, layer),
            )
        })
    }

    fn write_file(&self, layer: LayerId, file: &str, data: &[u8]) -> io::Result<()> {
        let mut layers = self.layers.write().unwrap();
        let files = layers.get_mut(&layer).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("layer {:?} not found", layer),
            )
        })?;
        files.insert(file.to_string(), Bytes::copy_from_slice(data));
        Ok(())
    }

    fn list_layers(&self) -> io::Result<Vec<LayerId>> {
        let layers = self.layers.read().unwrap();
        Ok(layers.keys().cloned().collect())
    }

    fn delete_layer(&self, id: LayerId) -> io::Result<()> {
        let mut layers = self.layers.write().unwrap();
        layers.remove(&id);
        Ok(())
    }
}

impl LabelPersistence for MemoryPersistence {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().unwrap();
        Ok(labels.values().cloned().collect())
    }

    fn create_label(&self, name: &str) -> io::Result<Label> {
        let mut labels = self.labels.write().unwrap();
        if labels.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("label {:?} already exists", name),
            ));
        }
        let label = Label {
            name: name.to_string(),
            layer: None,
            version: 0,
        };
        labels.insert(name.to_string(), label.clone());
        Ok(label)
    }

    fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        let labels = self.labels.read().unwrap();
        Ok(labels.get(name).cloned())
    }

    fn set_label(&self, label: &Label, layer: Option<LayerId>) -> io::Result<Option<Label>> {
        let mut labels = self.labels.write().unwrap();
        let stored = labels.get(&label.name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("label {:?} not found", label.name),
            )
        })?;

        if stored.version != label.version {
            return Ok(None);
        }

        let updated = Label {
            name: label.name.clone(),
            layer,
            version: label.version + 1,
        };
        labels.insert(label.name.clone(), updated.clone());
        Ok(Some(updated))
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut labels = self.labels.write().unwrap();
        Ok(labels.remove(name).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_check_layer() {
        let p = MemoryPersistence::new();
        let id: LayerId = [1, 2, 3, 4, 5];

        assert!(!p.layer_exists(id).unwrap());
        p.create_layer_dir(id).unwrap();
        assert!(p.layer_exists(id).unwrap());
    }

    #[test]
    fn create_duplicate_layer_errors() {
        let p = MemoryPersistence::new();
        let id: LayerId = [1, 2, 3, 4, 5];

        p.create_layer_dir(id).unwrap();
        let err = p.create_layer_dir(id).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn write_and_read_file() {
        let p = MemoryPersistence::new();
        let id: LayerId = [10, 20, 30, 40, 50];
        let data = b"hello world";

        p.create_layer_dir(id).unwrap();
        p.write_file(id, "test.bin", data).unwrap();

        let read_back = p.read_file(id, "test.bin").unwrap();
        assert_eq!(&read_back[..], data);
    }

    #[test]
    fn read_nonexistent_layer_errors() {
        let p = MemoryPersistence::new();
        let id: LayerId = [0, 0, 0, 0, 0];

        let err = p.read_file(id, "foo").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn read_nonexistent_file_errors() {
        let p = MemoryPersistence::new();
        let id: LayerId = [1, 1, 1, 1, 1];

        p.create_layer_dir(id).unwrap();
        let err = p.read_file(id, "missing").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn write_to_nonexistent_layer_errors() {
        let p = MemoryPersistence::new();
        let id: LayerId = [9, 9, 9, 9, 9];

        let err = p.write_file(id, "test", b"data").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn file_exists_check() {
        let p = MemoryPersistence::new();
        let id: LayerId = [2, 2, 2, 2, 2];

        p.create_layer_dir(id).unwrap();
        assert!(!p.file_exists(id, "foo").unwrap());

        p.write_file(id, "foo", b"bar").unwrap();
        assert!(p.file_exists(id, "foo").unwrap());
    }

    #[test]
    fn list_layers_returns_all() {
        let p = MemoryPersistence::new();
        let id1: LayerId = [1, 0, 0, 0, 0];
        let id2: LayerId = [2, 0, 0, 0, 0];

        p.create_layer_dir(id1).unwrap();
        p.create_layer_dir(id2).unwrap();

        let mut layers = p.list_layers().unwrap();
        layers.sort();
        assert_eq!(layers.len(), 2);
        assert!(layers.contains(&id1));
        assert!(layers.contains(&id2));
    }

    #[test]
    fn delete_layer_removes_it() {
        let p = MemoryPersistence::new();
        let id: LayerId = [3, 3, 3, 3, 3];

        p.create_layer_dir(id).unwrap();
        assert!(p.layer_exists(id).unwrap());

        p.delete_layer(id).unwrap();
        assert!(!p.layer_exists(id).unwrap());
    }

    #[test]
    fn delete_nonexistent_layer_ok() {
        let p = MemoryPersistence::new();
        let id: LayerId = [4, 4, 4, 4, 4];
        // Should not error
        p.delete_layer(id).unwrap();
    }

    #[test]
    fn label_create_and_get() {
        let p = MemoryPersistence::new();

        let label = p.create_label("main").unwrap();
        assert_eq!(label.name, "main");
        assert_eq!(label.layer, None);
        assert_eq!(label.version, 0);

        let fetched = p.get_label("main").unwrap().unwrap();
        assert_eq!(fetched, label);
    }

    #[test]
    fn label_create_duplicate_errors() {
        let p = MemoryPersistence::new();

        p.create_label("main").unwrap();
        let err = p.create_label("main").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn label_get_nonexistent_returns_none() {
        let p = MemoryPersistence::new();
        assert_eq!(p.get_label("nope").unwrap(), None);
    }

    #[test]
    fn label_set_with_matching_version() {
        let p = MemoryPersistence::new();
        let label = p.create_label("test").unwrap();
        let layer_id: LayerId = [5, 5, 5, 5, 5];

        let updated = p.set_label(&label, Some(layer_id)).unwrap().unwrap();
        assert_eq!(updated.name, "test");
        assert_eq!(updated.layer, Some(layer_id));
        assert_eq!(updated.version, 1);
    }

    #[test]
    fn label_set_with_stale_version_returns_none() {
        let p = MemoryPersistence::new();
        let label = p.create_label("test").unwrap();
        let layer_id: LayerId = [6, 6, 6, 6, 6];

        // Update once to bump version to 1
        p.set_label(&label, Some(layer_id)).unwrap().unwrap();

        // Try to update with stale version 0
        let result = p.set_label(&label, Some([7, 7, 7, 7, 7])).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn label_set_nonexistent_errors() {
        let p = MemoryPersistence::new();
        let fake_label = Label {
            name: "ghost".to_string(),
            layer: None,
            version: 0,
        };

        let err = p.set_label(&fake_label, None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn label_delete_existing() {
        let p = MemoryPersistence::new();
        p.create_label("doomed").unwrap();

        assert!(p.delete_label("doomed").unwrap());
        assert_eq!(p.get_label("doomed").unwrap(), None);
    }

    #[test]
    fn label_delete_nonexistent() {
        let p = MemoryPersistence::new();
        assert!(!p.delete_label("nope").unwrap());
    }

    #[test]
    fn labels_returns_all() {
        let p = MemoryPersistence::new();
        p.create_label("a").unwrap();
        p.create_label("b").unwrap();

        let mut labels: Vec<String> = p.labels().unwrap().into_iter().map(|l| l.name).collect();
        labels.sort();
        assert_eq!(labels, vec!["a", "b"]);
    }

    #[test]
    fn write_overwrites_existing_file() {
        let p = MemoryPersistence::new();
        let id: LayerId = [8, 8, 8, 8, 8];

        p.create_layer_dir(id).unwrap();
        p.write_file(id, "data", b"first").unwrap();
        p.write_file(id, "data", b"second").unwrap();

        let read_back = p.read_file(id, "data").unwrap();
        assert_eq!(&read_back[..], b"second");
    }
}
