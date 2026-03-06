#![cfg(not(target_arch = "wasm32"))]

use bytes::Bytes;
use std::fs;
use std::io;
use std::path::PathBuf;

use super::label::Label;
use super::layer::name_to_string;
use super::persistence::{LayerId, LayerPersistence, LabelPersistence};

fn layer_id_to_hex(id: LayerId) -> String {
    name_to_string(id)
}

fn hex_to_layer_id(hex: &str) -> Option<LayerId> {
    if hex.len() != 40 {
        return None;
    }
    let mut id = [0u32; 5];
    for (i, chunk) in hex.as_bytes().chunks(8).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        id[i] = u32::from_str_radix(s, 16).ok()?;
    }
    Some(id)
}

/// Filesystem persistence backend.
/// Available only on non-wasm32 targets.
pub struct FsPersistence {
    root: PathBuf,
}

impl FsPersistence {
    pub fn new(root: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn layer_dir(&self, id: LayerId) -> PathBuf {
        self.root.join(layer_id_to_hex(id))
    }

    fn label_path(&self, name: &str) -> PathBuf {
        self.root.join(format!("{}.label", name))
    }

    fn write_label_file(&self, label: &Label) -> io::Result<()> {
        let layer_str = match label.layer {
            Some(id) => layer_id_to_hex(id),
            None => "none".to_string(),
        };
        let content = format!("{}\n{}", layer_str, label.version);
        fs::write(self.label_path(&label.name), content)
    }

    fn read_label_file(&self, name: &str) -> io::Result<Option<Label>> {
        let path = self.label_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&path)?;
        let mut lines = content.lines();
        let layer_str = lines.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing layer line")
        })?;
        let version_str = lines.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing version line")
        })?;
        let layer = if layer_str == "none" {
            None
        } else {
            Some(hex_to_layer_id(layer_str).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid layer id")
            })?)
        };
        let version: u64 = version_str.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid version")
        })?;
        Ok(Some(Label {
            name: name.to_string(),
            layer,
            version,
        }))
    }
}

impl LayerPersistence for FsPersistence {
    fn layer_exists(&self, id: LayerId) -> io::Result<bool> {
        Ok(self.layer_dir(id).is_dir())
    }

    fn create_layer_dir(&self, id: LayerId) -> io::Result<()> {
        let dir = self.layer_dir(id);
        if dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("layer directory already exists: {:?}", dir),
            ));
        }
        fs::create_dir(&dir)
    }

    fn file_exists(&self, layer: LayerId, file: &str) -> io::Result<bool> {
        Ok(self.layer_dir(layer).join(file).is_file())
    }

    fn read_file(&self, layer: LayerId, file: &str) -> io::Result<Bytes> {
        let path = self.layer_dir(layer).join(file);
        let data = fs::read(&path)?;
        Ok(Bytes::from(data))
    }

    fn write_file(&self, layer: LayerId, file: &str, data: &[u8]) -> io::Result<()> {
        let path = self.layer_dir(layer).join(file);
        fs::write(&path, data)
    }

    fn list_layers(&self) -> io::Result<Vec<LayerId>> {
        let mut layers = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.len() == 40 {
                        if let Some(id) = hex_to_layer_id(name) {
                            layers.push(id);
                        }
                    }
                }
            }
        }
        Ok(layers)
    }

    fn delete_layer(&self, id: LayerId) -> io::Result<()> {
        let dir = self.layer_dir(id);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }
}

impl LabelPersistence for FsPersistence {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let mut result = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".label") {
                    let label_name = &name[..name.len() - 6];
                    if let Some(label) = self.read_label_file(label_name)? {
                        result.push(label);
                    }
                }
            }
        }
        Ok(result)
    }

    fn create_label(&self, name: &str) -> io::Result<Label> {
        let path = self.label_path(name);
        if path.exists() {
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
        self.write_label_file(&label)?;
        Ok(label)
    }

    fn get_label(&self, name: &str) -> io::Result<Option<Label>> {
        self.read_label_file(name)
    }

    fn set_label(&self, label: &Label, layer: Option<LayerId>) -> io::Result<Option<Label>> {
        let stored = self.read_label_file(&label.name)?;
        let stored = match stored {
            Some(s) => s,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("label {:?} not found", label.name),
                ));
            }
        };

        if stored.version != label.version {
            return Ok(None);
        }

        let updated = Label {
            name: label.name.clone(),
            layer,
            version: label.version + 1,
        };
        self.write_label_file(&updated)?;
        Ok(Some(updated))
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let path = self.label_path(name);
        if path.exists() {
            fs::remove_file(&path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_dir() -> PathBuf {
        let dir = env::temp_dir().join(format!("fs_persistence_test_{}", rand::random::<u64>()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn create_and_check_layer() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [1, 2, 3, 4, 5];

        assert!(!p.layer_exists(id).unwrap());
        p.create_layer_dir(id).unwrap();
        assert!(p.layer_exists(id).unwrap());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn create_duplicate_layer_errors() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [1, 2, 3, 4, 5];

        p.create_layer_dir(id).unwrap();
        let err = p.create_layer_dir(id).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn write_and_read_file() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [10, 20, 30, 40, 50];
        let data = b"hello world";

        p.create_layer_dir(id).unwrap();
        p.write_file(id, "test.bin", data).unwrap();

        let read_back = p.read_file(id, "test.bin").unwrap();
        assert_eq!(&read_back[..], data);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn file_exists_check() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [2, 2, 2, 2, 2];

        p.create_layer_dir(id).unwrap();
        assert!(!p.file_exists(id, "foo").unwrap());

        p.write_file(id, "foo", b"bar").unwrap();
        assert!(p.file_exists(id, "foo").unwrap());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn list_layers_returns_all() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id1: LayerId = [1, 0, 0, 0, 0];
        let id2: LayerId = [2, 0, 0, 0, 0];

        p.create_layer_dir(id1).unwrap();
        p.create_layer_dir(id2).unwrap();

        let mut layers = p.list_layers().unwrap();
        layers.sort();
        assert_eq!(layers.len(), 2);
        assert!(layers.contains(&id1));
        assert!(layers.contains(&id2));

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn delete_layer_removes_it() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [3, 3, 3, 3, 3];

        p.create_layer_dir(id).unwrap();
        assert!(p.layer_exists(id).unwrap());

        p.delete_layer(id).unwrap();
        assert!(!p.layer_exists(id).unwrap());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn delete_nonexistent_layer_ok() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [4, 4, 4, 4, 4];
        p.delete_layer(id).unwrap();

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_create_and_get() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();

        let label = p.create_label("main").unwrap();
        assert_eq!(label.name, "main");
        assert_eq!(label.layer, None);
        assert_eq!(label.version, 0);

        let fetched = p.get_label("main").unwrap().unwrap();
        assert_eq!(fetched, label);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_create_duplicate_errors() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();

        p.create_label("main").unwrap();
        let err = p.create_label("main").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_get_nonexistent_returns_none() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        assert_eq!(p.get_label("nope").unwrap(), None);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_set_with_matching_version() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let label = p.create_label("test").unwrap();
        let layer_id: LayerId = [5, 5, 5, 5, 5];

        let updated = p.set_label(&label, Some(layer_id)).unwrap().unwrap();
        assert_eq!(updated.name, "test");
        assert_eq!(updated.layer, Some(layer_id));
        assert_eq!(updated.version, 1);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_set_with_stale_version_returns_none() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let label = p.create_label("test").unwrap();
        let layer_id: LayerId = [6, 6, 6, 6, 6];

        p.set_label(&label, Some(layer_id)).unwrap().unwrap();

        let result = p.set_label(&label, Some([7, 7, 7, 7, 7])).unwrap();
        assert_eq!(result, None);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_set_nonexistent_errors() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let fake_label = Label {
            name: "ghost".to_string(),
            layer: None,
            version: 0,
        };

        let err = p.set_label(&fake_label, None).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_delete_existing() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        p.create_label("doomed").unwrap();

        assert!(p.delete_label("doomed").unwrap());
        assert_eq!(p.get_label("doomed").unwrap(), None);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn label_delete_nonexistent() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        assert!(!p.delete_label("nope").unwrap());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn labels_returns_all() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        p.create_label("a").unwrap();
        p.create_label("b").unwrap();

        let mut labels: Vec<String> = p.labels().unwrap().into_iter().map(|l| l.name).collect();
        labels.sort();
        assert_eq!(labels, vec!["a", "b"]);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn write_overwrites_existing_file() {
        let root = temp_dir();
        let p = FsPersistence::new(root.clone()).unwrap();
        let id: LayerId = [8, 8, 8, 8, 8];

        p.create_layer_dir(id).unwrap();
        p.write_file(id, "data", b"first").unwrap();
        p.write_file(id, "data", b"second").unwrap();

        let read_back = p.read_file(id, "data").unwrap();
        assert_eq!(&read_back[..], b"second");

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn hex_round_trip() {
        let id: LayerId = [0xdeadbeef, 0x12345678, 0xabcdef01, 0x00000000, 0xffffffff];
        let hex = layer_id_to_hex(id);
        assert_eq!(hex.len(), 40);
        let parsed = hex_to_layer_id(&hex).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn hex_invalid_length_returns_none() {
        assert_eq!(hex_to_layer_id("abc"), None);
        assert_eq!(hex_to_layer_id(""), None);
    }

    #[test]
    fn hex_invalid_chars_returns_none() {
        // 40 chars but not valid hex
        assert_eq!(hex_to_layer_id("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), None);
    }
}
