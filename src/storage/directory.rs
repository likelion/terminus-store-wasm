//! Directory-based implementation of storage traits.

use locking::*;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};


pub use tdb_succinct_wasm::storage::file::*;

use super::*;

const PREFIX_DIR_SIZE: usize = 3;

#[derive(Clone)]
pub struct DirectoryLayerStore {
    path: PathBuf,
}

impl DirectoryLayerStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore { path: path.into() }
    }
}

impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;
    fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = fs::read_dir(&self.path)?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next().transpose()? {
            if direntry.file_type()?.is_dir() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                result.push(string_to_name(name)?);
            }
        }

        Ok(result)
    }

    fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(name_str);

        fs::create_dir_all(p)?;

        Ok(name)
    }

    fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        let mut p = self.path.clone();
        let name = name_to_string(name);
        p.push(&name[0..PREFIX_DIR_SIZE]);
        p.push(name);

        match fs::metadata(p) {
            Ok(m) => Ok(m.is_dir()),
            Err(_) => Ok(false),
        }
    }

    fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(name);
        Ok(FileBackedStore::new(p))
    }

    fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(file);

        match fs::metadata(p) {
            Ok(m) => Ok(m.is_file()),
            Err(_) => Ok(false),
        }
    }
    fn finalize(&self, directory: [u32; 5]) -> io::Result<()> {
        if cfg!(unix) {
            // ensure the underlying directory record is properly synchronized
            let mut directory_path = self.path.clone();
            let dir_name = name_to_string(directory);
            directory_path.push(&dir_name[0..PREFIX_DIR_SIZE]);
            directory_path.push(dir_name);

            let mut options = std::fs::OpenOptions::new();
            options.create(false);
            options.read(true);
            options.write(false);
            let dir_fd = options.open(directory_path)?;
            dir_fd.sync_all()?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct DirectoryLabelStore {
    path: PathBuf,
}

impl DirectoryLabelStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLabelStore {
        DirectoryLabelStore { path: path.into() }
    }
}

fn get_label_from_data(name: String, data: &[u8]) -> io::Result<Label> {
    let s = String::from_utf8_lossy(&data);
    let lines: Vec<&str> = s.lines().collect();
    if lines.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected label file to have two lines. contents were ({:?})",
                lines
            ),
        ));
    }

    let version_str = &lines[0];
    let layer_str = &lines[1];

    let version = u64::from_str_radix(version_str, 10);
    if version.is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected first line of label file to be a number but it was {}",
                version_str
            ),
        ));
    }

    if layer_str.is_empty() {
        Ok(Label {
            name,
            layer: None,
            version: version.unwrap(),
        })
    } else {
        let layer = layer::string_to_name(layer_str)?;
        Ok(Label {
            name,
            layer: Some(layer),
            version: version.unwrap(),
        })
    }
}

fn get_label_from_file<P: Into<PathBuf>>(path: P) -> io::Result<Label> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = LockedFile::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    get_label_from_data(label, &data)
}

fn get_label_from_exclusive_locked_file<P: Into<PathBuf>>(
    path: P,
) -> io::Result<(Label, ExclusiveLockedFile)> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = ExclusiveLockedFile::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let label = get_label_from_data(label, &data)?;
    file.seek(SeekFrom::Start(0))?;

    Ok((label, file))
}

impl LabelStore for DirectoryLabelStore {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let mut stream = fs::read_dir(self.path.clone())?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next().transpose()? {
            if direntry.file_type()?.is_file() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                if name.ends_with(".label") {
                    let label = get_label_from_file(direntry.path())?;
                    result.push(label);
                }
            }
        }

        Ok(result)
    }

    fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));
        let contents = "0\n\n".to_string().into_bytes();
        match fs::metadata(&p) {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    let mut file = ExclusiveLockedFile::create_and_open(p)?;
                    file.write_all(&contents)?;
                    file.flush()?;
                    file.sync_all()?;

                    Ok(Label::new_empty(label))
                }
                _ => Err(e),
            },
        }
    }

    fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));

        match get_label_from_file(p) {
            Ok(label) => Ok(Some(label)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));
        let (retrieved_label, mut file) = get_label_from_exclusive_locked_file(p)?;
        if retrieved_label == *label {
            // all good, let's a go
            file.truncate()?;
            file.write_all(&contents)?;
            file.flush()?;
            file.sync_all()?;
            Ok(Some(new_label))
        } else {
            Ok(None)
        }
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", name));

        // We're not locking here to remove the file. The assumption
        // is that any concurrent operation that is done on the label
        // file will not matter. If it is a label read, a concurrent
        // operation will simply get the label contents, which
        // immediately afterwards become invalid. Similarly if it is
        // for a write, the write will appear to be succesful even
        // though the file will be gone afterwards. This is
        // indistinguishable from the case where the read/write and
        // the remove happened in reverse order.
        match std::fs::remove_file(p) {
            Ok(()) => Ok(true),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(false),
                _ => Err(e),
            },
        }
    }
}

/// A version of the directory label store that keeps all labels in
/// memory and doesn't lock.
///
/// This is useful for situations where we can be sure that only one
/// process is working with a set of label files. In that case, we can
/// keep all label files cached in memory in order to process reads as
/// quickly as possible, and we can perform writes without any sort of
/// file system locking.
pub struct CachedDirectoryLabelStore {
    path: PathBuf,
    labels: Arc<RwLock<HashMap<String, Label>>>,
}

impl CachedDirectoryLabelStore {
    /// Open a new label store.
    ///
    /// This will read in all label files on startup.
    pub fn open<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path: PathBuf = path.into();
        let labels = get_all_labels_from_dir(&path)?;

        Ok(Self {
            path,
            labels: Arc::new(RwLock::new(labels)),
        })
    }
}

fn get_all_labels_from_dir(p: &PathBuf) -> io::Result<HashMap<String, Label>> {
    let mut result = HashMap::new();
    let mut entries = std::fs::read_dir(p)?;

    while let Some(entry) = entries.next().transpose()? {
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Ok(file_name) = entry.file_name().into_string() {
            if !file_name.ends_with(".label") {
                continue;
            }

            let label_name = file_name[..file_name.len() - 6].to_string();
            let label = get_label_from_file(entry.path())?;

            result.insert(label_name, label);
        }
    }

    Ok(result)
}

impl LabelStore for CachedDirectoryLabelStore {
    fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().unwrap();
        Ok(labels.values().cloned().collect())
    }

    fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut labels = self.labels.write().unwrap();
        if labels.contains_key(label) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            ));
        }

        let mut p = self.path.clone();
        p.push(format!("{}.label", label));
        let contents = b"0\n\n";
        match fs::metadata(&p) {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::Other,
                "label was not in cached map but was found on disk",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    let mut options = fs::OpenOptions::new();
                    options.create_new(true);
                    options.write(true);
                    let mut file = options.open(p)?;
                    file.write_all(contents)?;
                    file.flush()?;
                    file.sync_all()?;

                    let l = Label::new_empty(label);
                    labels.insert(label.to_string(), l.clone());

                    Ok(l)
                }
                _ => Err(e),
            },
        }
    }
    fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let labels = self.labels.read().unwrap();
        Ok(labels.get(label).cloned())
    }
    fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut labels = self.labels.write().unwrap();
        if let Some(retrieved_label) = labels.get(&label.name) {
            if retrieved_label == label {
                // all good, let's a go
                let mut p = self.path.clone();
                p.push(format!("{}.label", label.name));
                let mut options = fs::OpenOptions::new();
                options.create(false);
                options.write(true);
                let mut file = options.open(p)?;
                file.write_all(&contents)?;
                file.flush()?;
                file.sync_data()?;

                labels.insert(label.name.clone(), new_label.clone());
                Ok(Some(new_label))
            } else {
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "label not found"))
        }
    }

    fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut labels = self.labels.write().unwrap();
        if labels.remove(name).is_some() {
            let mut p = self.path.clone();
            p.push(format!("{}.label", name));
            std::fs::remove_file(p)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;

    /// Simple temp directory helper that cleans up on drop (replaces tempfile::tempdir)
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> io::Result<Self> {
            let mut path = std::env::temp_dir();
            let random_name: [u32; 2] = rand::random();
            path.push(format!("terminus-test-{:08x}{:08x}", random_name[0], random_name[1]));
            std::fs::create_dir_all(&path)?;
            Ok(TempDir(path))
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> io::Result<TempDir> {
        TempDir::new()
    }

    #[test]
    fn write_and_read_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        w.flush().unwrap();
        let mut buf = Vec::new();
        file.open_read()
            
            .unwrap()
            .read_to_end(&mut buf)
            
            .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[test]
    fn write_and_map_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().unwrap();
        w.write_all(&[1, 2, 3]).unwrap();
        w.flush().unwrap();

        let map = file.map().unwrap();

        assert_eq!(&vec![1, 2, 3][..], &map.as_ref()[..]);
    }

    #[test]
    fn write_and_map_large_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().unwrap();
        let mut contents = vec![0u8; 4096 << 4];
        for i in 0..contents.capacity() {
            contents[i] = (i as usize % 256) as u8;
        }

        w.write_all(&contents).unwrap();
        w.flush().unwrap();

        let map = file.map().unwrap();

        assert_eq!(contents, map.as_ref());
    }

    #[test]
    fn create_layers_from_directory_store() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());

        let layer = {
            let mut builder = store.create_base_layer().unwrap();
            let base_name = builder.name();

            builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
            builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
            builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

            builder.commit_boxed().unwrap();

            let mut builder = store.create_child_layer(base_name).unwrap();
            let child_name = builder.name();

            builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
            builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

            builder.commit_boxed().unwrap();

            store.get_layer(child_name).unwrap().unwrap()
        };

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[test]
    fn directory_create_and_retrieve_equal_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let stored = store.create_label("foo").unwrap();
        let retrieved = store.get_label("foo").unwrap();

        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[test]
    fn directory_update_label_succeeds() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let stored = store.create_label("foo").unwrap();
        store.set_label(&stored, [6, 7, 8, 9, 10]).unwrap();
        let retrieved = store.get_label("foo").unwrap().unwrap();

        assert_eq!(Some([6, 7, 8, 9, 10]), retrieved.layer);
    }

    #[test]
    fn directory_update_label_twice_from_same_label_object_fails() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let stored1 = store.create_label("foo").unwrap();
        let stored2 = store.set_label(&stored1, [6, 7, 8, 9, 10]).unwrap();
        let stored3 = store.set_label(&stored1, [10, 9, 8, 7, 6]).unwrap();

        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }

    #[test]
    fn directory_create_label_twice_errors() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").unwrap();
        let result = store.create_label("foo");

        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
    }

    #[test]
    fn nonexistent_file_is_nonexistent() {
        let file = FileBackedStore::new("asdfasfopivbuzxcvopiuvpoawehkafpouzvxv");
        assert!(!file.exists().unwrap());
    }

    #[test]
    fn rollup_and_retrieve_base() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().unwrap();

        let mut builder = store.create_child_layer(base_name).unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().unwrap();

        let unrolled_layer = store.get_layer(child_name).unwrap().unwrap();

        let _rolled_id = store.clone().rollup(unrolled_layer).unwrap();
        let rolled_layer = store.get_layer(child_name).unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[test]
    fn rollup_and_retrieve_child() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().unwrap();

        let mut builder = store.create_child_layer(base_name).unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().unwrap();

        let mut builder = store.create_child_layer(child_name).unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("cow", "likes", "pig"));
        builder.add_value_triple(ValueTriple::new_node("cow", "hates", "pig"));

        builder.commit_boxed().unwrap();

        let unrolled_layer = store.get_layer(child_name).unwrap().unwrap();

        let _rolled_id = store
            .clone()
            .rollup_upto(unrolled_layer, base_name)
            
            .unwrap();
        let rolled_layer = store.get_layer(child_name).unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "hates", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[test]
    fn create_and_delete_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").unwrap();
        assert!(store.get_label("foo").unwrap().is_some());
        assert!(store.delete_label("foo").unwrap());
        assert!(store.get_label("foo").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        assert!(!store.delete_label("foo").unwrap());
    }

    #[test]
    fn delete_shared_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = LockedFile::open(label_path).unwrap();

        assert!(store.delete_label("foo").unwrap());
    }

    #[test]
    fn delete_exclusive_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = ExclusiveLockedFile::open(label_path).unwrap();

        assert!(store.delete_label("foo").unwrap());
    }
}
