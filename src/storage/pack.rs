use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::io::{self, Read, Write};
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use super::cache::*;
use super::consts::*;
use super::file::*;
use super::layer::*;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tar::*;

pub trait Packable {
    /// Export the given layers by creating a pack, a Vec<u8> that can later be used with `import_layers` on a different store.
    fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>>;

    /// Import the specified layers from the given pack, a byte slice that was previously generated with `export_layers`, on another store, and possibly even another machine).
    ///
    /// After this operation, the specified layers will be retrievable
    /// from this store, provided they existed in the pack. specified
    /// layers that are not in the pack are silently ignored.
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()>;
}

impl<T: PersistentLayerStore> Packable for T {
    fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        let mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        #[cfg(target_arch = "wasm32")]
        let mtime = 0u64;

        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            for id in layer_ids {
                tar_append_layer(&mut tar, self, id, mtime)?;
            }
            tar.finish().unwrap();
        }
        // TODO: Proper error handling
        Ok(enc.finish().unwrap())
    }

    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        let mut layer_id_set = HashSet::new();
        let mut existing_layers = HashSet::new();
        for id in layer_ids {
            let id_str = name_to_string(id);
            // Skip layers that already exist (Requirement 17.3)
            if self.directory_exists(id)? {
                existing_layers.insert(id_str.clone());
            } else {
                self.create_named_directory(id)?;
            }
            layer_id_set.insert(id_str);
        }

        {
            let cursor = io::Cursor::new(pack);
            let tar = GzDecoder::new(cursor);
            let mut archive = Archive::new(tar);

            // TODO we actually need to validate that these layers, when extracted, will make for a valid store.
            for e in archive.entries()? {
                let mut entry = e?;
                let path = entry.path()?;
                let os_file_name = path.file_name().unwrap();
                let file_name = os_file_name
                    .to_str()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unexpected non-utf8 directory name",
                        )
                    })?
                    .to_owned();

                // check if entry is prefixed with a layer id we are interested in
                let layer_id = path.iter().next().and_then(|p| p.to_str()).unwrap_or("");

                if layer_id_set.contains(layer_id) && !existing_layers.contains(layer_id) {
                    // this conversion should always work cause we are
                    // only able to match things that went through the
                    // conversion in the opposite direction.
                    let layer_id_arr = string_to_name(layer_id).unwrap();

                    let header = entry.header();
                    if !header.entry_type().is_file() {
                        continue;
                    }

                    let mut content = Vec::with_capacity(header.size()? as usize);
                    entry.read_to_end(&mut content)?;

                    {
                        let file = self.get_file(layer_id_arr, &file_name)?;
                        let mut writer = file.open_write()?;
                        writer.write_all(&content)?;
                        writer.flush()?;
                        writer.sync_all()?;
                    };
                }
            }

            for layer_id in layer_id_set {
                if existing_layers.contains(&layer_id) {
                    continue; // Skip finalization for already-existing layers
                }
                let layer_id_arr = string_to_name(&layer_id).unwrap();
                self.finalize_layer(layer_id_arr)?;
            }

            Ok(())
        }
    }
}

fn tar_append_file<S: PersistentLayerStore, W: io::Write>(
    store: &S,
    tar: &mut tar::Builder<W>,
    layer: [u32; 5],
    layer_path: &PathBuf,
    file_name: &str,
    mtime: u64,
) -> io::Result<()> {
    if store.file_exists(layer, file_name)? {
        let file = store.get_file(layer, file_name)?;
        let contents = file.map()?;
        let cursor = io::Cursor::new(&contents);

        let path = layer_path.join(file_name);

        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(file.size()? as u64);
        header.set_mtime(mtime);
        tar.append_data(&mut header, path, cursor).unwrap();

        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "file does not exist",
        ))
    }
}

fn tar_append_file_if_exists<S: PersistentLayerStore, W: io::Write>(
    store: &S,
    tar: &mut tar::Builder<W>,
    layer: [u32; 5],
    layer_path: &PathBuf,
    file_name: &str,
    mtime: u64,
) -> io::Result<()> {
    if store.file_exists(layer, file_name)? {
        let file = store.get_file(layer, file_name)?;
        let contents = file.map()?;
        let cursor = io::Cursor::new(&contents);

        let path = layer_path.join(file_name);

        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(file.size()? as u64);
        header.set_mtime(mtime);
        tar.append_data(&mut header, path, cursor).unwrap();
    }

    Ok(())
}

fn tar_append_layer<W: io::Write, S: PersistentLayerStore>(
    tar: &mut tar::Builder<W>,
    store: &S,
    layer: [u32; 5],
    mtime: u64,
) -> io::Result<()> {
    let mut header = Header::new_gnu();
    header.set_mode(0o755);
    header.set_entry_type(EntryType::Directory);
    header.set_mtime(mtime);
    header.set_size(0);
    let layer_name = name_to_string(layer);
    let mut path = PathBuf::new();
    path.push(layer_name);
    tar.append_data(&mut header, &path, std::io::empty())
        .unwrap();

    for f in &SHARED_REQUIRED_FILES {
        tar_append_file(store, tar, layer, &path, f, mtime)?;
    }
    for f in &SHARED_OPTIONAL_FILES {
        if f == &FILENAMES.rollup {
            // skip the rollup file. It will not be resolvable remotely.
            continue;
        }
        tar_append_file_if_exists(store, tar, layer, &path, f, mtime)?;
    }
    if store.file_exists(layer, FILENAMES.parent)? {
        // this is a child layer
        for f in &CHILD_LAYER_REQUIRED_FILES {
            tar_append_file(store, tar, layer, &path, f, mtime)?;
        }
        for f in &CHILD_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(store, tar, layer, &path, f, mtime)?;
        }
    } else {
        // this is a base layer
        for f in &BASE_LAYER_REQUIRED_FILES {
            tar_append_file(store, tar, layer, &path, f, mtime)?;
        }
        for f in &BASE_LAYER_OPTIONAL_FILES {
            tar_append_file_if_exists(store, tar, layer, &path, f, mtime)?;
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum PackError {
    LayerNotFound,
    Io(io::Error),
    Utf8Error(std::str::Utf8Error),
}

impl Display for PackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(formatter, "{:?}", self)
    }
}

impl From<io::Error> for PackError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
impl From<std::str::Utf8Error> for PackError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Utf8Error(err)
    }
}

pub fn pack_layer_parents<R: io::Read>(
    readable: R,
) -> Result<HashMap<[u32; 5], Option<[u32; 5]>>, PackError> {
    let tar = GzDecoder::new(readable);
    let mut archive = Archive::new(tar);

    // build a set out of the layer ids for easy retrieval
    let mut result_map = HashMap::new();

    for e in archive.entries()? {
        let mut entry = e?;
        let path = entry.path()?;

        let id = string_to_name(
            path.iter()
                .next()
                .expect("expected path to have at least one component")
                .to_str()
                .expect("expected proper unicode path"),
        )?;

        if path.file_name().expect("expected path to have a filename") == "parent.hex" {
            // this is an element we want to know the parent of
            // lets read it
            let mut parent_id_bytes = [0u8; 40];
            entry.read_exact(&mut parent_id_bytes)?;
            let parent_id_str = std::str::from_utf8(&parent_id_bytes)?;
            let parent_id = string_to_name(parent_id_str)?;

            result_map.insert(id, Some(parent_id));
        } else {
            // Ensure that an entry for this layer exists
            // If we encounter the parent file later on, this'll be overwritten with the parent id.
            // If not, it can be assumed to not have a parent.
            result_map.entry(id).or_insert(None);
        }
    }

    Ok(result_map)
}

impl Packable for CachedLayerStore {
    fn export_layers(
        &self,
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<Vec<u8>> {
        self.inner.export_layers(layer_ids)
    }

    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]> + Send>,
    ) -> io::Result<()> {
        self.inner.import_layers(pack, layer_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use crate::storage::directory::*;
    use std::sync::Arc;

    /// Simple temp directory helper that cleans up on drop (replaces tempfile::tempdir)
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> io::Result<Self> {
            let mut path = std::env::temp_dir();
            let random_name: [u32; 2] = rand::random();
            path.push(format!(
                "terminus-test-{:08x}{:08x}",
                random_name[0], random_name[1]
            ));
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
    fn export_import_layer_with_rollup() {
        let dir1 = tempdir().unwrap();
        let store1 = Arc::new(DirectoryLayerStore::new(dir1.path()));
        let dir2 = tempdir().unwrap();
        let store2 = Arc::new(DirectoryLayerStore::new(dir2.path()));

        let mut builder = store1.create_base_layer().unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "duck"));
        builder.add_value_triple(ValueTriple::new_node("duck", "hates", "cow"));

        builder.commit_boxed().unwrap();

        let mut builder = store1.create_child_layer(base_name).unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_node("duck", "hates", "cow"));
        builder.add_value_triple(ValueTriple::new_node("duck", "likes", "cow"));

        builder.commit_boxed().unwrap();

        let unrolled_layer = store1.get_layer(child_name).unwrap().unwrap();

        store1.clone().rollup(unrolled_layer).unwrap();

        let export = store1
            .export_layers(Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        store2
            .import_layers(&export, Box::new(vec![base_name, child_name].into_iter()))
            .unwrap();

        let imported_layer = store2.get_layer(child_name).unwrap().unwrap();
        let triples: Vec<_> = imported_layer
            .triples()
            .map(|t| imported_layer.id_triple_to_string(&t).unwrap())
            .collect();
        assert_eq!(
            vec![
                ValueTriple::new_node("cow", "likes", "duck"),
                ValueTriple::new_node("duck", "likes", "cow")
            ],
            triples
        );
    }
}
