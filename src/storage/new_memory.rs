use std::collections::HashMap;
use std::io;
use std::sync::{Arc, RwLock};

use super::file::*;
use super::layer::*;

use bytes::{Bytes, BytesMut};

enum MemoryBackedStoreContents {
    Nonexistent,
    Existent(Bytes),
}

#[derive(Clone)]
pub struct NewMemoryBackedStore {
    contents: Arc<RwLock<MemoryBackedStoreContents>>,
}

impl NewMemoryBackedStore {
    pub fn new() -> Self {
        Self {
            contents: Arc::new(RwLock::new(MemoryBackedStoreContents::Nonexistent)),
        }
    }
}

pub struct NewMemoryBackedStoreWriter {
    file: NewMemoryBackedStore,
    bytes: BytesMut,
}

impl SyncableFile for NewMemoryBackedStoreWriter {
    fn sync_all(self) -> io::Result<()> {
        let mut contents = self.file.contents.write().unwrap();
        *contents = MemoryBackedStoreContents::Existent(self.bytes.freeze());
        Ok(())
    }
}

impl std::io::Write for NewMemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl FileStore for NewMemoryBackedStore {
    type Write = NewMemoryBackedStoreWriter;

    fn open_write(&self) -> Self::Write {
        NewMemoryBackedStoreWriter {
            file: self.clone(),
            bytes: BytesMut::new(),
        }
    }
}

pub struct NewMemoryBackedStoreReader {
    bytes: Bytes,
    pos: usize,
}

impl std::io::Read for NewMemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        if self.bytes.len() == self.pos {
            // end of file
            Ok(0)
        } else if self.bytes.len() < self.pos + buf.len() {
            // read up to end
            let len = self.bytes.len() - self.pos;
            buf[..len].copy_from_slice(&self.bytes[self.pos..]);
            self.pos += len;
            Ok(len)
        } else {
            // read full buf
            buf.copy_from_slice(&self.bytes[self.pos..self.pos + buf.len()]);
            self.pos += buf.len();
            Ok(buf.len())
        }
    }
}

impl FileLoad for NewMemoryBackedStore {
    type Read = NewMemoryBackedStoreReader;

    fn exists(&self) -> bool {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => false,
            _ => true,
        }
    }

    fn size(&self) -> usize {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to retrieve size of nonexistent memory file")
            }
            MemoryBackedStoreContents::Existent(bytes) => bytes.len(),
        }
    }

    fn open_read_from(&self, offset: usize) -> NewMemoryBackedStoreReader {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => NewMemoryBackedStoreReader {
                bytes: bytes.clone(),
                pos: offset,
            },
        }
    }

    fn map(&self) -> io::Result<Bytes> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => Ok(bytes.clone()),
        }
    }
}

#[derive(Clone, Default)]
pub struct NewMemoryLayerStore {
    layers: std::sync::RwLock<HashMap<[u32; 5], HashMap<String, NewMemoryBackedStore>>>,
}

impl PersistentLayerStore for NewMemoryLayerStore {
    type File = NewMemoryBackedStore;

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
            Ok(files.contains_key(file))
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
                Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "layer not found"))
        }
    }

    fn export_layers(&self, _layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        todo!();
    }

    fn import_layers(
        &self,
        _pack: &[u8],
        _layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        todo!();
    }
}
