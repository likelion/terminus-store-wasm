// File format:
// <header>
//  [<filetype present>]*
//  [<offsets>]*
//

use std::{
    collections::HashMap,
    io::{self, ErrorKind, SeekFrom},
    ops::Range,
    path::PathBuf,
    sync::{Arc, RwLock},
};

#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lru::LruCache;
use std::fs::{self, File};
use std::io::{Read, Write, Seek};

use tdb_succinct_wasm::{
    logarray_length_from_control_word, smallbitarray::SmallBitArray, LateLogArrayBufBuilder,
    MonotonicLogArray,
};

use super::{
    consts::{LayerFileEnum, FILENAME_ENUM_MAP},
    locking::{ExclusiveLockedFile, LockedFile},
    name_to_string, string_to_name, FileLoad, FileStore, PersistentLayerStore, SyncableFile,
};

pub trait ArchiveBackend: Clone + Send + Sync {
    type Read: std::io::Read + Send;
    fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes>;
    fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Option<Bytes>>;
    fn store_layer_file(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()>;
    fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read>;
}

pub trait ArchiveMetadataBackend: Clone + Send + Sync {
    fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>>;
    fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool>;
    fn layer_size(&self, id: [u32; 5]) -> io::Result<u64>;
    fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool>;
    fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize>;
    fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
    fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()>;
    fn get_parent(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>>;
}

pub struct BytesReader(Bytes);

impl std::io::Read for BytesReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes = &mut self.0;
        let to_read = std::cmp::min(buf.len(), bytes.len());
        if to_read == 0 {
            return Ok(0);
        }
        let consumed = bytes.split_to(to_read);
        buf[..to_read].copy_from_slice(consumed.as_ref());
        Ok(to_read)
    }
}

#[derive(Clone)]
pub struct DirectoryArchiveBackend {
    path: PathBuf,
}

impl DirectoryArchiveBackend {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    fn path_for_layer(&self, name: [u32; 5]) -> PathBuf {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(&format!("{}.larch", name_str));

        p
    }

    fn path_for_rollup(&self, name: [u32; 5]) -> PathBuf {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(&format!("{}.rollup.hex", name_str));

        p
    }
}

impl ArchiveBackend for DirectoryArchiveBackend {
    type Read = ArchiveSliceReader;
    fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        let path = self.path_for_layer(id);
        let mut options = fs::OpenOptions::new();
        options.read(true);
        options.create(false);
        let mut result = options.open(path)?;
        let metadata = result.metadata()?;
        #[cfg(target_os = "windows")]
        let size = metadata.file_size();
        #[cfg(not(target_os = "windows"))]
        let size = metadata.size();
        let mut buf = Vec::with_capacity(size as usize);
        result.read_to_end(&mut buf)?;
        buf.shrink_to_fit();

        Ok(buf.into())
    }

    fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Option<Bytes>> {
        let path = self.path_for_layer(id);
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path)?;
        let header = ArchiveHeader::parse_from_reader(&mut file)?;
        if let Some(range) = header.range_for(file_type) {
            let mut data = vec![0; range.len()];
            file.seek(SeekFrom::Current((range.start) as i64))?;
            file.read_exact(&mut data)?;

            Ok(Some(Bytes::from(data)))
        } else {
            Ok(None)
        }
    }

    fn store_layer_file(&self, id: [u32; 5], mut bytes: Bytes) -> io::Result<()> {
        let path = self.path_for_layer(id);
        let mut directory_path = path.clone();
        directory_path.pop();
        fs::create_dir_all(&directory_path)?;

        let mut options = std::fs::OpenOptions::new();
        options.create(true);
        options.write(true);
        let mut file = options.open(path)?;
        while bytes.remaining() > 0 {
            let chunk = bytes.chunk();
            let written = file.write(chunk)?;
            bytes.advance(written);
        }

        file.flush()?;
        file.sync_all()?;

        if cfg!(unix) {
            // ensure the underlying directory record is properly synchronized
            let mut options = std::fs::OpenOptions::new();
            options.create(false);
            options.read(true);
            options.write(false);
            let dir_fd = options.open(directory_path)?;
            dir_fd.sync_all()?;
        }

        Ok(())
    }

    fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        let path = self.path_for_layer(id);
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path)?;
        let header = ArchiveHeader::parse_from_reader(&mut file)?;

        let range = header
            .range_for(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))?;

        let remaining = range.len() - read_from;
        file.seek(SeekFrom::Current((range.start + read_from) as i64))
            ?;

        Ok(ArchiveSliceReader { file, remaining })
    }
}

impl ArchiveMetadataBackend for DirectoryArchiveBackend {
    fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = fs::read_dir(&self.path)?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry()? {
            let os_name = direntry.file_name();
            let name = os_name.to_str().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected non-utf8 directory name",
                )
            })?;
            if name.ends_with(".larch") && direntry.file_type()?.is_file() {
                let name_component = &name[..name.len() - 6];
                result.push(string_to_name(name_component)?);
            }
        }

        Ok(result)
    }

    fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool> {
        let path = self.path_for_layer(id);
        let metadata = std::fs::metadata(path);
        if metadata.is_err() && metadata.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            // layer itself not found
            return Ok(false);
        }
        // propagate error if it was anything but NotFound
        metadata?;

        // if we got here it means the layer exists
        Ok(true)
    }

    fn layer_size(&self, id: [u32; 5]) -> io::Result<u64> {
        let path = self.path_for_layer(id);
        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }

    fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool> {
        let path = self.path_for_layer(id);
        let metadata = std::fs::metadata(&path);
        if metadata.is_err() && metadata.as_ref().err().unwrap().kind() == io::ErrorKind::NotFound {
            // layer itself not found
            return Ok(false);
        }
        // propagate error if it was anything but NotFound
        metadata?;

        // read header!
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path)?;
        let header = ArchiveFilePresenceHeader::new(file.read_u64()?);

        Ok(header.is_present(file_type))
    }

    fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize> {
        let path = self.path_for_layer(id);
        // read header!
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        let mut file = options.open(path)?;
        let header = ArchiveHeader::parse_from_reader(&mut file)?;

        header
            .size_of(file_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }

    fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        // acquire a shared lock on the layer. This ensures nobody will write a rollup file while we're retrieving it.
        let layer_path = self.path_for_layer(id);
        let layer_lock = LockedFile::open(layer_path);
        if layer_lock.is_err() && layer_lock.as_ref().err().unwrap().kind() == ErrorKind::NotFound {
            // no such layer - therefore no such rollup
            return Ok(None);
        }
        let _layer_lock = layer_lock.unwrap();

        let path = self.path_for_rollup(id);
        let result = fs::read_to_string(path);

        if result.is_err() && result.as_ref().err().unwrap().kind() == ErrorKind::NotFound {
            return Ok(None);
        }
        let data = result?;
        let name = data.lines().skip(1).next().expect(
            "Expected rollup file to have two lines but was unable to skip to the second line",
        );
        Ok(Some(string_to_name(&name)?))
    }

    fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        // acquire an exclusive lock on the layer. This ensures nobody tries to lookup the rollup while we're writing it.
        let layer_path = self.path_for_layer(id);
        let _layer_lock = ExclusiveLockedFile::open(layer_path)?;

        let path = self.path_for_rollup(id);
        let mut data = Vec::with_capacity(43);
        data.extend_from_slice(b"1\n");
        data.extend_from_slice(name_to_string(rollup).as_bytes());
        data.extend_from_slice(b"\n");
        let mut file = fs::File::create(path)?;
        file.write_all(&data)?;
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    fn get_parent(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        if let Some(parent_bytes) = self
            .get_layer_structure_bytes(id, LayerFileEnum::Parent)
            ?
        {
            let parent_string = std::str::from_utf8(&parent_bytes[..40]).unwrap();
            Ok(Some(string_to_name(parent_string).unwrap()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct LruArchiveBackend<M, D> {
    cache: Arc<std::sync::Mutex<LruCache<[u32; 5], CacheEntry>>>,
    limit: usize,
    current: usize,
    metadata_origin: M,
    data_origin: D,
}

#[derive(Clone)]
enum CacheEntry {
    Resolving(Arc<std::sync::RwLock<Option<Result<Bytes, io::ErrorKind>>>>),
    Resolved(Bytes),
}

impl CacheEntry {
    fn is_resolving(&self) -> bool {
        if let Self::Resolving(_) = self {
            true
        } else {
            false
        }
    }
}

impl<M, D> LruArchiveBackend<M, D> {
    pub fn new(metadata_origin: M, data_origin: D, limit: usize) -> Self {
        let cache = Arc::new(std::sync::Mutex::new(LruCache::unbounded()));

        Self {
            cache,
            limit,
            current: 0,
            metadata_origin,
            data_origin,
        }
    }

    fn limit_bytes(&self) -> usize {
        self.limit * 1024 * 1024
    }
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> LruArchiveBackend<M, D> {
    fn layer_fits_in_cache(&self, id: [u32; 5]) -> io::Result<bool> {
        let limit = self.limit_bytes();
        Ok(limit != 0 && self.layer_size(id)? as usize <= limit)
    }
}

fn ensure_additional_cache_space(cache: &mut LruCache<[u32; 5], CacheEntry>, mut required: usize) {
    if required == 0 {
        return;
    }

    loop {
        let peek = cache
            .peek_lru()
            .expect("cache is empty but stored entries were expected");
        if peek.1.is_resolving() {
            // this is a resolving entry, we don't want to pop it.
            let id = peek.0.clone();
            cache.promote(&id);
            continue;
        }
        // at this point the lru item is not resolving
        let entry = cache
            .pop_lru()
            .expect("cache is empty but stored entries were expected")
            .1;
        if let CacheEntry::Resolved(entry) = entry {
            if entry.len() >= required {
                // done!
                return;
            }

            // more needs to be popped
            required -= entry.len();
        } else {
            panic!("expected resolved entry but got a resolving");
        }
    }
}

fn ensure_enough_cache_space(
    cache: &mut LruCache<[u32; 5], CacheEntry>,
    limit: usize,
    current: usize,
    required: usize,
) -> bool {
    if required > limit {
        // this entry is too big for the cache
        return false;
    }

    let remaining = limit - current;
    if remaining < required {
        // we need to clean up some cache spacew to fit this entry
        ensure_additional_cache_space(cache, required - remaining);
    }

    true
}

fn drop_from_cache(cache: &mut LruCache<[u32; 5], CacheEntry>, id: [u32; 5]) {
    assert!(cache.contains(&id));
    cache.demote(&id);
    cache.pop_lru();
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> ArchiveBackend for LruArchiveBackend<M, D> {
    type Read = Either<BytesReader, D::Read>;
    fn get_layer_bytes(&self, id: [u32; 5]) -> io::Result<Bytes> {
        let mut cache = self.cache.lock();
        let cached = cache.get(&id).cloned();

        match cached {
            Some(CacheEntry::Resolved(bytes)) => Ok(bytes),
            Some(CacheEntry::Resolving(barrier)) => {
                // someone is already looking up this layer. we'll wait for them to be done.
                std::mem::drop(cache);
                let guard = barrier.read();
                match guard.as_ref().unwrap() {
                    Ok(bytes) => Ok(bytes.clone()),
                    Err(kind) => Err(io::Error::new(*kind, "layer resolve failed")),
                }
            }
            None => {
                // nobody is looking this up yet, it is up to us.
                let barrier = Arc::new(std::sync::RwLock::new(None));
                let mut result = barrier.write();
                cache.get_or_insert(id, || CacheEntry::Resolving(barrier.clone()));

                // drop the cache while doing the lookup
                std::mem::drop(cache);
                let lookup = self.data_origin.get_layer_bytes(id);

                *result = Some(lookup.as_ref().map_err(|e| e.kind()).cloned());

                // reacquire cache
                let mut cache = self.cache.lock();
                match lookup {
                    Ok(bytes) => {
                        if ensure_enough_cache_space(
                            &mut *cache,
                            self.limit_bytes(),
                            self.current,
                            bytes.len(),
                        ) {
                            let cached = cache
                                .get_mut(&id)
                                .expect("layer resolving entry not found in cache");
                            *cached = CacheEntry::Resolved(bytes.clone());
                        } else {
                            // this entry is uncachable. Just remove the resolving entry
                            drop_from_cache(&mut *cache, id);
                        }
                        Ok(bytes)
                    }
                    Err(e) => {
                        drop_from_cache(&mut *cache, id);

                        Err(e)
                    }
                }
            }
        }
    }
    fn get_layer_structure_bytes(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<Option<Bytes>> {
        if self.layer_fits_in_cache(id)? {
            let bytes = self.get_layer_bytes(id)?;
            let archive = Archive::parse(bytes);
            Ok(archive.slice_for(file_type))
        } else {
            self.data_origin
                .get_layer_structure_bytes(id, file_type)
                
        }
    }
    fn store_layer_file(&self, id: [u32; 5], bytes: Bytes) -> io::Result<()> {
        self.data_origin.store_layer_file(id, bytes.clone())?;

        let mut cache = self.cache.lock();
        cache.get_or_insert(id, move || CacheEntry::Resolved(bytes));

        Ok(())
    }
    fn read_layer_structure_bytes_from(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
        read_from: usize,
    ) -> io::Result<Self::Read> {
        if self.layer_fits_in_cache(id)? {
            let mut bytes = self
                .get_layer_structure_bytes(id, file_type)
                ?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "slice not found in archive")
                })?;
            bytes.advance(read_from);

            Ok(Either::Left(BytesReader(bytes)))
        } else {
            Ok(Either::Right(
                self.data_origin
                    .read_layer_structure_bytes_from(id, file_type, read_from)
                    ?,
            ))
        }
    }
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> ArchiveMetadataBackend
    for LruArchiveBackend<M, D>
{
    fn get_layer_names(&self) -> io::Result<Vec<[u32; 5]>> {
        self.metadata_origin.get_layer_names()
    }
    fn layer_exists(&self, id: [u32; 5]) -> io::Result<bool> {
        if let Some(CacheEntry::Resolved(_)) = self.cache.lock().peek(&id) {
            Ok(true)
        } else {
            self.metadata_origin.layer_exists(id)
        }
    }
    fn layer_size(&self, id: [u32; 5]) -> io::Result<u64> {
        if let Some(CacheEntry::Resolved(bytes)) = self.cache.lock().peek(&id) {
            Ok(bytes.len() as u64)
        } else {
            self.metadata_origin.layer_size(id)
        }
    }
    fn layer_file_exists(&self, id: [u32; 5], file_type: LayerFileEnum) -> io::Result<bool> {
        if let Some(CacheEntry::Resolved(bytes)) = self.cache.lock().peek(&id) {
            let header = ArchiveFilePresenceHeader::new(bytes.clone().get_u64());
            Ok(header.is_present(file_type))
        } else {
            self.metadata_origin.layer_file_exists(id, file_type)
        }
    }
    fn get_layer_structure_size(
        &self,
        id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> io::Result<usize> {
        if let Some(CacheEntry::Resolved(bytes)) = self.cache.lock().peek(&id) {
            let (header, _) = ArchiveHeader::parse(bytes.clone());

            if let Some(size) = header.size_of(file_type) {
                Ok(size)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "structure {file_type:?} not found in layer {}",
                        name_to_string(id)
                    ),
                ))
            }
        } else {
            self.metadata_origin
                .get_layer_structure_size(id, file_type)
                
        }
    }
    fn get_rollup(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        self.metadata_origin.get_rollup(id)
    }
    fn set_rollup(&self, id: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        self.metadata_origin.set_rollup(id, rollup)
    }

    fn get_parent(&self, id: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        if let Some(parent_bytes) = self
            .get_layer_structure_bytes(id, LayerFileEnum::Parent)
            ?
        {
            let parent_string = std::str::from_utf8(&parent_bytes[..40]).unwrap();
            Ok(Some(string_to_name(parent_string).unwrap()))
        } else {
            Ok(None)
        }
    }
}

pub enum ConstructionFileState {
    UnderConstruction(BytesMut),
    Finalizing,
    Finalized(Bytes),
}

#[derive(Clone)]
pub struct ConstructionFile(Arc<RwLock<ConstructionFileState>>);

impl ConstructionFile {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(
            ConstructionFileState::UnderConstruction(BytesMut::new()),
        )))
    }

    fn new_finalized(bytes: Bytes) -> Self {
        Self(Arc::new(RwLock::new(ConstructionFileState::Finalized(
            bytes,
        ))))
    }

    fn is_finalized(&self) -> bool {
        let guard = self.0.read().unwrap();
        if let ConstructionFileState::Finalized(_) = &*guard {
            true
        } else {
            false
        }
    }

    fn finalized_buf(self) -> Bytes {
        let guard = self.0.read().unwrap();
        if let ConstructionFileState::Finalized(bytes) = &*guard {
            bytes.clone()
        } else {
            panic!("tried to get the finalized buf from an unfinalized ConstructionFile");
        }
    }
}

impl FileStore for ConstructionFile {
    type Write = Self;
    fn open_write(&self) -> io::Result<Self::Write> {
        Ok(self.clone())
    }
}

impl std::io::Write for ConstructionFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.0.write().unwrap();
        match &mut *guard {
            ConstructionFileState::UnderConstruction(x) => {
                x.put_slice(buf);
                Ok(buf.len())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "file already written",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // noop
        Ok(())
    }
}

impl SyncableFile for ConstructionFile {
    fn sync_all(self) -> io::Result<()> {
        let mut guard = self.0.write().unwrap();
        let mut state = ConstructionFileState::Finalizing;
        std::mem::swap(&mut state, &mut *guard);

        match state {
            ConstructionFileState::UnderConstruction(x) => {
                let buf = x.freeze();
                *guard = ConstructionFileState::Finalized(buf);

                Ok(())
            }
            _ => {
                *guard = state;
                Err(io::Error::new(io::ErrorKind::Other, "file already written"))
            }
        }
    }
}

impl std::io::Read for ConstructionFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut guard = self.0.write().unwrap();
        match &mut *guard {
            ConstructionFileState::Finalized(x) => {
                let to_read = std::cmp::min(buf.len(), x.len());
                if to_read == 0 {
                    return Ok(0);
                }
                let slice = x.split_to(to_read);
                buf[..to_read].copy_from_slice(slice.as_ref());
                Ok(to_read)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "file not yet written",
            )),
        }
    }
}

impl FileLoad for ConstructionFile {
    type Read = Self;

    fn exists(&self) -> io::Result<bool> {
        let guard = self.0.read().unwrap();
        Ok(matches!(&*guard, ConstructionFileState::Finalized(_)))
    }
    fn size(&self) -> io::Result<usize> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(x) => Ok(x.len()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }

    fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(data) => {
                let mut data = data.clone();
                if data.len() < offset {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "offset is beyond end of file",
                    ))
                } else {
                    data.advance(offset);
                    // this is suspicious, why would we need a lock here? Maybe we should have a different reader type from the file type
                    Ok(ConstructionFile(Arc::new(RwLock::new(
                        ConstructionFileState::Finalized(data),
                    ))))
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }

    fn map(&self) -> io::Result<Bytes> {
        let guard = self.0.read().unwrap();
        match &*guard {
            ConstructionFileState::Finalized(x) => Ok(x.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file not finalized",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveFilePresenceHeader {
    present_files: SmallBitArray,
}

impl ArchiveFilePresenceHeader {
    pub fn new(val: u64) -> Self {
        Self {
            present_files: SmallBitArray::new(val),
        }
    }

    pub fn from_present<I: Iterator<Item = LayerFileEnum>>(present_files: I) -> Self {
        let mut val = 0;

        for file in present_files {
            val |= 1 << (u64::BITS - file as u32 - 1);
        }

        Self::new(val)
    }

    pub fn is_present(&self, file: LayerFileEnum) -> bool {
        self.present_files.get(file as usize)
    }

    pub fn inner(&self) -> u64 {
        self.present_files.inner()
    }

    pub fn file_index(&self, file: LayerFileEnum) -> Option<usize> {
        if !self.is_present(file) {
            return None;
        }

        Some(self.present_files.rank1(file as usize) - 1)
    }
}

#[derive(Debug, Clone)]
pub struct ArchiveHeader {
    file_presence: ArchiveFilePresenceHeader,
    file_offsets: MonotonicLogArray,
}

impl ArchiveHeader {
    pub fn parse(mut bytes: Bytes) -> (Self, Bytes) {
        let file_presence = ArchiveFilePresenceHeader::new(bytes.get_u64());
        let (file_offsets, remainder) = MonotonicLogArray::parse_header_first(bytes)
            .expect("unable to parse structure offsets");

        (
            Self {
                file_presence,
                file_offsets,
            },
            remainder,
        )
    }

    pub fn parse_from_reader<R: std::io::Read>(reader: &mut R) -> io::Result<Self> {
        let file_presence = ArchiveFilePresenceHeader::new(reader.read_u64()?);
        let mut logarray_bytes = BytesMut::new();
        logarray_bytes.resize(8, 0);
        reader.read_exact(&mut logarray_bytes[0..8])?;
        let len = logarray_length_from_control_word(&logarray_bytes[0..8]);
        logarray_bytes.reserve(len);
        unsafe {
            logarray_bytes.set_len(8 + len);
        }
        reader.read_exact(&mut logarray_bytes[8..])?;

        let (file_offsets, _) =
            MonotonicLogArray::parse_header_first(logarray_bytes.freeze()).expect("what the heck");

        Ok(Self {
            file_presence,
            file_offsets,
        })
    }

    pub fn range_for(&self, file: LayerFileEnum) -> Option<Range<usize>> {
        if let Some(file_index) = self.file_presence.file_index(file) {
            let start: usize = if file_index == 0 {
                0
            } else {
                self.file_offsets.entry(file_index - 1) as usize
            };

            let end: usize = self.file_offsets.entry(file_index) as usize;

            Some(start..end)
        } else {
            None
        }
    }

    pub fn size_of(&self, file: LayerFileEnum) -> Option<usize> {
        self.range_for(file).map(|range| range.end - range.start)
    }
}

pub struct Archive {
    pub header: ArchiveHeader,
    pub data: Bytes,
}

impl Archive {
    pub fn parse(bytes: Bytes) -> Self {
        let (header, data) = ArchiveHeader::parse(bytes);

        Self { header, data }
    }

    pub fn parse_from_reader<R: std::io::Read>(reader: &mut R) -> io::Result<Self> {
        let header = ArchiveHeader::parse_from_reader(reader)?;
        let data_len = header.file_offsets.entry(header.file_offsets.len() - 1) as usize;
        let mut data = BytesMut::with_capacity(data_len);
        data.reserve(data_len);
        unsafe { data.set_len(data_len) };
        reader.read_exact(&mut data[..])?;

        Ok(Self {
            header,
            data: data.freeze(),
        })
    }

    pub fn slice_for(&self, file: LayerFileEnum) -> Option<Bytes> {
        self.header
            .range_for(file)
            .map(|range| self.data.slice(range))
    }

    pub fn size_of(&self, file: LayerFileEnum) -> Option<usize> {
        self.header.size_of(file)
    }
}

#[derive(Clone)]
pub struct PersistentFileSlice<M, D> {
    metadata_backend: M,
    data_backend: D,
    layer_id: [u32; 5],
    file_type: LayerFileEnum,
}

impl<M, D> PersistentFileSlice<M, D> {
    fn new(
        metadata_backend: M,
        data_backend: D,
        layer_id: [u32; 5],
        file_type: LayerFileEnum,
    ) -> Self {
        Self {
            metadata_backend,
            data_backend,
            layer_id,
            file_type,
        }
    }
}

pub struct ArchiveSliceReader {
    file: File,
    remaining: usize,
}

impl ArchiveSliceReader {
    pub fn new(file: File, remaining: usize) -> Self {
        Self { file, remaining }
    }

    pub fn end_early(&mut self, end: usize) {
        self.remaining -= end;
    }
}

impl std::io::Read for ArchiveSliceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }

        let max_read = std::cmp::min(buf.len(), self.remaining);
        let n = self.file.read(&mut buf[..max_read])?;
        self.remaining -= n;
        Ok(n)
    }
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> FileLoad for PersistentFileSlice<M, D> {
    type Read = D::Read;

    fn exists(&self) -> io::Result<bool> {
        self.metadata_backend
            .layer_file_exists(self.layer_id, self.file_type)
            
    }

    fn size(&self) -> io::Result<usize> {
        self.metadata_backend
            .get_layer_structure_size(self.layer_id, self.file_type)
            
    }

    fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        self.data_backend
            .read_layer_structure_bytes_from(self.layer_id, self.file_type, offset)
            
    }

    fn map(&self) -> io::Result<Bytes> {
        self.data_backend
            .get_layer_structure_bytes(self.layer_id, self.file_type)
            ?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "slice not found in archive"))
    }

    fn map_if_exists(&self) -> io::Result<Option<Bytes>> {
        self.data_backend
            .get_layer_structure_bytes(self.layer_id, self.file_type)
            
    }
}

// This is some pretty ridiculous contrived logic but it saves having to refactor some other places which should just take a rollup id in the first place.
#[derive(Clone)]
pub struct ArchiveRollupFile<M> {
    layer_id: [u32; 5],
    metadata_backend: M,
}

impl<M: ArchiveMetadataBackend> FileLoad for ArchiveRollupFile<M> {
    type Read = BytesReader;

    fn exists(&self) -> io::Result<bool> {
        Ok(self
            .metadata_backend
            .get_rollup(self.layer_id)
            ?
            .is_some())
    }

    fn size(&self) -> io::Result<usize> {
        if self
            .metadata_backend
            .get_rollup(self.layer_id)
            ?
            .is_some()
        {
            Ok(std::mem::size_of::<[u32; 5]>() + 2)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "layer has no rollup",
            ))
        }
    }

    fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        let mut bytes = self.map()?;
        bytes.advance(offset);
        Ok(BytesReader(bytes))
    }

    fn map(&self) -> io::Result<Bytes> {
        let id = self.metadata_backend.get_rollup(self.layer_id)?;
        if let Some(id) = id {
            let mut bytes = Vec::with_capacity(42);
            bytes.extend_from_slice(b"1\n");
            bytes.extend_from_slice(name_to_string(id).as_bytes());
            Ok(bytes.into())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "layer has no rollup",
            ))
        }
    }
}

impl<M: ArchiveMetadataBackend> FileStore for ArchiveRollupFile<M> {
    type Write = ArchiveRollupFileWriter<M>;
    fn open_write(&self) -> io::Result<Self::Write> {
        Ok(ArchiveRollupFileWriter {
            layer_id: self.layer_id,
            data: BytesMut::new(),
            metadata_backend: self.metadata_backend.clone(),
        })
    }
}

pub struct ArchiveRollupFileWriter<M> {
    layer_id: [u32; 5],
    data: BytesMut,
    metadata_backend: M,
}

impl<M: ArchiveMetadataBackend> std::io::Write for ArchiveRollupFileWriter<M> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<M: ArchiveMetadataBackend> SyncableFile for ArchiveRollupFileWriter<M> {
    fn sync_all(self) -> io::Result<()> {
        let rollup_string =
            String::from_utf8(self.data.to_vec()).expect("rollup id was not a string");
        // first line of this string is going to be a version number. it should be discarded.
        let line = rollup_string.lines().skip(1).next().unwrap();
        let rollup_id = string_to_name(&line)?;

        self.metadata_backend
            .set_rollup(self.layer_id, rollup_id)
            
    }
}

#[derive(Clone)]
pub enum ArchiveLayerHandle<M, D> {
    Construction(ConstructionFile),
    Persistent(PersistentFileSlice<M, D>),
    Rollup(ArchiveRollupFile<M>),
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> FileStore for ArchiveLayerHandle<M, D> {
    type Write = ArchiveLayerHandleWriter<M>;
    fn open_write(&self) -> io::Result<Self::Write> {
        Ok(match self {
            Self::Construction(c) => ArchiveLayerHandleWriter::Construction(c.open_write()?),
            Self::Rollup(r) => ArchiveLayerHandleWriter::Rollup(r.open_write()?),
            _ => panic!("cannot write to a persistent file slice"),
        })
    }
}

impl<M: ArchiveMetadataBackend, D: ArchiveBackend> FileLoad for ArchiveLayerHandle<M, D> {
    type Read = ArchiveLayerHandleReader<D::Read, BytesReader>;

    fn exists(&self) -> io::Result<bool> {
        match self {
            Self::Construction(c) => c.exists(),
            Self::Persistent(p) => p.exists(),
            Self::Rollup(r) => r.exists(),
        }
    }
    fn size(&self) -> io::Result<usize> {
        match self {
            Self::Construction(c) => c.size(),
            Self::Persistent(p) => p.size(),
            Self::Rollup(r) => r.size(),
        }
    }

    fn open_read_from(&self, offset: usize) -> io::Result<Self::Read> {
        Ok(match self {
            Self::Construction(c) => {
                ArchiveLayerHandleReader::Construction(c.open_read_from(offset)?)
            }
            Self::Persistent(p) => {
                ArchiveLayerHandleReader::Persistent(p.open_read_from(offset)?)
            }
            Self::Rollup(r) => ArchiveLayerHandleReader::Rollup(r.open_read_from(offset)?),
        })
    }

    fn map(&self) -> io::Result<Bytes> {
        match self {
            Self::Construction(c) => c.map(),
            Self::Persistent(p) => p.map(),
            Self::Rollup(r) => r.map(),
        }
    }
}

pub enum ArchiveLayerHandleReader<P, R> {
    Construction(ConstructionFile),
    Persistent(P),
    Rollup(R),
}

impl<P: std::io::Read, R: std::io::Read> std::io::Read for ArchiveLayerHandleReader<P, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Construction(c) => c.read(buf),
            Self::Persistent(p) => p.read(buf),
            Self::Rollup(r) => r.read(buf),
        }
    }
}

pub enum ArchiveLayerHandleWriter<M> {
    Construction(ConstructionFile),
    Rollup(ArchiveRollupFileWriter<M>),
}

impl<M: ArchiveMetadataBackend> std::io::Write for ArchiveLayerHandleWriter<M> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Construction(c) => c.write(buf),
            Self::Rollup(r) => r.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Construction(c) => c.flush(),
            Self::Rollup(r) => r.flush(),
        }
    }
}

impl<M: ArchiveMetadataBackend> SyncableFile for ArchiveLayerHandleWriter<M> {
    fn sync_all(self) -> io::Result<()> {
        match self {
            Self::Construction(c) => c.sync_all(),
            Self::Rollup(r) => r.sync_all(),
        }
    }
}

type ArchiveLayerConstructionMap =
    Arc<RwLock<HashMap<[u32; 5], HashMap<LayerFileEnum, ConstructionFile>>>>;

#[derive(Clone)]
pub struct ArchiveLayerStore<M, D> {
    metadata_backend: M,
    data_backend: D,
    construction: ArchiveLayerConstructionMap,
}

impl<M, D> ArchiveLayerStore<M, D> {
    pub fn new(metadata_backend: M, data_backend: D) -> ArchiveLayerStore<M, D> {
        ArchiveLayerStore {
            metadata_backend,
            data_backend,
            construction: Default::default(),
        }
    }

    #[doc(hidden)]
    pub fn write_bytes(&self, name: [u32; 5], file: LayerFileEnum, bytes: Bytes) {
        let mut guard = self.construction.write().unwrap();
        if let Some(map) = guard.get_mut(&name) {
            if map.contains_key(&file) {
                panic!("tried to write bytes to an archive, but file is already open");
            }

            map.insert(file, ConstructionFile::new_finalized(bytes));
        } else {
            panic!("tried to write bytes to an archive, but layer is not under construction");
        }
    }
}

const PREFIX_DIR_SIZE: usize = 3;

impl<M: ArchiveMetadataBackend + 'static, D: ArchiveBackend + 'static> PersistentLayerStore
    for ArchiveLayerStore<M, D>
{
    type File = ArchiveLayerHandle<M, D>;

    fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut result = self.metadata_backend.get_layer_names()?;

        {
            let guard = self.construction.read().unwrap();

            for name in guard.keys() {
                result.push(*name);
            }
        }

        result.sort();
        result.dedup();

        Ok(result)
    }

    fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        if !self.metadata_backend.layer_exists(name)? {
            // layer does not exist yet on disk, good.
            let mut guard = self.construction.write().unwrap();
            if guard.contains_key(&name) {
                // whoops! Looks like layer is already under construction!
                panic!("tried to create a new layer which is already under construction");
            }

            // layer is neither on disk nor in the construction map. Let's create it.
            guard.insert(name, HashMap::new());
            return Ok(name);
        } else {
            // still here? That means the file existed, even though it shouldn't!
            panic!("tried to create a new layer which already exists");
        }
    }

    fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        {
            let guard = self.construction.read().unwrap();
            if guard.contains_key(&name) {
                return Ok(true);
            }
        }

        self.metadata_backend.layer_exists(name)
    }

    fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let file_type = FILENAME_ENUM_MAP[name];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming from disk, in its own file
            return Ok(ArchiveLayerHandle::Rollup(ArchiveRollupFile {
                layer_id: directory,
                metadata_backend: self.metadata_backend.clone(),
            }));
        }

        {
            let guard = self.construction.read().unwrap();
            if let Some(map) = guard.get(&directory) {
                if let Some(file) = map.get(&file_type) {
                    return Ok(ArchiveLayerHandle::Construction(file.clone()));
                }

                // the directory is there but the file is not. We'll have to construct it.
                std::mem::drop(guard);
                let mut guard = self.construction.write().unwrap();
                let map = guard.get_mut(&directory).unwrap();
                let file = ConstructionFile::new();
                map.insert(file_type, file.clone());

                Ok(ArchiveLayerHandle::Construction(file))
            } else {
                // layer does not appear to be under construction so it has to be in persistent storage
                Ok(ArchiveLayerHandle::Persistent(PersistentFileSlice::new(
                    self.metadata_backend.clone(),
                    self.data_backend.clone(),
                    directory,
                    file_type,
                )))
            }
        }
    }

    fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let file_type = FILENAME_ENUM_MAP[file];
        if file_type == LayerFileEnum::Rollup {
            // special case! This is always coming out of the persistent metadata
            return Ok(self.metadata_backend.get_rollup(directory)?.is_some());
        }

        {
            let guard = self.construction.read().unwrap();
            if let Some(map) = guard.get(&directory) {
                return Ok(map.contains_key(&file_type));
            }
        }

        self.metadata_backend
            .layer_file_exists(directory, file_type)
            
    }

    fn finalize(&self, directory: [u32; 5]) -> io::Result<()> {
        let files = {
            let mut guard = self.construction.write().unwrap();
            guard
                .remove(&directory)
                .expect("layer to be finalized was not found in construction map")
        };

        let mut files: Vec<(_, _)> = files
            .into_iter()
            .filter(|(_file_type, file)| file.is_finalized())
            .map(|(file_type, file)| (file_type, file.finalized_buf()))
            .collect();
        files.sort();
        let presence_header =
            ArchiveFilePresenceHeader::from_present(files.iter().map(|(t, _)| t).cloned());

        let mut offsets = LateLogArrayBufBuilder::new(BytesMut::new());
        let mut tally = 0;
        for (_file_type, data) in files.iter() {
            tally += data.len();
            offsets.push(tally as u64);
        }

        let offsets_buf = offsets.finalize_header_first();

        let mut data_buf = BytesMut::with_capacity(tally + 8 + offsets_buf.len());
        data_buf.put_u64(presence_header.inner());
        data_buf.extend(offsets_buf);
        for (_file_type, data) in files {
            data_buf.extend(data);
        }

        self.data_backend
            .store_layer_file(directory, data_buf.freeze())
            
    }

    fn layer_parent(&self, name: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        self.metadata_backend.get_parent(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_use_header() {
        let files_present = vec![
            LayerFileEnum::PredicateDictionaryBlocks,
            LayerFileEnum::NegObjects,
            LayerFileEnum::NodeValueIdMapBits,
            LayerFileEnum::Parent,
            LayerFileEnum::Rollup,
            LayerFileEnum::NodeDictionaryBlocks,
        ];

        let header = ArchiveFilePresenceHeader::from_present(files_present.iter().cloned());

        for file in files_present {
            assert!(header.is_present(file));
        }

        assert!(!header.is_present(LayerFileEnum::NodeDictionaryOffsets));
    }
}
