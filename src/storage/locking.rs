#![allow(unused)]
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::*;

/// A simple file wrapper for read access (replaces the async LockedFile).
/// File locking is removed since fs2 is no longer a dependency.
#[derive(Debug)]
pub struct LockedFile {
    file: std::fs::File,
}

impl LockedFile {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(LockedFile { file })
    }

    pub fn try_open<P: AsRef<Path>>(path: P) -> io::Result<Option<Self>> {
        match Self::open(path) {
            Ok(f) => Ok(Some(f)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    pub fn create_and_open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        match Self::try_open(path)? {
            Some(file) => Ok(file),
            None => {
                let _file = std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(path)?;
                Self::open(path)
            }
        }
    }
}

impl Read for LockedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

/// A simple file wrapper for exclusive read/write access (replaces the async ExclusiveLockedFile).
pub struct ExclusiveLockedFile {
    file: std::fs::File,
}

impl ExclusiveLockedFile {
    pub fn create_and_open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(false)
            .write(true)
            .open(path)?;
        Ok(ExclusiveLockedFile { file })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        Ok(ExclusiveLockedFile { file })
    }

    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        let pos = self.file.seek(SeekFrom::Current(0))?;
        self.file.set_len(pos)
    }

    pub fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

impl Read for ExclusiveLockedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Write for ExclusiveLockedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
