#![deny(missing_docs)]
//! Implements an in-memory key-value storage system.
use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use serde_cbor::{to_writer, Deserializer};
use std::collections::HashMap;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, ErrorKind, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Custom Result type used for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error thrown by remove() when the key does not exist
#[derive(Debug, Fail)]
#[fail(display = "Key not found")]
pub struct KeyNotFound;

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn value(self) -> String {
        match self {
            Command::Set { value, .. } => value,
            _ => panic!("Expected Set command"),
        }
    }

    fn key(self) -> String {
        match self {
            Command::Set { key, .. } => key,
            Command::Remove { key } => key,
        }
    }
}

// Exclusive range with len method for u64, unlike the one from std
#[derive(Debug, Clone)]
struct Range {
    start: u64,
    end: u64,
}

impl Range {
    fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    fn len(&self) -> u64 {
        self.end - self.start
    }
}

/// Interface for key-value store backend
pub trait KvsEngine {}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// Key-value store for storing strings.
/// ```
/// use kvs::Result;
///
/// # fn main() -> Result<()> {
///     use tempfile::TempDir;
///     use kvs::KvStore;
///
///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
///     let mut kv = KvStore::open(temp_dir.path())?;
///     kv.set("a".to_owned(), "b".to_owned())?;
///     assert_eq!(kv.get("a".to_owned())?, Some("b".to_owned()));
/// #   Ok(())
/// # }
/// ```
pub struct KvStore {
    // Directory where the data file is stored.
    dir: PathBuf,
    // Read and write handles to the data file. Must always point to kvs.cbor file in self.dir.
    reader: BufReader<File>,
    writer: BufWriter<File>,
    // Mapping between key to log file offset to make lookups faster
    index: HashMap<String, Range>,
    // Number of bytes taken up by stale commands
    stale_bytes: u64,
}

impl KvStore {
    fn log_path(dir: &Path) -> PathBuf {
        dir.join("kvs.cbor")
    }

    fn compacted_log_path(dir: &Path) -> PathBuf {
        dir.join("kvs_compact.cbor")
    }

    /// Loads the in-memory index of the storage from a file to construct a KvStore
    pub fn open(dir: &Path) -> Result<Self> {
        let log_path = Self::log_path(dir);
        let compact_path = Self::compacted_log_path(dir);

        // Delete any kvs_compact files if it exists, since it's the result of a failed compaction
        if let Err(error) = remove_file(&compact_path) {
            match error.kind() {
                ErrorKind::NotFound => (),
                _ => return Err(error.into()),
            }
        };

        let write_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_path)?;
        let read_file = OpenOptions::new().read(true).open(&log_path)?;

        let mut out = Self {
            reader: BufReader::new(read_file),
            writer: BufWriter::new(write_file),
            index: HashMap::new(),
            dir: dir.to_owned(),
            stale_bytes: 0,
        };
        out.build_index()?;
        Ok(out)
    }

    fn compaction(&mut self) -> Result<()> {
        let log_path = Self::log_path(&self.dir);
        let compact_path = Self::compacted_log_path(&self.dir);
        let mut compact_file = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(&compact_path)?,
        );

        // The following operations modify multiple object state, and failure at any point must
        // guarantee a consistent object state (reader, writer, index all refer to same file).
        // Also, even on a panic the disc data we care about must not be corrupted.

        let mut new_index = self.index.clone();
        // Use our index to figure out what data is fresh
        for (key, offset) in self.index.iter() {
            self.reader.seek(SeekFrom::Start(offset.start))?;
            let mut de = Deserializer::from_reader(&mut self.reader);
            let cmd: Command = serde::de::Deserialize::deserialize(&mut de).expect("bad offset");

            let new_offset = compact_file.seek(SeekFrom::Current(0))?;
            to_writer(&mut compact_file, &cmd)?;
            // Update new index with offsets in the new file
            *new_index.get_mut(key).unwrap() = Range::new(new_offset, new_offset + offset.len());
        }

        // Replace current active log with compacted log
        rename(&compact_path, &log_path)?;

        *self = Self {
            writer: BufWriter::new(OpenOptions::new().append(true).open(&log_path)?),
            reader: BufReader::new(OpenOptions::new().read(true).open(&log_path)?),
            index: new_index,
            dir: self.dir.clone(),
            stale_bytes: 0,
        };
        // Uphold invariant that file handles should always point to latest kvs.cbor file.
        Ok(())
    }

    /// Maps a key in the storage to a specific value.
    /// Overwrites previous value if the key already exists.
    /// ```
    /// use kvs::Result;
    ///
    /// # fn main() -> Result<()> {
    ///     use kvs::KvStore;
    ///     use tempfile::TempDir;
    ///
    ///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///     let mut kv = KvStore::open(temp_dir.path())?;
    ///     kv.set("key".to_owned(), "1".to_owned())?;
    ///     kv.set("key".to_owned(), "2".to_owned())?;
    ///     assert_eq!(kv.get("key".to_owned())?, Some("2".to_owned()));
    /// #   Ok(())
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };

        // Get the offset of the next command
        let start = self.writer.seek(SeekFrom::End(0))?;
        to_writer(&mut self.writer, &cmd)?;
        let end = self.writer.seek(SeekFrom::End(0))?;

        // Insert the offset into the index
        if self
            .index
            .insert(cmd.key(), Range::new(start, end))
            .is_some()
        {
            self.stale_bytes += end - start;
        }
        self.writer.flush()?;

        if self.stale_bytes > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    fn build_index(&mut self) -> Result<()> {
        // Read from beginning
        let mut start = self.reader.seek(SeekFrom::Start(0))?;
        self.index = HashMap::new();

        // Check if EOF has been reached
        while !self.reader.fill_buf()?.is_empty() {
            // For some reason calling byte_offset() on CBOR deserializers does not work for
            // files, so we have to get log offsets using seek() instead.
            // Deserialize command manually
            let mut de = Deserializer::from_reader(&mut self.reader);
            let cmd = serde::de::Deserialize::deserialize(&mut de)?;
            let end = self.reader.seek(SeekFrom::Current(0))?;

            match cmd {
                Command::Set { key, .. } => {
                    if let Some(old_offset) = self.index.insert(key, Range::new(start, end)) {
                        self.stale_bytes += old_offset.len();
                    }
                }
                Command::Remove { key } => {
                    self.stale_bytes +=
                        self.index.remove(&key).expect("removed not in index").len();
                }
            };

            start = end;
        }

        Ok(())
    }

    /// Returns a copy of the value mapped to a given key if it exists.
    /// Otherwise, return None.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.index.get(&key).cloned() {
            self.reader.seek(SeekFrom::Start(offset.start))?;
            let mut de = Deserializer::from_reader(&mut self.reader);
            let cmd: Command = serde::de::Deserialize::deserialize(&mut de).expect("bad offset");
            Ok(Some(cmd.value()))
        } else {
            Ok(None)
        }
    }

    /// Removes a key and its value from the storage.
    /// Does nothing if the key is not present in the storage.
    /// ```
    /// use kvs::Result;
    ///
    /// # fn main() -> Result<()> {
    ///     use kvs::KvStore;
    ///     use tempfile::TempDir;
    ///
    ///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///     let mut kv = KvStore::open(temp_dir.path())?;
    ///     kv.set("key".to_owned(), "1".to_owned())?;
    ///     kv.remove("key".to_owned())?;
    ///     assert_eq!(kv.get("key".to_owned())?, None);
    /// #   Ok(())
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove { key };
            to_writer(&mut self.writer, &cmd)?;
            // Remove key from index AFTER committing the command to disc
            self.stale_bytes += self
                .index
                .remove(&cmd.key())
                .expect("removed not in index")
                .len();
            Ok(())
        } else {
            Err(KeyNotFound.into())
        }
    }
}
