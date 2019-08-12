#![deny(missing_docs)]
//! Implements an in-memory key-value storage system.
use evmap;
use failure::{Error, Fail};
use log::error;
use serde::{Deserialize, Serialize};
use serde_cbor::{to_writer, Deserializer};
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fs::{read_dir, remove_file, rename, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Custom Result type used for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Client for sending KVSEngine requests
pub mod client;
/// Network protocol for communicating between server and client
pub mod protocol;
/// Server for handling KVSEngine requests
pub mod server;
/// Defines ThreadPool trait and implementation for concurrent KVS engine
pub mod thread_pool;

/// Error thrown by remove() when the key does not exist
#[derive(Debug, Fail)]
#[fail(display = "Key not found")]
pub struct KeyNotFound;

/// Error thrown by remove() when the key does not exist
#[derive(Debug, Fail)]
#[fail(display = "File data corrupted")]
pub struct CorruptData;

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
    fn new((start, end): (u64, u64)) -> Self {
        Self { start, end }
    }

    fn len(&self) -> u64 {
        self.end - self.start
    }
}

/// Interface for key-value store backend
pub trait KvsEngine: Clone + Send + 'static {
    /// Maps a key in the storage to a specific value.
    /// Overwrites previous value if the key already exists.
    /// ```
    /// use kvs::Result;
    ///
    /// # fn main() -> Result<()> {
    ///     use kvs::{KvsEngine, KvStore};
    ///     use tempfile::TempDir;
    ///
    ///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///     let kv = KvStore::open(temp_dir.path())?;
    ///     kv.set("key".to_owned(), "1".to_owned())?;
    ///     kv.set("key".to_owned(), "2".to_owned())?;
    ///     assert_eq!(kv.get("key".to_owned())?, Some("2".to_owned()));
    /// #   Ok(())
    /// # }
    /// ```
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Returns a copy of the value mapped to a given key if it exists.
    /// Otherwise, return None.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Removes a key and its value from the storage.
    /// Does nothing if the key is not present in the storage.
    /// ```
    /// use kvs::Result;
    ///
    /// # fn main() -> Result<()> {
    ///     use kvs::{KvsEngine, KvStore};
    ///     use tempfile::TempDir;
    ///
    ///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    ///     let kv = KvStore::open(temp_dir.path())?;
    ///     kv.set("key".to_owned(), "1".to_owned())?;
    ///     kv.remove("key".to_owned())?;
    ///     assert_eq!(kv.get("key".to_owned())?, None);
    /// #   Ok(())
    /// # }
    /// ```
    fn remove(&self, key: String) -> Result<()>;

    /// Remove all keys and values and clears underlying disc space
    fn clear(&self) -> Result<()>;
}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// Key-value store for storing strings.
/// ```
/// use kvs::Result;
///
/// # fn main() -> Result<()> {
///     use tempfile::TempDir;
///     use kvs::{KvsEngine, KvStore};
///
///     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
///     let kv = KvStore::open(temp_dir.path())?;
///     kv.set("a".to_owned(), "b".to_owned())?;
///     assert_eq!(kv.get("a".to_owned())?, Some("b".to_owned()));
/// #   Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct KvStore {
    reader: KvsReader,
    writer: Arc<Mutex<KvsWriter>>,
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.reader.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }

    fn clear(&self) -> Result<()> {
        self.writer.lock().unwrap().clear()
    }
}

impl KvStore {
    /// Loads the in-memory index of the storage from a file to construct a KvStore
    pub fn open(dir: &Path) -> Result<Self> {
        // Get the existing KVS log file with the largest generation, if it exists
        let gen = all_log_files(&dir, None)?
            .iter()
            .filter_map(|path| {
                path.file_name()
                    .and_then(std::ffi::OsStr::to_str)
                    .filter(|name| name.starts_with("kvs_"))
                    .and_then(|name| name.rsplit("_").next())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max();
        let gen = gen.unwrap_or(0);
        let log_path = log_path(&dir, gen);

        let (index_r, index_w) = evmap::with_meta(gen);
        let dir = Arc::new(dir.to_owned());
        let writer = BufWriter::new(open_write().create(true).open(&log_path)?);
        let reader = BufReader::new(open_read().open(&log_path)?);

        let mut writer = KvsWriter {
            dir: dir.clone(),
            index: index_w,
            stale_bytes: 0,
            writer,
            reader,
        };

        let reader = KvsReader {
            dir: dir.clone(),
            index: index_r,
            reader: RefCell::new((None, gen)),
        };

        writer.build_index()?;

        Ok(Self {
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(&format!("kvs_{}.cbor", gen))
}

fn compacted_log_path(dir: &Path) -> PathBuf {
    dir.join("kvs_compact.cbor")
}

fn open_read() -> OpenOptions {
    let mut opt = OpenOptions::new();
    opt.read(true);
    opt
}

fn open_write() -> OpenOptions {
    let mut opt = OpenOptions::new();
    opt.append(true);
    opt
}

fn all_log_files(dir: &Path, preserve_gen: Option<u64>) -> Result<Vec<PathBuf>> {
    read_dir(dir)?
        .map(|entry| {
            let entry = entry?;
            let path = entry.path();

            if entry.metadata()?.is_file() {
                let (name, stem) = (path.file_name(), path.file_stem());
                if let (Some(stem), Some(name)) = (stem, name) {
                    if stem == "cbor" {
                        // Wipe out every cbor file except the one that maps to the generation we want
                        // to keep
                        let useless = if let Some(gen) = preserve_gen {
                            name != &format!("kvs_{}", gen)[..]
                        } else {
                            true
                        };

                        if useless {
                            return Ok(Some(path));
                        }
                    }
                }
            }

            Ok(None)
        })
        .filter_map(Result::transpose)
        .collect()
}

// There will only ever be one writer for every KvStore
struct KvsWriter {
    dir: Arc<PathBuf>,
    writer: BufWriter<File>,
    reader: BufReader<File>,
    index: evmap::WriteHandle<String, (u64, u64), u64>,
    stale_bytes: u64,
}

impl KvsWriter {
    // This is only ever called from open(), so we don't need to worry about synchronization
    fn build_index(&mut self) -> Result<()> {
        // Read from beginning
        let mut start = self.reader.seek(SeekFrom::Start(0))?;
        let mut index: HashMap<_, Range> = HashMap::new();

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
                    if let Some(old) = index.get(&key) {
                        self.stale_bytes += old.len();
                    }
                    index.insert(key, Range::new((start, end)));
                }
                Command::Remove { key } => {
                    match index.get(&key) {
                        None => {
                            error!(
                                "Data corrupted, as remove was found in file before set for key {}",
                                key
                            );
                            return Err(CorruptData.into());
                        }
                        Some(old) => self.stale_bytes += old.len(),
                    }
                    index.remove(&key);
                }
            };

            start = end;
        }

        self.index
            .extend(index.into_iter().map(|(k, r)| (k, (r.start, r.end))));
        self.index.refresh();

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let value = self.index.get_and(&key, |v| Range::new(v[0]));

        if let Some(value) = value {
            let cmd = Command::Remove { key };

            to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            // Remove key from index AFTER committing the command to disc.
            // We can use this order for remove and set because the file changes for those
            // operations are additive, so file updates won't mess up concurrent reads.
            self.index.empty(cmd.key().clone());
            self.index.refresh();
            self.stale_bytes += value.len();

            if self.stale_bytes > COMPACTION_THRESHOLD {
                self.compaction()?;
            }
            Ok(())
        } else {
            Err(KeyNotFound.into())
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };

        // Get the offset of the next command
        let start = self.writer.seek(SeekFrom::End(0))?;
        // Write to file
        to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let end = self.writer.seek(SeekFrom::End(0))?;

        let key = cmd.key();
        // Update stale_bytes if necessary
        if let Some(old) = self.index.get_and(&key, |v| Range::new(v[0])) {
            self.stale_bytes += old.len();
        }
        // Insert the offset into the index
        self.index.update(key, (start, end));
        self.index.refresh();

        if self.stale_bytes > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    // Might cause read failures, but will guarantee removal of all files
    fn clear(&mut self) -> Result<()> {
        let gen = self.index.meta().unwrap();

        // Perform cleaup
        for file in all_log_files(&self.dir, Some(gen))? {
            if let Err(err) = remove_file(&file) {
                error!(
                    "Failed to remove {} during compaction: {}",
                    file.display(),
                    err
                );
            }
        }
        // Truncate current log file
        self.writer.get_mut().set_len(0)?;

        // Update index and generation
        self.index.purge();
        self.index.set_meta(gen);
        self.index.refresh();
        self.stale_bytes = 0;
        Ok(())
    }

    fn compaction(&mut self) -> Result<()> {
        let compact_path = compacted_log_path(&self.dir);
        let mut compact_file = BufWriter::new(open_write().create_new(true).open(&compact_path)?);

        // The following operations modify multiple object state, and failure at any point must
        // guarantee a consistent object state (reader, writer, index all refer to same file).
        // Also, even on a panic the disc data we care about must not be corrupted.

        let mut new_offsets = Vec::with_capacity(self.index.len());
        // Use our index to figure out what data is fresh
        let index: Vec<_> = self.index.map_into(|k, v| (k.to_owned(), Range::new(v[0])));
        for (key, offset) in index {
            self.reader.seek(SeekFrom::Start(offset.start))?;

            let mut buf = Vec::with_capacity(offset.len() as usize);
            buf.resize(offset.len() as usize, 0);
            self.reader.read_exact(&mut buf)?;

            let new_offset = compact_file.seek(SeekFrom::Current(0))?;
            compact_file.write_all(&mut buf)?;
            // Update new index with offsets in the new file
            new_offsets.push((key, (new_offset, new_offset + offset.len())));
        }

        let new_gen = self.index.meta().unwrap() + 1;
        let new_log_path = log_path(&self.dir, new_gen);

        // Do compact file writes and renames first, since failing those operations don't affect
        // our current readers and writer.
        compact_file.flush()?;
        rename(&compact_path, &new_log_path)?;

        // Next create file handles to the new compacted files. If this fails we fall back to using
        // the uncompacted file.
        let writer = open_write().open(&new_log_path)?;
        let reader = open_read().open(&new_log_path)?;

        // Finally we do the infallible mutations, including index and generation updates.
        self.writer = BufWriter::new(writer);
        self.reader = BufReader::new(reader);
        self.stale_bytes = 0;

        self.index.set_meta(new_gen);
        for (k, o) in new_offsets {
            self.index.update(k, o);
        }
        self.index.refresh();

        // On Windows removing files still open by reader will fail, so we don't worry too much
        // about it
        for file in all_log_files(&self.dir, Some(new_gen))? {
            if let Err(err) = remove_file(&file) {
                error!(
                    "Failed to remove {} during compaction: {}",
                    file.display(),
                    err
                );
            }
        }

        Ok(())
    }
}

// There can be multiple readers running concurrently with one writer
struct KvsReader {
    dir: Arc<PathBuf>,
    reader: RefCell<(Option<BufReader<File>>, u64)>,
    index: evmap::ReadHandle<String, (u64, u64), u64>,
}

impl KvsReader {
    fn get(&self, key: String) -> Result<Option<String>> {
        let (offset, current_gen) = self.index.meta_get_and(&key, |v| Range::new(v[0])).unwrap();

        let (mut reader, mut gen) = RefMut::map_split(self.reader.borrow_mut(), |(r, g)| (r, g));
        if current_gen > *gen || reader.is_none() {
            *reader = Some(BufReader::new(
                open_read().open(&log_path(&self.dir, current_gen))?,
            ));
            *gen = current_gen;
        }
        let mut reader = reader.as_mut().unwrap();

        if let Some(offset) = offset {
            reader.seek(SeekFrom::Start(offset.start))?;
            let mut de = Deserializer::from_reader(&mut reader);
            let cmd: Command = serde::de::Deserialize::deserialize(&mut de).expect("bad offset");
            Ok(Some(cmd.value()))
        } else {
            Ok(None)
        }
    }
}

impl Clone for KvsReader {
    fn clone(&self) -> Self {
        Self {
            reader: RefCell::new((None, 0)),
            dir: self.dir.clone(),
            index: self.index.clone(),
        }
    }
}

/// KvsEngine wrapper around sled DB engine
#[derive(Clone)]
pub struct SledKvsEngine(sled::Db);

impl SledKvsEngine {
    /// Creates or loads sled database at specified path using default configuration
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self(sled::Db::start_default(path)?))
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let out = self.0.get(&key).map(|s| {
            s.as_ref()
                .map(|s| String::from_utf8(s.to_vec()).expect("non-string in sled DB"))
        })?;
        Ok(out)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.0.set(&key, value.into_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        self.0.del(&key)?.ok_or(KeyNotFound)?;
        self.0.flush()?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.0.clear()?;
        Ok(())
    }
}
