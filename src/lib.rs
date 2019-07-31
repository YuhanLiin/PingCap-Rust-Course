#![deny(missing_docs)]
//! Implements an in-memory key-value storage system.
use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use serde_cbor::{to_writer, Deserializer};
use std::collections::HashMap;
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom};
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
            Command::Set { key: _, value } => value,
            _ => panic!("Expected Set command"),
        }
    }

    fn key(self) -> String {
        match self {
            Command::Set { key, value: _ } => key,
            Command::Remove { key } => key,
        }
    }
}

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// Key-value store for storing strings.
/// rust ```
///
/// # fn main() {
///     use kvs::KvStore;
///     let mut kv = KvStore::new();
///     kv.set("a".to_owned(), "b".to_owned());
///     assert_eq!(kv.get("a".to_owned()), Some("b".to_owned()));
/// # }
/// ```
pub struct KvStore {
    dir: PathBuf,
    file: File,
    index: Option<HashMap<String, u64>>,
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

        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(&log_path)?;

        Ok(Self {
            file,
            index: None,
            dir: dir.to_owned(),
        })
    }

    fn compaction(&mut self) -> Result<()> {
        let log_path = Self::log_path(&self.dir);
        let compact_path = Self::compacted_log_path(&self.dir);
        let index = self.build_index()?;
        let mut compact_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create_new(true)
            .open(&compact_path)?;

        let offsets = index.values().map(|o| *o).collect::<Vec<_>>();
        // Use our updated index to figure out what data is fresh
        for offset in offsets {
            self.file.seek(SeekFrom::Start(offset))?;
            let mut de = Deserializer::from_reader(&self.file);
            let cmd: Command = serde::de::Deserialize::deserialize(&mut de).expect("bad offset");
            to_writer(&mut compact_file, &cmd)?;
        }

        // Don't risk an outdated index
        self.index = None;
        // Replace current active log with compacted log
        rename(&compact_path, &log_path)?;
        // Uphold invariant that self.file should always point to latest kvs.cbor file
        self.file = OpenOptions::new().append(true).read(true).open(&log_path)?;
        Ok(())
    }

    /// Maps a key in the storage to a specific value.
    /// Overwrites previous value if the key already exists.
    /// rust ```
    /// # fn main() {
    ///     use kvs::KvStore;
    ///     let mut kv = KvStore::new();
    ///     kv.set("key".to_owned(), "1".to_owned());
    ///     kv.set("key".to_owned(), "2".to_owned());
    ///     assert_eq!(kv.get("key".to_owned()), Some("2".to_owned()));
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        // Get the offset of the next command
        let end = self.file.seek(SeekFrom::End(0))?;

        if let Some(index) = &mut self.index {
            to_writer(&mut self.file, &cmd)?;
            // Insert the offset into the index
            index.insert(cmd.key(), end);
        } else {
            to_writer(&mut self.file, &cmd)?;
        }

        if end > COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    fn build_index(&mut self) -> Result<&mut HashMap<String, u64>> {
        if let None = self.index {
            // Read from beginning
            self.file.seek(SeekFrom::Start(0))?;

            let mut cloned_file = self.file.try_clone()?;
            let mut cmds = Deserializer::from_reader(&self.file).into_iter();
            let mut map = HashMap::new();

            loop {
                // Get current offset. Doesn't actually change file offset
                let offset = cloned_file.seek(SeekFrom::Current(0))?;

                if let Some(cmd) = cmds.next() {
                    match cmd? {
                        Command::Set { key, value: _ } => map.insert(key, offset),
                        Command::Remove { key } => map.remove(&key),
                    };
                } else {
                    break;
                }
            }

            self.index = Some(map);
        }

        Ok(self.index.as_mut().unwrap())
    }

    /// Returns a copy of the value mapped to a given key if it exists.
    /// Otherwise, return None.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let map = self.build_index()?;

        if let Some(offset) = map.get(&key).map(|i| *i) {
            self.file.seek(SeekFrom::Start(offset))?;
            let mut de = Deserializer::from_reader(&self.file);
            let cmd: Command = serde::de::Deserialize::deserialize(&mut de).expect("bad offset");
            Ok(Some(cmd.value()))
        } else {
            Ok(None)
        }
    }

    /// Removes a key and its value from the storage.
    /// Does nothing if the key is not present in the storage.
    /// rust ```
    /// # fn main() {
    ///     use kvs::KvStore;
    ///     let mut kv = KvStore::new();
    ///     kv.set("key".to_owned(), "1".to_owned());
    ///     kv.remove("key".to_owned());
    ///     assert_eq!(kv.get("key".to_owned()), None);
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let map = self.build_index()?;

        if map.contains_key(&key) {
            let cmd = Command::Remove { key };
            to_writer(&mut self.file, &cmd)?;
            // Remove key from index AFTER committing the command to disc
            self.index.as_mut().unwrap().remove(&cmd.key());
            Ok(())
        } else {
            Err(KeyNotFound.into())
        }
    }
}
