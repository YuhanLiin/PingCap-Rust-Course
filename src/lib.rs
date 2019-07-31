#![deny(missing_docs)]
//! Implements an in-memory key-value storage system.
use failure::Error;
use serde::{Deserialize, Serialize};
use serde_cbor::{to_writer, Deserializer};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::path::Path;

/// Custom Result type used for KvStore operations.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

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
    file: File,
}

impl KvStore {
    /// Loads the in-memory index of the storage from a file to construct a KvStore
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path.join("kvs.cbor"))?;
        Ok(Self { file })
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
        to_writer(&mut self.file, &cmd)?;
        Ok(())
    }

    fn build_index(&mut self) -> Result<HashMap<String, String>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut map = HashMap::new();
        let de = Deserializer::from_reader(&self.file);
        for cmd in de.into_iter() {
            dbg!(&cmd);
            match cmd? {
                Command::Set { key, value } => map.insert(key, value),
                Command::Remove { key } => map.remove(&key),
            };
        }
        Ok(map)
    }

    /// Returns a copy of the value mapped to a given key if it exists.
    /// Otherwise, return None.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut map = self.build_index()?;
        Ok(map.remove(&key))
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
            Ok(())
        } else {
            Err(failure::err_msg("Key not found"))
        }
    }
}
