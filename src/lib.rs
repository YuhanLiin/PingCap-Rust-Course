#![deny(missing_docs)]
//! Implements an in-memory key-value storage system.
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::Path;

#[derive(Debug)]
struct CustomErr;

impl fmt::Display for CustomErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        panic!();
    }
}

impl Error for CustomErr {}

/// Custom Result type used for KvStore operations.
pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

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
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// Creates an empty KvStore
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Loads the in-memory index of the storage from a file to construct a KvStore
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::new())
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
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.0.insert(key, val);
        Ok(())
    }

    /// Returns a copy of the value mapped to a given key if it exists.
    /// Otherwise, return None.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.0.get(&key).cloned())
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
        self.0.remove(&key);
        Ok(())
    }
}
