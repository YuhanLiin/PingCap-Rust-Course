use crate::Result;
use serde::{Deserialize, Serialize};
use serde_cbor::{from_reader, to_writer};
use std::io::prelude::*;

#[allow(missing_docs)]
pub const GET: &str = "get";
#[allow(missing_docs)]
pub const SET: &str = "set";
#[allow(missing_docs)]
pub const REMOVE: &str = "remove";

/// Representation of a message sent over TCP between server and client
/// Transmitted over the network in the form of CBOR messages
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", content = "c")]
pub enum Message {
    /// List of strings used to represent commands and return values
    #[serde(rename = "a")]
    Array(Vec<String>),
    #[serde(rename = "e")]
    /// Error message inidicating failure
    Error(String),
}

impl Message {
    /// Serialize a message from a Reader
    pub fn read(reader: impl Read) -> Result<Self> {
        Ok(from_reader(reader)?)
    }

    /// Deserialize and send the message to a Writer
    pub fn write(&self, writer: impl Write) -> Result<()> {
        to_writer(writer, &self)?;
        Ok(())
    }
}
