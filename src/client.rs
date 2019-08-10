use crate::protocol;
use crate::Result;
use failure::{ensure, format_err};
use std::net::{Shutdown, SocketAddr, TcpStream};

/// Client that sends TCP requests to KVS server.
/// Holds the TCP stream for its entire lifetime.
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Create a new client on an address
    pub fn new(addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr)?,
        })
    }

    fn send(&mut self, req: protocol::Message) -> Result<protocol::Message> {
        req.write(&mut self.stream)?;
        // Need to shutdown write socket so server read socket gets dropped, otherwise server read
        // never finishes
        self.stream.shutdown(Shutdown::Write)?;
        Ok(protocol::Message::read(&mut self.stream)?)
    }

    /// Send a SET request to the server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = protocol::Message::Array(vec![protocol::SET.to_owned(), key, value]);

        match self.send(req)? {
            protocol::Message::Error(err) => Err(format_err!("Error: {}", err)),
            _ => Ok(()),
        }
    }

    /// Send a GET request to the server. Key may not exist
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = protocol::Message::Array(vec![protocol::GET.to_owned(), key]);

        match self.send(req)? {
            protocol::Message::Error(err) => Err(format_err!("Error: {}", err)),
            protocol::Message::Array(arr) => {
                if arr.is_empty() {
                    Ok(None)
                } else {
                    ensure!(
                        arr.len() == 1,
                        "unexpected server output: {}",
                        arr.join(" ")
                    );
                    Ok(Some(arr[0].to_owned()))
                }
            }
        }
    }

    /// Send a REMOVE request to the server
    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = protocol::Message::Array(vec![protocol::REMOVE.to_owned(), key]);

        match self.send(req)? {
            protocol::Message::Error(err) => Err(format_err!("Error: {}", err)),
            _ => Ok(()),
        }
    }
}
