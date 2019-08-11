use crate::protocol::*;
use crate::thread_pool::ThreadPool;
use crate::Result;
use crossbeam::sync::WaitGroup;
use failure::{ensure, format_err};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

/// Client that sends TCP requests to KVS server.
/// Holds the TCP stream for its entire lifetime.
pub struct KvsClient {
    addr: SocketAddr,
}

impl KvsClient {
    /// Create a new client on an address
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    fn send(&self, req: Message) -> Result<Message> {
        let mut stream = TcpStream::connect(&self.addr)?;
        req.write(&mut stream)?;
        // Need to shutdown write socket so server read socket gets dropped, otherwise server read
        // never finishes
        stream.shutdown(Shutdown::Write)?;
        Ok(Message::read(&mut stream)?)
    }

    /// Send a SET request to the server
    pub fn set(&self, key: String, value: String) -> Result<()> {
        let req = Message::Array(vec![SET.to_owned(), key, value]);

        match self.send(req)? {
            Message::Error(err) => Err(format_err!("Error: {}", err)),
            _ => Ok(()),
        }
    }

    /// Send a GET request to the server. Key may not exist
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let req = Message::Array(vec![GET.to_owned(), key]);

        match self.send(req)? {
            Message::Error(err) => Err(format_err!("Error: {}", err)),
            Message::Array(arr) => {
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
    pub fn remove(&self, key: String) -> Result<()> {
        let req = Message::Array(vec![REMOVE.to_owned(), key]);

        match self.send(req)? {
            Message::Error(err) => Err(format_err!("Error: {}", err)),
            _ => Ok(()),
        }
    }
}

/// Uses a threadpool to send multiple set or get requests
pub struct ThreadedKvsClient<P: ThreadPool> {
    addr: SocketAddr,
    pool: P,
}

impl<P: ThreadPool> ThreadedKvsClient<P> {
    /// Create a new client on an address
    pub fn new(addr: SocketAddr, threads: u32) -> Result<Self> {
        Ok(Self {
            addr,
            pool: P::new(threads)?,
        })
    }

    /// Set multiple key-value pairs concurrently. Blocks until all requests are done and returns
    /// Error is any operations failed.
    pub fn set(&self, kv_pairs: impl Iterator<Item = (String, String)>) -> Result<()> {
        let wg = WaitGroup::new();
        let result = Arc::new(Mutex::new(Ok(())));

        for (key, val) in kv_pairs {
            let result = Arc::clone(&result);
            let addr = self.addr.clone();
            let wg = wg.clone();

            // Instead of panicking, all errors are sent to the outer result so we can track them
            // from the main thread
            self.pool.spawn(move || {
                let client = KvsClient::new(addr);

                if let Err(err) = client.set(key, val) {
                    // Only panicks if result mutex is poisoned, which should never happen
                    *result.lock().unwrap() = Err(err);
                };

                drop(wg);
            })
        }

        // Once we get here all the spawned jobs should be done
        wg.wait();

        let mut result = result.lock().unwrap();
        std::mem::replace(&mut *result, Ok(()))
    }

    /// Get multiple keys concurrently. Blocks until all requests are done and returns Error is any
    /// operations failed. Instead of returning the values the method takes a fallible handler
    /// closure that processes each retrieved value concurrently.
    pub fn get(
        &self,
        keys: impl Iterator<Item = String>,
        // Call for each retrieved value in the threads. Must not panic.
        handler: impl Fn(Option<String>) -> Result<()> + Send + 'static + Clone,
    ) -> Result<()> {
        let wg = WaitGroup::new();
        let result = Arc::new(Mutex::new(Ok(())));

        for key in keys {
            let result = Arc::clone(&result);
            let addr = self.addr.clone();
            let wg = wg.clone();
            let handler = handler.clone();

            // Again, no panicking
            self.pool.spawn(move || {
                let client = KvsClient::new(addr);

                let handler_result = (|| {
                    let val = client.get(key)?;
                    handler(val)
                })();

                if let Err(err) = handler_result {
                    *result.lock().unwrap() = Err(err);
                }

                drop(wg);
            });
        }

        wg.wait();

        let mut result = result.lock().unwrap();
        std::mem::replace(&mut *result, Ok(()))
    }
}
