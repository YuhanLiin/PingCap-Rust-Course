use crate::protocol::*;
//use crate::thread_pool::ThreadPool;
use crate::Result;
//use crossbeam::sync::WaitGroup;
use failure::{ensure, format_err};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::net::{Shutdown, SocketAddr, TcpStream};
//use std::sync::{Arc, Mutex};

/// Client that sends TCP requests to KVS server.
/// Holds the TCP stream for its entire lifetime.
pub struct KvsClient {
    // These should point to same address
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Create a new client on an address
    pub fn new(addr: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let stream_clone = stream.try_clone()?;

        Ok(Self {
            reader: BufReader::new(stream),
            writer: BufWriter::new(stream_clone),
        })
    }

    fn set_write(&mut self, key: String, value: String) -> Result<()> {
        let req = Message::Array(vec![SET.to_owned(), key, value]);
        req.write(&mut self.writer)?;
        Ok(())
    }

    fn read_key(&mut self) -> Result<String> {
        let res = Message::read(&mut self.reader)?;

        match res {
            Message::Error(err) => Err(format_err!("Error: {}", err)),
            Message::Array(mut arr) => {
                ensure!(
                    arr.len() == 1,
                    "unexpected server output: {}",
                    arr.join(" ")
                );

                Ok(arr.remove(0))
            }
        }
    }

    fn get_write(&mut self, key: String) -> Result<()> {
        let req = Message::Array(vec![GET.to_owned(), key]);
        req.write(&mut self.writer)?;
        Ok(())
    }

    fn read_pair(&mut self) -> Result<(String, Option<String>)> {
        let res = Message::read(&mut self.reader)?;

        match res {
            Message::Error(err) => Err(format_err!("Error: {}", err)),
            // Return value format for GET is [key] or [key, value]
            Message::Array(mut arr) => {
                ensure!(
                    arr.len() == 1 || arr.len() == 2,
                    "unexpected server output: {}",
                    arr.join(" ")
                );

                let key = arr.remove(0);
                Ok((key, arr.pop()))
            }
        }
    }

    fn remove_write(&mut self, key: String) -> Result<()> {
        let req = Message::Array(vec![REMOVE.to_owned(), key]);
        req.write(&mut self.writer)?;
        Ok(())
    }

    // Length is restricted to a single byte
    // Write this to the start of every stream to tell server how many requests we are sending
    fn write_length(&mut self, len: u8) -> Result<()> {
        self.writer.write_all(&[len])?;
        Ok(())
    }

    // Need to call this after all writes are done so that server actually receives data
    // Once this is called we can no longer write to this client
    fn finish_writing(&mut self) -> Result<()> {
        self.writer.flush()?;
        // Need to shutdown write socket so server read socket gets dropped, otherwise server read
        // never finishes. Once shutdown is done, the socket can no longer be used to write.
        self.writer.get_ref().shutdown(Shutdown::Write)?;
        Ok(())
    }

    /// Send a SET request to the server
    pub fn set<'a>(
        mut self,
        key: String,
        value: String,
        batch_size: u8,
    ) -> Result<impl Iterator<Item = Result<String>> + 'a> {
        self.write_length(batch_size)?;
        self.set_write(key, value)?;
        self.finish_writing()?;
        Ok((0..batch_size).map(move |_| self.read_key()))
    }

    /// Send a GET request to the server. Key may not exist
    pub fn get<'a>(
        mut self,
        key: String,
        batch_size: u8,
    ) -> Result<impl Iterator<Item = Result<(String, Option<String>)>> + 'a> {
        self.write_length(batch_size)?;
        self.get_write(key)?;
        self.finish_writing()?;
        Ok((0..batch_size).map(move |_| self.read_pair()))
    }

    /// Send a REMOVE request to the server
    pub fn remove<'a>(
        mut self,
        key: String,
        batch_size: u8,
    ) -> Result<impl Iterator<Item = Result<String>> + 'a> {
        self.write_length(batch_size)?;
        self.remove_write(key)?;
        self.finish_writing()?;
        Ok((0..batch_size).map(move |_| self.read_key()))
    }
}
/*
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
*/
