use crate::protocol::*;
use crate::thread_pool::ThreadPool;
use crate::Result;
use crossbeam::sync::WaitGroup;
use failure::{ensure, format_err};
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::iter::ExactSizeIterator;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

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
        kv_pairs: impl ExactSizeIterator<Item = (String, String)>,
    ) -> Result<impl Iterator<Item = Result<String>> + 'a> {
        let batch_size = kv_pairs.len();
        self.write_length(batch_size as u8)?;

        for (key, value) in kv_pairs {
            self.set_write(key, value)?;
        }
        self.finish_writing()?;

        Ok((0..batch_size).map(move |_| self.read_key()))
    }

    /// Send a GET request to the server. Key may not exist
    pub fn get<'a>(
        mut self,
        keys: impl ExactSizeIterator<Item = String>,
    ) -> Result<impl Iterator<Item = Result<(String, Option<String>)>> + 'a> {
        let batch_size = keys.len();
        self.write_length(batch_size as u8)?;

        for key in keys {
            self.get_write(key)?;
        }
        self.finish_writing()?;

        Ok((0..batch_size).map(move |_| self.read_pair()))
    }

    /// Send a REMOVE request to the server
    pub fn remove<'a>(
        mut self,
        keys: impl ExactSizeIterator<Item = String>,
    ) -> Result<impl Iterator<Item = Result<String>> + 'a> {
        let batch_size = keys.len();
        self.write_length(batch_size as u8)?;

        for key in keys {
            self.remove_write(key)?;
        }
        self.finish_writing()?;

        Ok((0..batch_size).map(move |_| self.read_key()))
    }
}

/// Uses a threadpool to send multiple set or get requests
pub struct ThreadedKvsClient<P: ThreadPool> {
    addr: SocketAddr,
    pool: P,
    threads: u32,
}

impl<P: ThreadPool> ThreadedKvsClient<P> {
    /// Create a new client on an address
    pub fn new(addr: SocketAddr, threads: u32) -> Result<Self> {
        Ok(Self {
            addr,
            pool: P::new(threads)?,
            threads,
        })
    }

    // Returns amount of requests to be batched in each thread
    fn divide_work(&self, num_requests: usize) -> Vec<usize> {
        let threads = self.threads as usize;
        // Don't worry about overflow for now
        let per_thread = num_requests / threads;
        let mut remainder = num_requests % threads;

        (0..threads)
            .map(|_| {
                if remainder > 0 {
                    remainder -= 1;
                    per_thread + 1
                } else {
                    per_thread
                }
            })
            .take_while(|n| *n > 0)
            .collect()
    }

    /// Set multiple key-value pairs concurrently. Blocks until all requests are done and returns
    /// Error is any operations failed.
    pub fn set(&self, kv_pairs: Vec<(String, String)>) -> Result<()> {
        let wg = WaitGroup::new();
        let result = Arc::new(Mutex::new(Ok(())));

        let distribution = self.divide_work(kv_pairs.len());
        assert_eq!(distribution.iter().sum::<usize>(), kv_pairs.len());
        let mut kv_pairs = kv_pairs.into_iter();

        for batch_size in distribution {
            let batch: Vec<_> = kv_pairs.by_ref().take(batch_size).collect();
            let result = Arc::clone(&result);
            let wg = wg.clone();
            let addr = self.addr.clone();

            // Instead of panicking, all errors are sent to the outer result so we can track them
            // from the main thread
            self.pool.spawn(move || {
                let res = (|| {
                    let client = KvsClient::new(&addr)?;
                    client.set(batch.into_iter())
                })();

                match res {
                    Err(err) => *result.lock().unwrap() = Err(err),
                    Ok(response) => {
                        // If we get any error responses, it's an error
                        if let Some(err) = response.into_iter().filter_map(Result::err).next() {
                            *result.lock().unwrap() = Err(err);
                        }
                    }
                }

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
        keys: Vec<String>,
        // Call for each retrieved value in the threads. Must not panic.
        handler: impl Fn((String, Option<String>)) -> Result<()> + Send + 'static + Clone,
    ) -> Result<()> {
        let wg = WaitGroup::new();
        let result = Arc::new(Mutex::new(Ok(())));

        let distribution = self.divide_work(keys.len());
        assert_eq!(distribution.iter().sum::<usize>(), keys.len());
        let mut keys = keys.into_iter();

        for batch_size in distribution {
            let batch: Vec<_> = keys.by_ref().take(batch_size).collect();
            let result = Arc::clone(&result);
            let wg = wg.clone();
            let addr = self.addr.clone();
            let mut handler = handler.clone();

            // Again, no panicking
            self.pool.spawn(move || {
                let handler_result = (|| {
                    let client = KvsClient::new(&addr)?;
                    client.get(batch.into_iter())
                })();

                match handler_result {
                    Err(err) => *result.lock().unwrap() = Err(err),
                    Ok(response) => {
                        if let Some(err) = response
                            .into_iter()
                            .map(|r| r.and_then(&mut handler))
                            .filter_map(Result::err)
                            .next()
                        {
                            *result.lock().unwrap() = Err(err);
                        }
                    }
                }

                drop(wg);
            });
        }

        wg.wait();

        let mut result = result.lock().unwrap();
        std::mem::replace(&mut *result, Ok(()))
    }
}
