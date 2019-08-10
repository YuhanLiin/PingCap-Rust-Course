use crate::thread_pool::ThreadPool;
use crate::{protocol, KvsEngine, Result};
use failure::{ensure, format_err};
use log::info;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};

/// Handles TCP KVSEngine requests. Can specify underlying threadpool and KVS engine.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Instantiates threadpools and specifies underlying engine
    pub fn new(engine: E, num_threads: u32) -> Result<Self> {
        Ok(Self {
            engine,
            pool: P::new(num_threads)?,
        })
    }

    /// Runs the server request handling loop to handle incoming requests
    pub fn run(&mut self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("Bind to {}", addr);

        for stream in listener.incoming() {
            let mut stream = stream?;
            let mut store = self.engine.clone();

            self.pool.spawn(move || {
                let msg = protocol::Message::read(&mut stream).expect("message read error");
                info!("Finished reading request from stream");

                let resp = match Self::handle_request(msg, &mut store) {
                    Ok(Some(value)) => {
                        info!("Request SUCCESS, reply: {}", value.join(" "));
                        protocol::Message::Array(value)
                    }
                    Ok(None) => {
                        info!("Request SUCCESS, reply null");
                        protocol::Message::Null
                    }
                    Err(err) => {
                        let err = err.as_fail().to_string();
                        info!("Request FAILED, reply: {}", err);
                        protocol::Message::Error(err)
                    }
                };
                resp.write(&mut stream).expect("message write error");
                stream.flush().expect("stream flush error");
                info!("Finished writing response to stream");
            });
        }

        Ok(())
    }

    // Get returns a single element list or Null if element is not found
    // Set and Remove return empty list, indicating lack of stdout client-side
    fn handle_request(msg: protocol::Message, store: &mut E) -> Result<Option<Vec<String>>> {
        match msg {
            protocol::Message::Array(arr) => {
                info!("Received TCP args: {}", arr.join(" "));

                match arr.get(0).map(|s| &s[..]) {
                    Some(protocol::GET) => {
                        check_len(&arr, 2)?;
                        let key = &arr[1];
                        // If value does not exist, return None
                        Ok(store.get(key.to_owned())?.map(|val| vec![val]))
                    }

                    Some(protocol::SET) => {
                        check_len(&arr, 3)?;
                        let (key, value) = (&arr[1], &arr[2]);
                        store.set(key.to_owned(), value.to_owned())?;
                        Ok(Some(Vec::new()))
                    }

                    Some(protocol::REMOVE) => {
                        check_len(&arr, 2)?;
                        let key = &arr[1];
                        store.remove(key.to_owned())?;
                        Ok(Some(Vec::new()))
                    }

                    _ => Err(format_err!("invalid incoming message")),
                }
            }
            protocol::Message::Error(err) => Err(format_err!("received error message {}", err)),
            protocol::Message::Null => Err(format_err!("received null")),
        }
    }
}
fn check_len(arr: &[String], expected: usize) -> Result<()> {
    ensure!(
        arr.len() == expected,
        "server received {} args, expected {}",
        arr.len(),
        expected
    );
    Ok(())
}
