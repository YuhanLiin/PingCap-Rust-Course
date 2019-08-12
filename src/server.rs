use crate::protocol::*;
use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, Result};
use crossbeam::channel::{bounded, Receiver, Sender};
use crossbeam::sync::WaitGroup;
use failure::{ensure, format_err};
use log::{info, warn};
use std::io::{BufReader, BufWriter, ErrorKind, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

/// Handles TCP KVSEngine requests. Can specify underlying threadpool and KVS engine.
pub struct KvsServer<E: KvsEngine, P: ThreadPool + Send + Sync + 'static> {
    engine: E,
    pool: Arc<P>,
    receiver: Receiver<()>,
    sender: Sender<()>,
}

// Derive clone is not working properly, so we have to write this manually
impl<E: KvsEngine, P: ThreadPool + Send + Sync + 'static> Clone for KvsServer<E, P> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            pool: self.pool.clone(),
            receiver: self.receiver.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<E: KvsEngine, P: ThreadPool + Send + Sync + 'static> KvsServer<E, P> {
    /// Instantiates threadpools and specifies underlying engine
    pub fn new(engine: E, num_threads: u32) -> Result<Self> {
        let (sender, receiver) = bounded(1);

        Ok(Self {
            engine,
            pool: Arc::new(P::new(num_threads)?),
            sender,
            receiver,
        })
    }

    /// Shutdown a server running on the specified address
    pub fn shutdown(&self, addr: &SocketAddr) -> Result<()> {
        info!("Send server shutdown signal at {}", addr);
        self.sender.send(())?;

        // Server could still be waiting for connections, so send one to unblock it
        match TcpStream::connect(addr) {
            Err(err) => match err.kind() {
                // Server could have already shutdown after the cancellation signal, in which case
                // we receive one of these connection errors that we ignore
                ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable => (),
                _ => return Err(err.into()),
            },
            _ => (),
        };

        Ok(())
    }

    /// Runs the server in an infinte loop to handle incoming requests. Can be cancelled by sending
    /// message to the receiver.
    pub fn run(&self, addr: &SocketAddr, bind_event: Option<WaitGroup>) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("Bind to {}", addr);
        // Signal that binding has completed and that we can start connecting
        bind_event.map(|event| drop(event));

        for stream in listener.incoming() {
            // A disconnect error should never happen, since this method borrows the server, which
            // owns the sender half of the channel. Thus, we simply stop the server when we receive
            // a message.
            if self.receiver.try_recv().is_ok() {
                info!("Shutdown server at {}", addr);
                break;
            }

            let stream = stream?;
            let store = self.engine.clone();
            let pool = Arc::clone(&self.pool);

            self.pool.spawn(move || {
                let mut writer = BufWriter::new(stream.try_clone().expect("stream clone fail"));
                let mut reader = BufReader::new(stream);

                let len = {
                    let mut len_buf = [0];
                    reader.read_exact(&mut len_buf).expect("length read error");
                    len_buf[0]
                };

                if len == 0 {
                    warn!("Batch FAILED with invalid length of 0");
                    Message::Error("invalid batch length of 0".to_owned())
                        .write(&mut writer)
                        .unwrap();
                    return;
                }
                info!("{} requests incoming", len);

                // Wrap reader and writer in mutexes so they can be sent to other threads.
                // Need mutex protection around buffered writer/readers so we don't write/read
                // garbage data from multiple threads.
                let writer = Arc::new(Mutex::new(writer));
                let reader = Arc::new(Mutex::new(reader));

                for i in 0..len {
                    // Inexpensive Arc clones
                    let writer = Arc::clone(&writer);
                    let reader = Arc::clone(&reader);
                    let mut store = E::clone(&store);

                    pool.spawn(move || {
                        let msg = Message::read(&mut *reader.lock().unwrap())
                            .expect("message read error");
                        info!("Finished reading request {} from stream", i);

                        let resp = match Self::handle_request(msg, &mut store) {
                            Ok(value) => {
                                info!("Request SUCCESS, reply: {}", value.join(" "));
                                Message::Array(value)
                            }
                            Err(err) => {
                                let err = err.as_fail().to_string();
                                warn!("Request FAILED, reply: {}", err);
                                Message::Error(err)
                            }
                        };

                        resp.write(&mut *writer.lock().unwrap())
                            .expect("message write error");
                        info!("Finished writing response to stream");
                    });
                }
            });
        }

        Ok(())
    }

    // Get returns [key, value] or [key] if value is not found when successful
    // Set and Remove return [key] when successful
    fn handle_request(msg: Message, store: &mut E) -> Result<Vec<String>> {
        match msg {
            Message::Array(arr) => {
                info!("Received TCP args: {}", arr.join(" "));

                match arr.get(0).map(|s| &s[..]) {
                    Some(GET) => {
                        check_len(&arr, 2)?;
                        let key = arr[1].to_owned();
                        // If value does not exist, return empty list
                        Ok(store
                            .get(key.clone())?
                            .map(|val| vec![key.clone(), val])
                            .unwrap_or(vec![key]))
                    }

                    Some(SET) => {
                        check_len(&arr, 3)?;
                        let (key, value) = (&arr[1], &arr[2]);
                        store.set(key.to_owned(), value.to_owned())?;
                        Ok(vec![key.to_owned()])
                    }

                    Some(REMOVE) => {
                        check_len(&arr, 2)?;
                        let key = &arr[1];
                        store.remove(key.to_owned())?;
                        Ok(vec![key.to_owned()])
                    }

                    _ => Err(format_err!("invalid incoming message")),
                }
            }
            Message::Error(err) => Err(format_err!("received error message {}", err)),
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
