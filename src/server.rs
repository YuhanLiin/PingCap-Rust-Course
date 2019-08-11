use crate::thread_pool::ThreadPool;
use crate::{protocol, KvsEngine, Result};
use crossbeam::channel::{bounded, Receiver, Sender};
use crossbeam::sync::WaitGroup;
use failure::{ensure, format_err};
use log::info;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};

/// Handles TCP KVSEngine requests. Can specify underlying threadpool and KVS engine.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    receiver: Receiver<()>,
    sender: Sender<()>,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Instantiates threadpools and specifies underlying engine
    pub fn new(engine: E, num_threads: u32) -> Result<Self> {
        let (sender, receiver) = bounded(1);

        Ok(Self {
            engine,
            pool: P::new(num_threads)?,
            sender,
            receiver,
        })
    }

    /// Shutdown a server running on the specified address
    pub fn shutdown(&self, addr: &SocketAddr) -> Result<()> {
        info!("Send server shutdown signal at {}", addr);
        self.sender.send(())?;

        // Server could still be waiting for connections, so send one to unblock it
        let mut stream = TcpStream::connect(addr)?;
        stream.write(&[0])?;
        stream.flush()?;
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

            let mut stream = stream?;
            let mut store = self.engine.clone();

            self.pool.spawn(move || {
                let msg = protocol::Message::read(&mut stream).expect("message read error");
                info!("Finished reading request from stream");

                let resp = match Self::handle_request(msg, &mut store) {
                    Ok(value) => {
                        info!("Request SUCCESS, reply: {}", value.join(" "));
                        protocol::Message::Array(value)
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

    // Get returns a single element list or empty list if element is not found
    // Set and Remove also return empty lists
    fn handle_request(msg: protocol::Message, store: &mut E) -> Result<Vec<String>> {
        match msg {
            protocol::Message::Array(arr) => {
                info!("Received TCP args: {}", arr.join(" "));

                match arr.get(0).map(|s| &s[..]) {
                    Some(protocol::GET) => {
                        check_len(&arr, 2)?;
                        let key = &arr[1];
                        // If value does not exist, return empty list
                        Ok(store
                            .get(key.to_owned())?
                            .map(|val| vec![val])
                            .unwrap_or_default())
                    }

                    Some(protocol::SET) => {
                        check_len(&arr, 3)?;
                        let (key, value) = (&arr[1], &arr[2]);
                        store.set(key.to_owned(), value.to_owned())?;
                        Ok(Vec::new())
                    }

                    Some(protocol::REMOVE) => {
                        check_len(&arr, 2)?;
                        let key = &arr[1];
                        store.remove(key.to_owned())?;
                        Ok(Vec::new())
                    }

                    _ => Err(format_err!("invalid incoming message")),
                }
            }
            protocol::Message::Error(err) => Err(format_err!("received error message {}", err)),
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
