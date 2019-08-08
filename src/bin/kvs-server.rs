use failure::{ensure, format_err};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{protocol, KvStore, KvsEngine, Result};
use log::info;
use std::convert::{TryFrom, TryInto};
use std::env::current_dir;
use std::fs;
use std::io::ErrorKind;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use stderrlog;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs-server")]
struct Args {
    #[structopt(long = "addr")]
    addr: Option<SocketAddr>,
    #[structopt(long = "engine")]
    engine: Option<String>,
}

struct Config {
    addr: SocketAddr,
    engine: String,
}

impl TryFrom<Args> for Config {
    type Error = failure::Error;

    fn try_from(args: Args) -> Result<Config> {
        let addr = args.addr.unwrap_or("127.0.0.1:4000".parse().unwrap());

        // To remember which engine we used previously, we keep the engine name in a text file and
        // read it later to match.
        let engine_file = current_dir()?.join("engine.txt");
        let engine = match args.engine {
            Some(engine) => match open_existing_file(&engine_file)? {
                Some(old_engine) => {
                    ensure!(
                        old_engine == engine,
                        "should use engine {}, which exists in this directory",
                        old_engine
                    );
                    engine
                }
                None => {
                    fs::write(&engine_file, &engine)?;
                    engine
                }
            },
            None => match open_existing_file(&engine_file)? {
                Some(old_engine) => old_engine,
                None => {
                    fs::write(&engine_file, "kvs")?;
                    "kvs".to_owned()
                }
            },
        };

        Ok(Config { addr, engine })
    }
}

fn open_existing_file(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(None),
            _ => Err(err.into()),
        },
    }
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let config: Config = args.try_into()?;

    stderrlog::new()
        .module(module_path!())
        .verbosity(3)
        .init()?;

    info!("Version {}", env!("CARGO_PKG_VERSION"));
    info!("Engine: {}", config.engine);
    info!("Socket Address: {}", config.addr);

    run_server(&config)
}

fn run_server(config: &Config) -> Result<()> {
    let listener = TcpListener::bind(&config.addr)?;
    info!("Bind to {}", config.addr);
    let store = KvStore::open(&std::env::current_dir()?)?;
    let thread_pool = SharedQueueThreadPool::new(20)?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut store = store.clone();

        thread_pool.spawn(move || {
            let msg = protocol::Message::read(&mut stream).expect("message read error");
            info!("Finished reading request from stream");

            let resp = match handle_request(msg, &mut store) {
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
fn handle_request(
    msg: protocol::Message,
    store: &mut impl KvsEngine,
) -> Result<Option<Vec<String>>> {
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

fn check_len(arr: &[String], expected: usize) -> Result<()> {
    ensure!(
        arr.len() == expected,
        "server received {} args, expected {}",
        arr.len(),
        expected
    );
    Ok(())
}
