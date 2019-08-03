use failure::{ensure, format_err};
use kvs::{protocol, KvStore, Result};
use log::info;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
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

impl From<Args> for Config {
    fn from(args: Args) -> Config {
        let addr = args.addr.unwrap_or("127.0.0.1:4000".parse().unwrap());
        // TODO have actual validation
        let engine = args.engine.unwrap_or("kvs".to_owned());

        Config { addr, engine }
    }
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let config: Config = args.into();

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
    let mut store = KvStore::open(&std::env::current_dir()?)?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        let msg = protocol::Message::read(&mut stream)?;
        info!("Finished reading request from stream");

        let resp = match handle_request(msg, &mut store) {
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
        resp.write(&mut stream)?;
        stream.flush()?;
        info!("Finished writing response to stream");
    }
    Ok(())
}

fn handle_request(msg: protocol::Message, store: &mut KvStore) -> Result<Vec<String>> {
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
                        .unwrap_or(Vec::new()))
                }

                Some(protocol::SET) => {
                    check_len(&arr, 3)?;
                    let (key, value) = (&arr[1], &arr[2]);
                    store.set(key.to_owned(), value.to_owned())?;
                    Ok(vec!["Ok".to_owned()])
                }

                Some(protocol::REMOVE) => {
                    check_len(&arr, 2)?;
                    let key = &arr[1];
                    store.remove(key.to_owned())?;
                    Ok(vec!["Ok".to_owned()])
                }

                _ => Err(format_err!("invalid incoming message")),
            }
        }
        protocol::Message::Error(err) => Err(format_err!("received error message {}", err)),
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
