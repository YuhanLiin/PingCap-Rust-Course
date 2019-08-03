use failure::{ensure, format_err};
use kvs::{protocol, Result};
use log::info;
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

    info!("Version {}", env!("CARGO_PKG_VERSION"));
    info!("Engine: {}", config.engine);
    info!("Socket Address: {}", config.addr);

    stderrlog::new()
        .module(module_path!())
        .verbosity(3)
        .init()?;

    run_server(&config)
}

fn run_server(config: &Config) -> Result<()> {
    let listener = TcpListener::bind(&config.addr)?;
    info!("Bind to {}", config.addr);
    for stream in listener.incoming() {
        let mut stream = stream?;
        let msg = protocol::Message::read(&mut stream)?;

        let resp = match handle_request(msg) {
            Ok(value) => {
                info!("Request SUCCESS, reply: {}", value);
                protocol::Message::Array(vec![value])
            }
            Err(err) => {
                let err = err.as_fail().to_string();
                info!("Request FAILED, reply: {}", err);
                protocol::Message::Error(err)
            }
        };
        resp.write(&mut stream)?;
    }
    Ok(())
}

fn handle_request(msg: protocol::Message) -> Result<String> {
    match msg {
        protocol::Message::Array(mut arr) => {
            info!("Received TCP args: {}", arr.join(" "));
            ensure!(
                arr.len() == 2,
                "server received {} args, expected 2",
                arr.len()
            );

            let key = arr.pop();
            // TODO Handle other stuff
            Ok("placeholder".to_owned())
        }
        protocol::Message::Error(err) => {
            Err(format_err!("unexpected received error message: {}", err))
        }
    }
}
