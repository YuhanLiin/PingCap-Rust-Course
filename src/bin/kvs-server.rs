use failure::ensure;
use kvs::server::KvsServer;
use kvs::thread_pool::SharedQueueThreadPool;
use kvs::{KvStore, Result, SledKvsEngine};
use log::info;
use std::convert::{TryFrom, TryInto};
use std::env::current_dir;
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;
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
    threads: u32,
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
                    ensure!(
                        engine == "kvs" || engine == "sled",
                        "engine should be \"kvs\" or \"sled\""
                    );
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

        // Use magic number 20 for thread count
        Ok(Config {
            addr,
            engine,
            threads: 20,
        })
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

    match &config.engine[..] {
        "kvs" => KvsServer::<_, SharedQueueThreadPool>::new(
            KvStore::open(&current_dir()?)?,
            config.threads,
        )?
        .run(&config.addr),
        "sled" => KvsServer::<_, SharedQueueThreadPool>::new(
            SledKvsEngine::open(&current_dir()?)?,
            config.threads,
        )?
        .run(&config.addr),
        _ => unreachable!(),
    }
}
