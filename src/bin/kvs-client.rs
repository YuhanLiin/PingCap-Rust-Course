use kvs::KeyNotFound;
use kvs::KvStore;
use kvs::Result;
use std::env::current_dir;
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs-client")]
struct Args {
    #[structopt(subcommand)]
    sub: SubCommand,
    #[structopt(long = "addr")]
    addr: Option<SocketAddr>,
}

#[derive(StructOpt)]
enum SubCommand {
    #[structopt(name = "get")]
    Get { key: String },

    #[structopt(name = "set")]
    Set { key: String, value: String },

    #[structopt(name = "rm")]
    Remove { key: String },
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let mut kvs = KvStore::open(&current_dir()?)?;

    match args.sub {
        SubCommand::Get { key } => match kvs.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        SubCommand::Set { key, value } => kvs.set(key, value)?,
        SubCommand::Remove { key } => {
            let res = kvs.remove(key);
            if let Err(err) = res {
                match err.downcast::<KeyNotFound>() {
                    Ok(e) => {
                        println!("{}", e);
                        std::process::exit(1);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    };

    Ok(())
}
