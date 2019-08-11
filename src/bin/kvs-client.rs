use kvs::client::KvsClient;
use kvs::Result;
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt)]
enum Args {
    #[structopt(name = "get")]
    Get {
        key: String,
        #[structopt(name = "addr", long = "addr")]
        addr: Option<SocketAddr>,
    },

    #[structopt(name = "set")]
    Set {
        key: String,
        value: String,
        #[structopt(name = "addr", long = "addr")]
        addr: Option<SocketAddr>,
    },

    #[structopt(name = "rm")]
    Remove {
        key: String,
        #[structopt(name = "addr", long = "addr")]
        addr: Option<SocketAddr>,
    },
}

fn get_addr(addr: Option<SocketAddr>) -> SocketAddr {
    addr.unwrap_or("127.0.0.1:4000".parse().unwrap())
}

fn main() -> Result<()> {
    let args = Args::from_args();

    match args {
        Args::Get { key, addr } => {
            let value = KvsClient::new(get_addr(addr)).get(key)?;

            match value {
                Some(val) => println!("{}", val),
                None => println!("Key not found"),
            };
        }
        Args::Set { key, value, addr } => {
            KvsClient::new(get_addr(addr)).set(key, value)?;
        }
        Args::Remove { key, addr } => {
            KvsClient::new(get_addr(addr)).remove(key)?;
        }
    };

    Ok(())
}
