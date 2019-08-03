use failure::{ensure, format_err};
use kvs::protocol;
use kvs::Result;
use std::net::{Shutdown, SocketAddr, TcpStream};
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

fn main() -> Result<()> {
    let args = Args::from_args();

    let (req_args, addr) = match args {
        Args::Get { key, addr } => (vec![protocol::GET.to_owned(), key], addr),
        Args::Set { key, value, addr } => (vec![protocol::SET.to_owned(), key, value], addr),
        Args::Remove { key, addr } => (vec![protocol::REMOVE.to_owned(), key], addr),
    };

    let req = protocol::Message::Array(req_args);
    let addr = addr.unwrap_or("127.0.0.1:4000".parse().unwrap());
    let mut stream = TcpStream::connect(addr)?;

    req.write(&mut stream)?;
    // Need to shutdown write socket so server read socket gets dropped, otherwise server read
    // never finishes
    stream.shutdown(Shutdown::Write)?;

    let resp = protocol::Message::read(&mut stream)?;
    match resp {
        protocol::Message::Array(arr) => {
            // Empty array means output of nothing, so we don't print
            if !arr.is_empty() {
                ensure!(
                    arr.len() == 1,
                    "unexpected server output: {}",
                    arr.join(" ")
                );

                println!("{}", arr[0]);
            }
        }
        protocol::Message::Null => println!("Key not found\n"),
        protocol::Message::Error(err) => return Err(format_err!("Error: {}", err)),
    };

    Ok(())
}
