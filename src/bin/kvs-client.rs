use failure::{ensure, format_err};
use kvs::protocol;
use kvs::Result;
use std::net::{SocketAddr, TcpStream};
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

    let addr = args.addr.unwrap_or("127.0.0.1:4000".parse().unwrap());
    let mut stream = TcpStream::connect(addr)?;

    let req_args = match args.sub {
        SubCommand::Get { key } => vec![protocol::GET.to_owned(), key],
        SubCommand::Set { key, value } => vec![protocol::SET.to_owned(), key, value],
        SubCommand::Remove { key } => vec![protocol::REMOVE.to_owned(), key],
    };
    let req = protocol::Message::Array(req_args);

    req.write(&mut stream)?;

    let resp = protocol::Message::read(&mut stream)?;
    match resp {
        protocol::Message::Array(arr) => {
            ensure!(
                arr.len() == 1,
                "unexpected server output: {}",
                arr.join(" ")
            );
            println!("Value = {}", arr[0]);
        }
        protocol::Message::Error(err) => return Err(format_err!("Error: {}", err)),
    };

    Ok(())
}
