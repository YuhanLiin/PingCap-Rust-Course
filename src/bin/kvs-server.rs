use kvs::Result;
use std::net::IpAddr;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs-server")]
struct Args {
    #[structopt(long = "--addr")]
    ip_addr: Option<IpAddr>,
    #[structopt(long = "--engine")]
    engine: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::from_args();
    Ok(())
}
