use kvs::KvStore;
use kvs::Result;
use std::env::current_dir;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs")]
enum Args {
    #[structopt(name = "get")]
    Get { key: String },

    #[structopt(name = "set")]
    Set { key: String, value: String },

    #[structopt(name = "rm")]
    Remove { key: String },
}

pub fn main() -> Result<()> {
    let args = Args::from_args();
    let mut kvs = KvStore::open(&current_dir()?)?;

    match args {
        Args::Get { key } => match kvs.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        Args::Set { key, value } => kvs.set(key, value)?,
        Args::Remove { key } => kvs.remove(key)?,
    };

    Ok(())
}
