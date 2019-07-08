use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "kvs")]
enum Kvs {
    #[structopt(name = "get")]
    Get { key: String },

    #[structopt(name = "set")]
    Set { key: String, value: String },

    #[structopt(name = "rm")]
    Remove { key: String },
}

pub fn main() {
    let args = Kvs::from_args();

    eprintln!("unimplemented");
    std::process::exit(1);
}
