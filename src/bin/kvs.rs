use clap::{App, AppSettings, Arg, SubCommand};

pub fn main() {
    let name = env!("CARGO_PKG_NAME");
    let authors = env!("CARGO_PKG_AUTHORS");
    let version = env!("CARGO_PKG_VERSION");
    let description = env!("CARGO_PKG_DESCRIPTION");

    let _m = App::new("kvs")
        .name(name)
        .author(authors)
        .version(version)
        .about(description)
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(SubCommand::with_name("get").arg(Arg::with_name("KEY").required(true)))
        .subcommand(SubCommand::with_name("rm").arg(Arg::with_name("KEY").required(true)))
        .subcommand(
            SubCommand::with_name("set")
                .arg(Arg::with_name("KEY").required(true))
                .arg(Arg::with_name("VALUE").required(true)),
        )
        .get_matches();

    eprintln!("unimplemented");
    std::process::exit(1);
}
