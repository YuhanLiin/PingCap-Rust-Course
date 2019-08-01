use std::io::prelude::*;
use std::net::TcpStream;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:7878")?;
    println!("Connect completed");

    stream.write(b"PING\r\n")?;
    let mut buf = [0; 128];
    stream.read(&mut buf)?;
    println!("Reply1: {}", std::str::from_utf8(&buf)?);

    let mut stream = TcpStream::connect("127.0.0.1:7878")?;
    println!("Connect completed");

    stream.write(b"NOT PING\r\n")?;
    let mut buf = [0; 128];
    stream.read(&mut buf)?;
    println!("Reply2: {}", std::str::from_utf8(&buf)?);

    Ok(())
}
