use std::io::prelude::*;
use std::net::TcpListener;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:7878")?;
    println!("Bind completed");

    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut buf = [0; 128];
        let len = stream.read(&mut buf)?;
        let s = std::str::from_utf8(&buf[..len])?;
        println!("Received: {}", s);

        if s == "PING\r\n" {
            stream.write(b"+PONG\r\n")?;
        } else {
            stream.write(b"-ERROR\r\n")?;
        }
    }
    Ok(())
}
