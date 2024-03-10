use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

const ADDR: &str = "127.0.0.1:6379";

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 32];
    loop {
        match stream.read(&mut buf) {
            Ok(bytes_read) => {
                println!(
                    "INFO: read {} bytes from connection {:?}",
                    bytes_read,
                    stream.peer_addr()
                );
                if bytes_read == 0 {
                    break;
                }
                write!(stream, "+PONG\r\n").unwrap();
            }
            Err(error) => {
                eprintln!(
                    "ERROR: failed to read from connection {:?} with error {:?}",
                    stream.peer_addr(),
                    error
                );
            }
        }
    }
    Ok(())
}

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();

    println!("INFO: started listener on {:?}", ADDR);

    for incoming_stream in listener.incoming() {
        match incoming_stream {
            Ok(stream) => {
                println!(
                    "INFO: accepted incoming connection from {:?}",
                    stream.peer_addr()
                );
                std::thread::spawn(|| handle_connection(stream).unwrap());
            }
            Err(error) => {
                eprintln!(
                    "ERROR: failed to accept an incoming connection with error {:?}",
                    error
                );
            }
        }
    }
}
