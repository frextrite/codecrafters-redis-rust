use std::io::{Read, Write};
use std::net::TcpListener;

const ADDR: &str = "127.0.0.1:6379";

fn main() {
    let listener = TcpListener::bind(ADDR).unwrap();

    println!("INFO: started listener on {:?}", ADDR);

    for incoming_stream in listener.incoming() {
        match incoming_stream {
            Ok(mut stream) => {
                println!(
                    "INFO: accepted incoming connection from {:?}",
                    stream.peer_addr()
                );
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
