use std::net::TcpListener;

const ADDR: &str = "127.0.0.1:6379";

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
