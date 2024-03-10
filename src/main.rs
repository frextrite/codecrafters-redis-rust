use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;

const ADDR: &str = "127.0.0.1:6379";

enum Command<'a> {
    Ping,
    Echo(&'a str),
}

fn handle_command(command: Command, stream: &mut TcpStream) -> std::io::Result<()> {
    match command {
        Command::Ping => write!(stream, "+PONG\r\n")?,
        Command::Echo(data) => write!(stream, "${}\r\n{}\r\n", data.len(), data)?,
    }
    Ok(())
}

fn parse_message(message: &str) -> Option<Command> {
    let segments = message.split("\r\n").collect::<Vec<_>>();
    if segments[2].eq_ignore_ascii_case("ping") {
        return Some(Command::Ping);
    } else if segments[2].eq_ignore_ascii_case("echo") {
        return Some(Command::Echo(segments[4]));
    }

    None
}

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0; 32];
    loop {
        match stream.read(&mut buf) {
            Ok(bytes_read) => {
                let message = str::from_utf8(&buf[..bytes_read]).unwrap();
                println!(
                    "INFO: read message {:?} with {} bytes from connection {:?}",
                    message,
                    bytes_read,
                    stream.peer_addr()
                );
                if bytes_read == 0 {
                    break;
                }
                match parse_message(message) {
                    Some(command) => handle_command(command, &mut stream)?,
                    None => eprintln!("ERROR: failed to parse message {:?}", message),
                }
            }
            Err(error) => {
                eprintln!(
                    "ERROR: failed to read from connection {:?} with error {:?}",
                    stream.peer_addr(),
                    error
                );
                match error.kind() {
                    std::io::ErrorKind::Interrupted => continue,
                    _ => break,
                }
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
