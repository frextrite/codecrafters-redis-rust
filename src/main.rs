use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use redis_starter_rust::state;

const ADDR: &str = "127.0.0.1:6379";
#[allow(dead_code)]
const CR: u8 = b'\r';
#[allow(dead_code)]
const LF: u8 = b'\n';
const CRLF: &str = "\r\n";

struct State {
    store: state::ExpiringHashMap,
}

impl State {
    fn new() -> State {
        State {
            store: state::ExpiringHashMap::new(),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        self.store.set(key, value, expiry);
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.store.get(key)
    }
}

enum Command<'a> {
    Ping,
    Echo(&'a [u8]),
    Get(&'a [u8]),
    Set {
        key: &'a [u8],
        value: &'a [u8],
        #[allow(dead_code)]
        expiry: Option<Duration>,
    },
}

fn serialize_to_bulkstring(data: Option<&[u8]>) -> Vec<u8> {
    match data {
        Some(bytes) => format!(
            "${}{CRLF}{}{CRLF}",
            bytes.len(),
            std::str::from_utf8(bytes).unwrap()
        )
        .into_bytes(),
        None => format!("$-1{CRLF}").into_bytes(),
    }
}

fn serialize_to_simplestring(data: &[u8]) -> Vec<u8> {
    format!("+{}{CRLF}", std::str::from_utf8(data).unwrap()).into_bytes()
}

fn handle_command(
    command: Command,
    stream: &mut TcpStream,
    state: Arc<Mutex<State>>,
) -> std::io::Result<()> {
    match command {
        Command::Ping => stream.write(&serialize_to_simplestring(b"PONG"))?,
        Command::Echo(data) => stream.write(&serialize_to_bulkstring(Some(data)))?,
        Command::Get(key) => {
            let mut state = state.lock().unwrap();
            let value = state.get(key).map(|v| v.to_vec());
            drop(state);
            stream.write(&serialize_to_bulkstring(value.as_deref()))?
        }
        Command::Set { key, value, expiry } => {
            let mut state = state.lock().unwrap();
            state.set(key, value, expiry);
            println!(
                "DEBUG: setting key {:?} value {:?} with expiry {:?}",
                std::str::from_utf8(key),
                std::str::from_utf8(value),
                expiry
            );
            drop(state);
            stream.write(&serialize_to_simplestring(b"OK"))?
        }
    };
    Ok(())
}

fn bytes_to_unsigned(bytes: &[u8]) -> Option<u64> {
    let mut integer = 0;
    for digit in bytes {
        if digit.is_ascii_digit() {
            integer = integer * 10 + (digit - b'0') as u64;
        } else {
            return None;
        }
    }

    Some(integer)
}

// This only works with "correct" clients since it does not check for the correctness of the
// commands, and assumes that the client will send the correct number of arguments.
fn parse_message(message: &[u8]) -> Option<Command> {
    match message.first() {
        Some(b'*') => {
            let message = std::str::from_utf8(message).unwrap();
            println!("INFO: started parsing message {:?}", message);

            let segments = message.split("\r\n").collect::<Vec<_>>();
            if segments[2].eq_ignore_ascii_case("ping") {
                Some(Command::Ping)
            } else if segments[2].eq_ignore_ascii_case("echo") {
                Some(Command::Echo(segments[4].as_bytes()))
            } else if segments[2].eq_ignore_ascii_case("get") {
                Some(Command::Get(segments[4].as_bytes()))
            } else if segments[2].eq_ignore_ascii_case("set") {
                Some(Command::Set {
                    key: segments[4].as_bytes(),
                    value: segments[6].as_bytes(),
                    expiry: if segments.len() > 8 && segments[8].eq_ignore_ascii_case("px") {
                        Some(Duration::from_millis(
                            bytes_to_unsigned(segments[10].as_bytes()).unwrap(),
                        ))
                    } else {
                        None
                    },
                })
            } else {
                None
            }
        }
        Some(byte) => unimplemented!(
            "INFO: Found redis command starting with byte {}",
            byte.to_ascii_lowercase()
        ),
        None => None,
    }
}

// TcpStream::read() is *not* guaranteed to return the entire command, which means multiple
// calls to read() are required to read an entire command.
// TODO: handle the above scenario in which multiple calls to read() are required to obtain a
// single command
fn handle_connection(mut stream: TcpStream, state: Arc<Mutex<State>>) -> std::io::Result<()> {
    let mut buf = [0; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(bytes_read) => {
                let message = &buf[..bytes_read];
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
                    Some(command) => handle_command(command, &mut stream, state.clone())?,
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

    let state = Arc::new(Mutex::new(State::new()));
    for incoming_stream in listener.incoming() {
        match incoming_stream {
            Ok(stream) => {
                println!(
                    "INFO: accepted incoming connection from {:?}",
                    stream.peer_addr()
                );
                let state = state.clone();
                std::thread::spawn(|| handle_connection(stream, state).unwrap());
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
