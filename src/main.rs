use std::cmp::Ordering;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use redis_starter_rust::state;

const HOST: &str = "127.0.0.1";
#[allow(dead_code)]
const CR: u8 = b'\r';
#[allow(dead_code)]
const LF: u8 = b'\n';
const CRLF: &str = "\r\n";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
    #[arg(short, long, num_args = 2)]
    replicaof: Option<Vec<String>>,
}

#[derive(Debug)]
struct MasterInfo {
    replication_id: String,
    replication_offset: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct SlaveInfo {
    master_host: String,
    master_port: u16,
}

#[derive(Debug)]
enum ReplicaInfo {
    Master(MasterInfo),
    Slave(SlaveInfo),
}

#[derive(Debug)]
struct ServerMetadata {
    replica_info: ReplicaInfo,
}

impl ServerMetadata {
    fn get_replica_info(&self) -> Vec<u8> {
        match &self.replica_info {
            ReplicaInfo::Master(master_info) => format!(
                "role:master{CRLF}master_replid:{}{CRLF}master_repl_offset:{}",
                master_info.replication_id, master_info.replication_offset
            )
            .as_bytes()
            .to_vec(),
            ReplicaInfo::Slave(_slave_info) => b"role:slave".to_vec(),
        }
    }
}

struct State {
    metadata: ServerMetadata,
    store: Mutex<state::ExpiringHashMap>,
}

impl State {
    fn new(metadata: ServerMetadata) -> State {
        State {
            metadata,
            store: Mutex::new(state::ExpiringHashMap::new()),
        }
    }

    fn set(&self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        self.store.lock().unwrap().set(key, value, expiry);
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store.lock().unwrap().get(key)
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
    Info(&'a [u8]),
}

// TODO: avoid extra copies
fn serialize_to_array(data: &[&[u8]]) -> Vec<u8> {
    let count = data.len();
    let data = data.iter().flat_map(|&x| x).copied().collect::<Vec<u8>>();
    format!("*{}{CRLF}{}", count, std::str::from_utf8(&data).unwrap()).into_bytes()
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
    state: Arc<State>,
) -> std::io::Result<()> {
    match command {
        Command::Ping => stream.write_all(&serialize_to_simplestring(b"PONG"))?,
        Command::Echo(data) => stream.write_all(&serialize_to_bulkstring(Some(data)))?,
        Command::Get(key) => {
            let value = state.get(key).map(|v| v.to_vec());
            drop(state);
            stream.write_all(&serialize_to_bulkstring(value.as_deref()))?
        }
        Command::Set { key, value, expiry } => {
            state.set(key, value, expiry);
            println!(
                "DEBUG: setting key {:?} value {:?} with expiry {:?}",
                std::str::from_utf8(key),
                std::str::from_utf8(value),
                expiry
            );
            drop(state);
            stream.write_all(&serialize_to_simplestring(b"OK"))?
        }
        Command::Info(section) => match std::str::from_utf8(section).unwrap() {
            "replication" => stream.write_all(&serialize_to_bulkstring(Some(
                &state.metadata.get_replica_info(),
            )))?,
            _ => panic!("Not expecting to receive section other than replication"),
        },
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
            } else if segments[2].eq_ignore_ascii_case("info") {
                Some(Command::Info(segments[4].as_bytes()))
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
fn handle_connection(mut stream: TcpStream, state: Arc<State>) -> std::io::Result<()> {
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

fn cmp_simple_strings(received: &[u8], expected_data: &[u8]) -> Ordering {
    received.cmp(&serialize_to_simplestring(expected_data))
}

fn read_single_message(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buf = [0; 512];
    let n = stream.read(&mut buf)?;
    Ok(buf[..n].to_vec())
}

fn initiate_replication_as_slave(host: String, port: u16, host_port: u16) -> std::io::Result<()> {
    // checking correctness of the response is not mandated, but setting this value to `true` matches the
    // received response to the expected data and testing indicates this is working perfectly
    let wait_for_inline_acks = true;
    println!("INFO: connecting to master at {}:{}", &host, port);
    {
        let mut stream = TcpStream::connect((host, port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        // Step 1: Send PING
        // TODO: Use Vec instead of a slice since the array size cannot be known at compile time for generic arrays
        let ping_req = serialize_to_array(&[&serialize_to_bulkstring(Some(b"PING"))]);
        println!(
            "INFO: sending replication ping request {:?}",
            std::str::from_utf8(&ping_req).unwrap(),
        );
        stream.write_all(&ping_req).unwrap();

        // Receive and process ACK (PONG)
        if wait_for_inline_acks {
            let resp = read_single_message(&mut stream)?;
            let expected = "PONG";
            if let Ordering::Equal = cmp_simple_strings(&resp, expected.as_bytes()) {
            } else {
                panic!(
                    "Received {:?} from master but {:?} was expected",
                    std::str::from_utf8(&resp).unwrap(),
                    expected,
                );
            }
        }

        // Step 2: Send REPLCONF messages
        // Step 2.1: Send REPLCONF with listening-port information
        let repl_conf_port_req = serialize_to_array(&[
            &serialize_to_bulkstring(Some(b"REPLCONF")),
            &serialize_to_bulkstring(Some(b"listening-port")),
            &serialize_to_bulkstring(Some(host_port.to_string().as_bytes())),
        ]);
        println!(
            "INFO: sending replication REPLCONF request with listening port information {:?}",
            std::str::from_utf8(&repl_conf_port_req).unwrap()
        );
        stream.write_all(&repl_conf_port_req).unwrap();

        // Receive and process ACK (OK)
        if wait_for_inline_acks {
            let resp = read_single_message(&mut stream)?;
            let expected = "OK";
            if let Ordering::Equal = cmp_simple_strings(&resp, expected.as_bytes()) {
            } else {
                panic!(
                    "Received {:?} from master but {:?} was expected",
                    std::str::from_utf8(&resp).unwrap(),
                    expected,
                );
            }
        }

        // Step 2.2: Send REPLCONF with capabilities information
        let repl_conf_capa_req = serialize_to_array(&[
            &serialize_to_bulkstring(Some(b"REPLCONF")),
            &serialize_to_bulkstring(Some(b"capa")),
            &serialize_to_bulkstring(Some(b"psync2")),
        ]);
        println!(
            "INFO: sending replication REPLCONF request with capabilities information {:?}",
            std::str::from_utf8(&repl_conf_capa_req).unwrap(),
        );
        stream.write_all(&repl_conf_capa_req).unwrap();

        // Receive and process ACK (OK)
        if wait_for_inline_acks {
            let resp = read_single_message(&mut stream)?;
            let expected = "OK";
            if let Ordering::Equal = cmp_simple_strings(&resp, expected.as_bytes()) {
            } else {
                panic!(
                    "Received {:?} from master but {:?} was expected",
                    std::str::from_utf8(&resp).unwrap(),
                    expected,
                );
            }
        }
    }
    println!("INFO: successfully initiated replication for slave");
    Ok(())
}

fn initiate_replication(metadata: &ServerMetadata, host_port: u16) {
    match &metadata.replica_info {
        ReplicaInfo::Slave(info) => {
            let host = info.master_host.clone();
            let port = info.master_port;
            std::thread::spawn(move || {
                initiate_replication_as_slave(host, port, host_port).unwrap();
            });
        }
        ReplicaInfo::Master(_) => println!("ERROR: replication not implemented for master"),
    }
}

fn generate_server_metadata(replica_info: Option<Vec<String>>) -> ServerMetadata {
    match replica_info {
        Some(info) => {
            assert_eq!(info.len(), 2);
            ServerMetadata {
                replica_info: ReplicaInfo::Slave(SlaveInfo {
                    master_host: info[0].clone(),
                    master_port: info[1].parse().unwrap(),
                }),
            }
        }
        None => ServerMetadata {
            replica_info: ReplicaInfo::Master(MasterInfo {
                replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                replication_offset: 0,
            }),
        },
    }
}

fn main() {
    let args = Cli::parse();
    println!("DEBUG: parsed cli args: {:?}", args);
    let addr = (HOST, args.port);
    let listener = TcpListener::bind(addr).unwrap();

    println!("INFO: started listener on {:?}", addr);

    let metadata = generate_server_metadata(args.replicaof);
    initiate_replication(&metadata, args.port);
    let state = Arc::new(State::new(metadata));

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
