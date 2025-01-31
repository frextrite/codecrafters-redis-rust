use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::str;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use redis_starter_rust::network::connection::{Connection, ConnectionError, ConnectionResult};
use redis_starter_rust::parser::command::{parse_command, Command};
use redis_starter_rust::parser::rdb::parse_rdb_payload;
use redis_starter_rust::parser::resp::parse_buffer;
use redis_starter_rust::parser::resp::Token;
use redis_starter_rust::replication::rdb::{get_empty_rdb, serialize_rdb};
use redis_starter_rust::replication::replica_manager::{Replica, ReplicaManager};
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
    #[arg(short, long)]
    replicaof: Option<String>,
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

struct MasterLiveData;
struct SlaveLiveData {
    offset: usize,
    heartbeat_recv_time: Option<Instant>,
}

enum LiveData {
    Master(MasterLiveData),
    Slave(SlaveLiveData),
}

impl LiveData {
    fn new(info: &ReplicaInfo) -> LiveData {
        match info {
            ReplicaInfo::Master(..) => LiveData::Master(MasterLiveData),
            ReplicaInfo::Slave(..) => LiveData::Slave(SlaveLiveData {
                offset: 0,
                heartbeat_recv_time: None,
            }),
        }
    }
}

struct State {
    metadata: ServerMetadata,
    replica_manager: Mutex<ReplicaManager>,
    live_data: Mutex<LiveData>,
    store: Mutex<state::ExpiringHashMap>,
}

impl State {
    fn new(metadata: ServerMetadata) -> State {
        let live_data = Mutex::new(LiveData::new(&metadata.replica_info));
        State {
            metadata,
            replica_manager: Mutex::new(ReplicaManager::new()),
            live_data,
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

fn serialize_to_integer(value: usize) -> Vec<u8> {
    format!(":{}{CRLF}", value).into_bytes()
}

fn handle_command(
    command: &Command,
    stream: &mut TcpStream,
    state: Arc<State>,
) -> std::io::Result<()> {
    match command {
        Command::Ping => {
            if let ReplicaInfo::Master(_) = state.metadata.replica_info {
                stream.write_all(&serialize_to_simplestring(b"PONG"))?
            } else if let LiveData::Slave(data) = state.live_data.lock().unwrap().deref_mut() {
                data.heartbeat_recv_time = Some(Instant::now());
            }
        }
        Command::Echo(data) => stream.write_all(&serialize_to_bulkstring(Some(data)))?,
        Command::Get(key) => {
            let value = state.get(key).map(|v| v.to_vec());
            stream.write_all(&serialize_to_bulkstring(value.as_deref()))?
        }
        Command::Set { key, value, expiry } => {
            state.set(key, value, *expiry);
            println!(
                "DEBUG: setting key {:?} value {:?} with expiry {:?}",
                std::str::from_utf8(key),
                std::str::from_utf8(value),
                expiry
            );
            // TODO: Only send reply if we are master, and not if we are a replica receiving
            // replicated commands and make this generic instead of adding if conditions
            if let ReplicaInfo::Master(_) = state.metadata.replica_info {
                stream.write_all(&serialize_to_simplestring(b"OK"))?
            }
        }
        Command::Info(section) => match std::str::from_utf8(section).unwrap() {
            "replication" => stream.write_all(&serialize_to_bulkstring(Some(
                &state.metadata.get_replica_info(),
            )))?,
            _ => panic!("Not expecting to receive section other than replication"),
        },
        Command::ReplConf => {
            // TODO: Handle REPLCONF command validation before sending responses
            if let ReplicaInfo::Master(_) = state.metadata.replica_info {
                // Send OK as a response to REPLCONF listening-port or REPLCONF capa
                stream.write_all(&serialize_to_simplestring(b"OK"))?
            } else if let LiveData::Slave(data) = state.live_data.lock().unwrap().deref() {
                // Send REPLCONF ACK as a response to REPLCONF GETACK
                stream.write_all(&serialize_to_array(&[
                    &serialize_to_bulkstring(Some(b"REPLCONF")),
                    &serialize_to_bulkstring(Some(b"ACK")),
                    &serialize_to_bulkstring(Some(data.offset.to_string().as_bytes())),
                ]))?
            }
        }
        Command::Psync => {
            if let ReplicaInfo::Master(info) = &state.metadata.replica_info {
                let payload = format!(
                    "FULLRESYNC {} {}",
                    info.replication_id, info.replication_offset
                );
                stream.write_all(&serialize_to_simplestring(payload.as_bytes()))?;
                let rdb_payload = serialize_rdb(&get_empty_rdb());
                println!("DEBUG: sending RDB payload (as hex): {:x?}", &rdb_payload);
                stream.write_all(&rdb_payload)?;

                let replica = Replica::new(stream.try_clone().unwrap());
                state.replica_manager.lock().unwrap().add_replica(replica);
            } else {
                panic!("PSYNC not supported on slave")
            }
        }
        Command::Wait { .. } => {
            if let ReplicaInfo::Master(..) = state.metadata.replica_info {
                // Send REPLCONF GETACK to all replicas
                state
                    .replica_manager
                    .lock()
                    .unwrap()
                    .propagate_message_to_replicas(&serialize_to_array(&[
                        &serialize_to_bulkstring(Some(b"REPLCONF")),
                        &serialize_to_bulkstring(Some(b"GETACK")),
                        &serialize_to_bulkstring(Some(b"*")),
                    ]));

                // TODO: Read response from all replicas

                let response = serialize_to_integer(
                    state
                        .replica_manager
                        .lock()
                        .unwrap()
                        .get_connected_replica_count(),
                );
                println!(
                    "DEBUG: sending WAIT response {:?}",
                    std::str::from_utf8(&response).unwrap()
                );
                stream.write_all(&response)?;
            }
        }
    };
    Ok(())
}

fn can_replicate_command(command: &Command) -> bool {
    matches!(command, Command::Set { .. })
}

fn handle_command_replication(command: &Command, message: &[u8], state: &State) {
    if can_replicate_command(command) {
        println!(
            "DEBUG: replicating command {:?} with contents {:?}",
            command, message
        );
        state
            .replica_manager
            .lock()
            .unwrap()
            .propagate_message_to_replicas(message);
    }
}

fn handle_client_disconnect(stream: &TcpStream, state: &State) {
    if state
        .replica_manager
        .lock()
        .unwrap()
        .remove_replica(stream.peer_addr().unwrap())
        .is_some()
    {
        println!(
            "INFO: successfully removed {:?} as a replica listener",
            stream.peer_addr(),
        );
    }
}

fn handle_connection(mut conn: Connection, state: Arc<State>) -> std::io::Result<()> {
    loop {
        match conn.try_parse(parse_command) {
            Ok(result) => {
                let command = result.command;
                handle_command(&command, &mut conn.stream, state.clone())?;
                if let ReplicaInfo::Master(_) = state.metadata.replica_info {
                    handle_command_replication(&command, conn.get_buffer(), state.deref());
                } else if let LiveData::Slave(data) = state.live_data.lock().unwrap().deref_mut() {
                    data.offset += result.len;
                }
                conn.consume(result.len);
            }
            Err(err) => match err {
                ConnectionError::Io(io_err) => {
                    eprintln!(
                        "ERROR: failed to read from connection {:?} with error {:?}",
                        conn.stream.peer_addr(),
                        io_err
                    );
                    match io_err.kind() {
                        std::io::ErrorKind::Interrupted => continue,
                        _ => break,
                    }
                }
                ConnectionError::Parse(parse_err) => {
                    eprintln!(
                        "ERROR: failed to parse message {:?} with error {:?}",
                        conn.get_buffer(),
                        parse_err
                    );
                    break;
                }
            },
        }
    }
    // TOOD: Instead of calling remove_replica unconditionally, propagate replica status for the client here
    // and only make remove_replica call if the client is actively replicating
    //
    // We cannot know whether there is atleast one replica listening for commands since this
    // information is only known by replication sub-system, hence propagate all commands
    if let ReplicaInfo::Master(_) = state.metadata.replica_info {
        handle_client_disconnect(&conn.stream, state.deref());
    }
    Ok(())
}

fn send_req(req: &[u8], conn: &mut Connection, command: &str) -> std::io::Result<()> {
    println!(
        "INFO: sending {} request {:?}",
        command,
        std::str::from_utf8(req).unwrap()
    );
    conn.write_to_stream(req)?;
    Ok(())
}

// Validates FULLRESYNC response that is received, as well as the
// RDB payload response. This does not propagate the payload to the caller
// and panics in case the message is not as expected.
// Modify this to propagate information to the caller.
fn parse_and_validate_psync_response(conn: &mut Connection) {
    match conn.try_parse(parse_buffer) {
        Ok(result) => {
            if result.tokens.len() != 1 {
                panic!(
                    "Expected 1 token in PSYNC response but received {}",
                    result.tokens.len()
                );
            }
            match result.tokens.first().unwrap() {
                Token::SimpleString(data) => {
                    let data = std::str::from_utf8(data.as_bytes()).unwrap();
                    if data.starts_with("FULLRESYNC") {
                        println!("INFO: received FULLRESYNC from master: {:?}", data);
                    } else {
                        panic!("Expected FULLRESYNC but received {:?}", data);
                    }
                }
                _ => panic!(
                    "Expected SimpleString but received {:?}",
                    result.tokens.first().unwrap()
                ),
            }
            conn.consume(result.len);
        }
        Err(err) => panic!("Failed to parse PSYNC response with error {:?}", err),
    };
    match conn.try_parse(parse_rdb_payload) {
        Ok(result) => {
            println!(
                "DEBUG: successfully parsed RDB payload with length {}",
                result.len
            );
            conn.consume(result.len);
        }
        Err(err) => panic!("Failed to parse RDB payload with error {:?}", err),
    };
}

fn send_req_and_validate_simple_string_resp(
    req: &[u8],
    conn: &mut Connection,
    command: &str,
    expected: &str,
) -> ConnectionResult<()> {
    send_req(req, conn, command)?;
    let resp = conn.try_parse(parse_buffer)?;
    if resp.tokens.len() != 1
        || resp.tokens.first().unwrap() != &Token::SimpleString(expected.to_string())
    {
        panic!(
            "Expected SimpleString {:?} but received {:?}",
            expected,
            resp.tokens.first()
        );
    }
    conn.consume(resp.len);
    Ok(())
}

fn initiate_replication_as_slave(
    host: String,
    port: u16,
    host_port: u16,
    state: Arc<State>,
) -> ConnectionResult<()> {
    println!("INFO: connecting to master at {}:{}", &host, port);
    {
        let stream = TcpStream::connect((host, port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut conn = Connection::new(stream.try_clone()?);

        // Step 1: Send PING
        // TODO: Use Vec instead of a slice since the array size cannot be known at compile time for generic arrays
        let ping_req = serialize_to_array(&[&serialize_to_bulkstring(Some(b"PING"))]);
        send_req_and_validate_simple_string_resp(&ping_req, &mut conn, "PING", "PONG")?;

        // Step 2: Send REPLCONF messages
        // Step 2.1: Send REPLCONF with listening-port information
        let repl_conf_port_req = serialize_to_array(&[
            &serialize_to_bulkstring(Some(b"REPLCONF")),
            &serialize_to_bulkstring(Some(b"listening-port")),
            &serialize_to_bulkstring(Some(host_port.to_string().as_bytes())),
        ]);
        send_req_and_validate_simple_string_resp(&repl_conf_port_req, &mut conn, "REPLCONF", "OK")?;

        // Step 2.2: Send REPLCONF with capabilities information
        let repl_conf_capa_req = serialize_to_array(&[
            &serialize_to_bulkstring(Some(b"REPLCONF")),
            &serialize_to_bulkstring(Some(b"capa")),
            &serialize_to_bulkstring(Some(b"psync2")),
        ]);
        send_req_and_validate_simple_string_resp(&repl_conf_capa_req, &mut conn, "REPLCONF", "OK")?;

        // Step 3: Send PSYNC message
        let psync_req = serialize_to_array(&[
            &serialize_to_bulkstring(Some(b"PSYNC")),
            &serialize_to_bulkstring(Some(b"?")),
            &serialize_to_bulkstring(Some(b"-1")),
        ]);
        send_req(&psync_req, &mut conn, "PSYNC")?;
        parse_and_validate_psync_response(&mut conn);

        println!("INFO: successfully initiated replication for slave");

        // Start replication
        handle_connection(conn, state)?;
    }
    Ok(())
}

fn initiate_replication(state: Arc<State>, host_port: u16) {
    match &state.metadata.replica_info {
        ReplicaInfo::Slave(info) => {
            let host = info.master_host.clone();
            let port = info.master_port;
            std::thread::spawn(move || {
                initiate_replication_as_slave(host, port, host_port, state).unwrap();
            });
        }
        ReplicaInfo::Master(_) => println!("WARN: replication not implemented for master"),
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

    let metadata = generate_server_metadata(
        args.replicaof
            .map(|info| info.split_whitespace().map(str::to_string).collect()),
    );
    let state = Arc::new(State::new(metadata));
    initiate_replication(state.clone(), args.port);

    for incoming_stream in listener.incoming() {
        match incoming_stream {
            Ok(stream) => {
                println!(
                    "INFO: accepted incoming connection from {:?}",
                    stream.peer_addr()
                );
                let conn = Connection::new(stream);
                let state = state.clone();
                std::thread::spawn(|| handle_connection(conn, state).unwrap());
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
