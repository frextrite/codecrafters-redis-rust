use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{str, thread};

use clap::Parser;
use redis_starter_rust::common::CRLF;
use redis_starter_rust::network::connection::{Connection, ConnectionError, ConnectionResult};
use redis_starter_rust::parser::command::{parse_command, Command, ReplConfCommand};
use redis_starter_rust::parser::rdb::parse_rdb_payload;
use redis_starter_rust::parser::resp::Token;
use redis_starter_rust::parser::resp::{parse_buffer, ParseError};
use redis_starter_rust::replication::rdb::{get_empty_rdb, serialize_rdb};
use redis_starter_rust::replication::replica_manager::{Replica, ReplicaManager};
use redis_starter_rust::server::config::Config;
use redis_starter_rust::server::metadata::{self, ReplicaInfo, ServerMetadata};
use redis_starter_rust::storage::expiring_map::ExpiringHashMap;

const HOST: &str = "127.0.0.1";
#[allow(dead_code)]
const CR: u8 = b'\r';
#[allow(dead_code)]
const LF: u8 = b'\n';

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 6379)]
    port: u16,
    #[arg(short, long)]
    replicaof: Option<String>,
}

struct MasterLiveData {
    replication_offset: usize,
}

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
            ReplicaInfo::Master(..) => LiveData::Master(MasterLiveData {
                replication_offset: 0,
            }),
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
    store: Mutex<ExpiringHashMap>,
}

impl State {
    fn new(metadata: ServerMetadata) -> State {
        let live_data = Mutex::new(LiveData::new(&metadata.replica_info));
        State {
            metadata,
            replica_manager: Mutex::new(ReplicaManager::new()),
            live_data,
            store: Mutex::new(ExpiringHashMap::new()),
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
        Command::ReplConf(replconf_command) => {
            println!("DEBUG: received REPLCONF command {:?}", replconf_command);
            // TODO: Handle REPLCONF command validation before sending responses
            if let ReplicaInfo::Master(_) = state.metadata.replica_info {
                match replconf_command {
                    ReplConfCommand::Ack(offset) => {
                        println!("DEBUG: received ACK from replica");
                        state
                            .replica_manager
                            .lock()
                            .unwrap()
                            .update_replica_offset(stream, *offset);
                    }
                    ReplConfCommand::ListeningPort(_) | ReplConfCommand::Capa(_) => {
                        println!("DEBUG: sending OK response to REPLCONF");
                        stream.write_all(&serialize_to_simplestring(b"OK"))?
                    }
                    _ => {}
                }
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
                // TODO: Merge this with replica info in state
                let replication_offset = match state.live_data.lock().unwrap().deref() {
                    LiveData::Master(data) => data.replication_offset,
                    _ => panic!("Expected master data in live_data"),
                };
                let payload = format!("FULLRESYNC {} {}", info.replication_id, replication_offset);
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
        Command::Wait {
            replica_count,
            timeout,
        } => {
            println!(
                "DEBUG: received WAIT command with replica_count {:?} and timeout {:?}",
                replica_count, timeout
            );

            if replica_count == &0 {
                stream.write_all(&serialize_to_integer(0))?;
            } else if let ReplicaInfo::Master(..) = state.metadata.replica_info {
                // record the replication offset at the time of receiving the WAIT command
                let master_offset = match state.live_data.lock().unwrap().deref() {
                    LiveData::Master(data) => data.replication_offset,
                    _ => panic!("Expected master data in live_data"),
                };

                // Send REPLCONF GETACK to all replicas
                println!(
                    "DEBUG: sending GETACK to {} replicas",
                    state
                        .replica_manager
                        .lock()
                        .unwrap()
                        .get_connected_replica_count()
                );
                state
                    .replica_manager
                    .lock()
                    .unwrap()
                    .propagate_message_to_replicas(&serialize_to_array(&[
                        &serialize_to_bulkstring(Some(b"REPLCONF")),
                        &serialize_to_bulkstring(Some(b"GETACK")),
                        &serialize_to_bulkstring(Some(b"*")),
                    ]));

                // Synchronously wait for replica_count replicas to acknowledge the offset
                println!(
                    "DEBUG: sleeping for {:?} before getting replication status",
                    timeout
                );
                thread::sleep(*timeout);

                let count_replicated = state
                    .replica_manager
                    .lock()
                    .unwrap()
                    .get_up_to_date_replicas_count(master_offset);

                println!(
                    "DEBUG: {} replicas have replicated till the offset {}",
                    count_replicated, master_offset
                );

                let response = serialize_to_integer(count_replicated);
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
        // update master replication offset
        if let LiveData::Master(data) = state.live_data.lock().unwrap().deref_mut() {
            data.replication_offset += message.len();
        }
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
        match parse_command(conn.get_buffer()) {
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
            Err(ParseError::Incomplete) => conn.read_message()?, // Not enough data to parse the command
            Err(ParseError::Invalid) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid message format",
                ));
            }
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
    conn.write_message(req)?;
    Ok(())
}

// Validates FULLRESYNC response that is received, as well as the
// RDB payload response. This does not propagate the payload to the caller
// and panics in case the message is not as expected.
// Modify this to propagate information to the caller.
fn parse_and_validate_psync_response(conn: &mut Connection) {
    // Read and validate PSYNC response
    let result = loop {
        let message = conn.get_buffer();
        match parse_buffer(message) {
            Ok(result) => break result,
            Err(ParseError::Incomplete) => {
                conn.read_message().unwrap();
            }
            Err(err) => {
                eprintln!("ERROR: failed to parse PSYNC response with error {:?}", err);
                panic!("Failed to parse PSYNC response");
            }
        }
    };

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

    // Read and validate RDB payload
    let rdb_payload = loop {
        let message = conn.get_buffer();
        match parse_rdb_payload(message) {
            Ok(result) => break result,
            Err(ParseError::Incomplete) => {
                conn.read_message().unwrap();
            }
            Err(err) => {
                eprintln!("ERROR: failed to parse RDB payload with error {:?}", err);
                panic!("Failed to parse RDB payload");
            }
        }
    };

    println!(
        "DEBUG: successfully parsed RDB payload with length {}",
        rdb_payload.len
    );
    conn.consume(rdb_payload.len);
}

fn send_req_and_validate_simple_string_resp(
    req: &[u8],
    conn: &mut Connection,
    command: &str,
    expected: &str,
) -> ConnectionResult<()> {
    // send the request
    send_req(req, conn, command)?;

    // read the response
    let tokens = loop {
        let message = conn.get_buffer();
        match parse_buffer(message) {
            Ok(result) => {
                conn.consume(result.len);
                break result.tokens;
            }
            Err(ParseError::Incomplete) => conn.read_message()?,
            Err(err) => {
                eprintln!(
                    "ERROR: failed to parse response for command {} with error {:?}",
                    command, err
                );
                return Err(ConnectionError::Parse(err));
            }
        }
    };

    println!(
        "DEBUG: received response for command {}: {:?}",
        command, tokens
    );

    if tokens.len() != 1 {
        return Err(ConnectionError::Parse(ParseError::Invalid));
    }

    match &tokens[0] {
        Token::SimpleString(resp) if resp.to_lowercase() == expected.to_lowercase() => (),
        _ => return Err(ConnectionError::Parse(ParseError::Invalid)),
    };

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

fn serve_clients(state: Arc<State>) -> anyhow::Result<()> {
    let listening_port = state.metadata.listening_port;

    let addr = (HOST, listening_port);
    let listener = TcpListener::bind(addr)?;

    println!("INFO: started listener on {:?}", listener.local_addr());

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

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let config = Config::new();
    println!("DEBUG: parsed cli args: {:?}", config);

    let metadata = ServerMetadata::generate(&config);
    let state = Arc::new(State::new(metadata));

    // start replication
    if let ReplicaInfo::Slave(ref info) = state.metadata.replica_info {
        println!("INFO: starting replication as slave");

        let master_host = info.master_host.clone();
        let master_port = info.master_port;
        let state = state.clone();
        std::thread::spawn(move || {
            let result = initiate_replication_as_slave(
                master_host,
                master_port,
                state.metadata.listening_port,
                state,
            );
            if let Err(err) = result {
                eprintln!("ERROR: replication failed with error {:?}", err);
            }
        });
    }

    // start server
    serve_clients(state.clone())?;

    Ok(())
}
