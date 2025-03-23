use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::str;
use std::sync::Arc;
use std::time::Duration;

use redis_starter_rust::common::CRLF;
use redis_starter_rust::network::connection::{Connection, ConnectionError, ConnectionResult};
use redis_starter_rust::parser::command::parse_command;
use redis_starter_rust::parser::rdb::parse_rdb_payload;
use redis_starter_rust::parser::resp::Token;
use redis_starter_rust::parser::resp::{parse_buffer, ParseError};
use redis_starter_rust::server::config::Config;
use redis_starter_rust::server::data::{LiveData, Server};
use redis_starter_rust::server::handler::CommandHandler;
use redis_starter_rust::server::metadata::{ReplicaInfo, ServerMetadata};

const HOST: &str = "127.0.0.1";

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

fn handle_client_disconnect(stream: &TcpStream, server: &Server) {
    if server
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

fn handle_connection(mut conn: Connection, server: Arc<Server>) -> std::io::Result<()> {
    let mut handler = CommandHandler::new(conn.stream.try_clone()?, server.clone());

    loop {
        match parse_command(conn.get_buffer()) {
            Ok(result) => {
                let command = result.command;

                handler.handle_command(&command)?;

                if let LiveData::Slave(data) = server.live_data.lock().unwrap().deref_mut() {
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
    if let ReplicaInfo::Master(_) = server.metadata.replica_info {
        handle_client_disconnect(&conn.stream, server.deref());
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
    server: Arc<Server>,
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
        handle_connection(conn, server)?;
    }
    Ok(())
}

fn serve_clients(server: Arc<Server>) -> anyhow::Result<()> {
    let listening_port = server.metadata.listening_port;

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
                let server = server.clone();
                std::thread::spawn(|| handle_connection(conn, server).unwrap());
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
    let server = Arc::new(Server::new(metadata));

    // start replication
    if let ReplicaInfo::Slave(ref info) = server.metadata.replica_info {
        println!("INFO: starting replication as slave");

        let master_host = info.master_host.clone();
        let master_port = info.master_port;
        let server = server.clone();
        std::thread::spawn(move || {
            let result = initiate_replication_as_slave(
                master_host,
                master_port,
                server.metadata.listening_port,
                server,
            );
            if let Err(err) = result {
                eprintln!("ERROR: replication failed with error {:?}", err);
            }
        });
    }

    // start server
    serve_clients(server.clone())?;

    Ok(())
}
