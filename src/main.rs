use std::net::TcpListener;
use std::str;
use std::sync::Arc;

use codecrafters_redis::network::connection::Connection;
use codecrafters_redis::parser::command::parse_command;
use codecrafters_redis::parser::resp::ParseError;
use codecrafters_redis::replication::handshake::{self, Handshaker};
use codecrafters_redis::server::config::Config;
use codecrafters_redis::server::data::{LiveData, Server};
use codecrafters_redis::server::handler::CommandHandler;
use codecrafters_redis::server::metadata::{ReplicaInfo, ServerMetadata};

const HOST: &str = "127.0.0.1";

fn handle_read_loop(conn: &mut Connection, server: Arc<Server>) -> std::io::Result<()> {
    let mut handler = CommandHandler::new(conn.stream.try_clone()?, server.clone());

    loop {
        match parse_command(conn.get_buffer()) {
            Ok(result) => {
                let command = result.command;

                handler.handle_command(&command)?;

                if let LiveData::Slave(data) = &mut *server.live_data.lock().unwrap() {
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
}

fn handle_connection(conn: &mut Connection, server: Arc<Server>) -> std::io::Result<()> {
    match handle_read_loop(conn, server.clone()) {
        Ok(_) => {
            println!("INFO: client disconnected");
        }
        Err(err) => {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                println!("INFO: client disconnected");
            } else {
                eprintln!("ERROR: failed to handle connection with error {:?}", &err);
            }
        }
    }
    // TOOD: Instead of calling remove_replica unconditionally, propagate replica status for the client here
    // and only make remove_replica call if the client is actively replicating
    //
    // We cannot know whether there is atleast one replica listening for commands since this
    // information is only known by replication sub-system, hence propagate all commands
    if let ReplicaInfo::Master(_) = server.metadata.replica_info {
        server.handle_disconnect(conn);
    }
    Ok(())
}

fn initiate_replication(
    host: String,
    port: u16,
    host_port: u16,
    server: Arc<Server>,
) -> anyhow::Result<()> {
    println!("INFO: connecting to master at {}:{}", &host, port);

    let config = handshake::Config {
        master_host: host.clone(),
        master_port: port,
        replica_port: host_port,
    };
    let handshaker = Handshaker::new(config);

    let mut payload = match handshaker.perform_handshake() {
        Ok(payload) => {
            println!("INFO: successfully performed handshake with master");
            payload
        }
        Err(err) => {
            eprintln!("ERROR: failed to perform handshake with master: {:?}", &err);
            return Err(anyhow::anyhow!(
                "Failed to perform handshake with master: {:?}",
                &err
            ));
        }
    };

    handle_connection(payload.client.get_connection(), server)?;

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
                let mut conn = Connection::new(stream);
                let server = server.clone();
                std::thread::spawn(move || handle_connection(&mut conn, server));
            }
            Err(error) => {
                eprintln!(
                    "ERROR: failed to accept an incoming connection with error {:?}",
                    &error
                );
            }
        }
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let config = Config::new();
    println!("DEBUG: parsed cli args: {:?}", &config);

    let metadata = ServerMetadata::generate(&config);
    let server = Arc::new(Server::new(metadata));

    // start replication
    if let ReplicaInfo::Slave(ref info) = server.metadata.replica_info {
        println!("INFO: starting replication as slave");

        let master_host = info.master_host.clone();
        let master_port = info.master_port;
        let server = server.clone();
        std::thread::spawn(move || {
            let result = initiate_replication(
                master_host,
                master_port,
                server.metadata.listening_port,
                server,
            );
            if let Err(err) = result {
                eprintln!("ERROR: replication failed with error {:?}", &err);
            }
        });
    }

    // start server
    serve_clients(server.clone())?;

    Ok(())
}
