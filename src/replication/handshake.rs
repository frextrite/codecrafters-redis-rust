use std::net::TcpStream;

use crate::{
    client::{Client, ClientError},
    network::connection::Connection,
    parser::{
        rdb::parse_rdb_payload,
        resp::{ParseError, Token},
    },
};

pub struct RDBPayload {
    pub rdb: Vec<u8>,
}

#[derive(Debug)]
pub enum HandshakeError {
    ConnectionError(Option<String>),
    ParseError(Option<String>),
    IoError(Option<String>),
}

impl From<ClientError> for HandshakeError {
    fn from(value: ClientError) -> Self {
        match value {
            ClientError::ConnectionError(msg) => HandshakeError::ConnectionError(Some(msg)),
            ClientError::ParseError(parse_error) => {
                HandshakeError::ParseError(Some(parse_error.to_string()))
            }
        }
    }
}

pub struct HandshakePayload {
    pub client: Client,
    pub rdb: RDBPayload,
}

pub type HandshakeResult = Result<HandshakePayload, HandshakeError>;

pub struct Config {
    pub master_host: String,
    pub master_port: u16,
    pub replica_port: u16,
}

pub struct Handshaker {
    config: Config,
}

impl Handshaker {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn perform_handshake(&self) -> HandshakeResult {
        let mut client = self.create_client()?;

        // Step 1: Send PING
        self.perform_ping(&mut client)?;

        // Step 2: Send REPLCONF
        self.send_replconf_information(&mut client)?;

        // Step 3: Send PSYNC message
        self.perform_sync(&mut client)?;

        let rdb = self.receive_rdb_payload(&mut client)?;

        Ok(HandshakePayload { client, rdb })
    }

    fn create_client(&self) -> Result<Client, HandshakeError> {
        let conn = self.create_connection()?;
        let client = Client::new(conn);
        Ok(client)
    }

    fn create_connection(&self) -> Result<Connection, HandshakeError> {
        match TcpStream::connect((self.config.master_host.clone(), self.config.master_port)) {
            Ok(stream) => Ok(Connection::new(stream)),
            Err(e) => Err(HandshakeError::ConnectionError(Some(e.to_string()))),
        }
    }

    fn perform_ping(&self, client: &mut Client) -> Result<(), HandshakeError> {
        let ping = Token::Array(vec![Token::BulkString(b"PING".to_vec())]);
        let ping = ping.serialize();
        self.send_message(&ping, client.get_connection())?;
        let response = self.get_response(client)?;
        self.validate_ping(response)
    }

    fn validate_ping(&self, response: Vec<Token>) -> Result<(), HandshakeError> {
        if response.len() != 1 {
            return Err(HandshakeError::ParseError(Some(
                "Invalid PING response length".to_string(),
            )));
        }
        if let Token::SimpleString(data) = &response[0] {
            if data.to_lowercase() != "pong" {
                return Err(HandshakeError::ParseError(Some(format!(
                    "Unexpected PING response: {}",
                    &data
                ))));
            }
        } else {
            return Err(HandshakeError::ParseError(Some(
                "Expected SimpleString in PING response".to_string(),
            )));
        }
        Ok(())
    }

    fn send_replconf_information(&self, client: &mut Client) -> Result<(), HandshakeError> {
        // Send REPLCONF with listening-port
        let replconf_port = Token::Array(vec![
            Token::BulkString(b"REPLCONF".to_vec()),
            Token::BulkString(b"listening-port".to_vec()),
            Token::BulkString(self.config.replica_port.to_string().as_bytes().to_vec()),
        ]);
        let replconf_port = replconf_port.serialize();
        self.send_message(&replconf_port, client.get_connection())?;
        let response = self.get_response(client)?;
        self.validate_replconf(response)?;

        // Send REPLCONF with capa psync2
        let replconf_capa = Token::Array(vec![
            Token::BulkString(b"REPLCONF".to_vec()),
            Token::BulkString(b"capa".to_vec()),
            Token::BulkString(b"psync2".to_vec()),
        ]);
        let replconf_capa = replconf_capa.serialize();
        self.send_message(&replconf_capa, client.get_connection())?;
        let response = self.get_response(client)?;
        self.validate_replconf(response)?;

        Ok(())
    }

    fn validate_replconf(&self, response: Vec<Token>) -> Result<(), HandshakeError> {
        if response.len() != 1 {
            return Err(HandshakeError::ParseError(Some(
                "Invalid REPLCONF response length".to_string(),
            )));
        }
        if let Token::SimpleString(data) = &response[0] {
            if data.to_lowercase() != "ok" {
                return Err(HandshakeError::ParseError(Some(format!(
                    "Unexpected REPLCONF response: {}",
                    &data
                ))));
            }
        } else {
            return Err(HandshakeError::ParseError(Some(
                "Expected SimpleString in REPLCONF response".to_string(),
            )));
        }
        Ok(())
    }

    fn perform_sync(&self, client: &mut Client) -> Result<(), HandshakeError> {
        let psync = Token::Array(vec![
            Token::BulkString(b"PSYNC".to_vec()),
            Token::BulkString(b"?".to_vec()),
            Token::BulkString(b"-1".to_vec()),
        ]);
        let psync = psync.serialize();
        self.send_message(&psync, client.get_connection())?;

        let response = self.get_response(client)?;

        if response.len() != 1 {
            return Err(HandshakeError::ParseError(Some(
                "Invalid PSYNC response length".to_string(),
            )));
        }

        Ok(())
    }

    fn receive_rdb_payload(&self, client: &mut Client) -> Result<RDBPayload, HandshakeError> {
        let conn = client.get_connection();
        loop {
            match parse_rdb_payload(conn.get_buffer()) {
                Ok(rdb_payload) => {
                    conn.consume(rdb_payload.len);
                    return Ok(RDBPayload {
                        rdb: rdb_payload.rdb,
                    });
                }
                Err(err) => match err {
                    ParseError::Incomplete => conn
                        .read_message()
                        .map_err(|err| HandshakeError::IoError(Some(err.to_string())))?,
                    _ => {
                        return Err(HandshakeError::ParseError(Some(err.to_string())));
                    }
                },
            }
        }
    }

    fn send_message(&self, message: &[u8], conn: &mut Connection) -> Result<(), HandshakeError> {
        conn.write_message(message)
            .map_err(|err| HandshakeError::IoError(Some(err.to_string())))?;
        Ok(())
    }

    fn get_response(&self, client: &mut Client) -> Result<Vec<Token>, HandshakeError> {
        let response = client.get_next_message()?;
        Ok(response.tokens)
    }
}
