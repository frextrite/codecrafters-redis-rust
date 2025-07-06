use crate::{
    network::connection::Connection,
    parser::resp::{parse_buffer, ParseError, ParseResult},
};

pub struct Client {
    conn: Connection,
}

pub enum ClientError {
    ConnectionError(String),
    ParseError(ParseError),
}

impl From<ParseError> for ClientError {
    fn from(value: ParseError) -> Self {
        ClientError::ParseError(value)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(value: std::io::Error) -> Self {
        ClientError::ConnectionError(value.to_string())
    }
}

impl Client {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    pub fn get_connection(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn get_next_message(&mut self) -> Result<ParseResult, ClientError> {
        loop {
            match parse_buffer(self.conn.get_buffer()) {
                Ok(result) => {
                    self.conn.consume(result.len);
                    break Ok(result);
                }
                Err(e) => match e {
                    ParseError::Incomplete => {
                        self.conn.read_message()?;
                    }
                    _ => {
                        return Err(ClientError::from(e));
                    }
                },
            }
        }
    }
}
