use crate::parser::resp::ParseError;
use std::io;
use std::io::Write;
use std::{io::Read, net::TcpStream};

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

const MAX_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB

#[derive(Debug)]
pub enum ConnectionError {
    Io(std::io::Error),
    Parse(ParseError),
}

impl From<std::io::Error> for ConnectionError {
    fn from(value: std::io::Error) -> Self {
        ConnectionError::Io(value)
    }
}

pub struct Connection {
    pub stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
        }
    }

    pub fn from_existing(stream: TcpStream, buffer: &[u8]) -> Self {
        let mut conn = Self::new(stream);
        conn.buffer[..buffer.len()].copy_from_slice(buffer);
        conn
    }

    pub fn read_message(&mut self) -> io::Result<()> {
        let mut buffer = vec![0u8; 1024];

        let bytes_read = loop {
            match self.stream.read(&mut buffer) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Connection closed",
                    ))
                }
                Ok(n) => break n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        };

        self.buffer.extend_from_slice(&buffer[..bytes_read]);

        if self.buffer.len() > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Message size exceeds limit",
            ));
        }

        Ok(())
    }

    pub fn write_message(&mut self, message: &[u8]) -> io::Result<()> {
        self.stream.write_all(message)?;
        Ok(())
    }

    pub fn consume(&mut self, n: usize) {
        self.buffer.drain(..n);
    }

    pub fn get_buffer(&self) -> &[u8] {
        &self.buffer
    }
}
