use crate::parser::resp::ParseError;
use crate::parser::resp::Result;
use std::{io::Read, net::TcpStream};

pub type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

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
    offset: usize,
}

impl Connection {
    fn read_from_stream(&mut self) -> ConnectionResult<()> {
        let read = self.stream.read(&mut self.buffer[self.offset..])?;
        self.offset += read;
        Ok(())
    }
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: vec![0; 4096],
            offset: 0,
        }
    }

    pub fn from_existing(stream: TcpStream, buffer: &[u8]) -> Self {
        let mut conn = Self::new(stream);
        conn.buffer[..buffer.len()].copy_from_slice(buffer);
        conn.offset = buffer.len();
        conn
    }

    pub fn get_buffer(&self) -> &[u8] {
        &self.buffer[..self.offset]
    }

    pub fn consume(&mut self, n: usize) {
        self.offset -= n;
        for i in 0..self.offset {
            self.buffer[i] = self.buffer[i + n];
        }
    }

    pub fn try_parse<F, T>(&mut self, f: F) -> ConnectionResult<T>
    where
        F: Fn(&[u8]) -> Result<T>,
    {
        loop {
            match f(&self.buffer[..self.offset]) {
                Ok(value) => return Ok(value),
                Err(ParseError::Incomplete) => {
                    self.read_from_stream()?;
                }
                Err(e) => return Err(ConnectionError::Parse(e)),
            }
        }
    }
}
