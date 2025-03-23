use std::{
    io::{Read, Write},
    net::TcpStream,
};

pub struct ClientStream {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl ClientStream {
    pub fn read(&mut self) -> std::io::Result<()> {
        let mut buf = [0; 1024];
        let bytes_read = loop {
            match self.stream.read(&mut buf) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Connection closed",
                    ))
                }
                Ok(n) => break n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        };
        self.buffer.extend_from_slice(&buf[..bytes_read]);
        Ok(())
    }

    pub fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data)?;
        Ok(())
    }
}
