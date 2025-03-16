use core::fmt;
use std::num::ParseIntError;

const CR: u8 = b'\r';
const LF: u8 = b'\n';

pub type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Token {
    SimpleString(String),
    BulkString(Vec<u8>),
}

impl Token {
    pub fn get_bulk_string_data(&self) -> Result<&Vec<u8>> {
        if let Token::BulkString(data) = self {
            Ok(data)
        } else {
            Err(ParseError::Invalid)
        }
    }

    pub fn get_simple_string_data(&self) -> Result<&String> {
        if let Token::SimpleString(data) = self {
            Ok(data)
        } else {
            Err(ParseError::Invalid)
        }
    }
}

#[derive(Debug)]
pub struct ParseResult {
    pub tokens: Vec<Token>,
    pub len: usize,
}

#[derive(Debug)]
pub enum ParseError {
    Invalid,
    Incomplete,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Incomplete => write!(f, "need more data to correctly process message"),
            ParseError::Invalid => write!(f, "containing RESP message is malformed"),
        }
    }
}

impl From<ParseIntError> for ParseError {
    fn from(_value: ParseIntError) -> Self {
        ParseError::Invalid
    }
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(_value: std::str::Utf8Error) -> Self {
        ParseError::Invalid
    }
}

pub fn find_first_crlf(message: &[u8]) -> Option<usize> {
    message.windows(2).position(|window| window == [CR, LF])
}

fn bytes_to_unsigned(bytes: &[u8]) -> Result<usize> {
    Ok(std::str::from_utf8(bytes)?.parse::<usize>()?)
}

fn parse_bytes(message: &[u8], len: usize) -> Result<&[u8]> {
    if len + 2 > message.len() {
        return Err(ParseError::Incomplete);
    }
    if message[len] != CR || message[len + 1] != LF {
        return Err(ParseError::Invalid);
    }
    Ok(&message[..len])
}

fn parse_bulk_string(message: &[u8]) -> Result<ParseResult> {
    assert_eq!(message.first(), Some(&b'$'));

    let size_offset = find_first_crlf(message).ok_or(ParseError::Incomplete)?;
    let data_size = bytes_to_unsigned(&message[1..size_offset])?;
    let data_start = size_offset + 2; // Skip CRLF

    let data = parse_bytes(&message[data_start..], data_size)?;
    let offset = data_start + data_size + 2;

    Ok(ParseResult {
        tokens: vec![Token::BulkString(data.to_vec())],
        len: offset,
    })
}

fn parse_simple_string(message: &[u8]) -> Result<ParseResult> {
    assert_eq!(message.first(), Some(&b'+'));

    let str_size = find_first_crlf(message).ok_or(ParseError::Incomplete)?;
    let data = std::str::from_utf8(&message[1..str_size])?;

    Ok(ParseResult {
        tokens: vec![Token::SimpleString(data.to_owned())],
        len: str_size + 2,
    })
}

fn parse_array(message: &[u8]) -> Result<ParseResult> {
    assert_eq!(message.first(), Some(&b'*'));

    let size_offset = find_first_crlf(message).ok_or(ParseError::Incomplete)?;
    let num_elements = bytes_to_unsigned(&message[1..size_offset])?;

    let mut offset = size_offset + 2;
    let mut tokens = Vec::with_capacity(num_elements);

    for _ in 0..num_elements {
        let mut res = parse_buffer(&message[offset..])?;
        tokens.append(&mut res.tokens);
        offset += res.len;
    }

    Ok(ParseResult {
        tokens,
        len: offset,
    })
}

pub fn parse_buffer(buffer: &[u8]) -> Result<ParseResult> {
    match buffer {
        [first_byte, ..] => match first_byte {
            b'*' => parse_array(buffer),
            b'+' => parse_simple_string(buffer),
            b'$' => parse_bulk_string(buffer),
            byte => unimplemented!(
                "parser does not support parsing messages starting with {:?}",
                byte
            ),
        },
        [] => Err(ParseError::Incomplete),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_string_parsing_works() {
        let message = b"+OK\r\n";
        let result = parse_simple_string(message).unwrap();
        assert_eq!(result.len, message.len());
        assert_eq!(
            result.tokens.first(),
            Some(&Token::SimpleString("OK".to_owned()))
        )
    }

    #[test]
    fn bulk_string_parsing_works() {
        let message = b"$5\r\nhello\r\n";
        let result = parse_bulk_string(message).unwrap();
        assert_eq!(result.len, message.len());
        assert_eq!(
            result.tokens.first(),
            Some(&Token::BulkString(b"hello".to_vec()))
        )
    }

    #[test]
    fn array_parsing_works() {
        let message = b"*2\r\n$3\r\nget\r\n$5\r\nfruit\r\n";
        let result = parse_array(message).unwrap();
        assert_eq!(result.len, message.len());
        assert_eq!(
            result.tokens,
            vec![
                Token::BulkString(b"get".to_vec()),
                Token::BulkString(b"fruit".to_vec())
            ]
        );
    }

    #[test]
    fn array_of_array_parsing_works() {
        let message = b"*2\r\n*2\r\n$3\r\nget\r\n$5\r\nfruit\r\n*2\r\n$3\r\nget\r\n$5\r\nfruit\r\n";
        let result = parse_array(message).unwrap();
        assert_eq!(result.len, message.len());
        assert_eq!(
            result.tokens,
            vec![
                Token::BulkString(b"get".to_vec()),
                Token::BulkString(b"fruit".to_vec()),
                Token::BulkString(b"get".to_vec()),
                Token::BulkString(b"fruit".to_vec()),
            ]
        )
    }
}
