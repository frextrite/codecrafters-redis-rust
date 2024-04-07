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

pub fn find_first_crlf(message: &[u8]) -> Option<usize> {
    if message.len() < 2 {
        return None;
    }
    (0..message.len() - 1).find(|&i| message[i] == CR && message[i + 1] == LF)
}

fn bytes_to_unsigned(bytes: &[u8]) -> Option<usize> {
    let mut integer = 0;
    for digit in bytes {
        if digit.is_ascii_digit() {
            integer = integer * 10 + (digit - b'0') as usize;
        } else {
            return None;
        }
    }

    Some(integer)
}

fn parse_bytes(message: &[u8], len: usize) -> Result<Vec<u8>> {
    if len + 2 > message.len() {
        return Err(ParseError::Incomplete);
    }
    if message[len] != CR || message[len + 1] != LF {
        return Err(ParseError::Invalid);
    }
    Ok(message[..len].to_vec())
}

fn parse_bulk_string(message: &[u8]) -> Result<ParseResult> {
    assert!(message.first().unwrap() == &b'$');

    let crlf = find_first_crlf(message);
    match crlf {
        Some(mut len) => {
            let n = bytes_to_unsigned(&message[1..len]).ok_or(ParseError::Invalid)?;
            len += 2;
            let data = parse_bytes(&message[len..], n)?;
            len += n + 2;
            Ok(ParseResult {
                tokens: vec![Token::BulkString(data)],
                len,
            })
        }
        None => Err(ParseError::Incomplete),
    }
}

fn parse_simple_string(message: &[u8]) -> Result<ParseResult> {
    assert!(message.first().unwrap() == &b'+');

    let crlf = find_first_crlf(message);
    match crlf {
        Some(len) => Ok(ParseResult {
            tokens: vec![Token::SimpleString(
                std::str::from_utf8(&message[1..len]).unwrap().to_owned(),
            )],
            len: len + 2,
        }),
        None => Err(ParseError::Incomplete),
    }
}

fn parse_array(message: &[u8]) -> Result<ParseResult> {
    assert!(message.first().unwrap() == &b'*');

    let mut tokens = vec![];
    let crlf = find_first_crlf(message);
    match crlf {
        Some(mut len) => {
            let n = bytes_to_unsigned(&message[1..len]).ok_or(ParseError::Invalid)?;
            len += 2;
            for _ in 0..n {
                let mut res = parse_buffer(&message[len..])?;
                tokens.append(&mut res.tokens);
                len += res.len;
            }
            Ok(ParseResult { tokens, len })
        }
        None => Err(ParseError::Incomplete),
    }
}

pub fn parse_buffer(buffer: &[u8]) -> Result<ParseResult> {
    match buffer.first() {
        Some(b'*') => parse_array(buffer),
        Some(b'+') => parse_simple_string(buffer),
        Some(b'$') => parse_bulk_string(buffer),
        Some(byte) => unimplemented!(
            "parser does not support parsing messages starting with {:?}",
            byte
        ),
        None => Err(ParseError::Incomplete),
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
