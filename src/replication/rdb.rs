use base64::prelude::*;

use crate::parser::resp::find_first_crlf;

const EMPTY_RDB_BASE64_ENCODED: &[u8] = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
const CRLF: &str = "\r\n";

pub struct RdbParseResult {
    pub rdb: Vec<u8>,
    pub len: usize,
}

pub fn get_empty_rdb() -> Vec<u8> {
    base64::prelude::BASE64_STANDARD
        .decode(EMPTY_RDB_BASE64_ENCODED)
        .expect("Not a valid base64 encoded empty RDB file")
}

pub fn serialize_rdb(rdb: &[u8]) -> Vec<u8> {
    [b"$", rdb.len().to_string().as_bytes(), CRLF.as_bytes(), rdb].concat()
}

// TODO: Add error handling in case integer parsing fails or
// if the payload is incomplete
// TODO: Check for valid RDB payload
pub fn parse_rdb_payload(message: &[u8]) -> RdbParseResult {
    let crlf = find_first_crlf(message);
    match crlf {
        Some(len) => {
            let n = std::str::from_utf8(&message[1..len])
                .unwrap()
                .parse::<usize>()
                .unwrap();
            RdbParseResult {
                rdb: message[len + 2..len + 2 + n].to_vec(),
                len: len + 2 + n,
            }
        }
        None => panic!("Invalid RDB payload"),
    }
}
