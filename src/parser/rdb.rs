use super::resp::{find_first_crlf, ParseError, Result};

pub struct RdbParseResult {
    pub rdb: Vec<u8>,
    pub len: usize,
}

pub fn parse_rdb_payload(message: &[u8]) -> Result<RdbParseResult> {
    let crlf = find_first_crlf(message);
    match crlf {
        Some(len) => {
            let n = std::str::from_utf8(&message[1..len])
                .map_err(|_| ParseError::Invalid)?
                .parse::<usize>()
                .map_err(|_| ParseError::Invalid)?;
            let rdb_start = len + 2;
            if rdb_start + n > message.len() {
                return Err(ParseError::Incomplete);
            }
            Ok(RdbParseResult {
                rdb: message[rdb_start..rdb_start + n].to_vec(),
                len: rdb_start + n,
            })
        }
        None => Err(ParseError::Incomplete),
    }
}
