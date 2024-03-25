use base64::prelude::*;

const EMPTY_RDB_BASE64_ENCODED: &[u8] = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
const CRLF: &str = "\r\n";

pub fn get_empty_rdb() -> Vec<u8> {
    base64::prelude::BASE64_STANDARD
        .decode(EMPTY_RDB_BASE64_ENCODED)
        .expect("Not a valid base64 encoded empty RDB file")
}

pub fn serialize_rdb(rdb: &[u8]) -> Vec<u8> {
    [b"$", rdb.len().to_string().as_bytes(), CRLF.as_bytes(), rdb].concat()
}
