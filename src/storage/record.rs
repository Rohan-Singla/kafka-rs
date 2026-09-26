use serde::{Deserialize, Serialize};

pub const HEADER_LEN: usize = 24;

pub const MAX_RECORD_SIZE: u32 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub offset: u64,
    pub timestamp: u64,
    pub value: Vec<u8>,
}

impl Record {
    pub fn size_on_disk(&self) -> u64 {
        (HEADER_LEN + self.value.len()) as u64
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub offset: u64,
    pub timestamp: u64,
    pub crc: u32,
    pub length: u32,
}

pub fn checksum(offset: u64, timestamp: u64, value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&offset.to_be_bytes());
    hasher.update(&timestamp.to_be_bytes());
    hasher.update(&(value.len() as u32).to_be_bytes());
    hasher.update(value);
    hasher.finalize()
}

pub fn encode(offset: u64, timestamp: u64, value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + value.len());
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&timestamp.to_be_bytes());
    buf.extend_from_slice(&checksum(offset, timestamp, value).to_be_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    buf
}

pub fn decode_header(buf: &[u8; HEADER_LEN]) -> Header {
    Header {
        offset: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
        timestamp: u64::from_be_bytes(buf[8..16].try_into().unwrap()),
        crc: u32::from_be_bytes(buf[16..20].try_into().unwrap()),
        length: u32::from_be_bytes(buf[20..24].try_into().unwrap()),
    }
}

pub fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_header() {
        let value = b"order #1";
        let encoded = encode(7, 1_700_000_000_000, value);
        assert_eq!(encoded.len(), HEADER_LEN + value.len());

        let header = decode_header(&encoded[..HEADER_LEN].try_into().unwrap());
        assert_eq!(header.offset, 7);
        assert_eq!(header.timestamp, 1_700_000_000_000);
        assert_eq!(header.length, value.len() as u32);
        assert_eq!(header.crc, checksum(7, 1_700_000_000_000, value));
        assert_eq!(&encoded[HEADER_LEN..], value);
    }

    #[test]
    fn checksum_catches_payload_corruption() {
        let good = checksum(1, 2, b"hello");
        let bad = checksum(1, 2, b"hellp");
        assert_ne!(good, bad);
    }

    #[test]
    fn checksum_catches_metadata_corruption() {
        assert_ne!(checksum(1, 2, b"hello"), checksum(2, 2, b"hello"));
        assert_ne!(checksum(1, 2, b"hello"), checksum(1, 3, b"hello"));
    }
}
