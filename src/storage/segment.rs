use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use super::fileio::{read_exact_at, write_all_at};
use super::record::{self, HEADER_LEN, Header, MAX_RECORD_SIZE, Record};
use crate::error::{Error, Result};

pub const INDEX_ENTRY_LEN: u64 = 16;

pub struct Segment {
    pub base_offset: u64,
    log: File,
    index: File,
    log_path: PathBuf,
    index_path: PathBuf,
    next_offset: u64,
    log_size: u64,
}

impl Segment {
    pub fn open(dir: &Path, base_offset: u64) -> Result<Self> {
        let log_path = dir.join(format!("{:020}.log", base_offset));
        let index_path = dir.join(format!("{:020}.idx", base_offset));

        let log = Self::open_rw(&log_path)?;
        let index = Self::open_rw(&index_path)?;

        let mut segment = Self {
            base_offset,
            log,
            index,
            log_path,
            index_path,
            next_offset: base_offset,
            log_size: 0,
        };
        segment.recover()?;
        Ok(segment)
    }

    fn open_rw(path: &Path) -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?)
    }

    fn recover(&mut self) -> Result<()> {
        let file_len = self.log.metadata()?.len();
        let mut positions: Vec<u64> = Vec::new();
        let mut position = 0u64;
        let mut expected_offset = self.base_offset;

        while position + HEADER_LEN as u64 <= file_len {
            let mut header_buf = [0u8; HEADER_LEN];
            if read_exact_at(&self.log, &mut header_buf, position).is_err() {
                break;
            }
            let header = record::decode_header(&header_buf);

            if header.offset != expected_offset || header.length > MAX_RECORD_SIZE {
                break;
            }
            let end = position + HEADER_LEN as u64 + header.length as u64;
            if end > file_len {
                break;
            }

            let mut value = vec![0u8; header.length as usize];
            if read_exact_at(&self.log, &mut value, position + HEADER_LEN as u64).is_err() {
                break;
            }
            if record::checksum(header.offset, header.timestamp, &value) != header.crc {
                break;
            }

            positions.push(position);
            position = end;
            expected_offset += 1;
        }

        if position < file_len {
            tracing::warn!(
                segment = %self.log_path.display(),
                "truncating {} torn bytes at the tail of the log",
                file_len - position
            );
            self.log.set_len(position)?;
        }

        self.rewrite_index(&positions)?;
        self.log_size = position;
        self.next_offset = expected_offset;
        Ok(())
    }

    fn rewrite_index(&mut self, positions: &[u64]) -> Result<()> {
        let mut buf = Vec::with_capacity(positions.len() * INDEX_ENTRY_LEN as usize);
        for (i, position) in positions.iter().enumerate() {
            buf.extend_from_slice(&(self.base_offset + i as u64).to_be_bytes());
            buf.extend_from_slice(&position.to_be_bytes());
        }
        self.index.set_len(0)?;
        if !buf.is_empty() {
            write_all_at(&self.index, &buf, 0)?;
        }
        Ok(())
    }

    pub fn append(&mut self, value: &[u8]) -> Result<u64> {
        if value.len() as u64 > MAX_RECORD_SIZE as u64 {
            return Err(Error::Protocol(format!(
                "message of {} bytes exceeds the {} byte record limit",
                value.len(),
                MAX_RECORD_SIZE
            )));
        }

        let offset = self.next_offset;
        let timestamp = record::now_millis();
        let encoded = record::encode(offset, timestamp, value);
        let position = self.log_size;

        write_all_at(&self.log, &encoded, position)?;

        let index_position = (offset - self.base_offset) * INDEX_ENTRY_LEN;
        let mut entry = [0u8; INDEX_ENTRY_LEN as usize];
        entry[0..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..16].copy_from_slice(&position.to_be_bytes());
        write_all_at(&self.index, &entry, index_position)?;

        self.log_size += encoded.len() as u64;
        self.next_offset += 1;
        Ok(offset)
    }

    pub fn read(&self, offset: u64) -> Result<Record> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Err(Error::OffsetOutOfRange {
                offset,
                next_offset: self.next_offset,
            });
        }

        let index_position = (offset - self.base_offset) * INDEX_ENTRY_LEN + 8;
        let mut position_buf = [0u8; 8];
        read_exact_at(&self.index, &mut position_buf, index_position)?;
        let position = u64::from_be_bytes(position_buf);

        self.read_at_position(position)
    }

    fn read_at_position(&self, position: u64) -> Result<Record> {
        let mut header_buf = [0u8; HEADER_LEN];
        read_exact_at(&self.log, &mut header_buf, position)?;
        let header = record::decode_header(&header_buf);

        if header.length > MAX_RECORD_SIZE {
            return Err(Error::Corrupt {
                position,
                detail: format!("record length {} is implausible", header.length),
            });
        }

        let mut value = vec![0u8; header.length as usize];
        read_exact_at(&self.log, &mut value, position + HEADER_LEN as u64)?;

        self.verify(position, &header, &value)?;
        Ok(Record {
            offset: header.offset,
            timestamp: header.timestamp,
            value,
        })
    }

    fn verify(&self, position: u64, header: &Header, value: &[u8]) -> Result<()> {
        let actual = record::checksum(header.offset, header.timestamp, value);
        if actual != header.crc {
            return Err(Error::Corrupt {
                position,
                detail: format!(
                    "checksum mismatch, stored {} computed {}",
                    header.crc, actual
                ),
            });
        }
        Ok(())
    }

    pub fn read_from(
        &self,
        start_offset: u64,
        max_count: usize,
        max_bytes: u64,
    ) -> Result<Vec<Record>> {
        if max_count == 0 || start_offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let start_offset = start_offset.max(self.base_offset);

        let index_position = (start_offset - self.base_offset) * INDEX_ENTRY_LEN + 8;
        let mut position_buf = [0u8; 8];
        read_exact_at(&self.index, &mut position_buf, index_position)?;
        let mut position = u64::from_be_bytes(position_buf);

        let available = (self.next_offset - start_offset) as usize;
        let wanted = max_count.min(available);
        let mut records = Vec::with_capacity(wanted);
        let mut bytes = 0u64;

        for _ in 0..wanted {
            let record = self.read_at_position(position)?;
            let size = record.size_on_disk();
            if bytes + size > max_bytes {
                break;
            }
            bytes += size;
            position += size;
            records.push(record);
        }
        Ok(records)
    }

    pub fn sync(&self) -> Result<()> {
        self.log.sync_data()?;
        self.index.sync_data()?;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.log_size
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn is_empty(&self) -> bool {
        self.next_offset == self.base_offset
    }

    pub fn paths(&self) -> (&Path, &Path) {
        (&self.log_path, &self.index_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn append_assigns_sequential_offsets_from_base() {
        let dir = temp_dir();
        let mut segment = Segment::open(dir.path(), 100).unwrap();

        assert_eq!(segment.append(b"a").unwrap(), 100);
        assert_eq!(segment.append(b"b").unwrap(), 101);
        assert_eq!(segment.append(b"c").unwrap(), 102);
        assert_eq!(segment.next_offset(), 103);
    }

    #[test]
    fn read_returns_what_was_appended() {
        let dir = temp_dir();
        let mut segment = Segment::open(dir.path(), 0).unwrap();
        for i in 0..50 {
            segment.append(format!("message {}", i).as_bytes()).unwrap();
        }

        for i in 0..50u64 {
            let record = segment.read(i).unwrap();
            assert_eq!(record.offset, i);
            assert_eq!(record.value, format!("message {}", i).into_bytes());
        }
    }

    #[test]
    fn read_rejects_offsets_outside_the_segment() {
        let dir = temp_dir();
        let mut segment = Segment::open(dir.path(), 10).unwrap();
        segment.append(b"only").unwrap();

        assert!(matches!(
            segment.read(9),
            Err(Error::OffsetOutOfRange { .. })
        ));
        assert!(matches!(
            segment.read(11),
            Err(Error::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn read_from_walks_forward_and_respects_max_count() {
        let dir = temp_dir();
        let mut segment = Segment::open(dir.path(), 0).unwrap();
        for i in 0..20 {
            segment.append(format!("m{}", i).as_bytes()).unwrap();
        }

        let batch = segment.read_from(5, 4, u64::MAX).unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].offset, 5);
        assert_eq!(batch[3].offset, 8);

        let tail = segment.read_from(18, 100, u64::MAX).unwrap();
        assert_eq!(tail.len(), 2);
        assert!(segment.read_from(20, 10, u64::MAX).unwrap().is_empty());
    }

    #[test]
    fn reopening_resumes_at_the_next_offset() {
        let dir = temp_dir();
        {
            let mut segment = Segment::open(dir.path(), 0).unwrap();
            segment.append(b"first").unwrap();
            segment.append(b"second").unwrap();
        }

        let mut segment = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(segment.next_offset(), 2);
        assert_eq!(segment.append(b"third").unwrap(), 2);
        assert_eq!(segment.read(0).unwrap().value, b"first");
        assert_eq!(segment.read(2).unwrap().value, b"third");
    }

    #[test]
    fn recovery_truncates_a_torn_tail() {
        let dir = temp_dir();
        let good_size;
        {
            let mut segment = Segment::open(dir.path(), 0).unwrap();
            segment.append(b"complete one").unwrap();
            segment.append(b"complete two").unwrap();
            good_size = segment.size();
        }

        let log_path = dir.path().join(format!("{:020}.log", 0));
        let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
        file.write_all(&3u64.to_be_bytes()).unwrap();
        file.write_all(&[0u8; 6]).unwrap();
        drop(file);
        assert!(fs::metadata(&log_path).unwrap().len() > good_size);

        let segment = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(segment.next_offset(), 2);
        assert_eq!(segment.size(), good_size);
        assert_eq!(fs::metadata(&log_path).unwrap().len(), good_size);
        assert_eq!(segment.read(1).unwrap().value, b"complete two");
    }

    #[test]
    fn recovery_stops_at_a_flipped_bit() {
        let dir = temp_dir();
        {
            let mut segment = Segment::open(dir.path(), 0).unwrap();
            segment.append(b"keep me").unwrap();
            segment.append(b"corrupt me").unwrap();
            segment.append(b"unreachable").unwrap();
        }

        let log_path = dir.path().join(format!("{:020}.log", 0));
        let mut bytes = fs::read(&log_path).unwrap();
        let second_payload_start = HEADER_LEN + b"keep me".len() + HEADER_LEN;
        bytes[second_payload_start] ^= 0xFF;
        fs::write(&log_path, &bytes).unwrap();

        let segment = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(segment.next_offset(), 1);
        assert_eq!(segment.read(0).unwrap().value, b"keep me");
    }

    #[test]
    fn index_is_rebuilt_from_the_log() {
        let dir = temp_dir();
        {
            let mut segment = Segment::open(dir.path(), 0).unwrap();
            for i in 0..10 {
                segment.append(format!("m{}", i).as_bytes()).unwrap();
            }
        }

        let index_path = dir.path().join(format!("{:020}.idx", 0));
        fs::write(&index_path, b"").unwrap();

        let segment = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(segment.next_offset(), 10);
        assert_eq!(
            fs::metadata(&index_path).unwrap().len(),
            10 * INDEX_ENTRY_LEN
        );
        assert_eq!(segment.read(7).unwrap().value, b"m7");
    }

    #[test]
    fn empty_payloads_are_valid_records() {
        let dir = temp_dir();
        let mut segment = Segment::open(dir.path(), 0).unwrap();
        segment.append(b"").unwrap();
        segment.append(b"after").unwrap();

        assert!(segment.read(0).unwrap().value.is_empty());
        assert_eq!(segment.read(1).unwrap().value, b"after");
    }
}
