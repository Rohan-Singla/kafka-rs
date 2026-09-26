use std::fs;
use std::path::{Path, PathBuf};

use super::record::Record;
use super::segment::Segment;
use crate::error::{Error, Result};

/// Roll to a fresh segment once the active one passes this size.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// An ordered run of segments that together form one append only log.
///
/// Only the last segment is written to. The earlier ones are sealed and stay on
/// disk so a consumer can still replay them.
pub struct Partition {
    dir: PathBuf,
    segments: Vec<Segment>,
    max_segment_size: u64,
}

impl Partition {
    pub fn open(dir: PathBuf) -> Result<Self> {
        Self::with_segment_size(dir, DEFAULT_SEGMENT_SIZE)
    }

    pub fn with_segment_size(dir: PathBuf, max_segment_size: u64) -> Result<Self> {
        fs::create_dir_all(&dir)?;

        let mut base_offsets = Self::existing_base_offsets(&dir)?;
        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let mut segments = Vec::with_capacity(base_offsets.len());
        for base_offset in base_offsets {
            segments.push(Segment::open(&dir, base_offset)?);
        }

        // A crash right after rolling can leave an empty trailing segment. Drop
        // it so the active segment is always the one holding the highest offset.
        while segments.len() > 1 && segments.last().is_some_and(|s| s.is_empty()) {
            let dead = segments.pop().unwrap();
            let (log_path, index_path) = dead.paths();
            let (log_path, index_path) = (log_path.to_path_buf(), index_path.to_path_buf());
            drop(dead);
            let _ = fs::remove_file(log_path);
            let _ = fs::remove_file(index_path);
        }

        Ok(Self {
            dir,
            segments,
            max_segment_size,
        })
    }

    fn existing_base_offsets(dir: &Path) -> Result<Vec<u64>> {
        let mut offsets = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("log") {
                continue;
            }
            if let Some(base) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                offsets.push(base);
            }
        }
        offsets.sort_unstable();
        Ok(offsets)
    }

    pub fn append(&mut self, value: &[u8]) -> Result<u64> {
        if self.active().size() >= self.max_segment_size {
            let base_offset = self.active().next_offset();
            self.segments.push(Segment::open(&self.dir, base_offset)?);
        }
        self.active_mut().append(value)
    }

    pub fn read(&self, offset: u64) -> Result<Record> {
        let index = self.segment_for(offset)?;
        self.segments[index].read(offset)
    }

    /// Read up to `max_count` records from `start_offset`, following the log
    /// across segment boundaries.
    pub fn read_from(&self, start_offset: u64, max_count: usize) -> Result<Vec<Record>> {
        let end = self.next_offset();
        if start_offset >= end || max_count == 0 {
            return Ok(Vec::new());
        }
        if start_offset < self.start_offset() {
            return Err(Error::OffsetOutOfRange {
                offset: start_offset,
                next_offset: end,
            });
        }

        let mut records = Vec::new();
        let mut offset = start_offset;

        while records.len() < max_count && offset < end {
            let index = self.segment_for(offset)?;
            let batch = self.segments[index].read_from(offset, max_count - records.len())?;
            if batch.is_empty() {
                break;
            }
            offset = batch[batch.len() - 1].offset + 1;
            records.extend(batch);
        }

        Ok(records)
    }

    pub fn next_offset(&self) -> u64 {
        self.active().next_offset()
    }

    pub fn start_offset(&self) -> u64 {
        self.segments[0].base_offset
    }

    pub fn size(&self) -> u64 {
        self.segments.iter().map(|s| s.size()).sum()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn sync(&self) -> Result<()> {
        self.active().sync()
    }

    fn active(&self) -> &Segment {
        self.segments.last().expect("a partition always has one segment")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("a partition always has one segment")
    }

    /// The last segment whose base offset is at or below `offset`. Base offsets
    /// are sorted, so this is a binary search.
    fn segment_for(&self, offset: u64) -> Result<usize> {
        match self
            .segments
            .binary_search_by(|s| s.base_offset.cmp(&offset))
        {
            Ok(index) => Ok(index),
            Err(0) => Err(Error::OffsetOutOfRange {
                offset,
                next_offset: self.next_offset(),
            }),
            Err(index) => Ok(index - 1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn appends_and_reads_back_in_order() {
        let dir = temp_dir();
        let mut partition = Partition::open(dir.path().to_path_buf()).unwrap();

        for i in 0..100 {
            assert_eq!(partition.append(format!("m{}", i).as_bytes()).unwrap(), i);
        }
        assert_eq!(partition.next_offset(), 100);

        let records = partition.read_from(0, 1000).unwrap();
        assert_eq!(records.len(), 100);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.offset, i as u64);
            assert_eq!(record.value, format!("m{}", i).into_bytes());
        }
    }

    #[test]
    fn rolls_to_a_new_segment_past_the_size_limit() {
        let dir = temp_dir();
        // Small enough that a handful of records forces several rolls.
        let mut partition = Partition::with_segment_size(dir.path().to_path_buf(), 128).unwrap();

        for i in 0..40 {
            partition.append(format!("message number {}", i).as_bytes()).unwrap();
        }

        assert!(
            partition.segment_count() > 1,
            "expected a roll, got {} segment(s)",
            partition.segment_count()
        );
        assert_eq!(partition.next_offset(), 40);
    }

    #[test]
    fn reads_span_segment_boundaries() {
        let dir = temp_dir();
        let mut partition = Partition::with_segment_size(dir.path().to_path_buf(), 128).unwrap();
        for i in 0..40 {
            partition.append(format!("message number {}", i).as_bytes()).unwrap();
        }
        assert!(partition.segment_count() > 1);

        let all = partition.read_from(0, 40).unwrap();
        assert_eq!(all.len(), 40);
        for (i, record) in all.iter().enumerate() {
            assert_eq!(record.offset, i as u64);
        }

        // A window that starts inside one segment and ends inside another.
        let middle = partition.read_from(5, 20).unwrap();
        assert_eq!(middle.len(), 20);
        assert_eq!(middle[0].offset, 5);
        assert_eq!(middle[19].offset, 24);
    }

    #[test]
    fn point_reads_find_the_right_segment() {
        let dir = temp_dir();
        let mut partition = Partition::with_segment_size(dir.path().to_path_buf(), 128).unwrap();
        for i in 0..40 {
            partition.append(format!("message number {}", i).as_bytes()).unwrap();
        }

        for i in 0..40u64 {
            let record = partition.read(i).unwrap();
            assert_eq!(record.offset, i);
            assert_eq!(record.value, format!("message number {}", i).into_bytes());
        }
    }

    #[test]
    fn reopening_continues_the_offset_sequence() {
        let dir = temp_dir();
        {
            let mut partition =
                Partition::with_segment_size(dir.path().to_path_buf(), 128).unwrap();
            for i in 0..40 {
                partition.append(format!("message number {}", i).as_bytes()).unwrap();
            }
        }

        let mut partition = Partition::with_segment_size(dir.path().to_path_buf(), 128).unwrap();
        assert_eq!(partition.next_offset(), 40);
        assert_eq!(partition.append(b"after restart").unwrap(), 40);
        assert_eq!(partition.read(0).unwrap().value, b"message number 0");
        assert_eq!(partition.read(40).unwrap().value, b"after restart");
    }

    #[test]
    fn reading_past_the_end_yields_nothing() {
        let dir = temp_dir();
        let mut partition = Partition::open(dir.path().to_path_buf()).unwrap();
        partition.append(b"one").unwrap();

        assert!(partition.read_from(1, 10).unwrap().is_empty());
        assert!(partition.read_from(99, 10).unwrap().is_empty());
        assert!(matches!(
            partition.read(1),
            Err(Error::OffsetOutOfRange { .. })
        ));
    }

    #[test]
    fn a_fresh_partition_is_empty_not_broken() {
        let dir = temp_dir();
        let partition = Partition::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(partition.next_offset(), 0);
        assert_eq!(partition.start_offset(), 0);
        assert_eq!(partition.size(), 0);
        assert!(partition.read_from(0, 10).unwrap().is_empty());
    }
}
