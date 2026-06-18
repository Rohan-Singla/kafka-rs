use std::{fs, io, path::PathBuf};
use super::segment::Segment;

// roll to a new segment file after 64MB
const MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

pub struct Partition {
    dir: PathBuf,
    segments: Vec<Segment>,
}

impl Partition {
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;

        let mut base_offsets = Self::find_existing_segments(&dir)?;
        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let mut segments = Vec::new();
        for offset in base_offsets {
            segments.push(Segment::new(&dir, offset)?);
        }

        Ok(Self { dir, segments })
    }

    fn find_existing_segments(dir: &PathBuf) -> io::Result<Vec<u64>> {
        let mut offsets = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(offset) = stem.parse::<u64>() {
                        offsets.push(offset);
                    }
                }
            }
        }
        offsets.sort();
        Ok(offsets)
    }

    pub fn append(&mut self, message: &[u8]) -> io::Result<u64> {
        if self.active().size() >= MAX_SEGMENT_SIZE {
            let next = self.active().next_offset();
            self.segments.push(Segment::new(&self.dir, next)?);
        }
        self.active_mut().append(message)
    }

    pub fn read(&mut self, offset: u64) -> io::Result<Vec<u8>> {
        let idx = self.segment_for(offset)?;
        self.segments[idx].read(offset)
    }

    pub fn read_from(&mut self, start_offset: u64, max_count: usize) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let mut results = Vec::new();
        let end = self.next_offset();
        let mut offset = start_offset;

        while offset < end && results.len() < max_count {
            match self.read(offset) {
                Ok(msg) => results.push((offset, msg)),
                Err(_) => break,
            }
            offset += 1;
        }

        Ok(results)
    }

    pub fn next_offset(&self) -> u64 {
        self.active().next_offset()
    }

    fn active(&self) -> &Segment {
        self.segments.last().unwrap()
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments.last_mut().unwrap()
    }

    fn segment_for(&self, offset: u64) -> io::Result<usize> {
        for i in (0..self.segments.len()).rev() {
            if self.segments[i].base_offset <= offset {
                return Ok(i);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no segment contains offset {}", offset),
        ))
    }
}
