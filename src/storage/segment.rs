use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Segment {
    pub base_offset: u64,
    log_file: File,
    index_file: File,
    current_offset: u64,
    log_position: u64,
}

impl Segment {
    pub fn new(dir: &PathBuf, base_offset: u64) -> io::Result<Self> {
        // 20-digit zero-padded filenames so they sort correctly on disk
        let log_path = dir.join(format!("{:020}.log", base_offset));
        let idx_path = dir.join(format!("{:020}.idx", base_offset));

        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&log_path)?;

        let index_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&idx_path)?;

        let log_position = log_file.metadata()?.len();

        Ok(Self {
            base_offset,
            log_file,
            index_file,
            current_offset: base_offset,
            log_position,
        })
    }

    pub fn append(&mut self, message: &[u8]) -> io::Result<u64> {
        let offset = self.current_offset;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // log format: [offset 8B][timestamp 8B][length 4B][message NB]
        self.log_file.write_all(&offset.to_be_bytes())?;
        self.log_file.write_all(&timestamp.to_be_bytes())?;
        self.log_file.write_all(&(message.len() as u32).to_be_bytes())?;
        self.log_file.write_all(message)?;

        // index format: [offset 8B][log_byte_position 8B]
        self.index_file.write_all(&offset.to_be_bytes())?;
        self.index_file.write_all(&self.log_position.to_be_bytes())?;

        let entry_size = 8 + 8 + 4 + message.len() as u64;
        self.log_position += entry_size;
        self.current_offset += 1;

        Ok(offset)
    }

    pub fn read(&mut self, offset: u64) -> io::Result<Vec<u8>> {
        // each index entry is [offset 8B][log_position 8B] = 16 bytes
        // +8 skips the stored offset bytes and lands on the log_position bytes
        let idx_pos = (offset - self.base_offset) * 16 + 8;
        self.index_file.seek(SeekFrom::Start(idx_pos))?;

        let mut pos_buf = [0u8; 8];
        self.index_file.read_exact(&mut pos_buf)?;
        let log_pos = u64::from_be_bytes(pos_buf);

        self.log_file.seek(SeekFrom::Start(log_pos))?;

        // skip stored offset (8 bytes) and timestamp (8 bytes)
        self.log_file.seek(SeekFrom::Current(16))?;

        let mut len_buf = [0u8; 4];
        self.log_file.read_exact(&mut len_buf)?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        let mut msg = vec![0u8; msg_len];
        self.log_file.read_exact(&mut msg)?;

        Ok(msg)
    }

    pub fn size(&self) -> u64 {
        self.log_position
    }

    pub fn next_offset(&self) -> u64 {
        self.current_offset
    }
}
