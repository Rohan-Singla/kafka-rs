use std::{fs::{File, OpenOptions}, io, path::PathBuf};



pub struct Segment {
    pub base_offset : u64,
    log_file : File,
    index_file : File,
    current_offset : u64,
    log_position : u64,
}

impl Segment {
     // "Create or open a segment. Tell me which folder to put it in and what offset it starts at."
    pub fn new (dir: &PathBuf, base_offset : u64) -> io::Result<Self> {
       
      //   "Name the file 00000000000000000000.log. The 20 zeros are just so filenames sort correctly — segment
 // starting at 0 sorts before segment starting at 100."
        let log_path = dir.join(format!("{:020}.log",base_offset));
        let idx_path = dir.join(format!("{:020}.idx",base_offset));
   
        let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&log_path)?;
 //"Open the notebook. If it doesn't exist make a new one. Always write at the end. Never erase what's
 // already there."
        let index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&idx_path)?;

        let log_position = log_file.metadata()?.len();
//  "If this file already existed from before a crash, how many bytes are already in it? Start from there."
        Ok(Self {
            base_offset,
            log_file,
            index_file,
            current_offset : base_offset,
            log_position
        })
        
   
    }
}