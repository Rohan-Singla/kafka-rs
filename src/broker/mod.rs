use std::{
    fs,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use dashmap::DashMap;
use crate::storage::partition::Partition;

pub struct Broker {
    data_dir: PathBuf,
    // topic_name -> list of partitions (one per partition_id)
    topics: DashMap<String, Vec<Arc<Mutex<Partition>>>>,
    // "group_id:topic:partition_id" -> committed offset
    offsets: DashMap<String, u64>,
}

impl Broker {
    pub fn new(data_dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&data_dir)?;
        Ok(Self {
            data_dir,
            topics: DashMap::new(),
            offsets: DashMap::new(),
        })
    }

    pub fn create_topic(&self, name: &str, num_partitions: u32) -> io::Result<()> {
        if self.topics.contains_key(name) {
            return Ok(());
        }

        let mut partitions = Vec::new();
        for i in 0..num_partitions {
            let dir = self.data_dir.join(name).join(format!("partition-{}", i));
            partitions.push(Arc::new(Mutex::new(Partition::new(dir)?)));
        }

        self.topics.insert(name.to_string(), partitions);
        Ok(())
    }

    pub fn produce(&self, topic: &str, partition_id: u32, message: &[u8]) -> io::Result<u64> {
        let topics = self.topics.get(topic).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("topic '{}' not found", topic))
        })?;

        let partition = topics.get(partition_id as usize).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("partition {} not found", partition_id))
        })?;

        partition.lock().unwrap().append(message)
    }

    pub fn fetch(
        &self,
        topic: &str,
        partition_id: u32,
        offset: u64,
        max_count: usize,
    ) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let topics = self.topics.get(topic).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("topic '{}' not found", topic))
        })?;

        let partition = topics.get(partition_id as usize).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("partition {} not found", partition_id))
        })?;

        partition.lock().unwrap().read_from(offset, max_count)
    }

    pub fn commit_offset(&self, group: &str, topic: &str, partition_id: u32, offset: u64) {
        let key = format!("{}:{}:{}", group, topic, partition_id);
        self.offsets.insert(key, offset);
    }

    pub fn fetch_offset(&self, group: &str, topic: &str, partition_id: u32) -> u64 {
        let key = format!("{}:{}:{}", group, topic, partition_id);
        self.offsets.get(&key).map(|v| *v).unwrap_or(0)
    }
}
