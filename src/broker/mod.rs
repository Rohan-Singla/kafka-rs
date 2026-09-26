pub mod groups;
pub mod offsets;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::partition::DEFAULT_SEGMENT_SIZE;
use crate::storage::{Partition, Record};
use groups::{Assignment, Coordinator, DEFAULT_SESSION_TIMEOUT, GroupDescription, GroupSummary};
use offsets::OffsetStore;

const OFFSETS_FILE: &str = "__offsets.json";

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub fsync: bool,
    pub segment_size: u64,
    pub session_timeout: Duration,
}

impl BrokerConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            fsync: false,
            segment_size: DEFAULT_SEGMENT_SIZE,
            session_timeout: DEFAULT_SESSION_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition: u32,
    pub start_offset: u64,
    pub next_offset: u64,
    pub size_bytes: u64,
    pub segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
}

pub struct Broker {
    config: BrokerConfig,
    topics: DashMap<String, Vec<Arc<RwLock<Partition>>>>,
    offsets: Arc<OffsetStore>,
    coordinator: Coordinator,
    create_lock: tokio::sync::Mutex<()>,
}

impl Broker {
    pub fn open(config: BrokerConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;

        let offsets = Arc::new(OffsetStore::open(
            config.data_dir.join(OFFSETS_FILE),
            config.fsync,
        )?);
        let coordinator = Coordinator::new(config.session_timeout);
        let broker = Self {
            topics: DashMap::new(),
            config,
            offsets,
            coordinator,
            create_lock: tokio::sync::Mutex::new(()),
        };
        broker.load_topics_from_disk()?;
        Ok(broker)
    }

    fn load_topics_from_disk(&self) -> Result<()> {
        for entry in fs::read_dir(&self.config.data_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if validate_topic_name(name).is_err() {
                tracing::warn!("ignoring directory with an invalid topic name: {}", name);
                continue;
            }

            let mut numbered: Vec<u32> = fs::read_dir(&path)?
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .filter_map(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|n| n.strip_prefix("partition-"))
                        .and_then(|n| n.parse::<u32>().ok())
                })
                .collect();
            numbered.sort_unstable();

            if numbered.is_empty() {
                continue;
            }
            if numbered != (0..numbered.len() as u32).collect::<Vec<_>>() {
                tracing::warn!(
                    "topic '{}' has non-contiguous partition directories {:?}, skipping",
                    name,
                    numbered
                );
                continue;
            }

            let mut partitions = Vec::with_capacity(numbered.len());
            for id in &numbered {
                let dir = path.join(format!("partition-{}", id));
                partitions.push(Arc::new(RwLock::new(Partition::with_segment_size(
                    dir,
                    self.config.segment_size,
                )?)));
            }

            tracing::info!(
                "loaded topic '{}' with {} partition(s)",
                name,
                partitions.len()
            );
            self.topics.insert(name.to_string(), partitions);
        }
        Ok(())
    }

    pub async fn create_topic(&self, name: &str, partition_count: u32) -> Result<()> {
        validate_topic_name(name)?;
        if partition_count == 0 {
            return Err(Error::Protocol(
                "a topic needs at least one partition".to_string(),
            ));
        }

        let _creating = self.create_lock.lock().await;

        if let Some(existing) = self.topics.get(name).map(|p| p.len() as u32) {
            if existing != partition_count {
                return Err(Error::Protocol(format!(
                    "topic '{}' already exists with {} partition(s), not {}",
                    name, existing, partition_count
                )));
            }
            return Ok(());
        }

        let data_dir = self.config.data_dir.clone();
        let segment_size = self.config.segment_size;
        let topic = name.to_string();

        let partitions = spawn_blocking(move || {
            let topic_dir = data_dir.join(&topic);
            let mut partitions = Vec::with_capacity(partition_count as usize);
            let mut created: Vec<PathBuf> = Vec::new();

            for id in 0..partition_count {
                let dir = topic_dir.join(format!("partition-{}", id));
                let preexisting = dir.exists();
                if !preexisting {
                    created.push(dir.clone());
                }

                match Partition::with_segment_size(dir, segment_size) {
                    Ok(partition) => partitions.push(Arc::new(RwLock::new(partition))),
                    Err(e) => {
                        drop(partitions);
                        for dir in &created {
                            let _ = fs::remove_dir_all(dir);
                        }
                        if fs::remove_dir(&topic_dir).is_err() {
                            tracing::warn!(
                                "topic '{}' failed to create but {} still holds partition data \
                                 from an earlier attempt, so recreating it will be refused until \
                                 that directory is removed by hand",
                                topic,
                                topic_dir.display()
                            );
                        }
                        return Err(e);
                    }
                }
            }
            Ok(partitions)
        })
        .await?;

        self.topics.insert(name.to_string(), partitions);
        tracing::info!(
            "created topic '{}' with {} partition(s)",
            name,
            partition_count
        );
        Ok(())
    }

    pub fn partition_count(&self, topic: &str) -> Result<u32> {
        self.topics
            .get(topic)
            .map(|p| p.len() as u32)
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))
    }

    fn partition_counts(&self) -> HashMap<String, u32> {
        self.topics
            .iter()
            .map(|e| (e.key().clone(), e.value().len() as u32))
            .collect()
    }

    fn partition_handle(&self, topic: &str, partition: u32) -> Result<Arc<RwLock<Partition>>> {
        let entry = self
            .topics
            .get(topic)
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?;
        entry
            .get(partition as usize)
            .cloned()
            .ok_or_else(|| Error::UnknownPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    pub async fn produce(&self, topic: &str, partition: u32, value: Vec<u8>) -> Result<u64> {
        let handle = self.partition_handle(topic, partition)?;
        let fsync = self.config.fsync;

        spawn_blocking(move || {
            let mut guard = handle.write().unwrap_or_else(|e| e.into_inner());
            let offset = guard.append(&value)?;
            if fsync {
                guard.sync()?;
            }
            Ok(offset)
        })
        .await
    }

    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_count: usize,
    ) -> Result<Vec<Record>> {
        let handle = self.partition_handle(topic, partition)?;
        let max_count = max_count.clamp(1, MAX_FETCH_COUNT);

        spawn_blocking(move || {
            let guard = handle.read().unwrap_or_else(|e| e.into_inner());
            guard.read_from(offset, max_count, MAX_FETCH_BYTES)
        })
        .await
    }

    pub async fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        let offsets = Arc::clone(&self.offsets);
        let (group, topic) = (group.to_string(), topic.to_string());

        spawn_blocking(move || offsets.commit(&group, &topic, partition, offset)).await
    }

    pub fn fetch_offset(&self, group: &str, topic: &str, partition: u32) -> u64 {
        self.offsets.fetch(group, topic, partition)
    }

    pub fn join_group(&self, group: &str, topics: Vec<String>) -> Result<Assignment> {
        for topic in &topics {
            if !self.topics.contains_key(topic) {
                return Err(Error::UnknownTopic(topic.clone()));
            }
        }
        self.coordinator
            .join(group, topics, &self.partition_counts())
    }

    pub fn heartbeat(&self, group: &str, member_id: &str) -> Result<Assignment> {
        self.coordinator
            .heartbeat(group, member_id, &self.partition_counts())
    }

    pub fn leave_group(&self, group: &str, member_id: &str) -> Result<()> {
        self.coordinator
            .leave(group, member_id, &self.partition_counts())
    }

    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let mut snapshot: Vec<(String, Vec<Arc<RwLock<Partition>>>)> = self
            .topics
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        snapshot.sort_by(|a, b| a.0.cmp(&b.0));

        spawn_blocking(move || {
            Ok(snapshot
                .into_iter()
                .map(|(name, handles)| topic_info(name, &handles))
                .collect())
        })
        .await
    }

    pub async fn describe_topic(&self, topic: &str) -> Result<TopicInfo> {
        let handles: Vec<Arc<RwLock<Partition>>> = self
            .topics
            .get(topic)
            .ok_or_else(|| Error::UnknownTopic(topic.to_string()))?
            .clone();

        let name = topic.to_string();
        spawn_blocking(move || Ok(topic_info(name, &handles))).await
    }

    pub fn list_groups(&self) -> Vec<GroupSummary> {
        let mut summaries = self.coordinator.list();
        let live: Vec<String> = summaries.iter().map(|s| s.group.clone()).collect();

        for group in self.offsets.groups() {
            if !live.contains(&group) {
                summaries.push(GroupSummary {
                    group,
                    generation: 0,
                    members: 0,
                });
            }
        }
        summaries.sort_by(|a, b| a.group.cmp(&b.group));
        summaries
    }

    pub fn describe_group(&self, group: &str) -> Result<GroupDescription> {
        match self.coordinator.describe(group) {
            Ok(description) => Ok(description),
            Err(Error::UnknownGroup(_)) if !self.offsets.committed_for_group(group).is_empty() => {
                Ok(GroupDescription {
                    group: group.to_string(),
                    generation: 0,
                    members: Vec::new(),
                })
            }
            Err(e) => Err(e),
        }
    }

    pub fn committed_offsets(&self, group: &str) -> Vec<(String, u32, u64)> {
        self.offsets.committed_for_group(group)
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }
}

fn topic_info(name: String, handles: &[Arc<RwLock<Partition>>]) -> TopicInfo {
    let partitions = handles
        .iter()
        .enumerate()
        .map(|(id, handle)| {
            let guard = handle.read().unwrap_or_else(|e| e.into_inner());
            PartitionInfo {
                partition: id as u32,
                start_offset: guard.start_offset(),
                next_offset: guard.next_offset(),
                size_bytes: guard.size(),
                segments: guard.segment_count(),
            }
        })
        .collect();

    TopicInfo { name, partitions }
}

pub const MAX_FETCH_COUNT: usize = 10_000;

pub const MAX_FETCH_BYTES: u64 = 4 * 1024 * 1024;

async fn spawn_blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Protocol(format!("storage task failed: {}", e)))?
}

pub fn validate_topic_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 249 {
        return Err(Error::InvalidTopicName(name.to_string()));
    }
    if name == "." || name == ".." {
        return Err(Error::InvalidTopicName(name.to_string()));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(Error::InvalidTopicName(name.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_broker(dir: &tempfile::TempDir) -> Broker {
        Broker::open(BrokerConfig::new(dir.path().to_path_buf())).unwrap()
    }

    #[tokio::test]
    async fn produce_then_fetch_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("orders", 2).await.unwrap();

        for i in 0..10 {
            let offset = broker
                .produce("orders", 0, format!("order {}", i).into_bytes())
                .await
                .unwrap();
            assert_eq!(offset, i);
        }

        let records = broker.fetch("orders", 0, 0, 100).await.unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(records[3].value, b"order 3");

        assert!(broker.fetch("orders", 1, 0, 100).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unknown_topics_and_partitions_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("orders", 1).await.unwrap();

        assert!(matches!(
            broker.produce("nope", 0, b"x".to_vec()).await,
            Err(Error::UnknownTopic(_))
        ));
        assert!(matches!(
            broker.produce("orders", 7, b"x".to_vec()).await,
            Err(Error::UnknownPartition { .. })
        ));
    }

    #[tokio::test]
    async fn topics_and_data_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let broker = test_broker(&dir);
            broker.create_topic("orders", 3).await.unwrap();
            broker
                .produce("orders", 2, b"before restart".to_vec())
                .await
                .unwrap();
        }

        let broker = test_broker(&dir);
        assert_eq!(broker.partition_count("orders").unwrap(), 3);

        let records = broker.fetch("orders", 2, 0, 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, b"before restart");

        assert_eq!(
            broker
                .produce("orders", 2, b"after restart".to_vec())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn committed_offsets_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let broker = test_broker(&dir);
            broker.create_topic("orders", 1).await.unwrap();
            broker
                .commit_offset("analytics", "orders", 0, 17)
                .await
                .unwrap();
        }

        let broker = test_broker(&dir);
        assert_eq!(broker.fetch_offset("analytics", "orders", 0), 17);
        assert_eq!(broker.fetch_offset("other", "orders", 0), 0);
    }

    #[tokio::test]
    async fn recreating_a_topic_with_the_same_count_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);

        broker.create_topic("orders", 2).await.unwrap();
        broker.produce("orders", 0, b"kept".to_vec()).await.unwrap();

        broker.create_topic("orders", 2).await.unwrap();
        assert_eq!(broker.partition_count("orders").unwrap(), 2);
        assert_eq!(broker.fetch("orders", 0, 0, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn recreating_a_topic_with_a_different_count_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("orders", 2).await.unwrap();

        let refused = broker.create_topic("orders", 8).await;
        assert!(matches!(refused, Err(Error::Protocol(_))));
        assert_eq!(broker.partition_count("orders").unwrap(), 2);
    }

    #[tokio::test]
    async fn a_fetch_is_bounded_by_bytes_not_just_count() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("big", 1).await.unwrap();

        let payload = vec![b'x'; 512 * 1024];
        for _ in 0..40 {
            broker.produce("big", 0, payload.clone()).await.unwrap();
        }

        let records = broker.fetch("big", 0, 0, MAX_FETCH_COUNT).await.unwrap();
        let bytes: u64 = records.iter().map(|r| r.size_on_disk()).sum();

        assert!(!records.is_empty());
        assert!(records.len() < 40, "expected a byte bounded batch");
        assert!(bytes <= MAX_FETCH_BYTES, "returned {} bytes", bytes);
    }

    #[test]
    fn topic_names_that_escape_the_data_directory_are_rejected() {
        assert!(validate_topic_name("orders").is_ok());
        assert!(validate_topic_name("orders.v2_final-1").is_ok());

        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name("..").is_err());
        assert!(validate_topic_name("../../etc/passwd").is_err());
        assert!(validate_topic_name("a/b").is_err());
        assert!(validate_topic_name("a\\b").is_err());
        assert!(validate_topic_name(&"x".repeat(250)).is_err());
    }

    #[tokio::test]
    async fn describe_reports_real_partition_state() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("orders", 2).await.unwrap();
        broker.produce("orders", 0, b"one".to_vec()).await.unwrap();
        broker.produce("orders", 0, b"two".to_vec()).await.unwrap();

        let info = broker.describe_topic("orders").await.unwrap();
        assert_eq!(info.partitions.len(), 2);
        assert_eq!(info.partitions[0].next_offset, 2);
        assert_eq!(info.partitions[1].next_offset, 0);
        assert!(info.partitions[0].size_bytes > 0);
    }

    #[tokio::test]
    async fn joining_a_group_for_an_unknown_topic_fails() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        assert!(matches!(
            broker.join_group("g", vec!["ghost".to_string()]),
            Err(Error::UnknownTopic(_))
        ));
    }

    #[tokio::test]
    async fn fetch_count_is_clamped() {
        let dir = tempfile::tempdir().unwrap();
        let broker = test_broker(&dir);
        broker.create_topic("orders", 1).await.unwrap();
        for i in 0..50 {
            broker
                .produce("orders", 0, format!("m{}", i).into_bytes())
                .await
                .unwrap();
        }

        let records = broker.fetch("orders", 0, 0, usize::MAX).await.unwrap();
        assert_eq!(records.len(), 50);
    }
}
