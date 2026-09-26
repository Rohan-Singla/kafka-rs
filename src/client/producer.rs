use std::collections::HashMap;

use super::{Connection, unexpected};
use crate::broker::TopicInfo;
use crate::error::Result;
use crate::network::protocol::{Request, Response};

pub struct Producer {
    connection: Connection,
    partition_counts: HashMap<String, u32>,
    round_robin: HashMap<String, u32>,
}

impl Producer {
    pub async fn connect(addr: &str) -> Result<Self> {
        Ok(Self {
            connection: Connection::connect(addr).await?,
            partition_counts: HashMap::new(),
            round_robin: HashMap::new(),
        })
    }

    pub async fn create_topic(&mut self, name: &str, partitions: u32) -> Result<()> {
        self.connection
            .expect_ok(Request::CreateTopic {
                name: name.to_string(),
                partitions,
            })
            .await?;
        self.partition_counts.remove(name);
        Ok(())
    }

    pub async fn send(&mut self, topic: &str, message: &str) -> Result<u64> {
        let count = self.partition_count(topic).await?;
        let cursor = self.round_robin.entry(topic.to_string()).or_insert(0);
        let partition = *cursor % count;
        *cursor = cursor.wrapping_add(1);

        self.send_to(topic, partition, message).await
    }

    pub async fn send_to(&mut self, topic: &str, partition: u32, message: &str) -> Result<u64> {
        self.connection
            .expect_offset(Request::Produce {
                topic: topic.to_string(),
                partition,
                message: message.to_string(),
            })
            .await
    }

    pub async fn describe_topic(&mut self, topic: &str) -> Result<TopicInfo> {
        match self
            .connection
            .send(Request::DescribeTopic {
                topic: topic.to_string(),
            })
            .await?
        {
            Response::Topic { topic } => Ok(topic),
            other => Err(unexpected(other)),
        }
    }

    pub async fn list_topics(&mut self) -> Result<Vec<TopicInfo>> {
        match self.connection.send(Request::ListTopics).await? {
            Response::Topics { topics } => Ok(topics),
            other => Err(unexpected(other)),
        }
    }

    async fn partition_count(&mut self, topic: &str) -> Result<u32> {
        if let Some(count) = self.partition_counts.get(topic) {
            return Ok(*count);
        }
        let info = self.describe_topic(topic).await?;
        let count = info.partitions.len().max(1) as u32;
        self.partition_counts.insert(topic.to_string(), count);
        Ok(count)
    }
}
