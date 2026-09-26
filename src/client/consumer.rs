use std::collections::HashMap;

use super::{unexpected, Connection};
use crate::broker::groups::{GroupDescription, GroupSummary, TopicPartition};
use crate::error::{Error, Result};
use crate::network::protocol::{CommittedOffset, Request, Response};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub timestamp: u64,
    pub value: String,
}

/// Reads messages from a topic, either directly from a partition or as part of
/// a consumer group.
///
/// ```no_run
/// # async fn demo() -> kafka_rust::Result<()> {
/// use kafka_rust::client::Consumer;
///
/// let mut consumer = Consumer::connect("127.0.0.1:9092").await?;
/// consumer.subscribe("analytics", &["orders"]).await?;
///
/// for record in consumer.poll(100).await? {
///     println!("{} -> {}", record.offset, record.value);
/// }
/// consumer.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Consumer {
    connection: Connection,
    group: Option<String>,
    member_id: Option<String>,
    generation: u64,
    assignment: Vec<TopicPartition>,
    positions: HashMap<TopicPartition, u64>,
    /// Where the next poll starts in the assignment list, so a partition with a
    /// long backlog cannot starve the others.
    cursor: usize,
}

impl Consumer {
    pub async fn connect(addr: &str) -> Result<Self> {
        Ok(Self {
            connection: Connection::connect(addr).await?,
            group: None,
            member_id: None,
            generation: 0,
            assignment: Vec::new(),
            positions: HashMap::new(),
            cursor: 0,
        })
    }

    /// Join a consumer group and take ownership of a share of the partitions.
    pub async fn subscribe(&mut self, group: &str, topics: &[&str]) -> Result<()> {
        let response = self
            .connection
            .send(Request::JoinGroup {
                group: group.to_string(),
                topics: topics.iter().map(|t| t.to_string()).collect(),
            })
            .await?;

        let Response::Assignment {
            member_id,
            generation,
            partitions,
        } = response
        else {
            return Err(unexpected(response));
        };

        self.group = Some(group.to_string());
        self.member_id = Some(member_id);
        self.generation = generation;
        self.cursor = 0;
        self.adopt_assignment(partitions).await?;
        Ok(())
    }

    /// Fetch the next batch from the assigned partitions.
    ///
    /// Every poll heartbeats first. That renews the lease this consumer holds on
    /// its partitions and is where a rebalance is noticed, so a consumer that
    /// stops polling is treated as gone and its work is handed to someone else.
    pub async fn poll(&mut self, max_count: usize) -> Result<Vec<ConsumerRecord>> {
        let (group, member_id) = self.group_identity()?;

        let response = self
            .connection
            .send(Request::Heartbeat {
                group: group.clone(),
                member_id: member_id.clone(),
            })
            .await?;

        let Response::Assignment {
            generation,
            partitions,
            ..
        } = response
        else {
            return Err(unexpected(response));
        };

        if generation != self.generation {
            tracing::info!(
                "rebalanced from generation {} to {}",
                self.generation,
                generation
            );
            self.generation = generation;
            self.cursor = 0;
            self.adopt_assignment(partitions).await?;
        }

        if self.assignment.is_empty() {
            return Ok(Vec::new());
        }

        // Try each assigned partition once, starting where the last poll left off.
        for step in 0..self.assignment.len() {
            let index = (self.cursor + step) % self.assignment.len();
            let target = self.assignment[index].clone();
            let position = *self.positions.get(&target).unwrap_or(&0);

            let records = self
                .fetch_partition(&target.topic, target.partition, position, max_count)
                .await?;

            if !records.is_empty() {
                let next = records[records.len() - 1].offset + 1;
                self.positions.insert(target, next);
                self.cursor = (index + 1) % self.assignment.len();
                return Ok(records);
            }
        }

        Ok(Vec::new())
    }

    /// Commit the position of every partition this consumer has read from.
    ///
    /// The committed value is the next offset to read, not the last one read,
    /// so a restart resumes without replaying the final message.
    pub async fn commit(&mut self) -> Result<()> {
        let (group, _) = self.group_identity()?;

        let positions: Vec<(TopicPartition, u64)> = self
            .positions
            .iter()
            .map(|(tp, offset)| (tp.clone(), *offset))
            .collect();

        for (target, offset) in positions {
            self.connection
                .expect_ok(Request::CommitOffset {
                    group: group.clone(),
                    topic: target.topic,
                    partition: target.partition,
                    offset,
                })
                .await?;
        }
        Ok(())
    }

    /// Leave the group so the remaining members rebalance immediately rather
    /// than waiting out the session timeout.
    pub async fn leave(&mut self) -> Result<()> {
        let (group, member_id) = self.group_identity()?;
        self.connection
            .expect_ok(Request::LeaveGroup { group, member_id })
            .await?;

        self.group = None;
        self.member_id = None;
        self.assignment.clear();
        self.positions.clear();
        Ok(())
    }

    /// Read straight from one partition, with no group and no assignment.
    /// Useful for replaying a log from the beginning.
    pub async fn poll_partition(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_count: usize,
    ) -> Result<Vec<ConsumerRecord>> {
        self.fetch_partition(topic, partition, offset, max_count).await
    }

    pub async fn committed(&mut self, group: &str, topic: &str, partition: u32) -> Result<u64> {
        self.connection
            .expect_offset(Request::FetchOffset {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
            })
            .await
    }

    pub async fn list_groups(&mut self) -> Result<Vec<GroupSummary>> {
        match self.connection.send(Request::ListGroups).await? {
            Response::Groups { groups } => Ok(groups),
            other => Err(unexpected(other)),
        }
    }

    pub async fn describe_group(
        &mut self,
        group: &str,
    ) -> Result<(GroupDescription, Vec<CommittedOffset>)> {
        match self
            .connection
            .send(Request::DescribeGroup {
                group: group.to_string(),
            })
            .await?
        {
            Response::Group { group, committed } => Ok((group, committed)),
            other => Err(unexpected(other)),
        }
    }

    pub fn assignment(&self) -> &[TopicPartition] {
        &self.assignment
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn member_id(&self) -> Option<&str> {
        self.member_id.as_deref()
    }

    async fn fetch_partition(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_count: usize,
    ) -> Result<Vec<ConsumerRecord>> {
        let response = self
            .connection
            .send(Request::Fetch {
                topic: topic.to_string(),
                partition,
                offset,
                max_count,
            })
            .await?;

        let Response::Messages { messages } = response else {
            return Err(unexpected(response));
        };

        Ok(messages
            .into_iter()
            .map(|m| ConsumerRecord {
                topic: topic.to_string(),
                partition,
                offset: m.offset,
                timestamp: m.timestamp,
                value: m.value,
            })
            .collect())
    }

    /// Take a new assignment, seeding each newly owned partition from the
    /// group's committed offset so a rebalance does not replay from zero.
    async fn adopt_assignment(&mut self, partitions: Vec<TopicPartition>) -> Result<()> {
        let group = self.group.clone().ok_or_else(|| {
            Error::Protocol("consumer is not subscribed to a group".to_string())
        })?;

        // Drop positions for partitions that were taken away.
        self.positions.retain(|tp, _| partitions.contains(tp));

        for target in &partitions {
            if self.positions.contains_key(target) {
                continue;
            }
            let committed = self
                .connection
                .expect_offset(Request::FetchOffset {
                    group: group.clone(),
                    topic: target.topic.clone(),
                    partition: target.partition,
                })
                .await?;
            self.positions.insert(target.clone(), committed);
        }

        self.assignment = partitions;
        Ok(())
    }

    fn group_identity(&self) -> Result<(String, String)> {
        match (&self.group, &self.member_id) {
            (Some(group), Some(member_id)) => Ok((group.clone(), member_id.clone())),
            _ => Err(Error::Protocol(
                "call subscribe() before using the group API".to_string(),
            )),
        }
    }
}
