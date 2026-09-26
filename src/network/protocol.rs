use serde::{Deserialize, Serialize};

use crate::broker::TopicInfo;
use crate::broker::groups::{GroupDescription, GroupSummary, TopicPartition};
use crate::storage::Record;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    CreateTopic {
        name: String,
        partitions: u32,
    },
    Produce {
        topic: String,
        partition: u32,
        message: String,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_count: usize,
    },
    CommitOffset {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    FetchOffset {
        group: String,
        topic: String,
        partition: u32,
    },
    ListTopics,
    DescribeTopic {
        topic: String,
    },
    JoinGroup {
        group: String,
        topics: Vec<String>,
    },
    Heartbeat {
        group: String,
        member_id: String,
    },
    LeaveGroup {
        group: String,
        member_id: String,
    },
    ListGroups,
    DescribeGroup {
        group: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    Ok,
    Offset {
        offset: u64,
    },
    Messages {
        messages: Vec<Message>,
    },
    Topics {
        topics: Vec<TopicInfo>,
    },
    Topic {
        topic: TopicInfo,
    },
    Assignment {
        member_id: String,
        generation: u64,
        partitions: Vec<TopicPartition>,
    },
    Groups {
        groups: Vec<GroupSummary>,
    },
    Group {
        group: GroupDescription,
        committed: Vec<CommittedOffset>,
    },
    Error {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub timestamp: u64,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

const MESSAGE_OVERHEAD: usize = 72;

pub fn encoded_len(message: &Message) -> usize {
    MESSAGE_OVERHEAD + json_escaped_len(&message.value)
}

fn json_escaped_len(value: &str) -> usize {
    value
        .bytes()
        .map(|b| match b {
            b'"' | b'\\' | 0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
        .sum()
}

impl From<Record> for Message {
    fn from(record: Record) -> Self {
        Message {
            offset: record.offset,
            timestamp: record.timestamp,
            value: String::from_utf8_lossy(&record.value).into_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_tagged_by_type() {
        let json = serde_json::to_string(&Request::Produce {
            topic: "orders".into(),
            partition: 0,
            message: "hello".into(),
        })
        .unwrap();

        assert!(json.contains("\"type\":\"Produce\""));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, Request::Produce { .. }));
    }

    #[test]
    fn unit_variants_need_no_fields() {
        let parsed: Request = serde_json::from_str(r#"{"type":"ListTopics"}"#).unwrap();
        assert!(matches!(parsed, Request::ListTopics));
    }

    #[test]
    fn an_unknown_type_is_a_parse_error_not_a_panic() {
        assert!(serde_json::from_str::<Request>(r#"{"type":"Nonsense"}"#).is_err());
    }
}
