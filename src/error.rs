use std::{fmt, io};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Codec(serde_json::Error),
    /// A record failed its checksum or framing sanity check while being read
    /// back from disk. Carries the byte position so a corrupt segment can be
    /// pinpointed without re-scanning.
    Corrupt { position: u64, detail: String },
    FrameTooLarge { size: usize, max: usize },
    UnknownTopic(String),
    UnknownPartition { topic: String, partition: u32 },
    UnknownGroup(String),
    UnknownMember { group: String, member: String },
    /// The client is acting on an assignment from a previous generation, which
    /// means a rebalance happened while it was away. It must rejoin.
    StaleGeneration { expected: u64, got: u64 },
    OffsetOutOfRange { offset: u64, next_offset: u64 },
    InvalidTopicName(String),
    Protocol(String),
    Disconnected,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {}", e),
            Error::Codec(e) => write!(f, "codec error: {}", e),
            Error::Corrupt { position, detail } => {
                write!(f, "corrupt log at byte {}: {}", position, detail)
            }
            Error::FrameTooLarge { size, max } => {
                write!(f, "frame of {} bytes exceeds the {} byte limit", size, max)
            }
            Error::UnknownTopic(t) => write!(f, "unknown topic '{}'", t),
            Error::UnknownPartition { topic, partition } => {
                write!(f, "topic '{}' has no partition {}", topic, partition)
            }
            Error::UnknownGroup(g) => write!(f, "unknown consumer group '{}'", g),
            Error::UnknownMember { group, member } => {
                write!(f, "member '{}' is not in group '{}'", member, group)
            }
            Error::StaleGeneration { expected, got } => write!(
                f,
                "stale generation {}, group is now at generation {}",
                got, expected
            ),
            Error::OffsetOutOfRange {
                offset,
                next_offset,
            } => write!(
                f,
                "offset {} is out of range, next offset is {}",
                offset, next_offset
            ),
            Error::InvalidTopicName(n) => write!(f, "invalid topic name '{}'", n),
            Error::Protocol(m) => write!(f, "protocol error: {}", m),
            Error::Disconnected => write!(f, "peer closed the connection"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Codec(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Codec(e)
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(inner) => inner,
            other => io::Error::other(other.to_string()),
        }
    }
}
