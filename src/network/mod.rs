pub mod codec;
pub mod protocol;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};

use crate::broker::Broker;
use crate::error::{ErrorCode, Result};
use protocol::{CommittedOffset, Message, Request, Response};

pub use codec::MAX_FRAME_SIZE;

const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

pub struct Server {
    listener: TcpListener,
    broker: Arc<Broker>,
}

impl Server {
    pub async fn bind(broker: Arc<Broker>, addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, broker })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!("broker listening on {}", self.local_addr()?);

        loop {
            let (socket, peer) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!("accept failed: {}", e);
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            };

            let broker = Arc::clone(&self.broker);
            tokio::spawn(async move {
                tracing::debug!("connection from {}", peer);
                if let Err(e) = handle_connection(socket, broker).await {
                    tracing::warn!("connection from {} ended: {}", peer, e);
                }
            });
        }
    }
}

async fn handle_connection(mut socket: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let _ = socket.set_nodelay(true);

    loop {
        let frame = match codec::read_frame(&mut socket).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(e) => {
                let response = Response::Error {
                    reason: e.to_string(),
                    code: e.code(),
                };
                let _ = codec::write_frame(&mut socket, &serde_json::to_vec(&response)?).await;
                return Err(e);
            }
        };

        let response = match serde_json::from_slice::<Request>(&frame) {
            Ok(request) => dispatch(request, &broker).await,
            Err(e) => Response::Error {
                reason: format!("malformed request: {}", e),
                code: ErrorCode::Unknown,
            },
        };

        let mut payload = serde_json::to_vec(&response)?;
        if payload.len() > MAX_FRAME_SIZE {
            let refusal = Response::Error {
                reason: format!(
                    "response of {} bytes exceeds the {} byte frame limit, request a smaller batch",
                    payload.len(),
                    MAX_FRAME_SIZE
                ),
                code: ErrorCode::TooLarge,
            };
            payload = serde_json::to_vec(&refusal)?;
        }

        codec::write_frame(&mut socket, &payload).await?;
    }
}

pub async fn dispatch(request: Request, broker: &Broker) -> Response {
    match request {
        Request::CreateTopic { name, partitions } => {
            match broker.create_topic(&name, partitions).await {
                Ok(()) => Response::Ok,
                Err(e) => e.into(),
            }
        }

        Request::Produce {
            topic,
            partition,
            message,
        } => {
            let encoded = protocol::encoded_value_len(&message);
            if encoded > MAX_DELIVERABLE_MESSAGE {
                return Response::Error {
                    reason: format!(
                        "message encodes to {} bytes, over the {} byte limit, and could never be read back",
                        encoded, MAX_DELIVERABLE_MESSAGE
                    ),
                    code: ErrorCode::TooLarge,
                };
            }
            match broker
                .produce(&topic, partition, message.into_bytes())
                .await
            {
                Ok(offset) => Response::Offset { offset },
                Err(e) => e.into(),
            }
        }

        Request::Fetch {
            topic,
            partition,
            offset,
            max_count,
        } => match broker.fetch(&topic, partition, offset, max_count).await {
            Ok(records) => fetch_response(records),
            Err(e) => e.into(),
        },

        Request::CommitOffset {
            group,
            topic,
            partition,
            offset,
        } => match broker
            .commit_offset(&group, &topic, partition, offset)
            .await
        {
            Ok(()) => Response::Ok,
            Err(e) => e.into(),
        },

        Request::FetchOffset {
            group,
            topic,
            partition,
        } => Response::Offset {
            offset: broker.fetch_offset(&group, &topic, partition),
        },

        Request::ListTopics => match broker.list_topics().await {
            Ok(topics) => Response::Topics { topics },
            Err(e) => e.into(),
        },

        Request::DescribeTopic { topic } => match broker.describe_topic(&topic).await {
            Ok(info) => Response::Topic { topic: info },
            Err(e) => e.into(),
        },

        Request::JoinGroup { group, topics } => match broker.join_group(&group, topics) {
            Ok(assignment) => Response::Assignment {
                member_id: assignment.member_id,
                generation: assignment.generation,
                partitions: assignment.partitions,
            },
            Err(e) => e.into(),
        },

        Request::Heartbeat { group, member_id } => match broker.heartbeat(&group, &member_id) {
            Ok(assignment) => Response::Assignment {
                member_id: assignment.member_id,
                generation: assignment.generation,
                partitions: assignment.partitions,
            },
            Err(e) => e.into(),
        },

        Request::LeaveGroup { group, member_id } => match broker.leave_group(&group, &member_id) {
            Ok(()) => Response::Ok,
            Err(e) => e.into(),
        },

        Request::ListGroups => Response::Groups {
            groups: broker.list_groups(),
        },

        Request::DescribeGroup { group } => match broker.describe_group(&group) {
            Ok(description) => Response::Group {
                committed: broker
                    .committed_offsets(&group)
                    .into_iter()
                    .map(|(topic, partition, offset)| CommittedOffset {
                        topic,
                        partition,
                        offset,
                    })
                    .collect(),
                group: description,
            },
            Err(e) => e.into(),
        },
    }
}

const FRAME_HEADROOM: usize = 4096;

pub const MAX_DELIVERABLE_MESSAGE: usize = MAX_FRAME_SIZE - FRAME_HEADROOM;

fn fetch_response(records: Vec<crate::storage::Record>) -> Response {
    let mut remaining = MAX_DELIVERABLE_MESSAGE;
    let mut messages = Vec::with_capacity(records.len());

    for record in records {
        let offset = record.offset;
        let message = Message::from(record);
        let len = protocol::encoded_len(&message);

        if messages.is_empty() && len > MAX_DELIVERABLE_MESSAGE {
            return Response::Error {
                reason: format!(
                    "the record at offset {} encodes to {} bytes, over the {} byte limit, resume from offset {} to skip it",
                    offset,
                    len,
                    MAX_DELIVERABLE_MESSAGE,
                    offset + 1
                ),
                code: ErrorCode::TooLarge,
            };
        }
        if len > remaining {
            break;
        }
        remaining -= len;
        messages.push(message);
    }

    Response::Messages { messages }
}

impl From<crate::error::Error> for Response {
    fn from(e: crate::error::Error) -> Self {
        Response::Error {
            reason: e.to_string(),
            code: e.code(),
        }
    }
}
