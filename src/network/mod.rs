use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use serde::{Deserialize, Serialize};
use crate::broker::Broker;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    CreateTopic { name: String, partitions: u32 },
    Produce     { topic: String, partition: u32, message: String },
    Fetch       { topic: String, partition: u32, offset: u64, max_count: usize },
    CommitOffset{ group: String, topic: String, partition: u32, offset: u64 },
    FetchOffset { group: String, topic: String, partition: u32 },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    Ok,
    Offset   { offset: u64 },
    Messages { messages: Vec<Message> },
    Error    { reason: String },
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub offset: u64,
    pub value: String,
}

pub async fn run(broker: Arc<Broker>, addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("broker listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        tracing::info!("connection from {}", peer);
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, broker).await {
                tracing::error!("connection closed: {}", e);
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream, broker: Arc<Broker>) -> io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => {}
            // UnexpectedEof = client closed the connection normally
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        socket.read_exact(&mut buf).await?;

        let response = handle_request(&buf, &broker);
        let response_bytes = serde_json::to_vec(&response).unwrap();
        socket.write_all(&(response_bytes.len() as u32).to_be_bytes()).await?;
        socket.write_all(&response_bytes).await?;
    }
}

fn handle_request(buf: &[u8], broker: &Broker) -> Response {
    let request: Request = match serde_json::from_slice(buf) {
        Ok(r) => r,
        Err(e) => return Response::Error { reason: e.to_string() },
    };

    match request {
        Request::CreateTopic { name, partitions } => match broker.create_topic(&name, partitions) {
            Ok(_) => Response::Ok,
            Err(e) => Response::Error { reason: e.to_string() },
        },

        Request::Produce { topic, partition, message } => {
            match broker.produce(&topic, partition, message.as_bytes()) {
                Ok(offset) => Response::Offset { offset },
                Err(e) => Response::Error { reason: e.to_string() },
            }
        }

        Request::Fetch { topic, partition, offset, max_count } => {
            match broker.fetch(&topic, partition, offset, max_count) {
                Ok(msgs) => Response::Messages {
                    messages: msgs
                        .into_iter()
                        .map(|(offset, bytes)| Message {
                            offset,
                            value: String::from_utf8_lossy(&bytes).to_string(),
                        })
                        .collect(),
                },
                Err(e) => Response::Error { reason: e.to_string() },
            }
        }

        Request::CommitOffset { group, topic, partition, offset } => {
            broker.commit_offset(&group, &topic, partition, offset);
            Response::Ok
        }

        Request::FetchOffset { group, topic, partition } => {
            let offset = broker.fetch_offset(&group, &topic, partition);
            Response::Offset { offset }
        }
    }
}
