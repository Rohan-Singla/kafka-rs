pub mod consumer;
pub mod producer;

pub use consumer::{Consumer, ConsumerRecord};
pub use producer::Producer;

use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::network::codec;
use crate::network::protocol::{Request, Response};

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        Ok(Self { stream })
    }

    pub async fn send(&mut self, request: Request) -> Result<Response> {
        let payload = serde_json::to_vec(&request)?;
        codec::write_frame(&mut self.stream, &payload).await?;

        let frame = codec::read_frame(&mut self.stream)
            .await?
            .ok_or(Error::Disconnected)?;

        match serde_json::from_slice::<Response>(&frame)? {
            Response::Error { reason, code } => Err(Error::Broker { reason, code }),
            other => Ok(other),
        }
    }

    pub async fn expect_ok(&mut self, request: Request) -> Result<()> {
        match self.send(request).await? {
            Response::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn expect_offset(&mut self, request: Request) -> Result<u64> {
        match self.send(request).await? {
            Response::Offset { offset } => Ok(offset),
            other => Err(unexpected(other)),
        }
    }
}

pub(crate) fn unexpected(response: Response) -> Error {
    Error::Protocol(format!("unexpected response from broker: {:?}", response))
}
