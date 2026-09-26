pub mod consumer;
pub mod producer;

pub use consumer::{Consumer, ConsumerRecord};
pub use producer::Producer;

use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::network::codec;
use crate::network::protocol::{Request, Response};

/// One TCP connection to a broker, speaking the length prefixed JSON protocol.
///
/// The protocol is strictly request and response over a single socket, so a
/// `Connection` is deliberately not shareable: two callers interleaving writes
/// would each read back the other's reply.
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
            Response::Error { reason } => Err(Error::Protocol(reason)),
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
