use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};

/// Ceiling on one frame. The length prefix arrives before the body, so without
/// this a client could send four bytes claiming 4GB and make the broker try to
/// allocate it.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Framing is `[length: u32 big endian][payload: length bytes]`.
///
/// TCP is a byte stream with no message boundaries, so the length prefix is what
/// tells the reader where one request ends and the next begins.
pub async fn read_frame<R>(reader: &mut R) -> Result<Option<BytesMut>>
where
    R: AsyncRead + Unpin,
{
    let mut length_buf = [0u8; 4];
    match reader.read_exact(&mut length_buf).await {
        Ok(_) => {}
        // Hitting EOF on the length prefix is a clean disconnect, not a failure.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let length = u32::from_be_bytes(length_buf) as usize;
    if length > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge {
            size: length,
            max: MAX_FRAME_SIZE,
        });
    }

    let mut payload = BytesMut::zeroed(length);
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

/// Length prefix and payload go out in a single write, so a reader never sees a
/// header without its body behind it.
pub async fn write_frame<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge {
            size: payload.len(),
            max: MAX_FRAME_SIZE,
        });
    }

    let mut frame = BytesMut::with_capacity(4 + payload.len());
    frame.put_u32(payload.len() as u32);
    frame.put_slice(payload);
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn a_frame_survives_a_roundtrip() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, b"hello broker").await.unwrap();

        let mut cursor = Cursor::new(buffer);
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(&frame[..], b"hello broker");
    }

    #[tokio::test]
    async fn back_to_back_frames_keep_their_boundaries() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, b"one").await.unwrap();
        write_frame(&mut buffer, b"two").await.unwrap();
        write_frame(&mut buffer, b"").await.unwrap();

        let mut cursor = Cursor::new(buffer);
        assert_eq!(&read_frame(&mut cursor).await.unwrap().unwrap()[..], b"one");
        assert_eq!(&read_frame(&mut cursor).await.unwrap().unwrap()[..], b"two");
        assert_eq!(&read_frame(&mut cursor).await.unwrap().unwrap()[..], b"");
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_clean_disconnect_is_none_not_an_error() {
        let mut cursor = Cursor::new(Vec::new());
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_truncated_body_is_an_error() {
        // Claims 100 bytes, delivers 3.
        let mut buffer = 100u32.to_be_bytes().to_vec();
        buffer.extend_from_slice(b"abc");

        let mut cursor = Cursor::new(buffer);
        assert!(read_frame(&mut cursor).await.is_err());
    }

    /// A four byte header must not be able to trigger a huge allocation.
    #[tokio::test]
    async fn an_oversized_length_prefix_is_refused_before_allocating() {
        let buffer = u32::MAX.to_be_bytes().to_vec();
        let mut cursor = Cursor::new(buffer);

        assert!(matches!(
            read_frame(&mut cursor).await,
            Err(Error::FrameTooLarge { .. })
        ));
    }
}
