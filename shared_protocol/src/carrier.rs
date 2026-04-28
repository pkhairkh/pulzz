pub mod reliable {
    use serde::{Deserialize, Serialize};

    #[cfg(not(target_arch = "wasm32"))]
    use async_trait::async_trait;
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
    #[serde(rename_all = "snake_case")]
    pub enum ReliableCarrierKind {
        #[default]
        WebSocket,
        Tcp,
        QuicStream,
    }

    impl ReliableCarrierKind {
        pub fn parse(value: &str) -> Option<Self> {
            match value {
                "websocket" | "ws" => Some(Self::WebSocket),
                "tcp" => Some(Self::Tcp),
                "quic_stream" | "quic" => Some(Self::QuicStream),
                _ => None,
            }
        }

        pub fn slug(self) -> &'static str {
            match self {
                Self::WebSocket => "websocket",
                Self::Tcp => "tcp",
                Self::QuicStream => "quic_stream",
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait]
    pub trait ReliableCarrier: Send {
        type Error: Send;

        async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error>;

        async fn recv_frame(
            &mut self,
            max_frame_len: usize,
        ) -> Result<Option<Vec<u8>>, Self::Error>;

        async fn close(&mut self) -> Result<(), Self::Error>;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn write_length_prefixed_frame<W>(writer: &mut W, frame: &[u8]) -> io::Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let frame_len = u32::try_from(frame.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame exceeds u32 length"))?;
        writer.write_all(&frame_len.to_le_bytes()).await?;
        writer.write_all(frame).await?;
        writer.flush().await?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn read_length_prefixed_frame<R>(
        reader: &mut R,
        max_frame_len: usize,
    ) -> io::Result<Option<Vec<u8>>>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut len_bytes = [0_u8; 4];
        match reader.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        }
        let frame_len = u32::from_le_bytes(len_bytes) as usize;
        if frame_len > max_frame_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame length {frame_len} exceeds max {max_frame_len}"),
            ));
        }
        let mut frame = vec![0_u8; frame_len];
        reader.read_exact(&mut frame).await?;
        Ok(Some(frame))
    }
}

pub mod datagram {
    use serde::{Deserialize, Serialize};

    #[cfg(not(target_arch = "wasm32"))]
    use async_trait::async_trait;

    pub use crate::datagram::DatagramCarrierKind;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
    pub struct DatagramCarrierLimits {
        pub max_datagram_size: usize,
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait]
    pub trait DatagramCarrier: Send {
        type Error: Send;

        fn limits(&self) -> DatagramCarrierLimits;

        async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error>;

        async fn recv_datagram(
            &mut self,
            max_datagram_len: usize,
        ) -> Result<Option<Vec<u8>>, Self::Error>;

        async fn close(&mut self) -> Result<(), Self::Error>;
    }
}
