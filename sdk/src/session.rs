//! `PulzzSession` — wraps an accepted `server::NativeServerSession`. Provides
//! the same `send` / `recv` / `close` surface as `PulzzClient` but for the
//! server side of an accepted connection.

use std::collections::VecDeque;
use std::time::Duration;

use shared_protocol::{ItemId, Record, StreamId};
use tokio::time::timeout;

use crate::{
    config::ClientConfig,
    error::SdkError,
};

/// Server-side accepted session.
#[derive(Debug)]
pub struct PulzzSession {
    pub(crate) inner: server::NativeServerSession,
    pub(crate) config: ClientConfig,
    pub(crate) pending_recv: VecDeque<Record>,
}

impl PulzzSession {
    pub(crate) fn from_native(
        native: server::NativeServerSession,
        config: &ClientConfig,
    ) -> Self {
        Self {
            inner: native,
            config: config.clone(),
            pending_recv: VecDeque::new(),
        }
    }

    /// Send a single item over the accepted session. The record is
    /// protected by the session's AEAD protector before being shipped.
    pub async fn send(&mut self, item_id: ItemId, payload: &[u8]) -> Result<(), SdkError> {
        // stream_id is owned by the protector; the header value here is
        // overwritten by protect_record before AEAD AAD computation.
        let stream_id = StreamId(1);
        let plain = Record {
            header: shared_protocol::RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id: shared_protocol::EpochId(0),
                seq_no: shared_protocol::SeqNo(0),
                record_type: shared_protocol::RecordType::ExactState,
                codec_mode: shared_protocol::CodecMode::DirectExact,
                flags: shared_protocol::RecordFlags::empty(),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.to_vec(),
            auth_tag: [0u8; shared_protocol::AUTH_TAG_LEN],
        };
        let protected = self
            .inner
            .protector_mut()
            .protect_record(plain)
            .map_err(SdkError::Protection)?;
        let frame =
            shared_protocol::transport::encode_compact_transport_records(&[protected]);
        self.inner.send_transport_frame(frame).await?;
        Ok(())
    }

    /// Receive the next record from the peer. Returns `Ok(None)` on EOF.
    pub async fn recv(&mut self) -> Result<Option<Record>, SdkError> {
        let timeout_ms = self.config.timeout_ms();
        let frame_fut = self.inner.read_transport_frame();
        let frame = if timeout_ms > 0 {
            timeout(Duration::from_millis(timeout_ms), frame_fut)
                .await
                .map_err(|_| SdkError::Timeout(timeout_ms))?
                .map_err(SdkError::from)?
        } else {
            frame_fut.await.map_err(SdkError::from)?
        };
        let Some(bytes) = frame else {
            return Ok(None);
        };
        let records = self
            .inner
            .protector_mut()
            .unprotect_transport_frame(&bytes)
            .map_err(SdkError::Protection)?;
        let mut iter = records.into_iter();
        if let Some(first) = iter.next() {
            for extra in iter {
                self.pending_recv.push_back(extra);
            }
            Ok(Some(first))
        } else {
            Ok(None)
        }
    }

    /// Close the session.
    pub async fn close(self) -> Result<(), SdkError> {
        self.inner.close().await.map_err(SdkError::from)
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}
