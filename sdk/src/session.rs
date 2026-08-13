//! `PulzzSession` — wraps an accepted `server::NativeServerSession`. Provides
//! the same `send` / `recv` / `close` surface as `PulzzClient` but for the
//! server side of an accepted connection.

use std::collections::VecDeque;
use std::time::Duration;

use shared_protocol::{ItemId, Record};
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
    /// protected by the session's AEAD protector as a transport frame
    /// (outer AEAD layer) before being shipped.
    pub async fn send(&mut self, item_id: ItemId, payload: &[u8]) -> Result<(), SdkError> {
        // Read stream_id + seq_no from the protector BEFORE protect_transport_records.
        // The protector validates record.header.seq_no == expected_seq_no() and
        // advances the ratchet inside protect_transport_records (clone+assign).
        let (stream_id, seq_no) = {
            let p = self.inner.protector();
            (p.stream_id(), p.expected_seq_no())
        };
        // DirectExact codec mode requires the payload to start with a source
        // kind tag byte (1=text, 2=json, 3=binary, 4=image) followed by the
        // exact bytes. Prepend SourceKind::Binary so the receiver's
        // decode_direct_exact_payload can parse it.
        let mut encoded_payload = Vec::with_capacity(1 + payload.len());
        encoded_payload.push(shared_protocol::SourceKind::Binary as u8);
        encoded_payload.extend_from_slice(payload);
        let plain = Record {
            header: shared_protocol::RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id: shared_protocol::EpochId(0),
                seq_no,
                record_type: shared_protocol::RecordType::ExactState,
                codec_mode: shared_protocol::CodecMode::DirectExact,
                flags: shared_protocol::RecordFlags::empty(),
                item_id,
                payload_len: encoded_payload.len() as u32,
            },
            payload: encoded_payload,
            auth_tag: [0u8; shared_protocol::AUTH_TAG_LEN],
        };
        // Use protect_transport_records (not protect_record + encode) to
        // produce the outer AEAD transport frame that the peer's
        // unprotect_transport_frame expects.
        let frame = self
            .inner
            .protector_mut()
            .protect_transport_records(std::iter::once(plain))
            .map_err(SdkError::Protection)?;
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
