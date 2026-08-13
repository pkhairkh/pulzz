//! `PulzzClient` — the SDK client. Wraps a connected `ClientSession` and
//! exposes an idiomatic async `send` / `recv` / `close` surface.
//!
//! Two construction modes:
//! 1. **Network mode** (`PulzzClient::connect(url)`): builds a
//!    `client::NativeClientAbiConfig`, calls `connect_native_session`, and
//!    drives a real WebSocket/TCP/QUIC/WebTransport carrier.
//! 2. **In-memory test mode** (`PulzzClient::from_session`): wraps a
//!    pre-existing `ClientSession` (e.g. one created from
//!    `classic_ref1_pair_from_rng`). Used by integration tests and by
//!    callers that want to drive the state machine without a network.

use std::collections::VecDeque;
use std::time::Duration;

use shared_protocol::{
    ItemId, Record, RecordHeader, RecordType, StreamId,
    protection::StreamProtector,
};
#[cfg(not(target_arch = "wasm32"))]
use shared_protocol::{classic_ref1_pair_from_rng, transport::encode_compact_transport_records};
use rand::SeedableRng;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::timeout;

use crate::{
    config::{CarrierKind, ClientConfig, CompressionConfig, SecurityProfile},
    error::SdkError,
};

/// pulzZ SDK client.
///
/// Holds a `ClientSession` (state + AEAD protector) and an optional
/// connected transport. When `transport` is `None`, the client operates in
/// in-memory mode: callers can `push_record` protected records into a queue
/// and `recv` will pop + apply them.
#[derive(Debug)]
pub struct PulzzClient {
    pub(crate) inner: client::ClientSession,
    pub(crate) config: ClientConfig,
    /// Native-only transport handle. On `wasm32` the field does not exist;
    /// the client operates purely in in-memory mode (server-push pattern).
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) transport: Option<client::NativeClientSession>,
    pub(crate) pending_recv: VecDeque<Record>,
}

impl PulzzClient {
    /// Start a client builder.
    pub fn builder() -> PulzzClientBuilder {
        PulzzClientBuilder::default()
    }

    /// Construct a `PulzzClient` for in-memory testing using a pre-built
    /// `ClientSession` (typically paired with a `ServerSession` via
    /// `classic_ref1_pair_from_rng`). No network I/O occurs.
    pub fn from_session(session: client::ClientSession, config: ClientConfig) -> Self {
        Self {
            inner: session,
            config,
            #[cfg(not(target_arch = "wasm32"))]
            transport: None,
            pending_recv: VecDeque::new(),
        }
    }

    /// Push a protected record into the in-memory receive queue. The next
    /// `recv` call will pop and apply this record. Only valid in in-memory
    /// mode (i.e. `transport` is `None`).
    pub fn push_record(&mut self, record: Record) {
        self.pending_recv.push_back(record);
    }

    /// Build a single ExactState record from `item_id` + `payload`. The
    /// record is unprotected — callers that want to send it over the wire
    /// must either route through `send` (which protects + ships it) or
    /// call `protect_record` themselves.
    pub(crate) fn build_exact_record(
        &self,
        item_id: ItemId,
        payload: &[u8],
    ) -> Result<Record, SdkError> {
        // The stream_id is owned by the protector; the header must match it
        // exactly or protect_record will reject the record. In in-memory mode
        // we can read it directly; in network mode we fall back to the
        // placeholder's stream id (which protect_record will overwrite via
        // the active transport's protector).
        let stream_id = self.inner.protector().stream_id();
        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id: shared_protocol::EpochId(0),
                seq_no: shared_protocol::SeqNo(0),
                record_type: RecordType::ExactState,
                codec_mode: shared_protocol::CodecMode::DirectExact,
                flags: shared_protocol::RecordFlags::empty(),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload: payload.to_vec(),
            auth_tag: [0u8; shared_protocol::AUTH_TAG_LEN],
        })
    }

    /// Send a single item by ID + payload. In network mode this protects the
    /// record with the local AEAD protector and ships it over the carrier.
    /// In in-memory mode this is a no-op (the test pattern is server-push,
    /// not client-push).
    pub async fn send(&mut self, item_id: ItemId, payload: &[u8]) -> Result<(), SdkError> {
        let plain = self.build_exact_record(item_id, payload)?;
        let protected = self
            .protector_mut()
            .protect_record(plain)
            .map_err(SdkError::Protection)?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(transport) = self.transport.as_mut() {
                let frame = encode_compact_transport_records(&[protected]);
                transport.send_transport_frame(frame).await?;
            }
        }
        // On wasm32 `protected` is intentionally unused — the in-memory
        // server-push pattern doesn't use client send. Silence the warning.
        #[cfg(target_arch = "wasm32")]
        let _ = protected;
        Ok(())
    }

    /// Send a batch of items. Each item is `(ItemId, &[u8])`. Internally
    /// builds a `BatchEnvelope` record (when compression is enabled, the
    /// envelope is zstd-compressed as a single stream — see
    /// `docs/SDK_PROPOSAL.md` §5.3).
    pub async fn send_batch<I>(&mut self, items: I) -> Result<(), SdkError>
    where
        I: IntoIterator<Item = (ItemId, Vec<u8>)>,
    {
        let materialized: Vec<(ItemId, Vec<u8>)> = items.into_iter().collect();
        if materialized.is_empty() {
            return Ok(());
        }
        let mut envelope = shared_protocol::batch::BatchEnvelope::new();
        for (id, payload) in &materialized {
            envelope.push(*id, shared_protocol::SourceKind::Binary, payload.clone());
        }
        let envelope_bytes = envelope
            .encode()
            .map_err(|e| SdkError::CompressionFailed(e.to_string()))?;

        let payload = if self.config.compression.enabled && envelope_bytes.len() > 64 {
            match shared_protocol::compress::zstd_compress_raw(&envelope_bytes) {
                Ok(c) if c.len() < envelope_bytes.len() => c,
                _ => envelope_bytes.clone(),
            }
        } else {
            envelope_bytes.clone()
        };

        let stream_id = self.inner.protector().stream_id();
        let mut flags = shared_protocol::RecordFlags::empty();
        if payload.len() < envelope_bytes.len() {
            flags.insert(shared_protocol::RecordFlags::PAYLOAD_ZSTD);
        }
        let plain = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id: shared_protocol::EpochId(0),
                seq_no: shared_protocol::SeqNo(0),
                record_type: RecordType::BatchEnvelope,
                codec_mode: shared_protocol::CodecMode::PackedExact,
                flags,
                item_id: ItemId(0),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0u8; shared_protocol::AUTH_TAG_LEN],
        };
        let protected = self
            .protector_mut()
            .protect_record(plain)
            .map_err(SdkError::Protection)?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(transport) = self.transport.as_mut() {
                let frame = encode_compact_transport_records(&[protected]);
                transport.send_transport_frame(frame).await?;
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = protected;
        Ok(())
    }

    /// Receive the next record. In network mode this reads a transport
    /// frame from the carrier, unprotects it, applies each record to the
    /// client state, and returns the first. In in-memory mode it pops from
    /// `pending_recv`.
    ///
    /// Returns `Ok(None)` when the carrier signals end-of-stream.
    pub async fn recv(&mut self) -> Result<Option<Record>, SdkError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let timeout_ms = self.config.timeout_ms();
            if let Some(transport) = self.transport.as_mut() {
                let frame_fut = transport.read_transport_frame();
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
                    .protector_mut()
                    .unprotect_transport_frame(&bytes)
                    .map_err(SdkError::Protection)?;
                let mut iter = records.into_iter();
                if let Some(first) = iter.next() {
                    self.inner
                        .state_mut()
                        .apply_record(first.clone())
                        .map_err(SdkError::from)?;
                    for extra in iter {
                        self.pending_recv.push_back(extra);
                    }
                    return Ok(Some(first));
                }
                return Ok(None);
            }
        }
        // In-memory mode (always taken on wasm32; also taken on native when
        // no transport is connected).
        if let Some(record) = self.pending_recv.pop_front() {
            self.inner
                .apply_protected_record(record.clone())
                .map_err(SdkError::from)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Close the underlying transport (network mode). In in-memory mode
    /// this is a no-op.
    pub async fn close(self) -> Result<(), SdkError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(transport) = self.transport {
                transport.close().await.map_err(SdkError::from)?;
            }
        }
        // On wasm32 there is no transport field; close is a no-op.
        #[cfg(target_arch = "wasm32")]
        let _ = self;
        Ok(())
    }

    /// Read-only access to the inner client session. In in-memory mode this
    /// returns the session directly; in network mode this returns a
    /// reference to a placeholder (the real session is owned by the
    /// transport — use `session_mut()` to reach it).
    pub fn session(&self) -> &client::ClientSession {
        &self.inner
    }

    /// Mutable access to the inner client session / protector. In network
    /// mode this reaches into the transport's session.
    pub fn session_mut(&mut self) -> &mut client::ClientSession {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(transport) = self.transport.as_mut() {
            return transport.session_mut();
        }
        &mut self.inner
    }

    /// Direct mutable access to the AEAD protector. Reaches into the
    /// transport when in network mode.
    pub(crate) fn protector_mut(&mut self) -> &mut StreamProtector {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(transport) = self.transport.as_mut() {
            return transport.session_mut().protector_mut();
        }
        self.inner.protector_mut()
    }

    /// Read-only access to the underlying client config.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}

/// Builder for `PulzzClient`. Mirrors `docs/SDK_PROPOSAL.md` §5.1.
#[derive(Debug, Clone)]
pub struct PulzzClientBuilder {
    pub carrier: CarrierKind,
    pub security: SecurityProfile,
    pub compression: CompressionConfig,
    pub batch_size: Option<usize>,
    pub timeout_ms: u64,
}

impl Default for PulzzClientBuilder {
    fn default() -> Self {
        let cfg = ClientConfig::default();
        Self {
            carrier: cfg.carrier,
            security: cfg.security,
            compression: cfg.compression,
            batch_size: cfg.batch_size,
            timeout_ms: cfg.timeout_ms(),
        }
    }
}

impl PulzzClientBuilder {
    pub fn carrier(mut self, carrier: CarrierKind) -> Self {
        self.carrier = carrier;
        self
    }
    pub fn security(mut self, security: SecurityProfile) -> Self {
        self.security = security;
        self
    }
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }
    pub fn compression(mut self, compression: CompressionConfig) -> Self {
        self.compression = compression;
        self
    }
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    pub fn build_config(self) -> ClientConfig {
        ClientConfig {
            security: self.security,
            carrier: self.carrier,
            compression: self.compression,
            batch_size: self.batch_size,
            timeout: Duration::from_millis(self.timeout_ms),
        }
    }

    /// Connect to a server at `url` using the configured carrier. The URL
    /// scheme selects the transport:
    /// - `ws://` / `wss://` — WebSocket
    /// - `tcp://` — raw TCP
    /// - `quic://` — QUIC stream
    /// - `wt://` — WebTransport
    ///
    /// The carrier configured on the builder overrides the URL scheme if
    /// they conflict.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn connect(self, url: &str) -> Result<PulzzClient, SdkError> {
        let config = self.build_config();
        PulzzClient::connect_with_config(url, config).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PulzzClient {
    /// Connect to a server using the default config + the given URL.
    pub async fn connect(url: &str) -> Result<Self, SdkError> {
        Self::connect_with_config(url, ClientConfig::default()).await
    }

    /// Connect with a fully-specified `ClientConfig`. Builds a
    /// `client::ClientConnectConfig` and calls `connect_native_session`.
    ///
    /// **Security profile handling:**
    /// - `PqSimpleV1` — proceeds with the PqSimple handshake (the only
    ///   profile currently wired through the SDK's `connect_with_config`).
    /// - `PqMutualV1` — returns `InvalidArg`. The SDK does not yet expose
    ///   the `IssuedClientCredential` + `ServerIdentityBundle` inputs needed
    ///   to drive a mutual-PQ handshake.
    /// - `ClassicRef1` — returns `InvalidArg`. `ClientSecurityConfig` has
    ///   no ClassicRef1 variant; use `PulzzClient::from_session` with
    ///   `classic_ref1_pair_from_rng` for in-memory classic-ref1 testing.
    pub async fn connect_with_config(
        url: &str,
        config: ClientConfig,
    ) -> Result<Self, SdkError> {
        // Honor the caller's SecurityProfile choice. Only PqSimpleV1 is
        // wired through the SDK's network connect path today. The other
        // profiles require credentials or APIs not yet exposed — fail
        // fast with a clear InvalidArg error instead of silently mapping
        // them to PqSimple (which was the previous buggy behavior, bug #1).
        match config.security {
            SecurityProfile::PqSimpleV1 => {}
            SecurityProfile::PqMutualV1 => {
                return Err(SdkError::invalid_arg(
                    "PqMutualV1 is not yet wired through PulzzClient::connect_with_config; \
                     it requires IssuedClientCredential + ServerIdentityBundle inputs \
                     that the SDK does not yet expose. Use PqSimpleV1 for network connect, \
                     or PulzzClient::from_session for in-memory testing.",
                ));
            }
            SecurityProfile::ClassicRef1 => {
                return Err(SdkError::invalid_arg(
                    "ClassicRef1 is not wired through PulzzClient::connect_with_config; \
                     ClientSecurityConfig has no ClassicRef1 variant. Use \
                     PulzzClient::from_session with classic_ref1_pair_from_rng \
                     for in-memory classic-ref1 testing.",
                ));
            }
        }
        let carrier_kind = match config.carrier {
            CarrierKind::WebSocket => client::NativeClientCarrierKind::WebSocket,
            CarrierKind::Tcp => client::NativeClientCarrierKind::Tcp,
            CarrierKind::QuicStream => client::NativeClientCarrierKind::QuicStream,
            CarrierKind::QuicDatagram => client::NativeClientCarrierKind::QuicDatagram,
            CarrierKind::WebTransport => client::NativeClientCarrierKind::WebTransportDatagram,
            CarrierKind::UdpDatagram => client::NativeClientCarrierKind::Udp,
        };
        let security_cfg = client::ClientSecurityConfig::PqSimple;
        let stream_id = StreamId(1);
        let direction = shared_protocol::StreamDirection::ServerToClient;
        let session = shared_protocol::TransportSessionConfig {
            protection_profile: shared_protocol::ProtectionProfileKind::PqSimpleV1,
            ..Default::default()
        };
        let connect_config = client::ClientConnectConfig {
            url: url.to_string(),
            stream_id,
            direction,
            session,
            security: security_cfg,
            reconnect_policy: client::ReconnectPolicy::disabled(),
        };
        let abi_config = client::NativeClientAbiConfig::new(carrier_kind, connect_config);
        let native = client::connect_native_session(&abi_config)
            .await
            .map_err(SdkError::from)?;
        // Construct a placeholder inner session (used only when transport is None).
        // The real session for network mode lives inside `native`.
        //
        // Bug #2 fix: the placeholder protector's stream_id MUST match the
        // network session's stream_id (StreamId(1)). Previously this used
        // StreamId(0), which caused protect_record to reject the header at
        // runtime with UnexpectedStreamId.
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x00C1_DE0F_FACE);
        let (placeholder_protector, _) = classic_ref1_pair_from_rng(
            stream_id, // StreamId(1) — matches the network session
            shared_protocol::StreamDirection::ClientToServer,
            &mut rng,
        );
        let placeholder_session = client::ClientSession::new(placeholder_protector);
        Ok(Self {
            inner: placeholder_session,
            config,
            transport: Some(native),
            pending_recv: VecDeque::new(),
        })
    }
}

// Re-exported helper for tests so they can construct a classic_ref1 protector
// pair without depending on `shared_protocol` directly.
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub fn classic_pair_for_test(
    stream_id: StreamId,
) -> (StreamProtector, StreamProtector) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5EED_5EED);
    classic_ref1_pair_from_rng(stream_id, shared_protocol::StreamDirection::ServerToClient, &mut rng)
}
