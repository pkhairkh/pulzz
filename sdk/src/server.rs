//! `PulzzServer` — the SDK server. Wraps a `server::ServerSession` for the
//! in-memory emit path, and a `server::NativeServerAcceptor` for the real
//! network accept path. Returned sessions are `PulzzSession`.

use std::time::Duration;

use shared_protocol::protection::StreamProtector;
use tokio::net::TcpListener;

use crate::{
    config::{CarrierKind, ClientConfig, CompressionConfig, SecurityProfile},
    error::SdkError,
    session::PulzzSession,
};

/// pulzZ SDK server.
pub struct PulzzServer {
    pub(crate) session: server::ServerSession,
    pub(crate) config: ClientConfig,
    pub(crate) acceptor: Option<server::NativeServerAcceptor>,
}

impl std::fmt::Debug for PulzzServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulzzServer")
            .field("session", &"<ServerSession>")
            .field("config", &self.config)
            .field("acceptor", &self.acceptor.is_some())
            .finish()
    }
}

impl PulzzServer {
    pub fn builder() -> PulzzServerBuilder {
        PulzzServerBuilder::default()
    }

    /// Construct a `PulzzServer` for in-memory testing from a pre-built
    /// `StreamProtector` (typically paired via `classic_ref1_pair_from_rng`).
    pub fn from_protector(protector: StreamProtector, config: ClientConfig) -> Self {
        Self {
            session: server::ServerSession::new(protector),
            config,
            acceptor: None,
        }
    }

    /// Bind to `addr` (e.g. `"0.0.0.0:4433"`). Returns a server ready to
    /// `accept()` connections.
    pub async fn bind(addr: &str) -> Result<Self, SdkError> {
        Self::bind_with_config(addr, ClientConfig::default()).await
    }

    /// Bind with a fully-specified config.
    pub async fn bind_with_config(
        addr: &str,
        config: ClientConfig,
    ) -> Result<Self, SdkError> {
        let carrier_kind = match config.carrier {
            CarrierKind::WebSocket => server::NativeServerCarrierKind::WebSocket,
            CarrierKind::Tcp => server::NativeServerCarrierKind::Tcp,
            CarrierKind::QuicStream => server::NativeServerCarrierKind::QuicStream,
            CarrierKind::QuicDatagram => server::NativeServerCarrierKind::QuicDatagram,
            CarrierKind::WebTransport => server::NativeServerCarrierKind::WebTransportDatagram,
            CarrierKind::UdpDatagram => server::NativeServerCarrierKind::Udp,
        };
        let bootstrap = shared_protocol::BootstrapConfig::default();
        let session_cfg = shared_protocol::TransportSessionConfig {
            protection_profile: shared_protocol::ProtectionProfileKind::PqSimpleV1,
            bootstrap,
            ..Default::default()
        };
        let limits = server::transport::ConnectionLimits::default();
        let server_security = shared_protocol::ServerSecurityConfig::PqSimple {
            bootstrap: shared_protocol::PqSimpleServerBootstrapConfig::default(),
        };
        let bootstrap_policy = server::transport::BootstrapPolicy::new(
            shared_protocol::BootstrapServerConfig {
                stream_id: shared_protocol::StreamId(1),
                direction: shared_protocol::StreamDirection::ServerToClient,
                bootstrap: shared_protocol::BootstrapConfig::default(),
                security: server_security,
            },
        );
        let transport_server_cfg = server::transport::TransportServerConfig {
            session: session_cfg,
            connection_limits: limits,
            bootstrap_policy,
            carrier: shared_protocol::carrier::reliable::ReliableCarrierKind::WebSocket,
        };
        let abi_config =
            server::NativeServerAbiConfig::new(addr, carrier_kind, transport_server_cfg);
        let acceptor = server::NativeServerAcceptor::bind(&abi_config).await?;
        // Use a throwaway classic_ref1 protector for the SDK-level in-memory
        // emit surface; the real network sessions use the protector inside
        // their NativeServerSession.
        let placeholder = shared_protocol::protection::StreamProtector::from_bootstrap_root(
            shared_protocol::ProtectionProfileKind::ClassicRef1,
            shared_protocol::StreamId(1),
            shared_protocol::StreamDirection::ServerToClient,
            [0u8; 32],
        );
        Ok(Self {
            session: server::ServerSession::new(placeholder),
            config,
            acceptor: Some(acceptor),
        })
    }

    /// Accept a new connection. Returns a `PulzzSession` once the
    /// handshake completes. `None` indicates the acceptor has shut down.
    pub async fn accept(&mut self) -> Result<Option<PulzzSession>, SdkError> {
        let Some(acceptor) = self.acceptor.as_ref() else {
            return Ok(None);
        };
        let native = acceptor.accept().await?;
        Ok(Some(PulzzSession::from_native(native, &self.config)))
    }

    /// Returns the locally-bound socket address of the server's listener.
    ///
    /// Only available in network mode (after `bind` / `bind_with_config`).
    /// Returns `Err(SdkError::InvalidState)` in in-memory mode (after
    /// `from_protector`).
    ///
    /// # Example
    /// ```ignore
    /// let server = PulzzServer::bind("127.0.0.1:0").await?;
    /// let addr = server.local_addr()?;
    /// // addr is e.g. "127.0.0.1:54321" — clients can connect to it.
    /// ```
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, SdkError> {
        let acceptor = self.acceptor.as_ref().ok_or_else(|| {
            SdkError::invalid_state(
                "PulzzServer::local_addr requires network mode (bind/bind_with_config); \
                 in-memory mode (from_protector) has no listener.",
            )
        })?;
        acceptor.local_addr().map_err(SdkError::from)
    }

    /// Emit a single `ServerEvent` as a protected Record, in-memory.
    ///
    /// **In-memory only.** This method uses the `ServerSession`'s protector
    /// (the one passed to `from_protector`). When the server is in network
    /// mode (created via `bind` / `bind_with_config`), the in-memory
    /// `ServerSession` holds a throwaway placeholder protector that is NOT
    /// the same as the per-connection protector inside each accepted
    /// `NativeServerSession`. Emitting records through the placeholder would
    /// produce frames that cannot be decoded by the client (bug #3).
    ///
    /// In network mode, accept a session via `accept()` and call
    /// `PulzzSession::send` / `send_batch` instead.
    pub fn emit_event(
        &mut self,
        event: server::ServerEvent,
    ) -> Result<shared_protocol::Record, SdkError> {
        if self.acceptor.is_some() {
            return Err(SdkError::invalid_state(
                "PulzzServer::emit_event is in-memory-only; in network mode, \
                 accept a session via accept() and call PulzzSession::send instead. \
                 The in-memory ServerSession's protector is a throwaway placeholder \
                 that does not match any accepted connection's protector (bug #3).",
            ));
        }
        self.session.emit_event(event).map_err(SdkError::from)
    }

    /// Emit a batch as a single BatchEnvelope Record.
    ///
    /// **In-memory only** — see `emit_event` for the rationale.
    pub fn emit_batch<I>(
        &mut self,
        items: I,
    ) -> Result<shared_protocol::Record, SdkError>
    where
        I: IntoIterator<Item = (shared_protocol::ItemId, shared_protocol::ExactStateMaterial)>,
    {
        if self.acceptor.is_some() {
            return Err(SdkError::invalid_state(
                "PulzzServer::emit_batch is in-memory-only; in network mode, \
                 accept a session via accept() and call PulzzSession::send_batch instead.",
            ));
        }
        self.session.emit_batch(items).map_err(SdkError::from)
    }

    /// Read-only access to the inner ServerSession.
    pub fn session(&self) -> &server::ServerSession {
        &self.session
    }

    /// Mutable access to the inner ServerSession.
    pub fn session_mut(&mut self) -> &mut server::ServerSession {
        &mut self.session
    }
}

/// Builder for `PulzzServer`.
#[derive(Debug, Clone)]
pub struct PulzzServerBuilder {
    pub carrier: CarrierKind,
    pub security: SecurityProfile,
    pub bind_addr: String,
    pub timeout_ms: u64,
}

impl Default for PulzzServerBuilder {
    fn default() -> Self {
        Self {
            carrier: CarrierKind::WebSocket,
            security: SecurityProfile::PqSimpleV1,
            bind_addr: "0.0.0.0:0".to_string(),
            timeout_ms: 30_000,
        }
    }
}

impl PulzzServerBuilder {
    pub fn carrier(mut self, c: CarrierKind) -> Self {
        self.carrier = c;
        self
    }
    pub fn security(mut self, s: SecurityProfile) -> Self {
        self.security = s;
        self
    }
    pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
        self.bind_addr = addr.into();
        self
    }
    pub fn timeout(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }
    pub async fn bind(self) -> Result<PulzzServer, SdkError> {
        let cfg = ClientConfig {
            security: self.security,
            carrier: self.carrier,
            compression: CompressionConfig::default(),
            batch_size: None,
            timeout: Duration::from_millis(self.timeout_ms),
        };
        PulzzServer::bind_with_config(&self.bind_addr, cfg).await
    }
}

#[doc(hidden)]
impl PulzzServer {
    /// Test helper: bind a throwaway WebSocket listener and return its
    /// local address. Used by integration tests that need a real listener
    /// without going through the full NativeServerAcceptor path.
    pub async fn bind_test_listener() -> Result<(TcpListener, String), SdkError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        Ok((listener, addr))
    }

    /// Test helper: returns `true` if the server is in network mode
    /// (i.e. created via `bind` / `bind_with_config` and has an acceptor).
    /// Used by the `server_emit_event_network_mode` test to verify the
    /// `emit_event` / `emit_batch` in-memory-only guard.
    pub fn session_acceptor_is_some(&self) -> bool {
        self.acceptor.is_some()
    }
}
