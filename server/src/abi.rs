use std::net::SocketAddr;
use std::sync::Arc;

use quinn::Endpoint as QuicEndpoint;
use shared_protocol::{Record, StreamProtector, TransportConfig};
use tokio::net::{TcpListener, UdpSocket};

use crate::datagram_transport::{
    AuthenticatedQuicDatagramSession, AuthenticatedUdpSession,
    AuthenticatedWebTransportDatagramSession, BoundWebTransportDatagramServer,
    accept_quic_datagram_session, accept_udp_session, accept_webtransport_datagram_session,
    bind_udp_socket, bind_webtransport_datagram_server,
};
use crate::transport::{
    AuthenticatedQuicSession, AuthenticatedServerConnection, AuthenticatedTcpSession,
    ServerCarrierKind, TransportError, TransportServerConfig, accept_authenticated_session,
    accept_quic_session, accept_tcp_session, bind_quic_endpoint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeServerCarrierKind {
    WebSocket,
    Tcp,
    QuicStream,
    Udp,
    QuicDatagram,
    WebTransportDatagram,
}

#[derive(Debug, Clone)]
pub struct NativeServerAbiConfig {
    pub addr: String,
    pub carrier: NativeServerCarrierKind,
    pub transport: TransportServerConfig,
}

pub enum NativeServerAcceptor {
    WebSocket {
        listener: TcpListener,
        config: TransportServerConfig,
    },
    Tcp {
        listener: TcpListener,
        config: TransportServerConfig,
    },
    Quic {
        endpoint: QuicEndpoint,
        config: TransportServerConfig,
    },
    Udp {
        socket: Arc<UdpSocket>,
        config: TransportServerConfig,
    },
    QuicDatagram {
        endpoint: QuicEndpoint,
        config: TransportServerConfig,
    },
    WebTransportDatagram {
        server: BoundWebTransportDatagramServer,
        config: TransportServerConfig,
    },
}

#[derive(Debug)]
pub enum NativeServerSession {
    WebSocket(AuthenticatedServerConnection),
    Tcp(AuthenticatedTcpSession),
    Quic(AuthenticatedQuicSession),
    Udp(AuthenticatedUdpSession),
    QuicDatagram(AuthenticatedQuicDatagramSession),
    WebTransportDatagram(AuthenticatedWebTransportDatagramSession),
}

impl NativeServerAbiConfig {
    pub fn new(
        addr: impl Into<String>,
        carrier: NativeServerCarrierKind,
        mut transport: TransportServerConfig,
    ) -> Self {
        if let Some(reliable) = reliable_carrier_kind(carrier) {
            transport.carrier = reliable;
        }
        Self {
            addr: addr.into(),
            carrier,
            transport,
        }
    }

    pub fn websocket(addr: impl Into<String>, transport: TransportServerConfig) -> Self {
        Self::new(addr, NativeServerCarrierKind::WebSocket, transport)
    }

    pub fn tcp(addr: impl Into<String>, transport: TransportServerConfig) -> Self {
        Self::new(addr, NativeServerCarrierKind::Tcp, transport)
    }

    pub fn quic(addr: impl Into<String>, transport: TransportServerConfig) -> Self {
        Self::new(addr, NativeServerCarrierKind::QuicStream, transport)
    }

    pub fn udp(addr: impl Into<String>, transport: TransportServerConfig) -> Self {
        Self::new(addr, NativeServerCarrierKind::Udp, transport)
    }

    pub fn quic_datagram(addr: impl Into<String>, transport: TransportServerConfig) -> Self {
        Self::new(addr, NativeServerCarrierKind::QuicDatagram, transport)
    }

    pub fn webtransport_datagram(
        addr: impl Into<String>,
        transport: TransportServerConfig,
    ) -> Self {
        Self::new(
            addr,
            NativeServerCarrierKind::WebTransportDatagram,
            transport,
        )
    }

    pub fn carrier(&self) -> NativeServerCarrierKind {
        self.carrier
    }
}

impl NativeServerAcceptor {
    pub async fn bind(config: &NativeServerAbiConfig) -> Result<Self, TransportError> {
        let mut transport = config.transport.clone();
        if let Some(reliable) = reliable_carrier_kind(config.carrier) {
            transport.carrier = reliable;
        }
        match config.carrier {
            NativeServerCarrierKind::WebSocket => {
                let listener = TcpListener::bind(&config.addr).await?;
                Ok(Self::WebSocket {
                    listener,
                    config: transport,
                })
            }
            NativeServerCarrierKind::Tcp => {
                let listener = TcpListener::bind(&config.addr).await?;
                Ok(Self::Tcp {
                    listener,
                    config: transport,
                })
            }
            NativeServerCarrierKind::QuicStream => {
                let endpoint = bind_quic_endpoint(
                    &config.addr,
                    transport.connection_limits.max_transport_frame_bytes,
                )?;
                Ok(Self::Quic {
                    endpoint,
                    config: transport,
                })
            }
            NativeServerCarrierKind::Udp => {
                let socket = bind_udp_socket(&config.addr).await?;
                Ok(Self::Udp {
                    socket,
                    config: transport,
                })
            }
            NativeServerCarrierKind::QuicDatagram => {
                let endpoint = bind_quic_endpoint(
                    &config.addr,
                    transport.connection_limits.max_transport_frame_bytes,
                )?;
                Ok(Self::QuicDatagram {
                    endpoint,
                    config: transport,
                })
            }
            NativeServerCarrierKind::WebTransportDatagram => {
                let server = bind_webtransport_datagram_server(
                    &config.addr,
                    transport.connection_limits.max_transport_frame_bytes,
                )?;
                Ok(Self::WebTransportDatagram {
                    server,
                    config: transport,
                })
            }
        }
    }

    pub fn carrier(&self) -> NativeServerCarrierKind {
        match self {
            Self::WebSocket { .. } => NativeServerCarrierKind::WebSocket,
            Self::Tcp { .. } => NativeServerCarrierKind::Tcp,
            Self::Quic { .. } => NativeServerCarrierKind::QuicStream,
            Self::Udp { .. } => NativeServerCarrierKind::Udp,
            Self::QuicDatagram { .. } => NativeServerCarrierKind::QuicDatagram,
            Self::WebTransportDatagram { .. } => NativeServerCarrierKind::WebTransportDatagram,
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        match self {
            Self::WebSocket { listener, .. } => listener.local_addr(),
            Self::Tcp { listener, .. } => listener.local_addr(),
            Self::Quic { endpoint, .. } => endpoint.local_addr(),
            Self::Udp { socket, .. } => socket.local_addr(),
            Self::QuicDatagram { endpoint, .. } => endpoint.local_addr(),
            Self::WebTransportDatagram { server, .. } => Ok(server.local_addr()),
        }
    }

    pub async fn accept(&self) -> Result<NativeServerSession, TransportError> {
        match self {
            Self::WebSocket { listener, config } => accept_authenticated_session(listener, config)
                .await
                .map(NativeServerSession::WebSocket),
            Self::Tcp { listener, config } => accept_tcp_session(listener, config)
                .await
                .map(NativeServerSession::Tcp),
            Self::Quic { endpoint, config } => accept_quic_session(endpoint, config)
                .await
                .map(NativeServerSession::Quic),
            Self::Udp { socket, config } => accept_udp_session(socket, config)
                .await
                .map(NativeServerSession::Udp),
            Self::QuicDatagram { endpoint, config } => {
                accept_quic_datagram_session(endpoint, config)
                    .await
                    .map(NativeServerSession::QuicDatagram)
            }
            Self::WebTransportDatagram { server, config } => {
                accept_webtransport_datagram_session(server, config)
                    .await
                    .map(NativeServerSession::WebTransportDatagram)
            }
        }
    }

    pub async fn serve(
        &self,
        records: Vec<Record>,
        connections: usize,
    ) -> Result<(), TransportError> {
        for _ in 0..connections {
            let mut session = self.accept().await?;
            session.send_plain_records(records.clone()).await?;
            session.close().await?;
        }
        Ok(())
    }
}

fn reliable_carrier_kind(carrier: NativeServerCarrierKind) -> Option<ServerCarrierKind> {
    match carrier {
        NativeServerCarrierKind::WebSocket => Some(ServerCarrierKind::WebSocket),
        NativeServerCarrierKind::Tcp => Some(ServerCarrierKind::Tcp),
        NativeServerCarrierKind::QuicStream => Some(ServerCarrierKind::QuicStream),
        NativeServerCarrierKind::Udp
        | NativeServerCarrierKind::QuicDatagram
        | NativeServerCarrierKind::WebTransportDatagram => None,
    }
}

impl NativeServerSession {
    pub fn carrier(&self) -> NativeServerCarrierKind {
        match self {
            Self::WebSocket(_) => NativeServerCarrierKind::WebSocket,
            Self::Tcp(_) => NativeServerCarrierKind::Tcp,
            Self::Quic(_) => NativeServerCarrierKind::QuicStream,
            Self::Udp(_) => NativeServerCarrierKind::Udp,
            Self::QuicDatagram(_) => NativeServerCarrierKind::QuicDatagram,
            Self::WebTransportDatagram(_) => NativeServerCarrierKind::WebTransportDatagram,
        }
    }

    pub fn protector(&self) -> &StreamProtector {
        match self {
            Self::WebSocket(session) => session.protector(),
            Self::Tcp(session) => session.protector(),
            Self::Quic(session) => session.protector(),
            Self::Udp(session) => session.protector(),
            Self::QuicDatagram(session) => session.protector(),
            Self::WebTransportDatagram(session) => session.protector(),
        }
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        match self {
            Self::WebSocket(session) => session.protector_mut(),
            Self::Tcp(session) => session.protector_mut(),
            Self::Quic(session) => session.protector_mut(),
            Self::Udp(session) => session.protector_mut(),
            Self::QuicDatagram(session) => session.protector_mut(),
            Self::WebTransportDatagram(session) => session.protector_mut(),
        }
    }

    pub fn transport_config(&self) -> TransportConfig {
        match self {
            Self::WebSocket(session) => session.transport_config(),
            Self::Tcp(session) => session.transport_config(),
            Self::Quic(session) => session.transport_config(),
            Self::Udp(session) => session.transport_config(),
            Self::QuicDatagram(session) => session.transport_config(),
            Self::WebTransportDatagram(session) => session.transport_config(),
        }
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        match self {
            Self::WebSocket(session) => session.send_transport_frame(frame).await,
            Self::Tcp(session) => session.send_transport_frame(frame).await,
            Self::Quic(session) => session.send_transport_frame(frame).await,
            Self::Udp(session) => session.send_transport_frame(frame).await,
            Self::QuicDatagram(session) => session.send_transport_frame(frame).await,
            Self::WebTransportDatagram(session) => session.send_transport_frame(frame).await,
        }
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let frames: Vec<Vec<u8>> = frames
            .into_iter()
            .map(|frame| frame.as_ref().to_vec())
            .collect();
        match self {
            Self::WebSocket(session) => session.send_transport_frames(&frames).await,
            Self::Tcp(session) => session.send_transport_frames(&frames).await,
            Self::Quic(session) => session.send_transport_frames(&frames).await,
            Self::Udp(session) => session.send_transport_frames(&frames).await,
            Self::QuicDatagram(session) => session.send_transport_frames(&frames).await,
            Self::WebTransportDatagram(session) => session.send_transport_frames(&frames).await,
        }
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        let records: Vec<Record> = records.into_iter().collect();
        match self {
            Self::WebSocket(session) => session.send_plain_records(records).await,
            Self::Tcp(session) => session.send_plain_records(records).await,
            Self::Quic(session) => session.send_plain_records(records).await,
            Self::Udp(session) => session.send_plain_records(records).await,
            Self::QuicDatagram(session) => session.send_plain_records(records).await,
            Self::WebTransportDatagram(session) => session.send_plain_records(records).await,
        }
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
        match self {
            Self::WebSocket(session) => session.receive_records_until_close().await,
            Self::Tcp(session) => session.receive_records_until_close().await,
            Self::Quic(session) => session.receive_records_until_close().await,
            Self::Udp(session) => session.receive_records_until_close().await,
            Self::QuicDatagram(session) => session.receive_records_until_close().await,
            Self::WebTransportDatagram(session) => session.receive_records_until_close().await,
        }
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        match self {
            Self::WebSocket(session) => session.read_transport_frame().await,
            Self::Tcp(session) => session.read_transport_frame().await,
            Self::Quic(session) => session.read_transport_frame().await,
            Self::Udp(session) => session.read_transport_frame().await,
            Self::QuicDatagram(session) => session.read_transport_frame().await,
            Self::WebTransportDatagram(session) => session.read_transport_frame().await,
        }
    }

    pub async fn close(self) -> Result<(), TransportError> {
        match self {
            Self::WebSocket(session) => session.close().await,
            Self::Tcp(session) => session.close().await,
            Self::Quic(session) => session.close().await,
            Self::Udp(session) => session.close().await,
            Self::QuicDatagram(session) => session.close().await,
            Self::WebTransportDatagram(session) => session.close().await,
        }
    }
}

pub async fn accept_native_session(
    config: &NativeServerAbiConfig,
) -> Result<NativeServerSession, TransportError> {
    let acceptor = NativeServerAcceptor::bind(config).await?;
    acceptor.accept().await
}

pub async fn serve_native_session(
    config: &NativeServerAbiConfig,
    records: Vec<Record>,
    connections: usize,
) -> Result<(), TransportError> {
    let acceptor = NativeServerAcceptor::bind(config).await?;
    acceptor.serve(records, connections).await
}

pub async fn serve_native_session_once(
    config: &NativeServerAbiConfig,
    records: Vec<Record>,
) -> Result<(), TransportError> {
    serve_native_session(config, records, 1).await
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use client::{
        ClientConnectConfig, ClientSecurityConfig, ReconnectPolicy, connect_native_session,
    };
    use shared_protocol::{
        BOOTSTRAP_SIGNING_SEED_LEN, BootstrapConfig, CredentialScope,
        PqSimpleServerBootstrapConfig, ProtectionProfileKind, ReplayCache, ServerSecurityConfig,
        StreamDirection, StreamId, TransportSessionConfig, issue_client_credential,
        issue_server_identity,
    };

    use crate::transport::{BootstrapPolicy, ConnectionLimits};

    use super::*;

    fn sample_connection_limits() -> ConnectionLimits {
        let bootstrap = BootstrapConfig::default();
        ConnectionLimits {
            handshake_timeout_ms: bootstrap.handshake_timeout_ms,
            idle_read_timeout_ms: bootstrap.idle_read_timeout_ms,
            max_bootstrap_frame_bytes: bootstrap.max_bootstrap_frame_bytes,
            max_transport_frame_bytes: bootstrap.max_transport_frame_bytes,
        }
    }

    fn sample_scope(stream_id: StreamId) -> CredentialScope {
        CredentialScope {
            stream_id: Some(stream_id),
            allow_client_to_server: true,
            allow_server_to_client: true,
        }
    }

    fn current_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn pq_simple_server_abi(
        addr: &str,
        stream_id: StreamId,
        carrier: NativeServerCarrierKind,
    ) -> NativeServerAbiConfig {
        let bootstrap = BootstrapConfig::default();
        NativeServerAbiConfig::new(
            addr,
            carrier,
            TransportServerConfig {
                session: TransportSessionConfig {
                    protection_profile: ProtectionProfileKind::PqSimpleV1,
                    bootstrap,
                    ..TransportSessionConfig::default()
                },
                connection_limits: sample_connection_limits(),
                bootstrap_policy: BootstrapPolicy::with_replay_cache(
                    shared_protocol::BootstrapServerConfig {
                        stream_id,
                        direction: StreamDirection::ServerToClient,
                        bootstrap,
                        security: ServerSecurityConfig::PqSimple {
                            bootstrap: PqSimpleServerBootstrapConfig {
                                server_id: format!("simple-{carrier:?}").to_lowercase(),
                            },
                        },
                    },
                    ReplayCache::default(),
                ),
                carrier: reliable_carrier_kind(carrier).unwrap_or(ServerCarrierKind::WebSocket),
            },
        )
    }

    #[test]
    fn native_server_abi_constructors_select_expected_carriers() {
        let stream_id = StreamId(41);
        assert_eq!(
            NativeServerAbiConfig::websocket(
                "127.0.0.1:0",
                pq_simple_server_abi("127.0.0.1:0", stream_id, NativeServerCarrierKind::WebSocket)
                    .transport,
            )
            .carrier(),
            NativeServerCarrierKind::WebSocket
        );
        assert_eq!(
            NativeServerAbiConfig::tcp(
                "127.0.0.1:0",
                pq_simple_server_abi("127.0.0.1:0", stream_id, NativeServerCarrierKind::Tcp)
                    .transport,
            )
            .carrier(),
            NativeServerCarrierKind::Tcp
        );
        assert_eq!(
            NativeServerAbiConfig::quic(
                "127.0.0.1:0",
                pq_simple_server_abi(
                    "127.0.0.1:0",
                    stream_id,
                    NativeServerCarrierKind::QuicStream,
                )
                .transport,
            )
            .carrier(),
            NativeServerCarrierKind::QuicStream
        );
    }

    #[tokio::test]
    async fn native_server_and_client_abi_round_trip_over_tcp() {
        let stream_id = StreamId(52);
        let server_config =
            pq_simple_server_abi("127.0.0.1:0", stream_id, NativeServerCarrierKind::Tcp);
        let acceptor = NativeServerAcceptor::bind(&server_config).await.unwrap();
        let addr = acceptor.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = acceptor.accept().await?;
            session.send_transport_frame(vec![1, 2, 3, 4]).await?;
            session.close().await
        });

        let client_config = ClientConnectConfig {
            url: format!("tcp://{addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleV1,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqSimple,
            reconnect_policy: ReconnectPolicy::disabled(),
        };
        let mut session =
            connect_native_session(&client::abi::NativeClientAbiConfig::tcp(client_config))
                .await
                .unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![1, 2, 3, 4]);
        session.close().await.unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn native_server_abi_serves_quic_mutual_session() {
        let stream_id = StreamId(53);
        let bootstrap = BootstrapConfig::default();
        let now_unix_secs = current_unix_secs();
        let server_identity = issue_server_identity(
            "abi-mutual-server",
            now_unix_secs.saturating_sub(60),
            now_unix_secs + 3_600,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
        )
        .unwrap();
        let issued_credential = issue_client_credential(
            &server_identity,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "abi-mutual-client",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(stream_id),
            now_unix_secs.saturating_sub(60),
            now_unix_secs + 900,
        )
        .unwrap();
        let server_config = NativeServerAbiConfig::quic(
            "127.0.0.1:0",
            TransportServerConfig {
                session: TransportSessionConfig {
                    protection_profile: ProtectionProfileKind::PqMutualV1,
                    bootstrap,
                    ..TransportSessionConfig::default()
                },
                connection_limits: sample_connection_limits(),
                bootstrap_policy: BootstrapPolicy::with_replay_cache(
                    shared_protocol::BootstrapServerConfig {
                        stream_id,
                        direction: StreamDirection::ServerToClient,
                        bootstrap,
                        security: ServerSecurityConfig::PqMutual {
                            server_identity: server_identity.clone(),
                            server_signing_seed: [7; BOOTSTRAP_SIGNING_SEED_LEN],
                            revoked_client_ids: Vec::new(),
                        },
                    },
                    ReplayCache::default(),
                ),
                carrier: ServerCarrierKind::QuicStream,
            },
        );
        let acceptor = NativeServerAcceptor::bind(&server_config).await.unwrap();
        let addr = acceptor.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = acceptor.accept().await?;
            session.send_transport_frame(vec![9, 8, 7, 6]).await?;
            session.close().await
        });
        let client_config = ClientConnectConfig {
            url: format!("quic://{addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqMutualV1,
                bootstrap,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqMutual {
                issued_credential,
                expected_server_identity: server_identity,
            },
            reconnect_policy: ReconnectPolicy::disabled(),
        };
        let mut session =
            connect_native_session(&client::abi::NativeClientAbiConfig::quic(client_config))
                .await
                .unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![9, 8, 7, 6]);
        server_task.await.unwrap().unwrap();
    }
}
