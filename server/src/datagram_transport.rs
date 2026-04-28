use std::{
    collections::VecDeque,
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use bytes::Bytes;
use quinn::{Connection as QuicConnection, Endpoint as QuicEndpoint};
use rand::{RngCore, rngs::OsRng};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use shared_protocol::{
    BootstrapCompleted, BootstrapMessage, BootstrapMessageKind, DatagramPacket,
    DatagramReassemblyBuffer, DatagramSessionError, DatagramSessionMetrics, DatagramTransportState,
    ProtectionProfileKind, Record, ServerBootstrapState, StreamProtector,
    carrier::{
        datagram::{DatagramCarrier, DatagramCarrierLimits},
        reliable::{ReliableCarrier, read_length_prefixed_frame, write_length_prefixed_frame},
    },
    fragment_transport_frame, pack_record_groups,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UdpSocket,
    sync::Mutex,
    time::{Duration, sleep, timeout},
};
use webtrans_quinn::{Server as NativeWebTransportServer, Session as NativeWebTransportSession};

use crate::transport::{
    TransportError, TransportServerConfig, build_quic_transport_config,
    ensure_rustls_crypto_provider,
};

pub struct AuthenticatedUdpSession {
    inner: DatagramServerCore<UdpServerCarrier>,
}

pub struct AuthenticatedQuicDatagramSession {
    inner: DatagramServerCore<QuicDatagramServerCarrier>,
}

pub struct AuthenticatedWebTransportDatagramSession {
    inner: DatagramServerCore<WebTransportDatagramServerCarrier>,
}

pub struct BoundWebTransportDatagramServer {
    server: Mutex<NativeWebTransportServer>,
    local_addr: SocketAddr,
    certificate_hash: Vec<u8>,
}

struct DatagramServerCore<C> {
    protector: StreamProtector,
    carrier: C,
    transport_config: shared_protocol::TransportConfig,
    datagram_state: DatagramTransportState,
    inbound_frames: VecDeque<Vec<u8>>,
    max_datagram_len: usize,
}

struct BootstrapStreamCarrier<W, R> {
    writer: W,
    reader: R,
}

#[derive(Debug)]
struct UdpServerCarrier {
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
}

#[derive(Debug)]
struct QuicDatagramServerCarrier {
    _endpoint: QuicEndpoint,
    connection: QuicConnection,
}

#[derive(Clone)]
struct WebTransportDatagramServerCarrier {
    session: NativeWebTransportSession,
}

impl BoundWebTransportDatagramServer {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn certificate_hash(&self) -> &[u8] {
        &self.certificate_hash
    }
}

fn webtransport_certificate_names_for_bind_addr(bind_addr: SocketAddr) -> Vec<String> {
    let mut names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];

    match bind_addr.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => names.push(ip.to_string()),
        IpAddr::V6(ip) if !ip.is_unspecified() => names.push(ip.to_string()),
        _ => {}
    }

    names.sort();
    names.dedup();
    names
}

impl AuthenticatedUdpSession {
    pub fn protector(&self) -> &StreamProtector {
        &self.inner.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.inner.protector
    }

    pub fn transport_config(&self) -> shared_protocol::TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records(records).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
        self.inner.receive_records_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), TransportError> {
        self.inner.close().await
    }
}

impl AuthenticatedQuicDatagramSession {
    pub fn protector(&self) -> &StreamProtector {
        &self.inner.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.inner.protector
    }

    pub fn transport_config(&self) -> shared_protocol::TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records(records).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
        self.inner.receive_records_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), TransportError> {
        self.inner.close().await
    }
}

impl AuthenticatedWebTransportDatagramSession {
    pub fn protector(&self) -> &StreamProtector {
        &self.inner.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.inner.protector
    }

    pub fn transport_config(&self) -> shared_protocol::TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records(records).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
        self.inner.receive_records_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), TransportError> {
        self.inner.close().await
    }
}

impl fmt::Debug for AuthenticatedUdpSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticatedUdpSession")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for AuthenticatedQuicDatagramSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticatedQuicDatagramSession")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for AuthenticatedWebTransportDatagramSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticatedWebTransportDatagramSession")
            .finish_non_exhaustive()
    }
}

impl<C> DatagramServerCore<C>
where
    C: DatagramCarrier<Error = TransportError>,
{
    async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let datagrams = self.datagram_state.encode_transport_frame(&frame)?;
        for datagram in datagrams {
            self.carrier.send_datagram(&datagram).await?;
        }
        Ok(())
    }

    async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let datagrams = self.datagram_state.encode_transport_frames(frames)?;
        for datagram in datagrams {
            self.carrier.send_datagram(&datagram).await?;
        }
        Ok(())
    }

    async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = pack_record_groups(records, self.transport_config)
            .into_iter()
            .map(|group| self.protector.protect_transport_records(group))
            .collect::<Result<Vec<_>, _>>()?;
        self.send_transport_frames(frames).await
    }

    async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
        let mut records = Vec::new();
        while let Some(frame) = self.read_transport_frame().await? {
            for record in self.protector.unprotect_transport_frame(&frame)? {
                let is_close = matches!(
                    record.header.record_type,
                    shared_protocol::RecordType::Close
                );
                records.push(record);
                if is_close {
                    return Ok(records);
                }
            }
        }
        Ok(records)
    }

    async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        loop {
            if let Some(frame) = self.inbound_frames.pop_front() {
                if frame.is_empty() {
                    continue;
                }
                return Ok(Some(frame));
            }
            match timeout(
                Duration::from_millis(self.datagram_state.reliability().repair_timeout_ms),
                self.carrier.recv_datagram(self.max_datagram_len),
            )
            .await
            {
                Ok(Ok(Some(datagram))) => {
                    let outcome = self.datagram_state.handle_datagram(&datagram)?;
                    for outbound in outcome.outbound_datagrams {
                        self.carrier.send_datagram(&outbound).await?;
                    }
                    if outcome.close_code.is_some() {
                        return Ok(None);
                    }
                    self.inbound_frames.extend(outcome.ready_frames);
                }
                Ok(Ok(None)) => return Ok(None),
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    let repairs = self.datagram_state.build_repair_requests()?;
                    for outbound in repairs {
                        self.carrier.send_datagram(&outbound).await?;
                    }
                }
            }
        }
    }

    async fn close(mut self) -> Result<(), TransportError> {
        if let Ok(close) = self.datagram_state.encode_close(0) {
            let _ = self.carrier.send_datagram(&close).await;
        }
        sleep(Duration::from_millis(
            self.datagram_state.reliability().repair_timeout_ms.max(25),
        ))
        .await;
        self.carrier.close().await
    }
}

pub async fn bind_udp_socket(addr: &str) -> Result<Arc<UdpSocket>, TransportError> {
    Ok(Arc::new(UdpSocket::bind(addr).await?))
}

pub async fn accept_udp_session(
    socket: &Arc<UdpSocket>,
    config: &TransportServerConfig,
) -> Result<AuthenticatedUdpSession, TransportError> {
    validate_datagram_profile(config)?;
    let (peer_addr, completed) = bootstrap_server_over_udp(socket, config).await?;
    let carrier = UdpServerCarrier {
        socket: Arc::clone(socket),
        peer_addr,
    };
    let max_datagram_len = carrier.limits().max_datagram_size.max(2048);
    Ok(AuthenticatedUdpSession {
        inner: DatagramServerCore {
            protector: protector_from_completed(&completed, config.session.protection_profile),
            carrier,
            transport_config: config.session.transport,
            datagram_state: DatagramTransportState::new(
                config.bootstrap_policy.server.stream_id,
                config.session.datagram.reliability,
            ),
            inbound_frames: VecDeque::new(),
            max_datagram_len,
        },
    })
}

pub async fn serve_udp_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let socket = bind_udp_socket(addr).await?;
    for _ in 0..connections {
        let mut session = accept_udp_session(&socket, &config).await?;
        session.send_plain_records(records.clone()).await?;
        session.close().await?;
    }
    Ok(())
}

pub async fn accept_quic_datagram_session(
    endpoint: &QuicEndpoint,
    config: &TransportServerConfig,
) -> Result<AuthenticatedQuicDatagramSession, TransportError> {
    validate_datagram_profile(config)?;
    let connecting = endpoint
        .accept()
        .await
        .ok_or_else(|| TransportError::Quic("quic endpoint closed before accept".to_string()))?;
    let connection = connecting
        .await
        .map_err(|error| TransportError::Quic(error.to_string()))?;
    let (send, recv) = connection
        .accept_bi()
        .await
        .map_err(|error| TransportError::Quic(error.to_string()))?;
    let mut carrier = BootstrapStreamCarrier {
        writer: send,
        reader: recv,
    };
    let completed = bootstrap_server_over_reliable_carrier(&mut carrier, config).await?;
    let datagram_carrier = QuicDatagramServerCarrier {
        _endpoint: endpoint.clone(),
        connection,
    };
    let max_datagram_len = datagram_carrier.limits().max_datagram_size.max(2048);
    Ok(AuthenticatedQuicDatagramSession {
        inner: DatagramServerCore {
            protector: protector_from_completed(&completed, config.session.protection_profile),
            carrier: datagram_carrier,
            transport_config: config.session.transport,
            datagram_state: DatagramTransportState::new(
                config.bootstrap_policy.server.stream_id,
                config.session.datagram.reliability,
            ),
            inbound_frames: VecDeque::new(),
            max_datagram_len,
        },
    })
}

pub async fn serve_quic_datagram_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let endpoint = crate::transport::bind_quic_endpoint(
        addr,
        config.connection_limits.max_transport_frame_bytes,
    )?;
    for _ in 0..connections {
        let mut session = accept_quic_datagram_session(&endpoint, &config).await?;
        session.send_plain_records(records.clone()).await?;
        session.close().await?;
    }
    Ok(())
}

pub fn bind_webtransport_datagram_server(
    addr: &str,
    max_transport_frame_bytes: usize,
) -> Result<BoundWebTransportDatagramServer, TransportError> {
    ensure_rustls_crypto_provider()?;
    let bind_addr: SocketAddr = addr.parse().map_err(|error| {
        TransportError::WebTransport(format!("invalid WebTransport bind addr: {error}"))
    })?;
    let certificate =
        generate_simple_self_signed(webtransport_certificate_names_for_bind_addr(bind_addr))
            .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    let cert_der: CertificateDer<'static> = certificate.cert.der().clone();
    let certificate_hash = Sha256::digest(cert_der.as_ref()).to_vec();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certificate.signing_key.serialize_der(),
    ));
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    tls.alpn_protocols = vec![webtrans_quinn::ALPN.as_bytes().to_vec()];
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport_config(Arc::new(build_quic_transport_config(
        max_transport_frame_bytes,
    )?));
    let endpoint = QuicEndpoint::server(server_config, bind_addr)
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    let local_addr = endpoint
        .local_addr()
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    Ok(BoundWebTransportDatagramServer {
        server: Mutex::new(NativeWebTransportServer::new(endpoint)),
        local_addr,
        certificate_hash,
    })
}

pub async fn accept_webtransport_datagram_session(
    bound: &BoundWebTransportDatagramServer,
    config: &TransportServerConfig,
) -> Result<AuthenticatedWebTransportDatagramSession, TransportError> {
    validate_datagram_profile(config)?;
    let request = {
        let mut server = bound.server.lock().await;
        server
            .accept()
            .await
            .ok_or_else(|| TransportError::WebTransport("webtransport server closed".to_string()))?
    };
    let session = request
        .ok()
        .await
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    let (send, recv) = session
        .accept_bi()
        .await
        .map_err(|error| TransportError::WebTransport(error.to_string()))?;
    let mut carrier = BootstrapStreamCarrier {
        writer: send,
        reader: recv,
    };
    let completed = bootstrap_server_over_reliable_carrier(&mut carrier, config).await?;
    let datagram_carrier = WebTransportDatagramServerCarrier {
        session: session.clone(),
    };
    let max_datagram_len = datagram_carrier.limits().max_datagram_size.max(2048);
    Ok(AuthenticatedWebTransportDatagramSession {
        inner: DatagramServerCore {
            protector: protector_from_completed(&completed, config.session.protection_profile),
            carrier: datagram_carrier,
            transport_config: config.session.transport,
            datagram_state: DatagramTransportState::new(
                config.bootstrap_policy.server.stream_id,
                config.session.datagram.reliability,
            ),
            inbound_frames: VecDeque::new(),
            max_datagram_len,
        },
    })
}

pub async fn serve_webtransport_datagram_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let bound = bind_webtransport_datagram_server(
        addr,
        config.connection_limits.max_transport_frame_bytes,
    )?;
    for _ in 0..connections {
        let mut session = accept_webtransport_datagram_session(&bound, &config).await?;
        session.send_plain_records(records.clone()).await?;
        session.close().await?;
    }
    Ok(())
}

fn validate_datagram_profile(config: &TransportServerConfig) -> Result<(), TransportError> {
    let bootstrap_profile = config.bootstrap_policy.server.protection_profile();
    let expected = config.session.protection_profile.canonical_stream_profile();
    if expected != bootstrap_profile || !config.session.protection_profile.is_datagram_family() {
        return Err(TransportError::SecurityProfileMismatch {
            session: config.session.protection_profile,
            bootstrap: bootstrap_profile,
        });
    }
    Ok(())
}

fn protector_from_completed(
    completed: &BootstrapCompleted,
    requested_profile: ProtectionProfileKind,
) -> StreamProtector {
    StreamProtector::from_bootstrap_root(
        requested_profile,
        completed.stream_id,
        completed.direction,
        completed.root,
    )
}

fn first_client_message_kind(protection_profile: ProtectionProfileKind) -> BootstrapMessageKind {
    if protection_profile
        .canonical_stream_profile()
        .is_mutual_family()
    {
        BootstrapMessageKind::ClientHello
    } else {
        BootstrapMessageKind::SimpleClientHello
    }
}

async fn bootstrap_server_over_reliable_carrier<C>(
    carrier: &mut C,
    config: &TransportServerConfig,
) -> Result<BootstrapCompleted, TransportError>
where
    C: ReliableCarrier<Error = TransportError>,
{
    let client_hello = receive_bootstrap_message(
        carrier,
        config,
        first_client_message_kind(config.session.protection_profile),
    )
    .await?;
    let mut server_nonce = [0_u8; shared_protocol::BOOTSTRAP_NONCE_LEN];
    let mut server_kem_seed = [0_u8; shared_protocol::BOOTSTRAP_KEM_SEED_LEN];
    OsRng.fill_bytes(&mut server_nonce);
    OsRng.fill_bytes(&mut server_kem_seed);
    let response = {
        let mut replay_cache = config
            .bootstrap_policy
            .replay_cache()
            .lock()
            .map_err(|_| TransportError::ReplayCachePoisoned)?;
        ServerBootstrapState::respond_to_client_hello(
            config.bootstrap_policy.server.clone(),
            &mut replay_cache,
            client_hello,
            unix_time_secs(),
            server_nonce,
            server_kem_seed,
        )?
    };
    carrier
        .send_frame(&response.outbound.to_frame(&config.session.bootstrap)?)
        .await?;
    if let Some(completed) = response.completed {
        Ok(completed)
    } else {
        let client_finish =
            receive_bootstrap_message(carrier, config, BootstrapMessageKind::ClientFinish).await?;
        let state = response
            .state
            .ok_or_else(|| TransportError::Quic("missing datagram bootstrap state".to_string()))?;
        let (completed, server_finish) = state.handle_client_finish(client_finish)?;
        carrier
            .send_frame(&server_finish.to_frame(&config.session.bootstrap)?)
            .await?;
        Ok(completed)
    }
}

async fn bootstrap_server_over_udp(
    socket: &Arc<UdpSocket>,
    config: &TransportServerConfig,
) -> Result<(SocketAddr, BootstrapCompleted), TransportError> {
    let (peer_addr, client_hello) = receive_udp_bootstrap_message(
        socket,
        None,
        config,
        first_client_message_kind(config.session.protection_profile),
    )
    .await?;
    let mut server_nonce = [0_u8; shared_protocol::BOOTSTRAP_NONCE_LEN];
    let mut server_kem_seed = [0_u8; shared_protocol::BOOTSTRAP_KEM_SEED_LEN];
    OsRng.fill_bytes(&mut server_nonce);
    OsRng.fill_bytes(&mut server_kem_seed);
    let response = {
        let mut replay_cache = config
            .bootstrap_policy
            .replay_cache()
            .lock()
            .map_err(|_| TransportError::ReplayCachePoisoned)?;
        ServerBootstrapState::respond_to_client_hello(
            config.bootstrap_policy.server.clone(),
            &mut replay_cache,
            client_hello,
            unix_time_secs(),
            server_nonce,
            server_kem_seed,
        )?
    };
    send_udp_bootstrap_message(
        socket,
        peer_addr,
        config.bootstrap_policy.server.stream_id,
        &response.outbound,
        config,
    )
    .await?;
    if let Some(completed) = response.completed {
        Ok((peer_addr, completed))
    } else {
        let (_same_peer, client_finish) = receive_udp_bootstrap_message(
            socket,
            Some(peer_addr),
            config,
            BootstrapMessageKind::ClientFinish,
        )
        .await?;
        let state = response
            .state
            .ok_or_else(|| TransportError::Quic("missing datagram bootstrap state".to_string()))?;
        let (completed, server_finish) = state.handle_client_finish(client_finish)?;
        send_udp_bootstrap_message(
            socket,
            peer_addr,
            config.bootstrap_policy.server.stream_id,
            &server_finish,
            config,
        )
        .await?;
        Ok((peer_addr, completed))
    }
}

async fn send_udp_bootstrap_message(
    socket: &Arc<UdpSocket>,
    peer_addr: SocketAddr,
    stream_id: shared_protocol::StreamId,
    message: &BootstrapMessage,
    config: &TransportServerConfig,
) -> Result<(), TransportError> {
    let packets = fragment_transport_frame(
        stream_id,
        0,
        &message.to_frame(&config.session.bootstrap)?,
        config.session.datagram.reliability,
    )
    .map_err(DatagramSessionError::from)
    .map_err(TransportError::from)?;
    for packet in packets {
        let encoded = packet
            .to_bytes()
            .map_err(DatagramSessionError::from)
            .map_err(TransportError::from)?;
        socket.send_to(&encoded, peer_addr).await?;
    }
    Ok(())
}

async fn receive_udp_bootstrap_message(
    socket: &Arc<UdpSocket>,
    peer_addr: Option<SocketAddr>,
    config: &TransportServerConfig,
    expected_kind: BootstrapMessageKind,
) -> Result<(SocketAddr, BootstrapMessage), TransportError> {
    let deadline =
        Instant::now() + Duration::from_millis(config.session.bootstrap.handshake_timeout_ms);
    let mut buffer = vec![0_u8; config.session.bootstrap.max_bootstrap_frame_bytes.max(2048)];
    let mut reassembly = DatagramReassemblyBuffer::default();
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(TransportError::HandshakeTimeout(
                config.session.bootstrap.handshake_timeout_ms,
            ));
        }
        let remaining = deadline.duration_since(now);
        let (bytes_read, addr) = timeout(remaining, socket.recv_from(&mut buffer))
            .await
            .map_err(|_| {
                TransportError::HandshakeTimeout(config.session.bootstrap.handshake_timeout_ms)
            })??;
        if let Some(expected_addr) = peer_addr {
            if expected_addr != addr {
                continue;
            }
        }
        let packet = DatagramPacket::from_bytes(&buffer[..bytes_read])
            .map_err(DatagramSessionError::from)
            .map_err(TransportError::from)?;
        match packet {
            DatagramPacket::Bootstrap { frame, .. } => {
                let message = BootstrapMessage::from_frame(&frame, &config.session.bootstrap)?;
                if message.kind() == expected_kind {
                    return Ok((addr, message));
                }
            }
            DatagramPacket::Data { header, payload } => {
                reassembly
                    .insert_data(header, payload)
                    .map_err(DatagramSessionError::from)
                    .map_err(TransportError::from)?;
                if let Some((_message_id, frame)) = reassembly.pop_next_ready() {
                    let message = BootstrapMessage::from_frame(&frame, &config.session.bootstrap)?;
                    if message.kind() == expected_kind {
                        return Ok((addr, message));
                    }
                }
            }
            _ => continue,
        }
    }
}

async fn receive_bootstrap_message<C>(
    carrier: &mut C,
    config: &TransportServerConfig,
    expected_kind: BootstrapMessageKind,
) -> Result<BootstrapMessage, TransportError>
where
    C: ReliableCarrier<Error = TransportError>,
{
    timeout(
        Duration::from_millis(config.connection_limits.handshake_timeout_ms),
        async {
            match carrier
                .recv_frame(config.session.bootstrap.max_bootstrap_frame_bytes)
                .await?
            {
                Some(frame) => BootstrapMessage::from_frame(&frame, &config.session.bootstrap)
                    .map_err(TransportError::Bootstrap),
                None => Err(TransportError::Bootstrap(
                    shared_protocol::BootstrapError::UnexpectedMessageKind {
                        expected: expected_kind,
                        actual: expected_kind,
                    },
                )),
            }
        },
    )
    .await
    .map_err(|_| TransportError::HandshakeTimeout(config.connection_limits.handshake_timeout_ms))?
}

fn unix_time_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[async_trait]
impl<W, R> ReliableCarrier for BootstrapStreamCarrier<W, R>
where
    W: AsyncWrite + Unpin + Send,
    R: AsyncRead + Unpin + Send,
{
    type Error = TransportError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        write_length_prefixed_frame(&mut self.writer, frame).await?;
        Ok(())
    }

    async fn recv_frame(&mut self, max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(read_length_prefixed_frame(&mut self.reader, max_frame_len).await?)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DatagramCarrier for UdpServerCarrier {
    type Error = TransportError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: 65_507,
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.socket.send_to(datagram, self.peer_addr).await?;
        Ok(())
    }

    async fn recv_datagram(
        &mut self,
        max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut buffer = vec![0_u8; max_datagram_len.max(2048)];
        loop {
            let (bytes_read, peer_addr) = self.socket.recv_from(&mut buffer).await?;
            if peer_addr != self.peer_addr {
                continue;
            }
            buffer.truncate(bytes_read);
            return Ok(Some(buffer));
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DatagramCarrier for QuicDatagramServerCarrier {
    type Error = TransportError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: self.connection.max_datagram_size().unwrap_or(2048),
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.connection
            .send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|error| TransportError::Quic(error.to_string()))
    }

    async fn recv_datagram(
        &mut self,
        _max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        self.connection
            .read_datagram()
            .await
            .map(|bytes| Some(bytes.to_vec()))
            .map_err(|error| TransportError::Quic(error.to_string()))
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.connection
            .close(0_u32.into(), b"pulzz/datagram_close");
        Ok(())
    }
}

#[async_trait]
impl DatagramCarrier for WebTransportDatagramServerCarrier {
    type Error = TransportError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: self.session.max_datagram_size().max(2048),
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.session
            .send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|error| TransportError::WebTransport(error.to_string()))
    }

    async fn recv_datagram(
        &mut self,
        _max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        self.session
            .read_datagram()
            .await
            .map(|bytes| Some(bytes.to_vec()))
            .map_err(|error| TransportError::WebTransport(error.to_string()))
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.session.close(0, b"pulzz/datagram_close");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use client::{
        ClientConnectConfig, ClientSecurityConfig, ReconnectPolicy, connect_quic_datagram_session,
        connect_udp_session, connect_webtransport_datagram_session,
    };
    use shared_protocol::{
        BOOTSTRAP_SIGNING_SEED_LEN, BootstrapConfig, BootstrapServerConfig, CredentialScope,
        PqSimpleServerBootstrapConfig, ServerSecurityConfig, StreamDirection, StreamId,
        TransportSessionConfig, issue_client_credential, issue_server_identity,
    };

    fn sample_connection_limits() -> crate::transport::ConnectionLimits {
        crate::transport::ConnectionLimits::default()
    }

    fn sample_scope(stream_id: StreamId) -> CredentialScope {
        CredentialScope {
            stream_id: Some(stream_id),
            allow_client_to_server: true,
            allow_server_to_client: true,
        }
    }

    fn simple_datagram_server_config(stream_id: StreamId) -> TransportServerConfig {
        let bootstrap = BootstrapConfig::default();
        TransportServerConfig {
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleDgramV1,
                bootstrap,
                ..TransportSessionConfig::default()
            },
            connection_limits: sample_connection_limits(),
            bootstrap_policy: crate::transport::BootstrapPolicy::with_replay_cache(
                BootstrapServerConfig {
                    stream_id,
                    direction: StreamDirection::ServerToClient,
                    bootstrap,
                    security: ServerSecurityConfig::PqSimple {
                        bootstrap: PqSimpleServerBootstrapConfig {
                            server_id: "datagram-simple-server".to_string(),
                        },
                    },
                },
                shared_protocol::ReplayCache::default(),
            ),
            carrier: crate::transport::ServerCarrierKind::WebSocket,
        }
    }

    fn mutual_datagram_server_config(
        stream_id: StreamId,
    ) -> (TransportServerConfig, ClientSecurityConfig) {
        let bootstrap = BootstrapConfig::default();
        let now = unix_time_secs();
        let server_identity = issue_server_identity(
            "datagram-mutual-server",
            now.saturating_sub(60),
            now + 3_600,
            [9; BOOTSTRAP_SIGNING_SEED_LEN],
        )
        .unwrap();
        let issued_credential = issue_client_credential(
            &server_identity,
            [9; BOOTSTRAP_SIGNING_SEED_LEN],
            "datagram-mutual-client",
            [4; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(stream_id),
            now.saturating_sub(60),
            now + 900,
        )
        .unwrap();
        (
            TransportServerConfig {
                session: TransportSessionConfig {
                    protection_profile: ProtectionProfileKind::PqMutualDgramV1,
                    bootstrap,
                    ..TransportSessionConfig::default()
                },
                connection_limits: sample_connection_limits(),
                bootstrap_policy: crate::transport::BootstrapPolicy::with_replay_cache(
                    BootstrapServerConfig {
                        stream_id,
                        direction: StreamDirection::ServerToClient,
                        bootstrap,
                        security: ServerSecurityConfig::PqMutual {
                            server_identity: server_identity.clone(),
                            server_signing_seed: [9; BOOTSTRAP_SIGNING_SEED_LEN],
                            revoked_client_ids: Vec::new(),
                        },
                    },
                    shared_protocol::ReplayCache::default(),
                ),
                carrier: crate::transport::ServerCarrierKind::WebSocket,
            },
            ClientSecurityConfig::PqMutual {
                issued_credential,
                expected_server_identity: server_identity,
            },
        )
    }

    #[tokio::test]
    async fn udp_datagram_session_bootstraps_for_pq_simple() {
        let stream_id = StreamId(991);
        let server_config = simple_datagram_server_config(stream_id);
        let socket = bind_udp_socket("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = accept_udp_session(&socket, &server_config).await?;
            session.send_transport_frame(vec![1, 2, 3, 4]).await?;
            session.close().await
        });

        let client_config = ClientConnectConfig {
            url: format!("udp://{addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleDgramV1,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqSimple,
            reconnect_policy: ReconnectPolicy::disabled(),
        };
        let mut session = connect_udp_session(&client_config).await.unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![1, 2, 3, 4]);
        session.close().await.unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_datagram_session_bootstraps_for_pq_mutual() {
        let stream_id = StreamId(992);
        let (server_config, client_security) = mutual_datagram_server_config(stream_id);
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        drop(socket);
        let endpoint = crate::transport::bind_quic_endpoint(
            &addr.to_string(),
            server_config.connection_limits.max_transport_frame_bytes,
        )
        .unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = accept_quic_datagram_session(&endpoint, &server_config).await?;
            session.send_transport_frame(vec![5, 6, 7, 8]).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), TransportError>(())
        });

        let client_config = ClientConnectConfig {
            url: format!("quic_datagram://{addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqMutualDgramV1,
                ..TransportSessionConfig::default()
            },
            security: client_security,
            reconnect_policy: ReconnectPolicy::disabled(),
        };
        let mut session = connect_quic_datagram_session(&client_config).await.unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![5, 6, 7, 8]);
        let _ = session.close().await;
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn webtransport_datagram_session_bootstraps_for_pq_simple() {
        let stream_id = StreamId(993);
        let server_config = simple_datagram_server_config(stream_id);
        let bound = bind_webtransport_datagram_server(
            "127.0.0.1:0",
            server_config.connection_limits.max_transport_frame_bytes,
        )
        .unwrap();
        let addr = bound.local_addr();
        let server_task = tokio::spawn(async move {
            let mut session = accept_webtransport_datagram_session(&bound, &server_config).await?;
            session.send_transport_frame(vec![9, 8, 7, 6]).await?;
            Ok::<(), TransportError>(())
        });

        let client_config = ClientConnectConfig {
            url: format!("https://127.0.0.1:{}/pulzz", addr.port()),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleDgramV1,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqSimple,
            reconnect_policy: ReconnectPolicy::disabled(),
        };
        let mut session = connect_webtransport_datagram_session(&client_config)
            .await
            .unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![9, 8, 7, 6]);
        let _ = session.close().await;
        server_task.await.unwrap().unwrap();
    }
}
