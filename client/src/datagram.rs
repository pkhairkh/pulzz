#![cfg(not(target_arch = "wasm32"))]

use std::{collections::VecDeque, fmt, net::SocketAddr, time::Instant};

use async_trait::async_trait;
use bytes::Bytes;
use quinn::{Connection as QuicConnection, Endpoint as QuicEndpoint};
use rand::{RngCore, rngs::OsRng};
use shared_protocol::{
    BootstrapCompleted, BootstrapMessage, BootstrapMessageKind, ClientBootstrapState,
    DatagramPacket, DatagramReassemblyBuffer, DatagramSessionError, DatagramSessionMetrics,
    DatagramTransportState, ProtectionProfileKind, Record, StreamProtector,
    carrier::{
        datagram::{DatagramCarrier, DatagramCarrierLimits},
        reliable::{ReliableCarrier, read_length_prefixed_frame, write_length_prefixed_frame},
    },
    fragment_transport_frame,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{UdpSocket, lookup_host},
    time::{Duration, sleep, timeout},
};
use url::Url;
use webtrans_quinn::{
    ClientBuilder as WebTransportClientBuilder, Session as NativeWebTransportSession,
};

use crate::{
    ClientConnectConfig, ClientConnectError, ClientSecurityConfig, ClientSession, TransportConfig,
};

pub struct ConnectedUdpSession {
    inner: DatagramClientCore<UdpClientCarrier>,
}

pub struct ConnectedQuicDatagramSession {
    inner: DatagramClientCore<QuicDatagramClientCarrier>,
}

pub struct ConnectedWebTransportDatagramSession {
    inner: DatagramClientCore<WebTransportDatagramClientCarrier>,
}

struct DatagramClientCore<C> {
    session: ClientSession,
    carrier: C,
    transport_config: TransportConfig,
    datagram_state: DatagramTransportState,
    inbound_frames: VecDeque<Vec<u8>>,
    max_datagram_len: usize,
}

struct BootstrapStreamCarrier<W, R> {
    writer: W,
    reader: R,
}

#[derive(Debug)]
struct UdpClientCarrier {
    socket: UdpSocket,
}

#[derive(Debug)]
struct QuicDatagramClientCarrier {
    _endpoint: QuicEndpoint,
    connection: QuicConnection,
}

#[derive(Clone)]
struct WebTransportDatagramClientCarrier {
    session: NativeWebTransportSession,
}

impl ConnectedUdpSession {
    pub fn session(&self) -> &ClientSession {
        &self.inner.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.inner.session
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records_packed(records).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        self.inner.receive_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), ClientConnectError> {
        self.inner.close().await
    }
}

impl ConnectedQuicDatagramSession {
    pub fn session(&self) -> &ClientSession {
        &self.inner.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.inner.session
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records_packed(records).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        self.inner.receive_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), ClientConnectError> {
        self.inner.close().await
    }
}

impl ConnectedWebTransportDatagramSession {
    pub fn session(&self) -> &ClientSession {
        &self.inner.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.inner.session
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.inner.transport_config
    }

    pub fn datagram_metrics(&self) -> &DatagramSessionMetrics {
        self.inner.datagram_state.metrics()
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        self.inner.send_transport_frame(frame).await
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.inner.send_transport_frames(frames).await
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.inner.send_plain_records_packed(records).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        self.inner.receive_until_close().await
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        self.inner.read_transport_frame().await
    }

    pub async fn close(self) -> Result<(), ClientConnectError> {
        self.inner.close().await
    }
}

impl fmt::Debug for ConnectedUdpSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedUdpSession")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ConnectedQuicDatagramSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedQuicDatagramSession")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ConnectedWebTransportDatagramSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedWebTransportDatagramSession")
            .finish_non_exhaustive()
    }
}

impl<C> DatagramClientCore<C>
where
    C: DatagramCarrier<Error = ClientConnectError>,
{
    async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        let datagrams = self.datagram_state.encode_transport_frame(&frame)?;
        for datagram in datagrams {
            self.carrier.send_datagram(&datagram).await?;
        }
        Ok(())
    }

    async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
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

    async fn send_plain_records_packed<I>(&mut self, records: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = self
            .session
            .pack_protected_trace(records, self.transport_config)
            .map_err(ClientConnectError::Apply)?;
        self.send_transport_frames(frames).await
    }

    async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        let mut applied = 0_usize;
        while let Some(frame) = self.read_transport_frame().await? {
            applied += self
                .session
                .apply_protected_transport_frame(&frame)
                .map_err(ClientConnectError::Apply)?;
        }
        Ok(applied)
    }

    async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        loop {
            if let Some(frame) = self.inbound_frames.pop_front() {
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
                    if let Some(_code) = outcome.close_code {
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

    async fn close(mut self) -> Result<(), ClientConnectError> {
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

pub async fn connect_udp_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedUdpSession, ClientConnectError> {
    connect_datagram_with_retries(config, |config| async move {
        let remote = resolve_socket_addr(normalize_endpoint(&config.url, "udp://")).await?;
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(remote).await?;
        let completed = bootstrap_client_over_udp(&socket, &config).await?;
        let session = ClientSession::new(protector_from_completed(
            &completed,
            config.session.protection_profile,
        ));
        let carrier = UdpClientCarrier { socket };
        let max_datagram_len = carrier.limits().max_datagram_size.max(2048);
        Ok(ConnectedUdpSession {
            inner: DatagramClientCore {
                session,
                carrier,
                transport_config: config.session.transport,
                datagram_state: DatagramTransportState::new(
                    config.stream_id,
                    config.session.datagram.reliability,
                ),
                inbound_frames: VecDeque::new(),
                max_datagram_len,
            },
        })
    })
    .await
}

pub async fn connect_quic_datagram_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedQuicDatagramSession, ClientConnectError> {
    connect_datagram_with_retries(config, |config| async move {
        let mut endpoint = QuicEndpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(quic_client_config(
            config.session.runtime_limits.max_transport_frame_bytes,
        )?);
        let address =
            resolve_socket_addr(normalize_endpoint(&config.url, "quic_datagram://")).await?;
        let server_name = quic_server_name(&config.url);
        let connection = endpoint
            .connect(address, &server_name)
            .map_err(|error| ClientConnectError::Quic(error.to_string()))?
            .await
            .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
        let mut carrier = BootstrapStreamCarrier {
            writer: send,
            reader: recv,
        };
        let completed = bootstrap_client_over_reliable_carrier(&mut carrier, &config).await?;
        let datagram_carrier = QuicDatagramClientCarrier {
            _endpoint: endpoint,
            connection,
        };
        let max_datagram_len = datagram_carrier.limits().max_datagram_size.max(2048);
        Ok(ConnectedQuicDatagramSession {
            inner: DatagramClientCore {
                session: ClientSession::new(protector_from_completed(
                    &completed,
                    config.session.protection_profile,
                )),
                carrier: datagram_carrier,
                transport_config: config.session.transport,
                datagram_state: DatagramTransportState::new(
                    config.stream_id,
                    config.session.datagram.reliability,
                ),
                inbound_frames: VecDeque::new(),
                max_datagram_len,
            },
        })
    })
    .await
}

pub async fn connect_webtransport_datagram_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedWebTransportDatagramSession, ClientConnectError> {
    connect_datagram_with_retries(config, |config| async move {
        let url = webtransport_url(&config.url)?;
        let client = WebTransportClientBuilder::new()
            .dangerous()
            .with_no_certificate_verification()
            .map_err(|error| ClientConnectError::WebTransport(error.to_string()))?;
        let session = client
            .connect(url)
            .await
            .map_err(|error| ClientConnectError::WebTransport(error.to_string()))?;
        let (send, recv) = session
            .open_bi()
            .await
            .map_err(|error| ClientConnectError::WebTransport(error.to_string()))?;
        let mut carrier = BootstrapStreamCarrier {
            writer: send,
            reader: recv,
        };
        let completed = bootstrap_client_over_reliable_carrier(&mut carrier, &config).await?;
        let datagram_carrier = WebTransportDatagramClientCarrier { session };
        let max_datagram_len = datagram_carrier.limits().max_datagram_size.max(2048);
        Ok(ConnectedWebTransportDatagramSession {
            inner: DatagramClientCore {
                session: ClientSession::new(protector_from_completed(
                    &completed,
                    config.session.protection_profile,
                )),
                carrier: datagram_carrier,
                transport_config: config.session.transport,
                datagram_state: DatagramTransportState::new(
                    config.stream_id,
                    config.session.datagram.reliability,
                ),
                inbound_frames: VecDeque::new(),
                max_datagram_len,
            },
        })
    })
    .await
}

async fn connect_datagram_with_retries<T, F, Fut>(
    config: &ClientConnectConfig,
    connect_once: F,
) -> Result<T, ClientConnectError>
where
    F: Fn(ClientConnectConfig) -> Fut,
    Fut: std::future::Future<Output = Result<T, ClientConnectError>>,
{
    validate_datagram_profile(config)?;
    let attempts = config.reconnect_policy.max_attempts.max(1);
    let mut backoff_ms = config.reconnect_policy.initial_backoff_ms.max(1);
    let mut last_error = None;

    for attempt in 0..attempts {
        match connect_once(config.clone()).await {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(config.reconnect_policy.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }

    Err(last_error.expect("at least one datagram connect attempt was executed"))
}

fn validate_datagram_profile(config: &ClientConnectConfig) -> Result<(), ClientConnectError> {
    let security_profile = security_profile(&config.security);
    let expected = config.session.protection_profile.canonical_stream_profile();
    if expected != security_profile || !config.session.protection_profile.is_datagram_family() {
        return Err(ClientConnectError::SecurityProfileMismatch {
            session: config.session.protection_profile,
            security: security_profile,
        });
    }
    Ok(())
}

fn security_profile(security: &ClientSecurityConfig) -> ProtectionProfileKind {
    match security {
        ClientSecurityConfig::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
        ClientSecurityConfig::PqSimple => ProtectionProfileKind::PqSimpleV1,
    }
}

fn first_server_message_kind(protection_profile: ProtectionProfileKind) -> BootstrapMessageKind {
    if protection_profile
        .canonical_stream_profile()
        .is_mutual_family()
    {
        BootstrapMessageKind::ServerHello
    } else {
        BootstrapMessageKind::SimpleServerHello
    }
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

async fn bootstrap_client_over_reliable_carrier<C>(
    carrier: &mut C,
    config: &ClientConnectConfig,
) -> Result<BootstrapCompleted, ClientConnectError>
where
    C: ReliableCarrier<Error = ClientConnectError>,
{
    let canonical_profile = config.session.protection_profile.canonical_stream_profile();
    let mut client_nonce = [0_u8; shared_protocol::BOOTSTRAP_NONCE_LEN];
    let mut client_kem_seed = [0_u8; shared_protocol::BOOTSTRAP_KEM_SEED_LEN];
    OsRng.fill_bytes(&mut client_nonce);
    OsRng.fill_bytes(&mut client_kem_seed);

    let (mut bootstrap_state, client_hello) = ClientBootstrapState::start(
        config.bootstrap_client_config(),
        client_nonce,
        client_kem_seed,
    )?;
    carrier
        .send_frame(&client_hello.to_frame(&config.session.bootstrap)?)
        .await?;
    let server_hello = receive_bootstrap_message(
        carrier,
        config,
        first_server_message_kind(canonical_profile),
    )
    .await?;
    let progress = bootstrap_state.handle_server_hello(server_hello, unix_time_secs())?;
    if let Some(outbound) = progress.outbound {
        carrier
            .send_frame(&outbound.to_frame(&config.session.bootstrap)?)
            .await?;
        let server_finish =
            receive_bootstrap_message(carrier, config, BootstrapMessageKind::ServerFinish).await?;
        Ok(bootstrap_state.handle_server_finish(server_finish)?)
    } else {
        progress.completed.ok_or_else(|| {
            ClientConnectError::Bootstrap(shared_protocol::BootstrapError::UnexpectedMessageKind {
                expected: first_server_message_kind(canonical_profile),
                actual: first_server_message_kind(canonical_profile),
            })
        })
    }
}

async fn bootstrap_client_over_udp(
    socket: &UdpSocket,
    config: &ClientConnectConfig,
) -> Result<BootstrapCompleted, ClientConnectError> {
    let canonical_profile = config.session.protection_profile.canonical_stream_profile();
    let mut client_nonce = [0_u8; shared_protocol::BOOTSTRAP_NONCE_LEN];
    let mut client_kem_seed = [0_u8; shared_protocol::BOOTSTRAP_KEM_SEED_LEN];
    OsRng.fill_bytes(&mut client_nonce);
    OsRng.fill_bytes(&mut client_kem_seed);

    let (mut bootstrap_state, client_hello) = ClientBootstrapState::start(
        config.bootstrap_client_config(),
        client_nonce,
        client_kem_seed,
    )?;
    send_udp_bootstrap_message(socket, config.stream_id, &client_hello, config).await?;
    let server_hello =
        receive_udp_bootstrap_message(socket, config, first_server_message_kind(canonical_profile))
            .await?;
    let progress = bootstrap_state.handle_server_hello(server_hello, unix_time_secs())?;
    if let Some(outbound) = progress.outbound {
        send_udp_bootstrap_message(socket, config.stream_id, &outbound, config).await?;
        let server_finish =
            receive_udp_bootstrap_message(socket, config, BootstrapMessageKind::ServerFinish)
                .await?;
        Ok(bootstrap_state.handle_server_finish(server_finish)?)
    } else {
        progress.completed.ok_or_else(|| {
            ClientConnectError::Bootstrap(shared_protocol::BootstrapError::UnexpectedMessageKind {
                expected: first_server_message_kind(canonical_profile),
                actual: first_server_message_kind(canonical_profile),
            })
        })
    }
}

async fn send_udp_bootstrap_message(
    socket: &UdpSocket,
    stream_id: shared_protocol::StreamId,
    message: &BootstrapMessage,
    config: &ClientConnectConfig,
) -> Result<(), ClientConnectError> {
    let packets = fragment_transport_frame(
        stream_id,
        0,
        &message.to_frame(&config.session.bootstrap)?,
        config.session.datagram.reliability,
    )
    .map_err(DatagramSessionError::from)
    .map_err(ClientConnectError::from)?;
    for packet in packets {
        let encoded = packet
            .to_bytes()
            .map_err(DatagramSessionError::from)
            .map_err(ClientConnectError::from)?;
        socket.send(&encoded).await?;
    }
    Ok(())
}

async fn receive_udp_bootstrap_message(
    socket: &UdpSocket,
    config: &ClientConnectConfig,
    expected_kind: BootstrapMessageKind,
) -> Result<BootstrapMessage, ClientConnectError> {
    let deadline =
        Instant::now() + Duration::from_millis(config.session.bootstrap.handshake_timeout_ms);
    let mut buffer = vec![0_u8; config.session.bootstrap.max_bootstrap_frame_bytes.max(2048)];
    let mut reassembly = DatagramReassemblyBuffer::default();
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ClientConnectError::HandshakeTimeout(
                config.session.bootstrap.handshake_timeout_ms,
            ));
        }
        let remaining = deadline.duration_since(now);
        let bytes_read = timeout(remaining, socket.recv(&mut buffer))
            .await
            .map_err(|_| {
                ClientConnectError::HandshakeTimeout(config.session.bootstrap.handshake_timeout_ms)
            })??;
        let packet = DatagramPacket::from_bytes(&buffer[..bytes_read])
            .map_err(DatagramSessionError::from)
            .map_err(ClientConnectError::from)?;
        match packet {
            DatagramPacket::Bootstrap { frame, .. } => {
                let message = BootstrapMessage::from_frame(&frame, &config.session.bootstrap)?;
                if message.kind() == expected_kind {
                    return Ok(message);
                }
            }
            DatagramPacket::Data { header, payload } => {
                reassembly
                    .insert_data(header, payload)
                    .map_err(DatagramSessionError::from)
                    .map_err(ClientConnectError::from)?;
                if let Some((_message_id, frame)) = reassembly.pop_next_ready() {
                    let message = BootstrapMessage::from_frame(&frame, &config.session.bootstrap)?;
                    if message.kind() == expected_kind {
                        return Ok(message);
                    }
                }
            }
            _ => continue,
        }
    }
}

async fn receive_bootstrap_message<C>(
    carrier: &mut C,
    config: &ClientConnectConfig,
    expected_kind: BootstrapMessageKind,
) -> Result<BootstrapMessage, ClientConnectError>
where
    C: ReliableCarrier<Error = ClientConnectError>,
{
    timeout(
        Duration::from_millis(config.session.bootstrap.handshake_timeout_ms),
        async {
            match carrier
                .recv_frame(config.session.bootstrap.max_bootstrap_frame_bytes)
                .await?
            {
                Some(frame) => BootstrapMessage::from_frame(&frame, &config.session.bootstrap)
                    .map_err(ClientConnectError::Bootstrap),
                None => Err(ClientConnectError::Bootstrap(
                    shared_protocol::BootstrapError::UnexpectedMessageKind {
                        expected: expected_kind,
                        actual: expected_kind,
                    },
                )),
            }
        },
    )
    .await
    .map_err(|_| {
        ClientConnectError::HandshakeTimeout(config.session.bootstrap.handshake_timeout_ms)
    })?
}

fn normalize_endpoint<'a>(value: &'a str, scheme_prefix: &str) -> &'a str {
    value.strip_prefix(scheme_prefix).unwrap_or(value)
}

async fn resolve_socket_addr(value: &str) -> Result<SocketAddr, ClientConnectError> {
    lookup_host(value)
        .await?
        .next()
        .ok_or_else(|| ClientConnectError::Quic(format!("unable to resolve {value}")))
}

fn quic_server_name(value: &str) -> String {
    let trimmed = normalize_endpoint(value, "quic_datagram://");
    let without_path = trimmed.split('/').next().unwrap_or(trimmed);
    let host = without_path
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(without_path);
    if host.is_empty() {
        "localhost".to_string()
    } else {
        host.to_string()
    }
}

fn webtransport_url(value: &str) -> Result<Url, ClientConnectError> {
    let normalized = if value.starts_with("webtransport://") {
        format!("https://{}", normalize_endpoint(value, "webtransport://"))
    } else {
        value.to_string()
    };
    Url::parse(&normalized).map_err(|error| ClientConnectError::WebTransport(error.to_string()))
}

fn quic_client_config(
    max_transport_frame_bytes: usize,
) -> Result<quinn::ClientConfig, ClientConnectError> {
    crate::make_benchmark_only_insecure_quic_client_config(max_transport_frame_bytes)
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
    type Error = ClientConnectError;

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
impl DatagramCarrier for UdpClientCarrier {
    type Error = ClientConnectError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: 65_507,
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.socket.send(datagram).await?;
        Ok(())
    }

    async fn recv_datagram(
        &mut self,
        max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut buffer = vec![0_u8; max_datagram_len.max(2048)];
        let bytes_read = self.socket.recv(&mut buffer).await?;
        buffer.truncate(bytes_read);
        Ok(Some(buffer))
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
impl DatagramCarrier for QuicDatagramClientCarrier {
    type Error = ClientConnectError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: self.connection.max_datagram_size().unwrap_or(2048),
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.connection
            .send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|error| ClientConnectError::Quic(error.to_string()))
    }

    async fn recv_datagram(
        &mut self,
        _max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        self.connection
            .read_datagram()
            .await
            .map(|bytes| Some(bytes.to_vec()))
            .map_err(|error| ClientConnectError::Quic(error.to_string()))
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.connection
            .close(0_u32.into(), b"pulzz/datagram_close");
        Ok(())
    }
}

#[async_trait]
impl DatagramCarrier for WebTransportDatagramClientCarrier {
    type Error = ClientConnectError;

    fn limits(&self) -> DatagramCarrierLimits {
        DatagramCarrierLimits {
            max_datagram_size: self.session.max_datagram_size().max(2048),
        }
    }

    async fn send_datagram(&mut self, datagram: &[u8]) -> Result<(), Self::Error> {
        self.session
            .send_datagram(Bytes::copy_from_slice(datagram))
            .map_err(|error| ClientConnectError::WebTransport(error.to_string()))
    }

    async fn recv_datagram(
        &mut self,
        _max_datagram_len: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        self.session
            .read_datagram()
            .await
            .map(|bytes| Some(bytes.to_vec()))
            .map_err(|error| ClientConnectError::WebTransport(error.to_string()))
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.session.close(0, b"pulzz/datagram_close");
        Ok(())
    }
}
