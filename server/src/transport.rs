use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use quinn::{
    Connection as QuicConnection, Endpoint as QuicEndpoint, IdleTimeout as QuicIdleTimeout,
    ReadError as QuicReadError, ReadExactError as QuicReadExactError, RecvStream, SendStream,
    TransportConfig as QuicTransportConfig, VarInt as QuicVarInt,
};
use rand::{RngCore, rngs::OsRng};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use shared_protocol::{
    BootstrapMessage, BootstrapServerConfig, ProtectionProfileKind, Record, ReplayCache,
    ServerBootstrapState, StreamProtector, TransportConfig, TransportSessionConfig, WireError,
    carrier::reliable::{
        ReliableCarrier, ReliableCarrierKind, read_length_prefixed_frame,
        write_length_prefixed_frame,
    },
    decode_transport_records, pack_record_groups, pack_records,
};
use thiserror::Error;
use tokio::time::{Duration, timeout};
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{
    accept_async_with_config, connect_async_with_config,
    tungstenite::{self, Message, protocol::WebSocketConfig},
};

pub use shared_protocol::{
    TransportConfig as ServerTransportConfig, TransportMode as ServerTransportMode,
    carrier::reliable::ReliableCarrierKind as ServerCarrierKind,
};

#[derive(Debug, Clone, Copy)]
pub struct ConnectionLimits {
    pub handshake_timeout_ms: u64,
    pub idle_read_timeout_ms: u64,
    pub max_bootstrap_frame_bytes: usize,
    pub max_transport_frame_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct BootstrapPolicy {
    pub server: BootstrapServerConfig,
    replay_cache: Arc<Mutex<ReplayCache>>,
}

#[derive(Debug, Clone)]
pub struct TransportServerConfig {
    pub session: TransportSessionConfig,
    pub connection_limits: ConnectionLimits,
    pub bootstrap_policy: BootstrapPolicy,
    pub carrier: ReliableCarrierKind,
}

#[derive(Debug)]
pub struct AuthenticatedServerConnection {
    websocket: tokio_tungstenite::WebSocketStream<TcpStream>,
    protector: StreamProtector,
    transport_config: TransportConfig,
}

const WEBSOCKET_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(WEBSOCKET_MAX_MESSAGE_BYTES);
    config.max_frame_size = Some(WEBSOCKET_MAX_MESSAGE_BYTES);
    config
}

#[derive(Debug)]
pub struct AuthenticatedTcpSession {
    stream_reader: ReadHalf<TcpStream>,
    stream_writer: WriteHalf<TcpStream>,
    protector: StreamProtector,
    transport_config: TransportConfig,
    max_transport_frame_bytes: usize,
}

#[derive(Debug)]
pub struct AuthenticatedQuicSession {
    _endpoint: QuicEndpoint,
    _connection: QuicConnection,
    send: SendStream,
    recv: RecvStream,
    protector: StreamProtector,
    transport_config: TransportConfig,
    max_transport_frame_bytes: usize,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Protection(#[from] shared_protocol::ProtectionError),
    #[error(transparent)]
    Bootstrap(#[from] shared_protocol::BootstrapError),
    #[error(transparent)]
    Datagram(#[from] shared_protocol::DatagramSessionError),
    #[error("bootstrap timed out after {0} ms")]
    HandshakeTimeout(u64),
    #[error("security profile mismatch: session={session:?}, bootstrap={bootstrap:?}")]
    SecurityProfileMismatch {
        session: ProtectionProfileKind,
        bootstrap: ProtectionProfileKind,
    },
    #[error("replay cache mutex was poisoned")]
    ReplayCachePoisoned,
    #[error("quic error: {0}")]
    Quic(String),
    #[error("webtransport error: {0}")]
    WebTransport(String),
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        let session = TransportSessionConfig::default();
        Self {
            handshake_timeout_ms: session.runtime_limits.handshake_timeout_ms,
            idle_read_timeout_ms: session.runtime_limits.idle_read_timeout_ms,
            max_bootstrap_frame_bytes: session.runtime_limits.max_bootstrap_frame_bytes,
            max_transport_frame_bytes: session.runtime_limits.max_transport_frame_bytes,
        }
    }
}

impl BootstrapPolicy {
    pub fn new(server: BootstrapServerConfig) -> Self {
        Self {
            server,
            replay_cache: Arc::new(Mutex::new(ReplayCache::default())),
        }
    }

    pub fn with_replay_cache(server: BootstrapServerConfig, replay_cache: ReplayCache) -> Self {
        Self {
            server,
            replay_cache: Arc::new(Mutex::new(replay_cache)),
        }
    }

    pub(crate) fn replay_cache(&self) -> &Arc<Mutex<ReplayCache>> {
        &self.replay_cache
    }
}

impl AuthenticatedServerConnection {
    pub fn protector(&self) -> &StreamProtector {
        &self.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.protector
    }

    pub fn websocket_mut(
        &mut self,
    ) -> &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
        &mut self.websocket
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.websocket.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            self.websocket
                .send(Message::Binary(frame.as_ref().to_vec().into()))
                .await?;
        }
        Ok(())
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = pack_record_groups(records, self.transport_config)
            .into_iter()
            .map(|group| self.protector.protect_transport_records(group))
            .collect::<Result<Vec<_>, _>>()?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
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

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        loop {
            match self.websocket.next().await {
                Some(Ok(Message::Binary(frame))) => return Ok(Some(frame.to_vec())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(TransportError::WebSocket(error)),
                None => return Ok(None),
            }
        }
    }

    pub async fn close(mut self) -> Result<(), TransportError> {
        self.websocket.close(None).await?;
        Ok(())
    }
}

impl AuthenticatedTcpSession {
    pub fn protector(&self) -> &StreamProtector {
        &self.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.protector
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        write_length_prefixed_frame(&mut self.stream_writer, &frame).await?;
        Ok(())
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            write_length_prefixed_frame(&mut self.stream_writer, frame.as_ref()).await?;
        }
        Ok(())
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = pack_record_groups(records, self.transport_config)
            .into_iter()
            .map(|group| self.protector.protect_transport_records(group))
            .collect::<Result<Vec<_>, _>>()?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
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

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        Ok(
            read_length_prefixed_frame(&mut self.stream_reader, self.max_transport_frame_bytes)
                .await?,
        )
    }

    pub async fn close(mut self) -> Result<(), TransportError> {
        self.stream_writer.shutdown().await?;
        Ok(())
    }
}

impl AuthenticatedQuicSession {
    pub fn protector(&self) -> &StreamProtector {
        &self.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.protector
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        write_length_prefixed_frame(&mut self.send, &frame)
            .await
            .map_err(|error| TransportError::Quic(error.to_string()))
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            write_length_prefixed_frame(&mut self.send, frame.as_ref())
                .await
                .map_err(|error| TransportError::Quic(error.to_string()))?;
        }
        Ok(())
    }

    pub async fn send_plain_records<I>(&mut self, records: I) -> Result<(), TransportError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = pack_record_groups(records, self.transport_config)
            .into_iter()
            .map(|group| self.protector.protect_transport_records(group))
            .collect::<Result<Vec<_>, _>>()?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_records_until_close(&mut self) -> Result<Vec<Record>, TransportError> {
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

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        read_quic_length_prefixed_frame(&mut self.recv, self.max_transport_frame_bytes)
            .await
            .map_err(TransportError::Quic)
    }

    pub async fn close(mut self) -> Result<(), TransportError> {
        self.send
            .finish()
            .map_err(|error| TransportError::Quic(error.to_string()))?;
        let _ = self.send.stopped().await;
        Ok(())
    }
}

pub async fn serve_binary_frames_once(
    listener: TcpListener,
    frames: Vec<Vec<u8>>,
) -> Result<(), TransportError> {
    serve_binary_frames(listener, frames, 1).await
}

pub async fn serve_binary_frames(
    listener: TcpListener,
    frames: Vec<Vec<u8>>,
    connections: usize,
) -> Result<(), TransportError> {
    for _ in 0..connections {
        let (stream, _) = listener.accept().await?;
        let mut websocket = accept_async_with_config(stream, Some(websocket_config())).await?;
        for frame in &frames {
            websocket
                .send(Message::Binary(frame.clone().into()))
                .await?;
        }
        websocket.close(None).await?;
    }
    Ok(())
}

pub async fn serve_binary_frames_at(
    addr: &str,
    frames: Vec<Vec<u8>>,
    connections: usize,
) -> Result<(), TransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_binary_frames(listener, frames, connections).await
}

pub async fn serve_records(
    listener: TcpListener,
    records: Vec<Record>,
    connections: usize,
) -> Result<(), TransportError> {
    serve_records_with_config(listener, records, connections, TransportConfig::default()).await
}

pub async fn serve_records_with_config(
    listener: TcpListener,
    records: Vec<Record>,
    connections: usize,
    config: TransportConfig,
) -> Result<(), TransportError> {
    let frames = pack_records(records, config);
    serve_binary_frames(listener, frames, connections).await
}

pub async fn serve_records_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
) -> Result<(), TransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_records(listener, records, connections).await
}

pub async fn serve_records_at_with_config(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportConfig,
) -> Result<(), TransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_records_with_config(listener, records, connections, config).await
}

pub async fn serve_records_once(
    listener: TcpListener,
    records: Vec<Record>,
) -> Result<(), TransportError> {
    serve_records(listener, records, 1).await
}

pub async fn serve_records_once_with_config(
    listener: TcpListener,
    records: Vec<Record>,
    config: TransportConfig,
) -> Result<(), TransportError> {
    serve_records_with_config(listener, records, 1, config).await
}

pub async fn read_binary_frames_once(url: &str) -> Result<Vec<Vec<u8>>, TransportError> {
    let (mut websocket, _) =
        connect_async_with_config(url, Some(websocket_config()), false).await?;
    let mut frames = Vec::new();
    while let Some(message) = websocket.next().await {
        match message? {
            Message::Binary(frame) => frames.push(frame.to_vec()),
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
        }
    }
    Ok(frames)
}

pub async fn read_records_once(url: &str) -> Result<Vec<Record>, TransportError> {
    let frames = read_binary_frames_once(url).await?;
    let mut records = Vec::new();
    for frame in frames {
        records.extend(decode_transport_records(&frame)?);
    }
    Ok(records)
}

pub async fn accept_authenticated_session(
    listener: &TcpListener,
    config: &TransportServerConfig,
) -> Result<AuthenticatedServerConnection, TransportError> {
    let mut transient_failures = 0_usize;
    let websocket = loop {
        let (stream, _) = listener.accept().await?;
        match accept_async_with_config(stream, Some(websocket_config())).await {
            Ok(websocket) => break websocket,
            Err(error)
                if matches!(
                    error,
                    tungstenite::Error::Protocol(_) | tungstenite::Error::ConnectionClosed
                ) =>
            {
                transient_failures += 1;
                if transient_failures > 8 {
                    return Err(error.into());
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    let mut carrier = WebSocketServerCarrier { websocket };
    let protector = accept_authenticated_over_carrier(&mut carrier, config).await?;
    Ok(AuthenticatedServerConnection {
        websocket: carrier.websocket,
        protector,
        transport_config: config.session.transport,
    })
}

pub async fn accept_tcp_session(
    listener: &TcpListener,
    config: &TransportServerConfig,
) -> Result<AuthenticatedTcpSession, TransportError> {
    let (stream, _) = listener.accept().await?;
    let (stream_reader, stream_writer) = tokio::io::split(stream);
    let mut carrier = TcpServerCarrier {
        stream_reader,
        stream_writer,
    };
    let protector = accept_authenticated_over_carrier(&mut carrier, config).await?;
    Ok(AuthenticatedTcpSession {
        stream_reader: carrier.stream_reader,
        stream_writer: carrier.stream_writer,
        protector,
        transport_config: config.session.transport,
        max_transport_frame_bytes: config.connection_limits.max_transport_frame_bytes,
    })
}

pub async fn accept_quic_session(
    endpoint: &QuicEndpoint,
    config: &TransportServerConfig,
) -> Result<AuthenticatedQuicSession, TransportError> {
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
    let mut carrier = QuicServerCarrier {
        endpoint: endpoint.clone(),
        connection,
        send,
        recv,
    };
    let protector = accept_authenticated_over_carrier(&mut carrier, config).await?;
    Ok(AuthenticatedQuicSession {
        _endpoint: carrier.endpoint,
        _connection: carrier.connection,
        send: carrier.send,
        recv: carrier.recv,
        protector,
        transport_config: config.session.transport,
        max_transport_frame_bytes: config.connection_limits.max_transport_frame_bytes,
    })
}

pub async fn serve_websocket_session(
    listener: TcpListener,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    for _ in 0..connections {
        let mut connection = accept_authenticated_session(&listener, &config).await?;
        connection.send_plain_records(records.clone()).await?;
        connection.close().await?;
    }
    Ok(())
}

pub async fn serve_websocket_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_websocket_session(listener, records, connections, config).await
}

pub async fn serve_tcp_session(
    listener: TcpListener,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    for _ in 0..connections {
        let mut connection = accept_tcp_session(&listener, &config).await?;
        connection.send_plain_records(records.clone()).await?;
        connection.close().await?;
    }
    Ok(())
}

pub async fn serve_tcp_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let listener = TcpListener::bind(addr).await?;
    serve_tcp_session(listener, records, connections, config).await
}

pub async fn serve_quic_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    let endpoint = bind_quic_endpoint(addr, config.connection_limits.max_transport_frame_bytes)?;
    for _ in 0..connections {
        let mut connection = accept_quic_session(&endpoint, &config).await?;
        connection.send_plain_records(records.clone()).await?;
        connection.close().await?;
    }
    Ok(())
}

pub async fn serve_session(
    listener: TcpListener,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    match config.carrier {
        ReliableCarrierKind::WebSocket => {
            serve_websocket_session(listener, records, connections, config).await
        }
        ReliableCarrierKind::Tcp => serve_tcp_session(listener, records, connections, config).await,
        ReliableCarrierKind::QuicStream => Err(TransportError::Quic(
            "serve_session cannot dispatch quic_stream from a TcpListener; use serve_quic_session_at"
                .to_string(),
        )),
    }
}

pub async fn serve_session_at(
    addr: &str,
    records: Vec<Record>,
    connections: usize,
    config: TransportServerConfig,
) -> Result<(), TransportError> {
    match config.carrier {
        ReliableCarrierKind::WebSocket => {
            serve_websocket_session_at(addr, records, connections, config).await
        }
        ReliableCarrierKind::Tcp => serve_tcp_session_at(addr, records, connections, config).await,
        ReliableCarrierKind::QuicStream => {
            serve_quic_session_at(addr, records, connections, config).await
        }
    }
}

#[async_trait]
impl ReliableCarrier for WebSocketServerCarrier {
    type Error = TransportError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        self.websocket
            .send(Message::Binary(frame.to_vec().into()))
            .await?;
        Ok(())
    }

    async fn recv_frame(&mut self, _max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.websocket.next().await {
                Some(Ok(Message::Binary(frame))) => return Ok(Some(frame.to_vec())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(TransportError::WebSocket(error)),
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.websocket.close(None).await?;
        Ok(())
    }
}

#[async_trait]
impl ReliableCarrier for TcpServerCarrier {
    type Error = TransportError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        write_length_prefixed_frame(&mut self.stream_writer, frame).await?;
        Ok(())
    }

    async fn recv_frame(&mut self, max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(read_length_prefixed_frame(&mut self.stream_reader, max_frame_len).await?)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.stream_writer.shutdown().await?;
        Ok(())
    }
}

#[async_trait]
impl ReliableCarrier for QuicServerCarrier {
    type Error = TransportError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        write_length_prefixed_frame(&mut self.send, frame)
            .await
            .map_err(|error| TransportError::Quic(error.to_string()))
    }

    async fn recv_frame(&mut self, max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        read_quic_length_prefixed_frame(&mut self.recv, max_frame_len)
            .await
            .map_err(TransportError::Quic)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.send
            .finish()
            .map_err(|error| TransportError::Quic(error.to_string()))?;
        let _ = self.send.stopped().await;
        Ok(())
    }
}

struct WebSocketServerCarrier {
    websocket: tokio_tungstenite::WebSocketStream<TcpStream>,
}

struct TcpServerCarrier {
    stream_reader: ReadHalf<TcpStream>,
    stream_writer: WriteHalf<TcpStream>,
}

struct QuicServerCarrier {
    endpoint: QuicEndpoint,
    connection: QuicConnection,
    send: SendStream,
    recv: RecvStream,
}

async fn accept_authenticated_over_carrier<C>(
    carrier: &mut C,
    config: &TransportServerConfig,
) -> Result<StreamProtector, TransportError>
where
    C: ReliableCarrier<Error = TransportError>,
{
    let bootstrap_profile = config.bootstrap_policy.server.protection_profile();
    if config.session.protection_profile != bootstrap_profile {
        return Err(TransportError::SecurityProfileMismatch {
            session: config.session.protection_profile,
            bootstrap: bootstrap_profile,
        });
    }

    let client_hello = receive_bootstrap_message(
        carrier,
        config.connection_limits.handshake_timeout_ms,
        config.session.bootstrap,
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
            .replay_cache
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
    send_bootstrap_message(carrier, &response.outbound, config.session.bootstrap).await?;
    let completed = if let Some(completed) = response.completed {
        completed
    } else {
        let client_finish = receive_bootstrap_message(
            carrier,
            config.connection_limits.handshake_timeout_ms,
            config.session.bootstrap,
            shared_protocol::BootstrapMessageKind::ClientFinish,
        )
        .await?;
        let state = response
            .state
            .ok_or_else(|| TransportError::Quic("missing server bootstrap state".to_string()))?;
        let (completed, server_finish) = state.handle_client_finish(client_finish)?;
        send_bootstrap_message(carrier, &server_finish, config.session.bootstrap).await?;
        completed
    };
    Ok(StreamProtector::from_bootstrap_root(
        completed.protection_profile,
        completed.stream_id,
        completed.direction,
        completed.root,
    ))
}

async fn send_bootstrap_message<C>(
    carrier: &mut C,
    message: &BootstrapMessage,
    config: shared_protocol::BootstrapConfig,
) -> Result<(), TransportError>
where
    C: ReliableCarrier<Error = TransportError>,
{
    carrier.send_frame(&message.to_frame(&config)?).await?;
    Ok(())
}

async fn receive_bootstrap_message<C>(
    carrier: &mut C,
    handshake_timeout_ms: u64,
    config: shared_protocol::BootstrapConfig,
    expected_kind: shared_protocol::BootstrapMessageKind,
) -> Result<BootstrapMessage, TransportError>
where
    C: ReliableCarrier<Error = TransportError>,
{
    timeout(Duration::from_millis(handshake_timeout_ms), async {
        match carrier.recv_frame(config.max_bootstrap_frame_bytes).await? {
            Some(frame) => {
                BootstrapMessage::from_frame(&frame, &config).map_err(TransportError::Bootstrap)
            }
            None => Err(TransportError::Bootstrap(
                shared_protocol::BootstrapError::UnexpectedMessageKind {
                    expected: expected_kind,
                    actual: expected_kind,
                },
            )),
        }
    })
    .await
    .map_err(|_| TransportError::HandshakeTimeout(handshake_timeout_ms))?
}

fn first_client_message_kind(
    protection_profile: ProtectionProfileKind,
) -> shared_protocol::BootstrapMessageKind {
    match protection_profile.canonical_stream_profile() {
        ProtectionProfileKind::PqMutualV1 => shared_protocol::BootstrapMessageKind::ClientHello,
        ProtectionProfileKind::PqSimpleV1 => {
            shared_protocol::BootstrapMessageKind::SimpleClientHello
        }
        ProtectionProfileKind::ClassicRef1 => shared_protocol::BootstrapMessageKind::ClientHello,
        ProtectionProfileKind::PqSimpleDgramV1 | ProtectionProfileKind::PqMutualDgramV1 => {
            unreachable!("canonical_stream_profile removes datagram variants")
        }
    }
}

fn quic_certificate_names_for_bind_addr(bind_addr: SocketAddr) -> Vec<String> {
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

pub fn bind_quic_endpoint(
    addr: &str,
    max_transport_frame_bytes: usize,
) -> Result<QuicEndpoint, TransportError> {
    ensure_rustls_crypto_provider()?;
    let socket_addr: SocketAddr = addr
        .parse()
        .map_err(|error| TransportError::Quic(format!("invalid QUIC bind addr: {error}")))?;
    let certificate =
        generate_simple_self_signed(quic_certificate_names_for_bind_addr(socket_addr))
            .map_err(|error| TransportError::Quic(error.to_string()))?;
    let cert_der: CertificateDer<'static> = certificate.cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certificate.signing_key.serialize_der(),
    ));
    let mut server_config = quinn::ServerConfig::with_single_cert(vec![cert_der], key_der)
        .map_err(|error| TransportError::Quic(error.to_string()))?;
    server_config.transport_config(Arc::new(build_quic_transport_config(
        max_transport_frame_bytes,
    )?));
    QuicEndpoint::server(server_config, socket_addr)
        .map_err(|error| TransportError::Quic(error.to_string()))
}

pub(crate) fn build_quic_transport_config(
    max_transport_frame_bytes: usize,
) -> Result<QuicTransportConfig, TransportError> {
    let stream_window = max_transport_frame_bytes
        .saturating_mul(4)
        .max(8 * 1024 * 1024);
    let connection_window = stream_window.saturating_mul(4);
    let datagram_window = max_transport_frame_bytes.saturating_mul(4).max(64 * 1024);
    let mut transport = QuicTransportConfig::default();
    transport
        .max_idle_timeout(Some(
            QuicIdleTimeout::try_from(Duration::from_secs(120))
                .map_err(|error| TransportError::Quic(error.to_string()))?,
        ))
        .keep_alive_interval(Some(Duration::from_secs(5)))
        .stream_receive_window(
            QuicVarInt::try_from(stream_window)
                .map_err(|error| TransportError::Quic(error.to_string()))?,
        )
        .receive_window(
            QuicVarInt::try_from(connection_window)
                .map_err(|error| TransportError::Quic(error.to_string()))?,
        )
        .send_window(connection_window as u64);
    transport
        .datagram_receive_buffer_size(Some(datagram_window))
        .datagram_send_buffer_size(datagram_window);
    Ok(transport)
}

pub(crate) fn ensure_rustls_crypto_provider() -> Result<(), TransportError> {
    static PROVIDER: OnceLock<Result<(), String>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                rustls::crypto::aws_lc_rs::default_provider()
                    .install_default()
                    .map_err(|error| format!("{error:?}"))?;
            }
            Ok(())
        })
        .as_ref()
        .map_err(|error| TransportError::Quic(error.clone()))?;
    Ok(())
}

fn invalid_quic_frame_length(frame_len: usize, max_frame_len: usize) -> String {
    if frame_len == 0 {
        "frame length 0 is invalid".to_string()
    } else {
        format!("frame length {frame_len} exceeds max {max_frame_len}")
    }
}

async fn read_quic_length_prefixed_frame(
    recv: &mut RecvStream,
    max_frame_len: usize,
) -> Result<Option<Vec<u8>>, String> {
    let mut len_bytes = [0_u8; 4];
    match recv.read_exact(&mut len_bytes).await {
        Ok(()) => {}
        Err(QuicReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(QuicReadExactError::ReadError(QuicReadError::ConnectionLost(_)))
        | Err(QuicReadExactError::ReadError(QuicReadError::ClosedStream)) => return Ok(None),
        Err(error) => return Err(format!("{error:?}")),
    }

    let frame_len = u32::from_le_bytes(len_bytes) as usize;
    if frame_len == 0 || frame_len > max_frame_len {
        return Err(invalid_quic_frame_length(frame_len, max_frame_len));
    }

    let mut frame = vec![0_u8; frame_len];
    recv.read_exact(&mut frame)
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(Some(frame))
}

fn unix_time_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use client::{
        ClientConnectConfig, ClientSecurityConfig, ReconnectPolicy, connect_quic_session,
    };
    use shared_protocol::{
        AUTH_TAG_LEN, BOOTSTRAP_SIGNING_SEED_LEN, BootstrapConfig, CodecMode, CredentialScope,
        EpochId, ItemId, PROTOCOL_VERSION, PqSimpleServerBootstrapConfig, ProtectionProfileKind,
        RecordFlags, RecordHeader, ReplayCache, ServerSecurityConfig, StreamDirection, StreamId,
        TransportMode, TransportSessionConfig, issue_client_credential, issue_server_identity,
    };

    use super::*;

    fn sample_record(item_id: u64, payload_len: usize) -> Record {
        Record {
            header: RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: shared_protocol::SeqNo(item_id),
                record_type: shared_protocol::RecordType::ExactState,
                codec_mode: CodecMode::DirectExact,
                flags: RecordFlags::empty(),
                item_id: ItemId(item_id),
                payload_len: payload_len as u32,
            },
            payload: vec![7; payload_len],
            auth_tag: [0; AUTH_TAG_LEN],
        }
    }

    #[tokio::test]
    async fn serve_records_with_transport_mode_round_trips() {
        let records = vec![
            sample_record(1, 1024),
            sample_record(2, 1024),
            sample_record(3, 1024),
            sample_record(4, 1024),
            sample_record(5, 1024),
        ];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sent = records.clone();
        let server_task = tokio::spawn(async move {
            serve_records_once_with_config(
                listener,
                sent,
                TransportConfig {
                    mode: TransportMode::BurstSmall,
                },
            )
            .await
        });
        let received = read_records_once(&format!("ws://{addr}")).await.unwrap();
        server_task.await.unwrap().unwrap();
        assert_eq!(received, records);
    }

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

    fn simple_server_config(stream_id: StreamId) -> TransportServerConfig {
        let bootstrap = BootstrapConfig::default();
        TransportServerConfig {
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleV1,
                bootstrap,
                ..TransportSessionConfig::default()
            },
            connection_limits: sample_connection_limits(),
            bootstrap_policy: BootstrapPolicy {
                server: shared_protocol::BootstrapServerConfig {
                    stream_id,
                    direction: StreamDirection::ServerToClient,
                    bootstrap,
                    security: ServerSecurityConfig::PqSimple {
                        bootstrap: PqSimpleServerBootstrapConfig {
                            server_id: "quic-simple-server".to_string(),
                        },
                    },
                },
                replay_cache: Arc::new(Mutex::new(ReplayCache::default())),
            },
            carrier: ReliableCarrierKind::QuicStream,
        }
    }

    fn mutual_server_config(stream_id: StreamId) -> (TransportServerConfig, ClientSecurityConfig) {
        let bootstrap = BootstrapConfig::default();
        let now_unix_secs = unix_time_secs();
        let server_identity = issue_server_identity(
            "quic-mutual-server",
            now_unix_secs.saturating_sub(60),
            now_unix_secs + 3_600,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
        )
        .unwrap();
        let issued_credential = issue_client_credential(
            &server_identity,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "quic-mutual-client",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(stream_id),
            now_unix_secs.saturating_sub(60),
            now_unix_secs + 900,
        )
        .unwrap();
        (
            TransportServerConfig {
                session: TransportSessionConfig {
                    protection_profile: ProtectionProfileKind::PqMutualV1,
                    bootstrap,
                    ..TransportSessionConfig::default()
                },
                connection_limits: sample_connection_limits(),
                bootstrap_policy: BootstrapPolicy {
                    server: shared_protocol::BootstrapServerConfig {
                        stream_id,
                        direction: StreamDirection::ServerToClient,
                        bootstrap,
                        security: ServerSecurityConfig::PqMutual {
                            server_identity: server_identity.clone(),
                            server_signing_seed: [7; BOOTSTRAP_SIGNING_SEED_LEN],
                            revoked_client_ids: Vec::new(),
                        },
                    },
                    replay_cache: Arc::new(Mutex::new(ReplayCache::default())),
                },
                carrier: ReliableCarrierKind::QuicStream,
            },
            ClientSecurityConfig::PqMutual {
                issued_credential,
                expected_server_identity: server_identity,
            },
        )
    }

    #[tokio::test]
    async fn quic_session_bootstrap_succeeds_for_pq_simple() {
        let stream_id = StreamId(77);
        let server_config = simple_server_config(stream_id);
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind_addr = socket.local_addr().unwrap();
        drop(socket);
        let endpoint = bind_quic_endpoint(
            &bind_addr.to_string(),
            server_config.connection_limits.max_transport_frame_bytes,
        )
        .unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = accept_quic_session(&endpoint, &server_config).await?;
            session.send_transport_frame(vec![1, 2, 3, 4]).await?;
            session.close().await
        });

        let client_config = ClientConnectConfig {
            url: format!("quic://{bind_addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleV1,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqSimple,
            reconnect_policy: ReconnectPolicy::disabled(),
        };

        let mut session = connect_quic_session(&client_config).await.unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![1, 2, 3, 4]);
        session.close().await.unwrap();
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn quic_session_bootstrap_succeeds_for_pq_mutual() {
        let stream_id = StreamId(78);
        let (server_config, client_security) = mutual_server_config(stream_id);
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind_addr = socket.local_addr().unwrap();
        drop(socket);
        let endpoint = bind_quic_endpoint(
            &bind_addr.to_string(),
            server_config.connection_limits.max_transport_frame_bytes,
        )
        .unwrap();
        let server_task = tokio::spawn(async move {
            let mut session = accept_quic_session(&endpoint, &server_config).await?;
            session.send_transport_frame(vec![5, 6, 7, 8]).await?;
            session.close().await
        });

        let client_config = ClientConnectConfig {
            url: format!("quic://{bind_addr}"),
            stream_id,
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqMutualV1,
                ..TransportSessionConfig::default()
            },
            security: client_security,
            reconnect_policy: ReconnectPolicy::disabled(),
        };

        let mut session = connect_quic_session(&client_config).await.unwrap();
        let frame = session.read_transport_frame().await.unwrap().unwrap();
        assert_eq!(frame, vec![5, 6, 7, 8]);
        session.close().await.unwrap();
        server_task.await.unwrap().unwrap();
    }
}

pub fn pack_transform_route_record(
    header: shared_protocol::RecordHeader,
    payload: &shared_protocol::TransformInstancePayload,
    config: TransportConfig,
) -> Result<Vec<Vec<u8>>, shared_protocol::StateProgramError> {
    let record = shared_protocol::encode_transform_instance_record(header, payload)?;
    Ok(pack_records([record], config))
}
