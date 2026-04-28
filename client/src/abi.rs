use crate::{
    ClientConnectConfig, ClientConnectError, ClientSession, ConnectedQuicDatagramSession,
    ConnectedQuicSession, ConnectedTcpSession, ConnectedUdpSession, ConnectedWebSocketSession,
    ConnectedWebTransportDatagramSession, connect_quic_datagram_session, connect_quic_session,
    connect_tcp_session, connect_udp_session, connect_websocket_session,
    connect_webtransport_datagram_session,
};
use shared_protocol::{Record, TransportConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeClientCarrierKind {
    WebSocket,
    Tcp,
    QuicStream,
    Udp,
    QuicDatagram,
    WebTransportDatagram,
}

#[derive(Debug, Clone)]
pub struct NativeClientAbiConfig {
    pub carrier: NativeClientCarrierKind,
    pub connect: ClientConnectConfig,
}

#[derive(Debug)]
pub enum NativeClientSession {
    WebSocket(ConnectedWebSocketSession),
    Tcp(ConnectedTcpSession),
    Quic(ConnectedQuicSession),
    Udp(ConnectedUdpSession),
    QuicDatagram(ConnectedQuicDatagramSession),
    WebTransportDatagram(ConnectedWebTransportDatagramSession),
}

impl NativeClientAbiConfig {
    pub fn new(carrier: NativeClientCarrierKind, connect: ClientConnectConfig) -> Self {
        Self { carrier, connect }
    }

    pub fn websocket(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::WebSocket, connect)
    }

    pub fn tcp(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::Tcp, connect)
    }

    pub fn quic(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::QuicStream, connect)
    }

    pub fn udp(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::Udp, connect)
    }

    pub fn quic_datagram(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::QuicDatagram, connect)
    }

    pub fn webtransport_datagram(connect: ClientConnectConfig) -> Self {
        Self::new(NativeClientCarrierKind::WebTransportDatagram, connect)
    }

    pub fn carrier(&self) -> NativeClientCarrierKind {
        self.carrier
    }
}

impl NativeClientSession {
    pub fn carrier(&self) -> NativeClientCarrierKind {
        match self {
            Self::WebSocket(_) => NativeClientCarrierKind::WebSocket,
            Self::Tcp(_) => NativeClientCarrierKind::Tcp,
            Self::Quic(_) => NativeClientCarrierKind::QuicStream,
            Self::Udp(_) => NativeClientCarrierKind::Udp,
            Self::QuicDatagram(_) => NativeClientCarrierKind::QuicDatagram,
            Self::WebTransportDatagram(_) => NativeClientCarrierKind::WebTransportDatagram,
        }
    }

    pub fn session(&self) -> &ClientSession {
        match self {
            Self::WebSocket(session) => session.session(),
            Self::Tcp(session) => session.session(),
            Self::Quic(session) => session.session(),
            Self::Udp(session) => session.session(),
            Self::QuicDatagram(session) => session.session(),
            Self::WebTransportDatagram(session) => session.session(),
        }
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        match self {
            Self::WebSocket(session) => session.session_mut(),
            Self::Tcp(session) => session.session_mut(),
            Self::Quic(session) => session.session_mut(),
            Self::Udp(session) => session.session_mut(),
            Self::QuicDatagram(session) => session.session_mut(),
            Self::WebTransportDatagram(session) => session.session_mut(),
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

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        match self {
            Self::WebSocket(session) => session.send_transport_frame(frame).await,
            Self::Tcp(session) => session.send_transport_frame(frame).await,
            Self::Quic(session) => session.send_transport_frame(frame).await,
            Self::Udp(session) => session.send_transport_frame(frame).await,
            Self::QuicDatagram(session) => session.send_transport_frame(frame).await,
            Self::WebTransportDatagram(session) => session.send_transport_frame(frame).await,
        }
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
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

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        let records: Vec<Record> = records.into_iter().collect();
        match self {
            Self::WebSocket(session) => session.send_plain_records_packed(records).await,
            Self::Tcp(session) => session.send_plain_records_packed(records).await,
            Self::Quic(session) => session.send_plain_records_packed(records).await,
            Self::Udp(session) => session.send_plain_records_packed(records).await,
            Self::QuicDatagram(session) => session.send_plain_records_packed(records).await,
            Self::WebTransportDatagram(session) => session.send_plain_records_packed(records).await,
        }
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        match self {
            Self::WebSocket(session) => session.receive_until_close().await,
            Self::Tcp(session) => session.receive_until_close().await,
            Self::Quic(session) => session.receive_until_close().await,
            Self::Udp(session) => session.receive_until_close().await,
            Self::QuicDatagram(session) => session.receive_until_close().await,
            Self::WebTransportDatagram(session) => session.receive_until_close().await,
        }
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        match self {
            Self::WebSocket(session) => session.read_transport_frame().await,
            Self::Tcp(session) => session.read_transport_frame().await,
            Self::Quic(session) => session.read_transport_frame().await,
            Self::Udp(session) => session.read_transport_frame().await,
            Self::QuicDatagram(session) => session.read_transport_frame().await,
            Self::WebTransportDatagram(session) => session.read_transport_frame().await,
        }
    }

    pub async fn close(self) -> Result<(), ClientConnectError> {
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

pub async fn connect_native_session(
    config: &NativeClientAbiConfig,
) -> Result<NativeClientSession, ClientConnectError> {
    match config.carrier {
        NativeClientCarrierKind::WebSocket => connect_websocket_session(&config.connect)
            .await
            .map(NativeClientSession::WebSocket),
        NativeClientCarrierKind::Tcp => connect_tcp_session(&config.connect)
            .await
            .map(NativeClientSession::Tcp),
        NativeClientCarrierKind::QuicStream => connect_quic_session(&config.connect)
            .await
            .map(NativeClientSession::Quic),
        NativeClientCarrierKind::Udp => connect_udp_session(&config.connect)
            .await
            .map(NativeClientSession::Udp),
        NativeClientCarrierKind::QuicDatagram => connect_quic_datagram_session(&config.connect)
            .await
            .map(NativeClientSession::QuicDatagram),
        NativeClientCarrierKind::WebTransportDatagram => {
            connect_webtransport_datagram_session(&config.connect)
                .await
                .map(NativeClientSession::WebTransportDatagram)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientSecurityConfig, ReconnectPolicy};
    use shared_protocol::{
        ProtectionProfileKind, StreamDirection, StreamId, TransportSessionConfig,
    };

    fn sample_connect_config() -> ClientConnectConfig {
        ClientConnectConfig {
            url: "tcp://127.0.0.1:9000".to_string(),
            stream_id: StreamId(7),
            direction: StreamDirection::ServerToClient,
            session: TransportSessionConfig {
                protection_profile: ProtectionProfileKind::PqSimpleV1,
                ..TransportSessionConfig::default()
            },
            security: ClientSecurityConfig::PqSimple,
            reconnect_policy: ReconnectPolicy::disabled(),
        }
    }

    #[test]
    fn native_client_abi_constructors_select_expected_carriers() {
        let base = sample_connect_config();
        assert_eq!(
            NativeClientAbiConfig::websocket(base.clone()).carrier(),
            NativeClientCarrierKind::WebSocket
        );
        assert_eq!(
            NativeClientAbiConfig::tcp(base.clone()).carrier(),
            NativeClientCarrierKind::Tcp
        );
        assert_eq!(
            NativeClientAbiConfig::quic(base).carrier(),
            NativeClientCarrierKind::QuicStream
        );
        let base = sample_connect_config();
        assert_eq!(
            NativeClientAbiConfig::udp(base.clone()).carrier(),
            NativeClientCarrierKind::Udp
        );
        assert_eq!(
            NativeClientAbiConfig::quic_datagram(base.clone()).carrier(),
            NativeClientCarrierKind::QuicDatagram
        );
        assert_eq!(
            NativeClientAbiConfig::webtransport_datagram(base).carrier(),
            NativeClientCarrierKind::WebTransportDatagram
        );
    }
}
