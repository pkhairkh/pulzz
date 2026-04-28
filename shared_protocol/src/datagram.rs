use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{StreamId, WireError};

pub const DATAGRAM_PROTOCOL_VERSION: u8 = 1;
pub const DEFAULT_DATAGRAM_TARGET_PAYLOAD_SIZE_BYTES: usize = 1200;
pub const DEFAULT_DATAGRAM_REPAIR_TIMEOUT_MS: u64 = 50;
pub const DEFAULT_DATAGRAM_MAX_IN_FLIGHT_MESSAGES: usize = 256;
pub const DEFAULT_DATAGRAM_MAX_CHUNKS_PER_MESSAGE: usize = 2048;
pub const DEFAULT_DATAGRAM_KEEPALIVE_INTERVAL_MS: u64 = 5_000;
pub const DEFAULT_DATAGRAM_MAX_REPAIR_ATTEMPTS: u32 = 8;
pub const DATAGRAM_HEADER_LEN: usize = 1 + 1 + 8 + 8 + 2 + 2 + 2;
pub const DATAGRAM_ACK_LEN: usize = 1 + 1 + 8 + 8;
pub const DATAGRAM_KEEPALIVE_LEN: usize = 1 + 1 + 8;
pub const DATAGRAM_CLOSE_LEN: usize = 1 + 1 + 8 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DatagramCarrierKind {
    Udp,
    QuicDatagram,
    #[default]
    WebTransportDatagram,
}

impl DatagramCarrierKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "udp" => Some(Self::Udp),
            "quic_datagram" | "quic_dgram" => Some(Self::QuicDatagram),
            "webtransport_datagram" | "webtransport_dgram" | "webtransport" => {
                Some(Self::WebTransportDatagram)
            }
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::QuicDatagram => "quic_datagram",
            Self::WebTransportDatagram => "webtransport_datagram",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramReliabilityConfig {
    pub target_datagram_payload_size_bytes: usize,
    pub repair_timeout_ms: u64,
    pub max_in_flight_messages: usize,
    pub max_chunks_per_message: usize,
    pub keepalive_interval_ms: u64,
    pub max_repair_attempts: u32,
}

impl Default for DatagramReliabilityConfig {
    fn default() -> Self {
        Self {
            target_datagram_payload_size_bytes: DEFAULT_DATAGRAM_TARGET_PAYLOAD_SIZE_BYTES,
            repair_timeout_ms: DEFAULT_DATAGRAM_REPAIR_TIMEOUT_MS,
            max_in_flight_messages: DEFAULT_DATAGRAM_MAX_IN_FLIGHT_MESSAGES,
            max_chunks_per_message: DEFAULT_DATAGRAM_MAX_CHUNKS_PER_MESSAGE,
            keepalive_interval_ms: DEFAULT_DATAGRAM_KEEPALIVE_INTERVAL_MS,
            max_repair_attempts: DEFAULT_DATAGRAM_MAX_REPAIR_ATTEMPTS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DatagramSessionConfig {
    pub reliability: DatagramReliabilityConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DatagramPacketKind {
    Bootstrap = 1,
    Data = 2,
    Ack = 3,
    RepairRequest = 4,
    Keepalive = 5,
    Close = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramDataHeader {
    pub stream_id: StreamId,
    pub message_id: u64,
    pub chunk_index: u16,
    pub chunk_count: u16,
    pub payload_len: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramAck {
    pub stream_id: StreamId,
    pub message_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramChunkRange {
    pub start_chunk_index: u16,
    pub end_chunk_index_inclusive: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramRepairRequest {
    pub stream_id: StreamId,
    pub message_id: u64,
    pub missing_ranges: Vec<DatagramChunkRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramKeepalive {
    pub stream_id: StreamId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatagramClose {
    pub stream_id: StreamId,
    pub code: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatagramPacket {
    Bootstrap {
        stream_id: StreamId,
        frame: Vec<u8>,
    },
    Data {
        header: DatagramDataHeader,
        payload: Vec<u8>,
    },
    Ack(DatagramAck),
    RepairRequest(DatagramRepairRequest),
    Keepalive(DatagramKeepalive),
    Close(DatagramClose),
}

impl DatagramPacket {
    pub fn kind(&self) -> DatagramPacketKind {
        match self {
            Self::Bootstrap { .. } => DatagramPacketKind::Bootstrap,
            Self::Data { .. } => DatagramPacketKind::Data,
            Self::Ack(_) => DatagramPacketKind::Ack,
            Self::RepairRequest(_) => DatagramPacketKind::RepairRequest,
            Self::Keepalive(_) => DatagramPacketKind::Keepalive,
            Self::Close(_) => DatagramPacketKind::Close,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        match self {
            Self::Bootstrap { stream_id, .. } => *stream_id,
            Self::Data { header, .. } => header.stream_id,
            Self::Ack(ack) => ack.stream_id,
            Self::RepairRequest(repair) => repair.stream_id,
            Self::Keepalive(keepalive) => keepalive.stream_id,
            Self::Close(close) => close.stream_id,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, DatagramWireError> {
        match self {
            Self::Bootstrap { stream_id, frame } => {
                let frame_len = u16::try_from(frame.len())
                    .map_err(|_| DatagramWireError::PayloadTooLarge(frame.len()))?;
                let mut out = Vec::with_capacity(DATAGRAM_HEADER_LEN + frame.len());
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::Bootstrap as u8);
                out.extend_from_slice(&stream_id.0.to_le_bytes());
                out.extend_from_slice(&0_u64.to_le_bytes());
                out.extend_from_slice(&0_u16.to_le_bytes());
                out.extend_from_slice(&1_u16.to_le_bytes());
                out.extend_from_slice(&frame_len.to_le_bytes());
                out.extend_from_slice(frame);
                Ok(out)
            }
            Self::Data { header, payload } => {
                if payload.len() != header.payload_len as usize {
                    return Err(DatagramWireError::PayloadLengthMismatch {
                        header_len: header.payload_len as usize,
                        actual_len: payload.len(),
                    });
                }
                let mut out = Vec::with_capacity(DATAGRAM_HEADER_LEN + payload.len());
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::Data as u8);
                out.extend_from_slice(&header.stream_id.0.to_le_bytes());
                out.extend_from_slice(&header.message_id.to_le_bytes());
                out.extend_from_slice(&header.chunk_index.to_le_bytes());
                out.extend_from_slice(&header.chunk_count.to_le_bytes());
                out.extend_from_slice(&header.payload_len.to_le_bytes());
                out.extend_from_slice(payload);
                Ok(out)
            }
            Self::Ack(ack) => {
                let mut out = Vec::with_capacity(DATAGRAM_ACK_LEN);
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::Ack as u8);
                out.extend_from_slice(&ack.stream_id.0.to_le_bytes());
                out.extend_from_slice(&ack.message_id.to_le_bytes());
                Ok(out)
            }
            Self::RepairRequest(repair) => {
                let range_count = u8::try_from(repair.missing_ranges.len()).map_err(|_| {
                    DatagramWireError::TooManyRepairRanges {
                        actual: repair.missing_ranges.len(),
                    }
                })?;
                let mut out =
                    Vec::with_capacity(1 + 1 + 8 + 8 + 1 + repair.missing_ranges.len() * 4);
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::RepairRequest as u8);
                out.extend_from_slice(&repair.stream_id.0.to_le_bytes());
                out.extend_from_slice(&repair.message_id.to_le_bytes());
                out.push(range_count);
                for range in &repair.missing_ranges {
                    out.extend_from_slice(&range.start_chunk_index.to_le_bytes());
                    out.extend_from_slice(&range.end_chunk_index_inclusive.to_le_bytes());
                }
                Ok(out)
            }
            Self::Keepalive(keepalive) => {
                let mut out = Vec::with_capacity(DATAGRAM_KEEPALIVE_LEN);
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::Keepalive as u8);
                out.extend_from_slice(&keepalive.stream_id.0.to_le_bytes());
                Ok(out)
            }
            Self::Close(close) => {
                let mut out = Vec::with_capacity(DATAGRAM_CLOSE_LEN);
                out.push(DATAGRAM_PROTOCOL_VERSION);
                out.push(DatagramPacketKind::Close as u8);
                out.extend_from_slice(&close.stream_id.0.to_le_bytes());
                out.extend_from_slice(&close.code.to_le_bytes());
                Ok(out)
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DatagramWireError> {
        if bytes.len() < 2 {
            return Err(DatagramWireError::TruncatedPacket {
                minimum: 2,
                actual: bytes.len(),
            });
        }
        if bytes[0] != DATAGRAM_PROTOCOL_VERSION {
            return Err(DatagramWireError::UnsupportedVersion(bytes[0]));
        }
        match bytes[1] {
            kind if kind == DatagramPacketKind::Bootstrap as u8 => {
                if bytes.len() < DATAGRAM_HEADER_LEN {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: DATAGRAM_HEADER_LEN,
                        actual: bytes.len(),
                    });
                }
                let stream_id = StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap()));
                let payload_len = u16::from_le_bytes(bytes[22..24].try_into().unwrap()) as usize;
                if bytes.len() != DATAGRAM_HEADER_LEN + payload_len {
                    return Err(DatagramWireError::PayloadLengthMismatch {
                        header_len: payload_len,
                        actual_len: bytes.len().saturating_sub(DATAGRAM_HEADER_LEN),
                    });
                }
                Ok(Self::Bootstrap {
                    stream_id,
                    frame: bytes[DATAGRAM_HEADER_LEN..].to_vec(),
                })
            }
            kind if kind == DatagramPacketKind::Data as u8 => {
                if bytes.len() < DATAGRAM_HEADER_LEN {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: DATAGRAM_HEADER_LEN,
                        actual: bytes.len(),
                    });
                }
                let header = DatagramDataHeader {
                    stream_id: StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap())),
                    message_id: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
                    chunk_index: u16::from_le_bytes(bytes[18..20].try_into().unwrap()),
                    chunk_count: u16::from_le_bytes(bytes[20..22].try_into().unwrap()),
                    payload_len: u16::from_le_bytes(bytes[22..24].try_into().unwrap()),
                };
                if header.chunk_count == 0 || header.chunk_index >= header.chunk_count {
                    return Err(DatagramWireError::InvalidChunkIndex {
                        chunk_index: header.chunk_index,
                        chunk_count: header.chunk_count,
                    });
                }
                let payload_len = header.payload_len as usize;
                if bytes.len() != DATAGRAM_HEADER_LEN + payload_len {
                    return Err(DatagramWireError::PayloadLengthMismatch {
                        header_len: payload_len,
                        actual_len: bytes.len().saturating_sub(DATAGRAM_HEADER_LEN),
                    });
                }
                Ok(Self::Data {
                    header,
                    payload: bytes[DATAGRAM_HEADER_LEN..].to_vec(),
                })
            }
            kind if kind == DatagramPacketKind::Ack as u8 => {
                if bytes.len() != DATAGRAM_ACK_LEN {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: DATAGRAM_ACK_LEN,
                        actual: bytes.len(),
                    });
                }
                Ok(Self::Ack(DatagramAck {
                    stream_id: StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap())),
                    message_id: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
                }))
            }
            kind if kind == DatagramPacketKind::RepairRequest as u8 => {
                if bytes.len() < 1 + 1 + 8 + 8 + 1 {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: 19,
                        actual: bytes.len(),
                    });
                }
                let stream_id = StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap()));
                let message_id = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
                let range_count = bytes[18] as usize;
                let expected_len = 19 + range_count * 4;
                if bytes.len() != expected_len {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: expected_len,
                        actual: bytes.len(),
                    });
                }
                let mut missing_ranges = Vec::with_capacity(range_count);
                let mut cursor = 19;
                for _ in 0..range_count {
                    let start_chunk_index =
                        u16::from_le_bytes(bytes[cursor..cursor + 2].try_into().unwrap());
                    let end_chunk_index_inclusive =
                        u16::from_le_bytes(bytes[cursor + 2..cursor + 4].try_into().unwrap());
                    if end_chunk_index_inclusive < start_chunk_index {
                        return Err(DatagramWireError::InvalidRepairRange {
                            start_chunk_index,
                            end_chunk_index_inclusive,
                        });
                    }
                    missing_ranges.push(DatagramChunkRange {
                        start_chunk_index,
                        end_chunk_index_inclusive,
                    });
                    cursor += 4;
                }
                Ok(Self::RepairRequest(DatagramRepairRequest {
                    stream_id,
                    message_id,
                    missing_ranges,
                }))
            }
            kind if kind == DatagramPacketKind::Keepalive as u8 => {
                if bytes.len() != DATAGRAM_KEEPALIVE_LEN {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: DATAGRAM_KEEPALIVE_LEN,
                        actual: bytes.len(),
                    });
                }
                Ok(Self::Keepalive(DatagramKeepalive {
                    stream_id: StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap())),
                }))
            }
            kind if kind == DatagramPacketKind::Close as u8 => {
                if bytes.len() != DATAGRAM_CLOSE_LEN {
                    return Err(DatagramWireError::TruncatedPacket {
                        minimum: DATAGRAM_CLOSE_LEN,
                        actual: bytes.len(),
                    });
                }
                Ok(Self::Close(DatagramClose {
                    stream_id: StreamId(u64::from_le_bytes(bytes[2..10].try_into().unwrap())),
                    code: u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
                }))
            }
            other => Err(DatagramWireError::UnknownPacketKind(other)),
        }
    }
}

#[derive(Debug, Error)]
pub enum DatagramWireError {
    #[error("unsupported datagram packet version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown datagram packet kind {0}")]
    UnknownPacketKind(u8),
    #[error("truncated datagram packet: expected at least {minimum} bytes, got {actual}")]
    TruncatedPacket { minimum: usize, actual: usize },
    #[error("datagram payload too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("datagram payload length mismatch: header={header_len}, actual={actual_len}")]
    PayloadLengthMismatch {
        header_len: usize,
        actual_len: usize,
    },
    #[error("invalid chunk index {chunk_index} for chunk count {chunk_count}")]
    InvalidChunkIndex { chunk_index: u16, chunk_count: u16 },
    #[error("invalid repair range {start_chunk_index}..={end_chunk_index_inclusive}")]
    InvalidRepairRange {
        start_chunk_index: u16,
        end_chunk_index_inclusive: u16,
    },
    #[error("too many repair ranges: {actual}")]
    TooManyRepairRanges { actual: usize },
    #[error("message requires {chunk_count} chunks, exceeding max {max_chunks}")]
    TooManyChunksForMessage {
        chunk_count: usize,
        max_chunks: usize,
    },
}

#[derive(Debug, Clone)]
pub struct PendingDatagramMessage {
    pub stream_id: StreamId,
    pub message_id: u64,
    pub chunks: Vec<Vec<u8>>,
    pub repair_attempts: u32,
}

#[derive(Debug, Default, Clone)]
pub struct DatagramSendWindow {
    pending: BTreeMap<u64, PendingDatagramMessage>,
}

impl DatagramSendWindow {
    pub fn insert(&mut self, message: PendingDatagramMessage, max_messages: usize) {
        self.pending.insert(message.message_id, message);
        while self.pending.len() > max_messages {
            let first_key = match self.pending.first_key_value() {
                Some((message_id, _)) => *message_id,
                None => break,
            };
            self.pending.remove(&first_key);
        }
    }

    pub fn acknowledge(&mut self, message_id: u64) -> bool {
        self.pending.remove(&message_id).is_some()
    }

    pub fn pending(&self, message_id: u64) -> Option<&PendingDatagramMessage> {
        self.pending.get(&message_id)
    }

    pub fn pending_mut(&mut self, message_id: u64) -> Option<&mut PendingDatagramMessage> {
        self.pending.get_mut(&message_id)
    }
}

#[derive(Debug, Clone)]
struct PartialMessage {
    chunk_count: u16,
    received: BTreeMap<u16, Vec<u8>>,
}

impl PartialMessage {
    fn new(chunk_count: u16) -> Self {
        Self {
            chunk_count,
            received: BTreeMap::new(),
        }
    }

    fn insert(&mut self, chunk_index: u16, payload: Vec<u8>) -> bool {
        self.received.entry(chunk_index).or_insert(payload);
        self.received.len() == self.chunk_count as usize
    }

    fn missing_ranges(&self) -> Vec<DatagramChunkRange> {
        let mut missing = Vec::new();
        let mut range_start = None;
        for index in 0..self.chunk_count {
            let present = self.received.contains_key(&index);
            match (present, range_start) {
                (false, None) => range_start = Some(index),
                (true, Some(start)) => {
                    missing.push(DatagramChunkRange {
                        start_chunk_index: start,
                        end_chunk_index_inclusive: index.saturating_sub(1),
                    });
                    range_start = None;
                }
                _ => {}
            }
        }
        if let Some(start) = range_start {
            missing.push(DatagramChunkRange {
                start_chunk_index: start,
                end_chunk_index_inclusive: self.chunk_count.saturating_sub(1),
            });
        }
        missing
    }

    fn into_payload(self) -> Vec<u8> {
        let mut payload = Vec::new();
        for (_index, chunk) in self.received {
            payload.extend_from_slice(&chunk);
        }
        payload
    }
}

#[derive(Debug, Default, Clone)]
pub struct DatagramReassemblyBuffer {
    expected_message_id: u64,
    partial: HashMap<u64, PartialMessage>,
    completed: BTreeMap<u64, Vec<u8>>,
    acknowledged: BTreeSet<u64>,
}

impl DatagramReassemblyBuffer {
    pub fn expected_message_id(&self) -> u64 {
        self.expected_message_id
    }

    pub fn insert_data(
        &mut self,
        header: DatagramDataHeader,
        payload: Vec<u8>,
    ) -> Result<bool, DatagramWireError> {
        let entry = self
            .partial
            .entry(header.message_id)
            .or_insert_with(|| PartialMessage::new(header.chunk_count));
        if entry.chunk_count != header.chunk_count {
            return Err(DatagramWireError::InvalidChunkIndex {
                chunk_index: header.chunk_index,
                chunk_count: header.chunk_count,
            });
        }
        let complete = entry.insert(header.chunk_index, payload);
        if complete {
            if let Some(message) = self.partial.remove(&header.message_id) {
                self.completed
                    .insert(header.message_id, message.into_payload());
                self.acknowledged.insert(header.message_id);
            }
        }
        Ok(complete)
    }

    pub fn missing_ranges_for(&self, message_id: u64) -> Option<Vec<DatagramChunkRange>> {
        self.partial
            .get(&message_id)
            .map(PartialMessage::missing_ranges)
    }

    pub fn pop_next_ready(&mut self) -> Option<(u64, Vec<u8>)> {
        let payload = self.completed.remove(&self.expected_message_id)?;
        let message_id = self.expected_message_id;
        self.expected_message_id = self.expected_message_id.saturating_add(1);
        Some((message_id, payload))
    }

    pub fn take_acknowledged(&mut self) -> Vec<u64> {
        self.acknowledged.iter().copied().collect()
    }

    pub fn clear_acknowledged(&mut self) {
        self.acknowledged.clear();
    }

    pub fn incomplete_message_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.partial.keys().copied()
    }
}

pub fn fragment_transport_frame(
    stream_id: StreamId,
    message_id: u64,
    frame: &[u8],
    config: DatagramReliabilityConfig,
) -> Result<Vec<DatagramPacket>, DatagramWireError> {
    let chunk_payload_size = config.target_datagram_payload_size_bytes.max(1);
    let chunk_count = frame.len().div_ceil(chunk_payload_size);
    if chunk_count > config.max_chunks_per_message {
        return Err(DatagramWireError::TooManyChunksForMessage {
            chunk_count,
            max_chunks: config.max_chunks_per_message,
        });
    }
    let chunk_count = chunk_count.max(1);
    let chunk_count_u16 =
        u16::try_from(chunk_count).map_err(|_| DatagramWireError::TooManyChunksForMessage {
            chunk_count,
            max_chunks: config.max_chunks_per_message,
        })?;
    let mut packets = Vec::with_capacity(chunk_count);
    for (chunk_index, chunk) in frame.chunks(chunk_payload_size).enumerate() {
        let payload_len = u16::try_from(chunk.len())
            .map_err(|_| DatagramWireError::PayloadTooLarge(chunk.len()))?;
        packets.push(DatagramPacket::Data {
            header: DatagramDataHeader {
                stream_id,
                message_id,
                chunk_index: chunk_index as u16,
                chunk_count: chunk_count_u16,
                payload_len,
            },
            payload: chunk.to_vec(),
        });
    }
    Ok(packets)
}

pub fn repair_packets_for_message(
    pending: &PendingDatagramMessage,
    ranges: &[DatagramChunkRange],
) -> Vec<DatagramPacket> {
    let chunk_count = pending.chunks.len() as u16;
    let mut out = Vec::new();
    for range in ranges {
        for chunk_index in range.start_chunk_index..=range.end_chunk_index_inclusive {
            if let Some(payload) = pending.chunks.get(chunk_index as usize) {
                out.push(DatagramPacket::Data {
                    header: DatagramDataHeader {
                        stream_id: pending.stream_id,
                        message_id: pending.message_id,
                        chunk_index,
                        chunk_count,
                        payload_len: payload.len() as u16,
                    },
                    payload: payload.clone(),
                });
            }
        }
    }
    out
}

pub fn encode_datagram_frames(
    packets: impl IntoIterator<Item = DatagramPacket>,
) -> Result<Vec<Vec<u8>>, DatagramWireError> {
    packets
        .into_iter()
        .map(|packet| packet.to_bytes())
        .collect()
}

pub fn decode_datagram_frame(frame: &[u8]) -> Result<DatagramPacket, DatagramWireError> {
    DatagramPacket::from_bytes(frame)
}

#[derive(Debug, Error)]
pub enum DatagramSessionError {
    #[error(transparent)]
    Wire(#[from] DatagramWireError),
    #[error(transparent)]
    RecordWire(#[from] WireError),
    #[error("datagram stream id mismatch: expected {expected:?}, actual {actual:?}")]
    UnexpectedStreamId {
        expected: StreamId,
        actual: StreamId,
    },
    #[error("repair budget exceeded for message {message_id}")]
    RepairBudgetExceeded { message_id: u64 },
    #[error("datagram session is closed")]
    SessionClosed,
}

#[derive(Debug, Clone, Default)]
pub struct DatagramSessionMetrics {
    pub outbound_messages: u64,
    pub outbound_datagrams: u64,
    pub retransmitted_datagrams: u64,
    pub acknowledged_messages: u64,
    pub repair_requests_sent: u64,
    pub repair_requests_received: u64,
    pub duplicate_chunks_ignored: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DatagramIngressOutcome {
    pub outbound_datagrams: Vec<Vec<u8>>,
    pub ready_frames: Vec<Vec<u8>>,
    pub close_code: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct DatagramTransportState {
    stream_id: StreamId,
    reliability: DatagramReliabilityConfig,
    next_outbound_message_id: u64,
    send_window: DatagramSendWindow,
    reassembly: DatagramReassemblyBuffer,
    inbound_repair_attempts: HashMap<u64, u32>,
    closed: bool,
    metrics: DatagramSessionMetrics,
}

impl DatagramTransportState {
    pub fn new(stream_id: StreamId, reliability: DatagramReliabilityConfig) -> Self {
        Self {
            stream_id,
            reliability,
            next_outbound_message_id: 0,
            send_window: DatagramSendWindow::default(),
            reassembly: DatagramReassemblyBuffer::default(),
            inbound_repair_attempts: HashMap::new(),
            closed: false,
            metrics: DatagramSessionMetrics::default(),
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn reliability(&self) -> DatagramReliabilityConfig {
        self.reliability
    }

    pub fn metrics(&self) -> &DatagramSessionMetrics {
        &self.metrics
    }

    pub fn expected_inbound_message_id(&self) -> u64 {
        self.reassembly.expected_message_id()
    }

    pub fn encode_transport_frame(
        &mut self,
        frame: &[u8],
    ) -> Result<Vec<Vec<u8>>, DatagramSessionError> {
        if self.closed {
            return Err(DatagramSessionError::SessionClosed);
        }
        let message_id = self.next_outbound_message_id;
        self.next_outbound_message_id = self.next_outbound_message_id.saturating_add(1);
        let packets =
            fragment_transport_frame(self.stream_id, message_id, frame, self.reliability)?;
        let mut chunks = Vec::with_capacity(packets.len());
        for packet in &packets {
            let DatagramPacket::Data { payload, .. } = packet else {
                unreachable!("fragment_transport_frame only emits DATA packets");
            };
            chunks.push(payload.clone());
        }
        self.send_window.insert(
            PendingDatagramMessage {
                stream_id: self.stream_id,
                message_id,
                chunks,
                repair_attempts: 0,
            },
            self.reliability.max_in_flight_messages,
        );
        self.metrics.outbound_messages = self.metrics.outbound_messages.saturating_add(1);
        self.metrics.outbound_datagrams = self
            .metrics
            .outbound_datagrams
            .saturating_add(packets.len() as u64);
        Ok(encode_datagram_frames(packets)?)
    }

    pub fn encode_transport_frames<I, B>(
        &mut self,
        frames: I,
    ) -> Result<Vec<Vec<u8>>, DatagramSessionError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut out = Vec::new();
        for frame in frames {
            out.extend(self.encode_transport_frame(frame.as_ref())?);
        }
        Ok(out)
    }

    pub fn handle_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<DatagramIngressOutcome, DatagramSessionError> {
        if self.closed {
            return Err(DatagramSessionError::SessionClosed);
        }
        let packet = decode_datagram_frame(datagram)?;
        if packet.stream_id() != self.stream_id {
            return Err(DatagramSessionError::UnexpectedStreamId {
                expected: self.stream_id,
                actual: packet.stream_id(),
            });
        }

        let mut outcome = DatagramIngressOutcome::default();
        match packet {
            DatagramPacket::Bootstrap { .. } => {}
            DatagramPacket::Data { header, payload } => {
                let already_complete = self
                    .reassembly
                    .missing_ranges_for(header.message_id)
                    .is_none()
                    && header.message_id < self.reassembly.expected_message_id();
                let complete = self.reassembly.insert_data(header, payload)?;
                if !complete && already_complete {
                    self.metrics.duplicate_chunks_ignored =
                        self.metrics.duplicate_chunks_ignored.saturating_add(1);
                }
                for acked_message_id in self.reassembly.take_acknowledged() {
                    let ack = DatagramPacket::Ack(DatagramAck {
                        stream_id: self.stream_id,
                        message_id: acked_message_id,
                    });
                    outcome.outbound_datagrams.push(ack.to_bytes()?);
                }
                self.reassembly.clear_acknowledged();
                while let Some((_message_id, frame)) = self.reassembly.pop_next_ready() {
                    outcome.ready_frames.push(frame);
                }
            }
            DatagramPacket::Ack(ack) => {
                if self.send_window.acknowledge(ack.message_id) {
                    self.metrics.acknowledged_messages =
                        self.metrics.acknowledged_messages.saturating_add(1);
                }
            }
            DatagramPacket::RepairRequest(repair) => {
                self.metrics.repair_requests_received =
                    self.metrics.repair_requests_received.saturating_add(1);
                if let Some(pending) = self.send_window.pending_mut(repair.message_id) {
                    pending.repair_attempts = pending.repair_attempts.saturating_add(1);
                    if pending.repair_attempts > self.reliability.max_repair_attempts {
                        return Err(DatagramSessionError::RepairBudgetExceeded {
                            message_id: repair.message_id,
                        });
                    }
                    let repair_packets =
                        repair_packets_for_message(pending, &repair.missing_ranges);
                    self.metrics.retransmitted_datagrams = self
                        .metrics
                        .retransmitted_datagrams
                        .saturating_add(repair_packets.len() as u64);
                    outcome
                        .outbound_datagrams
                        .extend(encode_datagram_frames(repair_packets)?);
                }
            }
            DatagramPacket::Keepalive(_) => {}
            DatagramPacket::Close(close) => {
                self.closed = true;
                outcome.close_code = Some(close.code);
            }
        }
        Ok(outcome)
    }

    pub fn build_repair_requests(&mut self) -> Result<Vec<Vec<u8>>, DatagramSessionError> {
        if self.closed {
            return Err(DatagramSessionError::SessionClosed);
        }
        let mut out = Vec::new();
        let message_ids: Vec<u64> = self.reassembly.incomplete_message_ids().collect();
        for message_id in message_ids {
            let attempts = self.inbound_repair_attempts.entry(message_id).or_insert(0);
            *attempts = attempts.saturating_add(1);
            if *attempts > self.reliability.max_repair_attempts {
                return Err(DatagramSessionError::RepairBudgetExceeded { message_id });
            }
            let Some(missing_ranges) = self.reassembly.missing_ranges_for(message_id) else {
                continue;
            };
            if missing_ranges.is_empty() {
                continue;
            }
            let packet = DatagramPacket::RepairRequest(DatagramRepairRequest {
                stream_id: self.stream_id,
                message_id,
                missing_ranges,
            });
            out.push(packet.to_bytes()?);
            self.metrics.repair_requests_sent = self.metrics.repair_requests_sent.saturating_add(1);
        }
        Ok(out)
    }

    pub fn encode_keepalive(&self) -> Result<Vec<u8>, DatagramSessionError> {
        if self.closed {
            return Err(DatagramSessionError::SessionClosed);
        }
        Ok(DatagramPacket::Keepalive(DatagramKeepalive {
            stream_id: self.stream_id,
        })
        .to_bytes()?)
    }

    pub fn encode_close(&mut self, code: u16) -> Result<Vec<u8>, DatagramSessionError> {
        if self.closed {
            return Err(DatagramSessionError::SessionClosed);
        }
        self.closed = true;
        Ok(DatagramPacket::Close(DatagramClose {
            stream_id: self.stream_id,
            code,
        })
        .to_bytes()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datagram_data_packet_round_trips() {
        let packet = DatagramPacket::Data {
            header: DatagramDataHeader {
                stream_id: StreamId(9),
                message_id: 7,
                chunk_index: 1,
                chunk_count: 3,
                payload_len: 4,
            },
            payload: vec![1, 2, 3, 4],
        };
        let encoded = packet.to_bytes().unwrap();
        let decoded = DatagramPacket::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn fragment_and_reassemble_transport_frame_round_trip() {
        let config = DatagramReliabilityConfig {
            target_datagram_payload_size_bytes: 5,
            ..DatagramReliabilityConfig::default()
        };
        let frame = b"abcdefghijk".to_vec();
        let packets = fragment_transport_frame(StreamId(5), 0, &frame, config).unwrap();
        assert_eq!(packets.len(), 3);
        let mut reassembly = DatagramReassemblyBuffer::default();
        for packet in [packets[1].clone(), packets[0].clone(), packets[2].clone()] {
            let DatagramPacket::Data { header, payload } = packet else {
                unreachable!();
            };
            reassembly.insert_data(header, payload).unwrap();
        }
        let (_message_id, rebuilt) = reassembly.pop_next_ready().unwrap();
        assert_eq!(rebuilt, frame);
    }

    #[test]
    fn duplicate_chunk_is_ignored() {
        let config = DatagramReliabilityConfig {
            target_datagram_payload_size_bytes: 4,
            ..DatagramReliabilityConfig::default()
        };
        let frame = b"abcdefgh".to_vec();
        let packets = fragment_transport_frame(StreamId(1), 3, &frame, config).unwrap();
        let mut reassembly = DatagramReassemblyBuffer::default();
        let DatagramPacket::Data {
            header: first_header,
            payload: first_payload,
        } = packets[0].clone()
        else {
            unreachable!();
        };
        reassembly
            .insert_data(first_header, first_payload.clone())
            .unwrap();
        reassembly.insert_data(first_header, first_payload).unwrap();
        let missing = reassembly.missing_ranges_for(3).unwrap();
        assert_eq!(
            missing,
            vec![DatagramChunkRange {
                start_chunk_index: 1,
                end_chunk_index_inclusive: 1
            }]
        );
    }

    #[test]
    fn send_window_acknowledge_retires_message() {
        let mut window = DatagramSendWindow::default();
        window.insert(
            PendingDatagramMessage {
                stream_id: StreamId(1),
                message_id: 9,
                chunks: vec![vec![1, 2, 3]],
                repair_attempts: 0,
            },
            8,
        );
        assert!(window.pending(9).is_some());
        assert!(window.acknowledge(9));
        assert!(window.pending(9).is_none());
    }

    #[test]
    fn repair_request_packet_round_trips() {
        let packet = DatagramPacket::RepairRequest(DatagramRepairRequest {
            stream_id: StreamId(12),
            message_id: 42,
            missing_ranges: vec![
                DatagramChunkRange {
                    start_chunk_index: 1,
                    end_chunk_index_inclusive: 3,
                },
                DatagramChunkRange {
                    start_chunk_index: 7,
                    end_chunk_index_inclusive: 8,
                },
            ],
        });
        let decoded = DatagramPacket::from_bytes(&packet.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, packet);
    }
}
