use serde::{Deserialize, Serialize};

use crate::{
    AUTH_TAG_LEN, CodecMode, ControllerRouteFamily, EpochId, HybridRouteComponent, ItemId,
    PROTOCOL_VERSION, PredictiveRouteDispatchPayload, Record, RecordFlags, RecordType, SeqNo,
    StreamId, WireError,
};

pub const TRANSPORT_BURST_SMALL_CAP: usize = 4_096;
pub const TRANSPORT_BURST_MEDIUM_CAP: usize = 65_536;
pub const TRANSPORT_BURST_BIG_CAP: usize = 1_048_576;
pub const PROTECTED_TRANSPORT_FRAME_MAGIC: &[u8; 4] = b"WSTF";
pub const PROTECTED_TRANSPORT_FRAME_VERSION: u8 = 1;
pub const PROTECTED_TRANSPORT_FRAME_HEADER_LEN: usize = 4 + 1 + 4 + 8 + 4 + 4;
pub const COMPACT_TRANSPORT_RECORD_HEADER_LEN: usize = 1 + 1 + 2 + 8 + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransportMode {
    Bulk,
    BurstSmall,
    #[default]
    BurstMedium,
    BurstBig,
}

impl TransportMode {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Bulk => "bulk",
            Self::BurstSmall => "burst_small",
            Self::BurstMedium => "burst_medium",
            Self::BurstBig => "burst_big",
        }
    }

    pub fn payload_cap_bytes(self) -> Option<usize> {
        match self {
            Self::Bulk => None,
            Self::BurstSmall => Some(TRANSPORT_BURST_SMALL_CAP),
            Self::BurstMedium => Some(TRANSPORT_BURST_MEDIUM_CAP),
            Self::BurstBig => Some(TRANSPORT_BURST_BIG_CAP),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransportConfig {
    pub mode: TransportMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectedTransportFrameHeader {
    pub epoch_id: EpochId,
    pub seq_no: SeqNo,
    pub record_count: u32,
    pub payload_len: u32,
}

impl ProtectedTransportFrameHeader {
    pub fn aad_bytes(&self) -> [u8; PROTECTED_TRANSPORT_FRAME_HEADER_LEN] {
        let mut out = [0_u8; PROTECTED_TRANSPORT_FRAME_HEADER_LEN];
        out[..4].copy_from_slice(PROTECTED_TRANSPORT_FRAME_MAGIC);
        out[4] = PROTECTED_TRANSPORT_FRAME_VERSION;
        out[5..9].copy_from_slice(&self.epoch_id.0.to_le_bytes());
        out[9..17].copy_from_slice(&self.seq_no.0.to_le_bytes());
        out[17..21].copy_from_slice(&self.record_count.to_le_bytes());
        out[21..25].copy_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    pub fn from_aad_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != PROTECTED_TRANSPORT_FRAME_HEADER_LEN {
            return Err(WireError::InvalidTransportFrameHeaderLen {
                expected: PROTECTED_TRANSPORT_FRAME_HEADER_LEN,
                actual: bytes.len(),
            });
        }
        if &bytes[..4] != PROTECTED_TRANSPORT_FRAME_MAGIC {
            return Err(WireError::InvalidTransportFrameMagic);
        }
        if bytes[4] != PROTECTED_TRANSPORT_FRAME_VERSION {
            return Err(WireError::UnsupportedTransportFrameVersion(bytes[4]));
        }
        Ok(Self {
            epoch_id: EpochId(u32::from_le_bytes(bytes[5..9].try_into().unwrap())),
            seq_no: SeqNo(u64::from_le_bytes(bytes[9..17].try_into().unwrap())),
            record_count: u32::from_le_bytes(bytes[17..21].try_into().unwrap()),
            payload_len: u32::from_le_bytes(bytes[21..25].try_into().unwrap()),
        })
    }
}

pub fn transport_payload_len(record: &Record) -> usize {
    if matches!(
        record.header.record_type,
        RecordType::ExactState
            | RecordType::Repair
            | RecordType::PredictiveConfirm
            | RecordType::PredictiveCorrect
            | RecordType::AssemblyDef
            | RecordType::TransformDef
            | RecordType::SchemaDef
            | RecordType::EpisodeHint
            | RecordType::ReplayHint
            | RecordType::MemoryRetire
            | RecordType::TransformCorrect
            | RecordType::MemoryAck
    ) {
        record.payload.len()
    } else {
        0
    }
}

pub fn pack_record_groups(
    records: impl IntoIterator<Item = Record>,
    config: TransportConfig,
) -> Vec<Vec<Record>> {
    let cap = config.mode.payload_cap_bytes();
    let mut groups = Vec::<Vec<Record>>::new();
    let mut current = Vec::<Record>::new();
    let mut current_payload = 0_usize;

    for record in records {
        let record_payload = transport_payload_len(&record);
        if let Some(cap) = cap {
            if !current.is_empty() && current_payload + record_payload > cap {
                groups.push(std::mem::take(&mut current));
                current_payload = 0;
            }
        }

        current.push(record);
        current_payload += record_payload;

        if let Some(cap) = cap {
            if current_payload >= cap {
                groups.push(std::mem::take(&mut current));
                current_payload = 0;
            }
        }
    }

    if !current.is_empty() {
        groups.push(current);
    }

    groups
}

pub fn pack_records(
    records: impl IntoIterator<Item = Record>,
    config: TransportConfig,
) -> Vec<Vec<u8>> {
    pack_record_groups(records, config)
        .into_iter()
        .map(|group| {
            let frames = group
                .into_iter()
                .map(|record| record.to_bytes())
                .collect::<Vec<_>>();
            encode_transport_group(&frames)
        })
        .collect()
}

pub fn encode_transport_group(record_frames: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        record_frames
            .iter()
            .map(|frame| 4 + frame.len())
            .sum::<usize>(),
    );
    for frame in record_frames {
        out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
        out.extend_from_slice(frame);
    }
    out
}

pub fn decode_transport_records(frame: &[u8]) -> Result<Vec<Record>, WireError> {
    if frame.len() >= 4 {
        let mut cursor = 0_usize;
        let mut records = Vec::new();
        let mut chunk_ok = true;
        while cursor < frame.len() {
            if frame.len().saturating_sub(cursor) < 4 {
                chunk_ok = false;
                break;
            }
            let record_len =
                u32::from_le_bytes(frame[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if record_len == 0 || frame.len().saturating_sub(cursor) < record_len {
                chunk_ok = false;
                break;
            }
            match Record::from_bytes(&frame[cursor..cursor + record_len]) {
                Ok(record) => records.push(record),
                Err(_) => {
                    chunk_ok = false;
                    break;
                }
            }
            cursor += record_len;
        }
        if chunk_ok && !records.is_empty() && cursor == frame.len() {
            return Ok(records);
        }
    }

    Ok(vec![Record::from_bytes(frame)?])
}

pub fn encode_compact_transport_records(records: &[Record]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        records
            .iter()
            .map(|record| COMPACT_TRANSPORT_RECORD_HEADER_LEN + record.payload.len())
            .sum::<usize>(),
    );

    for record in records {
        out.push(record.header.record_type as u8);
        out.push(record.header.codec_mode as u8);
        out.extend_from_slice(&record.header.flags.bits().to_le_bytes());
        out.extend_from_slice(&record.header.item_id.0.to_le_bytes());
        out.extend_from_slice(&record.header.payload_len.to_le_bytes());
        out.extend_from_slice(&record.payload);
    }

    out
}

pub fn decode_compact_transport_records(
    payload: &[u8],
    stream_id: StreamId,
    start_epoch: EpochId,
    start_seq: SeqNo,
) -> Result<Vec<Record>, WireError> {
    let mut cursor = 0_usize;
    let mut epoch_id = start_epoch;
    let mut seq_no = start_seq;
    let mut records = Vec::new();

    while cursor < payload.len() {
        if payload.len().saturating_sub(cursor) < COMPACT_TRANSPORT_RECORD_HEADER_LEN {
            return Err(WireError::TruncatedCompactTransportRecord {
                actual: payload.len().saturating_sub(cursor),
                minimum: COMPACT_TRANSPORT_RECORD_HEADER_LEN,
            });
        }

        let record_type = decode_record_type(payload[cursor])?;
        let codec_mode = decode_codec_mode(payload[cursor + 1])?;
        let flags = RecordFlags::from_bits(u16::from_le_bytes(
            payload[cursor + 2..cursor + 4].try_into().unwrap(),
        ));
        let item_id = ItemId(u64::from_le_bytes(
            payload[cursor + 4..cursor + 12].try_into().unwrap(),
        ));
        let payload_len =
            u32::from_le_bytes(payload[cursor + 12..cursor + 16].try_into().unwrap()) as usize;
        cursor += COMPACT_TRANSPORT_RECORD_HEADER_LEN;

        if payload.len().saturating_sub(cursor) < payload_len {
            return Err(WireError::CompactTransportRecordLenMismatch {
                expected: payload_len,
                actual: payload.len().saturating_sub(cursor),
            });
        }

        let record = Record {
            header: crate::RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id,
                epoch_id,
                seq_no,
                record_type,
                codec_mode,
                flags,
                item_id,
                payload_len: payload_len as u32,
            },
            payload: payload[cursor..cursor + payload_len].to_vec(),
            auth_tag: [0_u8; AUTH_TAG_LEN],
        }
        .validate()?;
        cursor += payload_len;

        records.push(record.clone());

        if matches!(record.header.record_type, RecordType::Rekey) {
            epoch_id = EpochId(epoch_id.0 + 1);
            seq_no = SeqNo(0);
        } else {
            seq_no = SeqNo(seq_no.0 + 1);
        }
    }

    Ok(records)
}

pub fn encode_protected_transport_frame(
    header: ProtectedTransportFrameHeader,
    ciphertext: &[u8],
    auth_tag: &[u8; AUTH_TAG_LEN],
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(PROTECTED_TRANSPORT_FRAME_HEADER_LEN + ciphertext.len() + AUTH_TAG_LEN);
    out.extend_from_slice(&header.aad_bytes());
    out.extend_from_slice(ciphertext);
    out.extend_from_slice(auth_tag);
    out
}

pub fn decode_protected_transport_frame(
    frame: &[u8],
) -> Result<(ProtectedTransportFrameHeader, &[u8], [u8; AUTH_TAG_LEN]), WireError> {
    if frame.len() < PROTECTED_TRANSPORT_FRAME_HEADER_LEN + AUTH_TAG_LEN {
        return Err(WireError::TruncatedTransportFrame {
            actual: frame.len(),
            minimum: PROTECTED_TRANSPORT_FRAME_HEADER_LEN + AUTH_TAG_LEN,
        });
    }

    let header = ProtectedTransportFrameHeader::from_aad_bytes(
        &frame[..PROTECTED_TRANSPORT_FRAME_HEADER_LEN],
    )?;
    let ciphertext_len = frame.len() - PROTECTED_TRANSPORT_FRAME_HEADER_LEN - AUTH_TAG_LEN;
    if ciphertext_len != header.payload_len as usize {
        return Err(WireError::TransportFramePayloadLenMismatch {
            expected: header.payload_len as usize,
            actual: ciphertext_len,
        });
    }

    let payload_start = PROTECTED_TRANSPORT_FRAME_HEADER_LEN;
    let payload_end = payload_start + ciphertext_len;
    let mut auth_tag = [0_u8; AUTH_TAG_LEN];
    auth_tag.copy_from_slice(&frame[payload_end..]);
    Ok((header, &frame[payload_start..payload_end], auth_tag))
}

fn decode_record_type(value: u8) -> Result<RecordType, WireError> {
    RecordType::try_from_u8(value)
}

fn decode_codec_mode(value: u8) -> Result<CodecMode, WireError> {
    match value {
        0 => Ok(CodecMode::None),
        1 => Ok(CodecMode::PackedExact),
        2 => Ok(CodecMode::PredictedExact),
        3 => Ok(CodecMode::DirectExact),
        other => Err(WireError::UnknownCodecMode(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AUTH_TAG_LEN, CodecMode, EpochId, ItemId, PROTOCOL_VERSION, RecordFlags, RecordHeader,
        StreamId,
    };

    fn sample_record(payload_len: usize) -> Record {
        Record {
            header: RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: crate::SeqNo(0),
                record_type: crate::RecordType::ExactState,
                codec_mode: CodecMode::DirectExact,
                flags: RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload_len as u32,
            },
            payload: vec![7; payload_len],
            auth_tag: [0; AUTH_TAG_LEN],
        }
    }

    #[test]
    fn pack_records_groups_small_bursts() {
        let frames = pack_records(
            vec![sample_record(1024); 5],
            TransportConfig {
                mode: TransportMode::BurstSmall,
            },
        );
        assert_eq!(frames.len(), 2);
        assert_eq!(decode_transport_records(&frames[0]).unwrap().len(), 4);
        assert_eq!(decode_transport_records(&frames[1]).unwrap().len(), 1);
    }

    #[test]
    fn bulk_packs_records_into_transport_groups() {
        let frames = pack_records(
            vec![sample_record(512); 8],
            TransportConfig {
                mode: TransportMode::Bulk,
            },
        );
        assert_eq!(frames.len(), 1);
        assert_eq!(decode_transport_records(&frames[0]).unwrap().len(), 8);
    }

    #[test]
    fn bulk_has_no_artificial_transport_boundary() {
        let frames = pack_records(
            vec![sample_record(512); 5000],
            TransportConfig {
                mode: TransportMode::Bulk,
            },
        );
        assert_eq!(frames.len(), 1);
        assert_eq!(decode_transport_records(&frames[0]).unwrap().len(), 5000);
    }

    #[test]
    fn decode_transport_records_accepts_single_record_bytes() {
        let record = sample_record(128);
        let decoded = decode_transport_records(&record.to_bytes()).unwrap();
        assert_eq!(decoded, vec![record]);
    }
}

pub fn pack_assembly_def_record(
    header: crate::RecordHeader,
    payload: &crate::AssemblyDefPayload,
    config: TransportConfig,
) -> Result<Vec<Vec<u8>>, crate::StateProgramError> {
    let record = crate::encode_assembly_def_record(header, payload)?;
    Ok(pack_records([record], config))
}

pub fn decode_transport_assembly_defs(
    frame: &[u8],
) -> Result<Vec<crate::AssemblyDefPayload>, crate::StateProgramError> {
    let records = decode_transport_records(frame).map_err(|error| {
        crate::StateProgramError::AssemblyPayloadSerialization(error.to_string())
    })?;
    records
        .iter()
        .filter(|record| record.header.record_type == crate::RecordType::AssemblyDef)
        .map(crate::decode_assembly_def_record)
        .collect()
}

pub fn pack_transform_def_record(
    header: crate::RecordHeader,
    payload: &crate::TransformDefPayload,
    config: TransportConfig,
) -> Result<Vec<Vec<u8>>, crate::StateProgramError> {
    let record = crate::encode_transform_def_record(header, payload)?;
    Ok(pack_records([record], config))
}

pub fn decode_transport_transform_defs(
    frame: &[u8],
) -> Result<Vec<crate::TransformDefPayload>, crate::StateProgramError> {
    let records = decode_transport_records(frame).map_err(|error| {
        crate::StateProgramError::TransformPayloadSerialization(error.to_string())
    })?;
    records
        .iter()
        .filter(|record| record.header.record_type == crate::RecordType::TransformDef)
        .map(crate::decode_transform_def_record)
        .collect()
}

pub fn pack_transform_instance_record(
    header: crate::RecordHeader,
    payload: &crate::TransformInstancePayload,
    config: TransportConfig,
) -> Result<Vec<Vec<u8>>, crate::StateProgramError> {
    let record = crate::encode_transform_instance_record(header, payload)?;
    Ok(pack_records([record], config))
}

pub fn decode_transport_transform_instances(
    frame: &[u8],
) -> Result<Vec<crate::TransformInstancePayload>, crate::StateProgramError> {
    let records = decode_transport_records(frame).map_err(|error| {
        crate::StateProgramError::TransformPayloadSerialization(error.to_string())
    })?;
    records
        .iter()
        .filter(|record| record.header.record_type == crate::RecordType::TransformCorrect)
        .map(crate::decode_transform_instance_record)
        .collect()
}

pub fn pack_episode_hint_record(
    header: crate::RecordHeader,
    payload: &crate::EpisodeHintPayload,
    config: TransportConfig,
) -> Result<Vec<Vec<u8>>, crate::StateProgramError> {
    let record = crate::encode_episode_hint_record(header, payload)?;
    Ok(pack_records([record], config))
}

pub fn encode_transport_schema_defs(
    header: crate::RecordHeader,
    payloads: &[crate::SchemaDefPayload],
) -> Result<Vec<u8>, crate::StateProgramError> {
    let mut records = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let record = crate::encode_schema_def_record(header, payload)?;
        records.push(record);
    }
    Ok(encode_transport_group(
        &records
            .into_iter()
            .map(|record| record.to_bytes())
            .collect::<Vec<_>>(),
    ))
}

pub fn decode_transport_schema_defs(
    frame: &[u8],
) -> Result<Vec<crate::SchemaDefPayload>, crate::StateProgramError> {
    let records = decode_transport_records(frame)
        .map_err(|error| crate::StateProgramError::SchemaPayloadSerialization(error.to_string()))?;
    let mut out = Vec::new();
    for record in records {
        if record.header.record_type == crate::RecordType::SchemaDef {
            out.push(crate::decode_schema_def_record(&record)?);
        }
    }
    Ok(out)
}

pub fn decode_transport_episode_hints(
    frame: &[u8],
) -> Result<Vec<crate::EpisodeHintPayload>, crate::StateProgramError> {
    let records = decode_transport_records(frame).map_err(|error| {
        crate::StateProgramError::EpisodePayloadSerialization(error.to_string())
    })?;
    records
        .iter()
        .filter(|record| record.header.record_type == crate::RecordType::EpisodeHint)
        .map(crate::decode_episode_hint_record)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PredictiveTransportSummary {
    pub route_family: Option<ControllerRouteFamily>,
    pub substrate_components: u16,
    pub substrate_graph_components: u16,
    pub dependency_count: u16,
    pub output_len: u32,
}

pub fn summarize_predictive_transport(
    payload: &PredictiveRouteDispatchPayload,
) -> PredictiveTransportSummary {
    let mut summary = PredictiveTransportSummary {
        route_family: Some(payload.route_family),
        dependency_count: payload.dependency_closure.len().min(u16::MAX as usize) as u16,
        output_len: 0,
        ..PredictiveTransportSummary::default()
    };
    if let Some(route) = &payload.hybrid_route {
        summary.output_len = route.output_len;
        summary.dependency_count = route.dependency_closure.len().min(u16::MAX as usize) as u16;
        for component in &route.components {
            match component {
                HybridRouteComponent::Substrate(_) => {
                    summary.substrate_components = summary.substrate_components.saturating_add(1);
                }
                HybridRouteComponent::SubstrateGraph(_) => {
                    summary.substrate_graph_components =
                        summary.substrate_graph_components.saturating_add(1);
                }
                _ => {}
            }
        }
    }
    summary
}
