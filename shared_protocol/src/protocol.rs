use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PROTOCOL_VERSION: u8 = 2;
pub const AUTH_TAG_LEN: usize = 16;
pub const HEADER_LEN: usize = 37;
pub const REKEY_SEED_LEN: usize = 32;
pub const REKEY_PAYLOAD_LEN: usize = 4 + REKEY_SEED_LEN;
pub const CLOSE_CODE_LEN: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct EpochId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct SeqNo(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ItemId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum RecordType {
    #[default]
    ExactState = 17,
    Rekey = 2,
    Resync = 3,
    Close = 4,
    SourceMeta = 5,
    // Wire-history-only discriminants 6 (StateDef), 7 (StatePatch), 8 (BlockCatalogSync)
    // are retired. See WireError::RetiredDiscriminant.
    Repair = 9,
    PredictiveConfirm = 10,
    PredictiveCorrect = 11,
    AssemblyDef = 12,
    TransformDef = 13,
    SchemaDef = 14,
    EpisodeHint = 15,
    ReplayHint = 16,
    MemoryRetire = 18,
    TransformCorrect = 19,
    MemoryAck = 20,
    /// Wave 13 T-13-a: Batch envelope wrapping N items in a single AEAD-
    /// protected transport frame. Each batch item carries its own item_id,
    /// source_kind, and compressed payload, but the AEAD tag, record header,
    /// and transport envelope overhead is amortized across all items in the
    /// batch.
    ///
    /// Wire format (postcard-encoded):
    ///   [item_count: u16][item_0][item_1]...[item_N-1]
    /// where each item is:
    ///   [item_id: u64][source_kind: u8][payload_len: u32][payload: [u8; payload_len]]
    BatchEnvelope = 21,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum CodecMode {
    #[default]
    None = 0,
    PackedExact = 1,
    PredictedExact = 2,
    DirectExact = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum CacheOp {
    #[default]
    Insert = 1,
    UpsertObject = 2,
    Evict = 3,
    Invalidate = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RecordFlags(u16);

impl RecordFlags {
    pub const IS_EVICT: u16 = 1 << 0;
    pub const IS_INVALIDATE: u16 = 1 << 1;
    pub const POST_REKEY_CONFIRM: u16 = 1 << 2;
    pub const DIRECT_STATE_FALLBACK: u16 = 1 << 3;
    pub const SUBSTRATE_FALLBACK: u16 = 1 << 4;
    /// Indicates this direct-state fallback was caused by a demoted transform route.
    /// Transform routes are demoted from the active architecture (see extend.md §6 demotion).
    pub const TRANSFORM_DEMOTED_FALLBACK: u16 = 1 << 9;
    /// S3.4: Indicates this direct-state fallback was caused by a dependency
    /// being unavailable on the peer (admissibility failure).
    pub const DEPENDENCY_UNAVAILABLE_FALLBACK: u16 = 1 << 6;
    /// S3.4: Indicates this direct-state fallback was caused by assembly
    /// structuralization failure.
    pub const ASSEMBLY_STRUCTURALIZATION_FALLBACK: u16 = 1 << 7;
    /// S3.4: Indicates this direct-state fallback was caused by schema
    /// dependency inadmissibility.
    pub const SCHEMA_DEPENDENCY_INADMISSIBLE_FALLBACK: u16 = 1 << 8;
    /// P0: Indicates the record.payload is Zstd-compressed. The receiver must
    /// decompress before decoding the payload. This applies to ALL record types —
    /// PredictiveConfirm/Correct payloads (bincode-encoded
    /// PredictiveRouteDispatchPayload), DirectExact payloads, etc.
    pub const PAYLOAD_ZSTD: u16 = 1 << 10;
    /// P0: Indicates the record.payload is Zstd-compressed using a trained
    /// dictionary. The dictionary_id is embedded as the first 8 bytes of the
    /// compressed payload (after decompression of the outer zstd layer, the
    /// payload starts with [dict_id:u64][zstd_dict_compressed_data]).
    pub const PAYLOAD_ZSTD_DICT: u16 = 1 << 11;
    pub const KNOWN_MASK: u16 = Self::IS_EVICT
        | Self::IS_INVALIDATE
        | Self::POST_REKEY_CONFIRM
        | Self::DIRECT_STATE_FALLBACK
        | Self::SUBSTRATE_FALLBACK
        | Self::TRANSFORM_DEMOTED_FALLBACK
        | Self::DEPENDENCY_UNAVAILABLE_FALLBACK
        | Self::ASSEMBLY_STRUCTURALIZATION_FALLBACK
        | Self::SCHEMA_DEPENDENCY_INADMISSIBLE_FALLBACK
        | Self::PAYLOAD_ZSTD
        | Self::PAYLOAD_ZSTD_DICT;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn contains(self, flag: u16) -> bool {
        (self.0 & flag) == flag
    }

    pub fn insert(&mut self, flag: u16) {
        self.0 |= flag;
    }

    pub fn remove(&mut self, flag: u16) {
        self.0 &= !flag;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RecordHeader {
    pub version: u8,
    pub stream_id: StreamId,
    pub epoch_id: EpochId,
    pub seq_no: SeqNo,
    pub record_type: RecordType,
    pub codec_mode: CodecMode,
    pub flags: RecordFlags,
    pub item_id: ItemId,
    pub payload_len: u32,
}

impl RecordHeader {
    pub fn aad_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.push(self.version);
        out.extend_from_slice(&self.stream_id.0.to_le_bytes());
        out.extend_from_slice(&self.epoch_id.0.to_le_bytes());
        out.extend_from_slice(&self.seq_no.0.to_le_bytes());
        out.push(self.record_type as u8);
        out.push(self.codec_mode as u8);
        out.extend_from_slice(&self.flags.bits().to_le_bytes());
        out.extend_from_slice(&self.item_id.0.to_le_bytes());
        out.extend_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    pub fn from_aad_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != HEADER_LEN {
            return Err(WireError::InvalidHeaderLen {
                expected: HEADER_LEN,
                actual: bytes.len(),
            });
        }

        let version = bytes[0];
        let stream_id = StreamId(u64::from_le_bytes(bytes[1..9].try_into().unwrap()));
        let epoch_id = EpochId(u32::from_le_bytes(bytes[9..13].try_into().unwrap()));
        let seq_no = SeqNo(u64::from_le_bytes(bytes[13..21].try_into().unwrap()));
        let record_type = RecordType::try_from_u8(bytes[21])?;
        let codec_mode = CodecMode::try_from_u8(bytes[22])?;
        let flags = RecordFlags::from_bits(u16::from_le_bytes(bytes[23..25].try_into().unwrap()));
        let item_id = ItemId(u64::from_le_bytes(bytes[25..33].try_into().unwrap()));
        let payload_len = u32::from_le_bytes(bytes[33..37].try_into().unwrap());

        Ok(Self {
            version,
            stream_id,
            epoch_id,
            seq_no,
            record_type,
            codec_mode,
            flags,
            item_id,
            payload_len,
        })
    }

    pub fn validate(self) -> Result<Self, ValidationError> {
        if self.version != PROTOCOL_VERSION {
            return Err(ValidationError::UnsupportedVersion(self.version));
        }
        if (self.flags.bits() & !RecordFlags::KNOWN_MASK) != 0 {
            return Err(ValidationError::UnknownReservedFlagBits(self.flags.bits()));
        }

        match self.record_type {
            RecordType::ExactState => validate_data_header(self)?,
            RecordType::BatchEnvelope => validate_data_header(self)?,
            RecordType::Rekey | RecordType::Resync | RecordType::Close => {
                validate_control_header(self)?
            }
            RecordType::SourceMeta => validate_source_meta_header(self)?,

            RecordType::Repair => validate_catalog_or_repair_header(self)?,
            RecordType::PredictiveConfirm
            | RecordType::PredictiveCorrect
            | RecordType::AssemblyDef
            | RecordType::TransformDef
            | RecordType::SchemaDef
            | RecordType::EpisodeHint
            | RecordType::ReplayHint
            | RecordType::MemoryRetire
            | RecordType::TransformCorrect
            | RecordType::MemoryAck => validate_predictive_header(self)?,
        }

        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
    pub auth_tag: [u8; AUTH_TAG_LEN],
}

impl Record {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + AUTH_TAG_LEN);
        out.extend_from_slice(&self.header.aad_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&self.auth_tag);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < HEADER_LEN + AUTH_TAG_LEN {
            return Err(WireError::TruncatedRecord {
                actual: bytes.len(),
                minimum: HEADER_LEN + AUTH_TAG_LEN,
            });
        }

        let header = RecordHeader::from_aad_bytes(&bytes[..HEADER_LEN])?;
        let payload_len = header.payload_len as usize;
        let expected_len = HEADER_LEN + payload_len + AUTH_TAG_LEN;
        if bytes.len() != expected_len {
            return Err(WireError::RecordLenMismatch {
                expected: expected_len,
                actual: bytes.len(),
            });
        }

        let payload = bytes[HEADER_LEN..HEADER_LEN + payload_len].to_vec();
        let mut auth_tag = [0_u8; AUTH_TAG_LEN];
        auth_tag.copy_from_slice(&bytes[HEADER_LEN + payload_len..expected_len]);

        Ok(Self {
            header,
            payload,
            auth_tag,
        }
        .validate()?)
    }

    pub fn validate(self) -> Result<Self, ValidationError> {
        self.header.validate()?;
        if self.payload.len() != self.header.payload_len as usize {
            return Err(ValidationError::PayloadLenMismatch {
                header_len: self.header.payload_len,
                actual_len: self.payload.len(),
            });
        }
        Ok(self)
    }

    /// P0: Compress the record payload with Zstd. If compression reduces the
    /// payload size, the PAYLOAD_ZSTD flag is set and the payload is replaced
    /// with the compressed version. If compression doesn't help (small or
    /// random data), the record is returned unchanged.
    ///
    /// Compression level 3 provides ~400 MB/s encode, ~1 GB/s decode, with
    /// ~70% compression on structured data. The 10-byte overhead of zstd
    /// framing means payloads under ~50 bytes rarely benefit.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn maybe_compress_zstd(self) -> Self {
        const MIN_COMPRESS_SIZE: usize = 50;
        const ZSTD_LEVEL: i32 = 3;

        if self.payload.len() < MIN_COMPRESS_SIZE {
            return self;
        }

        match zstd::encode_all(&self.payload[..], ZSTD_LEVEL) {
            Ok(compressed) if compressed.len() < self.payload.len() => {
                let mut record = Record {
                    header: self.header,
                    payload: compressed,
                    auth_tag: self.auth_tag,
                };
                record.header.flags.insert(RecordFlags::PAYLOAD_ZSTD);
                record.header.payload_len = record.payload.len() as u32;
                record
            }
            _ => self, // Compression didn't help — return unchanged
        }
    }

    /// WASM stub: zstd not available on wasm32 (zstd-sys requires clang).
    #[cfg(target_arch = "wasm32")]
    pub fn maybe_compress_zstd(self) -> Self { self }

    /// P0: Compress the record payload with Zstd using a trained dictionary.
    /// If dictionary compression succeeds and is smaller than both the
    /// original and raw-zstd, the PAYLOAD_ZSTD_DICT flag is set and the
    /// payload is [dict_id:u64_le][zstd_dict_compressed_data].
    /// Falls back to raw zstd (PAYLOAD_ZSTD) if that's better, or no
    /// compression if neither helps.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn maybe_compress_zstd_with_dict(
        self,
        dict: &crate::compress::ZstdDictionary,
    ) -> Self {
        const MIN_COMPRESS_SIZE: usize = 50;

        if self.payload.len() < MIN_COMPRESS_SIZE {
            return self;
        }

        // Try dictionary compression first.
        match dict.compress(&self.payload) {
            Ok(dict_compressed) if dict_compressed.len() < self.payload.len() => {
                // Also try raw zstd for comparison.
                let raw_compressed = zstd::encode_all(&self.payload[..], 3)
                    .unwrap_or_else(|_| self.payload.clone());

                let dict_total = 8 + dict_compressed.len(); // 8 bytes for dict_id prefix
                let raw_total = raw_compressed.len();

                if dict_total < raw_total && dict_total < self.payload.len() {
                    // Dictionary compression wins — prepend dict_id.
                    let mut payload = Vec::with_capacity(8 + dict_compressed.len());
                    payload.extend_from_slice(&dict.dict_id.0.to_le_bytes());
                    payload.extend_from_slice(&dict_compressed);
                    let mut record = Record {
                        header: self.header,
                        payload,
                        auth_tag: self.auth_tag,
                    };
                    record.header.flags.insert(RecordFlags::PAYLOAD_ZSTD_DICT);
                    record.header.payload_len = record.payload.len() as u32;
                    record
                } else if raw_total < self.payload.len() {
                    // Raw zstd wins over dictionary.
                    self.maybe_compress_zstd()
                } else {
                    self // Neither helps
                }
            }
            _ => {
                // Dictionary compression failed — try raw zstd.
                self.maybe_compress_zstd()
            }
        }
    }

    /// WASM stub: zstd not available on wasm32.
    #[cfg(target_arch = "wasm32")]
    pub fn maybe_compress_zstd_with_dict(
        self,
        _dict: &crate::compress::ZstdDictionary,
    ) -> Self { self }

    /// WASM stub: zstd not available on wasm32.
    #[cfg(target_arch = "wasm32")]
    pub fn maybe_decompress_zstd_with_dict_provider<F>(
        self,
        _dict_provider: F,
    ) -> Result<Self, WireError>
    where
        F: Fn(crate::SourceKind, u64) -> Option<Vec<u8>>,
    {
        Ok(self)
    }

    /// P0: Decompress the record payload if it's compressed. Checks the
    /// PAYLOAD_ZSTD and PAYLOAD_ZSTD_DICT flags and decompresses accordingly.
    /// Returns the decompressed record with the compression flags cleared.
    /// If not compressed, returns the record unchanged.
    pub fn maybe_decompress_zstd(self) -> Result<Self, WireError> {
        self.maybe_decompress_zstd_with_dict_provider(|_, _| None)
    }

    /// P0: Decompress the record payload with optional dictionary provider.
    /// The `dict_provider` closure takes (source_kind, dict_id) and returns
    /// the dictionary bytes if available. This allows the caller to maintain
    /// its own dictionary store.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn maybe_decompress_zstd_with_dict_provider<F>(
        self,
        dict_provider: F,
    ) -> Result<Self, WireError>
    where
        F: Fn(crate::SourceKind, u64) -> Option<Vec<u8>>,
    {
        let flags = self.header.flags;
        if flags.contains(RecordFlags::PAYLOAD_ZSTD) {
            let decompressed = zstd::decode_all(&self.payload[..])
                .map_err(|e| WireError::InvalidPayload(
                    format!("zstd decompression failed: {e}")
                ))?;
            let mut record = Record {
                header: self.header,
                payload: decompressed,
                auth_tag: self.auth_tag,
            };
            record.header.flags.remove(RecordFlags::PAYLOAD_ZSTD);
            record.header.payload_len = record.payload.len() as u32;
            Ok(record)
        } else if flags.contains(RecordFlags::PAYLOAD_ZSTD_DICT) {
            if self.payload.len() < 8 {
                return Err(WireError::InvalidPayload(
                    "PAYLOAD_ZSTD_DICT payload too short for dict_id".into()
                ));
            }
            let dict_id = u64::from_le_bytes(
                self.payload[..8].try_into().map_err(|_| 
                    WireError::InvalidPayload("dict_id parse error".into()))?
            );
            let compressed_data = &self.payload[8..];

            // Determine source_kind from the record header for dictionary lookup.
            // For predictive records, we don't know the source_kind from the header,
            // so we try each kind. For direct-state records, we can infer it.
            let dict_bytes = dict_provider(crate::SourceKind::Json, dict_id)
                .or_else(|| dict_provider(crate::SourceKind::Text, dict_id))
                .or_else(|| dict_provider(crate::SourceKind::Binary, dict_id));

            let decompressed = match dict_bytes {
                Some(dict) => {
                    let mut decoder = zstd::stream::Decoder::with_dictionary(compressed_data, &dict)
                        .map_err(|e| WireError::InvalidPayload(
                            format!("zstd dict decompression init failed: {e}")
                        ))?;
                    let mut buf = Vec::new();
                    std::io::Read::read_to_end(&mut decoder, &mut buf)
                        .map_err(|e| WireError::InvalidPayload(
                            format!("zstd dict decompression failed: {e}")
                        ))?;
                    buf
                }
                None => {
                    // Dictionary not available — try raw zstd as fallback.
                    zstd::decode_all(compressed_data)
                        .map_err(|e| WireError::InvalidPayload(
                            format!("zstd raw fallback decompression failed: {e}")
                        ))?
                }
            };

            let mut record = Record {
                header: self.header,
                payload: decompressed,
                auth_tag: self.auth_tag,
            };
            record.header.flags.remove(RecordFlags::PAYLOAD_ZSTD_DICT);
            record.header.payload_len = record.payload.len() as u32;
            Ok(record)
        } else {
            Ok(self)
        }
    }
}

fn invalid_payload_error(payload_kind: &str, detail: impl Into<String>) -> WireError {
    WireError::InvalidPayload(format!("{payload_kind}: {}", detail.into()))
}

fn invalid_payload_version(payload_kind: &str, version: u8, expected: u8) -> WireError {
    invalid_payload_error(
        payload_kind,
        format!("unsupported version {version}; expected {expected}"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MemoryRetirePayload {
    pub version: u8,
    pub plane: crate::MemoryPlane,
    pub object_kind: crate::ObjectKind,
    pub object_id: String,
}

impl MemoryRetirePayload {
    pub const VERSION: u8 = 1;
    const KIND: &'static str = "memory retire payload";

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("serialization failed: {error}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        let (payload, _): (Self, usize) = bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("decode failed: {error}"))
        })?;
        if payload.version != Self::VERSION {
            return Err(invalid_payload_version(
                Self::KIND,
                payload.version,
                Self::VERSION,
            ));
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MemoryAckPayload {
    pub version: u8,
    pub plane: crate::MemoryPlane,
    pub object_kind: crate::ObjectKind,
    pub object_id: String,
    pub acked_record_type: RecordType,
    pub acked_seq_no: SeqNo,
}

impl MemoryAckPayload {
    pub const VERSION: u8 = 1;
    const KIND: &'static str = "memory ack payload";

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("serialization failed: {error}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        let (payload, _): (Self, usize) = bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("decode failed: {error}"))
        })?;
        if payload.version != Self::VERSION {
            return Err(invalid_payload_version(
                Self::KIND,
                payload.version,
                Self::VERSION,
            ));
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlaneResyncPayload {
    pub version: u8,
    pub plane: crate::MemoryPlane,
    pub object_kinds: Vec<crate::ObjectKind>,
    pub reset_predictors: bool,
}

impl PlaneResyncPayload {
    pub const VERSION: u8 = 1;
    const KIND: &'static str = "plane resync payload";

    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("serialization failed: {error}"))
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        let (payload, _): (Self, usize) = bincode::serde::decode_from_slice(bytes, bincode::config::standard()).map_err(|error| {
            invalid_payload_error(Self::KIND, format!("decode failed: {error}"))
        })?;
        if payload.version != Self::VERSION {
            return Err(invalid_payload_version(
                Self::KIND,
                payload.version,
                Self::VERSION,
            ));
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictorEntryMeta {
    pub item_id: ItemId,
    pub source_kind: crate::SourceKind,
    pub object_kind: crate::ObjectKind,
    pub cue: crate::SparseCue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictorState {
    Empty,
    Ready(PredictorEntryMeta),
}

impl PredictorState {
    pub fn requires_raw(self) -> bool {
        matches!(self, Self::Empty)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamState {
    Opening,
    Established,
    RekeyPending { next_epoch_id: EpochId },
    Closed,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("reserved flag bits are set: 0x{0:04x}")]
    UnknownReservedFlagBits(u16),

    #[error("literal data record has invalid flag combination")]
    InvalidDataFlags,

    #[error("literal data payload length and flag combination are inconsistent")]
    InvalidDataPayloadContract,

    #[error("control records must not carry data codec metadata")]
    InvalidControlHeader,

    #[error("control record payload contract is invalid")]
    InvalidControlPayloadContract,

    #[error("source meta record header is invalid")]
    InvalidSourceMetaHeader,

    #[error("catalog sync or repair record header is invalid")]
    InvalidCatalogOrRepairHeader,

    #[error("predictive-memory record header is invalid")]
    InvalidPredictiveHeader,

    #[error("payload length mismatch: header={header_len}, actual={actual_len}")]
    PayloadLenMismatch { header_len: u32, actual_len: usize },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WireError {
    #[error("invalid payload: {0}")]
    InvalidPayload(String),

    #[error("unknown record type byte: {0}")]
    UnknownRecordType(u8),

    #[error("unknown codec mode byte: {0}")]
    UnknownCodecMode(u8),

    #[error("protected transport frame magic is invalid")]
    InvalidTransportFrameMagic,

    #[error("unsupported protected transport frame version: {0}")]
    UnsupportedTransportFrameVersion(u8),

    #[error(
        "invalid protected transport frame header length: expected {expected}, actual {actual}"
    )]
    InvalidTransportFrameHeaderLen { expected: usize, actual: usize },

    #[error("truncated protected transport frame: actual={actual}, minimum={minimum}")]
    TruncatedTransportFrame { actual: usize, minimum: usize },

    #[error(
        "protected transport frame payload length mismatch: expected {expected}, actual {actual}"
    )]
    TransportFramePayloadLenMismatch { expected: usize, actual: usize },

    #[error("truncated compact transport record: actual={actual}, minimum={minimum}")]
    TruncatedCompactTransportRecord { actual: usize, minimum: usize },

    #[error(
        "compact transport record payload length mismatch: expected {expected}, actual {actual}"
    )]
    CompactTransportRecordLenMismatch { expected: usize, actual: usize },

    #[error("invalid encoded header length: expected {expected}, actual {actual}")]
    InvalidHeaderLen { expected: usize, actual: usize },

    #[error("truncated record: actual={actual}, minimum={minimum}")]
    TruncatedRecord { actual: usize, minimum: usize },

    #[error("record length mismatch: expected {expected}, actual {actual}")]
    RecordLenMismatch { expected: usize, actual: usize },

    #[error("retired wire discriminant encountered: {0} (StateDef/StatePatch/BlockCatalogSync are retired)")]
    RetiredDiscriminant(u8),

    #[error(transparent)]
    Validation(#[from] ValidationError),
}

impl RecordType {
    pub const fn route_family(self) -> crate::RouteFamily {
        match self {
            Self::ExactState | Self::BatchEnvelope | Self::MemoryRetire | Self::MemoryAck => {
                crate::RouteFamily::DirectState
            }
            Self::PredictiveConfirm => crate::RouteFamily::PredictiveConfirm,
            Self::PredictiveCorrect => crate::RouteFamily::PredictiveCorrect,
            Self::TransformCorrect => crate::RouteFamily::DirectState,
            Self::AssemblyDef => crate::RouteFamily::DirectState,
            Self::TransformDef => crate::RouteFamily::DirectState,
            Self::SchemaDef => crate::RouteFamily::DirectState,
            Self::EpisodeHint => crate::RouteFamily::DirectState,
            Self::ReplayHint => crate::RouteFamily::Replay,
            Self::Rekey
            | Self::Resync
            | Self::Close
            | Self::SourceMeta
            | Self::Repair => crate::RouteFamily::DirectState,
        }
    }

    pub const fn object_kind(self) -> crate::ObjectKind {
        match self {
            Self::ExactState | Self::BatchEnvelope => crate::ObjectKind::ExactState,
            Self::MemoryRetire | Self::MemoryAck => crate::ObjectKind::PredictiveObject,
            Self::PredictiveConfirm | Self::PredictiveCorrect => crate::ObjectKind::SparseCue,
            Self::TransformCorrect => crate::ObjectKind::Transform,
            Self::AssemblyDef => crate::ObjectKind::Assembly,
            Self::TransformDef => crate::ObjectKind::Transform,
            Self::SchemaDef => crate::ObjectKind::Schema,
            Self::EpisodeHint => crate::ObjectKind::EpisodeHint,
            Self::ReplayHint => crate::ObjectKind::ReplayHint,
            Self::Rekey
            | Self::Resync
            | Self::Close
            | Self::SourceMeta
            | Self::Repair => crate::ObjectKind::SourceDescriptor,
        }
    }

    pub fn try_from_u8(value: u8) -> Result<Self, WireError> {
        match value {
            2 => Ok(Self::Rekey),
            3 => Ok(Self::Resync),
            4 => Ok(Self::Close),
            5 => Ok(Self::SourceMeta),
            6 | 7 | 8 => Err(WireError::RetiredDiscriminant(value)),
            9 => Ok(Self::Repair),
            10 => Ok(Self::PredictiveConfirm),
            11 => Ok(Self::PredictiveCorrect),
            12 => Ok(Self::AssemblyDef),
            13 => Ok(Self::TransformDef),
            14 => Ok(Self::SchemaDef),
            15 => Ok(Self::EpisodeHint),
            16 => Ok(Self::ReplayHint),
            17 => Ok(Self::ExactState),
            18 => Ok(Self::MemoryRetire),
            19 => Ok(Self::TransformCorrect),
            20 => Ok(Self::MemoryAck),
            21 => Ok(Self::BatchEnvelope),
            other => Err(WireError::UnknownRecordType(other)),
        }
    }
}

impl CodecMode {
    pub fn try_from_u8(value: u8) -> Result<Self, WireError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::PackedExact),
            2 => Ok(Self::PredictedExact),
            3 => Ok(Self::DirectExact),
            other => Err(WireError::UnknownCodecMode(other)),
        }
    }
}

fn validate_data_header(header: RecordHeader) -> Result<(), ValidationError> {
    let is_evict = header.flags.contains(RecordFlags::IS_EVICT);
    let is_invalidate = header.flags.contains(RecordFlags::IS_INVALIDATE);

    if is_evict && is_invalidate {
        return Err(ValidationError::InvalidDataFlags);
    }

    if is_evict || is_invalidate {
        if header.payload_len != 0
            || header.codec_mode != CodecMode::None
            || header.item_id == ItemId(0)
        {
            return Err(ValidationError::InvalidDataFlags);
        }
        return Ok(());
    }

    // Wave 13 T-13-a: BatchEnvelope records use item_id=0 (the batch is
    // identified by the record itself, not by a single item_id) and may
    // have an empty payload (empty batch). Skip the standard data-header
    // contract check for BatchEnvelope.
    if header.record_type == RecordType::BatchEnvelope {
        return Ok(());
    }

    let codec_ok = matches!(
        header.codec_mode,
        CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
    );
    if header.item_id == ItemId(0) || header.payload_len == 0 || !codec_ok {
        return Err(ValidationError::InvalidDataPayloadContract);
    }
    Ok(())
}

fn validate_control_header(header: RecordHeader) -> Result<(), ValidationError> {
    if header.codec_mode != CodecMode::None
        || header.item_id != ItemId(0)
        || (header.flags.bits() & !RecordFlags::POST_REKEY_CONFIRM) != 0
    {
        return Err(ValidationError::InvalidControlHeader);
    }

    match header.record_type {
        RecordType::Rekey => {
            if header.payload_len != REKEY_PAYLOAD_LEN as u32 {
                return Err(ValidationError::InvalidControlPayloadContract);
            }
        }
        RecordType::Resync => {
            if header.payload_len != 0 && header.payload_len < 4 {
                return Err(ValidationError::InvalidControlPayloadContract);
            }
        }
        RecordType::Close => {
            if header.payload_len != 0 && header.payload_len != CLOSE_CODE_LEN as u32 {
                return Err(ValidationError::InvalidControlPayloadContract);
            }
        }
        RecordType::SourceMeta
        | RecordType::Repair
        | RecordType::PredictiveConfirm
        | RecordType::PredictiveCorrect
        | RecordType::AssemblyDef
        | RecordType::TransformDef
        | RecordType::SchemaDef
        | RecordType::EpisodeHint
        | RecordType::ReplayHint
        | RecordType::ExactState
        | RecordType::BatchEnvelope
        | RecordType::MemoryRetire
        | RecordType::TransformCorrect
        | RecordType::MemoryAck => {
            unreachable!("control header validation only accepts rekey/resync/close records")
        }
    }

    Ok(())
}

fn validate_catalog_or_repair_header(header: RecordHeader) -> Result<(), ValidationError> {
    if header.codec_mode != CodecMode::None
        || header.flags.bits() != 0
        || header.item_id != ItemId(0)
        || header.payload_len == 0
    {
        return Err(ValidationError::InvalidCatalogOrRepairHeader);
    }
    Ok(())
}

fn validate_predictive_header(header: RecordHeader) -> Result<(), ValidationError> {
    match header.record_type {
        RecordType::ExactState => {
            let mut allowed_flags = RecordFlags::POST_REKEY_CONFIRM
                | RecordFlags::PAYLOAD_ZSTD
                | RecordFlags::PAYLOAD_ZSTD_DICT;
            if header.flags.contains(RecordFlags::DIRECT_STATE_FALLBACK) {
                allowed_flags |= RecordFlags::DIRECT_STATE_FALLBACK;
                if header.flags.contains(RecordFlags::SUBSTRATE_FALLBACK) {
                    allowed_flags |= RecordFlags::SUBSTRATE_FALLBACK;
                }
                // Allow TRANSFORM_DEMOTED_FALLBACK alongside DIRECT_STATE_FALLBACK
                if header.flags.contains(RecordFlags::TRANSFORM_DEMOTED_FALLBACK) {
                    allowed_flags |= RecordFlags::TRANSFORM_DEMOTED_FALLBACK;
                }
                // S3.4: Allow reason-specific fallback flags alongside DIRECT_STATE_FALLBACK
                if header.flags.contains(RecordFlags::DEPENDENCY_UNAVAILABLE_FALLBACK) {
                    allowed_flags |= RecordFlags::DEPENDENCY_UNAVAILABLE_FALLBACK;
                }
                if header.flags.contains(RecordFlags::ASSEMBLY_STRUCTURALIZATION_FALLBACK) {
                    allowed_flags |= RecordFlags::ASSEMBLY_STRUCTURALIZATION_FALLBACK;
                }
                if header.flags.contains(RecordFlags::SCHEMA_DEPENDENCY_INADMISSIBLE_FALLBACK) {
                    allowed_flags |= RecordFlags::SCHEMA_DEPENDENCY_INADMISSIBLE_FALLBACK;
                }
            }
            if header.flags.bits() & !allowed_flags != 0
                || header.item_id == ItemId(0)
                || header.payload_len == 0
                || !matches!(
                    header.codec_mode,
                    CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
                )
            {
                return Err(ValidationError::InvalidPredictiveHeader);
            }
            Ok(())
        }
        RecordType::PredictiveConfirm
        | RecordType::PredictiveCorrect
        | RecordType::AssemblyDef
        | RecordType::TransformDef
        | RecordType::SchemaDef
        | RecordType::EpisodeHint
        | RecordType::ReplayHint
        | RecordType::MemoryRetire
        | RecordType::TransformCorrect
        | RecordType::MemoryAck => {
            // P0: Allow PAYLOAD_ZSTD and PAYLOAD_ZSTD_DICT flags for compressed payloads.
            let allowed_flags = RecordFlags::POST_REKEY_CONFIRM
                | RecordFlags::PAYLOAD_ZSTD
                | RecordFlags::PAYLOAD_ZSTD_DICT;
            if header.flags.bits() & !allowed_flags != 0
                || header.item_id == ItemId(0)
                || header.codec_mode != CodecMode::None
            {
                return Err(ValidationError::InvalidPredictiveHeader);
            }
            let requires_payload = !matches!(
                header.record_type,
                RecordType::EpisodeHint | RecordType::ReplayHint
            );
            if requires_payload && header.payload_len == 0 {
                return Err(ValidationError::InvalidPredictiveHeader);
            }
            Ok(())
        }
        _ => Err(ValidationError::InvalidPredictiveHeader),
    }
}

fn validate_source_meta_header(header: RecordHeader) -> Result<(), ValidationError> {
    if header.codec_mode != CodecMode::None
        || header.flags.bits() != 0
        || header.item_id == ItemId(0)
        || header.payload_len == 0
    {
        return Err(ValidationError::InvalidSourceMetaHeader);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_data_header() -> RecordHeader {
        RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::DirectExact,
            flags: RecordFlags::empty(),
            item_id: ItemId(42),
            payload_len: 3,
        }
    }

    #[test]
    fn aad_layout_matches_spec_size() {
        assert_eq!(base_data_header().aad_bytes().len(), HEADER_LEN);
    }

    #[test]
    fn valid_data_header_passes_validation() {
        assert!(base_data_header().validate().is_ok());
    }

    #[test]
    fn invalid_data_flags_are_rejected() {
        let mut header = base_data_header();
        header.flags.insert(RecordFlags::IS_EVICT);
        assert_eq!(
            header.validate().unwrap_err(),
            ValidationError::InvalidDataFlags
        );
    }

    #[test]
    fn evict_header_with_zero_payload_is_valid() {
        let mut flags = RecordFlags::empty();
        flags.insert(RecordFlags::IS_EVICT);
        let header = RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::None,
            flags,
            item_id: ItemId(42),
            payload_len: 0,
        };
        assert!(header.validate().is_ok());
    }

    #[test]
    fn invalidate_header_with_zero_payload_is_valid() {
        let mut flags = RecordFlags::empty();
        flags.insert(RecordFlags::IS_INVALIDATE);
        let header = RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::None,
            flags,
            item_id: ItemId(42),
            payload_len: 0,
        };
        assert!(header.validate().is_ok());
    }

    #[test]
    fn rekey_requires_fixed_payload_len() {
        let header = RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::Rekey,
            codec_mode: CodecMode::None,
            flags: RecordFlags::empty(),
            item_id: ItemId(0),
            payload_len: 0,
        };
        assert_eq!(
            header.validate().unwrap_err(),
            ValidationError::InvalidControlPayloadContract
        );
    }

    #[test]
    fn source_meta_requires_non_empty_payload() {
        let header = RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::SourceMeta,
            codec_mode: CodecMode::None,
            flags: RecordFlags::empty(),
            item_id: ItemId(9),
            payload_len: 12,
        };
        assert!(header.validate().is_ok());
    }

    #[test]
    fn close_accepts_empty_or_two_byte_payloads() {
        let base = RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::Close,
            codec_mode: CodecMode::None,
            flags: RecordFlags::empty(),
            item_id: ItemId(0),
            payload_len: 0,
        };
        assert!(base.validate().is_ok());

        let with_reason = RecordHeader {
            payload_len: CLOSE_CODE_LEN as u32,
            ..base
        };
        assert!(with_reason.validate().is_ok());
    }

    #[test]
    fn from_aad_bytes_rejects_unknown_record_type() {
        let mut bytes = base_data_header().aad_bytes();
        bytes[21] = 255;
        let error = RecordHeader::from_aad_bytes(&bytes).unwrap_err();
        assert!(matches!(error, WireError::UnknownRecordType(255)));
    }

    #[test]
    fn from_aad_bytes_rejects_unknown_codec_mode() {
        let mut bytes = base_data_header().aad_bytes();
        bytes[22] = 255;
        let error = RecordHeader::from_aad_bytes(&bytes).unwrap_err();
        assert!(matches!(error, WireError::UnknownCodecMode(255)));
    }

    #[test]
    fn record_round_trip_preserves_fields() {
        let record = Record {
            header: base_data_header(),
            payload: vec![1, 2, 3],
            auth_tag: [9; AUTH_TAG_LEN],
        };
        let decoded = Record::from_bytes(&record.to_bytes()).unwrap();
        assert_eq!(decoded, record);
    }
}

pub fn encode_assembly_def_record(
    header: RecordHeader,
    payload: &crate::AssemblyDefPayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_assembly_def_record(
    record: &Record,
) -> Result<crate::AssemblyDefPayload, crate::StateProgramError> {
    crate::AssemblyDefPayload::decode(&record.payload)
}

pub fn encode_transform_def_record(
    header: RecordHeader,
    payload: &crate::TransformDefPayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_transform_def_record(
    record: &Record,
) -> Result<crate::TransformDefPayload, crate::StateProgramError> {
    crate::TransformDefPayload::decode(&record.payload)
}

pub fn encode_transform_instance_record(
    header: RecordHeader,
    payload: &crate::TransformInstancePayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_transform_instance_record(
    record: &Record,
) -> Result<crate::TransformInstancePayload, crate::StateProgramError> {
    crate::TransformInstancePayload::decode(&record.payload)
}

pub fn encode_episode_hint_record(
    header: RecordHeader,
    payload: &crate::EpisodeHintPayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_episode_hint_record(
    record: &Record,
) -> Result<crate::EpisodeHintPayload, crate::StateProgramError> {
    crate::EpisodeHintPayload::decode(&record.payload)
}

pub fn encode_replay_hint_record(
    header: RecordHeader,
    payload: &crate::EpisodeHintPayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_replay_hint_record(
    record: &Record,
) -> Result<crate::EpisodeHintPayload, crate::StateProgramError> {
    crate::EpisodeHintPayload::decode(&record.payload)
}

pub fn encode_schema_def_record(
    header: RecordHeader,
    payload: &crate::SchemaDefPayload,
) -> Result<Record, crate::StateProgramError> {
    let payload_bytes = payload.encode()?;
    let header = RecordHeader {
        payload_len: payload_bytes.len() as u32,
        ..header
    };
    Ok(Record {
        header,
        payload: payload_bytes,
        auth_tag: [0_u8; AUTH_TAG_LEN],
    })
}

pub fn decode_schema_def_record(
    record: &Record,
) -> Result<crate::SchemaDefPayload, crate::StateProgramError> {
    crate::SchemaDefPayload::decode(&record.payload)
}

#[cfg(test)]
mod p0_tests {
    use super::*;

    #[test]
    fn record_zstd_compress_decompress_round_trip() {
        // Create a PredictiveConfirm record with a compressible payload.
        // The payload must be long enough for zstd compression to beat the
        // original size (zstd adds ~10 bytes of framing overhead).
        let payload = b"{\"id\":1,\"step\":5,\"locality\":\"cache-hot\",\"bucket\":3,\"stable\":\"odd\",\"assembly\":{\"def_id\":1,\"body\":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]},\"route_graph\":{\"node_count\":3,\"output_len\":85}}"
            .to_vec();
        let record = Record {
            header: RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: SeqNo(1),
                record_type: RecordType::PredictiveConfirm,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0u8; AUTH_TAG_LEN],
        };

        let original_len = record.payload.len();
        let compressed = record.maybe_compress_zstd();

        // Compression should reduce the size for structured JSON-like data.
        assert!(compressed.header.flags.contains(RecordFlags::PAYLOAD_ZSTD));
        assert!(compressed.payload.len() < original_len);
        assert_eq!(compressed.header.payload_len as usize, compressed.payload.len());

        // Decompress and verify round-trip.
        let decompressed = compressed.maybe_decompress_zstd().unwrap();
        assert!(!decompressed.header.flags.contains(RecordFlags::PAYLOAD_ZSTD));
        assert_eq!(decompressed.payload.len(), original_len);
        assert_eq!(
            decompressed.payload,
            b"{\"id\":1,\"step\":5,\"locality\":\"cache-hot\",\"bucket\":3,\"stable\":\"odd\",\"assembly\":{\"def_id\":1,\"body\":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]},\"route_graph\":{\"node_count\":3,\"output_len\":85}}"
                .to_vec()
        );
    }

    #[test]
    fn record_zstd_skips_small_payloads() {
        let payload = b"hi".to_vec();
        let record = Record {
            header: RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: SeqNo(1),
                record_type: RecordType::PredictiveConfirm,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0u8; AUTH_TAG_LEN],
        };

        let compressed = record.maybe_compress_zstd();
        // Small payloads should not be compressed.
        assert!(!compressed.header.flags.contains(RecordFlags::PAYLOAD_ZSTD));
    }

    #[test]
    fn record_zstd_decompress_unchanged_record() {
        let payload = b"hello world, this is a test that is long enough to be compressed by zstd for sure".to_vec();
        let record = Record {
            header: RecordHeader {
                version: PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: SeqNo(1),
                record_type: RecordType::PredictiveConfirm,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0u8; AUTH_TAG_LEN],
        };

        // A record without PAYLOAD_ZSTD flag should be returned unchanged.
        let result = record.clone().maybe_decompress_zstd().unwrap();
        assert_eq!(result.payload, record.payload);
    }
}
