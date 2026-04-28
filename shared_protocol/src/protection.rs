use chacha20poly1305::{
    ChaCha20Poly1305, Nonce, Tag,
    aead::{AeadInPlace, KeyInit},
};
use hkdf::Hkdf;
use rand_core::{CryptoRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
use ml_kem::{B32, FromSeed, MlKem768, Seed, kem::Decapsulate};
#[cfg(not(target_arch = "wasm32"))]
use x25519_dalek::{PublicKey, StaticSecret};

use crate::protocol::{
    AUTH_TAG_LEN, CLOSE_CODE_LEN, EpochId, REKEY_PAYLOAD_LEN, REKEY_SEED_LEN, Record, RecordFlags,
    RecordType, SeqNo, StreamId, StreamState, ValidationError,
};
use crate::transport::{
    ProtectedTransportFrameHeader, decode_compact_transport_records,
    decode_protected_transport_frame, encode_compact_transport_records,
    encode_protected_transport_frame,
};

const ROOT_LEN: usize = 32;
const NONCE_LEN: usize = 12;
pub const STREAM_ROOT_LEN: usize = ROOT_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProtectionProfileKind {
    ClassicRef1,
    #[serde(rename = "pq_simple_v1")]
    PqSimpleV1,
    #[serde(rename = "pq_simple_dgram_v1")]
    PqSimpleDgramV1,
    #[default]
    PqMutualV1,
    #[serde(rename = "pq_mutual_dgram_v1")]
    PqMutualDgramV1,
}

pub const DEFAULT_PROTECTION_PROFILE_KIND: ProtectionProfileKind =
    ProtectionProfileKind::PqMutualV1;

impl ProtectionProfileKind {
    pub fn slug(self) -> &'static str {
        match self {
            Self::ClassicRef1 => "classic_ref1",
            Self::PqSimpleV1 => "pq_simple_v1",
            Self::PqSimpleDgramV1 => "pq_simple_dgram_v1",
            Self::PqMutualV1 => "pq_mutual_v1",
            Self::PqMutualDgramV1 => "pq_mutual_dgram_v1",
        }
    }

    pub fn is_simple_family(self) -> bool {
        matches!(self, Self::PqSimpleV1 | Self::PqSimpleDgramV1)
    }

    pub fn is_mutual_family(self) -> bool {
        matches!(self, Self::PqMutualV1 | Self::PqMutualDgramV1)
    }

    pub fn is_datagram_family(self) -> bool {
        matches!(self, Self::PqSimpleDgramV1 | Self::PqMutualDgramV1)
    }

    pub fn canonical_stream_profile(self) -> Self {
        match self {
            Self::PqSimpleDgramV1 => Self::PqSimpleV1,
            Self::PqMutualDgramV1 => Self::PqMutualV1,
            other => other,
        }
    }

    pub fn datagram_variant(self) -> Self {
        match self {
            Self::PqSimpleV1 | Self::PqSimpleDgramV1 => Self::PqSimpleDgramV1,
            Self::PqMutualV1 | Self::PqMutualDgramV1 => Self::PqMutualDgramV1,
            Self::ClassicRef1 => Self::ClassicRef1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamDirection {
    ClientToServer,
    ServerToClient,
}

impl StreamDirection {
    fn root_label(self) -> &'static [u8] {
        match self {
            Self::ClientToServer => b"root/c2s/",
            Self::ServerToClient => b"root/s2c/",
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn epoch0_label(self) -> &'static [u8] {
        match self {
            Self::ClientToServer => b"root/c2s/epoch0",
            Self::ServerToClient => b"root/s2c/epoch0",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RekeyPayload {
    pub next_epoch_id: EpochId,
    pub rekey_seed: [u8; REKEY_SEED_LEN],
}

impl RekeyPayload {
    pub fn encode(self) -> [u8; REKEY_PAYLOAD_LEN] {
        let mut out = [0_u8; REKEY_PAYLOAD_LEN];
        out[..4].copy_from_slice(&self.next_epoch_id.0.to_le_bytes());
        out[4..].copy_from_slice(&self.rekey_seed);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtectionError> {
        if bytes.len() != REKEY_PAYLOAD_LEN {
            return Err(ProtectionError::InvalidRekeyPayload {
                actual_len: bytes.len(),
            });
        }

        let mut rekey_seed = [0_u8; REKEY_SEED_LEN];
        rekey_seed.copy_from_slice(&bytes[4..]);
        Ok(Self {
            next_epoch_id: EpochId(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
            rekey_seed,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingRekey {
    next_epoch_id: EpochId,
    next_root: [u8; ROOT_LEN],
}

#[derive(Debug, Clone)]
pub struct StreamProtector {
    kind: ProtectionProfileKind,
    stream_id: StreamId,
    direction: StreamDirection,
    stream_state: StreamState,
    current_epoch_id: EpochId,
    next_seq_no: SeqNo,
    current_root: [u8; ROOT_LEN],
    pending_rekey: Option<PendingRekey>,
}

impl StreamProtector {
    fn new(
        kind: ProtectionProfileKind,
        stream_id: StreamId,
        direction: StreamDirection,
        current_root: [u8; ROOT_LEN],
    ) -> Self {
        Self {
            kind,
            stream_id,
            direction,
            stream_state: StreamState::Opening,
            current_epoch_id: EpochId(0),
            next_seq_no: SeqNo(0),
            current_root,
            pending_rekey: None,
        }
    }

    pub fn from_bootstrap_root(
        kind: ProtectionProfileKind,
        stream_id: StreamId,
        direction: StreamDirection,
        current_root: [u8; ROOT_LEN],
    ) -> Self {
        Self::new(kind, stream_id, direction, current_root)
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn direction(&self) -> StreamDirection {
        self.direction
    }

    pub fn stream_state(&self) -> StreamState {
        self.stream_state
    }

    pub fn current_epoch_id(&self) -> EpochId {
        self.current_epoch_id
    }

    pub fn expected_epoch_id(&self) -> EpochId {
        self.pending_rekey
            .map(|pending| pending.next_epoch_id)
            .unwrap_or(self.current_epoch_id)
    }

    pub fn expected_seq_no(&self) -> SeqNo {
        if self.pending_rekey.is_some() {
            SeqNo(0)
        } else {
            self.next_seq_no
        }
    }

    pub fn post_rekey_confirm_required(&self) -> bool {
        self.pending_rekey.is_some()
    }

    pub fn bootstrap_root(&self) -> [u8; ROOT_LEN] {
        self.current_root
    }

    pub fn make_rekey_payload<R>(&self, rng: &mut R) -> RekeyPayload
    where
        R: CryptoRng + RngCore + ?Sized,
    {
        let mut rekey_seed = [0_u8; REKEY_SEED_LEN];
        rng.fill_bytes(&mut rekey_seed);
        RekeyPayload {
            next_epoch_id: EpochId(self.current_epoch_id.0 + 1),
            rekey_seed,
        }
    }

    pub fn protect_record(&mut self, record: Record) -> Result<Record, ProtectionError> {
        if matches!(self.stream_state, StreamState::Closed) {
            return Err(ProtectionError::StreamClosed);
        }

        let mut record = record.validate().map_err(ProtectionError::Validation)?;
        record = self.prepare_record_for_epoch(record)?;
        let active_root = self.active_root_for(&record.header)?;
        let material =
            derive_record_material(&active_root, record.header.epoch_id, record.header.seq_no);
        let aad = record.header.aad_bytes();
        let plaintext = record.payload.clone();
        let (ciphertext, auth_tag) =
            seal_payload(&record.payload, &material.key, &material.nonce, &aad)
                .map_err(|_| ProtectionError::AuthenticationFailure)?;

        let protected = Record {
            header: record.header,
            payload: ciphertext,
            auth_tag,
        };
        self.finish_success(&protected.header, &plaintext, material.next_root)?;
        Ok(protected)
    }

    pub fn unprotect_record(&mut self, record: Record) -> Result<Record, ProtectionError> {
        if matches!(self.stream_state, StreamState::Closed) {
            return Err(ProtectionError::StreamClosed);
        }

        let record = match record.validate() {
            Ok(record) => record,
            Err(error) => return self.fail(ProtectionError::Validation(error)),
        };
        let active_root = match self.active_root_for(&record.header) {
            Ok(active_root) => active_root,
            Err(error) => return self.fail(error),
        };
        let material =
            derive_record_material(&active_root, record.header.epoch_id, record.header.seq_no);
        let aad = record.header.aad_bytes();
        let plaintext = match open_payload(
            &record.payload,
            &record.auth_tag,
            &material.key,
            &material.nonce,
            &aad,
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => return self.fail(ProtectionError::AuthenticationFailure),
        };

        let unprotected = Record {
            header: record.header,
            payload: plaintext.clone(),
            auth_tag: record.auth_tag,
        };
        if let Err(error) = self.finish_success(&unprotected.header, &plaintext, material.next_root)
        {
            return self.fail(error);
        }
        Ok(unprotected)
    }

    pub fn protect_transport_records<I>(&mut self, records: I) -> Result<Vec<u8>, ProtectionError>
    where
        I: IntoIterator<Item = Record>,
    {
        if matches!(self.stream_state, StreamState::Closed) {
            return Err(ProtectionError::StreamClosed);
        }

        let mut working = self.clone();
        let mut prepared = Vec::new();
        let mut frame_root = None;
        let mut frame_header = None;

        for record in records {
            let record = record.validate().map_err(ProtectionError::Validation)?;
            let record = working.prepare_record_for_epoch(record)?;
            let active_root = working.active_root_for(&record.header)?;
            if frame_root.is_none() {
                frame_root = Some(active_root);
                frame_header = Some(ProtectedTransportFrameHeader {
                    epoch_id: record.header.epoch_id,
                    seq_no: record.header.seq_no,
                    record_count: 0,
                    payload_len: 0,
                });
            }

            let material =
                derive_record_material(&active_root, record.header.epoch_id, record.header.seq_no);
            working.finish_success(&record.header, &record.payload, material.next_root)?;
            prepared.push(record);
        }

        if prepared.is_empty() {
            return Err(ProtectionError::EmptyTransportFrame);
        }

        let payload = encode_compact_transport_records(&prepared);
        let mut header = frame_header.expect("non-empty frame has a header");
        header.record_count = prepared.len() as u32;
        header.payload_len = payload.len() as u32;
        let material = derive_frame_material(
            &frame_root.expect("non-empty frame has a frame root"),
            header.epoch_id,
            header.seq_no,
            header.record_count,
        );
        let aad = header.aad_bytes();
        let (ciphertext, auth_tag) = seal_payload(&payload, &material.key, &material.nonce, &aad)
            .map_err(|_| ProtectionError::AuthenticationFailure)?;
        *self = working;
        Ok(encode_protected_transport_frame(
            header,
            &ciphertext,
            &auth_tag,
        ))
    }

    pub fn unprotect_transport_frame(
        &mut self,
        frame: &[u8],
    ) -> Result<Vec<Record>, ProtectionError> {
        if matches!(self.stream_state, StreamState::Closed) {
            return Err(ProtectionError::StreamClosed);
        }

        let (header, ciphertext, auth_tag) = match decode_protected_transport_frame(frame) {
            Ok(decoded) => decoded,
            Err(error) => return self.fail(ProtectionError::Wire(error)),
        };
        let frame_root = match self.active_root_for_transport_frame(header) {
            Ok(root) => root,
            Err(error) => return self.fail(error),
        };
        let material = derive_frame_material(
            &frame_root,
            header.epoch_id,
            header.seq_no,
            header.record_count,
        );
        let aad = header.aad_bytes();
        let plaintext =
            match open_payload(ciphertext, &auth_tag, &material.key, &material.nonce, &aad) {
                Ok(plaintext) => plaintext,
                Err(_) => return self.fail(ProtectionError::AuthenticationFailure),
            };
        let records = match decode_compact_transport_records(
            &plaintext,
            self.stream_id,
            header.epoch_id,
            header.seq_no,
        ) {
            Ok(records) => records,
            Err(error) => return self.fail(ProtectionError::Wire(error)),
        };
        if records.len() != header.record_count as usize {
            return self.fail(ProtectionError::TransportFrameRecordCountMismatch {
                expected: header.record_count,
                actual: records.len(),
            });
        }

        let mut working = self.clone();
        let mut output = Vec::with_capacity(records.len());
        for record in records {
            let record = match record.validate() {
                Ok(record) => record,
                Err(error) => return self.fail(ProtectionError::Validation(error)),
            };
            let active_root = match working.active_root_for(&record.header) {
                Ok(root) => root,
                Err(error) => return self.fail(error),
            };
            let material =
                derive_record_material(&active_root, record.header.epoch_id, record.header.seq_no);
            if let Err(error) =
                working.finish_success(&record.header, &record.payload, material.next_root)
            {
                return self.fail(error);
            }
            output.push(record);
        }

        *self = working;
        Ok(output)
    }

    fn prepare_record_for_epoch(&mut self, mut record: Record) -> Result<Record, ProtectionError> {
        if record.header.stream_id != self.stream_id {
            return Err(ProtectionError::UnexpectedStreamId {
                expected: self.stream_id,
                actual: record.header.stream_id,
            });
        }

        if self.pending_rekey.is_some() {
            if !record
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
            {
                record.header.flags.insert(RecordFlags::POST_REKEY_CONFIRM);
            }
        } else if record
            .header
            .flags
            .contains(RecordFlags::POST_REKEY_CONFIRM)
        {
            return Err(ProtectionError::UnexpectedPostRekeyConfirm);
        }

        let expected_epoch = self.expected_epoch_id();
        if record.header.epoch_id != expected_epoch {
            return Err(ProtectionError::UnexpectedEpoch {
                expected: expected_epoch,
                actual: record.header.epoch_id,
            });
        }

        let expected_seq = self.expected_seq_no();
        if record.header.seq_no != expected_seq {
            return Err(ProtectionError::UnexpectedSeqNo {
                expected: expected_seq,
                actual: record.header.seq_no,
            });
        }

        Ok(record)
    }

    fn active_root_for(
        &self,
        header: &crate::protocol::RecordHeader,
    ) -> Result<[u8; ROOT_LEN], ProtectionError> {
        if header.stream_id != self.stream_id {
            return Err(ProtectionError::UnexpectedStreamId {
                expected: self.stream_id,
                actual: header.stream_id,
            });
        }

        if let Some(pending) = self.pending_rekey {
            if !header.flags.contains(RecordFlags::POST_REKEY_CONFIRM) {
                return Err(ProtectionError::MissingPostRekeyConfirm);
            }
            if header.epoch_id != pending.next_epoch_id {
                return Err(ProtectionError::UnexpectedEpoch {
                    expected: pending.next_epoch_id,
                    actual: header.epoch_id,
                });
            }
            if header.seq_no != SeqNo(0) {
                return Err(ProtectionError::UnexpectedSeqNo {
                    expected: SeqNo(0),
                    actual: header.seq_no,
                });
            }
            return Ok(pending.next_root);
        }

        if header.flags.contains(RecordFlags::POST_REKEY_CONFIRM) {
            return Err(ProtectionError::UnexpectedPostRekeyConfirm);
        }

        if header.epoch_id != self.current_epoch_id {
            return Err(ProtectionError::UnexpectedEpoch {
                expected: self.current_epoch_id,
                actual: header.epoch_id,
            });
        }
        if header.seq_no != self.next_seq_no {
            return Err(ProtectionError::UnexpectedSeqNo {
                expected: self.next_seq_no,
                actual: header.seq_no,
            });
        }
        Ok(self.current_root)
    }

    fn active_root_for_transport_frame(
        &self,
        header: ProtectedTransportFrameHeader,
    ) -> Result<[u8; ROOT_LEN], ProtectionError> {
        if header.record_count == 0 {
            return Err(ProtectionError::EmptyTransportFrame);
        }

        if let Some(pending) = self.pending_rekey {
            if header.epoch_id != pending.next_epoch_id {
                return Err(ProtectionError::UnexpectedEpoch {
                    expected: pending.next_epoch_id,
                    actual: header.epoch_id,
                });
            }
            if header.seq_no != SeqNo(0) {
                return Err(ProtectionError::UnexpectedSeqNo {
                    expected: SeqNo(0),
                    actual: header.seq_no,
                });
            }
            return Ok(pending.next_root);
        }

        if header.epoch_id != self.current_epoch_id {
            return Err(ProtectionError::UnexpectedEpoch {
                expected: self.current_epoch_id,
                actual: header.epoch_id,
            });
        }
        if header.seq_no != self.next_seq_no {
            return Err(ProtectionError::UnexpectedSeqNo {
                expected: self.next_seq_no,
                actual: header.seq_no,
            });
        }
        Ok(self.current_root)
    }

    fn finish_success(
        &mut self,
        header: &crate::protocol::RecordHeader,
        plaintext: &[u8],
        next_root: [u8; ROOT_LEN],
    ) -> Result<(), ProtectionError> {
        let was_pending = self.pending_rekey.take();
        if let Some(pending) = was_pending {
            self.current_epoch_id = pending.next_epoch_id;
            self.current_root = next_root;
            self.next_seq_no = SeqNo(header.seq_no.0 + 1);
            self.stream_state = StreamState::Established;
        } else {
            self.current_root = next_root;
            self.next_seq_no = SeqNo(header.seq_no.0 + 1);
            if !matches!(self.stream_state, StreamState::Closed) {
                self.stream_state = StreamState::Established;
            }
        }

        match header.record_type {
            RecordType::Rekey => {
                self.install_rekey(self.current_epoch_id, plaintext)?;
            }
            RecordType::Close => {
                if !(plaintext.is_empty() || plaintext.len() == CLOSE_CODE_LEN) {
                    return Err(ProtectionError::InvalidClosePayload {
                        actual_len: plaintext.len(),
                    });
                }
                self.stream_state = StreamState::Closed;
            }
            RecordType::ExactState
            | RecordType::Resync
            | RecordType::SourceMeta
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
            | RecordType::MemoryAck => {}
        }

        Ok(())
    }

    fn install_rekey(
        &mut self,
        current_epoch_id: EpochId,
        plaintext: &[u8],
    ) -> Result<(), ProtectionError> {
        if self.pending_rekey.is_some() {
            return Err(ProtectionError::RekeyAlreadyPending);
        }

        let payload = RekeyPayload::decode(plaintext)?;
        if payload.next_epoch_id.0 != current_epoch_id.0 + 1 {
            return Err(ProtectionError::InvalidNextEpoch {
                current: current_epoch_id,
                next: payload.next_epoch_id,
            });
        }

        let next_root = derive_rekey_root(
            self.direction,
            &self.current_root,
            current_epoch_id,
            payload.next_epoch_id,
            &payload.rekey_seed,
        );
        self.pending_rekey = Some(PendingRekey {
            next_epoch_id: payload.next_epoch_id,
            next_root,
        });
        self.stream_state = StreamState::RekeyPending {
            next_epoch_id: payload.next_epoch_id,
        };
        Ok(())
    }

    fn fail<T>(&mut self, error: ProtectionError) -> Result<T, ProtectionError> {
        self.pending_rekey = None;
        self.stream_state = StreamState::Closed;
        Err(error)
    }
}

#[derive(Debug, Clone, Copy)]
struct RecordMaterial {
    key: [u8; ROOT_LEN],
    nonce: [u8; NONCE_LEN],
    next_root: [u8; ROOT_LEN],
}

#[derive(Debug, Clone, Copy)]
struct FrameMaterial {
    key: [u8; ROOT_LEN],
    nonce: [u8; NONCE_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtectionError {
    #[error(transparent)]
    Validation(ValidationError),
    #[error(transparent)]
    Wire(#[from] crate::WireError),
    #[error("stream id mismatch: expected {expected:?}, actual {actual:?}")]
    UnexpectedStreamId {
        expected: StreamId,
        actual: StreamId,
    },
    #[error("sequence number mismatch: expected {expected:?}, actual {actual:?}")]
    UnexpectedSeqNo { expected: SeqNo, actual: SeqNo },
    #[error("epoch mismatch: expected {expected:?}, actual {actual:?}")]
    UnexpectedEpoch { expected: EpochId, actual: EpochId },
    #[error("authentication failure")]
    AuthenticationFailure,
    #[error("profile is not configured")]
    MissingProfile,
    #[error("stream is closed")]
    StreamClosed,
    #[error("rekey payload length is invalid: {actual_len}")]
    InvalidRekeyPayload { actual_len: usize },
    #[error("close payload length is invalid: {actual_len}")]
    InvalidClosePayload { actual_len: usize },
    #[error("next epoch must increment by one: current={current:?}, next={next:?}")]
    InvalidNextEpoch { current: EpochId, next: EpochId },
    #[error("rekey is already pending")]
    RekeyAlreadyPending,
    #[error("first record of a new epoch is missing POST_REKEY_CONFIRM")]
    MissingPostRekeyConfirm,
    #[error("POST_REKEY_CONFIRM appeared outside of a rekey confirmation")]
    UnexpectedPostRekeyConfirm,
    #[error("transport frame carried no records")]
    EmptyTransportFrame,
    #[error("transport frame record count mismatch: expected {expected}, actual {actual}")]
    TransportFrameRecordCountMismatch { expected: u32, actual: usize },
}

pub trait ProtectionProfile {
    fn kind(&self) -> ProtectionProfileKind;
    fn protect(&mut self, record: Record) -> Result<Record, ProtectionError>;
    fn unprotect(&mut self, record: Record) -> Result<Record, ProtectionError>;
}

impl ProtectionProfile for StreamProtector {
    fn kind(&self) -> ProtectionProfileKind {
        self.kind
    }

    fn protect(&mut self, record: Record) -> Result<Record, ProtectionError> {
        self.protect_record(record)
    }

    fn unprotect(&mut self, record: Record) -> Result<Record, ProtectionError> {
        self.unprotect_record(record)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn classic_ref1_pair_from_static_secrets(
    stream_id: StreamId,
    direction: StreamDirection,
    client_secret_bytes: [u8; 32],
    server_secret_bytes: [u8; 32],
) -> (StreamProtector, StreamProtector) {
    let client_secret = StaticSecret::from(client_secret_bytes);
    let server_secret = StaticSecret::from(server_secret_bytes);
    let client_public = PublicKey::from(&client_secret);
    let server_public = PublicKey::from(&server_secret);
    let client_shared = client_secret.diffie_hellman(&server_public);
    let server_shared = server_secret.diffie_hellman(&client_public);
    let client_bytes = client_shared.to_bytes();
    let server_bytes = server_shared.to_bytes();
    assert_eq!(client_bytes, server_bytes, "classic bootstrap must agree");

    let root = derive_epoch0_root(direction, stream_id, &client_bytes);
    let sender = StreamProtector::new(
        ProtectionProfileKind::ClassicRef1,
        stream_id,
        direction,
        root,
    );
    let receiver = StreamProtector::new(
        ProtectionProfileKind::ClassicRef1,
        stream_id,
        direction,
        root,
    );
    (sender, receiver)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn classic_ref1_pair_from_rng<R>(
    stream_id: StreamId,
    direction: StreamDirection,
    rng: &mut R,
) -> (StreamProtector, StreamProtector)
where
    R: CryptoRng + RngCore + ?Sized,
{
    let client_secret = StaticSecret::random_from_rng(&mut *rng);
    let server_secret = StaticSecret::random_from_rng(&mut *rng);
    classic_ref1_pair_from_static_secrets(
        stream_id,
        direction,
        client_secret.to_bytes(),
        server_secret.to_bytes(),
    )
}

#[cfg(not(target_arch = "wasm32"))]
pub fn pq_simple_v1_pair_from_rng<R>(
    stream_id: StreamId,
    direction: StreamDirection,
    rng: &mut R,
) -> (StreamProtector, StreamProtector)
where
    R: CryptoRng + RngCore + ?Sized,
{
    let mut seed_bytes = [0_u8; 64];
    rng.fill_bytes(&mut seed_bytes);
    let seed = Seed::from(seed_bytes);
    let (decapsulation_key, encapsulation_key) = MlKem768::from_seed(&seed);

    let mut sender_message_bytes = [0_u8; 32];
    rng.fill_bytes(&mut sender_message_bytes);
    let sender_message = B32::from(sender_message_bytes);
    let (ciphertext, sender_shared) = encapsulation_key.encapsulate_deterministic(&sender_message);
    let receiver_shared = decapsulation_key.decapsulate(&ciphertext);

    let mut sender_bytes = [0_u8; ROOT_LEN];
    sender_bytes.copy_from_slice(sender_shared.as_slice());
    let mut receiver_bytes = [0_u8; ROOT_LEN];
    receiver_bytes.copy_from_slice(receiver_shared.as_slice());
    assert_eq!(sender_bytes, receiver_bytes, "pq bootstrap must agree");

    let root = derive_epoch0_root(direction, stream_id, &sender_bytes);
    let sender = StreamProtector::new(
        ProtectionProfileKind::PqSimpleV1,
        stream_id,
        direction,
        root,
    );
    let receiver = StreamProtector::new(
        ProtectionProfileKind::PqSimpleV1,
        stream_id,
        direction,
        root,
    );
    (sender, receiver)
}

#[cfg(not(target_arch = "wasm32"))]
fn derive_epoch0_root(
    direction: StreamDirection,
    stream_id: StreamId,
    shared_secret: &[u8],
) -> [u8; ROOT_LEN] {
    let salt = concatenate_parts(&[b"pulzz-v1", &stream_id.0.to_le_bytes()]);
    let hkdf = Hkdf::<Sha256>::new(Some(&salt), shared_secret);
    let mut root = [0_u8; ROOT_LEN];
    hkdf.expand(direction.epoch0_label(), &mut root)
        .expect("hkdf expand for root should succeed");
    root
}

fn derive_record_material(
    root: &[u8; ROOT_LEN],
    epoch_id: EpochId,
    seq_no: SeqNo,
) -> RecordMaterial {
    let hkdf = Hkdf::<Sha256>::from_prk(root).expect("32-byte root is a valid HKDF PRK");
    let epoch_le = epoch_id.0.to_le_bytes();
    let seq_le = seq_no.0.to_le_bytes();
    let mut key = [0_u8; ROOT_LEN];
    let mut nonce = [0_u8; NONCE_LEN];
    let mut next_root = [0_u8; ROOT_LEN];
    hkdf.expand(&concatenate_parts(&[b"key", &epoch_le, &seq_le]), &mut key)
        .expect("hkdf expand for key should succeed");
    hkdf.expand(
        &concatenate_parts(&[b"nonce", &epoch_le, &seq_le]),
        &mut nonce,
    )
    .expect("hkdf expand for nonce should succeed");
    hkdf.expand(
        &concatenate_parts(&[b"next", &epoch_le, &seq_le]),
        &mut next_root,
    )
    .expect("hkdf expand for next root should succeed");
    RecordMaterial {
        key,
        nonce,
        next_root,
    }
}

fn derive_frame_material(
    root: &[u8; ROOT_LEN],
    epoch_id: EpochId,
    seq_no: SeqNo,
    record_count: u32,
) -> FrameMaterial {
    let hkdf = Hkdf::<Sha256>::from_prk(root).expect("32-byte root is a valid HKDF PRK");
    let epoch_le = epoch_id.0.to_le_bytes();
    let seq_le = seq_no.0.to_le_bytes();
    let count_le = record_count.to_le_bytes();
    let mut key = [0_u8; ROOT_LEN];
    let mut nonce = [0_u8; NONCE_LEN];
    hkdf.expand(
        &concatenate_parts(&[b"frame/key", &epoch_le, &seq_le, &count_le]),
        &mut key,
    )
    .expect("hkdf expand for frame key should succeed");
    hkdf.expand(
        &concatenate_parts(&[b"frame/nonce", &epoch_le, &seq_le, &count_le]),
        &mut nonce,
    )
    .expect("hkdf expand for frame nonce should succeed");
    FrameMaterial { key, nonce }
}

fn derive_rekey_root(
    direction: StreamDirection,
    current_root: &[u8; ROOT_LEN],
    current_epoch_id: EpochId,
    next_epoch_id: EpochId,
    rekey_seed: &[u8; REKEY_SEED_LEN],
) -> [u8; ROOT_LEN] {
    let salt = concatenate_parts(&[b"rekey", &current_epoch_id.0.to_le_bytes()]);
    let ikm = concatenate_parts(&[current_root, rekey_seed]);
    let hkdf = Hkdf::<Sha256>::new(Some(&salt), &ikm);
    let info = concatenate_parts(&[direction.root_label(), &next_epoch_id.0.to_le_bytes()]);
    let mut next_root = [0_u8; ROOT_LEN];
    hkdf.expand(&info, &mut next_root)
        .expect("hkdf expand for rekey root should succeed");
    next_root
}

fn seal_payload(
    plaintext: &[u8],
    key: &[u8; ROOT_LEN],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
) -> Result<(Vec<u8>, [u8; AUTH_TAG_LEN]), chacha20poly1305::aead::Error> {
    let cipher = ChaCha20Poly1305::new_from_slice(key).expect("32-byte key is valid");
    let mut payload = plaintext.to_vec();
    let tag = cipher.encrypt_in_place_detached(Nonce::from_slice(nonce), aad, &mut payload)?;
    let mut auth_tag = [0_u8; AUTH_TAG_LEN];
    auth_tag.copy_from_slice(tag.as_slice());
    Ok((payload, auth_tag))
}

fn open_payload(
    ciphertext: &[u8],
    auth_tag: &[u8; AUTH_TAG_LEN],
    key: &[u8; ROOT_LEN],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
) -> Result<Vec<u8>, chacha20poly1305::aead::Error> {
    let cipher = ChaCha20Poly1305::new_from_slice(key).expect("32-byte key is valid");
    let mut payload = ciphertext.to_vec();
    cipher.decrypt_in_place_detached(
        Nonce::from_slice(nonce),
        aad,
        &mut payload,
        Tag::from_slice(auth_tag),
    )?;
    Ok(payload)
}

fn concatenate_parts(parts: &[&[u8]]) -> Vec<u8> {
    let total_len = parts.iter().map(|part| part.len()).sum();
    let mut out = Vec::with_capacity(total_len);
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;
    use crate::protocol::{CodecMode, RecordHeader};

    fn data_record(
        stream_id: StreamId,
        epoch_id: EpochId,
        seq_no: SeqNo,
        payload: &[u8],
    ) -> Record {
        Record {
            header: RecordHeader {
                version: crate::protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id,
                seq_no,
                record_type: RecordType::ExactState,
                codec_mode: CodecMode::DirectExact,
                flags: RecordFlags::empty(),
                item_id: crate::protocol::ItemId(7),
                payload_len: payload.len() as u32,
            },
            payload: payload.to_vec(),
            auth_tag: [0; AUTH_TAG_LEN],
        }
    }

    fn rekey_record(
        stream_id: StreamId,
        epoch_id: EpochId,
        seq_no: SeqNo,
        payload: RekeyPayload,
    ) -> Record {
        Record {
            header: RecordHeader {
                version: crate::protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id,
                seq_no,
                record_type: RecordType::Rekey,
                codec_mode: crate::protocol::CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: crate::protocol::ItemId(0),
                payload_len: REKEY_PAYLOAD_LEN as u32,
            },
            payload: payload.encode().to_vec(),
            auth_tag: [0; AUTH_TAG_LEN],
        }
    }

    fn close_record(stream_id: StreamId, epoch_id: EpochId, seq_no: SeqNo) -> Record {
        Record {
            header: RecordHeader {
                version: crate::protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id,
                seq_no,
                record_type: RecordType::Close,
                codec_mode: crate::protocol::CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: crate::protocol::ItemId(0),
                payload_len: 0,
            },
            payload: Vec::new(),
            auth_tag: [0; AUTH_TAG_LEN],
        }
    }

    #[test]
    fn classic_profile_round_trips() {
        let mut rng = StdRng::seed_from_u64(9);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(11), StreamDirection::ServerToClient, &mut rng);
        let protected = sender
            .protect_record(data_record(StreamId(11), EpochId(0), SeqNo(0), b"alpha"))
            .unwrap();
        let recovered = receiver.unprotect_record(protected).unwrap();
        assert_eq!(recovered.payload, b"alpha");
        assert_eq!(receiver.stream_state(), StreamState::Established);
    }

    #[test]
    fn pq_profile_round_trips() {
        let mut rng = StdRng::seed_from_u64(19);
        let (mut sender, mut receiver) =
            pq_simple_v1_pair_from_rng(StreamId(12), StreamDirection::ServerToClient, &mut rng);
        let protected = sender
            .protect_record(data_record(StreamId(12), EpochId(0), SeqNo(0), b"beta"))
            .unwrap();
        let recovered = receiver.unprotect_record(protected).unwrap();
        assert_eq!(recovered.payload, b"beta");
        assert_eq!(receiver.stream_state(), StreamState::Established);
    }

    #[test]
    fn pq_simple_profile_serializes_canonically() {
        let canonical = serde_json::to_string(&ProtectionProfileKind::PqSimpleV1).unwrap();
        assert_eq!(canonical, "\"pq_simple_v1\"");
    }

    #[test]
    fn unexpected_sequence_fails_closed() {
        let mut rng = StdRng::seed_from_u64(10);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(13), StreamDirection::ServerToClient, &mut rng);
        let protected = sender
            .protect_record(data_record(StreamId(13), EpochId(0), SeqNo(0), b"alpha"))
            .unwrap();
        receiver.unprotect_record(protected).unwrap();

        let error = receiver
            .unprotect_record(Record {
                header: RecordHeader {
                    seq_no: SeqNo(0),
                    ..data_record(StreamId(13), EpochId(0), SeqNo(0), b"beta").header
                },
                payload: b"beta".to_vec(),
                auth_tag: [0; AUTH_TAG_LEN],
            })
            .unwrap_err();
        assert!(matches!(
            error,
            ProtectionError::Validation(_)
                | ProtectionError::AuthenticationFailure
                | ProtectionError::UnexpectedSeqNo { .. }
        ));
        assert_eq!(receiver.stream_state(), StreamState::Closed);
    }

    #[test]
    fn unexpected_epoch_fails_closed() {
        let mut rng = StdRng::seed_from_u64(11);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(14), StreamDirection::ServerToClient, &mut rng);
        let protected = sender
            .protect_record(data_record(StreamId(14), EpochId(0), SeqNo(0), b"alpha"))
            .unwrap();
        let error = receiver
            .unprotect_record(Record {
                header: RecordHeader {
                    epoch_id: EpochId(1),
                    ..protected.header
                },
                payload: protected.payload,
                auth_tag: protected.auth_tag,
            })
            .unwrap_err();
        assert!(matches!(error, ProtectionError::UnexpectedEpoch { .. }));
        assert_eq!(receiver.stream_state(), StreamState::Closed);
    }

    #[test]
    fn bad_tag_fails_closed() {
        let mut rng = StdRng::seed_from_u64(12);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(15), StreamDirection::ServerToClient, &mut rng);
        let mut protected = sender
            .protect_record(data_record(StreamId(15), EpochId(0), SeqNo(0), b"alpha"))
            .unwrap();
        protected.auth_tag[0] ^= 0x55;
        let error = receiver.unprotect_record(protected).unwrap_err();
        assert_eq!(error, ProtectionError::AuthenticationFailure);
        assert_eq!(receiver.stream_state(), StreamState::Closed);
    }

    #[test]
    fn rekey_transitions_to_new_epoch() {
        let mut rng = StdRng::seed_from_u64(13);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(16), StreamDirection::ServerToClient, &mut rng);
        let rekey_payload = sender.make_rekey_payload(&mut rng);
        let protected_rekey = sender
            .protect_record(rekey_record(
                StreamId(16),
                EpochId(0),
                SeqNo(0),
                rekey_payload,
            ))
            .unwrap();
        let unprotected_rekey = receiver.unprotect_record(protected_rekey).unwrap();
        assert_eq!(
            RekeyPayload::decode(&unprotected_rekey.payload).unwrap(),
            rekey_payload
        );
        assert_eq!(
            receiver.stream_state(),
            StreamState::RekeyPending {
                next_epoch_id: EpochId(1)
            }
        );

        let protected_data = sender
            .protect_record(data_record(StreamId(16), EpochId(1), SeqNo(0), b"epoch1"))
            .unwrap();
        assert!(
            protected_data
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
        );
        let recovered = receiver.unprotect_record(protected_data).unwrap();
        assert_eq!(recovered.payload, b"epoch1");
        assert_eq!(receiver.current_epoch_id(), EpochId(1));
        assert_eq!(receiver.expected_seq_no(), SeqNo(1));
        assert_eq!(receiver.stream_state(), StreamState::Established);
    }

    #[test]
    fn close_record_closes_stream() {
        let mut rng = StdRng::seed_from_u64(14);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(17), StreamDirection::ServerToClient, &mut rng);
        let protected = sender
            .protect_record(close_record(StreamId(17), EpochId(0), SeqNo(0)))
            .unwrap();
        receiver.unprotect_record(protected).unwrap();
        assert_eq!(receiver.stream_state(), StreamState::Closed);
    }

    #[test]
    fn protected_transport_frame_round_trips_multiple_records() {
        let mut rng = StdRng::seed_from_u64(27);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(31), StreamDirection::ServerToClient, &mut rng);
        let frame = sender
            .protect_transport_records(vec![
                data_record(StreamId(31), EpochId(0), SeqNo(0), b"alpha"),
                data_record(StreamId(31), EpochId(0), SeqNo(1), b"beta"),
            ])
            .unwrap();
        let recovered = receiver.unprotect_transport_frame(&frame).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].payload, b"alpha");
        assert_eq!(recovered[1].payload, b"beta");
        assert_eq!(receiver.expected_seq_no(), SeqNo(2));
    }

    #[test]
    fn protected_transport_frame_supports_rekey_transition() {
        let mut rng = StdRng::seed_from_u64(28);
        let (mut sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(32), StreamDirection::ServerToClient, &mut rng);
        let payload = sender.make_rekey_payload(&mut rng);
        let frame = sender
            .protect_transport_records(vec![
                rekey_record(StreamId(32), EpochId(0), SeqNo(0), payload),
                data_record(StreamId(32), EpochId(1), SeqNo(0), b"epoch1"),
            ])
            .unwrap();
        let recovered = receiver.unprotect_transport_frame(&frame).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            RekeyPayload::decode(&recovered[0].payload).unwrap(),
            payload
        );
        assert_eq!(recovered[1].payload, b"epoch1");
        assert!(
            recovered[1]
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
        );
        assert_eq!(receiver.current_epoch_id(), EpochId(1));
        assert_eq!(receiver.expected_seq_no(), SeqNo(1));
    }
}
