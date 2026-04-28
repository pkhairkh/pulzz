use std::collections::HashMap;

use hkdf::Hkdf;
use ml_dsa::{
    EncodedVerifyingKey, KeyGen, MlDsa65, Signature as MlDsaSignature,
    VerifyingKey as MlDsaVerifyingKey,
    signature::{Keypair, Signer, Verifier},
};
use ml_kem::{
    Ciphertext as MlKemCiphertext, DecapsulationKey as MlKemDecapsulationKey,
    EncapsulationKey as MlKemEncapsulationKey, KeyExport, MlKem768, Seed as MlKemSeed,
    kem::Decapsulate,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    DataPlaneCodecPreference, DatagramSessionConfig, ProtectionProfileKind, StreamDirection,
    StreamId, TransportConfig, TransportMode,
};

const BOOTSTRAP_MAGIC: &[u8; 8] = b"WSBOOT1\0";
const CLIENT_TRANSCRIPT_LABEL: &[u8] = b"pq_mutual_v1/client_finish";
const SERVER_TRANSCRIPT_LABEL: &[u8] = b"pq_mutual_v1/server_finish";
const ROOT_LABEL: &[u8] = b"pq_mutual_v1/root";
const ROOT_DIRECTION_CLIENT_TO_SERVER: &[u8] = b"pq_mutual_v1/root/c2s";
const ROOT_DIRECTION_SERVER_TO_CLIENT: &[u8] = b"pq_mutual_v1/root/s2c";
const SIMPLE_ROOT_LABEL: &[u8] = b"pq_simple_v1/root";
const SIMPLE_ROOT_DIRECTION_CLIENT_TO_SERVER: &[u8] = b"pq_simple_v1/root/c2s";
const SIMPLE_ROOT_DIRECTION_SERVER_TO_CLIENT: &[u8] = b"pq_simple_v1/root/s2c";
const SIMPLE_DEFAULT_CLIENT_ID: &str = "pq_simple_v1_client";
const SIMPLE_DEFAULT_SERVER_ID: &str = "pq_simple_v1_server";
pub const BOOTSTRAP_NONCE_LEN: usize = 16;
pub const BOOTSTRAP_SIGNING_SEED_LEN: usize = 32;
pub const BOOTSTRAP_KEM_SEED_LEN: usize = 64;
pub const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_IDLE_READ_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_MAX_BOOTSTRAP_FRAME_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_TRANSPORT_FRAME_BYTES: usize = 2 * 1024 * 1024;
pub const DEFAULT_CLOCK_SKEW_SECS: u64 = 120;
pub const DEFAULT_CLIENT_CREDENTIAL_LIFETIME_SECS: u64 = 15 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapMessageKind {
    ClientHello,
    ServerHello,
    ClientFinish,
    ServerFinish,
    SimpleClientHello,
    SimpleServerHello,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapMessage {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    ClientFinish(ClientFinish),
    ServerFinish(ServerFinish),
    SimpleClientHello(SimpleClientHello),
    SimpleServerHello(SimpleServerHello),
}

impl BootstrapMessage {
    pub fn kind(&self) -> BootstrapMessageKind {
        match self {
            Self::ClientHello(_) => BootstrapMessageKind::ClientHello,
            Self::ServerHello(_) => BootstrapMessageKind::ServerHello,
            Self::ClientFinish(_) => BootstrapMessageKind::ClientFinish,
            Self::ServerFinish(_) => BootstrapMessageKind::ServerFinish,
            Self::SimpleClientHello(_) => BootstrapMessageKind::SimpleClientHello,
            Self::SimpleServerHello(_) => BootstrapMessageKind::SimpleServerHello,
        }
    }

    pub fn to_frame(&self, config: &BootstrapConfig) -> Result<Vec<u8>, BootstrapError> {
        let payload = bincode::serde::encode_to_vec(self, bincode::config::standard())?;
        let mut out = Vec::with_capacity(BOOTSTRAP_MAGIC.len() + payload.len());
        out.extend_from_slice(BOOTSTRAP_MAGIC);
        out.extend_from_slice(&payload);
        if out.len() > config.max_bootstrap_frame_bytes {
            return Err(BootstrapError::FrameTooLarge {
                kind: self.kind(),
                actual_len: out.len(),
                max_len: config.max_bootstrap_frame_bytes,
            });
        }
        Ok(out)
    }

    pub fn from_frame(frame: &[u8], config: &BootstrapConfig) -> Result<Self, BootstrapError> {
        if frame.len() > config.max_bootstrap_frame_bytes {
            return Err(BootstrapError::FrameTooLarge {
                kind: BootstrapMessageKind::ClientHello,
                actual_len: frame.len(),
                max_len: config.max_bootstrap_frame_bytes,
            });
        }
        if frame.len() < BOOTSTRAP_MAGIC.len() || &frame[..BOOTSTRAP_MAGIC.len()] != BOOTSTRAP_MAGIC
        {
            return Err(BootstrapError::InvalidFrameMagic);
        }
        Ok(bincode::serde::decode_from_slice(&frame[BOOTSTRAP_MAGIC.len()..], bincode::config::standard()).map(|(m, _)| m)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHello {
    pub stream_id: StreamId,
    pub client_id: String,
    pub client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub client_ephemeral_kem_public_key: Vec<u8>,
    pub client_credential: ClientCredentialBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHello {
    pub stream_id: StreamId,
    pub server_id: String,
    pub server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub server_ephemeral_kem_public_key: Vec<u8>,
    pub encapsulated_shared_secret_to_client: Vec<u8>,
    pub server_identity: ServerIdentityBundle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientFinish {
    pub stream_id: StreamId,
    pub encapsulated_shared_secret_to_server: Vec<u8>,
    pub transcript_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerFinish {
    pub stream_id: StreamId,
    pub transcript_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleClientHello {
    pub stream_id: StreamId,
    pub client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub client_ephemeral_kem_public_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleServerHello {
    pub stream_id: StreamId,
    pub server_id: String,
    pub server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub encapsulated_shared_secret: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapConfig {
    pub handshake_timeout_ms: u64,
    pub idle_read_timeout_ms: u64,
    pub max_bootstrap_frame_bytes: usize,
    pub max_transport_frame_bytes: usize,
    pub clock_skew_secs: u64,
    pub default_client_credential_lifetime_secs: u64,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            idle_read_timeout_ms: DEFAULT_IDLE_READ_TIMEOUT_MS,
            max_bootstrap_frame_bytes: DEFAULT_MAX_BOOTSTRAP_FRAME_BYTES,
            max_transport_frame_bytes: DEFAULT_MAX_TRANSPORT_FRAME_BYTES,
            clock_skew_secs: DEFAULT_CLOCK_SKEW_SECS,
            default_client_credential_lifetime_secs: DEFAULT_CLIENT_CREDENTIAL_LIFETIME_SECS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialScope {
    pub stream_id: Option<StreamId>,
    pub allow_client_to_server: bool,
    pub allow_server_to_client: bool,
}

impl CredentialScope {
    pub fn allows(&self, stream_id: StreamId, direction: StreamDirection) -> bool {
        if let Some(expected_stream_id) = self.stream_id {
            if expected_stream_id != stream_id {
                return false;
            }
        }

        match direction {
            StreamDirection::ClientToServer => self.allow_client_to_server,
            StreamDirection::ServerToClient => self.allow_server_to_client,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientCredentialBundle {
    pub client_id: String,
    pub issuer_server_id: String,
    pub scope: CredentialScope,
    pub issued_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub client_signing_public_key: Vec<u8>,
    pub issuer_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedClientCredential {
    pub bundle: ClientCredentialBundle,
    pub client_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerIdentityBundle {
    pub server_id: String,
    pub issued_at_unix_secs: u64,
    pub expires_at_unix_secs: u64,
    pub server_signing_public_key: Vec<u8>,
    pub self_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapClientConfig {
    pub stream_id: StreamId,
    pub direction: StreamDirection,
    pub bootstrap: BootstrapConfig,
    pub security: ClientSecurityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapServerConfig {
    pub stream_id: StreamId,
    pub direction: StreamDirection,
    pub bootstrap: BootstrapConfig,
    pub security: ServerSecurityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientSecurityConfig {
    PqMutual {
        issued_credential: IssuedClientCredential,
        expected_server_identity: ServerIdentityBundle,
    },
    PqSimple,
}

impl ClientSecurityConfig {
    pub fn protection_profile(&self) -> ProtectionProfileKind {
        match self {
            Self::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
            Self::PqSimple => ProtectionProfileKind::PqSimpleV1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PqSimpleServerBootstrapConfig {
    pub server_id: String,
}

impl Default for PqSimpleServerBootstrapConfig {
    fn default() -> Self {
        Self {
            server_id: SIMPLE_DEFAULT_SERVER_ID.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerSecurityConfig {
    PqMutual {
        server_identity: ServerIdentityBundle,
        server_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
        revoked_client_ids: Vec<String>,
    },
    PqSimple {
        bootstrap: PqSimpleServerBootstrapConfig,
    },
}

impl ServerSecurityConfig {
    pub fn protection_profile(&self) -> ProtectionProfileKind {
        match self {
            Self::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
            Self::PqSimple { .. } => ProtectionProfileKind::PqSimpleV1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportSessionConfig {
    pub transport: TransportConfig,
    pub datagram: DatagramSessionConfig,
    pub protection_profile: ProtectionProfileKind,
    pub data_plane_codec: DataPlaneCodecPreference,
    pub bootstrap: BootstrapConfig,
    pub runtime_limits: SessionRuntimeLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeLimits {
    pub handshake_timeout_ms: u64,
    pub idle_read_timeout_ms: u64,
    pub max_bootstrap_frame_bytes: usize,
    pub max_transport_frame_bytes: usize,
}

impl Default for SessionRuntimeLimits {
    fn default() -> Self {
        Self {
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            idle_read_timeout_ms: DEFAULT_IDLE_READ_TIMEOUT_MS,
            max_bootstrap_frame_bytes: DEFAULT_MAX_BOOTSTRAP_FRAME_BYTES,
            max_transport_frame_bytes: DEFAULT_MAX_TRANSPORT_FRAME_BYTES,
        }
    }
}

impl Default for TransportSessionConfig {
    fn default() -> Self {
        Self {
            transport: TransportConfig {
                mode: TransportMode::BurstMedium,
            },
            datagram: DatagramSessionConfig::default(),
            protection_profile: ProtectionProfileKind::PqMutualV1,
            data_plane_codec: DataPlaneCodecPreference::DirectExactOnly,
            bootstrap: BootstrapConfig::default(),
            runtime_limits: SessionRuntimeLimits::default(),
        }
    }
}

impl TransportSessionConfig {
    pub fn with_data_plane_codec(mut self, preference: DataPlaneCodecPreference) -> Self {
        self.data_plane_codec = preference;
        self
    }

    pub fn direct_exact_only() -> Self {
        Self::default().with_data_plane_codec(DataPlaneCodecPreference::DirectExactOnly)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapCompleted {
    pub stream_id: StreamId,
    pub direction: StreamDirection,
    pub protection_profile: ProtectionProfileKind,
    pub root: [u8; 32],
    pub client_id: String,
    pub server_id: String,
    pub client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
}

#[derive(Debug, Clone)]
pub struct ClientBootstrapState {
    inner: ClientBootstrapStateInner,
}

#[derive(Debug, Clone)]
enum ClientBootstrapStateInner {
    PqMutual(MutualClientBootstrapState),
    PqSimple(SimpleClientBootstrapState),
}

#[derive(Debug, Clone)]
struct MutualClientBootstrapState {
    config: BootstrapClientConfig,
    client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    client_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    client_hello_frame: Vec<u8>,
    client_kem_secret: MlKemDecapsulationKey<MlKem768>,
    transcript_hash_after_client_finish: Option<[u8; 32]>,
    root: Option<[u8; 32]>,
    server_nonce: Option<[u8; BOOTSTRAP_NONCE_LEN]>,
}

#[derive(Debug, Clone)]
struct SimpleClientBootstrapState {
    config: BootstrapClientConfig,
    client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    client_kem_secret: MlKemDecapsulationKey<MlKem768>,
}

#[derive(Debug, Clone)]
pub struct ServerBootstrapState {
    inner: ServerBootstrapStateInner,
}

#[derive(Debug, Clone)]
enum ServerBootstrapStateInner {
    PqMutual(MutualServerBootstrapState),
}

#[derive(Debug, Clone)]
struct MutualServerBootstrapState {
    config: BootstrapServerConfig,
    client_hello: ClientHello,
    client_hello_frame: Vec<u8>,
    server_hello: ServerHello,
    server_hello_frame: Vec<u8>,
    server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    server_kem_secret: MlKemDecapsulationKey<MlKem768>,
    client_shared_secret: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct ClientBootstrapProgress {
    pub outbound: Option<BootstrapMessage>,
    pub completed: Option<BootstrapCompleted>,
}

#[derive(Debug, Clone)]
pub struct ServerBootstrapResponse {
    pub state: Option<ServerBootstrapState>,
    pub outbound: BootstrapMessage,
    pub completed: Option<BootstrapCompleted>,
}

#[derive(Debug, Clone, Default)]
pub struct ReplayCache {
    entries: HashMap<ReplayCacheKey, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplayCacheKey {
    pub client_id: String,
    pub client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    pub server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Codec(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
    #[error("bootstrap frame magic is invalid")]
    InvalidFrameMagic,
    #[error("bootstrap frame for {kind:?} exceeded {max_len} bytes (got {actual_len})")]
    FrameTooLarge {
        kind: BootstrapMessageKind,
        actual_len: usize,
        max_len: usize,
    },
    #[error("unexpected bootstrap message kind: expected {expected:?}, actual {actual:?}")]
    UnexpectedMessageKind {
        expected: BootstrapMessageKind,
        actual: BootstrapMessageKind,
    },
    #[error("bootstrap stream_id mismatch: expected {expected:?}, actual {actual:?}")]
    UnexpectedStreamId {
        expected: StreamId,
        actual: StreamId,
    },
    #[error("bootstrap profile {0:?} is not supported by the mutual PQ bootstrap")]
    UnsupportedProtectionProfile(ProtectionProfileKind),
    #[error("server identity is not the expected pinned identity")]
    UnexpectedServerIdentity,
    #[error("server identity is expired or not yet valid")]
    InvalidServerIdentityValidity,
    #[error("client credential is expired or not yet valid")]
    InvalidClientCredentialValidity,
    #[error("client credential does not authorize stream {stream_id:?} / direction {direction:?}")]
    CredentialScopeRejected {
        stream_id: StreamId,
        direction: StreamDirection,
    },
    #[error("client credential issuer is not the current server identity")]
    CredentialIssuerMismatch,
    #[error("credential subject and transcript client_id differ")]
    ClientIdMismatch,
    #[error("server hello id and server identity differ")]
    ServerIdMismatch,
    #[error("client credential signature verification failed")]
    InvalidClientCredentialSignature,
    #[error("server identity signature verification failed")]
    InvalidServerIdentitySignature,
    #[error("client transcript signature verification failed")]
    InvalidClientTranscriptSignature,
    #[error("server transcript signature verification failed")]
    InvalidServerTranscriptSignature,
    #[error("client credential has been revoked")]
    RevokedClientCredential,
    #[error("bootstrap replay detected")]
    ReplayDetected,
    #[error("ML-KEM public key is invalid")]
    InvalidKemPublicKey,
    #[error("ML-KEM ciphertext is invalid")]
    InvalidKemCiphertext,
    #[error("ML-DSA public key is invalid")]
    InvalidSigningPublicKey,
    #[error("ML-DSA signature is invalid")]
    InvalidSignatureEncoding,
}

pub fn issue_server_identity(
    server_id: impl Into<String>,
    issued_at_unix_secs: u64,
    expires_at_unix_secs: u64,
    server_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
) -> Result<ServerIdentityBundle, BootstrapError> {
    let server_signing_key = mldsa_signing_key(server_signing_seed);
    let mut bundle = ServerIdentityBundle {
        server_id: server_id.into(),
        issued_at_unix_secs,
        expires_at_unix_secs,
        server_signing_public_key: server_signing_key.verifying_key().encode().to_vec(),
        self_signature: Vec::new(),
    };
    let signature = server_signing_key
        .sign(identity_signable_bytes(&bundle)?.as_slice())
        .encode()
        .to_vec();
    bundle.self_signature = signature;
    Ok(bundle)
}

pub fn issue_client_credential(
    server_identity: &ServerIdentityBundle,
    server_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
    client_id: impl Into<String>,
    client_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
    scope: CredentialScope,
    issued_at_unix_secs: u64,
    expires_at_unix_secs: u64,
) -> Result<IssuedClientCredential, BootstrapError> {
    let server_signing_key = mldsa_signing_key(server_signing_seed);
    let client_signing_key = mldsa_signing_key(client_signing_seed);
    let mut bundle = ClientCredentialBundle {
        client_id: client_id.into(),
        issuer_server_id: server_identity.server_id.clone(),
        scope,
        issued_at_unix_secs,
        expires_at_unix_secs,
        client_signing_public_key: client_signing_key.verifying_key().encode().to_vec(),
        issuer_signature: Vec::new(),
    };
    let signature = server_signing_key
        .sign(client_credential_signable_bytes(&bundle)?.as_slice())
        .encode()
        .to_vec();
    bundle.issuer_signature = signature;
    Ok(IssuedClientCredential {
        bundle,
        client_signing_seed,
    })
}

impl ReplayCache {
    pub fn prune_expired(&mut self, now_unix_secs: u64) {
        self.entries
            .retain(|_, expires_at| *expires_at >= now_unix_secs);
    }

    pub fn check_and_insert(
        &mut self,
        key: ReplayCacheKey,
        expires_at_unix_secs: u64,
        now_unix_secs: u64,
    ) -> Result<(), BootstrapError> {
        self.prune_expired(now_unix_secs);
        if self.entries.contains_key(&key) {
            return Err(BootstrapError::ReplayDetected);
        }
        self.entries.insert(key, expires_at_unix_secs);
        Ok(())
    }
}

impl ClientBootstrapState {
    pub fn start(
        config: BootstrapClientConfig,
        client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
        client_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    ) -> Result<(Self, BootstrapMessage), BootstrapError> {
        match config.protection_profile() {
            ProtectionProfileKind::PqMutualV1 => {
                let (state, message) =
                    MutualClientBootstrapState::start(config, client_nonce, client_kem_seed)?;
                Ok((
                    Self {
                        inner: ClientBootstrapStateInner::PqMutual(state),
                    },
                    message,
                ))
            }
            ProtectionProfileKind::PqSimpleV1 => {
                let (state, message) =
                    SimpleClientBootstrapState::start(config, client_nonce, client_kem_seed)?;
                Ok((
                    Self {
                        inner: ClientBootstrapStateInner::PqSimple(state),
                    },
                    message,
                ))
            }
            other => Err(BootstrapError::UnsupportedProtectionProfile(other)),
        }
    }

    pub fn handle_server_hello(
        &mut self,
        message: BootstrapMessage,
        now_unix_secs: u64,
    ) -> Result<ClientBootstrapProgress, BootstrapError> {
        match &mut self.inner {
            ClientBootstrapStateInner::PqMutual(state) => {
                state.handle_server_hello(message, now_unix_secs)
            }
            ClientBootstrapStateInner::PqSimple(state) => state.handle_server_hello(message),
        }
    }

    pub fn handle_server_finish(
        &self,
        message: BootstrapMessage,
    ) -> Result<BootstrapCompleted, BootstrapError> {
        match &self.inner {
            ClientBootstrapStateInner::PqMutual(state) => state.handle_server_finish(message),
            ClientBootstrapStateInner::PqSimple(_) => Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::SimpleServerHello,
                actual: message.kind(),
            }),
        }
    }
}

impl BootstrapClientConfig {
    pub fn protection_profile(&self) -> ProtectionProfileKind {
        match &self.security {
            ClientSecurityConfig::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
            ClientSecurityConfig::PqSimple => ProtectionProfileKind::PqSimpleV1,
        }
    }
}

impl ServerBootstrapState {
    pub fn respond_to_client_hello(
        config: BootstrapServerConfig,
        replay_cache: &mut ReplayCache,
        message: BootstrapMessage,
        now_unix_secs: u64,
        server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
        server_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    ) -> Result<ServerBootstrapResponse, BootstrapError> {
        match config.protection_profile() {
            ProtectionProfileKind::PqMutualV1 => {
                let (state, outbound) = MutualServerBootstrapState::respond_to_client_hello(
                    config,
                    replay_cache,
                    message,
                    now_unix_secs,
                    server_nonce,
                    server_kem_seed,
                )?;
                Ok(ServerBootstrapResponse {
                    state: Some(Self {
                        inner: ServerBootstrapStateInner::PqMutual(state),
                    }),
                    outbound,
                    completed: None,
                })
            }
            ProtectionProfileKind::PqSimpleV1 => {
                let (completed, outbound) =
                    respond_to_simple_client_hello(config, message, server_nonce, server_kem_seed)?;
                Ok(ServerBootstrapResponse {
                    state: None,
                    outbound,
                    completed: Some(completed),
                })
            }
            other => Err(BootstrapError::UnsupportedProtectionProfile(other)),
        }
    }

    pub fn handle_client_finish(
        self,
        message: BootstrapMessage,
    ) -> Result<(BootstrapCompleted, BootstrapMessage), BootstrapError> {
        match self.inner {
            ServerBootstrapStateInner::PqMutual(state) => state.handle_client_finish(message),
        }
    }
}

impl BootstrapServerConfig {
    pub fn protection_profile(&self) -> ProtectionProfileKind {
        match &self.security {
            ServerSecurityConfig::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
            ServerSecurityConfig::PqSimple { .. } => ProtectionProfileKind::PqSimpleV1,
        }
    }
}

impl MutualClientBootstrapState {
    fn start(
        config: BootstrapClientConfig,
        client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
        client_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    ) -> Result<(Self, BootstrapMessage), BootstrapError> {
        let ClientSecurityConfig::PqMutual {
            issued_credential,
            expected_server_identity: _,
        } = &config.security
        else {
            return Err(BootstrapError::UnsupportedProtectionProfile(
                config.protection_profile(),
            ));
        };

        let client_kem_secret =
            MlKemDecapsulationKey::<MlKem768>::from_seed(MlKemSeed::from(client_kem_seed));
        let client_hello = ClientHello {
            stream_id: config.stream_id,
            client_id: issued_credential.bundle.client_id.clone(),
            client_nonce,
            client_ephemeral_kem_public_key: client_kem_secret
                .encapsulation_key()
                .to_bytes()
                .to_vec(),
            client_credential: issued_credential.bundle.clone(),
        };
        let message = BootstrapMessage::ClientHello(client_hello.clone());
        let client_hello_frame = message.to_frame(&config.bootstrap)?;
        Ok((
            Self {
                config,
                client_nonce,
                client_kem_seed,
                client_hello_frame,
                client_kem_secret,
                transcript_hash_after_client_finish: None,
                root: None,
                server_nonce: None,
            },
            message,
        ))
    }

    fn handle_server_hello(
        &mut self,
        message: BootstrapMessage,
        now_unix_secs: u64,
    ) -> Result<ClientBootstrapProgress, BootstrapError> {
        let BootstrapMessage::ServerHello(server_hello) = message else {
            return Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::ServerHello,
                actual: message.kind(),
            });
        };
        let ClientSecurityConfig::PqMutual {
            issued_credential,
            expected_server_identity,
        } = &self.config.security
        else {
            return Err(BootstrapError::UnsupportedProtectionProfile(
                self.config.protection_profile(),
            ));
        };
        if server_hello.stream_id != self.config.stream_id {
            return Err(BootstrapError::UnexpectedStreamId {
                expected: self.config.stream_id,
                actual: server_hello.stream_id,
            });
        }
        if server_hello.server_id != server_hello.server_identity.server_id {
            return Err(BootstrapError::ServerIdMismatch);
        }
        if server_hello.server_identity != *expected_server_identity {
            return Err(BootstrapError::UnexpectedServerIdentity);
        }
        validate_server_identity_bundle(
            &server_hello.server_identity,
            now_unix_secs,
            self.config.bootstrap.clock_skew_secs,
        )?;

        let server_ciphertext =
            decode_kem_ciphertext(server_hello.encapsulated_shared_secret_to_client.as_slice())?;
        let server_shared = self.client_kem_secret.decapsulate(&server_ciphertext);
        let server_kem_public =
            decode_kem_public_key(server_hello.server_ephemeral_kem_public_key.as_slice())?;
        let client_encapsulated_secret = kem_encapsulate_deterministic(
            &server_kem_public,
            &[b"client-finish", &self.client_kem_seed, &self.client_nonce],
        );
        let client_shared_secret = shared_key_bytes(client_encapsulated_secret.1.as_slice());

        let server_hello_frame =
            BootstrapMessage::ServerHello(server_hello.clone()).to_frame(&self.config.bootstrap)?;
        self.server_nonce = Some(server_hello.server_nonce);

        let finish_without_sig = ClientFinish {
            stream_id: self.config.stream_id,
            encapsulated_shared_secret_to_server: client_encapsulated_secret.0.to_vec(),
            transcript_signature: Vec::new(),
        };
        let signable = client_finish_signable_bytes(
            &self.client_hello_frame,
            &server_hello_frame,
            &finish_without_sig,
        )?;
        let client_signing_key = mldsa_signing_key(issued_credential.client_signing_seed);
        let signature = client_signing_key
            .sign(signable.as_slice())
            .encode()
            .to_vec();
        let client_finish = ClientFinish {
            transcript_signature: signature,
            ..finish_without_sig
        };
        let client_finish_frame = BootstrapMessage::ClientFinish(client_finish.clone())
            .to_frame(&self.config.bootstrap)?;
        let transcript_hash_after_client_finish = transcript_hash(&[
            self.client_hello_frame.as_slice(),
            server_hello_frame.as_slice(),
            client_finish_frame.as_slice(),
        ]);
        let combined_root = derive_pq_mutual_root(
            self.config.stream_id,
            self.config.direction,
            &shared_key_bytes(server_shared.as_slice()),
            &client_shared_secret,
            &transcript_hash_after_client_finish,
        );
        self.transcript_hash_after_client_finish = Some(transcript_hash_after_client_finish);
        self.root = Some(combined_root);
        Ok(ClientBootstrapProgress {
            outbound: Some(BootstrapMessage::ClientFinish(client_finish)),
            completed: None,
        })
    }

    fn handle_server_finish(
        &self,
        message: BootstrapMessage,
    ) -> Result<BootstrapCompleted, BootstrapError> {
        let BootstrapMessage::ServerFinish(server_finish) = message else {
            return Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::ServerFinish,
                actual: message.kind(),
            });
        };
        let ClientSecurityConfig::PqMutual {
            issued_credential,
            expected_server_identity,
        } = &self.config.security
        else {
            return Err(BootstrapError::UnsupportedProtectionProfile(
                self.config.protection_profile(),
            ));
        };
        let transcript_hash = self.transcript_hash_after_client_finish.ok_or(
            BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::ServerHello,
                actual: BootstrapMessageKind::ServerFinish,
            },
        )?;
        let verifying_key =
            decode_mldsa_verifying_key(&expected_server_identity.server_signing_public_key)?;
        let signable = server_finish_signable_bytes(
            self.config.stream_id,
            self.config.direction,
            &transcript_hash,
        );
        let signature = decode_mldsa_signature(server_finish.transcript_signature.as_slice())?;
        verifying_key
            .verify(&signable, &signature)
            .map_err(|_| BootstrapError::InvalidServerTranscriptSignature)?;

        Ok(BootstrapCompleted {
            stream_id: self.config.stream_id,
            direction: self.config.direction,
            protection_profile: ProtectionProfileKind::PqMutualV1,
            root: self.root.expect("client root is set after server hello"),
            client_id: issued_credential.bundle.client_id.clone(),
            server_id: expected_server_identity.server_id.clone(),
            client_nonce: self.client_nonce,
            server_nonce: self
                .server_nonce
                .expect("server nonce set after server hello"),
        })
    }
}

impl SimpleClientBootstrapState {
    fn start(
        config: BootstrapClientConfig,
        client_nonce: [u8; BOOTSTRAP_NONCE_LEN],
        client_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    ) -> Result<(Self, BootstrapMessage), BootstrapError> {
        let client_kem_secret =
            MlKemDecapsulationKey::<MlKem768>::from_seed(MlKemSeed::from(client_kem_seed));
        let client_hello = SimpleClientHello {
            stream_id: config.stream_id,
            client_nonce,
            client_ephemeral_kem_public_key: client_kem_secret
                .encapsulation_key()
                .to_bytes()
                .to_vec(),
        };
        Ok((
            Self {
                config,
                client_nonce,
                client_kem_secret,
            },
            BootstrapMessage::SimpleClientHello(client_hello),
        ))
    }

    fn handle_server_hello(
        &mut self,
        message: BootstrapMessage,
    ) -> Result<ClientBootstrapProgress, BootstrapError> {
        let BootstrapMessage::SimpleServerHello(server_hello) = message else {
            return Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::SimpleServerHello,
                actual: message.kind(),
            });
        };
        if server_hello.stream_id != self.config.stream_id {
            return Err(BootstrapError::UnexpectedStreamId {
                expected: self.config.stream_id,
                actual: server_hello.stream_id,
            });
        }
        let ciphertext = decode_kem_ciphertext(server_hello.encapsulated_shared_secret.as_slice())?;
        let shared_secret = self.client_kem_secret.decapsulate(&ciphertext);
        let client_hello_frame = BootstrapMessage::SimpleClientHello(SimpleClientHello {
            stream_id: self.config.stream_id,
            client_nonce: self.client_nonce,
            client_ephemeral_kem_public_key: self
                .client_kem_secret
                .encapsulation_key()
                .to_bytes()
                .to_vec(),
        })
        .to_frame(&self.config.bootstrap)?;
        let server_hello_frame = BootstrapMessage::SimpleServerHello(server_hello.clone())
            .to_frame(&self.config.bootstrap)?;
        let transcript_hash =
            transcript_hash(&[client_hello_frame.as_slice(), server_hello_frame.as_slice()]);
        let root = derive_pq_simple_root(
            self.config.stream_id,
            self.config.direction,
            &shared_key_bytes(shared_secret.as_slice()),
            &transcript_hash,
        );
        Ok(ClientBootstrapProgress {
            outbound: None,
            completed: Some(BootstrapCompleted {
                stream_id: self.config.stream_id,
                direction: self.config.direction,
                protection_profile: ProtectionProfileKind::PqSimpleV1,
                root,
                client_id: SIMPLE_DEFAULT_CLIENT_ID.to_string(),
                server_id: server_hello.server_id,
                client_nonce: self.client_nonce,
                server_nonce: server_hello.server_nonce,
            }),
        })
    }
}

impl MutualServerBootstrapState {
    fn respond_to_client_hello(
        config: BootstrapServerConfig,
        replay_cache: &mut ReplayCache,
        message: BootstrapMessage,
        now_unix_secs: u64,
        server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
        server_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
    ) -> Result<(Self, BootstrapMessage), BootstrapError> {
        let BootstrapMessage::ClientHello(client_hello) = message else {
            return Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::ClientHello,
                actual: message.kind(),
            });
        };
        let ServerSecurityConfig::PqMutual {
            server_identity,
            server_signing_seed: _,
            revoked_client_ids,
        } = &config.security
        else {
            return Err(BootstrapError::UnsupportedProtectionProfile(
                config.protection_profile(),
            ));
        };
        if client_hello.stream_id != config.stream_id {
            return Err(BootstrapError::UnexpectedStreamId {
                expected: config.stream_id,
                actual: client_hello.stream_id,
            });
        }
        validate_server_identity_bundle(
            server_identity,
            now_unix_secs,
            config.bootstrap.clock_skew_secs,
        )?;
        validate_client_credential_bundle(
            &client_hello.client_credential,
            server_identity,
            config.stream_id,
            config.direction,
            now_unix_secs,
            config.bootstrap.clock_skew_secs,
        )?;
        if client_hello.client_credential.client_id != client_hello.client_id {
            return Err(BootstrapError::ClientIdMismatch);
        }
        if revoked_client_ids
            .iter()
            .any(|revoked| revoked == &client_hello.client_id)
        {
            return Err(BootstrapError::RevokedClientCredential);
        }
        replay_cache.check_and_insert(
            ReplayCacheKey {
                client_id: client_hello.client_id.clone(),
                client_nonce: client_hello.client_nonce,
                server_nonce,
            },
            client_hello.client_credential.expires_at_unix_secs,
            now_unix_secs,
        )?;

        let client_kem_public =
            decode_kem_public_key(client_hello.client_ephemeral_kem_public_key.as_slice())?;
        let (ciphertext_to_client, server_shared) = kem_encapsulate_deterministic(
            &client_kem_public,
            &[
                b"server-hello",
                &server_kem_seed,
                &server_nonce,
                &client_hello.client_nonce,
            ],
        );
        let server_kem_secret =
            MlKemDecapsulationKey::<MlKem768>::from_seed(MlKemSeed::from(server_kem_seed));
        let server_hello = ServerHello {
            stream_id: config.stream_id,
            server_id: server_identity.server_id.clone(),
            server_nonce,
            server_ephemeral_kem_public_key: server_kem_secret
                .encapsulation_key()
                .to_bytes()
                .to_vec(),
            encapsulated_shared_secret_to_client: ciphertext_to_client.to_vec(),
            server_identity: server_identity.clone(),
        };
        let client_hello_frame =
            BootstrapMessage::ClientHello(client_hello.clone()).to_frame(&config.bootstrap)?;
        let server_hello_frame =
            BootstrapMessage::ServerHello(server_hello.clone()).to_frame(&config.bootstrap)?;
        let _ = transcript_hash(&[client_hello_frame.as_slice(), server_hello_frame.as_slice()]);

        Ok((
            Self {
                config,
                client_hello,
                client_hello_frame,
                server_hello: server_hello.clone(),
                server_hello_frame,
                server_nonce,
                server_kem_secret,
                client_shared_secret: shared_key_bytes(server_shared.as_slice()),
            },
            BootstrapMessage::ServerHello(server_hello),
        ))
    }

    fn handle_client_finish(
        self,
        message: BootstrapMessage,
    ) -> Result<(BootstrapCompleted, BootstrapMessage), BootstrapError> {
        let BootstrapMessage::ClientFinish(client_finish) = message else {
            return Err(BootstrapError::UnexpectedMessageKind {
                expected: BootstrapMessageKind::ClientFinish,
                actual: message.kind(),
            });
        };
        let ServerSecurityConfig::PqMutual {
            server_identity: _,
            server_signing_seed,
            revoked_client_ids: _,
        } = &self.config.security
        else {
            return Err(BootstrapError::UnsupportedProtectionProfile(
                self.config.protection_profile(),
            ));
        };
        if client_finish.stream_id != self.config.stream_id {
            return Err(BootstrapError::UnexpectedStreamId {
                expected: self.config.stream_id,
                actual: client_finish.stream_id,
            });
        }

        let client_kem_ciphertext = decode_kem_ciphertext(
            client_finish
                .encapsulated_shared_secret_to_server
                .as_slice(),
        )?;
        let server_shared = self.server_kem_secret.decapsulate(&client_kem_ciphertext);
        let client_vk = decode_mldsa_verifying_key(
            self.client_hello
                .client_credential
                .client_signing_public_key
                .as_slice(),
        )?;
        let signable = client_finish_signable_bytes(
            &self.client_hello_frame,
            &self.server_hello_frame,
            &ClientFinish {
                transcript_signature: Vec::new(),
                ..client_finish.clone()
            },
        )?;
        let signature = decode_mldsa_signature(client_finish.transcript_signature.as_slice())?;
        client_vk
            .verify(signable.as_slice(), &signature)
            .map_err(|_| BootstrapError::InvalidClientTranscriptSignature)?;

        let transcript_hash_after_client_finish = transcript_hash(&[
            self.client_hello_frame.as_slice(),
            self.server_hello_frame.as_slice(),
            BootstrapMessage::ClientFinish(client_finish.clone())
                .to_frame(&self.config.bootstrap)?
                .as_slice(),
        ]);
        let root = derive_pq_mutual_root(
            self.config.stream_id,
            self.config.direction,
            &self.client_shared_secret,
            &shared_key_bytes(server_shared.as_slice()),
            &transcript_hash_after_client_finish,
        );

        let server_signing_key = mldsa_signing_key(*server_signing_seed);
        let server_finish_signature = server_signing_key
            .sign(
                server_finish_signable_bytes(
                    self.config.stream_id,
                    self.config.direction,
                    &transcript_hash_after_client_finish,
                )
                .as_slice(),
            )
            .encode()
            .to_vec();
        let server_finish = BootstrapMessage::ServerFinish(ServerFinish {
            stream_id: self.config.stream_id,
            transcript_signature: server_finish_signature,
        });

        Ok((
            BootstrapCompleted {
                stream_id: self.config.stream_id,
                direction: self.config.direction,
                protection_profile: ProtectionProfileKind::PqMutualV1,
                root,
                client_id: self.client_hello.client_id,
                server_id: self.server_hello.server_id,
                client_nonce: self.client_hello.client_nonce,
                server_nonce: self.server_nonce,
            },
            server_finish,
        ))
    }
}

fn respond_to_simple_client_hello(
    config: BootstrapServerConfig,
    message: BootstrapMessage,
    server_nonce: [u8; BOOTSTRAP_NONCE_LEN],
    server_kem_seed: [u8; BOOTSTRAP_KEM_SEED_LEN],
) -> Result<(BootstrapCompleted, BootstrapMessage), BootstrapError> {
    let BootstrapMessage::SimpleClientHello(client_hello) = message else {
        return Err(BootstrapError::UnexpectedMessageKind {
            expected: BootstrapMessageKind::SimpleClientHello,
            actual: message.kind(),
        });
    };
    let ServerSecurityConfig::PqSimple { bootstrap } = &config.security else {
        return Err(BootstrapError::UnsupportedProtectionProfile(
            config.protection_profile(),
        ));
    };
    if client_hello.stream_id != config.stream_id {
        return Err(BootstrapError::UnexpectedStreamId {
            expected: config.stream_id,
            actual: client_hello.stream_id,
        });
    }
    let client_kem_public =
        decode_kem_public_key(client_hello.client_ephemeral_kem_public_key.as_slice())?;
    let (ciphertext, shared_secret) = kem_encapsulate_deterministic(
        &client_kem_public,
        &[
            b"simple-server-hello",
            &server_kem_seed,
            &server_nonce,
            &client_hello.client_nonce,
        ],
    );
    let server_id = if bootstrap.server_id.is_empty() {
        SIMPLE_DEFAULT_SERVER_ID.to_string()
    } else {
        bootstrap.server_id.clone()
    };
    let server_hello = SimpleServerHello {
        stream_id: config.stream_id,
        server_id: server_id.clone(),
        server_nonce,
        encapsulated_shared_secret: ciphertext.to_vec(),
    };
    let client_hello_frame =
        BootstrapMessage::SimpleClientHello(client_hello.clone()).to_frame(&config.bootstrap)?;
    let server_hello_frame =
        BootstrapMessage::SimpleServerHello(server_hello.clone()).to_frame(&config.bootstrap)?;
    let transcript_hash =
        transcript_hash(&[client_hello_frame.as_slice(), server_hello_frame.as_slice()]);
    let root = derive_pq_simple_root(
        config.stream_id,
        config.direction,
        &shared_key_bytes(shared_secret.as_slice()),
        &transcript_hash,
    );
    Ok((
        BootstrapCompleted {
            stream_id: config.stream_id,
            direction: config.direction,
            protection_profile: ProtectionProfileKind::PqSimpleV1,
            root,
            client_id: SIMPLE_DEFAULT_CLIENT_ID.to_string(),
            server_id,
            client_nonce: client_hello.client_nonce,
            server_nonce,
        },
        BootstrapMessage::SimpleServerHello(server_hello),
    ))
}

pub fn validate_server_identity_bundle(
    bundle: &ServerIdentityBundle,
    now_unix_secs: u64,
    clock_skew_secs: u64,
) -> Result<(), BootstrapError> {
    if bundle.issued_at_unix_secs > now_unix_secs.saturating_add(clock_skew_secs)
        || bundle.expires_at_unix_secs.saturating_add(clock_skew_secs) < now_unix_secs
    {
        return Err(BootstrapError::InvalidServerIdentityValidity);
    }
    let verifying_key = decode_mldsa_verifying_key(bundle.server_signing_public_key.as_slice())?;
    let signature = decode_mldsa_signature(bundle.self_signature.as_slice())?;
    verifying_key
        .verify(identity_signable_bytes(bundle)?.as_slice(), &signature)
        .map_err(|_| BootstrapError::InvalidServerIdentitySignature)?;
    Ok(())
}

pub fn validate_client_credential_bundle(
    bundle: &ClientCredentialBundle,
    server_identity: &ServerIdentityBundle,
    stream_id: StreamId,
    direction: StreamDirection,
    now_unix_secs: u64,
    clock_skew_secs: u64,
) -> Result<(), BootstrapError> {
    if bundle.issuer_server_id != server_identity.server_id {
        return Err(BootstrapError::CredentialIssuerMismatch);
    }
    if bundle.issued_at_unix_secs > now_unix_secs.saturating_add(clock_skew_secs)
        || bundle.expires_at_unix_secs.saturating_add(clock_skew_secs) < now_unix_secs
    {
        return Err(BootstrapError::InvalidClientCredentialValidity);
    }
    if !bundle.scope.allows(stream_id, direction) {
        return Err(BootstrapError::CredentialScopeRejected {
            stream_id,
            direction,
        });
    }
    let server_key =
        decode_mldsa_verifying_key(server_identity.server_signing_public_key.as_slice())?;
    let signature = decode_mldsa_signature(bundle.issuer_signature.as_slice())?;
    server_key
        .verify(
            client_credential_signable_bytes(bundle)?.as_slice(),
            &signature,
        )
        .map_err(|_| BootstrapError::InvalidClientCredentialSignature)?;
    Ok(())
}

pub fn derive_pq_mutual_root(
    stream_id: StreamId,
    direction: StreamDirection,
    server_to_client_shared_secret: &[u8; 32],
    client_to_server_shared_secret: &[u8; 32],
    transcript_hash: &[u8; 32],
) -> [u8; 32] {
    let salt = concatenate_parts(&[ROOT_LABEL, &stream_id.0.to_le_bytes(), transcript_hash]);
    let ikm = concatenate_parts(&[
        server_to_client_shared_secret,
        client_to_server_shared_secret,
        transcript_hash,
    ]);
    let hkdf = Hkdf::<Sha256>::new(Some(&salt), &ikm);
    let mut root = [0_u8; 32];
    hkdf.expand(direction_root_label(direction), &mut root)
        .expect("hkdf expand for pq_mutual_v1 root should succeed");
    root
}

pub fn derive_pq_simple_root(
    stream_id: StreamId,
    direction: StreamDirection,
    shared_secret: &[u8; 32],
    transcript_hash: &[u8; 32],
) -> [u8; 32] {
    let salt = concatenate_parts(&[
        SIMPLE_ROOT_LABEL,
        &stream_id.0.to_le_bytes(),
        transcript_hash,
    ]);
    let ikm = concatenate_parts(&[shared_secret, transcript_hash]);
    let hkdf = Hkdf::<Sha256>::new(Some(&salt), &ikm);
    let mut root = [0_u8; 32];
    hkdf.expand(simple_direction_root_label(direction), &mut root)
        .expect("hkdf expand for pq_simple_v1 root should succeed");
    root
}

fn direction_root_label(direction: StreamDirection) -> &'static [u8] {
    match direction {
        StreamDirection::ClientToServer => ROOT_DIRECTION_CLIENT_TO_SERVER,
        StreamDirection::ServerToClient => ROOT_DIRECTION_SERVER_TO_CLIENT,
    }
}

fn simple_direction_root_label(direction: StreamDirection) -> &'static [u8] {
    match direction {
        StreamDirection::ClientToServer => SIMPLE_ROOT_DIRECTION_CLIENT_TO_SERVER,
        StreamDirection::ServerToClient => SIMPLE_ROOT_DIRECTION_SERVER_TO_CLIENT,
    }
}

fn client_finish_signable_bytes(
    client_hello_frame: &[u8],
    server_hello_frame: &[u8],
    finish: &ClientFinish,
) -> Result<Vec<u8>, BootstrapError> {
    Ok(concatenate_parts(&[
        CLIENT_TRANSCRIPT_LABEL,
        &transcript_hash(&[
            client_hello_frame,
            server_hello_frame,
            &bincode::serde::encode_to_vec(finish, bincode::config::standard())?,
        ]),
    ]))
}

fn server_finish_signable_bytes(
    stream_id: StreamId,
    direction: StreamDirection,
    transcript_hash: &[u8; 32],
) -> Vec<u8> {
    concatenate_parts(&[
        SERVER_TRANSCRIPT_LABEL,
        &stream_id.0.to_le_bytes(),
        direction_root_label(direction),
        transcript_hash,
    ])
}

fn client_credential_signable_bytes(
    bundle: &ClientCredentialBundle,
) -> Result<Vec<u8>, BootstrapError> {
    #[derive(Serialize)]
    struct Signable<'a> {
        client_id: &'a str,
        issuer_server_id: &'a str,
        scope: &'a CredentialScope,
        issued_at_unix_secs: u64,
        expires_at_unix_secs: u64,
        client_signing_public_key: &'a [u8],
    }

    Ok(bincode::serde::encode_to_vec(&Signable {
        client_id: &bundle.client_id,
        issuer_server_id: &bundle.issuer_server_id,
        scope: &bundle.scope,
        issued_at_unix_secs: bundle.issued_at_unix_secs,
        expires_at_unix_secs: bundle.expires_at_unix_secs,
        client_signing_public_key: &bundle.client_signing_public_key,
    }, bincode::config::standard())?)
}

fn identity_signable_bytes(bundle: &ServerIdentityBundle) -> Result<Vec<u8>, BootstrapError> {
    #[derive(Serialize)]
    struct Signable<'a> {
        server_id: &'a str,
        issued_at_unix_secs: u64,
        expires_at_unix_secs: u64,
        server_signing_public_key: &'a [u8],
    }

    Ok(bincode::serde::encode_to_vec(&Signable {
        server_id: &bundle.server_id,
        issued_at_unix_secs: bundle.issued_at_unix_secs,
        expires_at_unix_secs: bundle.expires_at_unix_secs,
        server_signing_public_key: &bundle.server_signing_public_key,
    }, bincode::config::standard())?)
}

fn decode_mldsa_verifying_key(bytes: &[u8]) -> Result<MlDsaVerifyingKey<MlDsa65>, BootstrapError> {
    let encoded = EncodedVerifyingKey::<MlDsa65>::try_from(bytes)
        .map_err(|_| BootstrapError::InvalidSigningPublicKey)?;
    Ok(MlDsaVerifyingKey::<MlDsa65>::decode(&encoded))
}

fn decode_mldsa_signature(bytes: &[u8]) -> Result<MlDsaSignature<MlDsa65>, BootstrapError> {
    MlDsaSignature::<MlDsa65>::try_from(bytes).map_err(|_| BootstrapError::InvalidSignatureEncoding)
}

fn decode_kem_public_key(bytes: &[u8]) -> Result<MlKemEncapsulationKey<MlKem768>, BootstrapError> {
    let key = ml_kem::Key::<MlKemEncapsulationKey<MlKem768>>::try_from(bytes)
        .map_err(|_| BootstrapError::InvalidKemPublicKey)?;
    MlKemEncapsulationKey::<MlKem768>::new(&key).map_err(|_| BootstrapError::InvalidKemPublicKey)
}

fn decode_kem_ciphertext(bytes: &[u8]) -> Result<MlKemCiphertext<MlKem768>, BootstrapError> {
    MlKemCiphertext::<MlKem768>::try_from(bytes).map_err(|_| BootstrapError::InvalidKemCiphertext)
}

fn mldsa_signing_key(seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN]) -> ml_dsa::SigningKey<MlDsa65> {
    MlDsa65::from_seed(&seed.into())
}

fn transcript_hash(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn shared_key_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out.copy_from_slice(bytes);
    out
}

fn kem_encapsulate_deterministic(
    public_key: &MlKemEncapsulationKey<MlKem768>,
    parts: &[&[u8]],
) -> (MlKemCiphertext<MlKem768>, ml_kem::SharedKey) {
    let seed = transcript_hash(parts);
    public_key.encapsulate_deterministic(&seed.into())
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
    use super::*;

    fn sample_scope() -> CredentialScope {
        CredentialScope {
            stream_id: Some(StreamId(7)),
            allow_client_to_server: true,
            allow_server_to_client: true,
        }
    }

    fn sample_server_identity() -> ServerIdentityBundle {
        issue_server_identity("server-a", 1_000, 10_000, [7; BOOTSTRAP_SIGNING_SEED_LEN]).unwrap()
    }

    fn sample_client_credential() -> IssuedClientCredential {
        issue_client_credential(
            &sample_server_identity(),
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "client-a",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(),
            1_000,
            2_000,
        )
        .unwrap()
    }

    #[test]
    fn bootstrap_message_round_trip_all_variants() {
        let config = BootstrapConfig::default();
        let messages = vec![
            BootstrapMessage::ClientHello(ClientHello {
                stream_id: StreamId(7),
                client_id: "client-a".to_string(),
                client_nonce: [1; BOOTSTRAP_NONCE_LEN],
                client_ephemeral_kem_public_key: vec![9; 32],
                client_credential: sample_client_credential().bundle,
            }),
            BootstrapMessage::ServerHello(ServerHello {
                stream_id: StreamId(7),
                server_id: "server-a".to_string(),
                server_nonce: [2; BOOTSTRAP_NONCE_LEN],
                server_ephemeral_kem_public_key: vec![8; 32],
                encapsulated_shared_secret_to_client: vec![7; 48],
                server_identity: sample_server_identity(),
            }),
            BootstrapMessage::ClientFinish(ClientFinish {
                stream_id: StreamId(7),
                encapsulated_shared_secret_to_server: vec![6; 48],
                transcript_signature: vec![5; 64],
            }),
            BootstrapMessage::ServerFinish(ServerFinish {
                stream_id: StreamId(7),
                transcript_signature: vec![4; 64],
            }),
        ];

        for message in messages {
            let frame = message.to_frame(&config).unwrap();
            let decoded = BootstrapMessage::from_frame(&frame, &config).unwrap();
            assert_eq!(decoded, message);
        }
    }

    #[test]
    fn client_credential_validation_rejects_expired_credentials() {
        let server_identity = sample_server_identity();
        let issued = issue_client_credential(
            &server_identity,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "client-a",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(),
            1_000,
            1_100,
        )
        .unwrap();

        assert!(matches!(
            validate_client_credential_bundle(
                &issued.bundle,
                &server_identity,
                StreamId(7),
                StreamDirection::ServerToClient,
                10_000,
                DEFAULT_CLOCK_SKEW_SECS,
            ),
            Err(BootstrapError::InvalidClientCredentialValidity)
        ));
    }

    #[test]
    fn client_credential_validation_rejects_wrong_scope() {
        let server_identity = sample_server_identity();
        let issued = issue_client_credential(
            &server_identity,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "client-a",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            CredentialScope {
                stream_id: Some(StreamId(99)),
                allow_client_to_server: true,
                allow_server_to_client: false,
            },
            1_000,
            2_000,
        )
        .unwrap();

        assert!(matches!(
            validate_client_credential_bundle(
                &issued.bundle,
                &server_identity,
                StreamId(7),
                StreamDirection::ServerToClient,
                1_100,
                DEFAULT_CLOCK_SKEW_SECS,
            ),
            Err(BootstrapError::CredentialScopeRejected { .. })
        ));
    }

    #[test]
    fn replay_cache_rejects_duplicate_triplets() {
        let mut cache = ReplayCache::default();
        let key = ReplayCacheKey {
            client_id: "client-a".to_string(),
            client_nonce: [1; BOOTSTRAP_NONCE_LEN],
            server_nonce: [2; BOOTSTRAP_NONCE_LEN],
        };
        cache.check_and_insert(key.clone(), 2_000, 1_500).unwrap();
        assert!(matches!(
            cache.check_and_insert(key, 2_000, 1_600),
            Err(BootstrapError::ReplayDetected)
        ));
    }

    #[test]
    fn derive_pq_mutual_root_is_deterministic() {
        let transcript_hash = [9; 32];
        let a = derive_pq_mutual_root(
            StreamId(7),
            StreamDirection::ServerToClient,
            &[1; 32],
            &[2; 32],
            &transcript_hash,
        );
        let b = derive_pq_mutual_root(
            StreamId(7),
            StreamDirection::ServerToClient,
            &[1; 32],
            &[2; 32],
            &transcript_hash,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn full_mutual_bootstrap_round_trip_completes() {
        let bootstrap = BootstrapConfig::default();
        let stream_id = StreamId(7);
        let direction = StreamDirection::ServerToClient;
        let server_identity =
            issue_server_identity("server-a", 1_000, 10_000, [7; BOOTSTRAP_SIGNING_SEED_LEN])
                .unwrap();
        let issued_credential = issue_client_credential(
            &server_identity,
            [7; BOOTSTRAP_SIGNING_SEED_LEN],
            "client-a",
            [3; BOOTSTRAP_SIGNING_SEED_LEN],
            sample_scope(),
            1_000,
            2_000,
        )
        .unwrap();
        let client_config = BootstrapClientConfig {
            stream_id,
            direction,
            bootstrap,
            security: ClientSecurityConfig::PqMutual {
                issued_credential,
                expected_server_identity: server_identity.clone(),
            },
        };
        let server_config = BootstrapServerConfig {
            stream_id,
            direction,
            bootstrap,
            security: ServerSecurityConfig::PqMutual {
                server_identity,
                server_signing_seed: [7; BOOTSTRAP_SIGNING_SEED_LEN],
                revoked_client_ids: Vec::new(),
            },
        };

        let (mut client, client_hello) =
            ClientBootstrapState::start(client_config, [1; BOOTSTRAP_NONCE_LEN], [2; 64]).unwrap();
        let mut replay_cache = ReplayCache::default();
        let server_response = ServerBootstrapState::respond_to_client_hello(
            server_config,
            &mut replay_cache,
            client_hello,
            1_500,
            [3; BOOTSTRAP_NONCE_LEN],
            [4; 64],
        )
        .unwrap();
        let server = server_response
            .state
            .expect("mutual bootstrap should produce server state");
        let client_progress = client
            .handle_server_hello(server_response.outbound, 1_500)
            .unwrap();
        let client_finish = client_progress
            .outbound
            .expect("mutual bootstrap should produce client finish");
        let (server_done, server_finish) = server.handle_client_finish(client_finish).unwrap();
        let client_done = client.handle_server_finish(server_finish).unwrap();

        assert_eq!(client_done.root, server_done.root);
        assert_eq!(client_done.client_id, "client-a");
        assert_eq!(server_done.server_id, "server-a");
    }

    #[test]
    fn simple_bootstrap_round_trip_completes() {
        let bootstrap = BootstrapConfig::default();
        let stream_id = StreamId(9);
        let direction = StreamDirection::ClientToServer;
        let client_config = BootstrapClientConfig {
            stream_id,
            direction,
            bootstrap,
            security: ClientSecurityConfig::PqSimple,
        };
        let server_config = BootstrapServerConfig {
            stream_id,
            direction,
            bootstrap,
            security: ServerSecurityConfig::PqSimple {
                bootstrap: PqSimpleServerBootstrapConfig {
                    server_id: "simple-server".to_string(),
                },
            },
        };

        let (mut client, client_hello) =
            ClientBootstrapState::start(client_config, [5; BOOTSTRAP_NONCE_LEN], [6; 64]).unwrap();
        let mut replay_cache = ReplayCache::default();
        let server_response = ServerBootstrapState::respond_to_client_hello(
            server_config,
            &mut replay_cache,
            client_hello,
            1_500,
            [7; BOOTSTRAP_NONCE_LEN],
            [8; 64],
        )
        .unwrap();
        assert!(server_response.state.is_none());
        let server_done = server_response
            .completed
            .expect("simple bootstrap should complete on server hello");
        let client_progress = client
            .handle_server_hello(server_response.outbound, 1_500)
            .unwrap();
        let client_done = client_progress
            .completed
            .expect("simple bootstrap should complete after server hello");

        assert_eq!(client_done.root, server_done.root);
        assert_eq!(client_done.server_id, "simple-server");
        assert_eq!(server_done.client_id, SIMPLE_DEFAULT_CLIENT_ID);
    }
}
