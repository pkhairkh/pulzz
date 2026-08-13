//! Configuration types for the pulzZ SDK.
//!
//! Mirrors `docs/SDK_PROPOSAL.md` §5.4. These types intentionally use plain
//! enums and POD structs so they can be cheaply cloned across builder calls
//! and easily projected into the FFI/WASM/Python/Go bindings later.

use std::time::Duration;

// Re-export the PqMutualV1 credential types from shared_protocol so callers
// don't have to depend on shared_protocol directly.
pub use shared_protocol::bootstrap::{IssuedClientCredential, ServerIdentityBundle};
use shared_protocol::bootstrap::BOOTSTRAP_SIGNING_SEED_LEN;

/// Credentials required for the PqMutualV1 (mutual PQ) handshake.
///
/// The client presents `issued_credential` (proving its identity to the
/// server via ML-DSA-65) and `expected_server_identity` (the server identity
/// it expects to authenticate). Both sides must agree on these for the
/// 4-message handshake (ClientHello → ServerHello → ClientFinish →
/// ServerFinish) to complete.
///
/// Use `shared_protocol::bootstrap::issue_client_credential(...)` to mint
/// a credential (typically by the server operator during provisioning).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqMutualV1Credentials {
    pub issued_credential: IssuedClientCredential,
    pub expected_server_identity: ServerIdentityBundle,
}

/// Server-side configuration for the PqMutualV1 handshake.
///
/// The server holds `server_identity` (its public identity bundle) +
/// `server_signing_seed` (the ML-DSA-65 secret seed used to sign during
/// the handshake) + `revoked_client_ids` (runtime revocation list).
///
/// Use `shared_protocol::bootstrap::issue_server_identity(...)` to mint
/// the identity bundle from a seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqMutualV1ServerConfig {
    pub server_identity: ServerIdentityBundle,
    pub server_signing_seed: [u8; BOOTSTRAP_SIGNING_SEED_LEN],
    pub revoked_client_ids: Vec<String>,
}

/// Selects which PQC (or classical reference) handshake to use.
///
/// `PqMutualV1` carries the client credentials required for the mutual-PQ
/// handshake. `PqSimpleV1` is the default (no credentials needed).
/// `ClassicRef1` is in-memory only (not wired through connect).
///
/// **Breaking change (v0.7):** `PqMutualV1` was a unit variant in v0.6;
/// it now carries `PqMutualV1Credentials`. Callers must update from
/// `.security(SecurityProfile::PqMutualV1)` to
/// `.security(SecurityProfile::PqMutualV1(creds))`.
///
/// `SecurityProfile` is no longer `Copy` (because `PqMutualV1Credentials`
/// contains `Vec<u8>`). Use `.clone()` where needed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SecurityProfile {
    /// ML-KEM-768 + ML-DSA-65 (full PQ mutual auth, 4-message handshake).
    /// Carries the client credentials required for the handshake.
    PqMutualV1(PqMutualV1Credentials),
    /// ML-KEM-768 only (PQ KEM, no signatures, 2-message handshake).
    /// Default — no credentials needed.
    #[default]
    PqSimpleV1,
    /// X25519 + Ed25519 (classical, for in-memory testing only).
    ClassicRef1,
}

/// Underlying transport carrier for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CarrierKind {
    #[default]
    WebSocket,
    Tcp,
    QuicStream,
    QuicDatagram,
    WebTransport,
    UdpDatagram,
}

/// Compression configuration for batched emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub batch_threshold: usize,
    pub zstd_level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_threshold: 10,
            zstd_level: 3,
        }
    }
}

impl CompressionConfig {
    /// WASM-friendly default: compression disabled.
    pub const fn wasm() -> Self {
        Self {
            enabled: false,
            batch_threshold: 0,
            zstd_level: 0,
        }
    }
}

/// Top-level SDK configuration. Applies to both client and server sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub security: SecurityProfile,
    pub carrier: CarrierKind,
    pub compression: CompressionConfig,
    pub batch_size: Option<usize>,
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            security: SecurityProfile::PqSimpleV1,
            carrier: CarrierKind::WebSocket,
            compression: CompressionConfig::default(),
            batch_size: None,
            timeout: Duration::from_secs(30),
        }
    }
}

impl ClientConfig {
    /// Convenience: returns the timeout in milliseconds (for FFI/WASM).
    pub fn timeout_ms(&self) -> u64 {
        self.timeout.as_millis() as u64
    }

    /// Build a `ClientConfig` from raw u64 milliseconds (FFI-friendly).
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout = Duration::from_millis(timeout_ms);
        self
    }
}
