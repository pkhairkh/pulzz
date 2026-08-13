//! Configuration types for the pulzZ SDK.
//!
//! Mirrors `docs/SDK_PROPOSAL.md` §5.4. These types intentionally use plain
//! enums and POD structs so they can be cheaply cloned across builder calls
//! and easily projected into the FFI/WASM/Python/Go bindings later.

use std::time::Duration;

/// Selects which PQC (or classical reference) handshake to use.
///
/// `PqMutualV1` requires native crypto (x25519-dalek + ml-dsa) and is not
/// available in WASM builds. `PqSimpleV1` is the WASM default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecurityProfile {
    /// ML-KEM-768 + ML-DSA-65 (full PQ mutual auth).
    #[default]
    PqMutualV1,
    /// ML-KEM-768 only (PQ KEM, no signatures).
    PqSimpleV1,
    /// X25519 + Ed25519 (classical, for testing only).
    ClassicRef1,
}

/// Underlying transport carrier for a session.
///
/// Not all carriers are available in every build target — see
/// `docs/SDK_PROPOSAL.md` §11 (backend parity matrix). On WASM only
/// `WebSocket` and `WebTransport` are usable; the others return
/// `SdkError::UnsupportedCarrier` at connect time.
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
///
/// On WASM `enabled` should be `false` (zstd-sys requires clang for
/// wasm32). The batch envelope still works; payloads are uncompressed.
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
            security: SecurityProfile::PqMutualV1,
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
