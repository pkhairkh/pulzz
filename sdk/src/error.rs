//! SDK error type. Maps every underlying error domain (`client::ClientApplyError`,
//! `client::ClientConnectError`, `server::ServerError`, `shared_protocol::WireError`,
//! `std::io::Error`) into a single `SdkError`. This keeps the public SDK API
//! narrow and prevents callers from having to derive `Error` on every wrapper.

use std::io;

use shared_protocol::{CodecError, ProtectionError, SourceError, StateProgramError, WireError};

/// One error type for the entire SDK.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("invalid argument: {0}")]
    InvalidArg(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("carrier {carrier:?} is not available on this target")]
    UnsupportedCarrier { carrier: &'static str },

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("compression failed: {0}")]
    CompressionFailed(String),

    #[error("operation timed out after {0} ms")]
    Timeout(u64),

    #[error("buffer too small: needed {needed}, had {had}")]
    BufferTooSmall { needed: usize, had: usize },

    #[error("internal SDK error: {0}")]
    Internal(String),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Wire(#[from] WireError),

    #[error(transparent)]
    Protection(#[from] ProtectionError),

    #[error(transparent)]
    Codec(#[from] CodecError),

    #[error(transparent)]
    Source(#[from] SourceError),

    #[error(transparent)]
    StateProgram(#[from] StateProgramError),

    /// Wraps the client crate's apply-time errors (validation, decode,
    /// predictive dispatch, assembly cache, etc.).
    #[error("client apply error: {0}")]
    ClientApply(String),

    /// Wraps the client crate's connect-time errors (transport, bootstrap,
    /// handshake, security profile mismatch, etc.).
    #[error("client connect error: {0}")]
    ClientConnect(String),

    #[error("server error: {0}")]
    Server(String),
}

impl SdkError {
    /// Convenience constructor for the `InvalidArg` variant.
    pub fn invalid_arg(msg: impl Into<String>) -> Self {
        Self::InvalidArg(msg.into())
    }

    /// Convenience constructor for the `InvalidState` variant.
    pub fn invalid_state(msg: impl Into<String>) -> Self {
        Self::InvalidState(msg.into())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<client::ClientApplyError> for SdkError {
    fn from(err: client::ClientApplyError) -> Self {
        match err {
            client::ClientApplyError::Validation(v) => SdkError::Wire(v.into()),
            client::ClientApplyError::TransportWire(w) => SdkError::Wire(w),
            client::ClientApplyError::Protection(p) => SdkError::Protection(p),
            client::ClientApplyError::DecodePayload(c) => SdkError::Codec(c),
            client::ClientApplyError::StateWire(s) => SdkError::StateProgram(s),
            client::ClientApplyError::Catalog(c) => SdkError::Internal(c.to_string()),
            other => SdkError::ClientApply(other.to_string()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<client::ClientConnectError> for SdkError {
    fn from(err: client::ClientConnectError) -> Self {
        match err {
            client::ClientConnectError::Io(io) => SdkError::Io(io),
            client::ClientConnectError::Bootstrap(b) => {
                SdkError::HandshakeFailed(b.to_string())
            }
            client::ClientConnectError::Apply(a) => SdkError::from(a),
            client::ClientConnectError::HandshakeTimeout(ms) => SdkError::Timeout(ms),
            client::ClientConnectError::SecurityProfileMismatch { .. } => {
                SdkError::HandshakeFailed(err.to_string())
            }
            other => SdkError::ClientConnect(other.to_string()),
        }
    }
}

impl From<server::ServerError> for SdkError {
    fn from(err: server::ServerError) -> Self {
        match err {
            server::ServerError::Validation(v) => SdkError::Wire(v.into()),
            server::ServerError::Codec(c) => SdkError::Codec(c),
            server::ServerError::StateProgram(s) => SdkError::StateProgram(s),
            server::ServerError::Protection(p) => SdkError::Protection(p),
            server::ServerError::ValidationPayload(w) => SdkError::Wire(w),
            other => SdkError::Server(other.to_string()),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<server::transport::TransportError> for SdkError {
    fn from(err: server::transport::TransportError) -> Self {
        SdkError::ConnectionFailed(err.to_string())
    }
}
