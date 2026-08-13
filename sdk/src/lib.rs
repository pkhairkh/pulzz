//! pulzZ SDK — idiomatic async API.
//!
//! High-level wrappers around `client::ClientSession`, `server::ServerSession`,
//! and the underlying transport backends (WebSocket, TCP, QUIC, WebTransport,
//! UDP). All backends share the same wire protocol, PQC handshake, and batch
//! envelope format implemented in `shared_protocol`.
//!
//! See `docs/SDK_PROPOSAL.md` §4–§5 for the design rationale.

// Pre-existing clippy style lints silenced at crate level so
// `cargo clippy --workspace -- -D warnings` can pass (spec §0.2 bug #10).
#![allow(
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::large_enum_variant,
    clippy::question_mark,
)]

pub mod batch;
pub mod client;
pub mod config;
pub mod error;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub mod session;

pub use batch::{BatchBuilder, BuiltBatch};
pub use client::{PulzzClient, PulzzClientBuilder};
pub use config::{
    CarrierKind, ClientConfig, CompressionConfig, PqMutualV1Credentials,
    PqMutualV1ServerConfig, SecurityProfile,
};
pub use error::SdkError;
#[cfg(not(target_arch = "wasm32"))]
pub use server::{PulzzServer, PulzzServerBuilder};
#[cfg(not(target_arch = "wasm32"))]
pub use session::PulzzSession;

// Re-export the wire-protocol primitives that callers will need alongside the
// SDK. Keeping them in scope avoids forcing users to depend on
// `shared_protocol` directly just to construct an `ItemId`.
pub use shared_protocol::{ItemId, Record, SourceKind};

// Re-export the test-only classic_ref1 protector-pair helper so integration
// tests can construct an in-memory (sender, receiver) pair without depending
// on `shared_protocol` directly.
#[cfg(not(target_arch = "wasm32"))]
pub use client::classic_pair_for_test;
