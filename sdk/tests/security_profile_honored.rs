//! Verify that `PulzzClient::connect_with_config` honors the caller's
//! `SecurityProfile` choice instead of silently mapping every profile to
//! `PqSimple` (bug #1).
//!
//! As of v0.7, `PqMutualV1` IS wired through connect_with_config (carries
//! credentials). Only `ClassicRef1` still returns `SdkError::InvalidArg`
//! (in-memory only, no network path).

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{ClientConfig, PulzzClient, SecurityProfile, SdkError};

#[tokio::test]
async fn connect_with_classic_ref1_returns_invalid_arg_error() {
    let cfg = ClientConfig {
        security: SecurityProfile::ClassicRef1,
        ..Default::default()
    };
    let err = PulzzClient::connect_with_config("ws://127.0.0.1:1", cfg)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SdkError::InvalidArg(_)),
        "expected SdkError::InvalidArg for ClassicRef1, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("ClassicRef1"),
        "error message should mention ClassicRef1, got: {msg}"
    );
}
