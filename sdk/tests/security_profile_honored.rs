//! Verify that `PulzzClient::connect_with_config` honors the caller's
//! `SecurityProfile` choice instead of silently mapping every profile to
//! `PqSimple` (bug #1).
//!
//! Only `PqSimpleV1` is wired through the SDK's network connect path today.
//! `PqMutualV1` and `ClassicRef1` must return `SdkError::InvalidArg` with a
//! clear message explaining what's missing, so callers fail fast instead of
//! silently getting a weaker security profile than they asked for.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{ClientConfig, PulzzClient, SecurityProfile, SdkError};

#[tokio::test]
async fn connect_with_pq_mutual_v1_returns_invalid_arg_error() {
    let cfg = ClientConfig {
        security: SecurityProfile::PqMutualV1,
        ..Default::default()
    };
    // Connecting to an unroutable address is fine — the SecurityProfile
    // check happens before any network I/O, so we should get InvalidArg
    // immediately, not a connection error.
    let err = PulzzClient::connect_with_config("ws://127.0.0.1:1", cfg)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SdkError::InvalidArg(_)),
        "expected SdkError::InvalidArg for PqMutualV1, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("PqMutualV1"),
        "error message should mention PqMutualV1, got: {msg}"
    );
}

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
