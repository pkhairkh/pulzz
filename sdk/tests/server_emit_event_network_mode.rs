//! Verify that `PulzzServer::emit_event` and `emit_batch` return
//! `SdkError::InvalidState` when the server is in network mode (bug #3).
//!
//! In network mode, the in-memory `ServerSession`'s protector is a throwaway
//! placeholder that does not match any accepted connection's protector.
//! Emitting records through it would produce frames that cannot be decoded
//! by the client. The methods must fail fast with a clear error.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{ClientConfig, PulzzServer, SdkError};
use server::ServerEvent;
use shared_protocol::{ItemId, SourceKind, source::ExactStateMaterial};

#[tokio::test]
async fn emit_event_in_network_mode_returns_invalid_state() {
    // Bind to an ephemeral port — this puts the server in network mode
    // (acceptor = Some(...)).
    let mut server = match PulzzServer::bind("127.0.0.1:0").await {
        Ok(s) => s,
        Err(e) => {
            // If the network bind fails (e.g. no network stack in the test
            // environment), skip this test rather than failing.
            eprintln!("WARN: could not bind server for test, skipping: {e}");
            return;
        }
    };
    assert!(
        server.session_acceptor_is_some(),
        "server should be in network mode after bind"
    );

    let material = ExactStateMaterial::new(SourceKind::Text, b"payload".to_vec());
    let err = server
        .emit_event(ServerEvent::Insert {
            item_id: ItemId(1),
            block: material,
        })
        .unwrap_err();
    assert!(
        matches!(err, SdkError::InvalidState(_)),
        "expected SdkError::InvalidState in network mode, got: {err:?}"
    );
}

#[tokio::test]
async fn emit_batch_in_network_mode_returns_invalid_state() {
    let mut server = match PulzzServer::bind("127.0.0.1:0").await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("WARN: could not bind server for test, skipping: {e}");
            return;
        }
    };

    let items = vec![(ItemId(1), ExactStateMaterial::new(SourceKind::Text, b"x".to_vec()))];
    let err = server.emit_batch(items).unwrap_err();
    assert!(
        matches!(err, SdkError::InvalidState(_)),
        "expected SdkError::InvalidState in network mode, got: {err:?}"
    );
}

#[tokio::test]
async fn emit_event_in_in_memory_mode_succeeds() {
    // In-memory mode (from_protector) should still work.
    let (sender, _receiver) = pulzz_sdk::classic_pair_for_test(shared_protocol::StreamId(42));
    let cfg = ClientConfig::default();
    let mut server = PulzzServer::from_protector(sender, cfg);
    assert!(
        !server.session_acceptor_is_some(),
        "server should be in in-memory mode after from_protector"
    );

    let material = ExactStateMaterial::new(SourceKind::Text, b"payload".to_vec());
    let result = server.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: material,
    });
    assert!(result.is_ok(), "emit_event should succeed in in-memory mode");
}
