//! PqSimpleV1 bootstrap regression tests (condensed Waves 2-5).
//!
//! The end-to-end test in `ws_server_send.rs` proves the full PqSimpleV1
//! handshake works over a real WebSocket. These unit tests provide targeted
//! regression coverage for the bootstrap in isolation.

#![cfg(not(target_arch = "wasm32"))]

use shared_protocol::bootstrap::{
    BootstrapClientConfig, BootstrapConfig, BootstrapServerConfig, BOOTSTRAP_KEM_SEED_LEN,
    BOOTSTRAP_NONCE_LEN, ClientBootstrapState, ClientSecurityConfig, ReplayCache,
    ServerSecurityConfig,
};
use shared_protocol::{StreamDirection, StreamId};

fn make_client_config() -> BootstrapClientConfig {
    BootstrapClientConfig {
        stream_id: StreamId(1),
        direction: StreamDirection::ServerToClient,
        bootstrap: BootstrapConfig::default(),
        security: ClientSecurityConfig::PqSimple,
    }
}

fn make_server_config() -> BootstrapServerConfig {
    BootstrapServerConfig {
        stream_id: StreamId(1),
        direction: StreamDirection::ServerToClient,
        bootstrap: BootstrapConfig::default(),
        security: ServerSecurityConfig::PqSimple {
            bootstrap: shared_protocol::PqSimpleServerBootstrapConfig::default(),
        },
    }
}

#[test]
fn pq_simple_client_hello_construction() {
    let cfg = make_client_config();
    let mut nonce = [0u8; BOOTSTRAP_NONCE_LEN];
    let mut kem_seed = [0u8; BOOTSTRAP_KEM_SEED_LEN];
    nonce[0] = 1;
    kem_seed[0] = 2;
    let (_state, _client_hello) = ClientBootstrapState::start(cfg, nonce, kem_seed)
        .expect("client bootstrap start must succeed");
    // BootstrapMessage is a structured type, not a raw byte vec.
    // The end-to-end test in ws_server_send.rs verifies the full round-trip.
}

#[test]
fn pq_simple_bootstrap_round_trip_in_memory() {
    let client_cfg = make_client_config();
    let server_cfg = make_server_config();

    // Message 1: client_hello
    let mut nonce = [0u8; BOOTSTRAP_NONCE_LEN];
    let mut kem_seed = [0u8; BOOTSTRAP_KEM_SEED_LEN];
    nonce[0] = 1;
    kem_seed[0] = 2;
    let (mut client_state, client_hello) =
        ClientBootstrapState::start(client_cfg, nonce, kem_seed)
            .expect("client start must succeed");

    // Message 2: server processes client_hello, produces server_hello
    let mut replay_cache = ReplayCache::default();
    let mut server_nonce = [0u8; BOOTSTRAP_NONCE_LEN];
    let mut server_kem_seed = [0u8; BOOTSTRAP_KEM_SEED_LEN];
    server_nonce[0] = 3;
    server_kem_seed[0] = 4;
    let server_response = shared_protocol::bootstrap::ServerBootstrapState::respond_to_client_hello(
        server_cfg,
        &mut replay_cache,
        client_hello,
        0,
        server_nonce,
        server_kem_seed,
    )
    .expect("server respond_to_client_hello must succeed");

    let server_completed = server_response
        .completed
        .expect("PqSimple server should complete after server_hello");

    // Message 3: client processes server_hello
    let client_progress = client_state
        .handle_server_hello(server_response.outbound, 0)
        .expect("client handle_server_hello must succeed");

    let client_completed = client_progress
        .completed
        .expect("PqSimple client should complete after server_hello");

    // Both sides should have matching root keys.
    assert_eq!(
        client_completed.root, server_completed.root,
        "client and server must derive the same root key"
    );
    assert_eq!(
        client_completed.stream_id, server_completed.stream_id,
        "stream_id must match"
    );
}
