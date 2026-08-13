//! PqMutualV1 end-to-end WebSocket round-trip test (Wave 4 T-4.c).
//!
//! Provisions an IssuedClientCredential + ServerIdentityBundle using the
//! public test helpers (issue_server_identity, issue_client_credential),
//! then connects a PulzzClient with SecurityProfile::PqMutualV1(creds)
//! to a PulzzServer bound with bind_with_security, and round-trips a
//! record through the resulting mutual-PQ-protected session.
//!
//! The PqMutualV1 handshake is 4 messages / 2 RTTs:
//!   ClientHello → ServerHello → ClientFinish → ServerFinish
//! Both sides derive matching AEAD protectors from the ML-KEM-768 shared
//! secret + ML-DSA-65 signature verification.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{
    CarrierKind, ClientConfig, PqMutualV1Credentials, PqMutualV1ServerConfig, PulzzClient,
    PulzzServer, SecurityProfile,
};
use shared_protocol::{
    ItemId, StreamId,
    bootstrap::{
        issue_client_credential, issue_server_identity, CredentialScope,
        BOOTSTRAP_SIGNING_SEED_LEN,
    },
};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn provision_pq_mutual_v1_credentials(
) -> (PqMutualV1ServerConfig, PqMutualV1Credentials) {
    let server_signing_seed = [0xAA; BOOTSTRAP_SIGNING_SEED_LEN];
    let client_signing_seed = [0xBB; BOOTSTRAP_SIGNING_SEED_LEN];
    let now = now_secs();
    let server_id = "test-server-001".to_string();
    let client_id = "test-client-001".to_string();

    // 1. Issue server identity (self-signed).
    let server_identity = issue_server_identity(
        server_id.clone(),
        now,
        now + 86400,
        server_signing_seed,
    )
    .expect("issue_server_identity must succeed");

    // 2. Issue client credential (signed by server).
    let scope = CredentialScope {
        stream_id: Some(StreamId(1)),
        allow_client_to_server: true,
        allow_server_to_client: true,
    };
    let issued_credential = issue_client_credential(
        &server_identity,
        server_signing_seed,
        client_id,
        client_signing_seed,
        scope,
        now,
        now + 86400,
    )
    .expect("issue_client_credential must succeed");

    let server_config = PqMutualV1ServerConfig {
        server_identity: server_identity.clone(),
        server_signing_seed,
        revoked_client_ids: Vec::new(),
    };
    let client_creds = PqMutualV1Credentials {
        issued_credential,
        expected_server_identity: server_identity,
    };
    (server_config, client_creds)
}

#[tokio::test]
async fn pq_mutual_v1_round_trips_over_websocket() {
    // 1. Provision credentials.
    let (server_config, client_creds) = provision_pq_mutual_v1_credentials();

    // 2. Server binds with PqMutualV1.
    let server_cfg = ClientConfig {
        security: SecurityProfile::PqMutualV1(client_creds.clone()),
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut server = PulzzServer::bind_with_security(
        "127.0.0.1:0",
        server_cfg,
        Some(server_config),
    )
    .await
    .expect("server bind must succeed");
    let addr = server.local_addr().expect("local_addr must succeed");

    // 3. Spawn accept task.
    let accept_task = tokio::spawn(async move { server.accept().await });

    // 4. Client connects with PqMutualV1.
    let client_cfg = ClientConfig {
        security: SecurityProfile::PqMutualV1(client_creds),
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        PulzzClient::connect_with_config(&format!("ws://{addr}"), client_cfg),
    )
    .await
    .expect("client connect timed out")
    .expect("client connect must succeed");

    // 5. Get the accepted session.
    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        accept_task,
    )
    .await
    .expect("accept task timed out")
    .expect("accept task panicked")
    .expect("accept should not error")
    .expect("accept should return Some(session)");

    // 6. Server sends a record; client receives it.
    session
        .send(ItemId(42), b"hello-mutual-pq")
        .await
        .expect("session.send must succeed");

    let record = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.recv(),
    )
    .await
    .expect("client.recv timed out")
    .expect("client.recv errored")
    .expect("client.recv returned None");

    assert_eq!(record.header.item_id, ItemId(42));
    // Payload has SourceKind::Binary (3) prefix byte from DirectExact encoding.
    assert_eq!(record.payload[0], 3, "source kind prefix should be Binary (3)");
    assert_eq!(&record.payload[1..], b"hello-mutual-pq");

    // 7. Cleanup.
    let _ = client.close().await;
    let _ = session.close().await;
}

#[tokio::test]
async fn pq_mutual_v1_multi_record_round_trip() {
    // Verify multi-record works with PqMutualV1 too (combines Deliverable #1 + #2).
    let (server_config, client_creds) = provision_pq_mutual_v1_credentials();

    let server_cfg = ClientConfig {
        security: SecurityProfile::PqMutualV1(client_creds.clone()),
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut server = PulzzServer::bind_with_security(
        "127.0.0.1:0",
        server_cfg,
        Some(server_config),
    )
    .await
    .expect("bind");
    let addr = server.local_addr().expect("local_addr");
    let accept_task = tokio::spawn(async move { server.accept().await });

    let client_cfg = ClientConfig {
        security: SecurityProfile::PqMutualV1(client_creds),
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        PulzzClient::connect_with_config(&format!("ws://{addr}"), client_cfg),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    let mut session = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        accept_task,
    )
    .await
    .expect("accept timeout")
    .expect("accept")
    .expect("accept")
    .expect("accept Some");

    // Send 5 records.
    for i in 1..=5u64 {
        let payload = format!("mutual-{i}").into_bytes();
        session.send(ItemId(i), &payload)
            .await
            .expect(&format!("send {i}"));
    }

    // Receive all 5.
    for i in 1..=5u64 {
        let expected = format!("mutual-{i}").into_bytes();
        let record = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.recv(),
        )
        .await
        .expect(&format!("recv {i} timeout"))
        .expect(&format!("recv {i} error"))
        .expect(&format!("recv {i} None"));
        assert_eq!(record.header.item_id, ItemId(i));
        assert_eq!(&record.payload[1..], &expected);
    }

    let _ = client.close().await;
    let _ = session.close().await;
}
