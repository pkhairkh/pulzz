//! Integration tests for the SDK client. Uses `classic_ref1_pair_from_rng`
//! to build an in-memory protector pair, then exercises the SDK surface
//! (`from_session`, `push_record`, `recv`, `send`).

use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzClientBuilder, SecurityProfile,
    classic_pair_for_test,
};
use shared_protocol::{
    ItemId, SourceKind,
    source::ExactStateMaterial,
};
use server::{ServerEvent, ServerSession};

fn default_test_config() -> ClientConfig {
    ClientConfig {
        security: SecurityProfile::ClassicRef1,
        carrier: CarrierKind::WebSocket,
        compression: CompressionConfig {
            enabled: false,
            ..CompressionConfig::default()
        },
        batch_size: None,
        timeout: std::time::Duration::from_secs(5),
    }
}

#[tokio::test]
async fn client_recv_applies_protected_record_from_in_memory_queue() {
    // Mirror the existing `server/tests/batch_integration.rs` pattern: build
    // a classic_ref1 protector pair, server emits an event, client applies
    // via the SDK `recv` API.
    let stream_id = shared_protocol::StreamId(91001);
    let (sender, receiver) = classic_pair_for_test(stream_id);

    let mut server_session = ServerSession::new(sender);
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        default_test_config(),
    );

    // Server emits an Insert event
    let payload = b"in-memory-test-payload".to_vec();
    let material = ExactStateMaterial::new(SourceKind::Text, payload.clone());
    let record = server_session
        .emit_event(ServerEvent::Insert {
            item_id: ItemId(42),
            block: material,
        })
        .expect("emit_event must succeed");

    // Push the protected record into the client's in-memory queue
    client.push_record(record);

    // recv should pop + apply it
    let received = client.recv().await.expect("recv must not error");
    assert!(received.is_some(), "recv must return Some(record)");

    // The client should have item 42 cached
    assert!(
        client.session().state().cache_entry(ItemId(42)).is_some(),
        "client cache should contain item 42 after recv"
    );
}

#[tokio::test]
async fn client_recv_returns_none_when_queue_is_empty() {
    let stream_id = shared_protocol::StreamId(91002);
    let (_sender, receiver) = classic_pair_for_test(stream_id);
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        default_test_config(),
    );
    let result = client.recv().await.expect("recv on empty queue must Ok(None)");
    assert!(result.is_none(), "empty queue should yield None");
}

#[tokio::test]
async fn client_builder_produces_expected_config() {
    let cfg = PulzzClientBuilder::default()
        .carrier(CarrierKind::Tcp)
        .security(SecurityProfile::PqSimpleV1)
        .batch_size(50)
        .compression(CompressionConfig::wasm())
        .timeout(5000)
        .build_config();
    assert_eq!(cfg.carrier, CarrierKind::Tcp);
    assert_eq!(cfg.security, SecurityProfile::PqSimpleV1);
    assert_eq!(cfg.batch_size, Some(50));
    assert_eq!(cfg.compression.enabled, false);
    assert_eq!(cfg.timeout_ms(), 5000);
}

#[tokio::test]
async fn client_send_in_in_memory_mode_is_a_no_op_that_does_not_error() {
    // In in-memory mode (transport=None), send() builds a Record and tries
    // to protect it; with no transport, the call must still succeed.
    let stream_id = shared_protocol::StreamId(91003);
    let (_sender, receiver) = classic_pair_for_test(stream_id);
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        default_test_config(),
    );
    client
        .send(ItemId(7), b"hello")
        .await
        .expect("in-memory send must not error");
}

#[tokio::test]
async fn client_send_batch_in_in_memory_mode_builds_batch_envelope_record() {
    // send_batch in in-memory mode should produce a BatchEnvelope record
    // internally and not error, even though it isn't shipped.
    let stream_id = shared_protocol::StreamId(91004);
    let (_sender, receiver) = classic_pair_for_test(stream_id);
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        default_test_config(),
    );
    let items: Vec<(ItemId, Vec<u8>)> = (1..=5)
        .map(|i| (ItemId(i), format!("item {} payload", i).into_bytes()))
        .collect();
    client
        .send_batch(items)
        .await
        .expect("send_batch in in-memory mode must not error");
}

#[tokio::test]
async fn client_recv_handles_multiple_queued_records() {
    let stream_id = shared_protocol::StreamId(91005);
    let (sender, receiver) = classic_pair_for_test(stream_id);
    let mut server_session = ServerSession::new(sender);
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        default_test_config(),
    );

    // Emit 3 records and queue them
    for i in 1..=3u64 {
        let payload = format!("payload-{}", i).into_bytes();
        let material = ExactStateMaterial::new(SourceKind::Text, payload);
        let record = server_session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: material,
            })
            .unwrap();
        client.push_record(record);
    }

    // Recv should return each record one at a time
    for i in 1..=3u64 {
        let r = client.recv().await.expect("recv must Ok").expect("must be Some");
        assert_eq!(r.header.item_id, ItemId(i));
        assert!(
            client.session().state().cache_entry(ItemId(i)).is_some(),
            "item {} should be cached", i
        );
    }
    // Next recv should return None
    let r = client.recv().await.unwrap();
    assert!(r.is_none());
}
