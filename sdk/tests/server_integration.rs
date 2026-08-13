//! Integration tests for `PulzzServer`. Uses `classic_ref1_pair_from_rng`
//! to build an in-memory protector pair, then exercises the SDK surface
//! (`from_protector`, `emit_event`, `emit_batch`).

use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzServer, PulzzServerBuilder, SecurityProfile,
};
use shared_protocol::{ItemId, SourceKind, source::ExactStateMaterial};

fn default_test_config() -> ClientConfig {
    ClientConfig {
        security: SecurityProfile::ClassicRef1,
        carrier: CarrierKind::WebSocket,
        compression: CompressionConfig::default(),
        batch_size: None,
        timeout: std::time::Duration::from_secs(5),
    }
}

#[test]
fn server_emit_event_produces_exact_state_record() {
    let stream_id = shared_protocol::StreamId(92001);
    let (sender, _receiver) =
        pulzz_sdk::classic_pair_for_test(stream_id);
    let mut server = PulzzServer::from_protector(sender, default_test_config());

    let payload = b"server-side-payload".to_vec();
    let material = ExactStateMaterial::new(SourceKind::Text, payload);
    let record = server
        .emit_event(server::ServerEvent::Insert {
            item_id: ItemId(1),
            block: material,
        })
        .expect("emit_event must succeed");
    assert_eq!(
        record.header.record_type,
        shared_protocol::RecordType::ExactState
    );
    assert_eq!(record.header.item_id, ItemId(1));
}

#[test]
fn server_emit_batch_produces_batch_envelope_record() {
    let stream_id = shared_protocol::StreamId(92002);
    let (sender, _receiver) = pulzz_sdk::classic_pair_for_test(stream_id);
    let mut server = PulzzServer::from_protector(sender, default_test_config());

    let items: Vec<(ItemId, ExactStateMaterial)> = (1..=10)
        .map(|i| {
            let payload = format!("batch item {}", i).into_bytes();
            (ItemId(i), ExactStateMaterial::new(SourceKind::Text, payload))
        })
        .collect();
    let record = server.emit_batch(items).expect("emit_batch must succeed");
    assert_eq!(
        record.header.record_type,
        shared_protocol::RecordType::BatchEnvelope
    );
}

#[test]
fn server_builder_produces_expected_defaults() {
    let b = PulzzServerBuilder::default();
    assert_eq!(b.carrier, CarrierKind::WebSocket);
    assert_eq!(b.security, SecurityProfile::PqSimpleV1);
    assert_eq!(b.bind_addr, "0.0.0.0:0");
    assert_eq!(b.timeout_ms, 30_000);
}

#[tokio::test]
async fn server_bind_test_listener_succeeds() {
    // bind_test_listener is a smoke-test helper that does NOT go through the
    // full NativeServerAcceptor bootstrap path.
    let (listener, addr) = PulzzServer::bind_test_listener()
        .await
        .expect("bind_test_listener must succeed");
    assert!(!addr.is_empty(), "must bind to a non-empty address");
    drop(listener);
}

#[test]
fn server_session_mut_allerts_to_inner_state() {
    let stream_id = shared_protocol::StreamId(92003);
    let (sender, _receiver) = pulzz_sdk::classic_pair_for_test(stream_id);
    let mut server = PulzzServer::from_protector(sender, default_test_config());
    // Read-only access to inner ServerSession
    let _ = server.session().state();
    let _ = server.session_mut().state_mut();
}
