//! Multi-emit in-memory round-trip test (Wave 2 T-2.c).
//!
//! The audit (1-a) found that `ServerSession::emit_event` / `emit_batch`
//! ALREADY correctly read `expected_seq_no()` via `header_context()` — no
//! hardcoded `SeqNo(0)`. This test confirms 5 sequential emits work
//! end-to-end through the in-memory path.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{ClientConfig, PulzzClient, PulzzServer, classic_pair_for_test};
use server::ServerEvent;
use shared_protocol::{ItemId, SourceKind, StreamId, source::ExactStateMaterial};
use client::ClientSession;

#[tokio::test]
async fn five_emits_round_trip_in_memory() {
    let stream_id = StreamId(70_001);
    let (sender, receiver) = classic_pair_for_test(stream_id);
    let cfg = ClientConfig::default();
    let mut server = PulzzServer::from_protector(sender, cfg.clone());
    let mut client = PulzzClient::from_session(ClientSession::new(receiver), cfg);

    // Emit 5 events sequentially. Use item_ids 1-5 (item_id=0 is reserved).
    for i in 1..=5u64 {
        let payload = format!("emit-{i}").into_bytes();
        let material = ExactStateMaterial::new(SourceKind::Text, payload);
        let record = server
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: material,
            })
            .expect(&format!("emit_event {i} must succeed"));
        client.push_record(record);
    }

    // Client receives all 5. Verify item_ids (the payload is AEAD-protected
    // and encoded, so we verify via the client's cache after apply).
    for i in 1..=5u64 {
        let record = client
            .recv()
            .await
            .expect(&format!("recv {i} errored"))
            .expect(&format!("recv {i} returned None"));
        assert_eq!(record.header.item_id, ItemId(i), "emit {i} item_id mismatch");
    }

    // Verify the client's cache has all 5 items.
    for i in 1..=5u64 {
        let cached = client
            .session()
            .state()
            .cache_entry(ItemId(i))
            .unwrap_or_else(|| panic!("item {i} should be cached after 5 emits"));
        let expected = format!("emit-{i}").into_bytes();
        assert_eq!(
            cached.exact_bytes(),
            &expected[..],
            "item {i} cached payload mismatch"
        );
    }
}

#[tokio::test]
async fn ten_emits_round_trip_in_memory() {
    // Stress test: 10 emits.
    let stream_id = StreamId(70_002);
    let (sender, receiver) = classic_pair_for_test(stream_id);
    let cfg = ClientConfig::default();
    let mut server = PulzzServer::from_protector(sender, cfg.clone());
    let mut client = PulzzClient::from_session(ClientSession::new(receiver), cfg);

    for i in 1..=10u64 {
        let payload = format!("ten-emit-{i}").into_bytes();
        let material = ExactStateMaterial::new(SourceKind::Text, payload);
        let record = server
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(200 + i),
                block: material,
            })
            .expect(&format!("emit {i}"));
        client.push_record(record);
    }

    for i in 1..=10u64 {
        let record = client
            .recv()
            .await
            .expect(&format!("recv {i}"))
            .expect(&format!("recv {i} None"));
        assert_eq!(record.header.item_id, ItemId(200 + i));
    }
}
