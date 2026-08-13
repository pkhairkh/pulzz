//! Multi-record WebSocket round-trip test (Wave 1 T-1.d).
//!
//! Verifies that multiple records can be sent sequentially through a single
//! PulzzSession over a real WebSocket. This is the seq_no management DoD:
//! before the fix, only the first record succeeded (hardcoded SeqNo(0));
//! after the fix, the ratchet advances 0->1->2->3->4 and all 5 records
//! round-trip.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::{CarrierKind, ClientConfig, PulzzClient, PulzzServer, SecurityProfile};
use shared_protocol::ItemId;

#[tokio::test]
async fn five_records_round_trip_over_websocket() {
    // 1. Server binds with PqSimpleV1.
    let mut server = PulzzServer::bind("127.0.0.1:0").await.expect("bind must succeed");
    let addr = server.local_addr().expect("local_addr must succeed");

    // 2. Spawn accept task.
    let accept_task = tokio::spawn(async move { server.accept().await });

    // 3. Client connects with PqSimpleV1 over WebSocket.
    let cfg = ClientConfig {
        security: SecurityProfile::PqSimpleV1,
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut client = PulzzClient::connect_with_config(&format!("ws://{addr}"), cfg)
        .await
        .expect("connect must succeed");

    // 4. Get the accepted session.
    let mut session = accept_task
        .await
        .expect("accept task should not panic")
        .expect("accept should not error")
        .expect("accept should return Some(session)");

    // 5. Send 5 records sequentially from server to client.
    // Use item_ids 1..=5 (item_id=0 is reserved — triggers InvalidDataPayloadContract).
    for i in 1..=5u64 {
        let payload = format!("record-{i}").into_bytes();
        session
            .send(ItemId(i), &payload)
            .await
            .expect(&format!("send record {i} must succeed"));
    }

    // 6. Client receives all 5 records.
    for i in 1..=5u64 {
        let expected_payload = format!("record-{i}").into_bytes();
        let record = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.recv(),
        )
        .await
        .expect(&format!("recv record {i} timed out"))
        .expect(&format!("recv record {i} errored"))
        .expect(&format!("recv record {i} returned None"));
        assert_eq!(record.header.item_id, ItemId(i), "record {i} item_id mismatch");
        // Payload has SourceKind::Binary (3) prefix byte from DirectExact encoding.
        assert_eq!(record.payload[0], 3, "record {i} source kind prefix should be Binary (3)");
        assert_eq!(&record.payload[1..], &expected_payload, "record {i} payload mismatch");
    }

    // 7. Cleanup.
    let _ = client.close().await;
    let _ = session.close().await;
}

#[tokio::test]
async fn ten_records_round_trip_over_websocket() {
    // Stress test: 10 records to make sure the ratchet advances past 5.
    let mut server = PulzzServer::bind("127.0.0.1:0").await.expect("bind");
    let addr = server.local_addr().expect("local_addr");
    let accept_task = tokio::spawn(async move { server.accept().await });

    let cfg = ClientConfig {
        security: SecurityProfile::PqSimpleV1,
        carrier: CarrierKind::WebSocket,
        ..Default::default()
    };
    let mut client = PulzzClient::connect_with_config(&format!("ws://{addr}"), cfg)
        .await
        .expect("connect");

    let mut session = accept_task
        .await
        .expect("accept task")
        .expect("accept")
        .expect("accept Some");

    for i in 0..10u64 {
        let payload = format!("ten-{i}").into_bytes();
        session.send(ItemId(100 + i), &payload)
            .await
            .expect(&format!("send {i}"));
    }

    for i in 0..10u64 {
        let expected = format!("ten-{i}").into_bytes();
        let record = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.recv(),
        )
        .await
        .expect(&format!("recv {i} timeout"))
        .expect(&format!("recv {i} error"))
        .expect(&format!("recv {i} None"));
        assert_eq!(record.header.item_id, ItemId(100 + i));
        assert_eq!(&record.payload[1..], &expected);
    }

    let _ = client.close().await;
    let _ = session.close().await;
}
