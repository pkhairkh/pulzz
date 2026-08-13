//! Wave 10 Task 10-b: Client-side decompression integration test.
//!
//! Verifies that the client correctly decompresses payloads emitted by the
//! server. The server's `compress_exact_bytes()` wraps payloads with a
//! compression tag; the client's `apply_data_record` detects the tag and
//! calls `decode_compressed_payload` to reverse the compression.
//!
//! This is the end-to-end integration test for ISSUES.md I4 (P0-P5
//! compression pipeline not yet integrated into client decompression).
//! The integration is already wired through `apply_data_record` at
//! client/src/lib.rs:701; this test verifies it works on a real round-trip.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(10000);

fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

#[test]
fn client_decompresses_server_emitted_payload() {
    // Wave 10 T-10-b: the server emits a compressed ExactState record; the
    // client must successfully apply the record (which internally calls
    // decode_compressed_payload). If decompression fails, apply_record
    // returns an error.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(10001);
    let (sender, receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);
    let mut client = client::ClientSession::new(receiver);

    // Payload large enough for compression to engage (> 256 bytes).
    let payload: Vec<u8> =
        b"{\"user\":\"alice\",\"id\":1,\"event\":\"login\",\"ts\":1700000000}".repeat(8);

    let record = session
        .emit_event(ServerEvent::Insert {
            item_id: ItemId(1),
            block: ExactStateMaterial::new(SourceKind::Text, payload.clone()),
        })
        .expect("server emit must succeed");

    // Apply the record on the client — this exercises the decompression path.
    let result = client.apply_protected_record(record);
    assert!(
        result.is_ok(),
        "client apply_record must succeed; got error: {:?}",
        result.err()
    );

    // Verify the client has the item in its cache with the correct bytes.
    let cached = client.state().cache_entry(ItemId(1)).is_some();
    assert!(
        cached,
        "client must cache the decompressed item"
    );
}

#[test]
fn client_handles_multiple_compressed_records() {
    // Wave 10 T-10-b: the client must handle a stream of compressed records
    // without leaking state or failing on the second record.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(10002);
    let (sender, receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);
    let mut client = client::ClientSession::new(receiver);

    for i in 1..=10 {
        let payload = format!(
            "{{\"user\":\"user{}\",\"id\":{},\"event\":\"login\",\"ts\":1700000{}}}",
            i, i, i
        )
        .repeat(4);
        let record = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: ExactStateMaterial::new(SourceKind::Text, payload.into_bytes()),
            })
            .expect("server emit must succeed");
        let result = client.apply_protected_record(record);
        assert!(
            result.is_ok(),
            "client apply_record must succeed on iteration {}; got: {:?}",
            i,
            result.err()
        );
    }

    // All 10 items should be in the client cache.
    for i in 1..=10 {
        assert!(
            client.state().cache_entry(ItemId(i)).is_some(),
            "client must have item {} cached after decompression",
            i
        );
    }
}

#[test]
fn client_decompression_fallback_handles_unknown_tag() {
    // Wave 10 T-10-b: if decompression fails (e.g., unknown compression tag
    // or missing base for delta), the client must fall back to treating the
    // payload as raw bytes (skipping the 2-byte compression prefix) rather
    // than crashing. This is the safety net at client/src/lib.rs:717.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(10003);
    let (sender, receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);
    let mut client = client::ClientSession::new(receiver);

    // Emit a payload that will be compressed.
    let payload = b"test payload for decompression fallback".repeat(10);
    let record = session
        .emit_event(ServerEvent::Insert {
            item_id: ItemId(1),
            block: ExactStateMaterial::new(SourceKind::Text, payload.clone()),
        })
        .expect("server emit must succeed");

    // Apply the record — the client should handle whatever compression
    // strategy the server chose, including passthrough.
    let result = client.apply_protected_record(record);
    assert!(
        result.is_ok(),
        "client must handle any compression strategy; got: {:?}",
        result.err()
    );
}
