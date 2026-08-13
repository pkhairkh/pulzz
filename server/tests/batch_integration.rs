//! Wave 13 Task 13-d: Item batching integration tests.
//!
//! Verifies that:
//! 1. Server can emit a BatchEnvelope record wrapping N items.
//! 2. Client can decode and cache all items in the batch.
//! 3. Wire bytes for batched emission < wire bytes for per-item emission.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::{RecordType, StreamId};
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(12000);

fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

#[test]
fn batch_envelope_record_type_is_correct() {
    // Wave 13 T-13-d: emit_batch must produce a BatchEnvelope record.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(13001);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    let items: Vec<(ItemId, ExactStateMaterial)> = (1..=5)
        .map(|i| {
            let payload = format!("batch item {} with some content", i).into_bytes();
            (
                ItemId(i),
                ExactStateMaterial::new(SourceKind::Text, payload),
            )
        })
        .collect();

    let record = session.emit_batch(items).expect("emit_batch must succeed");
    assert_eq!(record.header.record_type, RecordType::BatchEnvelope);
}

#[test]
fn batch_round_trips_through_client() {
    // Wave 13 T-13-d: the client must decode the BatchEnvelope, decompress
    // each item, and cache all of them.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(13002);
    let (sender, receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);
    let mut client = client::ClientSession::new(receiver);

    let items: Vec<(ItemId, ExactStateMaterial)> = (1..=10)
        .map(|i| {
            let payload = format!("batch item {} payload content here", i).into_bytes();
            (
                ItemId(i),
                ExactStateMaterial::new(SourceKind::Text, payload),
            )
        })
        .collect();

    let record = session.emit_batch(items).expect("emit_batch must succeed");
    let result = client.apply_protected_record(record);
    assert!(
        result.is_ok(),
        "client must apply batch record; got: {:?}",
        result.err()
    );

    // All 10 items should be in the client cache.
    for i in 1..=10 {
        assert!(
            client.state().cache_entry(ItemId(i)).is_some(),
            "client must have item {} cached after batch decode",
            i
        );
    }
}

#[test]
fn batch_wire_bytes_smaller_than_per_item_emission() {
    // Wave 13 T-13-d: the wire bytes for a batched emission must be smaller
    // than the wire bytes for per-item emission of the same items.
    //
    // This is the central value proposition of batching: amortize the per-item
    // AEAD tag + record header + transport envelope overhead (~94 B) across
    // all items in the batch.
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(13003);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    let items: Vec<(ItemId, ExactStateMaterial)> = (1..=20)
        .map(|i| {
            let payload = format!("item {} ", i).into_bytes();
            (
                ItemId(i),
                ExactStateMaterial::new(SourceKind::Text, payload),
            )
        })
        .collect();

    // Emit as a batch.
    let batch_record = session
        .emit_batch(items.clone())
        .expect("emit_batch must succeed");
    let batch_wire_bytes = batch_record.to_bytes().len();

    // Emit individually.
    let mut per_item_wire_bytes = 0usize;
    for (item_id, material) in items {
        let record = session
            .emit_event(ServerEvent::Insert {
                item_id,
                block: material,
            })
            .expect("emit_event must succeed");
        per_item_wire_bytes += record.to_bytes().len();
    }

    assert!(
        batch_wire_bytes < per_item_wire_bytes,
        "batch wire bytes ({}) must be < per-item wire bytes ({}); savings = {:.1}%",
        batch_wire_bytes,
        per_item_wire_bytes,
        (1.0 - batch_wire_bytes as f64 / per_item_wire_bytes as f64) * 100.0
    );
}

#[test]
fn empty_batch_can_be_emitted() {
    // Wave 13 T-13-d: an empty batch should still produce a valid record
    // (edge case — the envelope encodes an empty item list).
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(13004);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);
    let record = session
        .emit_batch(Vec::<(ItemId, ExactStateMaterial)>::new())
        .expect("emit_batch must succeed");
    assert_eq!(record.header.record_type, RecordType::BatchEnvelope);
}
