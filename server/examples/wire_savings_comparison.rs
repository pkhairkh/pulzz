//! Direct comparison: per-item emission vs batched emission.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(14000);
fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

fn main() {
    println!("=== pulzZ wire savings comparison ===\n");

    let corpus: Vec<(ItemId, ExactStateMaterial)> = (1..=100)
        .map(|i| {
            let payload = format!(
                "{{\"id\":{},\"user\":\"user{}\",\"event\":\"login\",\"ts\":1700000{}}}",
                i, i, i % 10
            )
            .into_bytes();
            (ItemId(i), ExactStateMaterial::new(SourceKind::Json, payload))
        })
        .collect();

    let total_original: usize = corpus.iter().map(|(_, m)| m.exact_bytes.len()).sum();
    println!("Corpus: 100 items, {} total original bytes (avg {} B/item)", total_original, total_original / 100);
    println!();

    // Path A: per-item
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);

    let mut per_item_wire_bytes = 0usize;
    for (item_id, material) in &corpus {
        let record = session
            .emit_event(ServerEvent::Insert {
                item_id: *item_id,
                block: material.clone(),
            })
            .expect("emit must succeed");
        per_item_wire_bytes += record.to_bytes().len();
    }

    println!("Path A (per-item emission):");
    println!("  Wire bytes:     {}", per_item_wire_bytes);
    println!("  Wire savings:   {:.2}%", (1.0 - per_item_wire_bytes as f64 / total_original as f64) * 100.0);
    println!();

    // Path B: 10 batches of 10
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);

    let mut batched_wire_bytes = 0usize;
    for chunk in corpus.chunks(10) {
        let record = session
            .emit_batch(chunk.to_vec())
            .expect("emit_batch must succeed");
        batched_wire_bytes += record.to_bytes().len();
    }

    println!("Path B (10 batches of 10 items):");
    println!("  Wire bytes:     {}", batched_wire_bytes);
    println!("  Wire savings:   {:.2}%", (1.0 - batched_wire_bytes as f64 / total_original as f64) * 100.0);
    println!();

    // Path C: single batch of 100
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);

    let record = session
        .emit_batch(corpus.clone())
        .expect("emit_batch must succeed");
    let single_batch_wire_bytes = record.to_bytes().len();

    println!("Path C (single batch of 100 items):");
    println!("  Wire bytes:     {}", single_batch_wire_bytes);
    println!("  Wire savings:   {:.2}%", (1.0 - single_batch_wire_bytes as f64 / total_original as f64) * 100.0);
    println!();

    // Summary
    println!("=== Summary ===");
    println!("Original payload bytes:       {}", total_original);
    println!("Per-item wire bytes:          {}  (savings: {:.1}%)", per_item_wire_bytes, (1.0 - per_item_wire_bytes as f64 / total_original as f64) * 100.0);
    println!("Batched(10x10) wire bytes:    {}  (savings: {:.1}%)", batched_wire_bytes, (1.0 - batched_wire_bytes as f64 / total_original as f64) * 100.0);
    println!("Single-batch(100) wire bytes:  {}  (savings: {:.1}%)", single_batch_wire_bytes, (1.0 - single_batch_wire_bytes as f64 / total_original as f64) * 100.0);
    println!();
    println!("Batching improvement over per-item: {:.1}%",
        (1.0 - single_batch_wire_bytes as f64 / per_item_wire_bytes as f64) * 100.0
    );
}
