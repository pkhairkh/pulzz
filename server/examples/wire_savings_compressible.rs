//! Wire savings with HIGHLY COMPRESSIBLE payloads.
//! Uses repeated JSON structures where zstd dictionary compression can engage.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(16000);
fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

fn run_compressible_comparison(item_payload_bytes: usize) {
    let n_items = 100;
    // Highly compressible JSON: same structure, only the id field changes.
    // This is the kind of data zstd dictionary compression excels at.
    let corpus: Vec<(ItemId, ExactStateMaterial)> = (1..=n_items)
        .map(|i| {
            let mut payload = Vec::new();
            while payload.len() < item_payload_bytes {
                payload.extend_from_slice(
                    format!(
                        "{{\"user\":\"alice\",\"id\":{},\"event\":\"page_view\",\"url\":\"/api/v1/users\",\"method\":\"GET\",\"status\":200,\"duration_ms\":42}}",
                        i
                    ).as_bytes(),
                );
            }
            payload.truncate(item_payload_bytes);
            (ItemId(i), ExactStateMaterial::new(SourceKind::Json, payload))
        })
        .collect();

    let total_original: usize = corpus.iter().map(|(_, m)| m.exact_bytes.len()).sum();

    // Path A: per-item
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);
    let mut per_item_wire = 0usize;
    for (item_id, material) in &corpus {
        let record = session
            .emit_event(ServerEvent::Insert {
                item_id: *item_id,
                block: material.clone(),
            })
            .unwrap();
        per_item_wire += record.to_bytes().len();
    }

    // Path B: single batch
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);
    let record = session.emit_batch(corpus.clone()).unwrap();
    let batch_wire = record.to_bytes().len();

    let per_item_savings = (1.0 - per_item_wire as f64 / total_original as f64) * 100.0;
    let batch_savings = (1.0 - batch_wire as f64 / total_original as f64) * 100.0;
    let improvement = (1.0 - batch_wire as f64 / per_item_wire as f64) * 100.0;

    println!(
        "| {} B/item | {} | {} | {:.1}% | {} | {:.1}% | {:.1}% |",
        item_payload_bytes,
        total_original,
        per_item_wire,
        per_item_savings,
        batch_wire,
        batch_savings,
        improvement
    );
}

fn main() {
    println!("=== pulzZ wire savings — HIGHLY COMPRESSIBLE JSON payloads ===");
    println!("100 items per run, repeated JSON structures (only id field changes)\n");
    println!("| Payload size | Original | Per-item wire | Per-item savings | Batched wire | Batched savings | Batch improvement |");
    println!("|---|---|---|---|---|---|---|");

    for &size in &[200, 500, 1000, 2000, 4000, 8000] {
        run_compressible_comparison(size);
    }

    println!();
    println!("Positive savings = batched wire < original bytes.");
}
