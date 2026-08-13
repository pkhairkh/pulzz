//! Wire savings comparison with VARYING payload sizes.
//! Tests 50B, 200B, 1KB, 4KB payloads to find the break-even point.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(15000);
fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

fn run_comparison(item_payload_bytes: usize) {
    let n_items = 100;
    let corpus: Vec<(ItemId, ExactStateMaterial)> = (1..=n_items)
        .map(|i| {
            // Repeat a JSON-like template to reach the target payload size.
            let template = format!(
                "{{\"id\":{},\"user\":\"user{}\",\"event\":\"login\",\"ts\":1700000000,\"data\":\"",
                i, i
            );
            let mut payload = template.into_bytes();
            let filler = b"abcdefghijklmnopqrstuvwxyz0123456789";
            while payload.len() < item_payload_bytes {
                payload.extend_from_slice(filler);
            }
            payload.truncate(item_payload_bytes);
            payload.extend_from_slice(b"\"}");
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
    println!("=== pulzZ wire savings vs payload size ===");
    println!("100 items per run, single-batch emission vs per-item emission\n");
    println!("| Payload size | Original | Per-item wire | Per-item savings | Batched wire | Batched savings | Batch improvement |");
    println!("|---|---|---|---|---|---|---|");

    for &size in &[50, 100, 200, 500, 1000, 2000, 4000] {
        run_comparison(size);
    }

    println!();
    println!("Break-even (0% savings) occurs where batched wire < original bytes.");
}
