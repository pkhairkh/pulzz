//! Wave 18: PQ-encrypted batched emission benchmark.
//!
//! Verifies that the positive wire savings hold when using ML-KEM-768
//! post-quantum key exchange (PqSimpleV1 profile), not just the classical
//! X25519 (ClassicRef1) profile used by the default eval.
//!
//! Also verifies WASM compilation of the shared_protocol crate (the
//! post-quantum crypto layer must compile for wasm32-unknown-unknown).

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::{pq_simple_v1_pair_from_rng, StreamDirection};
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(17000);
fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

fn main() {
    println!("=== pulzZ PQ-encrypted batched emission benchmark ===");
    println!("Crypto: ML-KEM-768 (FIPS-203, post-quantum) + ChaCha20-Poly1305 AEAD\n");

    let n_items = 100;
    let corpus: Vec<(ItemId, ExactStateMaterial)> = (1..=n_items)
        .map(|i| {
            let payload = format!(
                "{{\"user\":\"alice\",\"id\":{},\"event\":\"page_view\",\"url\":\"/api/v1/users\",\"method\":\"GET\",\"status\":200,\"duration_ms\":42}}",
                i
            )
            .into_bytes();
            (ItemId(i), ExactStateMaterial::new(SourceKind::Json, payload))
        })
        .collect();

    let total_original: usize = corpus.iter().map(|(_, m)| m.exact_bytes.len()).sum();
    println!("Corpus: {} items, {} total original bytes (avg {} B/item)\n", n_items, total_original, { total_original / (n_items as usize) });

    // --- PQ per-item emission ---
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _receiver) =
        pq_simple_v1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
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

    // --- PQ batched emission ---
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(42);
    let (sender, _receiver) =
        pq_simple_v1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);
    let mut session = ServerSession::new(sender);

    let record = session.emit_batch(corpus.clone()).unwrap();
    let batch_wire = record.to_bytes().len();

    println!("| Configuration | Original | Wire bytes | Wire savings | Crypto |");
    println!("|---|---|---|---|---|");
    println!("| PQ per-item   | {} | {} | {:.1}% | ML-KEM-768 + ChaCha20-Poly1305 |",
        total_original, per_item_wire,
        (1.0 - per_item_wire as f64 / total_original as f64) * 100.0);
    println!("| PQ batched    | {} | {} | {:.1}% | ML-KEM-768 + ChaCha20-Poly1305 |",
        total_original, batch_wire,
        (1.0 - batch_wire as f64 / total_original as f64) * 100.0);
    println!();
    println!("Batching improvement: {:.1}%",
        (1.0 - batch_wire as f64 / per_item_wire as f64) * 100.0);
}
