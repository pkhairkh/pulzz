//! pulzZ SDK batched emission demo: build a 50-item batch on the server
//! side, ship it as a single BatchEnvelope record, and verify the client
//! decodes + caches all 50 items.
//!
//! Run with:
//! ```sh
//! cargo run --example batched -p pulzz-sdk
//! ```

use pulzz_sdk::{
    BatchBuilder, CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzServer,
    SecurityProfile, classic_pair_for_test,
};
use shared_protocol::{ItemId, SourceKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("pulzZ SDK batched emission demo");

    let stream_id = shared_protocol::StreamId(99);
    let (sender, receiver) = classic_pair_for_test(stream_id);

    let config = ClientConfig {
        security: SecurityProfile::ClassicRef1,
        carrier: CarrierKind::WebSocket,
        compression: CompressionConfig::default(),
        batch_size: Some(50),
        timeout: std::time::Duration::from_secs(5),
    };
    let mut server = PulzzServer::from_protector(sender, config.clone());
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        config,
    );

    // Build a 50-item batch using BatchBuilder
    let mut builder = BatchBuilder::new();
    for i in 1..=50u64 {
        let payload = format!("{{\"user\":\"alice-{i}\",\"event\":\"login\"}}").into_bytes();
        builder = builder.item(ItemId(i), SourceKind::Json, &payload);
    }
    let batch = builder.build();
    println!("  built batch of {} items", batch.len());

    // Server emits the batch as a single BatchEnvelope record
    let record = server.emit_batch(batch.items.clone())?;
    println!(
        "  server emitted BatchEnvelope record ({} bytes on wire, {} bytes raw)",
        record.to_bytes().len(),
        batch.raw_payload_bytes()
    );

    // Client receives + decodes
    client.push_record(record);
    let received = client.recv().await?.expect("client should receive a record");
    println!("  client received record type={:?}", received.header.record_type);

    // Verify all 50 items are cached
    let mut cached_count = 0;
    for i in 1..=50u64 {
        if client.session().state().cache_entry(ItemId(i)).is_some() {
            cached_count += 1;
        }
    }
    println!("  client cached {}/50 items", cached_count);
    assert_eq!(cached_count, 50, "all 50 items must be cached");

    println!("batched emission demo complete");
    Ok(())
}
